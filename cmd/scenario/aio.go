package main

import (
	"context"
	"flag"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/fahimbagar/postgres-tuning/internal/pg"
)

func init() {
	register("aio", "PG 18: sequential scan throughput under the current io_method (run via `make aio`)", aio)
}

// io_method only changes at server start, so `make aio` sets it with ALTER
// SYSTEM, restarts the pod, and runs this once per value.
func aio(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("aio", flag.ContinueOnError)
	rows := fs.Int("rows", 3_000_000, "rows of ~1kB each; must be well over the pod's 2GiB memory")
	runs := fs.Int("runs", 3, "scans to time")
	if err := fs.Parse(args); err != nil {
		return err
	}

	conn, err := pgx.Connect(ctx, "")
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if ok, err := pg.TableExists(ctx, conn, "aio_scan", int64(*rows)); err != nil {
		return err
	} else if !ok {
		fmt.Printf("creating aio_scan (%d rows of ~1kB)\n", *rows)
		// No indexes: this table only exists to be scanned.
		if err := pg.ExecAll(ctx, conn,
			`DROP TABLE IF EXISTS aio_scan`,
			`CREATE TABLE aio_scan (id bigint, filler text)`,
			fmt.Sprintf(`INSERT INTO aio_scan SELECT g, repeat(md5(g::text), 30) FROM generate_series(1, %d) g`, *rows),
			`VACUUM (FREEZE, ANALYZE) aio_scan`,
		); err != nil {
			return err
		}
	}

	var method, workers, eic, size string
	var bytes int64
	if err := conn.QueryRow(ctx, `SELECT current_setting('io_method'), current_setting('io_workers'),
			current_setting('effective_io_concurrency'),
			pg_size_pretty(pg_relation_size('aio_scan')), pg_relation_size('aio_scan')`).
		Scan(&method, &workers, &eic, &size, &bytes); err != nil {
		return err
	}
	pg.Section(fmt.Sprintf("io_method=%s io_workers=%s effective_io_concurrency=%s, table %s", method, workers, eic, size))

	// One backend, no parallel workers: the point is how much a single
	// process can read, not how many processes we can throw at it.
	if _, err := conn.Exec(ctx, "SET max_parallel_workers_per_gather = 0"); err != nil {
		return err
	}

	// Sample pg_aios while scanning to see how many I/Os are in flight.
	monitor, err := pgx.Connect(ctx, "")
	if err != nil {
		return err
	}
	defer monitor.Close(context.Background())

	for i := range *runs {
		var peak atomic.Int64
		sctx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for sctx.Err() == nil {
				var n int64
				if err := monitor.QueryRow(sctx, "SELECT count(*) FROM pg_aios").Scan(&n); err == nil && n > peak.Load() {
					peak.Store(n)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}()

		start := time.Now()
		plan, err := pg.Explain(ctx, conn, "ANALYZE, BUFFERS, TIMING OFF", "SELECT count(*) FROM aio_scan")
		elapsed := time.Since(start)
		stop()
		<-done
		if err != nil {
			return err
		}
		fmt.Printf("scan %d: %s, %.0f MB/s, peak in-flight I/Os %d\n",
			i+1, elapsed.Round(time.Millisecond), float64(bytes)/1e6/elapsed.Seconds(), peak.Load())
		printPlanLines(plan, "Buffers:", "I/O Timings:")
	}
	return nil
}
