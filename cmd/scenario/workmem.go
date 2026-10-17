package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/fahimbagar/postgres-tuning/internal/bench"
	"github.com/fahimbagar/postgres-tuning/internal/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func init() {
	register("workmem", "mistake: raising work_mem for the whole server", workMem)
}

// Top customers of the last 30 days. About 450k orders and 365k groups, so
// the hash table needs ~150MB and spills to disk at the default 4MB.
const topCustomers = `SELECT customer_id, count(*), sum(total)
FROM orders WHERE created_at >= '2026-08-28'
GROUP BY customer_id ORDER BY sum(total) DESC LIMIT 20`

func workMem(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("workmem", flag.ExitOnError)
	big := fs.String("big", "256MB", "the 'just raise it' value")
	concurrency := fs.Int("concurrency", 12, "reports running at once in the server-wide test")
	dur := fs.Duration("duration", 30*time.Second, "length of each concurrency test")
	fs.Parse(args)

	pool, err := pg.Pool(ctx, nil)
	if err != nil {
		return err
	}
	defer pool.Close()

	pg.Section("one report, default work_mem (4MB)")
	if err := reportOnce(ctx, pool, ""); err != nil {
		return err
	}

	pg.Section(fmt.Sprintf("one report, SET LOCAL work_mem = '%s' in its transaction", *big))
	if err := reportOnce(ctx, pool, *big); err != nil {
		return err
	}

	// The server-wide test can get backends OOM-killed. Run the safe variant
	// first and always put the setting back, even if the server restarted.
	pg.Section(fmt.Sprintf("%d concurrent reports, default work_mem", *concurrency))
	if err := concurrentReports(ctx, *concurrency, *dur); err != nil {
		return err
	}

	pg.Section(fmt.Sprintf("%d concurrent reports, ALTER SYSTEM SET work_mem = '%s'", *concurrency, *big))
	if _, err := pool.Exec(ctx, fmt.Sprintf("ALTER SYSTEM SET work_mem = '%s'", *big)); err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		return err
	}
	runErr := concurrentReports(ctx, *concurrency, *dur)
	resetErr := resetWorkMem(ctx)
	return errors.Join(runErr, resetErr)
}

func reportOnce(ctx context.Context, pool *pgxpool.Pool, workMem string) error {
	// One connection for everything, so pg_stat_force_next_flush below
	// flushes the counters of the session that ran the reports.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Every run happens in a transaction so SET LOCAL has somewhere to live
	// and disappears at COMMIT.
	inTx := func(fn func(tx pgx.Tx) error) error {
		return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if workMem != "" {
				if _, err := tx.Exec(ctx, "SET LOCAL work_mem = '"+workMem+"'"); err != nil {
					return err
				}
			}
			return fn(tx)
		})
	}

	var plan string
	if err := inTx(func(tx pgx.Tx) (err error) {
		plan, err = pg.Explain(ctx, tx, "ANALYZE, BUFFERS, COSTS OFF", topCustomers)
		return err
	}); err != nil {
		return err
	}
	printPlanLines(plan, "Batches:", "temp read", "Execution Time")

	before, err := tempStats(ctx, conn)
	if err != nil {
		return err
	}
	res, err := bench.Repeat(ctx, 5, func(ctx context.Context, _ int) error {
		return inTx(func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, topCustomers)
			return err
		})
	})
	if err != nil {
		return err
	}
	after, err := tempStats(ctx, conn)
	if err != nil {
		return err
	}
	fmt.Printf("5 runs: p50=%s max=%s, temp files=%d, temp written=%s\n",
		res.P50.Round(time.Millisecond), res.Max.Round(time.Millisecond),
		after.files-before.files, mib(after.bytes-before.bytes))
	return nil
}

// printPlanLines prints the first plan line containing each marker. Buffer
// and timing lines repeat on every parent node; the first one is the total.
func printPlanLines(plan string, markers ...string) {
	for _, m := range markers {
		for line := range strings.Lines(plan) {
			if strings.Contains(line, m) {
				fmt.Println(pg.Indent(strings.TrimSpace(line)))
				break
			}
		}
	}
}

type temp struct{ files, bytes int64 }

func tempStats(ctx context.Context, q pg.Querier) (temp, error) {
	// Backends report to pg_stat_database at most once a second.
	// pg_stat_force_next_flush() makes this session report when its current
	// transaction ends, so it must be a separate statement from the read.
	if _, err := q.Exec(ctx, "SELECT pg_stat_force_next_flush()"); err != nil {
		return temp{}, err
	}
	var t temp
	err := q.QueryRow(ctx, `SELECT pg_stat_clear_snapshot(), temp_files, temp_bytes
		FROM pg_stat_database WHERE datname = current_database()`).Scan(nil, &t.files, &t.bytes)
	return t, err
}

func concurrentReports(ctx context.Context, n int, d time.Duration) error {
	// Fresh pool so every session starts with the server's current settings.
	pool, err := pg.Pool(ctx, func(c *pgxpool.Config) { c.MaxConns = int32(n) })
	if err != nil {
		return err
	}
	defer pool.Close()

	monitor, err := pgx.Connect(ctx, "")
	if err != nil {
		return err
	}
	defer monitor.Close(context.Background())

	startAnon, err := readAnon(ctx, monitor)
	if err != nil {
		return err
	}
	var bootedAt time.Time
	if err := monitor.QueryRow(ctx, "SELECT pg_postmaster_start_time()").Scan(&bootedAt); err != nil {
		return err
	}
	sampler := startMemSampler(ctx, monitor, 250*time.Millisecond)
	res := bench.Run(ctx, n, 0, d, func(ctx context.Context, _ int) error {
		_, err := pool.Exec(ctx, topCustomers)
		return err
	})
	peak := sampler.Stop()

	fmt.Println("reports:", res)
	fmt.Printf("backend memory (cgroup anon): %s at start, %s peak seen\n", mib(startAnon), mib(peak))

	// Kubernetes (1.28+, cgroup v2) sets memory.oom.group, so an OOM kills
	// the whole container rather than one backend. The container's cgroup
	// and its oom_kill counter are recreated on restart, so compare the
	// postmaster start time instead.
	rebootedAt, err := waitForServer(ctx)
	if err != nil {
		return err
	}
	if rebootedAt.Equal(bootedAt) {
		fmt.Println("Postgres restarted during the test: no")
	} else {
		fmt.Printf("Postgres restarted during the test: YES (was up since %s, now since %s)\n",
			bootedAt.UTC().Format(time.TimeOnly), rebootedAt.UTC().Format(time.TimeOnly))
	}
	return nil
}

// waitForServer reconnects until the server accepts queries again (after an
// OOM kill the pod restarts and runs crash recovery first) and returns the
// postmaster start time.
func waitForServer(ctx context.Context) (time.Time, error) {
	deadline := time.Now().Add(3 * time.Minute)
	for {
		var t time.Time
		conn, err := pgx.Connect(ctx, "")
		if err == nil {
			err = conn.QueryRow(ctx, "SELECT pg_postmaster_start_time()").Scan(&t)
			conn.Close(context.Background())
			if err == nil {
				return t, nil
			}
		}
		if time.Now().After(deadline) {
			return time.Time{}, fmt.Errorf("server not back after 3 minutes: %w", err)
		}
		time.Sleep(time.Second)
	}
}

func resetWorkMem(ctx context.Context) error {
	if _, err := waitForServer(ctx); err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, "")
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	return pg.ExecAll(ctx, conn, "ALTER SYSTEM RESET work_mem", "SELECT pg_reload_conf()")
}
