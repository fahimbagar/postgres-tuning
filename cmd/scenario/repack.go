package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/fahimbagar/postgres-tuning/internal/bench"
	"github.com/fahimbagar/postgres-tuning/internal/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func init() {
	register("repack", "extensions: pgstattuple to measure bloat, pg_repack vs VACUUM FULL to remove it", repack)
}

func repack(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("repack", flag.ExitOnError)
	rows := fs.Int("rows", 1_500_000, "rows per table before the delete")
	fs.Parse(args)

	pool, err := pg.Pool(ctx, func(c *pgxpool.Config) { c.MaxConns = 4 })
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := pg.ExecAll(ctx, pool,
		`CREATE EXTENSION IF NOT EXISTS pgstattuple`,
		`CREATE EXTENSION IF NOT EXISTS pg_repack`,
	); err != nil {
		return err
	}

	for _, t := range []string{"rp_vacuum_full", "rp_repack"} {
		fmt.Printf("creating %s: %d rows, then deleting 70%% and running plain VACUUM\n", t, *rows)
		id := pgx.Identifier{t}.Sanitize()
		if err := pg.ExecAll(ctx, pool,
			`DROP TABLE IF EXISTS `+id,
			`CREATE TABLE `+id+` (id bigint PRIMARY KEY, account_id int NOT NULL, payload text NOT NULL, created_at timestamptz NOT NULL DEFAULT now())`,
			fmt.Sprintf(`INSERT INTO %s SELECT g, g %% 10000, repeat(md5(g::text), 6) FROM generate_series(1, %d) g`, id, *rows),
			`DELETE FROM `+id+` WHERE id % 10 < 7`,
			`VACUUM ANALYZE `+id,
		); err != nil {
			return err
		}
	}

	pg.Section("bloat after the delete (pgstattuple, pgstatindex)")
	for _, t := range []string{"rp_vacuum_full", "rp_repack"} {
		if err := printBloat(ctx, pool, t); err != nil {
			return err
		}
	}

	pg.Section("VACUUM FULL rp_vacuum_full, with a writer updating the table")
	err = withWriter(ctx, pool, "rp_vacuum_full", func() error {
		_, err := pool.Exec(ctx, `VACUUM FULL rp_vacuum_full`)
		return err
	})
	if err != nil {
		return err
	}

	pg.Section("pg_repack --no-order -t rp_repack, with the same writer")
	err = withWriter(ctx, pool, "rp_repack", func() error {
		// pg_repack reads PGHOST, PGUSER, PGPASSWORD and PGDATABASE like psql.
		out, err := exec.CommandContext(ctx, "pg_repack", "--no-order", "--table", "rp_repack").CombinedOutput()
		fmt.Println(pg.Indent(strings.TrimRight(string(out), "\n")))
		return err
	})
	if err != nil {
		return err
	}

	pg.Section("after")
	for _, t := range []string{"rp_vacuum_full", "rp_repack"} {
		if err := printBloat(ctx, pool, t); err != nil {
			return err
		}
	}
	return nil
}

func printBloat(ctx context.Context, q pg.Querier, table string) error {
	var (
		size                       string
		tuplePct, deadPct, freePct float64
		leafDensity                float64
	)
	err := q.QueryRow(ctx, `
		SELECT pg_size_pretty(t.table_len), t.tuple_percent, t.dead_tuple_percent, t.free_percent,
		       i.avg_leaf_density
		FROM pgstattuple($1::regclass) t, pgstatindex($2::regclass) i`, table, table+"_pkey").
		Scan(&size, &tuplePct, &deadPct, &freePct, &leafDensity)
	if err != nil {
		return err
	}
	fmt.Printf("%-15s heap %8s: live tuples %4.1f%%, dead %4.1f%%, free space %4.1f%%; pkey leaf density %4.1f%%\n",
		table, size, tuplePct, deadPct, freePct, leafDensity)
	return nil
}

// withWriter runs op while one goroutine keeps updating random rows of the
// table, then reports how long op took and what the writer saw meanwhile.
func withWriter(ctx context.Context, pool *pgxpool.Pool, table string, op func() error) error {
	wctx, stop := context.WithCancel(ctx)
	var (
		mu  sync.Mutex
		lat []time.Duration
		wg  sync.WaitGroup
	)
	sql := `UPDATE ` + pgx.Identifier{table}.Sanitize() + ` SET payload = md5(random()::text) WHERE id = $1`
	wg.Go(func() {
		r := rand.New(rand.NewPCG(1, 1))
		for wctx.Err() == nil {
			// Rows with id % 10 >= 7 survived the delete.
			id := 10*(1+r.Int64N(100_000)) + 7 + r.Int64N(3)
			start := time.Now()
			if _, err := pool.Exec(wctx, sql, id); err != nil {
				if wctx.Err() == nil {
					fmt.Println("writer:", err)
				}
				continue
			}
			mu.Lock()
			lat = append(lat, time.Since(start))
			mu.Unlock()
		}
	})

	time.Sleep(2 * time.Second) // let the writer get going
	start := time.Now()
	err := op()
	took := time.Since(start)
	time.Sleep(time.Second)
	stop()
	wg.Wait()
	if err != nil {
		return err
	}

	if len(lat) == 0 {
		return fmt.Errorf("writer completed no updates")
	}
	slow := 0
	for _, l := range lat {
		if l > 100*time.Millisecond {
			slow++
		}
	}
	fmt.Printf("operation took %s; writer: %d updates, slowest %s, %d took over 100ms\n",
		took.Round(time.Millisecond), len(lat), bench.Round(slices.Max(lat)), slow)
	return nil
}
