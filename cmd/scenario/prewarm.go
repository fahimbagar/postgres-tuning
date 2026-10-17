package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/fahimbagar/postgres-tuning/internal/bench"
	"github.com/fahimbagar/postgres-tuning/internal/pg"
	"github.com/jackc/pgx/v5"
)

func init() {
	register("prewarm", "extensions: pg_buffercache + pg_prewarm after a restart (run via `make prewarm`)", prewarm)
}

// `make prewarm` restarts Postgres between phases:
//
//	manual: autoprewarm off. Cold lookups, pg_prewarm by hand, lookups again.
//	dump:   autoprewarm on. Load the cache and write the block list to disk.
//	auto:   right after the next restart, see what autoprewarm brought back.
func prewarm(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("prewarm", flag.ExitOnError)
	phase := fs.String("phase", "manual", "manual | dump | auto")
	lookups := fs.Int("lookups", 3000, "random primary key lookups per measurement")
	fs.Parse(args)

	conn, err := pgx.Connect(ctx, "")
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	if err := pg.ExecAll(ctx, conn,
		`CREATE EXTENSION IF NOT EXISTS pg_buffercache`,
		`CREATE EXTENSION IF NOT EXISTS pg_prewarm`,
	); err != nil {
		return err
	}

	var uptimeSec float64
	if err := conn.QueryRow(ctx, `SELECT extract(epoch FROM now() - pg_postmaster_start_time())::float8`).Scan(&uptimeSec); err != nil {
		return err
	}
	uptime := time.Duration(uptimeSec * float64(time.Second))

	switch *phase {
	case "manual":
		pg.Section(fmt.Sprintf("just restarted (%s ago), pg_prewarm.autoprewarm = off", uptime.Round(time.Second)))
		if err := printCached(ctx, conn); err != nil {
			return err
		}
		if err := timedLookups(ctx, conn, *lookups, 1); err != nil {
			return err
		}

		pg.Section("SELECT pg_prewarm('customers'), pg_prewarm('customers_pkey')")
		var heap, idx int64
		start := time.Now()
		if err := conn.QueryRow(ctx, `SELECT pg_prewarm('customers'), pg_prewarm('customers_pkey')`).Scan(&heap, &idx); err != nil {
			return err
		}
		fmt.Printf("loaded %d + %d blocks in %s\n", heap, idx, time.Since(start).Round(time.Millisecond))
		if err := printCached(ctx, conn); err != nil {
			return err
		}
		// A different set of ids, so the improvement isn't just the first
		// run having touched the same pages.
		return timedLookups(ctx, conn, *lookups, 2)

	case "dump":
		pg.Section("pg_prewarm.autoprewarm = on: load the cache, then dump the block list")
		if _, err := conn.Exec(ctx, `SELECT pg_prewarm('customers'), pg_prewarm('customers_pkey')`); err != nil {
			return err
		}
		var blocks int64
		if err := conn.QueryRow(ctx, `SELECT autoprewarm_dump_now()`).Scan(&blocks); err != nil {
			return err
		}
		fmt.Printf("autoprewarm_dump_now() wrote %d blocks to autoprewarm.blocks\n", blocks)
		return nil

	case "auto":
		pg.Section(fmt.Sprintf("just restarted (%s ago), pg_prewarm.autoprewarm = on", uptime.Round(time.Second)))
		if err := printCached(ctx, conn); err != nil {
			return err
		}
		return timedLookups(ctx, conn, *lookups, 3)
	}
	return fmt.Errorf("unknown phase %q", *phase)
}

func printCached(ctx context.Context, conn *pgx.Conn) error {
	rows, err := conn.Query(ctx, `
		SELECT c.relname, count(b.bufferid), pg_size_pretty(count(b.bufferid) * 8192),
		       pg_size_pretty(pg_relation_size(c.oid))
		FROM pg_class c
		LEFT JOIN pg_buffercache b ON b.relfilenode = pg_relation_filenode(c.oid)
		     AND b.reldatabase = (SELECT oid FROM pg_database WHERE datname = current_database())
		WHERE c.relname IN ('customers', 'customers_pkey')
		GROUP BY c.oid, c.relname ORDER BY c.relname`)
	if err != nil {
		return err
	}
	type cached struct {
		Rel     string
		Buffers int64
		Cached  string
		Size    string
	}
	all, err := pgx.CollectRows(rows, pgx.RowToStructByPos[cached])
	if err != nil {
		return err
	}
	for _, c := range all {
		fmt.Printf("in shared_buffers: %-15s %6d buffers (%s of %s)\n", c.Rel, c.Buffers, c.Cached, c.Size)
	}
	return nil
}

// timedLookups does n primary key lookups on customers and reports latency
// and how many blocks came from outside shared_buffers.
func timedLookups(ctx context.Context, conn *pgx.Conn, n int, seed uint64) error {
	// pg_stat_force_next_flush() takes effect when the current transaction
	// ends, so it has to be its own statement before the read. Otherwise the
	// previous statement's counters (e.g. pg_prewarm's own reads) land in
	// the wrong measurement.
	read := func() (int64, error) {
		if _, err := conn.Exec(ctx, `SELECT pg_stat_force_next_flush()`); err != nil {
			return 0, err
		}
		var r int64
		err := conn.QueryRow(ctx, `SELECT pg_stat_clear_snapshot(),
			(SELECT heap_blks_read + idx_blks_read FROM pg_statio_user_tables WHERE relname = 'customers')`).
			Scan(nil, &r)
		return r, err
	}
	before, err := read()
	if err != nil {
		return err
	}
	r := rand.New(rand.NewPCG(seed, seed))
	res, err := bench.Repeat(ctx, n, func(ctx context.Context, _ int) error {
		_, err := conn.Exec(ctx, `SELECT email FROM customers WHERE id = $1`, 1+r.Int64N(1_000_000))
		return err
	})
	if err != nil {
		return err
	}
	after, err := read()
	if err != nil {
		return err
	}
	fmt.Printf("%d lookups: total %s, p50=%s p99=%s; blocks read from outside shared_buffers: %d\n",
		n, res.Elapsed.Round(time.Millisecond), bench.Round(res.P50), bench.Round(res.P99), after-before)
	return nil
}
