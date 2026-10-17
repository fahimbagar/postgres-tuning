package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/fahimbagar/postgres-tuning/internal/pg"
)

func init() {
	register("autovacuum", "mistake: default autovacuum thresholds on a big table", autovacuum)
}

// Accounts in a range: the planner uses an index-only scan, which is only
// cheap while the visibility map says the heap pages are all-visible.
const accountRange = `SELECT count(*) FROM av_events WHERE account_id BETWEEN 1000 AND 51000`

func autovacuum(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("autovacuum", flag.ContinueOnError)
	rows := fs.Int("rows", 5_000_000, "table size")
	updatePct := fs.Int("update-pct", 10, "percent of rows to update")
	wait := fs.Duration("wait", 90*time.Second, "how long to give autovacuum (autovacuum_naptime is 1min)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	conn, err := pgx.Connect(ctx, "")
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	fmt.Printf("creating av_events with %d rows\n", *rows)
	if err := pg.ExecAll(ctx, conn,
		`DROP TABLE IF EXISTS av_events`,
		`CREATE TABLE av_events (id bigint PRIMARY KEY, account_id int NOT NULL, kind text NOT NULL, updated_at timestamptz NOT NULL)`,
		fmt.Sprintf(`INSERT INTO av_events SELECT g, 1 + (g %% 500000), 'created', now() FROM generate_series(1, %d) g`, *rows),
		`CREATE INDEX av_events_account_idx ON av_events (account_id)`,
		`VACUUM ANALYZE av_events`,
	); err != nil {
		return err
	}

	pg.Section("freshly vacuumed")
	if err := avReport(ctx, conn); err != nil {
		return err
	}

	pg.Section(fmt.Sprintf("after updating %d%% of rows, then waiting %s", *updatePct, *wait))
	n := *rows * *updatePct / 100
	if _, err := conn.Exec(ctx, fmt.Sprintf(
		`UPDATE av_events SET kind = 'updated', updated_at = now() WHERE id %% 100 < %d`, *updatePct)); err != nil {
		return err
	}
	fmt.Printf("updated %d rows\n", n)
	time.Sleep(*wait)
	if err := avReport(ctx, conn); err != nil {
		return err
	}

	pg.Section("ALTER TABLE av_events SET (autovacuum_vacuum_scale_factor = 0.01)")
	if _, err := conn.Exec(ctx, `ALTER TABLE av_events SET (autovacuum_vacuum_scale_factor = 0.01)`); err != nil {
		return err
	}
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		var done bool
		if err := conn.QueryRow(ctx, `SELECT last_autovacuum IS NOT NULL FROM pg_stat_user_tables WHERE relname = 'av_events'`).Scan(&done); err != nil {
			return err
		}
		if done {
			break
		}
		time.Sleep(5 * time.Second)
	}
	return avReport(ctx, conn)
}

func avReport(ctx context.Context, conn *pgx.Conn) error {
	// Make sure this session sees the latest counters.
	if _, err := conn.Exec(ctx, "SELECT pg_stat_clear_snapshot()"); err != nil {
		return err
	}
	var (
		live, dead   int64
		lastAuto     *time.Time
		scale, base  float64
		maxThreshold int64
		reltuples    float64
	)
	err := conn.QueryRow(ctx, `
		SELECT s.n_live_tup, s.n_dead_tup, s.last_autovacuum,
		       coalesce((SELECT split_part(o, '=', 2)::float8 FROM unnest(c.reloptions) o
		                 WHERE o LIKE 'autovacuum_vacuum_scale_factor=%'),
		                current_setting('autovacuum_vacuum_scale_factor')::float8),
		       current_setting('autovacuum_vacuum_threshold')::float8,
		       current_setting('autovacuum_vacuum_max_threshold')::bigint,
		       c.reltuples
		FROM pg_stat_user_tables s JOIN pg_class c ON c.oid = s.relid
		WHERE s.relname = 'av_events'`).
		Scan(&live, &dead, &lastAuto, &scale, &base, &maxThreshold, &reltuples)
	if err != nil {
		return err
	}
	// The formula from the docs, including the PG 18 cap.
	threshold := min(base+scale*reltuples, float64(maxThreshold))
	auto := "never"
	if lastAuto != nil {
		auto = lastAuto.UTC().Format(time.TimeOnly)
	}
	fmt.Printf("n_live_tup=%d n_dead_tup=%d autovacuum triggers at %.0f dead rows (scale_factor=%g, max_threshold=%d); last_autovacuum=%s\n",
		live, dead, threshold, scale, maxThreshold, auto)

	plan, err := pg.Explain(ctx, conn, "ANALYZE, BUFFERS", accountRange)
	if err != nil {
		return err
	}
	printPlanLines(plan, "Index Only Scan", "Heap Fetches", "Buffers", "Execution Time")
	start := time.Now()
	for range 5 {
		if _, err := conn.Exec(ctx, accountRange); err != nil {
			return err
		}
	}
	fmt.Printf("account range count, mean of 5: %s\n", (time.Since(start) / 5).Round(100*time.Microsecond))
	return nil
}
