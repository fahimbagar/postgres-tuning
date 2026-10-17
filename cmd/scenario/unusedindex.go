package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/fahimbagar/postgres-tuning/internal/bench"
	"github.com/fahimbagar/postgres-tuning/internal/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func init() {
	register("unusedindex", "mistake: keeping unused and duplicate indexes", unusedIndex)
}

// Indexes that have never been scanned since stats were last reset. Unique
// and primary key indexes are skipped: they enforce constraints even when
// no query reads them.
const neverScanned = `
SELECT s.relname, s.indexrelname, s.idx_scan, pg_size_pretty(pg_relation_size(s.indexrelid))
FROM pg_stat_user_indexes s JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.idx_scan = 0 AND NOT i.indisunique AND s.relname = 'ui_events'
ORDER BY pg_relation_size(s.indexrelid) DESC`

// Pairs of indexes on the same table where one's columns are a leading
// prefix of the other's (or identical). indkey's text form is the column
// numbers separated by spaces, which makes the prefix test a LIKE.
// Expression and partial indexes are left out to keep the query honest.
const redundantIndexes = `
SELECT a.indexrelid::regclass::text AS redundant, b.indexrelid::regclass::text AS covered_by
FROM pg_index a JOIN pg_index b
  ON a.indrelid = b.indrelid AND a.indexrelid <> b.indexrelid
WHERE a.indrelid = 'ui_events'::regclass
  AND a.indexprs IS NULL AND b.indexprs IS NULL
  AND a.indpred IS NULL AND b.indpred IS NULL
  AND NOT a.indisunique
  AND (b.indkey::text LIKE a.indkey::text || ' %'
       OR (b.indkey::text = a.indkey::text AND a.indexrelid > b.indexrelid))`

func unusedIndex(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("unusedindex", flag.ExitOnError)
	workers := fs.Int("workers", 4, "concurrent inserting sessions")
	dur := fs.Duration("duration", 30*time.Second, "insert time per setup")
	fs.Parse(args)

	pool, err := pg.Pool(ctx, func(c *pgxpool.Config) { c.MaxConns = int32(*workers) + 1 })
	if err != nil {
		return err
	}
	defer pool.Close()

	setups := []struct {
		name    string
		indexes []string
	}{
		{"primary key + the one index the app uses", []string{
			`CREATE INDEX ui_events_account_created_idx ON ui_events (account_id, created_at)`,
		}},
		{"primary key + 7 more: unused, duplicate, prefix-redundant", []string{
			`CREATE INDEX ui_events_account_created_idx ON ui_events (account_id, created_at)`,
			`CREATE INDEX ui_events_account_idx ON ui_events (account_id)`,
			`CREATE INDEX ui_events_account_created_idx2 ON ui_events (account_id, created_at)`,
			`CREATE INDEX ui_events_created_idx ON ui_events (created_at)`,
			`CREATE INDEX ui_events_kind_idx ON ui_events (kind)`,
			`CREATE INDEX ui_events_kind_created_idx ON ui_events (kind, created_at)`,
			`CREATE INDEX ui_events_payload_idx ON ui_events (payload)`,
			`CREATE INDEX ui_events_lower_kind_idx ON ui_events (lower(kind))`,
		}},
	}

	for _, s := range setups {
		pg.Section(s.name)
		stmts := append([]string{
			`DROP TABLE IF EXISTS ui_events`,
			`CREATE TABLE ui_events (
				id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
				account_id int NOT NULL,
				kind text NOT NULL,
				payload text NOT NULL,
				created_at timestamptz NOT NULL DEFAULT now())`,
		}, s.indexes...)
		if err := pg.ExecAll(ctx, pool, stmts...); err != nil {
			return err
		}

		var walBefore string
		if err := pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&walBefore); err != nil {
			return err
		}
		// 100 rows per statement, generated server-side so the client is
		// not the bottleneck.
		res := bench.Run(ctx, *workers, 2*time.Second, *dur, func(ctx context.Context, _ int) error {
			_, err := pool.Exec(ctx, `INSERT INTO ui_events (account_id, kind, payload)
				SELECT (random() * 100000)::int,
				       (ARRAY['view','click','cart','buy'])[1 + (random() * 3)::int],
				       md5(random()::text)
				FROM generate_series(1, 100)`)
			return err
		})
		var rows int64
		var walBytes int64
		var size string
		if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM ui_events),
				pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::bigint,
				pg_size_pretty(pg_total_relation_size('ui_events'))`, walBefore).Scan(&rows, &walBytes, &size); err != nil {
			return err
		}
		fmt.Printf("insert statements (100 rows each): %s\n", res)
		fmt.Printf("rows/s: %.0f, WAL per row: %d bytes, table+indexes: %s\n",
			res.TPS()*100, walBytes/max(rows, 1), size)
	}

	// The app's real query, so the one useful index gets a scan count. Both
	// statements go in one simple-protocol call so the flush happens in the
	// session that did the scan.
	if _, err := pool.Exec(ctx, `SELECT count(*) FROM ui_events WHERE account_id = 42 AND created_at > now() - interval '1 hour';
		SELECT pg_stat_force_next_flush()`); err != nil {
		return err
	}

	pg.Section("never-scanned indexes")
	if err := printRows(ctx, pool, neverScanned); err != nil {
		return err
	}
	pg.Section("redundant indexes (same or leading columns of another index)")
	return printRows(ctx, pool, redundantIndexes)
}

// printRows prints every row of a query whose columns are all printable.
func printRows(ctx context.Context, q pg.Querier, sql string) error {
	rows, err := q.Query(ctx, sql)
	if err != nil {
		return err
	}
	all, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) ([]any, error) { return r.Values() })
	if err != nil {
		return err
	}
	for _, r := range all {
		fmt.Print("   ")
		for _, v := range r {
			fmt.Printf(" %v", v)
		}
		fmt.Println()
	}
	return nil
}
