// Package pg holds the few helpers every scenario needs: connecting, running
// EXPLAIN, and reading the counters used for before/after comparisons.
package pg

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Querier is satisfied by *pgxpool.Pool, *pgxpool.Conn, *pgx.Conn and pgx.Tx.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Pool connects using the libpq environment variables (PGHOST, PGUSER, ...).
// tweak, if not nil, can change the config before the pool is created.
func Pool(ctx context.Context, tweak func(*pgxpool.Config)) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, err
	}
	if tweak != nil {
		tweak(cfg)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// Explain runs EXPLAIN with the given options, e.g. "ANALYZE, BUFFERS".
// Empty options means a plain EXPLAIN.
func Explain(ctx context.Context, q Querier, options, sql string, args ...any) (string, error) {
	if options != "" {
		options = "(" + options + ") "
	}
	rows, err := q.Query(ctx, "EXPLAIN "+options+sql, args...)
	if err != nil {
		return "", err
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return "", err
	}
	return strings.Join(lines, "\n"), nil
}

// ExecAll runs each statement in order and stops at the first error.
func ExecAll(ctx context.Context, q Querier, stmts ...string) error {
	for _, s := range stmts {
		if _, err := q.Exec(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", firstLine(s), err)
		}
	}
	return nil
}

// Count returns the result of a query that yields a single integer.
func Count(ctx context.Context, q Querier, sql string, args ...any) (int64, error) {
	var n int64
	err := q.QueryRow(ctx, sql, args...).Scan(&n)
	return n, err
}

// TableExists reports whether a table exists and has at least `rows` rows,
// so seeding can be skipped on a rerun.
func TableExists(ctx context.Context, q Querier, table string, rows int64) (bool, error) {
	var ok bool
	err := q.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&ok)
	if err != nil || !ok {
		return false, err
	}
	n, err := Count(ctx, q, "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize())
	return n >= rows, err
}

// RelationSize returns pg_total_relation_size (table + indexes + toast) as
// a human readable string.
func RelationSize(ctx context.Context, q Querier, rel string) (string, error) {
	var s string
	err := q.QueryRow(ctx, "SELECT pg_size_pretty(pg_total_relation_size($1::regclass))", rel).Scan(&s)
	return s, err
}

func Section(title string) {
	fmt.Printf("\n== %s ==\n", title)
}

func Indent(s string) string {
	return "    " + strings.ReplaceAll(s, "\n", "\n    ")
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i] + " ..."
	}
	return s
}
