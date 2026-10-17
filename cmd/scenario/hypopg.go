package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/fahimbagar/postgres-tuning/internal/pg"
)

func init() {
	register("hypopg", "extension: try an index with hypopg before building it", hypopg)
}

// Two candidate indexes for two queries. One will be used, one won't.
var hypoCases = []struct {
	query, index string
}{
	{`SELECT id, tier FROM customers WHERE email = 'user4242@example.com'`,
		`CREATE INDEX ON customers (email)`},
	{`SELECT count(*) FROM customers WHERE tier = 'standard'`,
		`CREATE INDEX ON customers (tier)`},
}

func hypopg(ctx context.Context, _ []string) error {
	conn, err := pgx.Connect(ctx, "")
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS hypopg`); err != nil {
		return err
	}

	for _, c := range hypoCases {
		pg.Section(c.query)
		if err := hypoCase(ctx, conn, c.query, c.index); err != nil {
			return err
		}
	}
	return nil
}

func hypoCase(ctx context.Context, conn *pgx.Conn, query, index string) error {
	plan, err := pg.Explain(ctx, conn, "", query)
	if err != nil {
		return err
	}
	fmt.Println("plan now:")
	fmt.Println(pg.Indent(plan))

	// hypopg_create_index only exists in this session's memory: no disk,
	// no lock on the table, nothing for other sessions to see.
	start := time.Now()
	var hypoName string
	var hypoSize int64
	if err := conn.QueryRow(ctx, `SELECT indexname, hypopg_relation_size(indexrelid)
		FROM hypopg_create_index($1)`, index).Scan(&hypoName, &hypoSize); err != nil {
		return err
	}
	fmt.Printf("\nhypothetical %s (%s) created in %s, estimated size %s\n",
		hypoName, index, time.Since(start).Round(time.Microsecond), mib(hypoSize))

	plan, err = pg.Explain(ctx, conn, "", query)
	if err != nil {
		return err
	}
	fmt.Println("plan with the hypothetical index (plain EXPLAIN; EXPLAIN ANALYZE ignores it):")
	fmt.Println(pg.Indent(plan))
	if _, err := conn.Exec(ctx, `SELECT hypopg_reset()`); err != nil {
		return err
	}

	// For comparison, the real thing. Dropped again so the shared tables
	// stay as seeded.
	start = time.Now()
	if _, err := conn.Exec(ctx, index); err != nil {
		return err
	}
	build := time.Since(start)
	var realName string
	var realSize int64
	if err := conn.QueryRow(ctx, `SELECT c.relname, pg_relation_size(c.oid) FROM pg_class c
		JOIN pg_index i ON i.indexrelid = c.oid
		WHERE i.indrelid = 'customers'::regclass ORDER BY c.oid DESC LIMIT 1`).Scan(&realName, &realSize); err != nil {
		return err
	}
	plan, err = pg.Explain(ctx, conn, "ANALYZE, COSTS OFF", query)
	if err != nil {
		return err
	}
	fmt.Printf("\nreal index %s built in %s, actual size %s\n", realName, build.Round(time.Millisecond), mib(realSize))
	fmt.Println(pg.Indent(plan))
	_, err = conn.Exec(ctx, "DROP INDEX "+pgx.Identifier{realName}.Sanitize())
	return err
}
