package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/fahimbagar/postgres-tuning/internal/pg"
	"github.com/jackc/pgx/v5"
)

func init() {
	register("fkindex", "mistake: foreign key column without an index", fkIndex)
}

// Lists foreign keys whose columns are not the leading columns of any index
// on the referencing table. Works for multi-column keys too.
const unindexedFKs = `
SELECT c.conrelid::regclass AS table_name,
       string_agg(a.attname, ', ' ORDER BY x.n) AS columns,
       c.confrelid::regclass AS referenced_table,
       pg_size_pretty(pg_relation_size(c.conrelid)) AS table_size
FROM pg_constraint c
CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS x(attnum, n)
JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = x.attnum
WHERE c.contype = 'f'
  AND NOT EXISTS (
    SELECT 1 FROM pg_index i
    WHERE i.indrelid = c.conrelid
      AND (i.indkey::int2[])[0:cardinality(c.conkey) - 1] @> c.conkey
  )
GROUP BY c.oid, c.conrelid, c.confrelid
ORDER BY pg_relation_size(c.conrelid) DESC`

// Cleaning up 100 accounts that never ordered anything.
const deleteInactive = `DELETE FROM fk_customers WHERE id BETWEEN 90001 AND 90100`

func fkIndex(ctx context.Context, args []string) error {
	conn, err := pgx.Connect(ctx, "")
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	fmt.Println("creating fk_customers (100k) and fk_orders (2M, only for customers 1..90000)")
	if err := pg.ExecAll(ctx, conn,
		`DROP TABLE IF EXISTS fk_orders, fk_customers`,
		`CREATE TABLE fk_customers (id bigint PRIMARY KEY, email text NOT NULL)`,
		`CREATE TABLE fk_orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL REFERENCES fk_customers, total numeric(10,2) NOT NULL)`,
		`INSERT INTO fk_customers SELECT g, 'user' || g || '@example.com' FROM generate_series(1, 100000) g`,
		`INSERT INTO fk_orders SELECT g, 1 + (g % 90000), 10 FROM generate_series(1, 2000000) g`,
		`VACUUM ANALYZE fk_customers, fk_orders`,
	); err != nil {
		return err
	}

	pg.Section("foreign keys without a matching index (whole database)")
	rows, err := conn.Query(ctx, unindexedFKs)
	if err != nil {
		return err
	}
	type fk struct{ Table, Columns, Referenced, Size string }
	fks, err := pgx.CollectRows(rows, pgx.RowToStructByPos[fk])
	if err != nil {
		return err
	}
	for _, f := range fks {
		fmt.Printf("    %s(%s) -> %s, table size %s\n", f.Table, f.Columns, f.Referenced, f.Size)
	}

	pg.Section("DELETE 100 parent rows, no index on fk_orders.customer_id")
	if err := fkDelete(ctx, conn); err != nil {
		return err
	}

	pg.Section("same DELETE after CREATE INDEX ON fk_orders (customer_id)")
	start := time.Now()
	if _, err := conn.Exec(ctx, `CREATE INDEX ON fk_orders (customer_id)`); err != nil {
		return err
	}
	fmt.Printf("index build took %s\n", time.Since(start).Round(time.Millisecond))
	return fkDelete(ctx, conn)
}

// fkDelete runs the delete under EXPLAIN ANALYZE and rolls it back, so both
// runs delete the same rows.
func fkDelete(ctx context.Context, conn *pgx.Conn) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	plan, err := pg.Explain(ctx, tx, "ANALYZE, COSTS OFF", deleteInactive)
	if err != nil {
		return err
	}
	for line := range strings.Lines(plan) {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "Trigger for constraint") || strings.HasPrefix(t, "Execution Time") {
			fmt.Println(pg.Indent(t))
		}
	}
	return nil
}
