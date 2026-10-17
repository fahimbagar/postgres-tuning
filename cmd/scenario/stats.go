package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fahimbagar/postgres-tuning/internal/bench"
	"github.com/fahimbagar/postgres-tuning/internal/pg"
)

func init() {
	register("stats", "mistake: planner assumes independent columns (CREATE STATISTICS)", stats)
}

// Newest orders for a city/warehouse pair, the kind of query a support tool
// runs from two dropdowns. Every city is served by exactly one warehouse, so
// most pairs match nothing.
const ordersForPair = `SELECT id, total, created_at FROM st_orders
WHERE city = $1 AND warehouse = $2 ORDER BY created_at DESC LIMIT 20`

func stats(ctx context.Context, _ []string) error {
	// QueryExecModeExec plans every call with the real parameter values, so
	// the generic-plan behaviour from the genericplan scenario can't mix in.
	pool, err := pg.Pool(ctx, func(c *pgxpool.Config) {
		c.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	})
	if err != nil {
		return err
	}
	defer pool.Close()

	if ok, err := pg.TableExists(ctx, pool, "st_orders", 3_000_000); err != nil {
		return err
	} else if !ok {
		fmt.Println("creating st_orders (first 3M rows of orders)")
		if err := pg.ExecAll(ctx, pool,
			`DROP TABLE IF EXISTS st_orders`,
			`CREATE TABLE st_orders AS SELECT * FROM orders WHERE id <= 3000000`,
			`ALTER TABLE st_orders ADD PRIMARY KEY (id)`,
			`CREATE INDEX ON st_orders (city)`,
			`CREATE INDEX ON st_orders (created_at)`,
			`VACUUM ANALYZE st_orders`,
		); err != nil {
			return err
		}
	}
	if err := pg.ExecAll(ctx, pool,
		`DROP STATISTICS IF EXISTS st_orders_city_warehouse`,
		`ANALYZE st_orders`,
	); err != nil {
		return err
	}

	pairs := [][2]string{{"Jakarta", "SG-WH1"}, {"Jakarta", "ID-WH1"}}

	pg.Section("default statistics (each column on its own)")
	if err := statsRun(ctx, pool, pairs); err != nil {
		return err
	}

	pg.Section("after CREATE STATISTICS ... (mcv) ON city, warehouse")
	if err := pg.ExecAll(ctx, pool,
		`CREATE STATISTICS st_orders_city_warehouse (mcv) ON city, warehouse FROM st_orders`,
		`ANALYZE st_orders`,
	); err != nil {
		return err
	}
	return statsRun(ctx, pool, pairs)
}

func statsRun(ctx context.Context, pool *pgxpool.Pool, pairs [][2]string) error {
	for _, p := range pairs {
		plan, err := pg.Explain(ctx, pool, "ANALYZE, BUFFERS", ordersForPair, p[0], p[1])
		if err != nil {
			return err
		}
		// Warm-up already happened in EXPLAIN ANALYZE above.
		res, err := bench.Repeat(ctx, 10, func(ctx context.Context, _ int) error {
			_, err := pool.Exec(ctx, ordersForPair, p[0], p[1])
			return err
		})
		if err != nil {
			return err
		}
		fmt.Printf("\ncity=%s warehouse=%s: 10 runs p50=%s max=%s\n", p[0], p[1], bench.Round(res.P50), bench.Round(res.Max))
		for line := range strings.Lines(plan) {
			// Keep the node lines (with estimates and actuals) and the
			// filter details; drop buffer and timing noise.
			t := strings.TrimSpace(line)
			if strings.Contains(t, "rows=") || strings.HasPrefix(t, "Filter") ||
				strings.HasPrefix(t, "Rows Removed") || strings.HasPrefix(t, "Execution Time") {
				fmt.Print(pg.Indent(strings.TrimRight(line, "\n")), "\n")
			}
		}
	}
	return nil
}
