package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fahimbagar/postgres-tuning/internal/bench"
	"github.com/fahimbagar/postgres-tuning/internal/pg"
)

func init() {
	register("genericplan", "mistake: pgx prepared statements switch to a generic plan on skewed data", genericPlan)
}

// The admin "latest orders by status" page. 90% of orders are delivered,
// 0.01% are refunded.
const latestByStatus = `SELECT id, customer_id, total, created_at FROM orders
WHERE status = $1 ORDER BY created_at DESC LIMIT 50`

func genericPlan(ctx context.Context, _ []string) error {
	setups := []struct {
		name  string
		tweak func(*pgxpool.Config)
	}{
		{"pgx default (QueryExecModeCacheStatement)", nil},
		{"pgx default + plan_cache_mode=force_custom_plan", func(c *pgxpool.Config) {
			c.ConnConfig.RuntimeParams["plan_cache_mode"] = "force_custom_plan"
		}},
		{"pgx QueryExecModeExec (unnamed statement)", func(c *pgxpool.Config) {
			c.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
		}},
	}

	for i, s := range setups {
		pg.Section(s.name)
		if err := genericPlanRun(ctx, s.tweak, i == 0); err != nil {
			return err
		}
	}
	return nil
}

func genericPlanRun(ctx context.Context, tweak func(*pgxpool.Config), explain bool) error {
	// One connection, so every call hits the same prepared statement.
	pool, err := pg.Pool(ctx, func(c *pgxpool.Config) {
		c.MaxConns = 1
		if tweak != nil {
			tweak(c)
		}
	})
	if err != nil {
		return err
	}
	defer pool.Close()

	// Normal traffic first: the page is mostly opened for busy statuses.
	for _, status := range []string{"delivered", "shipped", "delivered", "pending", "delivered", "delivered", "shipped", "delivered"} {
		if _, err := pool.Exec(ctx, latestByStatus, status); err != nil {
			return err
		}
	}
	if err := printPlanCounters(ctx, pool); err != nil {
		return err
	}

	for _, status := range []string{"delivered", "refunded"} {
		res, err := bench.Repeat(ctx, 20, func(ctx context.Context, _ int) error {
			_, err := pool.Exec(ctx, latestByStatus, status)
			return err
		})
		if err != nil {
			return err
		}
		fmt.Printf("status=%-9s 20 runs: p50=%s max=%s\n", status, bench.Round(res.P50), bench.Round(res.Max))
	}
	if !explain {
		return nil
	}

	// EXPLAIN (GENERIC_PLAN) (PG 16+) shows the plan a prepared statement
	// falls back to, without needing parameter values. It has to go over
	// the simple protocol: the extended protocol would insist on a value
	// for $1.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	results, err := conn.Conn().PgConn().Exec(ctx, "EXPLAIN (GENERIC_PLAN, COSTS OFF) "+latestByStatus).ReadAll()
	conn.Release()
	if err != nil {
		return err
	}
	fmt.Println("generic plan:")
	for _, row := range results[0].Rows {
		fmt.Println(pg.Indent(string(row[0])))
	}
	plan, err := pg.Explain(ctx, pool, "COSTS OFF",
		"SELECT id, customer_id, total, created_at FROM orders WHERE status = 'refunded' ORDER BY created_at DESC LIMIT 50")
	if err != nil {
		return err
	}
	fmt.Println("custom plan for 'refunded':")
	fmt.Println(pg.Indent(plan))
	return nil
}

func printPlanCounters(ctx context.Context, q pg.Querier) error {
	rows, err := q.Query(ctx, `SELECT name, generic_plans, custom_plans FROM pg_prepared_statements
		WHERE statement LIKE '%status = $1%' AND statement NOT LIKE '%pg_prepared_statements%'`)
	if err != nil {
		return err
	}
	type counters struct {
		Name         string
		GenericPlans int64
		CustomPlans  int64
	}
	stmts, err := pgx.CollectRows(rows, pgx.RowToStructByPos[counters])
	if err != nil {
		return err
	}
	if len(stmts) == 0 {
		fmt.Println("pg_prepared_statements: none (the driver did not prepare a named statement)")
	}
	for _, s := range stmts {
		fmt.Printf("pg_prepared_statements: %s generic_plans=%d custom_plans=%d\n", s.Name, s.GenericPlans, s.CustomPlans)
	}
	return nil
}
