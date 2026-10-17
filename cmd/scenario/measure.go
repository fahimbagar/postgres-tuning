package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/fahimbagar/postgres-tuning/internal/bench"
	"github.com/fahimbagar/postgres-tuning/internal/pg"
)

func init() {
	register("measure", "mistake: tuning before measuring (pg_stat_statements baseline)", measure)
}

// A rough stand-in for a shop backend: many cheap lookups, some listing
// pages, and a report that a cron job runs every few seconds.
var oltpMix = []struct {
	weight int
	sql    string
	args   func(r *rand.Rand) []any
}{
	{70, `SELECT id, email, tier FROM customers WHERE id = $1`,
		func(r *rand.Rand) []any { return []any{1 + r.Int64N(1_000_000)} }},
	{25, `SELECT id, status, total, created_at FROM orders WHERE customer_id = $1 ORDER BY created_at DESC LIMIT 10`,
		func(r *rand.Rand) []any { return []any{1 + r.Int64N(1_000_000)} }},
	{5, `SELECT id, customer_id, total FROM orders WHERE status = $1 ORDER BY created_at DESC LIMIT 50`,
		func(r *rand.Rand) []any { return []any{[]string{"pending", "shipped", "cancelled"}[r.IntN(3)]} }},
}

const vipReport = `SELECT c.tier, count(*), sum(o.total)
FROM customers c JOIN orders o ON o.customer_id = c.id
WHERE c.tier = 'vip' GROUP BY c.tier`

func measure(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("measure", flag.ContinueOnError)
	workers := fs.Int("workers", 8, "concurrent OLTP sessions")
	dur := fs.Duration("duration", 60*time.Second, "how long to run the workload")
	reportEvery := fs.Duration("report-every", 5*time.Second, "how often the report query runs")
	if err := fs.Parse(args); err != nil {
		return err
	}

	pool, err := pg.Pool(ctx, nil)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := pg.ExecAll(ctx, pool,
		`CREATE EXTENSION IF NOT EXISTS pg_stat_statements`,
		`SELECT pg_stat_statements_reset()`,
	); err != nil {
		return err
	}

	total := 0
	for _, q := range oltpMix {
		total += q.weight
	}

	pg.Section(fmt.Sprintf("workload: %d OLTP sessions + 1 report every %s, %s", *workers, *reportEvery, *dur))
	reportCtx, stopReports := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() {
		t := time.NewTicker(*reportEvery)
		defer t.Stop()
		for {
			if _, err := pool.Exec(reportCtx, vipReport); err != nil && reportCtx.Err() == nil {
				fmt.Println("report failed:", err)
			}
			select {
			case <-reportCtx.Done():
				return
			case <-t.C:
			}
		}
	})

	rngs := make([]*rand.Rand, *workers)
	for i := range rngs {
		rngs[i] = rand.New(rand.NewPCG(uint64(i), uint64(time.Now().UnixNano())))
	}
	res := bench.Run(ctx, *workers, 5*time.Second, *dur, func(ctx context.Context, w int) error {
		r := rngs[w]
		n := r.IntN(total)
		for _, q := range oltpMix {
			if n < q.weight {
				rows, err := pool.Query(ctx, q.sql, q.args(r)...)
				if err != nil {
					return err
				}
				rows.Close()
				return rows.Err()
			}
			n -= q.weight
		}
		return nil
	})
	stopReports()
	wg.Wait()
	fmt.Println("OLTP mix:", res)

	pg.Section("top statements by total_exec_time")
	if err := printTopStatements(ctx, pool, "total_exec_time", 5); err != nil {
		return err
	}
	pg.Section("top statements by calls")
	return printTopStatements(ctx, pool, "calls", 5)
}

// printTopStatements prints the heaviest statements from pg_stat_statements.
// orderBy is a trusted column name, never user input.
func printTopStatements(ctx context.Context, q pg.Querier, orderBy string, limit int) error {
	rows, err := q.Query(ctx, `
		SELECT calls,
		       round(total_exec_time)::bigint,
		       round((100 * total_exec_time / sum(total_exec_time) OVER ())::numeric, 1),
		       round(mean_exec_time::numeric, 3),
		       round(rows::numeric / calls, 1),
		       round(100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0), 1),
		       query
		FROM pg_stat_statements
		WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
		  AND query NOT ILIKE '%pg_stat_statements%'
		ORDER BY `+orderBy+` DESC
		LIMIT $1`, limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	fmt.Printf("%9s %10s %6s %10s %8s %6s  %s\n", "calls", "total_ms", "%time", "mean_ms", "rows", "hit%", "query")
	for rows.Next() {
		var (
			calls, totalMs int64
			share, mean    float64
			rowsPerCall    float64
			hit            *float64
			query          string
		)
		if err := rows.Scan(&calls, &totalMs, &share, &mean, &rowsPerCall, &hit, &query); err != nil {
			return err
		}
		hitStr := "-"
		if hit != nil {
			hitStr = fmt.Sprintf("%.1f", *hit)
		}
		fmt.Printf("%9d %10d %6.1f %10.3f %8.1f %6s  %s\n", calls, totalMs, share, mean, rowsPerCall, hitStr, oneLine(query, 90))
	}
	return rows.Err()
}

func oneLine(s string, limit int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > limit {
		return s[:limit-3] + "..."
	}
	return s
}
