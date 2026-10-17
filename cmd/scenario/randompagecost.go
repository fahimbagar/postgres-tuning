package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/fahimbagar/postgres-tuning/internal/bench"
	"github.com/fahimbagar/postgres-tuning/internal/pg"
)

func init() {
	register("randompagecost", "mistake: random_page_cost = 4 when the data is cached / on SSD", randomPageCost)
}

// Revenue by tier for three cities: ~30k customers, ~300k of the 10M orders.
const regionalReport = `SELECT c.tier, count(*), sum(o.total)
FROM customers c JOIN orders o ON o.customer_id = c.id
WHERE c.city_id IN (7, 8, 9) GROUP BY c.tier`

func randomPageCost(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("randompagecost", flag.ContinueOnError)
	runs := fs.Int("runs", 5, "runs per setting; settings alternate so cache warmth is shared")
	if err := fs.Parse(args); err != nil {
		return err
	}

	conn, err := pgx.Connect(ctx, "")
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	values := []string{"4", "1.1"}
	for _, v := range values {
		pg.Section("random_page_cost = " + v)
		if _, err := conn.Exec(ctx, "SET random_page_cost = "+v); err != nil {
			return err
		}
		plan, err := pg.Explain(ctx, conn, "", regionalReport)
		if err != nil {
			return err
		}
		fmt.Println(pg.Indent(plan))
	}

	// One warm-up run each, then alternate so neither setting gets an
	// unfairly warm or cold cache.
	lat := map[string][]time.Duration{}
	for i := range *runs + 1 {
		for _, v := range values {
			if _, err := conn.Exec(ctx, "SET random_page_cost = "+v); err != nil {
				return err
			}
			start := time.Now()
			if _, err := conn.Exec(ctx, regionalReport); err != nil {
				return err
			}
			if i > 0 {
				lat[v] = append(lat[v], time.Since(start))
			}
		}
	}
	pg.Section(fmt.Sprintf("regional report, %d warm runs each", *runs))
	for _, v := range values {
		fmt.Printf("random_page_cost=%-4s %s\n", v, durations(lat[v]))
	}
	return nil
}

// durations prints the median and the individual samples, rounded to ms.
func durations(d []time.Duration) string {
	parts := make([]string, len(d))
	for i, x := range d {
		parts[i] = x.Round(time.Millisecond).String()
	}
	return fmt.Sprintf("median=%s  runs=[%s]", bench.Median(d).Round(time.Millisecond), strings.Join(parts, " "))
}
