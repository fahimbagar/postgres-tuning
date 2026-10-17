package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/fahimbagar/postgres-tuning/internal/pg"
	"github.com/jackc/pgx/v5"
)

func init() {
	register("jit", "mistake: JIT compiling queries that finish in under a second", jit)
}

func jit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("jit", flag.ExitOnError)
	runs := fs.Int("runs", 7, "runs per setting, alternating")
	fs.Parse(args)

	conn, err := pgx.Connect(ctx, "")
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	pg.Section("plan cost vs JIT thresholds")
	var above, inline, optimize string
	if err := conn.QueryRow(ctx, `SELECT current_setting('jit_above_cost'),
		current_setting('jit_inline_above_cost'), current_setting('jit_optimize_above_cost')`).
		Scan(&above, &inline, &optimize); err != nil {
		return err
	}
	// pgx decodes the json column straight into the struct.
	var plans []struct {
		Plan struct {
			TotalCost float64 `json:"Total Cost"`
		}
	}
	if err := conn.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+vipReport).Scan(&plans); err != nil {
		return err
	}
	fmt.Printf("VIP report total cost %.0f; jit_above_cost=%s jit_inline_above_cost=%s jit_optimize_above_cost=%s\n",
		plans[0].Plan.TotalCost, above, inline, optimize)

	pg.Section("JIT section of EXPLAIN (ANALYZE, VERBOSE), leader and each parallel worker")
	plan, err := pg.Explain(ctx, conn, "ANALYZE, VERBOSE", vipReport)
	if err != nil {
		return err
	}
	inJIT := false
	for line := range strings.Lines(plan) {
		t := strings.TrimSpace(line)
		switch {
		case t == "JIT:" || strings.HasPrefix(t, "Worker ") && strings.Contains(t, "JIT"):
			inJIT = true
		case strings.HasPrefix(t, "Execution Time"):
			fmt.Println(pg.Indent(t))
			inJIT = false
		}
		if inJIT && (strings.Contains(t, "JIT") || strings.HasPrefix(t, "Functions") ||
			strings.HasPrefix(t, "Options") || strings.HasPrefix(t, "Timing")) {
			fmt.Println(pg.Indent(t))
		}
	}

	lat := map[string][]time.Duration{}
	settings := []string{"on", "off"}
	for i := range *runs + 1 {
		for _, s := range settings {
			if _, err := conn.Exec(ctx, "SET jit = "+s); err != nil {
				return err
			}
			start := time.Now()
			if _, err := conn.Exec(ctx, vipReport); err != nil {
				return err
			}
			if i > 0 {
				lat[s] = append(lat[s], time.Since(start))
			}
		}
	}
	pg.Section(fmt.Sprintf("VIP report, %d runs each", *runs))
	for _, s := range settings {
		fmt.Printf("jit=%-3s %s\n", s, durations(lat[s]))
	}
	return nil
}
