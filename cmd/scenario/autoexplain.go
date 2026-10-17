package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/fahimbagar/postgres-tuning/internal/pg"
)

func init() {
	register("autoexplain", "extension: auto_explain catches the plan that actually ran", autoExplain)
}

// autoExplain reuses the generic-plan trap from the genericplan scenario.
// A manual EXPLAIN with a literal value shows the good custom plan, so it
// can't explain why the app is slow. auto_explain logs the plan the slow
// execution really used.
func autoExplain(ctx context.Context, _ []string) error {
	var notices []string
	cfg, err := pgx.ParseConfig("")
	if err != nil {
		return err
	}
	// auto_explain.log_level = notice sends the plan to the client as well
	// as (depending on log_min_messages) the server log. Handy for a demo or
	// a debugging session; in production you'd leave it at the default LOG.
	// pgx calls OnNotice synchronously while it reads the query's response.
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		notices = append(notices, n.Message)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if err := pg.ExecAll(ctx, conn,
		`LOAD 'auto_explain'`,
		`SET auto_explain.log_min_duration = '50ms'`,
		`SET auto_explain.log_analyze = on`,
		`SET auto_explain.log_buffers = on`,
		`SET auto_explain.log_timing = off`,
		`SET auto_explain.log_parameter_max_length = -1`,
		`SET auto_explain.log_level = notice`,
	); err != nil {
		return err
	}

	pg.Section("app traffic through pgx (prepared statement), then one slow call")
	for _, status := range []string{"delivered", "shipped", "delivered", "pending", "delivered", "delivered", "refunded"} {
		if _, err := conn.Exec(ctx, latestByStatus, status); err != nil {
			return err
		}
	}

	fmt.Printf("auto_explain notices: %d\n", len(notices))
	for _, n := range notices {
		for line := range strings.Lines(n) {
			fmt.Println(pg.Indent(strings.TrimRight(line, "\n")))
		}
	}
	return nil
}
