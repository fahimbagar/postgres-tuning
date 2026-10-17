package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/fahimbagar/postgres-tuning/internal/bench"
	"github.com/fahimbagar/postgres-tuning/internal/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func init() {
	register("idletx", "mistake: transactions left open (idle in transaction)", idleTx)
}

var errOutOfStock = errors.New("out of stock")

// reserveLeaky has the bug: on the out-of-stock path it returns without
// Commit or Rollback. The transaction stays open on the server and the
// pool never gets the connection back.
func reserveLeaky(ctx context.Context, pool *pgxpool.Pool, sku int) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	var qty int
	if err := tx.QueryRow(ctx, `SELECT qty FROM it_stock WHERE sku = $1 FOR UPDATE`, sku).Scan(&qty); err != nil {
		return err
	}
	if qty == 0 {
		return errOutOfStock
	}
	if _, err := tx.Exec(ctx, `UPDATE it_stock SET qty = qty - 1 WHERE sku = $1`, sku); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// reserve is the fix. Rollback after a successful Commit is a no-op, so the
// deferred call covers every early return.
func reserve(ctx context.Context, pool *pgxpool.Pool, sku int) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var qty int
	if err := tx.QueryRow(ctx, `SELECT qty FROM it_stock WHERE sku = $1 FOR UPDATE`, sku).Scan(&qty); err != nil {
		return err
	}
	if qty == 0 {
		return errOutOfStock
	}
	if _, err := tx.Exec(ctx, `UPDATE it_stock SET qty = qty - 1 WHERE sku = $1`, sku); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func idleTx(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("idletx", flag.ExitOnError)
	dur := fs.Duration("duration", 45*time.Second, "churn time per phase")
	fs.Parse(args)

	phases := []struct {
		name    string
		leak    bool
		timeout string // idle_in_transaction_session_timeout for the app pool
	}{
		{"healthy: reserve() with defer tx.Rollback", false, ""},
		{"leak: reserveLeaky() hits the out-of-stock path once", true, ""},
		{"leak + idle_in_transaction_session_timeout = 5s", true, "5s"},
	}
	for _, p := range phases {
		pg.Section(p.name)
		if err := idleTxPhase(ctx, p.leak, p.timeout, *dur); err != nil {
			return err
		}
	}
	return nil
}

func idleTxPhase(ctx context.Context, leak bool, timeout string, d time.Duration) error {
	admin, err := pgx.Connect(ctx, "")
	if err != nil {
		return err
	}
	defer admin.Close(context.Background())

	if err := pg.ExecAll(ctx, admin,
		`DROP TABLE IF EXISTS it_stock`,
		`CREATE TABLE it_stock (sku int PRIMARY KEY, qty int NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())`,
		`INSERT INTO it_stock SELECT g, CASE WHEN g = 1 THEN 0 ELSE 100 END FROM generate_series(1, 10000) g`,
		`VACUUM ANALYZE it_stock`,
	); err != nil {
		return err
	}

	// The "app": a small pool tagged with an application_name so the leaked
	// session is easy to find in pg_stat_activity. It is not closed:
	// pool.Close waits for every acquired connection, and the leaked one is
	// never released. That hang is part of the bug.
	app, err := pg.Pool(ctx, func(c *pgxpool.Config) {
		c.MaxConns = 4
		c.ConnConfig.RuntimeParams["application_name"] = "shop-api"
		if timeout != "" {
			c.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = timeout
		}
	})
	if err != nil {
		return err
	}
	if !leak {
		defer app.Close()
	}

	if leak {
		err := reserveLeaky(ctx, app, 1)
		fmt.Printf("reserveLeaky(sku=1): %v; pool connections still acquired afterwards: %d of %d\n",
			err, app.Stat().AcquiredConns(), app.Stat().MaxConns())
	} else {
		err := reserve(ctx, app, 1)
		fmt.Printf("reserve(sku=1): %v; pool connections still acquired afterwards: %d of %d\n",
			err, app.Stat().AcquiredConns(), app.Stat().MaxConns())
	}

	// A stock-sync job updates random SKUs as fast as it can.
	sync, err := pg.Pool(ctx, func(c *pgxpool.Config) { c.MaxConns = 4 })
	if err != nil {
		return err
	}
	defer sync.Close()
	rngs := make([]*rand.Rand, 4)
	for i := range rngs {
		rngs[i] = rand.New(rand.NewPCG(uint64(i), 7))
	}
	first := bench.Run(ctx, 4, 0, d/3, func(ctx context.Context, w int) error {
		_, err := sync.Exec(ctx, `UPDATE it_stock SET qty = qty + 1, updated_at = now() WHERE sku = $1`, 2+rngs[w].IntN(9999))
		return err
	})
	rest := bench.Run(ctx, 4, 0, d-d/3, func(ctx context.Context, w int) error {
		_, err := sync.Exec(ctx, `UPDATE it_stock SET qty = qty + 1, updated_at = now() WHERE sku = $1`, 2+rngs[w].IntN(9999))
		return err
	})
	fmt.Printf("stock sync, first %s: %s\n", d/3, first)
	fmt.Printf("stock sync, last  %s: %s\n", d-d/3, rest)

	var idle []string
	rows, err := admin.Query(ctx, `SELECT format('pid=%s state=%s xact_age=%s query=%s',
			pid, state, date_trunc('second', now() - xact_start), left(query, 60))
		FROM pg_stat_activity WHERE application_name = 'shop-api' AND state LIKE 'idle in transaction%'`)
	if err != nil {
		return err
	}
	if idle, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
		return err
	}
	fmt.Printf("idle-in-transaction sessions from shop-api: %d\n", len(idle))
	for _, s := range idle {
		fmt.Println(pg.Indent(s))
	}

	if err := vacuumReport(ctx, "it_stock"); err != nil {
		return err
	}
	size, err := pg.RelationSize(ctx, admin, "it_stock")
	if err != nil {
		return err
	}
	fmt.Println("it_stock size (10,000 rows):", size)

	// Clean up the leaked session so the next phase starts fresh.
	_, err = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE application_name = 'shop-api' AND state LIKE 'idle in transaction%'`)
	return err
}

// vacuumReport runs VACUUM (VERBOSE) and prints the lines that say how many
// dead rows it could and couldn't remove. VERBOSE output arrives as notices.
func vacuumReport(ctx context.Context, table string) error {
	var notices []string
	cfg, err := pgx.ParseConfig("")
	if err != nil {
		return err
	}
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { notices = append(notices, n.Message) }
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, "VACUUM (VERBOSE) "+pgx.Identifier{table}.Sanitize()); err != nil {
		return err
	}
	fmt.Println("VACUUM (VERBOSE):")
	for _, n := range notices {
		for line := range strings.Lines(n) {
			t := strings.TrimSpace(line)
			if strings.HasPrefix(t, "tuples:") || strings.HasPrefix(t, "removable cutoff:") {
				fmt.Println(pg.Indent(t))
			}
		}
	}
	return nil
}
