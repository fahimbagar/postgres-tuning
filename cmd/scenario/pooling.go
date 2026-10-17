package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fahimbagar/postgres-tuning/internal/bench"
	"github.com/fahimbagar/postgres-tuning/internal/pg"
)

func init() {
	register("pooling", "mistake: hundreds of direct connections, no pooler", pooling)
}

func pooling(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pooling", flag.ContinueOnError)
	clients := fs.Int("clients", 300, "concurrent goroutines issuing queries")
	small := fs.Int("small-pool", 8, "pool size for the sized-pool run")
	dur := fs.Duration("duration", 30*time.Second, "length of each run")
	if err := fs.Parse(args); err != nil {
		return err
	}
	bigPool, err := pg.MaxConns(*clients)
	if err != nil {
		return err
	}
	smallPool, err := pg.MaxConns(*small)
	if err != nil {
		return err
	}

	runs := []struct {
		name  string
		tweak func(*pgxpool.Config)
	}{
		{fmt.Sprintf("direct, pgxpool MaxConns=%d (one connection per client)", *clients), func(c *pgxpool.Config) {
			c.MaxConns = bigPool
		}},
		{fmt.Sprintf("direct, pgxpool MaxConns=%d", *small), func(c *pgxpool.Config) {
			c.MaxConns = smallPool
		}},
		{fmt.Sprintf("pgbouncer (transaction mode, 16 server connections), pgxpool MaxConns=%d", *clients), func(c *pgxpool.Config) {
			c.MaxConns = bigPool
			c.ConnConfig.Host, c.ConnConfig.Port = "pgbouncer", 6432
			// With libpq's default sslmode=prefer, pgx adds a non-TLS
			// fallback that keeps the host from PGHOST. PgBouncer here has no
			// TLS, so the first attempt fails and the fallback quietly
			// connects straight to Postgres. Drop both.
			c.ConnConfig.TLSConfig, c.ConnConfig.Fallbacks = nil, nil
		}},
	}

	monitor, err := pgx.Connect(ctx, "")
	if err != nil {
		return err
	}
	defer monitor.Close(context.Background())

	for _, r := range runs {
		pg.Section(r.name)
		if err := poolingRun(ctx, monitor, r.tweak, *clients, *dur); err != nil {
			return err
		}
		// Let the closed connections' backends exit before the next run
		// reads memory.
		time.Sleep(3 * time.Second)
	}
	return nil
}

func poolingRun(ctx context.Context, monitor *pgx.Conn, tweak func(*pgxpool.Config), clients int, d time.Duration) error {
	pool, err := pg.Pool(ctx, tweak)
	if err != nil {
		return err
	}
	defer pool.Close()

	startAnon, err := readAnon(ctx, monitor)
	if err != nil {
		return err
	}
	sampler := startMemSampler(ctx, monitor, 500*time.Millisecond)

	// Halfway through, count the backends Postgres is actually running.
	// This needs its own connection: the sampler is using the monitor one
	// and a pgx.Conn is not safe for concurrent use.
	type activity struct{ total, active int64 }
	seen := make(chan activity, 1)
	go func() {
		var a activity
		defer func() { seen <- a }()
		select {
		case <-ctx.Done():
			return
		case <-time.After(5*time.Second + d/2):
		}
		conn, err := pgx.Connect(ctx, "")
		if err != nil {
			fmt.Println("pg_stat_activity:", err)
			return
		}
		defer func() { _ = conn.Close(ctx) }()
		err = conn.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE state = 'active')
			FROM pg_stat_activity WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()`).
			Scan(&a.total, &a.active)
		if err != nil {
			fmt.Println("pg_stat_activity:", err)
		}
	}()

	total := 0
	for _, q := range oltpMix {
		total += q.weight
	}
	rngs := make([]*rand.Rand, clients)
	for i := range rngs {
		rngs[i] = rand.New(rand.NewPCG(uint64(i), 42))
	}
	res := bench.Run(ctx, clients, 5*time.Second, d, func(ctx context.Context, w int) error {
		r := rngs[w]
		n := r.IntN(total)
		for _, q := range oltpMix {
			if n < q.weight {
				_, err := pool.Exec(ctx, q.sql, q.args(r)...)
				return err
			}
			n -= q.weight
		}
		return nil
	})
	peak := sampler.Stop()
	a := <-seen

	fmt.Println(res)
	// The memory sampler's session is included in the count.
	fmt.Printf("client backends on the server: %d (%d active at that moment)\n", a.total, a.active)
	fmt.Printf("backend memory (cgroup anon): %s before, %s peak\n", mib(startAnon), mib(peak))
	return nil
}
