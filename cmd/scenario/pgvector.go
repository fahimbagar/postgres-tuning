package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/fahimbagar/postgres-tuning/internal/bench"
	"github.com/fahimbagar/postgres-tuning/internal/pg"
	"github.com/jackc/pgx/v5"
)

func init() {
	register("pgvector", "extension: pgvector HNSW recall vs ef_search, and filtered queries", pgvector)
}

// Vectors go over the wire as text ('[0.1,0.2,...]') and are cast on the
// server, so no pgvector Go package is needed.
const (
	knn         = `SELECT id FROM items ORDER BY embedding <-> $1::text::vector LIMIT 10`
	knnFiltered = `SELECT id FROM items WHERE category = $2 ORDER BY embedding <-> $1::text::vector LIMIT 10`
)

func pgvector(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pgvector", flag.ExitOnError)
	nrows := fs.Int("rows", 100_000, "vectors in the table")
	dim := fs.Int("dim", 128, "vector dimensions")
	queries := fs.Int("queries", 50, "query vectors per measurement")
	fs.Parse(args)

	conn, err := pgx.Connect(ctx, "")
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	var version string
	if err := pg.ExecAll(ctx, conn, `CREATE EXTENSION IF NOT EXISTS vector`); err != nil {
		return err
	}
	if err := conn.QueryRow(ctx, `SELECT extversion FROM pg_extension WHERE extname = 'vector'`).Scan(&version); err != nil {
		return err
	}

	fmt.Printf("pgvector %s; creating items: %d %d-d vectors around 100 cluster centres, 50 categories\n", version, *nrows, *dim)
	// Uniform random vectors have no structure for HNSW to navigate and give
	// misleadingly low recall. Real embeddings cluster, so each row is a
	// random centre plus noise. The WHERE c >= 0 / g > 0 conditions make the
	// subqueries correlated, so they run once per row instead of once.
	if err := pg.ExecAll(ctx, conn,
		`DROP TABLE IF EXISTS items`,
		fmt.Sprintf(`CREATE TABLE items (id int PRIMARY KEY, category int NOT NULL, embedding vector(%d) NOT NULL)`, *dim),
		fmt.Sprintf(`CREATE TEMP TABLE centres AS
			SELECT c, (SELECT array_agg(random()) FROM generate_series(1, %d) WHERE c >= 0) AS v
			FROM generate_series(0, 99) c`, *dim),
		fmt.Sprintf(`INSERT INTO items
			SELECT s.g, s.g %% 50,
			       (SELECT array_agg(ce.v[d] + (random() - 0.5) * 0.2 ORDER BY d) FROM generate_series(1, %d) d)::vector
			FROM (SELECT g, floor(random() * 100)::int AS c FROM generate_series(1, %d) g) s
			JOIN centres ce ON ce.c = s.c`, *dim, *nrows),
		`VACUUM ANALYZE items`,
	); err != nil {
		return err
	}

	// A parallel HNSW build puts the graph in a dynamic shared memory
	// segment sized by maintenance_work_mem. In a pod that segment lives in
	// /dev/shm (256Mi here), so 512MB fails with "could not resize shared
	// memory segment ... No space left on device".
	start := time.Now()
	if err := pg.ExecAll(ctx, conn,
		`SET maintenance_work_mem = '192MB'`,
		`CREATE INDEX items_embedding_idx ON items USING hnsw (embedding vector_l2_ops)`,
	); err != nil {
		return err
	}
	size, err := pg.RelationSize(ctx, conn, "items_embedding_idx")
	if err != nil {
		return err
	}
	fmt.Printf("HNSW index (m=16, ef_construction=64) built in %s, %s\n", time.Since(start).Round(time.Second), size)

	// Queries are existing items plus a little noise: "more like this one".
	rows, err := conn.Query(ctx, `
		SELECT (SELECT array_agg(x + (random() - 0.5) * 0.05) FROM unnest(embedding::real[]) x)::vector::text,
		       category
		FROM items ORDER BY random() LIMIT $1`, *queries)
	if err != nil {
		return err
	}
	type query struct {
		Vec string
		Cat int
	}
	picked, err := pgx.CollectRows(rows, pgx.RowToStructByPos[query])
	if err != nil {
		return err
	}
	qs := make([]string, len(picked))
	cats := make([]int, len(picked))
	for i, p := range picked {
		qs[i] = p.Vec
		// A different category from the item's own, so the filter really
		// has to look past the nearest neighbours.
		cats[i] = (p.Cat + 1 + i%49) % 50
	}

	// Exact answers: with index scans off the planner sorts every row.
	exact, err := runKNN(ctx, conn, qs, nil, "SET LOCAL enable_indexscan = off")
	if err != nil {
		return err
	}
	exactFiltered, err := runKNN(ctx, conn, qs, cats, "SET LOCAL enable_indexscan = off")
	if err != nil {
		return err
	}

	pg.Section("unfiltered top-10: recall vs hnsw.ef_search")
	fmt.Printf("    exact (seq scan): p50=%s\n", bench.Round(exact.res.P50))
	for _, ef := range []int{10, 40, 100, 200} {
		got, err := runKNN(ctx, conn, qs, nil, fmt.Sprintf("SET LOCAL hnsw.ef_search = %d", ef))
		if err != nil {
			return err
		}
		fmt.Printf("    ef_search=%-3d recall@10=%.3f p50=%s\n", ef, recall(exact.ids, got.ids), bench.Round(got.res.P50))
	}

	pg.Section("filtered top-10 (WHERE category = $2, 2% of rows), ef_search = 40")
	for _, mode := range []string{"off", "relaxed_order"} {
		got, err := runKNN(ctx, conn, qs, cats, "SET LOCAL hnsw.iterative_scan = "+mode)
		if err != nil {
			return err
		}
		returned := 0
		for _, ids := range got.ids {
			returned += len(ids)
		}
		fmt.Printf("    iterative_scan=%-13s rows returned per query=%.1f recall@10=%.3f p50=%s\n",
			mode, float64(returned)/float64(len(qs)), recall(exactFiltered.ids, got.ids), bench.Round(got.res.P50))
	}
	fmt.Printf("    exact (seq scan):            p50=%s\n", bench.Round(exactFiltered.res.P50))
	return nil
}

type knnResult struct {
	ids [][]int32
	res bench.Result
}

// runKNN runs one query per vector, each in a transaction that first applies
// setup (SET LOCAL ...). cats, if not nil, switches to the filtered query.
func runKNN(ctx context.Context, conn *pgx.Conn, qs []string, cats []int, setup string) (knnResult, error) {
	out := knnResult{ids: make([][]int32, len(qs))}
	res, err := bench.Repeat(ctx, len(qs), func(ctx context.Context, i int) error {
		return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, setup); err != nil {
				return err
			}
			var rows pgx.Rows
			var err error
			if cats == nil {
				rows, err = tx.Query(ctx, knn, qs[i])
			} else {
				rows, err = tx.Query(ctx, knnFiltered, qs[i], cats[i])
			}
			if err != nil {
				return err
			}
			out.ids[i], err = pgx.CollectRows(rows, pgx.RowTo[int32])
			return err
		})
	})
	out.res = res
	return out, err
}

// recall is the share of the exact top-k ids that the approximate search
// also returned, averaged over queries.
func recall(exact, got [][]int32) float64 {
	var sum float64
	for i := range exact {
		if len(exact[i]) == 0 {
			continue
		}
		want := map[int32]bool{}
		for _, id := range exact[i] {
			want[id] = true
		}
		hit := 0
		for _, id := range got[i] {
			if want[id] {
				hit++
			}
		}
		sum += float64(hit) / float64(len(exact[i]))
	}
	return sum / float64(len(exact))
}
