# postgres-tuning

[![static analysis](https://github.com/fahimbagar/postgres-tuning/actions/workflows/static-analysis.yml/badge.svg)](https://github.com/fahimbagar/postgres-tuning/actions/workflows/static-analysis.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Reproducible Postgres 18 tuning demos in Go. Each scenario sets up its own
tables, measures a common mistake, applies the fix, and measures again.

Everything runs in a local k3d cluster. Postgres 18 is capped at 2 CPU and
2 GiB, and the Go load generator runs as a Job inside the cluster so its
latency numbers don't include `kubectl port-forward`.

## Requirements

- Docker, [k3d](https://k3d.io), kubectl
- Go 1.25+
- About 15 GB of free disk. The `aio` table alone is ~3 GB.

## Run

```sh
make cluster-up        # k3d cluster "pgtune"; your current kubectl context is left alone
make build deploy      # build the image (Postgres 18 + hypopg, pg_repack, pgvector, pgbouncer), import it, deploy
make run S=seed        # 1M customers, 10M orders (~5 minutes)

make run S=genericplan # any scenario; output also goes to results/<name>.txt
make aio               # restarts Postgres once per io_method
make prewarm           # restarts Postgres between phases

make cluster-down
```

`make aio` and `make prewarm` drop the OS page cache between restarts
(`echo 1 > /proc/sys/vm/drop_caches` inside the privileged k3d node). That
affects the whole host. Clean cached pages are discarded and nothing is lost,
but other programs will read from disk again for a while.

`make run S=workmem` deliberately gets the Postgres container OOM-killed. It
puts `work_mem` back afterwards.

## Scenarios

| Name | What it shows |
| :--- | :--- |
| `measure` | pg_stat_statements: which queries really use the time |
| `workmem` | spills at 4MB, `SET LOCAL` vs server-wide `work_mem`, OOM kill |
| `pooling` | 300 direct connections vs a pool of 8 vs PgBouncer |
| `idletx` | a Go transaction leak blocks vacuum; the timeout that limits it |
| `autovacuum` | default scale factor on a 5M-row table; per-table override |
| `randompagecost` | `random_page_cost` 4 vs 1.1 on a mostly cached data set |
| `stats` | correlated columns, `CREATE STATISTICS` |
| `genericplan` | pgx prepared statements switching to a generic plan |
| `fkindex` | deleting parent rows when the FK column has no index |
| `unusedindex` | insert throughput and WAL with 7 extra indexes |
| `jit` | JIT compile time vs query time |
| `aio` | PG 18 `io_method` sync / worker / io_uring |
| `autoexplain` | auto_explain capturing the plan a slow call actually used |
| `prewarm` | pg_buffercache and pg_prewarm after a restart |
| `repack` | pgstattuple bloat numbers, VACUUM FULL vs pg_repack under writes |
| `hypopg` | hypothetical indexes, and where the estimate goes wrong |
| `pgvector` | HNSW recall vs `ef_search`, filtered queries with iterative scans |

`results/` holds the output of one full run on a 4-core laptop, with the
Postgres pod capped at 2 CPU and 2 GiB. Your numbers will differ. `make run`
overwrites these files, so `git diff results/` shows how a rerun compares.
