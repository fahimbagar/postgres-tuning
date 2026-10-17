# One image for the database, pgbouncer, and the scenario Job. The Job shells
# out to pg_repack, which must match the server-side extension version, so
# shipping both from the same image keeps them in step.
FROM postgres:18

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      postgresql-18-hypopg \
      postgresql-18-repack \
      postgresql-18-pgvector \
      pgbouncer \
 && rm -rf /var/lib/apt/lists/*

COPY bin/scenario /usr/local/bin/scenario
