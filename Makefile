CLUSTER := pgtune
KUBECTL := kubectl --context k3d-$(CLUSTER)
IMAGE   := pgtune:dev
S       ?= measure
ARGS    ?=

.PHONY: cluster-up cluster-down build deploy psql run restart-postgres restart-postgres-cold aio prewarm

# --kubeconfig-switch-context=false leaves your current kubectl context alone.
cluster-up:
	k3d cluster create $(CLUSTER) --servers 1 --agents 0 --no-lb \
		--k3s-arg "--disable=traefik@server:0" \
		--k3s-arg "--disable=metrics-server@server:0" \
		--kubeconfig-switch-context=false --wait

cluster-down:
	k3d cluster delete $(CLUSTER)

build:
	CGO_ENABLED=0 go build -o bin/scenario ./cmd/scenario
	docker build -t $(IMAGE) -f docker/Dockerfile .
	k3d image import $(IMAGE) -c $(CLUSTER)

deploy:
	$(KUBECTL) apply -f k8s/postgres.yaml -f k8s/pgbouncer.yaml
	$(KUBECTL) rollout status statefulset/postgres --timeout=180s
	$(KUBECTL) rollout status deployment/pgbouncer --timeout=60s

psql:
	$(KUBECTL) exec -it postgres-0 -- psql -U postgres

# make run S=workmem ARGS="--concurrency 16"
# Streams the output, then saves the complete log to results/$(S).txt.
# `kubectl logs -f` can drop a quiet stream, so completion is checked by
# polling the Job status rather than by the stream ending.
run:
	@mkdir -p results
	-@$(KUBECTL) delete job scenario-$(S) --ignore-not-found --wait >/dev/null
	@sed -e 's|__NAME__|scenario-$(S)|' -e 's|__ARGS__|$(S) $(ARGS)|' k8s/job.yaml | $(KUBECTL) apply -f - >/dev/null
	@until $(KUBECTL) logs -f job/scenario-$(S) 2>/dev/null; do sleep 1; done
	@while :; do \
		st=$$($(KUBECTL) get job scenario-$(S) -o jsonpath='{.status.succeeded}/{.status.failed}'); \
		case "$$st" in 1/*) break;; */1) $(KUBECTL) logs job/scenario-$(S) > results/$(S).txt; echo "scenario $(S) failed"; exit 1;; esac; \
		sleep 2; \
	done
	@$(KUBECTL) logs job/scenario-$(S) > results/$(S).txt
	@echo "saved results/$(S).txt"

# Restart Postgres with an empty shared_buffers. Used by the scenarios whose
# settings only apply at server start.
restart-postgres:
	$(KUBECTL) delete pod postgres-0 --wait
	$(KUBECTL) rollout status statefulset/postgres --timeout=180s
	$(KUBECTL) wait --for=condition=ready pod/postgres-0 --timeout=180s

# Same, but with the OS page cache dropped while Postgres is down, so the
# next reads really come from disk. drop_caches is host-wide: the k3d node is
# a privileged container sharing the host kernel. It only discards clean
# cached pages, nothing is lost, but every program on the machine will
# re-read its files from disk once.
restart-postgres-cold:
	$(KUBECTL) scale statefulset/postgres --replicas=0
	$(KUBECTL) wait --for=delete pod/postgres-0 --timeout=180s
	docker exec k3d-$(CLUSTER)-server-0 sh -c 'sync && echo 1 > /proc/sys/vm/drop_caches'
	$(KUBECTL) scale statefulset/postgres --replicas=1
	$(KUBECTL) rollout status statefulset/postgres --timeout=180s
	$(KUBECTL) wait --for=condition=ready pod/postgres-0 --timeout=180s

PSQL := $(KUBECTL) exec postgres-0 -- psql -U postgres -qc

# io_method can only change at server start, so this restarts once per value.
aio:
	@mkdir -p results
	@rm -f results/aio-all.txt
	@for m in sync worker io_uring; do \
		$(PSQL) "ALTER SYSTEM SET io_method = '$$m'" && \
		$(PSQL) "ALTER SYSTEM SET pg_prewarm.autoprewarm = off" && \
		$(MAKE) --no-print-directory restart-postgres-cold >/dev/null && \
		$(MAKE) --no-print-directory run S=aio ARGS="$(ARGS)" && \
		cat results/aio.txt >> results/aio-all.txt || exit 1; \
	done
	@mv results/aio-all.txt results/aio.txt
	$(PSQL) "ALTER SYSTEM RESET io_method"
	$(PSQL) "ALTER SYSTEM RESET pg_prewarm.autoprewarm"
	$(MAKE) --no-print-directory restart-postgres >/dev/null

# Three phases; see cmd/scenario/prewarm.go.
prewarm:
	@mkdir -p results
	$(PSQL) "ALTER SYSTEM SET pg_prewarm.autoprewarm = off"
	$(MAKE) --no-print-directory restart-postgres-cold >/dev/null
	$(MAKE) --no-print-directory run S=prewarm ARGS="--phase manual"
	@cp results/prewarm.txt results/prewarm-all.txt
	$(PSQL) "ALTER SYSTEM SET pg_prewarm.autoprewarm = on"
	$(MAKE) --no-print-directory restart-postgres >/dev/null
	$(MAKE) --no-print-directory run S=prewarm ARGS="--phase dump"
	@cat results/prewarm.txt >> results/prewarm-all.txt
	$(MAKE) --no-print-directory restart-postgres-cold >/dev/null
	$(MAKE) --no-print-directory run S=prewarm ARGS="--phase auto"
	@cat results/prewarm.txt >> results/prewarm-all.txt
	@mv results/prewarm-all.txt results/prewarm.txt
	$(PSQL) "ALTER SYSTEM RESET pg_prewarm.autoprewarm"
