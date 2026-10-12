# Tick Lab. `make help` lists the targets.
#
# Two stacks share one Compose file under different project names, so they keep separate
# volumes: `ticklab` (live) and `ticklab-replay`. They use the same ports, so only one runs
# at a time. SIZE=lite (default) or SIZE=full picks the memory limits.

SHELL := /bin/bash
.SHELLFLAGS := -e -o pipefail -c
.DEFAULT_GOAL := help

SIZE ?= lite
UV := uv run --locked
UV_ENV := $(UV) --env-file .env
COMPOSE := docker compose --env-file .env --env-file config/sizing/$(SIZE).env
LIVE := $(COMPOSE) -p ticklab
REPLAY := TICKLAB_STACK=replay $(COMPOSE) -p ticklab-replay
FIXTURE ?= binance-2026-10-10T0400-15m
KRAKEN_FIXTURE ?= kraken-2026-10-11T0900-15m
SEED ?= 7
# The exchanges a replay or test stack carries; the Flink job reads only their partitions.
EXCHANGES ?= BINANCE
# The Flink jobs a replay stack runs; the analyses alone can replay weeks of history.
JOBS ?= core,signals,analyses
E2E := $(COMPOSE) -f compose.yaml -f compose.ci.yaml

.PHONY: help up down ps logs console replay-up replay-down replay replay-history \
	replay-kraken-history replay-drain record history kraken-history study-disorder study-gaps \
	study-signals study-lead-lag study-round-levels reconcile fixture e2e e2e-kraken chaos fail-taskmanager fail-producer \
	fail-network bench lint test test-java ci check-m1 check-m2 check-m3 check-m4 check-m5 check-m6 check-m7 check-m8 \
	codespace-network

JAR := flink/target/ticklab-flink.jar
JAVA_SOURCES := $(shell find flink/src/main schemas -type f) flink/pom.xml

help: ## List the targets
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

.env:
	cp .env.example .env

data/recordings data/history:
	mkdir -p $@

$(JAR): $(JAVA_SOURCES)
	cd flink && ./mvnw -B -q package -DskipTests

up: .env codespace-network $(JAR) | data/recordings ## Start the live stack, and the Flink job (resuming its state)
	@if [ -n "$$($(REPLAY) ps -q)" ]; then echo "The replay stack is running: make replay-down"; exit 1; fi
	$(LIVE) --profile live up -d --build --wait
	$(UV_ENV) python -m ticklab flink start

down: .env ## Stop the live stack; the Flink job first takes a savepoint (volumes are kept)
	-@if [ -n "$$($(LIVE) ps -q flink-jobmanager)" ]; then $(UV_ENV) python -m ticklab flink stop; fi
	$(LIVE) --profile live --profile replay --profile ci --profile console down

ps: .env ## Show the live stack's containers
	$(LIVE) --profile live --profile console ps

logs: .env ## Follow the live stack's logs
	$(LIVE) --profile live logs -f --tail 50

console: .env ## Add Redpanda Console to the live stack (http://localhost:8080)
	$(LIVE) --profile console up -d --wait console

replay-up: .env codespace-network $(JAR) ## Start an empty replay stack with fresh Flink jobs [JOBS=analyses]
	@if [ -n "$$($(LIVE) ps -q)" ]; then echo "The live stack is running: make down"; exit 1; fi
	$(REPLAY) --profile replay down -v
	rm -f data/flink/replay/*.savepoint
	$(REPLAY) --profile replay up -d --build --wait
	TICKLAB_EXCHANGES=$(EXCHANGES) TICKLAB_JOBS=$(JOBS) TICKLAB_STACK=replay \
		$(UV_ENV) python -m ticklab flink start

replay-drain: .env ## End a replay: Flink drains, so every remaining window fires
	TICKLAB_STACK=replay $(UV_ENV) python -m ticklab flink drain

replay-down: .env ## Stop the replay stack and delete its data
	$(REPLAY) --profile replay --profile ci down -v
	rm -f data/flink/replay/*.savepoint

replay: .env ## Replay recordings: make replay FROM=2026-10-11T10:00 TO=2026-10-11T10:15 [SESSION=live-] [SPEED=10] [EXCHANGES=BINANCE,KRAKEN]
	TICKLAB_EXCHANGES=$(EXCHANGES) $(UV_ENV) python -m ticklab replay recordings \
		--from $(FROM) --to $(TO) $(if $(SESSION),--session $(SESSION)) $(if $(SPEED),--speed $(SPEED))

replay-history: .env ## Replay whole days of history: make replay-history FROM=2026-10-09 [TO=2026-10-10] [SPEED=100] [EXCHANGES=BINANCE,KRAKEN]
	TICKLAB_EXCHANGES=$(EXCHANGES) $(UV_ENV) python -m ticklab replay history --from $(FROM) \
		$(if $(TO),--to $(TO)) $(if $(SPEED),--speed $(SPEED))

replay-kraken-history: .env ## Replay Kraken history: make replay-kraken-history FROM=2026-10-10T08:00 TO=2026-10-11T08:00 [SPEED=100]
	$(UV_ENV) python -m ticklab replay kraken-history --from $(FROM) --to $(TO) \
		$(if $(SPEED),--speed $(SPEED))

record: .env | data/recordings ## Record live frames only, without Docker or Kafka: make record [EXCHANGE=kraken]
	$(UV_ENV) python -m ticklab record $(or $(EXCHANGE),binance)

history: .env | data/history ## Download and check Binance daily trade files: make history FROM=2026-10-09 [TO=...]
	$(UV_ENV) python -m ticklab history download --from $(FROM) $(if $(TO),--to $(TO))
	$(UV_ENV) python -m ticklab history check --from $(FROM) $(if $(TO),--to $(TO))

kraken-history: .env | data/history ## Download and check Kraken trades: make kraken-history FROM=2026-10-10T08:00 TO=2026-10-11T08:00
	$(UV_ENV) python -m ticklab kraken-history download --from $(FROM) --to $(TO)
	$(UV_ENV) python -m ticklab kraken-history check --from $(FROM) --to $(TO)

study-disorder: .env ## Arrival disorder in recordings: make study-disorder FROM=... TO=... [SESSION=...] [EXCHANGES=KRAKEN]
	TICKLAB_EXCHANGES=$(EXCHANGES) $(UV_ENV) python -m ticklab study disorder \
		--from $(FROM) --to $(TO) $(if $(SESSION),--session $(SESSION))

study-signals: .env ## Spreads, moves and volumes behind the alert thresholds: make study-signals FROM=2026-10-04 TO=2026-10-10
	$(UV_ENV) python -m ticklab study signals --from $(FROM) --to $(TO)

study-lead-lag: .env ## 1-second returns and moves behind the lead-lag thresholds: make study-lead-lag FROM=2026-10-04 TO=2026-10-10
	$(UV_ENV) python -m ticklab study lead-lag --from $(FROM) --to $(TO)

study-round-levels: .env ## 60-second moves and level tests behind the round-level bands: make study-round-levels FROM=2026-09-12 TO=2026-10-10
	$(UV_ENV) python -m ticklab study round-levels --from $(FROM) --to $(TO)

study-gaps: .env ## Gaps between trades in history: make study-gaps FROM=2026-10-03 TO=2026-10-09 [EXCHANGE=kraken]
	$(UV_ENV) python -m ticklab study gaps --from $(FROM) --to $(TO) \
		--exchange $(or $(EXCHANGE),binance)

# Every exchange by default, as in the live stack; EXCHANGES=... on the command line limits it,
# for a replay stack that carries fewer.
reconcile: .env ## Compare candles with official ones: make reconcile FROM=2026-10-10T00:00 TO=2026-10-11T00:00 [EXCHANGES=BINANCE]
	$(if $(filter command line,$(origin EXCHANGES)),TICKLAB_EXCHANGES=$(EXCHANGES)) \
		$(UV_ENV) python -m ticklab reconcile range --from $(FROM) --to $(TO)

fixture: .env ## Cut a fixture: make fixture FROM=2026-10-10T04:00 [MINUTES=15] [SOURCE=history|recordings|kraken] NAME=...
	$(UV_ENV) python -m ticklab fixture --from $(FROM) --minutes $(or $(MINUTES),15) \
		--source $(or $(SOURCE),history) --name $(NAME)

e2e: .env codespace-network $(JAR) ## End-to-end test: a fixture with chaos through a throwaway stack
	@for p in ticklab ticklab-replay; do if [ -n "$$($(COMPOSE) -p $$p ps -q)" ]; then \
		echo "Stack $$p is running; e2e needs its ports: stop it first"; exit 1; fi; done
	TICKLAB_STACK=ci $(E2E) -p ticklab-ci --profile ci down -v
	rm -f data/flink/ci/*.savepoint
	FIXTURE=$(FIXTURE) TICKLAB_STACK=ci $(E2E) -p ticklab-ci --profile ci up -d --build --wait
	TICKLAB_EXCHANGES=$(EXCHANGES) TICKLAB_STACK=ci $(UV_ENV) python -m ticklab flink start
	@status=0; TICKLAB_STACK=ci $(UV_ENV) python -m ticklab e2e --fixture $(FIXTURE) \
		--seed $(SEED) || status=$$?; TICKLAB_STACK=ci $(E2E) -p ticklab-ci --profile ci down -v; \
		exit $$status

chaos: .env ## The end-to-end test with another seed: make chaos SEED=11
	$(MAKE) e2e SEED=$(SEED) FIXTURE=$(FIXTURE)

e2e-kraken: .env ## The end-to-end test on the Kraken fixture: Kraken's gap repair, by time
	$(MAKE) e2e FIXTURE=$(KRAKEN_FIXTURE) EXCHANGES=KRAKEN

# The busiest hour of the history, replayed into an empty replay stack sized by the profile.
# The stack stays up afterwards for its dashboards; `make replay-down` removes it.
PROFILE ?= lite
# Only the benchmark has a default speed: replays without one run as fast as they can.
bench: SPEED ?= 10

bench: .env codespace-network $(JAR) | data/history ## Benchmark: the busiest hour, replayed: make bench SPEED=1|10|100 PROFILE=lite|full
	$(MAKE) replay-up SIZE=$(PROFILE) EXCHANGES=BINANCE,KRAKEN JOBS=core,signals
	TICKLAB_STACK=replay $(UV_ENV) python -m ticklab bench --speed $(SPEED) --profile $(PROFILE)

fail-taskmanager: ## Failure: the Flink TaskManager dies and comes back 10 s later
	docker kill ticklab-flink-taskmanager-1
	sleep 10
	docker start ticklab-flink-taskmanager-1

PRODUCER ?= binance

fail-producer: ## Failure: a producer restarts: make fail-producer [PRODUCER=kraken]
	docker restart ticklab-producer-$(PRODUCER)-1

fail-network: ## Failure: a producer loses the internet for 60 s; Kafka stays reachable
	docker network disconnect ticklab_internet ticklab-producer-$(PRODUCER)-1
	sleep 60
	docker network connect ticklab_internet ticklab-producer-$(PRODUCER)-1

# GitHub Codespaces' default image (October 2026) runs Docker with iptables-nft rules, but
# leftover iptables-legacy rules are also active: their FORWARD policy is DROP and they only
# allow Docker's default bridge, so every Compose network is cut off, between containers and
# from the internet. This lets Docker's bridges forward in the legacy rules too. It does
# nothing outside Codespaces, or when the legacy policy isn't DROP.
codespace-network:
	@if [ "$${CODESPACES:-}" = true ] && command -v iptables-legacy >/dev/null \
		&& sudo -n iptables-legacy -S FORWARD 2>/dev/null | grep -q -- '-P FORWARD DROP' \
		&& ! sudo -n iptables-legacy -C FORWARD -i br-+ -j ACCEPT 2>/dev/null; then \
		echo "Codespaces: letting Docker bridges forward in iptables-legacy"; \
		sudo -n iptables-legacy -I FORWARD 1 -i br-+ -j ACCEPT; \
		sudo -n iptables-legacy -I FORWARD 1 -o br-+ -j ACCEPT; \
	fi

lint: ## Check style and common mistakes (ruff)
	$(UV) ruff check .
	$(UV) ruff format --check .

test: ## Run the Python unit tests
	$(UV) pytest

test-java: ## Build the Flink jobs and run their tests
	cd flink && ./mvnw -B verify

ci: lint test test-java e2e e2e-kraken ## Everything GitHub Actions runs

check-m1: .env | data/recordings ## The M1 checks (about 30 minutes; needs the live stack down)
	$(UV_ENV) python -m ticklab.checks.m1

check-m2: .env | data/recordings ## The M2 checks (about 3 hours; needs the live stack down)
	$(UV_ENV) python -m ticklab.checks.m2

check-m3: .env | data/recordings ## The M3 checks (about 90 minutes)
	$(UV_ENV) python -m ticklab.checks.m3

check-m4: .env | data/recordings data/history ## The M4 checks (about 5 hours)
	$(UV_ENV) python -m ticklab.checks.m4

check-m5: .env | data/history ## The M5 checks for one profile: make check-m5 PROFILE=lite|full
	$(UV_ENV) python -m ticklab.checks.m5 --profile $(PROFILE)

check-m6: .env | data/history ## The M6 checks: a week of history through the analyses job (about 1.5 hours)
	$(UV_ENV) python -m ticklab.checks.m6

check-m7: .env | data/history ## The M7 checks: two days through the analyses job, four weeks in Python (about 1.5 hours)
	$(UV_ENV) python -m ticklab.checks.m7

check-m8: .env | data/history ## The M8 checks: two days through the analyses job, four weeks in Python (about 1.5 hours)
	$(UV_ENV) python -m ticklab.checks.m8
