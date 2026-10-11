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

.PHONY: help up down ps logs console replay-up replay-down replay replay-history replay-drain \
	record history study-disorder study-gaps reconcile lint test test-java ci check-m1 \
	check-m2 codespace-network

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
	$(LIVE) --profile live --profile console down

ps: .env ## Show the live stack's containers
	$(LIVE) --profile live --profile console ps

logs: .env ## Follow the live stack's logs
	$(LIVE) --profile live logs -f --tail 50

console: .env ## Add Redpanda Console to the live stack (http://localhost:8080)
	$(LIVE) --profile console up -d --wait console

replay-up: .env codespace-network $(JAR) ## Start an empty replay stack with a fresh Flink job
	@if [ -n "$$($(LIVE) ps -q)" ]; then echo "The live stack is running: make down"; exit 1; fi
	$(REPLAY) down -v
	rm -f data/flink/replay/last-savepoint
	$(REPLAY) up -d --build --wait
	TICKLAB_STACK=replay $(UV_ENV) python -m ticklab flink start

replay-drain: .env ## End a replay: Flink drains, so every remaining window fires
	TICKLAB_STACK=replay $(UV_ENV) python -m ticklab flink drain

replay-down: .env ## Stop the replay stack and delete its data
	$(REPLAY) down -v
	rm -f data/flink/replay/last-savepoint

replay: .env ## Replay recordings: make replay FROM=2026-10-11T10:00 TO=2026-10-11T10:15 [SESSION=live-] [SPEED=10]
	$(UV_ENV) python -m ticklab replay recordings --from $(FROM) --to $(TO) \
		$(if $(SESSION),--session $(SESSION)) $(if $(SPEED),--speed $(SPEED))

replay-history: .env ## Replay history: make replay-history FROM=2026-10-09 [TO=2026-10-10] [SPEED=100]
	$(UV_ENV) python -m ticklab replay history --from $(FROM) $(if $(TO),--to $(TO)) \
		$(if $(SPEED),--speed $(SPEED))

record: .env | data/recordings ## Record live Binance frames only, without Docker or Kafka
	$(UV_ENV) python -m ticklab record binance

history: .env | data/history ## Download and check Binance daily trade files: make history FROM=2026-10-09 [TO=...]
	$(UV_ENV) python -m ticklab history download --from $(FROM) $(if $(TO),--to $(TO))
	$(UV_ENV) python -m ticklab history check --from $(FROM) $(if $(TO),--to $(TO))

study-disorder: .env ## Arrival disorder in recordings: make study-disorder FROM=... TO=... [SESSION=...]
	$(UV_ENV) python -m ticklab study disorder --from $(FROM) --to $(TO) \
		$(if $(SESSION),--session $(SESSION))

study-gaps: .env ## Gaps between trades in history: make study-gaps FROM=2026-10-03 TO=2026-10-09
	$(UV_ENV) python -m ticklab study gaps --from $(FROM) --to $(TO)

reconcile: .env ## Compare candles with official ones: make reconcile FROM=2026-10-10T00:00 TO=2026-10-11T00:00
	$(UV_ENV) python -m ticklab reconcile range --from $(FROM) --to $(TO)

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

ci: lint test test-java ## Everything GitHub Actions runs

check-m1: .env | data/recordings ## The M1 checks (about 30 minutes; needs the live stack down)
	$(UV_ENV) python -m ticklab.checks.m1

check-m2: .env | data/recordings ## The M2 checks (about 3 hours; needs the live stack down)
	$(UV_ENV) python -m ticklab.checks.m2
