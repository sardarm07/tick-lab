# 0012: Failure scripts, and an internal network that makes the network cut possible

**Status:** accepted, milestone 3

## Context

The pipeline claims to survive a dead TaskManager, a producer restart and a lost internet
connection. Each claim needs a script that causes the failure the same way every time.

Cutting the producer's internet is the awkward one: on a normal Docker network, every container
reaches both the other containers and the internet over the same interface, so there is nothing
to remove that leaves Kafka reachable.

## Decision

- **Two networks** (`compose.yaml`): the default network is `internal` (no route out), and only
  services that need the internet or publish a port also join `internet`. `make fail-network`
  detaches the producer from `internet` for 60 s; it keeps talking to Redpanda on the internal
  network.
- **`make fail-taskmanager`** kills the TaskManager and starts it again 10 s later. Docker
  treats `docker kill` as a deliberate stop and wouldn't restart it on its own.
- **`make fail-producer`** restarts the producer, which reconnects and leaves a short gap.
- **Pause:** `docker pause` freezes the producer, to test silence detection.

## Consequences

- Each failure is followed by checking every affected minute against the exchange
  (`make check-m3`): recovery is proven by the result, not by the absence of errors.
- The internal network also documents which services talk to the outside world: the producer,
  gap repair, the reconciler and Grafana (for its plugins).
