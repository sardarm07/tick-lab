# 0010: One jar, one Flink session cluster, Maven wrapper

**Status:** accepted, milestone 2

## Context

The project runs on a machine with 8 GB of RAM, shared with Redpanda, ClickHouse, Prometheus,
Grafana and the Python services. Flink can run each job in its own cluster (application mode)
or several jobs in one long-running cluster (session mode).

## Decision

- **One session cluster:** a JobManager (640 MB) and a TaskManager (1,300 MB, two slots), with
  in-memory (HashMap) state and a checkpoint every 10 s to a Docker volume (`compose.yaml`).
  The TaskManager has 64 MB of managed memory, which Flink SQL's window aggregations need for
  their buffers (the signals job, milestone 4); the DataStream jobs use none.
- **One jar** (`flink/`, built by Maven) holding all jobs. Classes are generated from the same
  Avro schemas the Python side uses (`schemas/`). The Kafka connector, Avro and the schema
  registry client are bundled into it; Flink itself is not.
- **Maven wrapper:** `flink/mvnw` downloads Maven 3.9.16 and checks it against a pinned SHA-256,
  so builds don't depend on whatever Maven a machine has.
- **Java 17**, which Flink 2.2 supports and the official `flink:2.2.1-java17` image uses.
- **Submission:** jobs go in through Flink's REST API (`ticklab/flinkctl.py`), which also stops
  them with a savepoint and resumes them from it.

## Consequences

- One JVM pair for all jobs fits the memory budget; jobs share the TaskManager's slots.
- A JobManager crash takes all jobs down; there is no high availability. Restarting resumes from
  the last savepoint, or from Kafka's committed offsets.
- `confluent` is the one Maven repository outside Maven Central, for the schema registry client.
