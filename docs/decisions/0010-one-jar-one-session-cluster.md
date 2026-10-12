# 0010: One jar, one Flink session cluster, Maven wrapper

**Status:** accepted, milestone 2

## Context

The project runs on a machine with 8 GB of RAM, shared with Redpanda, ClickHouse, Prometheus,
Grafana and the Python services. Flink can run each job in its own cluster (application mode)
or several jobs in one long-running cluster (session mode).

## Decision

- **One session cluster:** a JobManager (740 MB) and a TaskManager (1,300 MB), with
  in-memory (HashMap) state and a checkpoint every 10 s to a Docker volume (`compose.yaml`).
  Jobs never share a slot, so the TaskManager has one slot per job and parallel task: 3 on the
  lite profile (core, signals, analyses), 6 on the full one at parallelism 2. It has managed
  memory (96 MB lite, 256 MB full, split evenly between the slots), which Flink SQL's window
  aggregations need for their buffers (the signals job, milestone 4); the DataStream jobs use
  none.
- **Metaspace sized by measurement** (milestone 6): the JobManager loads every submitted job's
  jar, and the signals job's SQL planner, into the JVM's metaspace. With the third job it
  reached 163 MiB, past its 160 MB, and the JobManager died. It now has 256 MB; the TaskManager
  used 111 MiB of its 192 MB (`config/sizing/`).
- **One jar** (`flink/`, built by Maven) holding all jobs. Classes are generated from the same
  Avro schemas the Python side uses (`schemas/`). The Kafka connector, Avro and the schema
  registry client are bundled into it; Flink itself is not.
- **Maven wrapper:** `flink/mvnw` downloads Maven 3.9.16 and checks it against a pinned SHA-256,
  so builds don't depend on whatever Maven a machine has.
- **Java 17**, which Flink 2.2 supports and the official `flink:2.2.1-java17` image uses.
- **Submission:** jobs go in through Flink's REST API (`ticklab/flinkctl.py`), which also stops
  them with a savepoint and resumes them from it.

## Consequences

- One JVM pair for all jobs fits the memory budget; the jobs share the TaskManager, each in
  slots of its own.
- A JobManager crash takes all jobs down; there is no high availability. Restarting resumes from
  the last savepoint, or from Kafka's committed offsets.
- `confluent` is the one Maven repository outside Maven Central, for the schema registry client.
