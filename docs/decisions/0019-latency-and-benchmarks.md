# 0019: Latency, defined three ways, and benchmarks of the busiest hour

**Status:** accepted, milestone 5

## Context

"How fast is it?" needs a definition before it can be measured, and a benchmark needs a load
that is realistic, repeatable, and not limited by the machine generating it.

## Decision

- **Three latencies,** each computed in ClickHouse from timestamps that exist in the data
  (LEARNING.md, milestone 5):
  - feed latency: received minus trade time, across two clocks;
  - pipeline latency: stored in ClickHouse minus published to Kafka, per trade;
  - output latency: a candle emitted by Flink minus the publishing of its newest trade. For
    this, candles carry `input_published_at`, the Kafka timestamp of their newest trade.
- **The busiest hour of the data study** (2026-10-08 15:00 UTC: 1.32 million Binance trades and
  41 thousand Kraken trades) is the benchmark load, replayed at 1x, 10x and 100x of real time
  into an empty replay stack with both jobs (`make bench`, `ticklab/bench.py`).
- **The load is encoded before the clock starts.** Turning history rows into Avro in Python is
  slower than Kafka; sending pre-encoded messages keeps the replay from being the limit.
- **Measured per run:** trades sent per second and whether the schedule was kept; the largest
  consumer lag of each job; pipeline and output latency percentiles; the longest and largest
  checkpoint of each job; the peak memory of every container. Results go to `bench/results/`.
- **Both profiles:** lite (2 cores, 8 GB, parallelism 1) and full (4 cores, 16 GB, parallelism
  2), on the same codespace, switched between runs.
- **The core and signals jobs only,** the two there were when the benchmarks were run; `make
  bench` still starts just those two (milestone 6 added the analyses job), so a rerun measures
  the same thing.

## Consequences

- The numbers describe one machine and one hour of one week; they are a baseline to compare
  changes against, not a capacity claim.
- The lite profile keeps up with 10x the busiest hour (3,783 trades a second) and falls behind
  at 100x, by up to 664,000 trades. The full profile keeps up at 100x (37,830 a second), never
  more than 19,000 trades behind. It changes cores, memory and parallelism together, so the
  runs don't say which of them mattered.
- Pipeline latency mostly measures ClickHouse's Kafka batching (blocks flushed every 5 s at
  most), which is a setting, not a limit of the pipeline: 2.6 s in the median wherever the
  stack keeps up.
- Output latency is the watermark's, not processing time. In a replay a candle waits for the
  quietest feed's next trade, so it is shorter at 10x (0.6 s in the median) than at 1x (4.6 s);
  live, idleness ends the wait after 10 s of silence.
- Feed latency can't be benchmarked from history (it has no arrival times); the "Pipeline"
  dashboard shows it live.
