# 0015: Watermark idleness for live data only

**Status:** accepted, milestone 2 (found by its replay checks)

## Context

The job's watermark is the minimum over all partitions, so a quiet feed holds every window
back. Flink's remedy is *idleness*: a partition that gets no data for a while (10 s here) stops
counting until it speaks again.

That "while" is measured on the machine's clock, which caused two failures in the first run of
the milestone 2 checks:

- **A day of history replayed:** the whole day sat in Kafka at once. While the job worked
  through the backlog, the Kafka consumer handed it BTC and SOL records for more than 10 s
  before any ETH records. The ETH partition was marked idle, the watermark raced ahead on the
  other two, and ETH's first minutes then arrived far behind it: too late even for the lateness
  budget, so they went to `late-trades` instead of into candles.
- **Draining after a pause:** when every partition had been idle for a while, draining didn't
  fire the last open window, so the last minute of the replay had no candle.

## Decision

Idleness is a job setting (`--idleness-seconds`): 10 s in the live stack, off in the replay and
CI stacks (`ticklab/flinkctl.py` passes it, `CorePipeline.watermarks` applies it).

## Consequences

- In a replay, the watermark waits for every feed, so nothing is late that wasn't late in the
  data, and draining fires every window.
- In a replay, one quiet feed delays every window until it trades again. That slows the
  candles but never changes them.
- Live, a restart leaves a backlog of minutes, which the job reads in well under a second, far
  inside the 10 s timeout. A much larger live backlog could hit the same problem, so after a
  long outage it is safer to start the job without idleness and switch it on once caught up.
