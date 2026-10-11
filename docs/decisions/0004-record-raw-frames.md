# 0004: Record raw frames before parsing

**Status:** accepted, milestone 1

## Context

Tests, fixtures, replays and the data study all need real data. If only normalised trades were
kept, a bug in a normaliser would be baked into everything recorded while it existed.

## Decision

- Each producer writes every WebSocket frame, exactly as received, before parsing it
  (`ticklab/recorder.py`). The same recorder runs without Kafka as `make record`.
- Files are gzip-compressed JSON lines, one file per UTC hour and process:
  `data/recordings/<exchange>/<date>/<exchange>-<YYYYMMDDTHH>-<session>.jsonl.gz`.
- Each line holds the local arrival time in microseconds. `open` and `close` lines record each
  connection and its URL, and every new file repeats the `open` lines of connections still
  open, so a file can be replayed on its own.
- Files are never appended to. The recorder flushes at least once a second, so after a crash a
  file is readable up to its last flush; the reader drops a final partial line.
- The exchanges' history files are kept unmodified in the same spirit, and normalised when
  replayed.
- Recordings and history stay on the machine that made them (git-ignored). Only small
  fixtures, under 15 minutes, are committed.

## Consequences

- `make replay` reproduces a live run exactly. The M1 check proves it by comparing the stored
  trade sets.
- Replays exercise the real normaliser code, so normaliser fixes apply to old data.
- Recordings take about 17 bytes per trade, compressed: about 0.9 MB an hour for the three
  Binance pairs in quiet night hours, and about 4 MB an hour on an average weekday.
