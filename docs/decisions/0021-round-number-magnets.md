# 0021: Round-number magnets, tested against random levels

**Status:** accepted, milestone 7

## Context

Traders talk of round numbers as support and resistance: BTC "holding 80,000". If that were so,
prices reaching a round level would turn back more often than prices reaching any other level.
Testing it needs levels, a fair comparison, a rule for what counts as reaching a level, and a
verdict that doesn't depend on hindsight.

## Decision

- **Binance's three pairs** (`RoundLevelTester.java`, `ticklab/round_levels.py`): the deeper
  market, trading in cents, with daily history files cheap enough for four weeks and more.
- **Two tiers of levels:** major (BTC every 1,000 USDT, ETH 100, SOL 10) and minor (BTC every
  100, ETH 10, SOL 1; a major level is also a minor one). Each tier is reported separately:
  majors are rarer, but they are the ones people mean.
- **Random levels as the control:** one in each step of each tier, at a place drawn from
  SHA-256 of a fixed seed, at least a tenth of a minor step from any round level. They have the
  same density as the round levels and are nowhere special, so the difference between the two
  is what roundness adds. Drawn the same way in Java and in Python, from exact integers.
- **A test** starts at the first trade that reaches a level from beyond its band, and the level
  is then disarmed until the price is beyond the band again. So a price hovering at a level is
  one test, not hundreds.
- **One test of a level at a time:** reaching a level again while its test is open starts no
  other. Tests of the same level that overlap would see the same 60 seconds, and counting them
  as separate observations would make the intervals look narrower than they are. A test is
  identified by the trade that reached the level, which no other test can share.
- **The verdict** is the close at the end of the second 60 s later: BREAK at least a band past
  the level, BOUNCE at least a band back, NEUTRAL in between. A 60-second event-time timer
  decides it, in event-time order with the seconds (`InEventTimeOrder`).
- **The band:** the pair's median 60-second move over four weeks, in whole cents: BTC 16.50,
  ETH 0.77 and SOL 0.05 USDT (`make study-round-levels`, docs/data-study.md). A level is reached
  "from beyond" when the price was at least that far away, and decided when the price has gone
  at least that far either way: as far as it typically goes in a minute.
- **Gaps:** a test during which the feed's trade IDs skip is discarded, in the job and in Python.
- **Findings from four weeks of history** by the Python definitions, which `make check-m7`
  proves equal to the job's on a replay; replaying four weeks through Kafka would take hours and
  more disk than the codespace has.

## Consequences

- A level with no pull at all does not bounce half the time: over four weeks, random levels
  bounced in 39% to 51% of decided tests, depending on the pair and tier. A price that has just
  travelled a band's width to reach a level tends to go on. Without the control, any level would
  look like a weak barrier.
- A bounce share is a proportion with a sampling error; each is reported with its 95% interval,
  and a difference between round and random levels only counts if it is larger than those.
- The first rules allowed overlapping tests, and in a spike on 2026-10-08 ETH reached the same
  level up to five times within one microsecond. The job stored those tests under one key and
  Python counted them all, so `make check-m7` found seven tests missing from the job. One test
  at a time, identified by its trade, removed 42% of the tests over those two days and every
  collision.
