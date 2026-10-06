# Table-pressure selection by waste, and splits owned by compaction

## Evidence

TSBS replay (`profiles-r8a-replay/replay2`): day one (207M live rows) was copied 3.4x by
compaction. The forecast split it into 10 folders and rewrote each prefix (157M rows); the
mid-partition squash rule folded 9 of them back into one folder on the commit that created
day two (191M rows); 40 late merge blocks left 19.4M dead rows (9.4%) in that folder; table
pressure, caused by day two's dead rows, then picked it as the *oldest* folder and rewrote all
207M rows (26.5 GB for 2.5 GB reclaimed). It was too clean for MOVE-TAIL (>10% dead) but
REWRITE under pressure has no floor.

## Change 1: table pressure picks by waste, with a floor

Today pressure takes the head of the age tier - the oldest folder, any dead ratio.

New tier between waste and piece count, active only while pressure is on:

| tier | fires when | order |
|---|---|---|
| waste | dead > 100% live, > `dead.min.size` | waste % desc |
| **pressure waste** | dead > **50%** live, > `dead.min.size` | waste % desc |
| piece count | pieces > `effectiveMaxPieces` | pieces desc |
| age | idle, still wasteful or > 1 piece | oldest |

Pressure never falls through to the age tier. Empty band: select nothing, keep the
latch, re-check next commit. Percent, not bytes: it is rows reclaimed per row copied.
Config `cairo.partition.compaction.table.pressure.dead.ratio` = `0.5`, must be below
`dead.rows.ratio`. Hot handling unchanged.

## Change 2: splits belong to compaction, not the commit

Today a non-last logical partition may hold 1 split (`o3.mid.partition.max.splits`), so
all splits are squashed in the apply path the moment the next day appears.

- One cap for every logical partition: `cairo.o3.partition.max.splits` = 20 (old
  last-partition value). `o3.mid.partition.max.splits` becomes a deprecated no-op.
- The cap is a squash target, not a split gate: overflow, then squash. A split that pays -
  MOVE-TAIL, its ingestion forecast, or the O3 prefix split - happens even when the day
  already holds the cap. After the commit, housekeeping squashes the smallest cold adjacent
  pair, again and again, until the day is back at the cap. A hot folder (written in the last
  `hot.commits` commits) is never a squash source or target, so a day stays above the cap
  until its new folders cool, and later commits finish the squash. Gating splits on the cap
  could not work: a day at the cap never went over it, so the squash never ran. In TSBS
  every day reached 20 folders by mid-day, and from then on the last folder grew to
  1.4K-2.8K pieces and 100-225M physical rows.
- One hard ceiling, `PartitionCompactionPolicy.getSplitCeiling`: cap + min(cap,
  max(1, `hot.commits`)). The overflow allowance covers the folders the hot window keeps
  out of the squash's reach, and is at most the cap itself, so a day never holds more than
  twice the cap. Every split path checks `getSplitRoom`, so all paths agree on when a day is
  full. The O3 split reserves room per logical day, because concurrent partition jobs can
  split the same day in one commit.
- Plain WAL folders age by their native seqTxn. A folder with no seqTxn stamp (an O3 split's
  suffix, a MOVE-TAIL tail, a squash of unstamped sources) ages by the txn that named its
  directory instead. Treating it as hot forever would keep the day above the cap for good.
- `isPendingSquashSource` tells a commit not to fold a partition that the commit's own
  squash is about to consume. With a hot window it is always false: the commit writes the
  partition, so the partition is hot, and the squash will not touch it until it cools.
- Folding a day to one folder is a sweep step: `JOIN -> MOVE-TAIL -> MAKE-PLAIN -> SQUASH
  -> REWRITE`. SQUASH is eligible when the day is not last, > 1 split, no split written in
  the last `hot.commits`, and selected by age or pressure waste. It reuses
  `squashPartitionRange` (`force = false`, frozen `lastCommitTxn`), copies live ranges only,
  folds pairs until `time.budget.ms` and resumes next sweep.
- MOVE-TAIL fresh partitions count against the ceiling, and the squash brings them back to
  the cap. Parquet conversion keeps `force = true`.

## Change 3: a large folder is never rewritten whole when MOVE-TAIL pays

Today MOVE-TAIL has its own gate (dead > split size and > 10% live, or > 1,000 pieces);
below it the sweep falls straight to REWRITE, which is how a 207M-row folder got copied.

For any folder above `cairo.o3.partition.split.min.size`, whichever rule selected it, the
sweep tries MOVE-TAIL first whenever it is beneficial: the cold prefix (pieces untouched
by the last `hot.commits` commits) holds more than half the live rows. The tail goes to a
fresh split; the prefix is then rewritten only if it is wasteful on its own, else left as
is. The gate decides *whether* to act; once a rewrite is due, moving the tail is always
cheaper than copying it. Small folders keep the plain REWRITE.

Result on TSBS: late ticks merge into small splits; a split over 50% dead is rewritten
alone; day one is folded once, when idle, never twice within a minute.

## Tests and acceptance

Policy: pressure with folders at 9/30/60/90% dead selects 90, 60, then -1; oldest never
preferred. Splits: a day with 5 splits survives the next day's creation and is folded by the
sweep after `idle.timeout`; a day at the cap squashes the smallest pair on commit; a hot split
blocks SQUASH; SQUASH stops at the budget and resumes. MOVE-TAIL and the O3 split still cut a
day that is at the cap; the count never passes the ceiling, and it settles at the cap once the
folders cool, `hot.commits` commits later (`CompactionSplitOverflowTest`). Fuzz with cap 2, 3,
20 and checkpoints.
Replay acceptance: no REWRITE below 50% dead under pressure, no day squash inside apply,
day-one compaction copies < 1x live, amplification < 3x (was 4.28x). No day stays at 19-20
folders while its last folder keeps growing ("moving compaction tail" stops happening).
"squashing partitions" lines appear in the log, and no day's last folder exceeds a few
hundred pieces in a 4-day replay.
