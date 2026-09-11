---
name: debug-fuzz-top
description: Debug a failing WAL/O3 test on the composite-partition (lazy geometry) branch — assume a partition made of several pieces over one set of column files is the fast or latent culprit
argument-hint: [test name, seeds, or failure log]
allowed-tools: Bash(mvn *), Read, Grep, Glob
---

Debug the failure described by `$ARGUMENTS`.

## Read these first

- `core/src/main/java/io/questdb/cairo/COMPOSITE_PARTITIONS.md` — what is built, in
  four short tables, plus what is not.
- `git log --oneline lazy-geometry2` — the running record: every defect found so far and how it was
  fixed. Most failures rhyme with one already in there.

This branch is `lazy-geometry2`, based on clean `master`. A DIFFERENT tree
(`questdb-enterprise3`, `feat-partition-top-split`) implements the same feature as a SPLIT design where
`_txn` carries one entry per piece. **Its notes do not transfer.** Here `attachedPartitions` is exactly
as master has it — one 4-long record per DIRECTORY — and no partition index ever addresses a piece.

## Vocabulary

- **partition / directory / record** — the same thing. One `_txn` record, one directory, one set of
  column files.
- **piece** — `(tsLo, tsHi, rowOffset, rowCount)` inside that directory's `_geometry`. Pieces exist ONLY
  there; nothing outside `PartitionGeometry`, the planner, the executor and the two frame cursors sees
  them.
- **live rows** — the sum of the pieces' row counts. What a reader returns, and what `_txn` records.
- **physical rows** — how far the column files reach, dead space included. What a reader MAPS and what
  every whole-partition consumer must walk. Equal to live rows for an ordinary partition.
- **composite** — a partition whose `_txn` slot 3 carries a geometry pointer. `NO_GEOMETRY_REF == 0`.

## The invariants, and how each one has already been broken

Every one of these has produced at least one defect. When something is wrong, suspect them in order.

1. **The unit of an index, a column top, a directory name, a seal and a type conversion is the
   DIRECTORY, never the piece.** One directory holds one file per column and ONE index every piece
   reads. Ranges over it are `[columnTop, physicalRows)` via `TableWriter.getPartitionFileRowCount`.
   Using live rows there silently drops every relocated piece's rows.
2. **Physical row order is NOT timestamp order.** A merge-append parks a rewritten piece at the tail,
   above pieces that sort before it. Anything that binary-searches or assumes ascending timestamps over
   file rows is wrong — see `CompositeTimestampFinder` and the covering sidecar.
3. **GROW-ONLY.** Files only ever grow; every write is a tail append. A close truncates each column from
   the append position, so that position must be physical rows, not live rows.
4. **The geometry is stale between commits.** Only `commitUpdate`/`publish` rebuild it. Nothing
   mid-apply may read it as authoritative.
5. **Nothing per-piece may be linear.** A directory can hold thousands of pieces; `findPiece` and
   `findPieceByRow` are binary searches and `getPieceCumulativeLo` is a slot read.


## Step 0: Get the seeds

Find the random seeds in the test output:
```
random seeds: 137113830825708L, 1776424558803L
```

If the user provided a failure log, extract the seeds from there and compare them against
the seeds currently hardcoded in the test method. If the test uses `generateRandom(LOG)`
(no fixed seeds), note this — you'll need to hardcode the failing seeds to reproduce later.

If the seeds are already hardcoded in the test and match the failure log, no reproduction
step is needed — proceed directly to tracing.


## Step 1 — was a composite partition even involved?

```bash
grep -n "merge-append composite partition" run.log
```

One INFO line per partition per commit:

```
merge-append composite partition [table=, ts=, pieces=A->B, composite=, keep=, merge=, newPiece=,
                                  liveRows=A->B, physicalRows=A->B, deadPct=]
```

- **absent** — no composite write happened; this is not the feature's bug. Fall back to the generic
  `debug-fuzz` skill.

## Step 2 - use seeds to reproduce

Instrument the correct `generateRandom` call with the seeds. Run the test once: if it fails — i.e. it
reproduces the bug — good, there is a 100% repeatable reproducer. Next step is to create a minimum
deterministic reproducer without the fuzz framework.

If it does not fail, try running it a few more times. Some tests fail only under parallel writing, or
because of races, and only some of the time. If the test fails with the fixed seeds once in a while — say
1 out of 10 — it is still somewhat repeatable and can be used for Step 4.

If it never fails, or fails rarely with different symptoms each time, report back and stop investigation.

## Step 4 — form a theory, then build a deterministic reproducer

Random-seed replay is not reliable on its own: WAL application runs on a real thread pool, and
record order under concurrent writing is not guaranteed for a given seed even with the
data-generation `Rnd` stream pinned. Don't burn more than one attempt on `new Rnd(s0, s1)` before
moving to the loop below — this branch's own history already confirms a fixed-seed replay can pass
cleanly against the exact code and log that caught it.

1. **Record the shape.** Add a temporary `LOG.info()` at the point that plans or executes the
   composite write (`assembleFreshPartitionVersion`, `executeCompositePlan`, or wherever step 2
   pointed) that dumps, for the commit about to run: every action's
   type/pieceIndex/o3Lo/o3Hi/pieceLo/pieceHi/tsLo/tsHi, and the current per-column tops
   (`columnVersionReader.getColumnTop(ts, col)`) for the columns in play. `describePieces(tableName)`
   in `O3PartitionPreSplitTest` is the existing print for a geometry's pieces alone
   (`pieces=[i:[tsLo..tsHi]@rowOffset+rowCount, ...] E=`) if that's all a given theory needs.
2. **Reproduce naturally, then read the dump.** Loop the failing test with fixed random seeds. Walk the log
   backward from the crash and note the last 2-4 commits that built the failing shape in order —
   table, action, row counts — not just the one commit that crashed.
3. **Form a theory from the numbers, not just the stack trace.** The recorded pieceLo/pieceHi and
   column-top values should let you hand-derive the exact offsets in the failing call (e.g. decode
   a `CairoException`'s `srcOffset`/`size` back into piece bounds via the column's byte width) and
   name which piece and which prior action produced the wrong value. A theory that doesn't
   reproduce the crash's own numbers by arithmetic is not yet the right theory.
4. **Build the deterministic reproducer from the theory, not from the fuzz trace.** Once the
   theory names a *shape* (e.g. "two composite pieces both below a just-added column's top, then a
   forced fresh-rewrite"), construct it with the simplest possible sequence of SQL/API calls that
   produces an equivalent shape in `O3CompositePartitionTest` or `O3PartitionPreSplitTest` — the
   fuzz run's exact row counts and timestamps don't matter, only the qualitative structure the
   theory depends on. Replay just the last few transactions/apply iterations identified in step 2,
   in order, against that constructed shape. If the shape genuinely can't be built deterministically
   (a cross-thread timing race, not a sequence-dependent one), say so explicitly and fall back to confirming the fix via repeated fuzz runs instead.

Remove the temporary logging from step 1 before the fix ships.

## Working notes

- `JAVA_HOME` must be Java 25: `/opt/homebrew/opt/java/libexec/openjdk.jdk/Contents/Home`.
- `mvn -Dtest='A+B'` selects NOTHING and passes vacuously. Use `-Dtest=ClassName` or
  `-Dtest.include="%regex[.*/ClassName.*]"`, and confirm against surefire report MTIMES.
- **An IntelliJ build server compiling into the same `core/target` produces bogus runs** — 0 tests
  selected, or a subset. If a count looks wrong, that is why, not the change under test.
- A JVM crash leaves `core/hs_err_pid*.log`; its "Problematic frame" plus the Java frames under it name
  the offending call directly. Surefire reports it as "The forked VM terminated without properly saying
  goodbye".
