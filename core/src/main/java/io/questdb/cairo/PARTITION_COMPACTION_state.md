# Compaction implementation state

Tracks the port of `PARTITION_COMPACTION.md` (from the enterprise `feat-partition-top-split` branch,
commits `2e8b406c44..4e00cfaaea`) onto this branch's simpler composite-partition model. That branch's own
design doc and `PARTITION_COMPACTION_state.md` are the specification this port started from; this file is
the record of what changed to make it fit this branch, what was built, and what is left.

Read `core/src/main/java/io/questdb/cairo/cmposite_partition_like_parquet_partition.md` and
`COMPOSITE_PARTITION_STATE.md` first for the vocabulary this file assumes: a composite partition here is
exactly one `attachedPartitions` entry - one directory, one `(partitionTimestamp, nameTxn)`, one
`_geometry` chain, one set of column files. There is no hardlink split and no "partition top": the
reference repo's word "folder" (a physical partition, possibly one of several sharing a logical partition)
and this branch's word "partition" (there is only ever one physical partition per logical one) name the
same thing here.

## Scope of this pass

Steps 0, 1, 2, 3, 5 and, later, half of step 4 (MOVE-TAIL only - see "MOVE-TAIL: a second pass" below).
Step 4's other two parts (MAKE-PLAIN, TRIM-FILES) and step 6 (turn on by default, delete the temporary
squash budget) are not attempted, matching the reference's own open items.

| step | in this pass | contents | status |
|---|---|---|---|
| 0 | n/a | a geometry chain must never restart at offset 0 | **already true here** - see D1 |
| 1 | yes | per-partition waste tracking | **done**, no new list - see D1 |
| 2 | n/a | the `lastWriteMicros` `_geometry` header field | **already present** before this pass - see D2 |
| 3 | yes | measuring, `PartitionCompactionPolicy`, JOIN | **done** |
| 4 | **half** | MOVE-TAIL (done, ported onto classic splits - see below); MAKE-PLAIN, TRIM-FILES | MOVE-TAIL done, other two not started |
| 5 | yes | REWRITE and the four rules | **done** |
| 6 | **no** | on by default, delete the temporary squash budget | not started, as in the reference |

## What got built

- `core/src/main/java/io/questdb/cairo/PartitionCompactionPolicy.java` (new) - the four rules
  (waste ratio, piece count, age, table pressure) plus the cooldown/backoff bookkeeping, ported from the
  reference file of the same name. Operates on plain partition indices; see D3.
- `TableWriter.java` - `runCompaction`, `compactPhysicalPartition`, `foldContiguousPieces` (JOIN),
  `foldFoldableFolders`, `moveTailToFreshPartition` (MOVE-TAIL, added in a second pass - see below),
  `rewritePhysicalPartition` (REWRITE), `avgRecordSize()`, `getCompactionWrittenRows()`, wired into
  `housekeep` right before `processPartitionRemoveCandidates()`, matching the reference's own ordering
  rationale (REWRITE is what puts a directory on the remove-candidate list).
- `ColumnVersionWriter.java` - the D5-equivalent `hasChanges` fix (see D7 below): ported even though
  neither JOIN nor REWRITE ends up calling `squashPartition` in this model (D5) - the pre-existing
  classic-split squash path still calls it and still has the bug.
- `std/Numbers.java` - the `T` size suffix (D8 below), needed by the acceptance test's `1T` setting.
- Five-file config pattern for twelve `cairo.partition.compaction.*` settings - `PropertyKey.java`,
  `PropServerConfiguration.java`, `CairoConfiguration.java`, `DefaultCairoConfiguration.java`,
  `CairoConfigurationWrapper.java`. The reference's eleven implemented keys, plus `prefix.min.percent`
  (added in the MOVE-TAIL pass, default `50`) - `make.plain.enabled` and `queue.capacity` still belong to
  the unstarted parts of step 4/6.
- `griffin/engine/table/ShowPartitionsRecordCursorFactory.java` - `deadRows` and `lastWriteTimestamp`
  columns on `table_partitions()`, columns 16 and 17, following the reference's shape. This branch's
  `table_partitions()` has no per-piece donor/child row split (COMPOSITE_PARTITION_STATE.md never built
  one), so unlike the reference there is exactly one row per partition and these two columns are simply
  populated from `TableReader.getPartitionPhysicalRowCount()` / `TableReader.getGeometry().getLastWriteMicros()`.
- `core/src/test/java/io/questdb/test/cairo/o3/O3PartitionCompactionTest.java` (new) - the acceptance
  suite, ported test-by-test from the reference file, adapted to this branch's `table_partitions()`
  shape (no `donorName`/piece rows to filter on) and to a Java-side piece-count helper (see D3).

## Test results

`O3PartitionCompactionTest`: **7 of 10 pass**, after the second (MOVE-TAIL) pass described below. Before
that pass it was 6 of 10, the same bar the reference reports, though not always for the same reason.

| test | result |
|---|---|
| `testWasteRatioTriggerReclaimsDeadRows` | pass |
| `testAgeTriggerCompactsAPartitionNothingHasWrittenTo` | pass |
| `testPieceCountTriggerReducesTheNumberOfPieces` | pass |
| `testTablePressureTriggerCompactsTheColdestPartitionFirst` | pass |
| `testCompactionDoesNotLoopOnAPartitionItAlreadyEmptied` | pass |
| `testTablePartitionsReportsDeadRowsAndLastWriteTimestamp` | pass |
| `testMoveTailCopiesTheTailNotTheWholePartition` | **pass** (was fail - step 4 not implemented) |
| `testMakePlainWaitsForAReaderHoldingThePreMoveTailTransaction` | **fail - MAKE-PLAIN not implemented** |
| `testTrimFilesWaitsForAReaderThatMappedTheOldExtent` | **fail - TRIM-FILES not implemented** |
| `testJoinMergesAdjacentPiecesWithoutWritingAnything` | **fail - the fixture cannot reach JOIN** |

All four rules (waste ratio, piece count, age, table pressure) fire correctly and reclaim the expected
waste. The two remaining step-4 failures are exactly what the reference predicts, for the reference's own
reasons - REWRITE runs regardless of a pinned reader (it never writes below `E`, so it needs no reader
check), and both fixtures' stride sits mid-partition (~25% front, below MOVE-TAIL's `prefix.min.percent`
default of 50%), so REWRITE - not MOVE-TAIL - is what handles them:

- `testMakePlainWaitsForAReaderHoldingThePreMoveTailTransaction` fails because the partition is already
  non-composite by the time the pinned reader is released - the assertion expects it to still be
  composite at that point, which is exactly the state MAKE-PLAIN would still be waiting in.
- `testTrimFilesWaitsForAReaderThatMappedTheOldExtent` fails because disk usage does not fall a SECOND
  time - REWRITE already reclaimed everything on the first pass, so there is nothing left for a
  TRIM-FILES-shaped step to do.
- `testJoinMergesAdjacentPiecesWithoutWritingAnything` fails the same way the reference's own D1 describes:
  a chronological pre-split + merge-append relocates the hot piece to the tail, landing it BETWEEN its
  cold neighbours in the file, so the cold pieces are list-adjacent but never file-adjacent and JOIN has
  nothing to fold. `before=3, after=3` (extra pieces, not raw piece count - see D3).

### Fixture corrections needed to reach this branch's actual dead/live ratio

The reference's `20_000`-row-day + two `200`-row backdates fixture (used unmodified by four of its tests)
leaves `dead=20200` against `live=20400` in THIS branch - just under the `dead.rows.ratio=1` threshold,
so the waste-ratio rule never fires and every one of those four tests fails for the wrong reason (nothing
runs at all, rather than reaching the intended assertion). A THIRD backdate round was added to
`testCompactionDoesNotLoopOnAPartitionItAlreadyEmptied`, `testMakePlainWaitsForAReaderHoldingThePreMoveTailTransaction`
and `testTrimFilesWaitsForAReaderThatMappedTheOldExtent` to push `dead` safely past `live`. This branch has
no equivalent of the reference's cluster pre-split kicking in unasked for a plain `insert`-sized batch
against a 20k-row partition with no explicit `cairo.o3.partition.split.min.size` override, so every
backdate in those three fixtures rewrites the WHOLE partition rather than just its tail - which is a
fixture-tuning difference, not a functional one.

`testMoveTailCopiesTheTailNotTheWholePartition` needed a different, more deliberate fixture correction
once MOVE-TAIL was implemented, rather than just a third backdate round - see "MOVE-TAIL: a second pass"
below.

## Design corrections found while porting

Numbered independently of the reference's D1-D13; several of the reference's corrections do not apply at
all here, for reasons explained below.

**D1 - the reference's whole Sec.1 "per-folder list" does not need porting.** The reference built a
resident `LongList` inside `TxReader`/`TxWriter`, filled at load and rebuilt every commit, because ITS
`TxReader` flattens every piece into `attachedPartitions` and has no other place to keep folder-level
totals (`E`, `pieceCount`, `lastWriteMicros`) cheaply. This branch's `TxReader` never flattens pieces at
all - `attachedPartitions` already holds exactly one entry per partition, which for this branch's purposes
IS the reference's "folder" - and `PartitionGeometry` (the lazy per-partition resolver,
`core/src/main/java/io/questdb/cairo/PartitionGeometry.java`) already tracks `E`, `getPieceCount` and
`getLastWriteMicros` per partition, resolving from `_geometry` only on first touch and answering from a
resident cache afterwards. So `PartitionCompactionPolicy.selectPartition` just calls
`txWriter.getPartitionSize(i)` / `geometry.getE(i)` / `geometry.getPieceCount(i)` /
`geometry.getLastWriteMicros(i)` directly, once per partition, every commit. For a non-composite partition
this costs one resident read and zero I/O (`PartitionGeometry.resolveInternal` short-circuits on
`!txReader.isPartitionComposite(i)` before it ever opens a file); for a composite one it costs one
`_geometry` read the FIRST time this writer's lifetime touches it and nothing after, because
`PartitionGeometry`'s cache is keyed on the committed geometry ref and only re-reads when that ref changes.
This also means the reference's D3 ("a from-scratch rebuild cannot supply `lastWriteMicros`, because
`publishGroupGeometry` skips unchanged folders") and D4 ("one folder row per `_txn` record, so the
incremental tail reload can truncate it") simply do not arise: there is no rebuilt-every-commit list to
lose data from, because `PartitionGeometry.resolved` is itself incrementally maintained (populated by
`commitUpdate`/`publish`, or lazily by `resolveInternal`), never thrown away and rebuilt.

**D2 - the reference's step 0 (never restart a geometry chain at offset 0) was never a bug here.** That
fix existed because the reference's `publishGroupGeometry` dropped a folder's committed geometry pointer
back to `-1` the moment it stopped being composite, and a LATER write to the same `(dirTs, nameTxn)` would
then start a brand new chain at file offset 0 - potentially overwriting a record a pinned reader still
resolves. This branch's `O3PartitionJob`/`append-piece.md` machinery already keeps a composite partition's
existing chain alive (see `PartitionGeometry.resolveInternal`'s "a partition stays composite after folding
back to a single piece" note) and REWRITE never writes MORE geometry for a partition it just made plain -
it sets the pointer to `NO_GEOMETRY_REF` and moves to a FRESH `nameTxn`, so nothing ever writes a new
record at offset 0 of an old, still-referenced chain. No fix was needed.

**D3 - the `lastWriteMicros` `_geometry` header field, and its plumbing through `PartitionGeometry`, were
already present before this pass**, apparently added when the field's home
(`PartitionGeometryFile.HEADER_OFFSET_LAST_WRITE_MICROS_64`, header size 48) was built - `git log` shows
it landed in commit `b07153036a`, the very first composite-partition commit on this branch, long before
this port started. It was unused by any caller until now; `PartitionCompactionPolicy` is its first
consumer. Nothing needed adding to `PartitionGeometryFile`, `PartitionGeometry.publish` (already calls
`geometryFile.setLastWriteMicros(nowMicros)`) or `PartitionGeometry.getLastWriteMicros` - only the policy
that reads it.

**D4 - `pieceCount` in the acceptance test has to be an "extra pieces" count, not a raw total.**
`runCompactionPasses` adds six new, ordinary (non-composite) partitions as housekeeping filler, and this
branch's `PartitionGeometry.getPieceCount` answers `1` for an ordinary partition (not `0`, and there is no
`table_partitions()` row filter equivalent to the reference's `physicalRows = null` piece-row check to
exclude them with). A raw `sum(getPieceCount(i))` therefore grows by 6 on every `runCompactionPasses` call
regardless of what JOIN did, which is what caused `testJoinMergesAdjacentPiecesWithoutWritingAnything`'s
first failed revision to report "piece count went UP". The test's `pieceCount` helper now sums
`getPieceCount(i) - 1`, which is 0 for every ordinary partition and immune to how many of them
`runCompactionPasses` adds.

**D5 - JOIN and REWRITE need no `_cv` work at all, because this branch's `ColumnVersionWriter` records
are keyed by `partitionTimestamp` alone, never by `nameTxn` or by piece.** The reference's design
(`materialiseSharedColumnRecords`, `getSharedColumnTop`, a `columnVersionWriter.squashPartition` call per
folded or removed piece) exists because ITS `_cv` can carry one record per PIECE, and folding or
re-rooting pieces can leave a column-top record pointing at a piece that no longer exists. Here, a
composite partition's column top is one property of the DIRECTORY as a whole
(`ColumnVersionReader.getColumnTopByIndexOrDefault(recordIndex, partitionTimestamp, ...)`, looked up once
per `FrameColumn`, shared by every piece in that partition's own array - see COMPOSITE_PARTITION_STATE.md,
"Column tops belong to the column"). JOIN never touches `_cv`: it only rewrites `PartitionGeometry`'s own
piece array (`beginUpdate`/`addPiece`/`commitUpdate`-or-`abandonUpdate`/`publish`), and the folder's single
`_cv` record needs no change because nothing about which FILE ROWS hold which column changed. REWRITE
changes `nameTxn` but not `partitionTimestamp`, so the SAME `_cv` record still applies to the new
directory unchanged - and the correct NEW column top (rows below which the target has no data) is rebuilt
for free as a side effect of copying pieces in `tsLo` order through `FrameAlgebra.append`, whose existing
`nullPaddingRowCount`/`addTop`/`appendNulls` logic (in `FrameAlgebra`'s private `append` helper) already
handles "some of what I'm about to append predates this column" correctly for ANY append, not just a
compaction-shaped one. This was verified empirically: `testWasteRatioTriggerReclaimsDeadRows` compacts a
table that has taken no `ADD COLUMN` at all, so it could not have caught a column-top bug either way, but
`O3PartitionPreSplitTest` and `O3CompositePartitionTest` (which do exercise late-added and var-size
columns through the ordinary composite write path that `FrameAlgebra.append` also serves) stayed green
throughout, and no compaction test's row-content assertion (the `fingerprintOfDay` UNION-free per-row
Java-side check - see D6) ever caught a wrong value.

**D6 - a genuine, pre-existing bug, found and reproduced but NOT fixed: a vectorized SQL aggregate (e.g.
`sum(i)`) over a composite partition can read wrong data or SIGSEGV the JVM.** Found while writing
`testWasteRatioTriggerReclaimsDeadRows`'s oracle: `select coalesce(sum(i), 0) from x where ts in 'day'`
against a partition this branch's OWN merge-append had already made composite (via 8 repeated overlapping
backdates, no compaction involved at that point at all) returned `1071249267900`, where hand-computed
arithmetic and this port's own REWRITE both agree on `1600821900`. A second, more aggressively composited
fixture (same shape, no compaction code in the tree - reproduced against a `git stash`ed clean baseline)
SIGSEGVs the JVM inside `sumInt_Vanilla`, called from `AsyncGroupByNotKeyedRecordCursorFactory.aggregateVect`.
Proven independent of this port's changes: it reproduces identically with every file this port touches
reverted via `git stash`, and the corrupted "before" snapshot in `testWasteRatioTriggerReclaimsDeadRows`
was captured BEFORE `runCompactionPasses` ever runs (compaction cannot even select the day in question at
that point - it is the table's only, and therefore last, partition, which Sec.4's last-partition exclusion
always skips). The likely mechanism: this branch's page-frame-per-piece read path
(COMPOSITE_PARTITION_STATE.md item 4, "BUILT, untested against a real composite partition") was exercised
by `O3CompositePartitionTest`'s row-level UNION-ALL comparisons but never by a vectorized/async aggregate,
which appears to size its scan off `E` (physical rows, dead space included) rather than off the live piece
boundaries. Out of scope for this port - it is a defect in the pre-existing composite READ path, not in
compaction - but it is a real, user-reachable hazard the moment `cairo.o3.partition.merge.append.enabled`
and any `sum`/`avg`/similar aggregate meet in the same query, with or without compaction. Worked around in
`O3PartitionCompactionTest.fingerprintOfDay`, which now sums `i` by walking a plain row cursor
(`RecordCursor.hasNext()`/`getRecord().getInt()`) instead of using SQL `sum()` - the ordinary per-piece
frame cursor path, which IS covered by `O3CompositePartitionTest` and stayed correct throughout. Flagged
here in the same spirit as COMPOSITE_PARTITION_STATE.md's own "found, not fixed" sections (25, 26) -
someone should pick this up before `cairo.o3.partition.merge.append.enabled` ships wired to any query path
that uses vectorized aggregation.

**D7 - the `ColumnVersionWriter.squashPartition` `hasChanges` bug (reference's D5) is real here too, fixed,
but unreachable from this port's own code.** Checked per the task's explicit instruction: this branch's
`squashPartition(long targetPartitionTimestamp, long sourcePartitionTimestamp)` has the identical shape as
the reference's pre-fix version - the loop that reassigns a `COL_TOP_DEFAULT_PARTITION` marker away from
`sourcePartitionTimestamp` does not set `hasChanges`, so a source partition carrying only that marker (no
explicit per-column records) leaves `commit()` a no-op and the marker stale on disk. Fixed with the same
one-line addition the reference made. It is not reachable from JOIN or REWRITE (per D5, neither calls
`squashPartition` at all in this model), but IS reachable from the pre-existing classic-split
`squashSplitPartitions` path (`TableWriter.java:13826` calls it), which this port did not otherwise touch -
so the fix is a real, if narrow, correctness improvement independent of compaction.

**D8 - `Numbers.parseLongSize` stopped at `G`, same as the reference's D9.** The acceptance test's `1T`
"effectively infinite" setting needed the same `T`/`t` case added, symmetric with `K`/`M`/`G`. Affects
every byte-valued setting, not just compaction's.

**D9 - no `isRecordTimestampFree` / free-destination-timestamp check is needed for REWRITE.** The
reference needs one because a hardlink-split logical partition can have several folders, and
`updatePartitionSizeAndTxnByRawIndex`-style re-rooting could in principle pick a destination timestamp
another folder of a DIFFERENT logical partition already owns. In this branch `attachedPartitions` holds
exactly one entry per partition with a unique `partitionTimestamp` by construction (this branch has no
hardlink splits under merge-append), and REWRITE only ever bumps that SAME entry's `nameTxn` - the exact
convention the pre-existing classic-split squash code already uses
(`txWriter.updatePartitionSizeAndTxnByRawIndex(...)` in `squashSplitPartitions`'s `copyTargetFrame` branch).
No new directory-naming collision is possible.

**D10 - REWRITE never has to decline for "pieces not one contiguous run".** The reference declines when a
folder's live pieces are not adjacent in the GLOBAL piece list (they can be interleaved with an outsider
folder's pieces after a fresh-directory merge of a middle piece). In this branch pieces never leave their
own partition's array - there is no global list to interleave with - so every live piece of a partition is
unconditionally copied, in ascending `tsLo` (ordinal) order, into the one fresh directory REWRITE opens.

## MOVE-TAIL: a second pass

Added after the rest of this document, once JOIN/REWRITE had been in the tree long enough to see the
common shape merge-append actually produces: a large, untouched cold front and one small hot piece near
the tail, recopied wholesale on every REWRITE. This section is the record of that pass; it does not
revise anything above except where noted.

**The reference's mechanism does not apply here at all.** The reference's MOVE-TAIL is built on a
hardlink-donor `partitionTop` scheme (see the ENTERPRISE branch's `zerocopy_split_plan.md`) this branch
never ported (`PARTITION_COMPACTION_state.md`'s own opening paragraph: "There is no hardlink split and no
'partition top'"). Reading the reference's own MOVE-TAIL section closely, though, its actual mechanism is
simpler than that: it copies the tail into a **plain new folder** and leaves the front's folder completely
alone, unchanged - no hardlink, no shared bytes. That is exactly this branch's pre-existing **classic
split** shape (`TxWriter.insertPartition`, already used by the ordinary, merge-append-independent O3
mid-partition-split feature), so MOVE-TAIL here is ported onto THAT instead: `TableWriter.moveTailToFreshPartition`
copies only the tail pieces (ordinal 1 and up, once JOIN has run) into a brand new sibling
`attachedPartitions` entry, floored at the first tail piece's own `tsLo`; the front keeps its `nameTxn`,
its files, its `_cv` records and its `E` completely untouched - only its live-row total and its geometry
(now a single piece at row 0) change.

**MAKE-PLAIN and TRIM-FILES were deliberately not ported, by explicit choice, not oversight.** Both exist
in the reference to shrink the front's files *in place*, behind two reader-checked waits. This port skips
both: after MOVE-TAIL, the front is left exactly as REWRITE would have found it before MOVE-TAIL ever ran
- one piece at row 0, `E` above it - so it is STILL a normal compaction candidate, and its own remaining
dead space (visible via `deadRows` on `table_partitions()`) is reclaimed by an ordinary future REWRITE if
and when it separately crosses a threshold again, which by then is far cheaper (only the now much smaller
front needs copying). No new reader-safety machinery was needed for this, because nothing about the front
changed at all - the reader-safety argument `compactPhysicalPartition`'s own javadoc already makes
("nothing here writes below `E`... which is why none of it needs a reader check") continues to hold
unmodified for MOVE-TAIL.

**Eligibility**, checked in `moveTailToFreshPartition` after JOIN has already run: at least two pieces; the
first piece already sits at row 0 (otherwise the whole partition was relocated wholesale on some earlier
merge-append and there is no clean front left to preserve - matches the reference's own "first piece does
not start at row 0 -> REWRITE" rule); at least one live tail row; and the front's share of live rows is at
least `cairo.partition.compaction.prefix.min.percent` (new setting, default `50`, the one setting this
pass added beyond the eleven already in the tree). The caller (`compactPhysicalPartition`, via a new
`allowMoveTail` parameter) also excludes it entirely for the AGE rule (an idle partition will not be
written to again, so there is no future write worth sparing a clean front for) and for
`compactPartitionForConversion` (parquet conversion needs `partitionIndex` ITSELF to end up plain; leaving
a new sibling behind as a MOVE-TAIL side effect is the wrong shape for that caller).

**Total dead rows can go UP right after a MOVE-TAIL, and that is correct, not a bug.** The moved rows'
old physical location becomes dead the instant a fresh copy exists in the new sibling partition, on top of
whatever was already dead in the front before. `O3PartitionCompactionTest#testMoveTailCopiesTheTailNotTheWholePartition`
asserts this precisely: `deadRowsOfDay` after equals `deadRowsOfDay` before PLUS the rows written, never
zero. The reference's own table (§5) says the same thing plainly - MOVE-TAIL's "disk given back" column is
"none".

**Fixture correction beyond the usual third-backdate-round fix.** This test's original fixture (churn near
`23:00:00` against a `1s`-per-row, `20_000`-row day) does not actually reach `23:00:00` at all - the day
only spans about 5.5 hours at that row spacing - so the repeated "backdate" was really an in-order tail
append with no pre-split in play, and every merge-append relocated the WHOLE partition (matching this
file's own earlier, now superseded, note about that fixture). Fixed by moving the churn to `05:00:00`
(genuinely near the end of the actual 5.5-hour span) and adding a low `cairo.o3.partition.split.min.size`
(`512`) plus raised split-count budgets, so the pre-split genuinely isolates the repeated stride into its
own piece and a stable front survives. The waste-ratio rule also could not be what selects this partition
any more once the front is genuinely large: `dead > ratio*live` cannot cross `1` when dead is one small
piece against a ~20,000-row front, so the fixture now uses the piece-count rule instead
(`max.pieces=2`, `dead.min.size=1T` to keep the ratio rule out of the way) - matching the shape
`testPieceCountTriggerReducesTheNumberOfPieces` already uses for the same reason.

**Open question 1 (below) is now answered, deliberately, by construction.** MOVE-TAIL is the first thing
in this port that makes "a partition IS the logical unit" false on purpose: after it runs, a logical
partition legitimately owns two `attachedPartitions` entries, exactly the classic-split shape that
question worried about. It is safe here because the split is total and immediate - the front and tail's
piece/row ranges are disjoint by construction, `insertPartition` picks a floor timestamp that cannot
collide (a partition's pieces never span more than its own `attachedPartitions` entry), and
`PartitionCompactionPolicy` treats the resulting tail as an ordinary, non-composite partition (skipped by
its very first per-partition gate) and the front as an ordinary composite partition like any other. The
piece-count-under-counts-across-a-split concern the question raised does not arise from MOVE-TAIL itself,
since MOVE-TAIL only ever produces at most one extra sibling per compacted partition, not an
externally-driven classic split interacting with an existing composite one - that combination (a
classic-split partition that ALSO becomes independently composite via merge-append) remains untraced, as
the question already said.

## Decisions taken

- **Compaction never touches `minSplitPartitionTimestamp` or any of the classic-split (`squashSplitPartitions`)
  bookkeeping.** That machinery is orthoginal to composite partitions on this branch (master's own O3
  partition-split feature, unrelated to `cairo.o3.partition.merge.append.enabled`); JOIN and REWRITE only
  ever touch one `attachedPartitions` entry's own fields and this branch's `PartitionGeometry`. MOVE-TAIL
  (added in a second pass - see above) is the one exception: it deliberately DOES insert a new
  `attachedPartitions` entry, reusing `TxWriter.insertPartition` the same way the classic-split feature
  itself does, but still touches no classic-split BOOKKEEPING (`minSplitPartitionTimestamp`,
  `squashSplitPartitions`'s own state) - only the plain insertion primitive.
- **The piece-count rule does not sum pieces across a "logical partition"** the way the reference's
  `logicalPartitionPieceCount` does. In this branch a partition IS the logical unit (mostly - see the open
  question below about classic splits), so `pieces > maxPieces` on the one partition being considered is
  the whole check.
- **Cooldown and decline-backoff are keyed on plain `partitionTimestamp`**, not on a `(dirTs, nameTxn)`
  pair. This is deliberately the reference's own D11 fix (key the cooldown on the logical unit, because a
  REWRITE re-roots into a fresh `nameTxn`), applied from the start rather than discovered as a bug, because
  `partitionTimestamp` already IS this branch's logical unit and never changes under REWRITE.
- **`foldFoldableFolders` retries the same partition index after a successful fold** (only advancing past
  it after a fold FAILS to find anything to merge), rather than restarting the whole scan from index 0 the
  way the reference does after every fold. The reference's restart exists because ITS fold rebuilds a
  resident folder list that a fold can reorder; nothing here ever reorders `attachedPartitions` (JOIN and
  REWRITE never insert or remove an entry), so partition indices stay stable across a whole `housekeep`
  call and a plain retry-or-advance is enough.
- **The last-partition exclusion is kept, unlifted**, exactly matching the reference's own decision not to
  apply its D12 in this pass (the fix was documented as an open question there, not applied). This port
  inherits the same limitation for the same reason: lifting it needs the `fixedRowCount`/`transientRowCount`
  adjustment, `closeActivePartition`/`openLastPartition` handling this port did not attempt.
- **`O3SplitWriteAmplificationBenchTest`'s compaction scenario was not ported.** Lower priority per the
  porting brief, and none of this branch's own benches were adapted for it in the time available.

## Open questions

1. **Interaction with the classic (non-merge-append) partition split.** This branch's own O3
   partition-split feature (`squashSplitPartitions`, `TableWriter:13674+`) is independent of
   `cairo.o3.partition.merge.append.enabled` and can, in principle, split ONE logical partition into
   SEVERAL `attachedPartitions` entries sharing a floor timestamp - the one scenario where this port's
   "a partition IS the logical unit" simplification (D1, D9, "Decisions taken") would not hold. Whether a
   partition can be BOTH a classic split AND independently composite under merge-append was not traced
   end to end, and no test exercises the combination. If it can happen, `PartitionCompactionPolicy`'s
   piece-count rule would under-count pieces across the split, and REWRITE's destination-naming argument
   (D9) would need re-examining.
2. **D6's composite-aggregate-read bug** needs an owner and a fix before `cairo.o3.partition.merge.append.enabled`
   is safe to combine with any vectorized aggregate query, with or without compaction turned on.
3. **Should the last-partition exclusion be lifted?** Same question the reference leaves open (its D12);
   not investigated further here.

## Not done, carried forward

- MAKE-PLAIN, TRIM-FILES (the rest of step 4, MOVE-TAIL now done - see above) and turning compaction on by
  default (step 6) - out of scope, matching the reference's own pass for the two still-unstarted parts.
- `O3SplitWriteAmplificationBenchTest`'s compaction scenario.
- A `PartitionGeometryFile`/`PartitionGeometry` round-trip unit test for `lastWriteMicros` specifically -
  it is exercised only indirectly, through `O3PartitionCompactionTest`'s age-rule test.
- Fuzz coverage with compaction on and thresholds low, held against the three suites the porting brief
  named as the ones that matter for this branch's composite-partition work
  (`O3PartitionPreSplitTest`, `O3CompositePartitionTest`, `O3CompositeMergeStrategyTest`) - these were run
  as ordinary (compaction-off) regression, not fuzzed with compaction on.

## Regression, as of this pass

Green, full class runs: `O3CompositePartitionTest` 9/9, `O3CompositeMergeStrategyTest` 16/16,
`TableWriterTest` 157/157, `TxnTest` 7/7, `ColumnVersionWriterTest` 15/15, `NumbersTest` 230/230,
`PropServerConfigurationTest` 103/103 (1 pre-existing skip), `DynamicPropServerConfigurationTest` 42/42,
`ShowPartitionsTest` 76/76, `ServerMainShowPartitionsTest` 4/4.

`ShowPartitionsTest` and `ServerMainShowPartitionsTest` needed their golden `SHOW PARTITIONS`/
`table_partitions()` expectation strings extended by the two new columns - 87 changed lines across both
files, mechanical (every existing row got `\t0\t` appended if attached, `\tnull\t` if not - a `LONG` NULL
prints as the word `null` in this test harness's TSV output, unlike the empty string a NULL `TIMESTAMP`
prints as, which is what `lastWriteTimestamp` always is in these tests since none of them use merge-append).

### MOVE-TAIL pass

Green, full class runs added for this pass: `O3PartitionCompactionTest` 7/10 (see "Test results" above for
the three still-red, all step-4-remainder or pre-existing), `O3CompositePartitionTest` and
`O3CompositeMergeStrategyTest` re-run clean, `O3PartitionPreSplitTest` 41/42 (the one red is the
pre-existing, proven-independent `testDedupDowngradeDoesNotStampMergeAppend` below, unchanged),
`TxnTest`/`TableWriterTest` clean, `ShowPartitionsTest`/`ServerMainShowPartitionsTest`/
`PropServerConfigurationTest`/`DynamicPropServerConfigurationTest` clean (the new `prefix.min.percent`
setting needed no golden-string changes - it adds no new `table_partitions()` column). Three
`WalWriterFuzzTest#testWalWriteFullRandom` runs (this branch's existing randomized compaction-budget fuzz
- see the fuzz test's own `setRndPartitionCompactionProperties`) came back clean; none happened to draw a
tight enough budget to actually exercise MOVE-TAIL in those three runs, so this is a crash/regression
smoke test for this pass, not a targeted MOVE-TAIL fuzz result - `O3PartitionCompactionTest`'s own
`testMoveTailCopiesTheTailNotTheWholePartition` is what pins the actual behaviour, deterministically.

## Known-red, not caused by this work

Both proven by an A/B against a `git stash`-clean baseline of this same tree: identical test name,
identical assertion, identical failure, with none of this port's changes present.

- `O3PartitionPreSplitTest.testDedupDowngradeDoesNotStampMergeAppend` - `AssertionError: the piece was
  rewritten in place instead of at the tail`.
- `O3PartitionPurgeTest.testCheckpointInProgressDefersPartitionRemovalWal` (both `MICRO` and `NANO`
  timestamp params) - `AssertionError` on `x.d` existing when it was expected not to.
