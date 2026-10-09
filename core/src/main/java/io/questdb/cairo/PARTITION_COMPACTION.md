# Compaction: reclaiming wasted space in composite partitions

## The problem

Merge-append never overwrites. When it rewrites a piece it appends the new copy at the end of the folder's
column files and abandons the old copy where it lay. A folder's size grows with how many times it has been
merged, not with how many rows it holds. Compaction reclaims that.

## Vocabulary

- **folder** - one physical partition: one directory, one `(dirTs, nameTxn)`, one set of column files, one
  geometry chain. A **logical partition** (one day, say) is the run of folders sharing a start: the main
  folder plus the siblings a MOVE-TAIL or an O3 split left (`2024-01-01T050000-...`).
- **piece** - a window onto part of a folder's files. Several pieces can share one folder.
- **`E`** - how far the folder's files have ever been written, in rows. Only goes up until MAKE-PLAIN.
- **dead rows** - rows in the files no piece points at any more: `E` minus live rows.

## Where compaction runs

1. **On commit, inside the partition task** - MOVE-TAIL only (`O3PartitionJob.moveTailToFreshPartition`).
   The task that applies a commit to a composite folder decides, off the plan and dedup forecast it already
   built, whether to leave the untouched prefix behind and write the tail pieces plus the commit's rows ONCE
   into a fresh sibling at the first tail timestamp. It fires when the plan would leave the folder with dead
   rows above `move.tail.dead.rows.percent` of live (default 10%) or more than `move.tail.piece.threshold`
   pieces (default 1000), dead rows above `cairo.o3.partition.split.min.size`, and a prefix of leading
   untouched pieces ending below everything the commit and the queued transactions write, more than
   `move.tail.prefix.multiple` (default 2) times the tail plus incoming rows. The sink reports it as a split;
   the writer inserts the sibling and republishes the prefix's geometry. A valid move wins over a fresh-version
   rewrite; only geometry-generation exhaustion overrides it. This is the one path that acts on the active
   partition.
2. **On commit, on the writer thread** (`runCompaction`, after the split squash) - `PartitionCompactionPolicy`
   picks one folder per commit and `compactPhysicalPartition` runs one move on it. Non-WAL tables have only
   this path.
3. **The background sweep** (`PartitionCompactionScanJob`, WAL tables only) - revisits partitions that went
   cold with waste nobody will commit into again. `PARTITION_COMPACTION_JOB.md`.

## When the per-commit policy picks a folder

Candidates are composite folders (more than one piece, or one piece not starting at row 0, or `E` above the
live rows) that are not the active logical partition - compacting it would fight the writer's append state.

| reason | fires when |
|---|---|
| **waste ratio** | dead rows exceed `dead.rows.ratio` x live rows AND `dead.min.size` |
| **piece count** | pieces exceed `max(piece.threshold, liveRows / avg.rows.piece.lim)` - a flat floor, scaled up for large folders |
| **age** | idle past `idle.timeout` and still wasteful or multi-piece |
| **table pressure** | the table's dead rows cross `table.dead.threshold.percent` of its live rows AND `table.dead.threshold` bytes (or `table.dead.trigger` bytes alone); oldest wasteful folder first, until `table.dead.stop.percent` |

The policy keeps composite folders in a primitive max-heap keyed by a packed priority (waste-ratio
candidates first, then piece count, then age/pressure oldest first) and maintains the table's dead-row total
incrementally, so no commit scans the partition table. A declined folder backs off exponentially
(`decline.backoff.*`). A folder written in the last `hot.commits` commits is reported hot; under table
pressure REWRITE is withheld from it (`COMPACTION_SKIPPED_HOT`, no backoff) while the cheaper moves stay
available.

When nothing qualifies, the commit still JOINs any foldable folder and MAKE-PLAINs any folder in MAKE-PLAIN
shape (`foldFoldableFolders`, `makePlainFoldableFolders`).

## The moves, cheapest first

| move | what it does | reader wait |
|---|---|---|
| **JOIN** | merges pieces adjacent in both the piece list and the files into one; copies nothing | no |
| **MOVE-TAIL** | copies the tail pieces into a new sibling folder; the prefix keeps its files, `E`, name txn and its own pieces - holes and nonzero offsets included - under a shorter geometry | no |
| **MAKE-PLAIN** | a folder reduced to one piece at row 0: lowers `E` to the row count, so it stops being composite, then **TRIM-FILES** shortens every column file to that size in the same call | yes, see below |
| **SQUASH** | for AGE and TABLE-PRESSURE reasons: folds a cold logical partition's siblings into one folder (`squashColdLogicalPartition`). The commit-time `squashSplitPartitions` is the only control on folder count: nothing gates a split, and cold siblings are folded back to `cairo.o3.partition.max.splits` as they cool past `hot.commits` | no |
| **REWRITE** | copies every live row into a fresh folder, retires the old one | no (the delete has its own check) |

`compactPhysicalPartition` tries them in that order: JOIN always first, then MOVE-TAIL when a majority cold
prefix exists (`PartitionCompactionPolicy.moveTailCut`, preferring a prefix that tiles from row 0 so
MAKE-PLAIN can finish it for free), then MAKE-PLAIN when the shape allows, then SQUASH, then REWRITE unless
the folder is hot. A MOVE-TAIL's prefix is finished in the same commit when it can be (`finishMovedTailPrefix`:
MAKE-PLAIN, or REWRITE if still wasteful); a fragmented prefix otherwise stays composite for a later JOIN,
SQUASH or REWRITE.

Pieces can only be JOINed or copied together if they are neighbours in the folder's piece list.

## Reader safety

No move writes below `E`. Live rows are always copied to a location nothing has pointed at, and only
afterwards is `E` lowered or a file shortened - bookkeeping gated on a reader check.

MAKE-PLAIN waits until no reader is pinned below the transaction that published the current one-piece record,
because readers below it still resolve the pieces MOVE-TAIL removed, which sit above row 0. TRIM-FILES needs
no wait of its own: a `TableReader` maps a folder's files only as far as its highest live piece reaches
(`PartitionGeometry#getLiveFileExtent`, `TableReader#mappedRowCount`), never to `E`, so once MAKE-PLAIN's
check has cleared every live and arriving reader maps exactly the row count TRIM-FILES cuts to.
`isRangeAvailable` pushes the scoreboard's max txn, so a reader arriving after the check sees the new shape.

That is a claim about `TableReader` only. Writer-side consumers do size to `E` (squash, MOVE-TAIL, REWRITE,
`O3PartitionJob`, `RebuildColumnBase`, `ConvertOperatorImpl`, `TableSnapshotRestore`, via
`getPartitionPhysicalRowCount`); they are safe because they run on the writer thread or hold the scoreboard
txn they were dispatched at. A new consumer that sizes to `E` and runs concurrently with TRIM-FILES needs its
own check.

**Checkpoint.** A running CHECKPOINT is not a reader: backup sizes files by the `E` in its manifest and reads
them from the live root. MAKE-PLAIN therefore declines while a checkpoint is in progress - a folder that cannot
be trimmed must not be recorded as plain, or its dead bytes are reported by nothing and reclaimed by nothing.
The ordinary decline/backoff brings it back afterwards.
