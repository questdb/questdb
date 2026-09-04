# Composite partitions, modelled on parquet partitions

A partition can hold several PIECES over ONE set of column files. A composite partition is the parquet
FILE; a piece is a ROW GROUP. New rows go at the tail, untouched pieces stay put, and the geometry lives
in a `_geometry` file opened only when a query or commit lands on that partition.

Behind `cairo.o3.partition.merge.append.enabled`. OFF in production, ON by default in tests.

## Storage

| Item | What it does |
|---|---|
| `_geometry` file | Append-only full snapshots: header + one `(tsLo, tsHi, rowOffset, rowCount)` per piece |
| `_txn` slot 3 | Composite flag + byte offset of the live geometry record |
| `PartitionGeometry` | Lazy resolver, reads `_geometry` on first touch of a partition |
| Physical rows | How far the files reach, dead space included. Live rows is the sum of piece rows |

## Write

| Item | What it does |
|---|---|
| Pre-split | Cuts a piece in two. No bytes move - two records over the same files |
| Transaction clustering | Picks where to cut from the block's cold gaps, without reading rows |
| Planner | Per piece: KEEP (nothing written), MERGE (piece + batch out at the tail), NEW_PIECE |
| Executor | Writes at the tail only, so one writer and any number of readers share the files |
| `FrameAlgebra.merge` | Fixed and var-size columns, plus column-top-aware kernels |
| Dedup | Deduplicating merge index; an all-duplicate commit with identical values writes nothing |
| Active partition | Left closed like parquet; when mapped, positioned at physical rows so a close trims nothing live |
| Non-WAL | Never founds one, but still handles a partition that is already composite |
| WAL lag | None, and no fast append through it either (`tryFastAppendInOrderBlock`): every commit is a merge-append |
| Replace commits | Fully implemented, including the table's own last partition: a piece fully inside the declared range is dropped (KEEP) or superseded (MERGE, rewritten to NEW_PIECE) |

## Read

| Item | What it does |
|---|---|
| Page frames | Maps `[0, physicalRows)`, one frame per piece |
| `CompositeTimestampFinder` | Interval scans in DIRECTORY row order - file order is not timestamp order |
| Indexes | Build, seal and rebuild cover the directory's whole `[columnTop, physicalRows)`, not one piece |
| Covering sidecar | Accepts unsorted timestamps, which a relocated piece produces |
| Covering index growth | Every write lands at the tail, so a covering POSTING index grows by one generation per commit, appended after the write with the rows' covered values (`O3PartitionJob.publishCoveredIndexesForAppend`) - the O3 worker does it for a composite plan, the seal sweep for the last partition's in-place append. Never a rebuild of the whole sidecar; entries of rows a MERGE or DROP retired stay, unreachable, as they would after a rebuild |

## Other

| Item | What it does |
|---|---|
| `ALTER COLUMN TYPE` | Converts the file's whole extent |
| `table_partitions()` | A composite directory reports its summed rows and disk size |
| Logging | One INFO line per merge-append: pieces before/after, keep/merge/newPiece, live and physical rows, dead % |

## Compaction of dead space

All four moves are built and on by default - see `PARTITION_COMPACTION.md` for the rules and
`PARTITION_COMPACTION_JOB_DESIGN.md` for the background job that drives them off the writer thread.

| Move | What it does |
|---|---|
| JOIN | Folds file-adjacent pieces. Copies nothing |
| MOVE-TAIL | Copies only the messy tail pieces to a fresh sibling partition |
| MAKE-PLAIN | Drops the dead space above a single front piece, once no reader can still resolve the old record |
| REWRITE | Copies every live row into a fresh directory. The fallback for everything else |

## Not done

- A floor on very small pieces (attempted, crashes)
- Piece-wise parquet conversion. A composite partition is folded to plain first
  (`TableWriter.compactPartitionToPlain`), as DETACH and SQUASH also do

## Downgrade caveat

Bit 61 of the `_txn` slot-3 word carries the composite flag, with no `META_FORMAT_MINOR_VERSION`
bump - the same way the parquet flag was introduced. An older binary therefore opens a table with
composite partitions without complaint and reads each one as flat `[0, liveRows)`, and its
checkpoint scrub zeroes the geometry pointer permanently. Turning the flag back OFF on a current
binary is safe: the writer folds a composite partition back to plain before any legacy path reads
it (`TableWriter.processO3Block`).
