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
| Replace commits | Fully implemented, including the table's own last partition: a piece fully inside the declared range is dropped (KEEP) or superseded (MERGE, rewritten to NEW_PIECE) |

## Read

| Item | What it does |
|---|---|
| Page frames | Maps `[0, physicalRows)`, one frame per piece |
| `CompositeTimestampFinder` | Interval scans in DIRECTORY row order - file order is not timestamp order |
| Indexes | Build, seal and rebuild cover the directory's whole `[columnTop, physicalRows)`, not one piece |
| Covering sidecar | Accepts unsorted timestamps, which a relocated piece produces |

## Other

| Item | What it does |
|---|---|
| `ALTER COLUMN TYPE` | Converts the file's whole extent |
| `table_partitions()` | A composite directory reports its summed rows and disk size |
| Logging | One INFO line per merge-append: pieces before/after, keep/merge/newPiece, live and physical rows, dead % |

## Not done

- Squash and purge over a composite partition
- Compaction of dead space: JOIN (fold file-adjacent pieces), MOVE-TAIL (copy only the tail to a fresh
  sibling) and REWRITE (copy live rows to a fresh directory) are built and always on - see
  PARTITION_COMPACTION_state.md. MAKE-PLAIN and TRIM-FILES (an instalment plan that reclaims space without
  copying a whole partition) are still not built; REWRITE stands in for both.
- A floor on very small pieces (attempted, crashes)
- Parquet conversion (out of scope)
