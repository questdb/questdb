# Purging retired `_geometry` generations

## The gap

`PartitionGeometry.publish` rotates to `_geometry.<gen+1>` once a record would push the current file past
`PartitionGeometryFile.MAX_FILE_SIZE`. The rotation stays inside the SAME partition directory, so the ordinary
partition purge - the thing that waits on `TxnScoreboard` and on a running checkpoint - never sees the retired
generation. Nothing deletes it: `TableUtils.PARTITION_GEOMETRY_FILE_NAME` has exactly two uses, the open-RW in
`PartitionGeometryFile.append` and the open-RO in `PartitionGeometryFile.read`. Retired generations accumulate for
the life of the directory.

The file cannot simply be deleted at rotation time either. A reader resolves its geometry record lazily, out of the
generation its own `_txn` snapshot names, and a checkpoint's copied `_txn` names one too. `CompositeGeometryPurgeTest`
pins both across a rotation and asserts the old file survives.

So the retired generation goes to the column purge queue, which is already built to retry until the readers are
gone.

## Who retires a generation

Every site that moves a partition's geometry ref while KEEPING its directory:

| Site | Transition |
| --- | --- |
| `o3ConsumePartitionUpdateSink` (O3 commit update) | generation G -> G+1 |
| `foldContiguousPieces` (JOIN) | G -> G+1, or G -> `NO_GEOMETRY_REF` when the fold collapses the partition |
| `moveTailToFreshPartition` (MOVE-TAIL front) | generation G -> G+1 |
| `makePartitionPlain` (MAKE-PLAIN) | generation G -> `NO_GEOMETRY_REF` |
| `squashSplitPartitions` (squash target) | already plain in practice, routed through the helper anyway |

The sites that write a NEW directory - `assembleFreshPartitionVersion`, `swapCompactedCompositePartition` - retire
nothing here: the whole directory goes through `safeDeletePartitionDir`, which already waits properly.

## Queue entry

Reuse `PurgingOperator` / `ColumnPurgeTask` / `ColumnPurgeJob` unchanged in shape. One entry per retired generation:

- `columnIndex` = new sentinel `PurgingOperator.GEOMETRY_COLUMN_INDEX` (`-2`), so geometry entries group into a task
  of their own and never collide with a real column's index.
- `columnName` = `TableUtils.PARTITION_GEOMETRY_FILE_NAME` (`_geometry`).
- `columnType` = `ColumnType.NULL` - the marker both the operator and the job branch on. No real column ever carries
  it, and it survives the round trip through the purge log's `columnType int`.
- `indexType` = `IndexType.NONE`.
- `columnVersion` = the retired GENERATION, not a txn. It is the file's name suffix.
- `partitionTimestamp`, `partitionNameTxn` = the directory the file lives in.
- task-level `updateTxn` = the txn of the commit that rotated.

Nothing in the purge log's schema or in `ColumnPurgeJob`'s restart replay needs changing: `getColumnIndexQuiet`
already returns -1 for a name that is not a column and leaves `indexType` at its default.

## Writer side

Async only. A rotation is rare enough that nothing is gained by trying to delete inline, and the purge job is the
only thing that knows how to wait out the readers still resolving those records.

1. `PartitionGeometryFile.geometryFileName` is package-private rather than private, so `ColumnPurgeOperator` names
   the file exactly the way `append` and `read` do.
2. `PurgingOperator.purgeColumnVersionAsync` is a public static taking `Log` and `MessageBus`, so a caller with
   nothing else to purge gets at the queue without the operator's per-column bookkeeping. The instance method
   delegates to it.
3. `TableWriter.setGeometryRefRetiringGenerations(partitionTimestamp, newRef)` replaces
   `txWriter.setPartitionGeometryRef` at every site that keeps the directory: JOIN (both branches), MOVE-TAIL's
   front, MAKE-PLAIN, and the squash target. It reads the committed ref first and appends one
   `retiredGeometryGenerations` entry per generation in `[oldGeneration, newGeneration)` -
   `[oldGeneration, oldGeneration + 1)` when the new ref is `NO_GEOMETRY_REF`. Everything below `oldGeneration` was
   queued by the rotation that moved past it, so a move only ever queues what it retires itself.
4. The O3 commit path cannot use that helper: slot 3 holds the geometry pointer, and several of the size updates
   that run before it in `o3ConsumePartitionUpdateSink` rewrite that word. It captures the committed ref and name
   txn where `partitionIndexRaw` is resolved - before any of them - and calls `retireGeometryGenerations` with them.
5. `commitTxWriter` and `commitTxWriterAndPublishPendingPostingSealPurges` publish the collected entries right after
   `txWriter.commit`, with the committed txn as `updateTxn`. `rollback` clears them: a rolled-back transaction's
   geometry refs never reached `_txn`, so the generations it meant to retire are still the current ones.

## Purge job side

In `ColumnPurgeOperator.purge0`, branch on `task.getColumnType() == ColumnType.NULL` before the existing
column-file logic:

- Path is `<partition dir>/_geometry.<columnVersion>`; skip the `dFile`/`iFile`/index probing entirely.
- Existence check as usual - a missing file means done.
- Reader check: **read the retired file's record at offset 0** and take its
  `PartitionGeometryFile.HEADER_OFFSET_WRITER_TXN_64`. That is the txn of the commit that STARTED this generation,
  so the generation is live for readers in `[firstWriterTxn, updateTxn)`. Delete only when
  `txnScoreboard.isRangeAvailable(firstWriterTxn, updateTxn)` and no checkpoint is in progress; otherwise leave the
  entry incomplete and let the job retry.
  - This is why the generation number alone is enough in `columnVersion`: the txn watermark the scoreboard needs is
    in the file, not in the queue entry, so the purge log's four columns stay as they are.
  - A record whose magic or checksum does not verify cannot be resolved by any reader, so log and delete it.
- The existing `engine.getCheckpointStatus().isInProgress()` guard and the read-only-partition guard apply
  unchanged.

## Edge cases

- **Rotation and a new directory in the same commit.** Cannot happen: `shouldAssembleFreshPartitionVersion` routes
  the commit away from `publish` entirely, so either the directory moves or the generation does.
- **Generation 0.** Its offset-0 record is written by the partition's first composite commit, so the watermark rule
  holds for it too.
- **Sparse files.** A retired generation is up to 100MB of mostly-hole. `read` at offset 0 touches one page.
- **Table dropped / truncated between queueing and purging.** Already handled by `openScoreboardAndTxn`'s table-id
  and truncate-version checks.
- **Restart with entries in flight.** Replayed from the purge log like any other task; the geometry branch is keyed
  on `columnType`, which the log stores.

## Tests

`CompositeGeometryPurgeTest` plants a record 8 bytes short of `MAX_FILE_SIZE` (the same fake-up as
`O3PartitionPreSplitTest.plantFakeGeometryRecordNearFileLimit`), commits over it, and asserts the roll happened in
place - ref generation 0 -> 1, `_geometry.1` written, `nameTxn` unchanged. On top of that:

- a pinned `TableReader` across the rotation: the old generation stays on disk and the reader still resolves its
  pieces out of it
- a checkpoint across the rotation: same
- two purge passes while pinned leave the retired generation alone; one pass after the pin is released removes it
- the live generation survives the purge
