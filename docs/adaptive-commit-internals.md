# Adaptive commit: how it works

The developer companion to [`adaptive-commit-mode.md`](adaptive-commit-mode.md). It follows a commit from the client
to the disk and back after a crash, names every file the mode adds or changes, and says which code does what.
Methods are named rather than line numbers, which drift.

`W` is the group-commit window, `cairo.adaptive.commit.group.window`.

## 1. The idea

Adaptive keeps two things apart that `sync` treats alike:

- **The WAL is the truth.** A commit is acknowledged once its WAL is on stable storage (with `W` = 0), or once the
  writer's own WAL files are and the shared sequencer flush is scheduled within `W`.
- **The table files are a rebuildable copy.** The apply job writes them lazily, as in `nosync`, with no flush per
  commit.
- **A save point bounds the rebuild.** Each table periodically makes the table data durable and writes a durable
  copy of its three small state files (`_txn`, `_cv`, `_meta`) into a save point file. After a crash, start-up
  recovery writes that copy back over the live files, and the ordinary WAL apply replays every transaction after
  it. Everything the replay needs is kept until a newer save point replaces it: the table keeps its last two save
  points, so that a start can fall back to the older one when the newer one is damaged, and the WAL after the older
  one and the partition versions either one names stay on disk.

A second, smaller part detects torn files instead of reading them as data: a checksum in `_txn`, a trailer in
`_cv`, and a checksum in every record of the version 2 sequencer log.

Adaptive covers WAL tables only. A non-WAL table has nothing to replay and behaves as in `nosync`.

### Two frontiers

| Frontier | Moves when | Read by |
|---|---|---|
| `localDurableSeqTxn` (the durable frontier) | a WAL flush that covers the transaction completes | the QWP `local` acknowledgement, the Enterprise uploader gate, `wal_tables()` |
| `durableEpochSeqTxn` (the save point) | a save point is published | the WAL purge floor (with the older generation's header on disk), the close-time save point, `wal_tables()` |

Both live in the table's `SeqTxnTracker`. Readers see applied data as soon as it is applied, durable or not: the
frontiers bound what a crash can take, not what a query sees.

### One mode per instance

`cairo.commit.mode` is read once at start, so every table answers to the same mode and no commit straddles a
change. `CommitMode` holds the predicates the code uses:

- `appliesColumnSync(mode)`: true for `sync` and `async` only. It gates the apply path's flushes of columns,
  indexes, symbol maps, `_cv` and `_txn`. `adaptive` is excluded: that is the lazy apply.
- `isPowerLossDurable(mode)`: true for `sync` and `adaptive`. It gates the directory fsyncs that make names durable,
  the schema-file swap fsyncs, and the engine stop on a failed flush. `async` and `nosync` keep their previous
  behaviour.
- `structuralCommitMode(mode)`: maps `adaptive` to `sync` for the one-shot writers outside a table writer (the
  conversion, REBASE staging, checkpoint restore).

Directory fsyncs follow the promise: a mode takes one only where it promises that the named file survives a power
loss. Under `adaptive` the save point flush makes the table directory's entries durable, and recovery recreates any
later ones when it replays the WAL.

## 2. The commit path

`WalWriter.commit` appends the event record, then `syncIfRequired`, then asks the sequencer for a transaction
number, then updates the durable frontier.

`WalWriter.syncIfRequired0`, per mode:

| Mode | WAL column files | `_event`, `_event.i` | Before sequencing |
|---|---|---|---|
| `nosync` | nothing | nothing | nothing |
| `async` | `msync(MS_ASYNC)` | `msync(MS_ASYNC)` | nothing |
| `sync` | `msync(MS_SYNC)` | `msync(MS_SYNC)` | nothing |
| `adaptive` (any `W`) | `msync(MS_ASYNC)`, a write-back drain (`sync_file_range`, if `cairo.wal.commit.writeback.drain`), then a barrier per file | `msync(MS_ASYNC)`, drain, barrier | every private WAL byte is on the device |

The barrier is `Files.barrierFsync`: `fdatasync` on Linux and Windows, `F_BARRIERFSYNC` (ordering only) on macOS,
where the sequencer's `fdatasync` (`F_FULLFSYNC`) that follows makes everything ordered before it durable.

On Linux, an `adaptive` WAL column page that is full is released with `msync(MS_ASYNC)` instead of
`msync(MS_SYNC)` (`MemoryPMARImpl`): the commit's own barrier writes it back. macOS keeps `MS_SYNC` there.

Then the sequencer (`TableTransactionLogV2.sync0`, the same order in V1):

| Mode | Sequencer log |
|---|---|
| `nosync` | nothing |
| `async` | `msync(MS_ASYNC)` of the header |
| `sync` | `msync(MS_SYNC)` of the part file, then of the header |
| `adaptive`, `W` = 0 | `msync(MS_SYNC)`, then `fdatasync` of the part file, then of the header; the durable frontier moves on the commit thread |
| `adaptive`, `W` > 0 | `msync(MS_ASYNC)` of the part file and the header; the `fdatasync` is deferred to the batched flush |

The part file is always made durable before the header that names its record, so a durable `maxTxn` never points
at a record that is not on the disk.

**The batched flush** (`W` > 0). `recordPendingDurable` registers the commit in `WalGroupCommitFlushQueue`. When the
oldest pending commit is `W` old, `flushPendingDurable` runs `fdatasyncTxnLog` and `markWriterDurable`, which moves
the frontier to the highest transaction the flush covered with no older one still pending. When commits stop, the
`WalPurgeJob` pass flushes the idle tail (`forceDurableIfPending`), so the bound is `W` plus the shared worker's
sleep. A failed barrier fences the frontier for the life of the process, and the engine stops (section 9).

Because each writer's private WAL is durable before it takes a sequence number, one writer's sequencer flush can
never make durable a record whose data another writer has not flushed.

**Replays inside a commit.** When a concurrent schema change makes the sequencer answer `NO_TXN`, the writer replays
the metadata change into its pending commit. The two replays that rewrite the commit's own WAL files (rolling rows
into a new segment, and rewriting the event record for ADD COLUMN ... SYMBOL) flush what they rewrite themselves.

## 3. The apply path

`ApplyWal2TableJob` applies WAL transactions through `TableWriter`, as in every mode. `TableWriter.commit00` updates
the indexes, publishes the indexers, then, only where `appliesColumnSync` holds (`sync`, `async`), flushes columns
and symbol maps; then commits `_cv`, then `_txn` last, the pointer that makes the rows visible. `_cv` and `_txn`
use the same predicate, so under `adaptive` the pointers are as lazy as the data they point at: a durable `_txn`
naming columns that are not durable would be worse after a crash than rolling both back together.

Under `adaptive` the column memories also skip the `msync` when an append page is released (`applyLazy`).

A new partition directory is fsynced, with the table directory, only under `sync`. Under `adaptive` the save point
flush covers directory entries, and the replay recreates later ones.

## 4. The save point file

Each enrolled table has up to two generation files in its directory, `_epoch.manifest.0` and `_epoch.manifest.1`
(`DurableEpochManifest`). Each holds one whole save point: a 120-byte header, then byte-for-byte copies of the live
`_txn`, `_cv` and `_meta`, in that order. All integers are little-endian.

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `0x4D534B4843504F45` |
| 8 | 4 | format version, 3 |
| 12 | 4 | generation, 0 or 1 |
| 16 | 4 | table id |
| 20 | 4 | unused, 0 |
| 24 | 8 | seqTxn of the save point |
| 32 | 8 | txn of the save point |
| 40 | 8 | column version |
| 48 | 8 | length of the `_txn` section |
| 56 | 8 | checksum of the `_txn` section |
| 64 | 8 | length of the `_cv` section |
| 72 | 8 | checksum of the `_cv` section |
| 80 | 8 | length of the `_meta` section |
| 88 | 8 | checksum of the `_meta` section |
| 96 | 8 | metadata version |
| 104 | 8 | magic again |
| 112 | 8 | checksum of bytes 0 to 103 |
| 120 | | the `_txn`, `_cv` and `_meta` sections |

The checksums are the 64-bit hash `TableUtils.calculateCvAreaChecksum` (an xxh3 avalanche over the bytes).

A generation's header is read as one of four states:

- **absent**: no file;
- **unpublished**: shorter than a header, no magic, or an older format. A write that a crash interrupted before its
  header landed. Recovery deletes it;
- **damaged**: the magic is there but a field or the header checksum is wrong;
- **valid**: everything matches, including the table id from the table registry.

The **current save point** is the valid generation with the higher (seqTxn, txn). A table is **enrolled** when it has
one. A new save point is always written into the other generation, so the current one is never
touched while the next is written: a torn rewrite loses to the untouched file, and a complete one wins with its
higher cut. The other valid generation is the fallback: recovery restores it when the current one fails its
checksums or its content checks, so its WAL and its partition versions are kept until the next save point
overwrites it (section 9).

### Writing a generation

`DurableEpochManifest.write`, with the table's writer holding the table, so the live files cannot change meanwhile:

1. Note whether the target already holds a valid header, which means its name is already durable.
2. Open the target, truncate it to 0, write the three sections from byte 120, `fsync` the file.
3. If the name is new, `fsync` the table directory.
4. Write the header in one call at offset 0 and flush it to the device (`fsyncDurable`). That flush is the publish
   point: a crash before it leaves a file without a header, which never counts.

## 5. When a save point is taken

- **Periodic.** `ApplyWal2TableJob.maybeAdvanceDurableEpoch`, after a batch that applied transactions: on the first
  batch, when `cairo.adaptive.epoch.interval` has passed, when `cairo.adaptive.epoch.max.rows` rows were applied
  since the last one, after a backwards clock step, or when one is owed. Skipped when the writer is not in
  `adaptive` mode, on a replica, and when the interval is negative and nothing is owed.
- **Inline**, right after an operation that changes table files outside the ordinary apply
  (`TableWriter.advanceDurableEpochAfter`): ALTER COLUMN TYPE, CONVERT PARTITION TO PARQUET and TO NATIVE, the
  native-to-Parquet partition switch (Enterprise cold storage), ATTACH and DETACH PARTITION, the attach at the end
  of a parallel COPY into an existing table, a forced change outside the WAL, and `RESUME WAL`. Each is best effort: a failure logs a
  critical message and marks the save point owed, and the next apply pass, clean close or `RESUME WAL` takes it.
  `RESUME WAL` is the exception: if the owed save point fails there, the table stays suspended.
- **At a clean close**, when the table has transactions after its save point (or one is owed) and
  `cairo.adaptive.epoch.flush.on.close` is true. That is what makes a clean restart restore nothing.
- **At enrolment** (section 7).

The posting index reseal takes no save point of its own: the next periodic one covers the superseded files, which
the save point's partition pin keeps until then.

## 6. Publishing a save point

`TableWriter.advanceDurableEpoch`:

1. Skip, with an info line, when local durability is off (a replica), while a structural change is in flight, or
   when the durable frontier is fenced after a failed flush.
2. Take the target generation: the one that is not current.
3. Flush what the save point will name (`fsyncMaterializedState`): commit and sync every indexer; then
   - Linux with `cairo.adaptive.epoch.column.sync.batched` (and no ext4 `fast_commit`): start the write-back of
     every dirty column file, wait for it, then one `syncfs`, which also makes the sequencer log, the symbol maps
     and every directory entry durable;
   - Linux otherwise: a flush per column file and symbol map, then one `syncfs`;
   - macOS and Windows: a flush per file of every partition written since the last save point (every partition at
     enrolment), an `fsync` of the table directory, one device barrier, and an `fdatasync` of the sequencer log, so
     the save point never names a transaction the WAL has not made durable;
   - then `fsync` the live `_cv`, then the live `_txn`.
4. Release the pin of the generation about to be overwritten, which stops being a fallback once its file is
   truncated, then pin the partition versions the live `_txn` names in the transaction scoreboard, in that same
   epoch slot (the one the current save point does not use). A busy slot skips the save point and keeps the
   current one.
5. Write the target generation (section 4).
6. The previous save point becomes the fallback and keeps its pin; a previous pin that no generation on disk names
   any more (a restore removed it) is released. Set the save point seqTxn in the tracker, record the time, clear the
   owed mark and the row count. Log `adaptive durable epoch [table=..., epochSeqTxn=..., epochTxn=...]`.
7. Finish the partition moves the save point now covers (`finishPartitionMovesAfterSavePoint`): delete the
   `.attachable` directories of this writer's ATTACHes and give its DETACHes their final name (section 10).

A failed flush stops the engine (section 9). Any other failure releases the new pin and keeps the previous save
point, which stays valid and protected. A failure after step 4 released the older pin leaves the older generation
valid but unpinned until the next save point; a loss through it needs the current one damaged as well and a
partition purge in between.

After the column flush a save point makes six sync calls on Linux: `msync` and `fsync` of `_cv`, the same of `_txn`,
and two `fsync` of the generation file (sections, then header).

## 7. Enrolment and exit

"Enrolled" means "has a valid save point". There is no other record of it.

- **A new table** on an `adaptive` primary takes its first save point when it is created (as does the clone of
  REBASE WAL and a table converted to WAL), before it is registered. The REBASE clone and the converted table name
  partitions already, so their save point is pinned where it is published
  (`RecoveryCoordinator.pinPublishedBaseline`), as one a writer publishes.
- **An existing table** enrols when a writer first opens it on an `adaptive` primary: the first save point flushes
  every partition, because the run before it kept no save points. Until then the writer runs at `sync` grade, and
  it enrols at the end of its constructor (`enrollTableInAdaptiveMode`), before it applies anything, so a table is
  never applied lazily without a save point behind it. A failed enrolment fails the writer open (`could not publish
  adaptive enrollment baseline [...]`). A writer on a private root (the COPY staging writers) or holding a non-WAL
  token for a WAL table (COPY into a new table, until the next restart) does not enrol.
- **The exit.** A start in `nosync`, `async` or `sync` mode, after recovery (`RecoveryCoordinator.recover`), makes
  every enrolled table durable and removes its save point: one flush (`syncfs` of the database root and of each
  volume on Linux, `fsyncTableFiles` per table elsewhere), then per table the two-step removal and the release of
  the pin, logged as `table left adaptive commit mode, its save point is removed [table=...]`. A table with a
  conversion to non-WAL pending is skipped; the conversion removes its save point (section 10). A replica's writer
  does the same for itself (section 12).

**The two-step removal.** Delete the older generation, `fsync` the directory, delete the newer one, `fsync` the
directory. Unsynced deletions can reach the disk in any order; with one sync, a crash could keep the older
generation alone and valid, and recovery would rewind to it while its WAL and partitions are no longer protected.
The pin and the floor are released only after both deletions are durable.

The removal (`RecoveryCoordinator.removeAdaptiveEpochArtifacts`) also deletes, when it finds them, the copy files
that development builds of this feature wrote: `_snapshot`, `_meta.epoch`, `_cv.epoch`, `_txn.epoch` and their `.0`
and `.1` forms. No release wrote them. Two other development-build names, `_epoch.enrol` and `_convert.replay`, are
read by nothing and not deleted.

## 8. Start-up recovery

`RecoveryCoordinator.recover` runs once at start, before any WAL apply, job or conversion, for every WAL table
(views skipped), in every mode and role:

1. If neither generation file exists, the table is not enrolled: nothing to do. That is two existence checks per
   WAL table, the whole cost of a start for a table without a save point.
2. If `cairo.adaptive.recovery.roll.forward.enabled` is false, refuse.
3. Repair an interrupted `_meta` swap (the `_todo_` file), as the writer would.
4. Delete unpublished generation files. If none is left, the table is not enrolled.
5. Take the valid generations, the current one first. If a file exists and none is valid, refuse: the save point
   is damaged.
6. Check the chosen generation's sections: lengths and checksums. A generation that fails them never touches the
   live files: it is logged as critical (`adaptive save point failed its checksums, trying the other generation
   [...]`) and the other generation is tried.
7. **Lineage guard.** If the save point's seqTxn is past the sequencer's last transaction, it belongs to a WAL that
   was replaced: remove it and treat the table as not enrolled (`adaptive epoch belongs to a superseded WAL lineage,
   ...`, logged as critical). If it is ahead of the live `_txn` but within the sequencer, and the live `_txn` equals
   the other valid generation's, it is a generation an earlier start rejected: skip it. Any other "ahead" case
   refuses (`adaptive epoch post-dates live state; ...`, naming the generation file to move out).
8. **Restore** (`DurableEpochManifest.restoreLiveFiles`). Write each section that differs from its live file over
   it (truncated first, `_meta` first), `fsync` each, `fsync` the directory. A section equal to its live file is
   not written.
9. **Content checks** on the live files, which now equal the sections: the table, txn, column version and metadata
   version agree, and the symbol counts of `_txn` and `_meta` match. If they fail, fall back to the other
   generation. If no generation passes, refuse; the message then says whether the live files were rewritten, since
   moving the save point files out helps only when they were not.
10. **Make the selected save point durable**, on an `adaptive` start only (`DurableEpochManifest.rewriteHeader`). A
    failed header flush stops the engine and leaves the header only in the page cache, so a restart without a power
    loss can select a save point the device does not hold, and the WAL purge, the partition purge and the next save
    point (which overwrites the other generation) would trust it. Recovery therefore writes the selected header over
    itself (after a failed write-back the page is clean, so a bare flush would write nothing) and makes it durable:
    where `syncfs` is filesystem-wide (Linux) every rewritten header is flushed by one `syncRestoredState` of the
    database root and each volume after the loop over the tables, before `recover()` returns; elsewhere each
    header gets its own `fsyncDurable`. A failed open, write or flush fails the start (`could not open read-write
    [file=...]`, `could not rewrite adaptive save point header [path=...]`), and a failed flush stops the engine;
    the next start writes the headers again.
11. Delete a generation rejected in step 9 if the failure was a definite mismatch (a read error never deletes
    anything), logged as critical (`removing adaptive save point that failed its content checks [...]`), and sync
    the directory.
12. **Clean restart.** If no section differed, pin and stop: no live file is written and nothing is counted
    (`adaptive epoch matches live state, nothing to restore`).
13. Otherwise pin, set the save point seqTxn in the tracker, count the recovery (`recoveryIncarnation`, the recovery
    metric) and log `adaptive epoch roll-forward restored durable cut`. The pin covers the selected save point's
    partition versions and those of the other valid generation (not one rejected on its content), which a later
    start may still fall back to.
14. If the table has POSTING indexes and was restored (or its sequencer is past the save point), trim each
    partition's index chain back to the save point and delete the index files it does not name.

The ordinary WAL apply then replays every transaction after the save point.

Recovery is idempotent: its source is the current save point, which it never changes (it rewrites the selected
header with its own bytes, and deletes a generation it has proved wrong), so a crash at any step leads to the same
restore on the next start.

On a start in another mode, the exit of section 7 follows for every enrolled table. A table with a conversion to
non-WAL pending is left to the conversion (section 10).

## 9. The WAL floor, the partition pin, and the engine stop

**The WAL floor.** `WalPurgeJob.getSafeToPurgeUpToTxn` keeps the WAL after the lowest of the applied seqTxn and the
view consumers' floors. On an `adaptive` primary it also keeps the WAL after the save point
(`SeqTxnTracker.getDurableEpochSeqTxn`) and after the lowest valid save point header on disk
(`readAnchorFloorSeqTxn`, two small reads per table and purge run): recovery falls back to the older generation
when the newer one is damaged, so the WAL kept reaches one save point interval further back than the newest save
point (with a negative interval, possibly days). The floor is raised only after a new generation is durable and
cleared only after a removal is durable, so it never passes a save point a crash could still bring back. A table
whose tracker holds a save point pin keeps its floor in any mode, so a save point that outlives a start outside
`adaptive` (a conversion to non-WAL that could not finish and could not remove it) keeps its WAL. On a replica of an
`adaptive` instance, where the writer keeps no save points, the purge reads the lowest valid header seqTxn from the
files. In `nosync`, `async` and `sync` a table without a save point has no extra floor: the purge
behaves as before.

**The partition pin.** The save point names partition versions that later out-of-order writes, conversions and
detaches supersede. `TxnScoreboard` holds two epoch slots, A and B, one per generation: the current save point's
pin and the fallback's. A new save point takes the slot of the generation it overwrites, releasing that pin just
before the write truncates its file, and the previous save point keeps its pin as the new fallback (section 6).
`SeqTxnTracker` records both pins, so a scoreboard that idle eviction frees is seeded with both again. Recovery pins
both valid generations in every mode; the removal releases the pins last. A pinned version is not purged, so
superseded versions and dropped partitions wait one save point longer than the newest save point alone would need.

A dropped partition (DROP PARTITION, TTL, the original of a DETACH) has left `_txn`, and the older save point's pin
always lies below the current txn, so a range check against the pins would keep it for ever. For a version that
left the table, `O3PartitionPurgeJob.isDroppedVersionFree` checks readers and a running checkpoint over [name txn,
current txn) as before, but treats a save point pin in that range only as a reason to look: it then reads the `_txn`
copy in each save point file with a valid header (`DurableEpochManifest.namesPartition`, `TxReader
.imageListsPartition`) and keeps the version while either copy lists it. A read or checksum problem counts as
listed. Nothing else queues a purge once a version is free, so a writer that leaves versions to the purge job
records the txn (`partitionPurgeOwedTxn`), and after each save point it queues a partition purge until the older pin
has reached that txn. A table that gets no further write takes no further save point, so its dropped partitions stay
until its next write; `VACUUM TABLE` cannot remove them earlier.

**The partition purge after a restart.** After a failed flush and a restart, the page cache can hold a `_txn` (in
`sync`) or a save point header (in `adaptive`) that the device never got, and a purge that trusts it can remove the
partition version the state on the device still names. So in `sync`, and for WAL tables in `adaptive`,
`O3PartitionPurgeJob` leaves a table alone until a writer has opened it in this process
(`CairoEngine.isPartitionPurgeSafe`; `partition purge deferred to the next writer open [table=...]`; `VACUUM
TABLE` still reports success), and the writer open removes old versions itself once that state is durable: in
`sync` it first writes `_txn` again and flushes it (`TxWriter.rewriteAndFsync`), a failed rewrite failing the open
with nothing removed; in `adaptive` recovery has already made the selected save point's header durable (section 8,
step 10), so the open only marks the table safe (`TableWriter.purgeUnusedPartitionsAtOpen`). A writer on a private
root (COPY staging) never marks a table safe. `nosync`, `async` and non-WAL tables in `adaptive` are not gated.

**The engine stop.** In `sync` and `adaptive` mode, a failed `fsync`, `fdatasync`, `syncfs`, `sync_file_range` or
synchronous `msync` is fatal, because the data it covered may not be on the disk and the next commit would build on
it. `CairoEngine.poisonOnDataSyncFailure` logs `fatal durability barrier failure [operation=..., errno=...,
message=...]`, fences writers, acknowledgements and the WAL purge, and `ServerMain` halts the process with exit code
55 without closing writers (closing them would flush and publish state built on the failed flush). The paths that
stop the engine include the WAL commit barriers and the batched flush, the save point, the schema-file swap, ADD
COLUMN, ATTACH of a Parquet partition, ALTER COLUMN TYPE (also when a conversion task on a worker thread fails its
flush), CONVERT PARTITION TO PARQUET and TO NATIVE (and the squash that runs before TO PARQUET), the native-to-
Parquet partition switch, the recovery header flush (section 8), the flush at the end of a parallel COPY into a
new table (after the import's clean-up has dropped the table), and the Enterprise uploader's start flush. `nosync`
and `async` keep their previous behaviour on these paths. The flush after the WAL replay before a conversion to
non-WAL runs in every mode, and so does its stop.

When the stop follows a failed `_txn` flush inside a partition conversion or the partition switch, the error handler
no longer removes the new partition directory once the `_txn` write has started, so the restart finds the partition
the `_txn` names. And in `sync`, before a writer open purges superseded partition versions, it writes `_txn` again
and flushes it (`TxWriter.rewriteAndFsync`): after a failed write-back the page is clean, so a plain flush would
write nothing, and the purge could remove the version the `_txn` on the device still names.

## 10. Other paths that touch the save point

- **Conversion to non-WAL** (`TableConverter.replayWalBeforeConversion`, at start, after recovery). Before
  converting any WAL table, the start applies every outstanding WAL transaction, in every mode; only a table with no
  save point whose `_txn` has caught up with its sequencer skips the replay. After a finished replay: flush the
  table (`fsyncMaterializedState`, in every mode; a failed flush stops the engine in every mode), remove its save
  point (two steps), release the pin, convert. The
  conversion removes the sequencer and the WAL directories before it resets `_txn`'s seqTxn, so a kill in between
  never makes the next start replay the WAL from 0. A crash in the middle of the replay leaves the save point, so
  the next start rewinds and replays again (on an `adaptive` start, from the latest periodic save point the replay
  itself took).

  If the replay cannot finish (the table suspends, or a pass applies nothing), the conversion is refused in every
  mode: the table stays a WAL table, `_convert` and the WAL are kept, the critical line
  `table conversion failed, the table keeps its type [...]` carries `could not apply the WAL before the conversion
  to non-WAL, the table stays a WAL table; resume it, let it catch up and restart [...]`, and the next start tries
  again. On an `adaptive` start the refused table keeps its save point, from which the next replay starts. On a
  start outside `adaptive` the table is flushed and its save point removed and unpinned first, as on success:
  otherwise a transaction skipped with `RESUME WAL FROM TXN` would be replayed again from the save point at every
  start. A sequencer that cannot be read refuses the conversion of a table with a save point (recovery has just
  rewound it, so its WAL must be replayed); a table without one converts as before. A sequencer damaged for good
  leaves that refusal in place at every start; `ALTER TABLE ... SET TYPE WAL` cancels it, because it rewrites
  `_convert` without checking the pending type, and the next start skips the conversion (`skipping conversion,
  table already has the expected type`) and removes `_convert`. The table keeps the rows of its save point (by
  reading; not probed).
- **Conversion to WAL.** The converted table takes its first save point as a new table does, pinned where it is
  published (section 7).
- **Checkpoint restore** (`TableSnapshotRestore`). The table's save point is removed before the checkpoint's `_txn`,
  `_cv` and `_meta` are copied in. The restored table is then not enrolled, and its first `adaptive` writer enrols
  it.
- **REBASE WAL.** The staging clone excludes the source's save point files and takes its own first save point,
  pinned where it is published, before the rename that publishes it. That save point comes before REBASE's two empty seed
  transactions (`WalWriter.commitRebaseSeed`). A kill or power loss before the table's next
  save point makes recovery restore it, and the replay applies the seeds in one block with the first insert, which
  sets the table's minimum timestamp to 1970 ([questdb#7842](https://github.com/questdb/questdb/issues/7842); with
  assertions on, as in the tests, the apply fails and suspends the table instead). Base can reach the same block by
  timing; this sequence makes it certain. No row is lost.
- **ATTACH PARTITION** on an enrolled table of a primary. The partition is attached by hard link, not by rename
  (`TableWriter.linkAttachable`): the files of `.attachable` are linked into the new partition directory (copied
  when a link is not possible, for example across devices; a soft-linked `.attachable` gets a second soft link to
  the same target), `_txn` is committed, and the inline save point follows. `.attachable` is deleted once a save
  point covers the attach (section 6, step 7). A crash before that point rewinds, the writer removes the linked
  directory as an orphan, and the replay links again from the untouched `.attachable`. A crash after the save point
  and before the deletion leaves `.attachable` next to the attached partition, sharing its inodes: the writer open
  keeps it and logs `partition source next to the attached partition may share its files, do not attach it again
  [...]` (advisory), and a failed deletion logs `could not remove attached partition source, it shares files with
  the attached partition and is unsafe to attach again [...]` (critical). The operator deletes it, after a restart
  or once a save point followed the ATTACH: the writer open cannot tell a covered leftover from one a save point
  has not covered yet, which a writer reopened in the same process while save points are skipped can still need.
  After a restart the advisory is always true, because recovery made the selected save point durable. On a replica
  (`TableWriter.defersPartitionMoves` is false without local durability), in other modes, and with
  `cairo.attach.partition.copy`, ATTACH works as before.
- **DETACH PARTITION**, in every mode. The partition is linked (copied across devices) into a temporary directory
  `<partition>.<txn>.detaching`, where `<txn>` is the transaction of the DETACH commit, and the `_meta`, `_cv` and
  `_txn` copies go there; in `sync` and `adaptive` every file of it, the directory and its entry are synced before
  the commit. Then `_txn` is committed. On an enrolled `adaptive` table of a primary the partition waits for the
  next published save point (the inline one after the statement, or the next one if that fails); on any other table,
  a replica's included, it finishes at once. Finishing (`finishDetach`) links the final `.detached` name, syncs it and the table directory in `sync` and
  `adaptive`, and deletes the temporary name; a `.detached` left half linked by a crash is completed file by file
  (`linkMissingFiles`), so the finish can be repeated after any crash. At writer open, a temporary name whose txn the
  live `_txn` has not reached is a rollback leftover and is deleted; one it has reached gets its final name (on an
  enrolled table of a primary only once the pinned save point covers it). Without the temporary name, a crash between the link
  and the commit, or between the commit and its save point, would leave the partition both attached and detached,
  and a replayed or repeated DETACH would be refused.
- **Parallel COPY.** Into an existing enrolled table, the attach phase that moves the imported partitions into place
  takes one save point for all of them (`ParallelCsvFileImporter.attachPartitions`). COPY into a new table holds a
  non-WAL token for it until the next restart, so its writer does not enrol and takes no save point. On an
  `adaptive` instance the importer flushes such a table once, after the last partition is attached and before COPY
  reports it finished (`ParallelCsvFileImporter.flushCreatedTable`): one `syncRestoredState` of the database root
  where `syncfs` is filesystem-wide (the importer never places a table in a volume), `fsyncTableFiles` elsewhere. A
  failed flush fails the import; its clean-up drops the table, and a data-sync failure then stops the engine. If
  the drop itself fails, its error propagates and the engine is not stopped (a double failure, one line). The
  table's first writer after a restart enrols it with a whole-table flush. In `sync` the importer flushes nothing
  of its own (guide section 7.6).
- **`RESUME WAL`** takes the save point the table owes before the table resumes.
- **The old UDP receiver** writes outside the WAL. In `adaptive` mode it refuses WAL tables
  (`LineUdpParserImpl.refuseWalTable`), because recovery would roll its rows back.

## 11. Held-back transactions (the saved WAL lag)

When the apply job stops in the middle of out-of-order data (its time quota, a shutdown, an error), `commitSeqTxn`
records the not-yet-merged rows in `_txn` as the WAL lag. Those rows sit in the last partition's column files after
the committed rows, and the next commit that merges them rewrites that area before it writes the new `_txn`. A crash
between the two left `_txn` reading a lag from changed bytes: rows lost or duplicated, with no error, in every mode
([questdb#7804](https://github.com/questdb/questdb/issues/7804)). A save point taken right after `commitSeqTxn` made
it worse: it saved the lag, and any crash before the next save point lost those rows.

The writer now never trusts a saved lag. On open and on rollback, `TableWriter.discardWalLag` zeroes the lag in
memory (the `_txn` on disk and the save point keep their txn) and the apply job applies those transactions again
from the WAL, which the purge keeps from the table's seqTxn on. Paths that change the table outside the WAL (forced
ALTERs, the writer command queue) drop a held lag first (`applyOutsideWal`) and reopen the last partition. The writer
logs `discarded WAL lag, its transactions apply again from the WAL [table=..., reason=..., ...]`. The cost is one
re-apply per table after a restart.

Separately, resetting the lag wrote its four-byte checksum with an eight-byte store, which overwrote the lag
transaction count beside it. For about half of all tables past txn 73,045 that made a checkpoint restore, a backup
restore, a conversion to WAL or a REBASE skip a WAL transaction. `TxWriter.resetLagAppliedRows` now writes four
bytes.

## 12. QuestDB Enterprise

- **Replicas keep no save points.** A replica installs `LocalDurabilityPolicy.REPLICA_SKIP`: its writers take no
  save points and its truth is the object store. Its WAL floor comes from any save point files still on disk. It
  never rewinds, so ATTACH and DETACH PARTITION finish at once there (section 10).
- **Demote** (`ReplicaRoleState.clearAdaptiveEpochArtifacts`). For each WAL table with a save point file, the role
  switch takes the table's writer without waiting (`getWriterUnsafe`). If it gets it, the writer's reconcile makes
  the table durable and removes the save point in two steps (section 7), and outside `adaptive` a leftover save
  point is removed under the same lock. In `adaptive` a save point the reconcile could not remove (its flush
  failed, say) stays, with the WAL it keeps, and the table counts as failed: removing it without the flush could
  lose the rows applied lazily since. If the writer is busy, the table is skipped and that writer's own reconcile
  does the same at the end of its current batch, or before its next one, on its own thread, so the removal never
  races a save point being written. A table that fails for another reason is logged and left to its writer; a
  failed flush stops the engine. The switch logs `cleared adaptive save points on becoming a replica [tables=...,
  busy=..., failed=...]`.
- **Promote** (`EntCairoEngine.fsyncReplicaStateForPromote`, on every promote path). On an `adaptive` instance, before
  the apply job is set on the tables, one flush makes what the replica applied lazily durable
  (`DurableEpochManifest.fsyncTables`: one `syncfs` of the database root and of each volume on Linux; elsewhere the
  tables one by one, each file flushed, a failure logged per table). Each table then enrols at its writer's first
  batch with a whole-table flush. A batch that began on the replica and is still running at the promotion keeps
  applying lazily after that flush, and the promotion's notification for its busy table can be dropped, so
  `ApplyWal2TableJob` reconciles the writer again at the end of every batch, while it still holds the writer. Outside
  `adaptive` that call is one field compare, and in steady state one volatile read; it skips a distressed writer,
  one a DROP TABLE destroyed included. If such a straddling batch fails, the writer is distressed and not
  reconciled, so the rows it applied after the promotion flush have no flush and no save point until the table is
  resumed or the server restarts; a power loss in between can lose them (very unlikely; no cheap safe fix, since a
  writer the job stopped trusting must not publish a save point).
- **The uploader waits for the durable frontier.** In `adaptive` and `sync` mode the uploader ships a sequencer
  record only once the local durable frontier covers it, for V1 and V2 logs alike, so a power loss can never make
  the primary reuse a transaction number the object store already holds. While it waits it logs `holding uploads
  past the local durable frontier [...]` once.
- **The uploader start flush.** At boot and at promotion, in `adaptive` and `sync` mode, the uploader makes every
  table's sequencer log durable before its first upload: one `syncfs` of the database root and of each linked volume
  on Linux, an `fdatasync` per table elsewhere. A failure stops the engine.
- **The V1 log on a replica.** The downloader is the only writer of a replica's V1 sequencer log. A Java open leaves
  records past the published tail alone (the downloader wrote them and has not published them yet); a torn tail is
  still cut.
- **Backup restore** (`BackupRestoreAgent`). The restored table's save point is removed before its files are written,
  and the first `adaptive` writer enrols it. In every mode, each restored table is flushed before its
  commit marker
  (one `syncfs` on Linux; every file and directory of the table elsewhere, files only on Windows), because a resumed
  restore skips a table whose marker matches and nothing else flushes a table without a save point; the root is
  synced once more before the trigger file goes. A backup skips a `_chk` file (a partition checksum file of
  development builds) instead of stopping on it.
- **Default mode.** Without an explicit `cairo.commit.mode`, Enterprise runs `nosync` when replication is configured.
- **Durable acknowledgements.** The `local` tier is served only on an `adaptive` primary; the `replicated` tier
  follows the upload frontier.

## 13. Detecting torn files

These run in every mode. Each uses space the released format already left unused, so a file written before this
change reads as unverified, never as torn, and a released binary ignores the new bytes.

- **`_txn` body checksum.** An 8-byte checksum at offset 116 of each `_txn` record and a 4-byte stamp at offset 124
  (the record's txn with the top bit flipped), in the 12 bytes before offset 128 that were unused. It covers the
  record header (without the structure version, the seqTxn and the lag fields, which change in place) and the
  partition table. `TxWriter` writes it before the version bump that publishes the record. `TxReader` checks it on
  the first read: a stamp of 0, or one naming another txn, reads unverified; a mismatch falls back to the other A/B
  area, and if both fail the read fails with `_txn body checksum mismatch in both A and B areas [...]`.
- **`_cv` trailer.** 16 bytes after each `_cv` area: a 64-bit magic (`TableUtils.CV_CHECKSUM_MAGIC`), then a
  checksum of the whole area. `ColumnVersionWriter` writes it before the version bump, in every mode, and the
  checkpoint copy writes one too. A file too short for it, or one without the magic there (every file a released
  version wrote), reads unverified; a mismatch falls back to the other area (`read fell back to other _cv area
  after checksum mismatch [...]`), and if both fail: `_cv checksum mismatch in both A and B areas [...]`.
- **Version 2 sequencer log.** Each 60-byte record carries a checksum of its first 52 bytes in its 8 reserved
  bytes. A stored 0 reads unverified; a mismatch fails the read with `torn sequencer txnlog record [txn=...,
  ...]`. A new log's header also carries a capability marker at offset 24, which nothing reads yet.
- **Version 1 sequencer log: the zero tail.** A V1 log has no room for a checksum. Instead, an open scans the records
  after a hint kept in the header (offset 40: the first transaction a completed flush may not have covered) and
  cuts the tail at the first record that cannot be whole (zero WAL id, zero timestamp, a gap in the structure
  version): it lowers `maxTxn`, zeroes the bytes after it and flushes (`dropping torn sequencer txnlog tail
  [...]`). An open that cuts nothing leaves the bytes past the tail alone; the first append after it zeroes and
  flushes them if any are non-zero, so a stale record can never look valid after the next power loss. Damage to a
  V1 record already written (bit rot, a foreign write) is not detected.
- **WAL segment length guard.** Before applying, the writer checks that each WAL column file is long enough for
  the committed rows, from the mapping it already holds, and that the last aux entry of a var-size column is whole
  (`WAL segment column too short for committed row range [...]`, `WAL segment column has a torn aux entry [...]`).

A torn `_txn` or `_cv` live area makes a reader stay on the previous transaction (`_txn live area is torn, reader
cannot advance past the previous transaction [...]`) and a writer refuse to build on it, rather than read wrong data.
Under `adaptive`, recovery restores these files from the save point before anything reads them.

## 14. Files and formats

Added, per enrolled WAL table:

| File | Content | Written by |
|---|---|---|
| `_epoch.manifest.0`, `_epoch.manifest.1` | one save point each (section 4) | the table writer, and a conversion to WAL and REBASE WAL for their first one; recovery rewrites the selected header on an `adaptive` start; deleted by the exit, a lineage discard, recovery (a generation rejected on its content), a restore, a demote and a conversion |
| `<partition>.<txn>.detaching`, where the `.detached` directory goes | a detached partition until its DETACH commit is final | DETACH, in every mode (section 10) |

Changed, in files a released version also reads (none changes a format version, and a released binary ignores the
new bytes):

| File | Change |
|---|---|
| `_txn` | body checksum and stamp at offsets 116-127; the lag checksum reset writes 4 bytes instead of 8 (a fix) |
| `_cv` | a 16-byte trailer after each area; areas are placed with room for it |
| sequencer V2 part files | a checksum in the 8 reserved bytes of each record; a capability marker in the header (written, not read) |
| sequencer V1 `_txnlog` | the unsynced-from hint at header offset 40 |
| `_meta` | none: the base layout (format minor version 2). A development build's enrolment field still loads. |
| `_convert` | none |

Not added, though earlier development builds had them: `_snapshot`, `_meta.epoch`, `_cv.epoch`, `_txn.epoch` (and
their `.0`/`.1` forms), `_epoch.enrol`, `_convert.replay`, `_event.c`, `_txnlog.c`, `_chk`, a `_meta` enrolment
field and body checksum. The save point removal deletes the first four when it finds them (section 7).

## 15. Caveats

1. The mode is fixed for the life of the process; there is no per-table mode.
2. `syncfs` flushes the whole file system, so a save point's cost depends on unrelated dirty data on the same file
   system. On ext4 with `fast_commit` the batched column flush is turned off, because the per-inode journal does
   not journal the columns' within-page writes when `_cv` is synced.
3. On macOS and Windows a save point flushes every file written since the last one, then the table directory, then
   the device: slower than `syncfs`, and the enrolment flushes every partition.
4. A renamed directory is durable only once its parent is synced. REBASE WAL syncs the database root between the
   rename of its staging clone and the registry swap, and the table registry syncs the root between its compaction
   rename and the unlink of the old file.
5. `_snapshot` in a checkpoint directory is the legacy checkpoint metadata file, unrelated to this feature.
6. The test suite runs `adaptive` by default (`questdb.test.commit.mode`); CI splits each job group between `nosync`
   and `adaptive` and swaps them every run. A mode seen in a test run says nothing about the shipped default, which
   is `nosync`.
7. `syncfs` reports a write-back error once per file system, and only from Linux 5.8 on: an error that another
   process's `syncfs` collected first, or any error on an older kernel, goes unseen. The save point's batched flush,
   the exit, the recovery header flush and the Enterprise uploader start flush rely on that report alike.
