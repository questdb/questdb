# Adaptive commit: how it works

The developer companion to [`adaptive-commit-mode.md`](adaptive-commit-mode.md). It follows a commit from the client
to the disk and back after a crash, names every file the mode adds or changes, and says which code does what.
Methods are named rather than line numbers, which drift.

`W` is the group-commit window, `cairo.adaptive.commit.group.window`.

Timing and system-call totals below are historical measurements, not fresh measurements of the final design.
The measured revisions, machine and limitations are in the [guide, section 9](adaptive-commit-mode.md#9-what-it-costs).
In particular, the final required completion of every selectable save point was not remeasured.

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
| `durableEpochSeqTxn` (the save point) | a save point is published | the WAL purge floor (with the older generation's header on disk), the close-time save point, the idle save point request, `wal_tables()` |

Both live in the table's `SeqTxnTracker`. Readers see applied data as soon as it is applied, durable or not: the
frontiers bound what a crash can take, not what a query sees.

### One mode per instance

`cairo.commit.mode` is the configured instance mode, fixed for the process lifetime; there is no SQL per-table
mode. Enterprise roles can change while the process runs. Applied-state and downloaded-log flush decisions use
`LocalDurabilityPolicy.resolveCommitMode`: configured `adaptive` becomes `nosync` on a replica; explicit `sync`
stays `sync`. Local WAL commits, boot before role installation and the promotion flush keep the configured grade.
Save-point lifecycle checks also retain the configured mode so inherited save points can be reconciled after a
role change. `CommitMode` holds the predicates applied to those mode values:

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
| `sync` | `msync(MS_SYNC)` | `msync(MS_SYNC)` | event files and the segment `_meta` are flushed before sequencing |
| `adaptive` (any `W`) | `msync(MS_ASYNC)`, a write-back drain (if `cairo.wal.commit.writeback.drain`: `sync_file_range(WRITE)` on every column and `_event` file, then `WRITE \| WAIT_AFTER` on each, so the files are written back in parallel), then a barrier per file | drain, `msync(MS_ASYNC)`, barrier | every private WAL byte is on the device |

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

The flushes make the part file durable before the header that names its record. Between flushes the kernel's own
write-back can still write the header page first, so outside `sync` and `adaptive` with `W` = 0 a power loss can
leave a header that names a record past the durable frontier that is not on the disk.

**The batched flush** (`W` > 0). Pending registration under the sequencer lock prevents a peer flush from
advancing past an unregistered transaction. `recordPendingDurable` adds the writer to `WalGroupCommitFlushQueue`.
When its oldest pending commit is `W` old, `flushPendingDurable` runs `fdatasyncTxnLog` and `markWriterDurable`.
The flush returns the transaction sampled under the sequencer lock; the frontier cannot include a later commit
made after that lock was released. It also cannot pass an older pending writer.

A writer returned to the pool flushes its pending batch at once: `WalWriter.cleanupBeforeClose` calls
`flushPendingDurable` on every pool return and close, so the next acquirer and any durable-ack consumer find the
frontier on disk. A writer taken for one statement, such as a SQL `INSERT`, therefore flushes per statement and
shares nothing with the window; batching applies to writers held across commits (ILP, QWP).

When commits stop, `WalPurgeJob` checks the queue on its worker passes, independently of its slower purge cadence,
and calls `forceDurableIfPending`. Nominal delay is `W` plus worker sleep, scheduling and flush time, not a hard
real-time RPO bound. A fatal barrier failure fences the frontier and the engine (section 9).

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

The **current save point** is the generation the table stands on: the one recovery selected at the last start, or
the one the table's writer last published. The writer learns it from the save point's pin, which `SeqTxnTracker`
records with the generation (`pinnedEpochGeneration`); a writer without a pin (enrolment, a baseline published
outside a writer) takes the valid generation with the higher (seqTxn, txn), the one recovery would select. A txn
cannot name the current generation: the idle save point (section 9) leaves both generations at one txn, and after a
fallback the table stands on the older one. A table is **enrolled** when it has a valid generation. A new save
point is always written into the other generation, so the current one is never touched while the next is written:
a torn rewrite loses to the untouched file, and a complete one wins with its higher cut. The other valid generation
is the fallback: recovery restores it when the current one fails its checksums or its content checks, so its WAL
and its partition versions are kept until the next save point overwrites it (section 9).

### Writing a generation

`DurableEpochManifest.write`, with the table's writer holding the table, so the live files cannot change meanwhile:

1. Note whether the target already holds a valid header, which means its name is already durable.
2. Open the target, truncate it to 0, write the three sections from byte 120, `fsync` the file.
3. If the name is new, `fsync` the table directory.
4. Write the header in one call at offset 0 and flush it to the device (`fsyncDurable`). That flush is the
   publication barrier. A crash before the header write leaves no published header; a write interrupted before
   the barrier may or may not survive. Recovery validates the surviving header and sections instead of inferring
   whether the publishing call returned.

## 5. When a save point is taken

- **Periodic.** `ApplyWal2TableJob.maybeAdvanceDurableEpoch`, after a batch that applied transactions: on the first
  batch, when `cairo.adaptive.epoch.interval` has passed, when `cairo.adaptive.epoch.max.rows` rows were applied
  since the last one, after a backwards clock step, or when one is owed. Skipped when the writer is not in
  `adaptive` mode, on a replica, and when the interval is negative and nothing is owed.
- **Inline**, right after an operation that changes table files outside the ordinary apply
  (`TableWriter.advanceDurableEpochAfter`): ALTER COLUMN TYPE, CONVERT PARTITION TO PARQUET and TO NATIVE, the
  native-to-Parquet partition switch (Enterprise cold storage), ATTACH and DETACH PARTITION, the attach at the end
  of a parallel COPY into an existing table, a forced change outside the WAL, and `RESUME WAL`. Physical conversions
  and WAL ATTACH remain best effort: a failure logs a critical message and marks the save point owed. DETACH,
  RESUME, and direct logical partition changes require synchronous completion (`completeDurableEpochAfter`):
  every selectable generation must cover the effect before completion, taking another save point if needed.
  A skipped or failed required publication fails the operation; WAL apply or RESUME keeps the table suspended.
  Ordinary INSERT/data commits take no additional save point.
- **At a clean close** (`TableWriter.doClose`), when `cairo.adaptive.epoch.flush.on.close` is true and one is owed,
  or, with an interval that is not negative, when the table has transactions after its save point or its txn moved
  since the save point without a WAL transaction (`getTxn() != pinnedEpochTxn`: an Enterprise storage policy step
  applied inline, a partition removed by the writer). That is what makes a clean restart restore nothing.
- **When the table goes idle** (section 9): one more save point about one interval after the table's last one, so
  that both generations stand on the current state.
- **At enrolment** (section 7).

The posting index reseal takes no save point of its own: the next periodic one covers the superseded files, which
the save point's partition pin keeps until then.

## 6. Publishing a save point

`TableWriter.advanceDurableEpoch`:

1. Skip, with an info line, when local durability is off (a replica), while a structural change is in flight, or
   when the durable frontier is fenced after a failed flush.
2. Take the target generation: the one that is not current (the pinned generation, section 4).
3. Flush what the save point will name (`fsyncMaterializedState`): commit and sync every indexer; then
   - Linux with `cairo.adaptive.epoch.column.sync.batched`: start the write-back of
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
7. Finish the partition moves the save point now covers (`finishPartitionMovesAfterSavePoint`): give this writer's
   DETACHes their final name only when every on-disk candidate covers the removal, and delete the `.attachable`
   directory of each of its ATTACHes once the older
   generation also stands at or past the attach's commit (section 10). The rule for every side effect of a publish:
   a change the WAL can redo keeps its undo material until the older generation stands at or past its commit; a
   change the WAL cannot redo takes save points until both generations stand at or past it, and reports success
   only then.

A failed flush stops the engine (section 9). Any other failure releases the new pin and keeps the previous save
point, which stays valid and protected. A skipped reservation or failed replacement restores the released older
pin: a failed open can leave that generation entirely intact. Even if the replacement partially truncated it,
retaining its old pin is conservative; the next successful publication releases it. A writer reopen must not
purge the partitions of a selectable fallback just because a failed publication released its pin.

After the column flush a save point makes six sync calls on Linux: `msync` and `fsync` of `_cv`, the same of `_txn`,
and two `fsync` of the generation file (sections, then header). In all, a save point of a 4-column table makes 47
system calls (168 at 64 columns, 552 at 256) and takes 19 ms, nearly all of it the `syncfs`; it barely grows with
width or partitions in that experiment (22 ms at 1,000 partitions or at 256 columns). These are the older
measurements identified above, not a bound on current structural operations, which may publish two generations.

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
  token for a WAL table (the import's writer of a WAL table COPY creates) does not enrol.
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

1. If neither generation file exists, the table is not enrolled: nothing to do. That is two existence checks (four
   `access` calls) per WAL table, the whole cost of recovery for a table without a save point: about 2 µs per table,
   2.2 ms per 1,000 tables.
2. If the internal roll-forward test hook is disabled, refuse. There is no server-side property for this; it
   exists only so a test can force the refusal path.
3. Repair an interrupted `_meta` swap (the `_todo_` file), as the writer would.
4. Delete unpublished generation files. If none is left, the table is not enrolled.
5. Take the valid generations, the current one first. If a file exists and none is valid, refuse: the save point
   is damaged.
6. Check the chosen generation's sections: lengths and checksums. A generation that fails them never touches the
   live files: it is logged as critical (`adaptive save point failed its checksums, trying the other generation
   [...]`) and the other generation is tried. A read error (a failed stat, open or map of the file) is never a
   verdict: it fails the start, which can be retried, instead of sending the table down the fallback; the same
   holds for the reads of steps 7 and 9.
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
    the next start writes the headers again. On Linux, 1,000 tables take 244 ms at an `adaptive` start, of which
    the `syncfs` is 51 ms and the rest the reads and rewrites of the headers.
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
(`SeqTxnTracker.getDurableEpochSeqTxn`) and after the lowest valid save point header on disk (`readAnchorFloorSeqTxn`,
two small reads per table and purge run): recovery falls back to the older generation when the newer one is damaged,
so the WAL kept reaches one save point interval further back than the newest save point (with a negative interval,
possibly days; on a table that stops changing, until its idle save point). The floor is raised only after a new
generation is durable and cleared only after a removal is durable, so it never passes a save point a crash could still
bring back. A table whose tracker holds a save point pin keeps its floor in any mode, so a save point that outlives a
start outside `adaptive` (a conversion to non-WAL that could not finish and could not remove it) keeps its WAL. On a
replica of an `adaptive` instance, where the writer keeps no save points, the purge reads the lowest valid header
seqTxn from the files. In `nosync`, `async` and `sync` a table without a save point has no extra floor: the purge
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
has reached that txn. On a table that gets no further write, the idle save point below moves the older pin; `VACUUM
TABLE` cannot remove the versions earlier.

**The idle save point.** On a table that stops changing, nothing else would replace the older save point, so its WAL,
superseded and dropped partition versions, superseded column files and posting files would stay for ever. At the
end of each table's sweep, `WalPurgeJob.requestIdleSavePoint` asks for one more save point at the current state, on
an `adaptive` primary only (`LocalDurabilityPolicy.resolveCommitMode` is ADAPTIVE, so a replica never asks), with
an interval that is not negative, outside plain views, suspended tables and a fenced durable frontier, when:

- the two pins differ, or the newest save point's seqTxn is below the applied seqTxn, or the txn the writer last
  committed is past the newest pin (`SeqTxnTracker.committedTxn`, which a database-root WAL table writer publishes
  each time it is released, so a commit without a WAL transaction counts too; a COPY staging writer publishes
  nothing; the publish is a map lookup and a volatile store, within the noise of an apply pass). Equal pins mean equal
  save points, since every `_txn` write moves the txn, so the check reads no file;
- and the table has taken no save point for one interval (`getLastEpochTs`), and the job has not asked for one in
  the last interval (`getIdleEpochRequestTs`, so a request that cannot be taken is asked again one interval later).

It then sets `SeqTxnTracker.requestIdleEpoch` and notifies the apply job, which consumes the request in a run that
applies nothing and calls `maybeAdvanceDurableEpoch` with the usual gates (the writer is taken without waiting: a
busy writer means the table is not idle). Such a run skips a suspended table (no retry of its failed transaction;
`RESUME WAL` notifies it again) and does not log `unsolicited table lock` for a writer held under another reason.
For a live view the job only sets the request, since the apply job drops a live view's notifications:
`LiveViewRefreshJob.scanForLaggingViews` takes the save point under the view's refresh latch, and the job asks only
when the view's refresh worker can take it (`isLiveViewRefreshable`: refresh workers on, a real, live, valid view).
A pass asks at most `WalPurgeJob.IDLE_SAVE_POINT_REQUESTS_PER_PASS` (16) tables; a pass that reaches the cap makes
the next one start after the last table it asked, so every eligible table is asked within about two rounds of
tables / 16 passes: after a restart with 1,000 such tables, 63 passes over about 31 minutes at the defaults, each
with about 0.3 s of apply work (about 20 ms per save point). Both generations then stand on the current state, the WAL
floor rises to it, and the purge queued after the save point (`partitionPurgeOwedTxn`) frees the versions; the column
purge and the posting purge retry until the pins move. The writer's close (section 5) covers only what changed since the
newest save point, which leaves the two generations apart; the idle save point is the purge job's, not the writer
pool's.

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
it. The one exception is the write-back drain's kick (`sync_file_range(WRITE)`, which starts a write-back and does
not wait): ENOMEM, EAGAIN and EINTR leave the pages dirty, and the drain's wait and the barrier after it write them
back and report any error, so the kick ignores them (`Vm.checkWritebackKick`); any other error is fatal, since ext4
can drop pages whose block allocation failed for good without recording an error for a later wait or barrier. The failure is decided where it is created: `CairoException.dataSyncFailure(ff, ...)`, the one Java factory of a
flush failure (with the forms `dataSyncFailureOfFd` and `dataSyncFailureOfMapping` that name the file), tells the
engine that owns the file at once, so a path that later drops, counts or wraps the exception (a task's error counter,
a job's top-level catch, a wrapper without the cause) cannot lose the stop. The owner is a live engine whose files
facade is `ff` and whose database, checkpoint, COPY work or volume (`cairo.volumes`) root holds the failed file, which the
factory looks up on a failure only (Linux: `/proc/self/fd`, `/proc/self/maps`); so two engines on one facade object, a primary and a replica
in one test JVM, hear only their own failures. Without a path (other systems, a test facade that gives none) or when
no engine holds it, every engine on `ff` hears it, so a stop is never lost. A closed engine, or one whose construction
failed, is unregistered. Each engine decides by `CairoEngine.isDataSyncFailureFatal`: the flag, and the commit mode
`LocalDurabilityPolicy.resolveCommitMode` resolves for its role, so an Enterprise replica in `adaptive`, which resolves
to `nosync` (the object store holds the durable copy), reports the error and keeps running as on the base release,
and a replica in `sync` stops. A
fatal failure logs `fatal durability barrier failure [operation=..., errno=..., message=...]`, fences writers,
acknowledgements, the WAL purge and `CHECKPOINT CREATE` (an Enterprise incremental backup takes one, without the final
`sync()`, so a fenced engine ships no state whose flush failed), and `ServerMain` halts the process with exit code 55 there, without closing
writers or running any clean-up (closing them would flush and publish state built on the failed flush). The
handlers at the catch sites (`handleDataSyncFailure`, `poisonOnDataSyncFailure`) apply the same rule and turn the
failure into the `CairoError` the caller throws; they no longer test a mode of their own. `nosync` and `async` keep
their previous behaviour. Two stops do not follow the resolved mode: the flush after the WAL replay before a
conversion to non-WAL stops in every mode, and the Enterprise promote flush, which runs only in `adaptive`, stops
even while the replica policy may still be in force; both call `handleDataSyncFailureInEveryMode`.
`DataSyncFailureModePolicyTest` keeps these rules by source scan, and `DataSyncFailurePropagationTest` fails every
flush of its workloads in turn.

Without `ServerMain` (an embedded engine, a test JVM) the fence is the stop. The default handler returns, and the
factory's record outlives the exception: write and acknowledgement fences reject further work, purge is blocked,
and `CHECKPOINT CREATE` is refused with `CairoError` (`engine is poisoned by a failed durability barrier`), whatever
catch the failure itself reached. The rolled segment's column copy (`CopyWalSegmentUtils`) keeps the flag in its
cause, so the WAL writer's handler drops the pending durable txn and raises the stop. UPDATE, partition changes,
WAL close and materialized-view refresh preserve the fatal failure instead of replacing it during cleanup or
turning it into suspension or invalidation. The embedder closes the engine, fixes the cause and constructs a new one:
nothing past the failure was acknowledged, the WAL holds the rest, and the new engine recovers as a restart does. The engine does not close itself (a close from inside a flush,
under writer monitors or the sequencer lock, would deadlock or free memory under a caller), and a library never halts
its host. Enterprise installs the handler at construction (`EntBootstrap` passes the bootstrap's handler to the
`EntCairoEngine` constructor), so a failure inside the partial constructor halts as one after it does.

Known gap: a flush that Rust runs (the Parquet `_pm` barriers in an O3 merge or an ATTACH, Enterprise cold
storage) reaches Java as a plain error without the flag, so it fails the statement or suspends the table instead of
stopping the engine. No acknowledged data is lost: the `_pm` footer is written past the committed size and `_txn`
does not move, so the retry rewrites the same region. One Java flush also stays outside the
factory: the final `sync()` of `CHECKPOINT CREATE` checks its own return code and fails the statement, as on the base
release (it makes the checkpoint durable, not the live tables).

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
- **REBASE WAL.** The staging clone excludes the source's save point files and takes its own first save point, pinned
  where it is published, before the rename that publishes it. That save point comes before REBASE's two empty seed
  transactions (`WalWriter.commitRebaseSeed`, whose `_event` records carry min and max timestamp 0, as before). A kill
  or power loss before the table's next save point makes recovery restore it, and the replay applies the seeds in one
  block with the first insert after them; base reaches the same block when the apply job loses a race. Such a block
  used to set the table's minimum timestamp to 1970 ([questdb#7842](https://github.com/questdb/questdb/issues/7842)).
  Now, in every mode, `WalTxnDetails.prepareCopySegments` lets a transaction with no rows add nothing to an apply
  block's min, max or in-order flag, and `WalTxnRangeLoader` keeps the seeds (zero-row data transactions that are not
  a replace-range commit) out of a materialized view's refresh intervals. A table that already reads 1970 is not
  repaired.
- **ATTACH PARTITION** on an enrolled table of a primary. The partition is attached by hard link, not by rename
  (`TableWriter.linkAttachable`): the files of `.attachable` are linked into the new partition directory (copied
  when a link is not possible, for example across devices; a soft-linked `.attachable` gets a second soft link to
  the same target), `_txn` is committed, and the inline save point follows. `.attachable` is deleted once both
  generations cover the attach: the save point after it, and the next one, which puts the older generation at or
  past the attach's commit (section 6, step 7). A crash before that point rewinds, to the newer generation or, when
  it is damaged, to the older one; the writer removes the linked directory as an orphan, and the replay links again
  from the untouched `.attachable`. A close or a crash after the first save point and before the deletion leaves
  `.attachable` next to the attached partition, sharing its inodes. The writer open queues it for the deletion
  again when the older pin does not yet cover the attach (the partition's name txn plus one), unless
  `cairo.attach.partition.copy` is set, since the open cannot tell a copy the attach leaves in place from a linked
  source; otherwise it keeps it and logs `partition source next to the attached partition may share its files, do
  not attach it again [...]` (advisory), and a failed deletion logs `could not remove attached partition source, it
  shares files with the attached partition and is unsafe to attach again [...]` (critical). The operator deletes
  such a leftover only after both selectable generations cover the ATTACH; a restart alone is not proof of
  coverage. Prefer the writer's own deferred cleanup. On a replica
  (`TableWriter.defersPartitionMoves` is false without local durability), in other modes, and with
  `cairo.attach.partition.copy`, ATTACH works as before.
- **DETACH PARTITION**, in every mode. The partition is linked (copied across devices) into a temporary directory
  `<partition>.<txn>.detaching`, where `<txn>` is the transaction of the DETACH commit, and the `_meta`, `_cv` and
  `_txn` copies go there; in `sync` and `adaptive` every file of it, the directory and its entry are synced before
  the commit. Then `_txn` is committed. On an enrolled `adaptive` table of a primary the partition keeps its temporary
  name through the first covering save point. The statement synchronously publishes another if needed, so every
  selectable recovery cut includes the removal before `.detached` is exposed and apply completes. This works
  even with periodic epochs disabled. It is apply/artifact completion, not the earlier WAL enqueue acknowledgement.
  On any other table, a replica's included, it finishes at once. Finishing (`finishDetach`) links the final `.detached` name, syncs it and the table directory in `sync` and
  `adaptive`, and deletes the temporary name; a `.detached` left half linked by a crash is completed file by file
  (`linkMissingFiles`), so the finish can be repeated after any crash. At writer open, a temporary name whose txn the
  live `_txn` has not reached is a rollback leftover and is deleted; one it has reached gets its final name (on an
  enrolled table of a primary only once every on-disk candidate covers it). A failed required publication keeps
  the temporary copy and is retryable through RESUME; fallback below the removal replays DETACH from its original
  partition. An unrelated pre-existing `.detached` destination still refuses DETACH. Without the temporary name, a crash between the link
  and the commit, or between the commit and its save point, would leave the partition both attached and detached,
  and a replayed or repeated DETACH would be refused.
- **Parallel COPY.** Into an existing enrolled table, the attach phase that moves the imported partitions into place
  takes one save point for all of them, and then a second one so that the older generation also stands at or past
  the attaches (`ParallelCsvFileImporter.attachPartitions`): nothing in the WAL redoes an import, so a start that
  fell back to the older generation would lose it. When a save point is skipped or fails (both are logged as
  critical and leave it owed), COPY fails with `import is not durable against a save point fallback [...]` instead
  of reporting the import done, and its clean-up truncates the target, which was empty. COPY into a new table holds a
  non-WAL token for it, so its writer does not enrol and takes no save point. The import writes its partitions and
  merged symbol files outside a commit, so in `sync` (any target) and in `adaptive` for a WAL table the import
  created (`isImportFlushed`), the importer flushes twice. Before the first attach, `flushPartitionsToAttach`
  flushes the moved `<partition>.attachable` directories and the table root (the symbol files), because each attach
  names its partition in `_txn`, which in `sync` the writer flushes at once. After the last attach,
  `flushImportedTable` flushes `_txn` and the renames, before COPY reports the import finished. Where `syncfs` is
  filesystem-wide each is one `syncRestoredState` of the table's directory (a created table is never in a volume,
  so the same sync covers its entry in the database root); elsewhere the first walks the `.attachable` directories
  (`DurableEpochManifest.fsyncDirectoryFiles`, since `_txn` does not name them yet) and the table root
  (`fsyncTableRoot`), and the second is `fsyncTableFiles`. A data sync failure anywhere in the import, a failed
  `_txn` flush in the writer's attach included, notifies the engine at the failure factory. With `ServerMain`,
  the process halts before the import's catch can clean up: created tables and partially attached partitions may
  remain. An embedded engine fences itself but does not halt, so its catch can attempt cleanup. A nonfatal
  required-save-point failure also reaches COPY's cleanup. These are different outcomes (guide section 11.6).
  In `nosync`, `async` and for a non-WAL table in `adaptive` nothing is flushed. In `sync` the import also flushes each
  partition directory it creates in its work directory, and that directory's parent, and the attach flushes the
  partition and table directories: about 60 directory `fsync` for 30 partitions. A 5-million-row import takes
  2.52 s in `sync` against 2.36 s in the base release (+6.7%, of which the two `syncfs` are 31 ms) and 1.83 s in
  `adaptive` (two `syncfs`, 30 ms); `nosync` is unchanged. What a power loss can still leave (an attach
  whose rename did not reach the disk, partitions attached before it) and the manual steps are in guide section
  11.6. A WAL table COPY creates gets no sequencer, in every mode, a base issue (guide section 7.6), so after a
  restart it cannot be written, altered or dropped.
- **`RESUME WAL`.** Ordinary plain `RESUME WAL` is writer-free when no required logical completion is outstanding,
  as on a non-adaptive table; it no longer answers "table busy" right after a plain `SUSPEND WAL`. `RESUME WAL FROM
  TXN` on a suspended table, and a plain RESUME with a required completion outstanding (an unfinished DETACH,
  ATTACH, DROP, FORCE DROP, SET TTL or a previous FROM TXN), synchronously certify every selectable save-point
  candidate and finish the pending logical finalization before the table resumes. A skip is not recorded in the
  WAL, so one covering generation is insufficient: fallback must not replay the skipped interval. Writer
  acquisition, enrolment, save-point publication and close for that certification run outside the sequencer WRITE
  lock (an earlier revision held it, which could hang the global WAL purge off Linux). At least one fully
  validated covering image must exist; header-valid older cuts are included conservatively. A failed required
  completion leaves the table suspended; fix the logged cause and explicitly retry `RESUME WAL`.

  Suspension itself is not persisted. After a restart, recovery conservatively reconstructs a completion
  obligation for every recovered, configured-`adaptive` table (not only tables it believes were suspended, since
  that state lives only in memory and does not survive the crash): the first `adaptive` writer certifies the
  recovered cut before lazy apply resumes. This cold check can replace up to two lagging save points and finish a
  pending DETACH finalization; a failure there leaves the obligation pending for the next RESUME. It is a
  once-per-recovered-table cost at the first writer open after a restart, not a per-batch check; it has not been
  measured.
- **Direct logical partition changes** (ATTACH, DETACH, DROP, FORCE DROP and SET TTL through the direct writer or
  its queue) use the same synchronous completion rule, including partial-list failures and no-op retries. These
  commands have no WAL replay owner; retaining the original bytes does not preserve the requested absence or
  membership. Structural column-type changes are not eligible for the suspended-table bypass. Physical partition
  conversions and background storage-policy tasks retain their existing publication cadence; they do not acquire
  a blanket two-save-point rule.
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

- **Replicas take no new save points.** A replica installs `LocalDurabilityPolicy.REPLICA_SKIP`: its writers take
  no save points and its durable source is the object store. Inherited save points still protect their WAL until
  safely removed. Startup recovery runs before role installation, including over inherited anchors (section 8).
  Once reconciled out, the replica has no adaptive rewind; ATTACH and DETACH finish at once (section 10), and it
  never asks for an idle save point (section 9). With `adaptive` configured, structural flush gates on applied
  state and on the downloaded
  log resolve to `nosync` through the live policy (`CairoEngine.getResolvedCommitMode` and
  `TableWriter.getFlushCommitMode`). Explicit `sync` replicas remain `sync`. The promote flush, boot before
  role installation, and local commits (including REBASE staging and view definitions) keep their configured
  grade: the object store does not hold those local writes. Every-mode repair barriers remain unchanged.
- **Demote** (`ReplicaRoleState.clearAdaptiveEpochArtifacts`). For each WAL table with a save point file, the role
  switch takes the table's writer without waiting (`getWriterUnsafe`). If it gets it, the writer's reconcile makes
  the table durable and removes the save point in two steps (section 7), and outside `adaptive` a leftover save
  point is removed under the same lock. In `adaptive` a save point the reconcile could not remove (its flush
  failed, say) stays, with the WAL it keeps, and the table counts as failed: removing it without the flush could
  lose the rows applied lazily since. If the writer is busy, the table is skipped and that writer's own reconcile
  does the same at the end of its current batch, or before its next one, on its own thread, so the removal never
  races a save point being written. A table that fails for another reason is logged and left to its writer; a
  failed flush follows the engine's resolved mode (section 9): in `adaptive` the table keeps its save point
  and WAL floor and retries the flush and removal before each batch; in `sync` it stops the engine. The switch logs `cleared adaptive save points on becoming a replica [tables=...,
  busy=..., failed=...]`. On Linux each enrolled table costs one `syncfs` and two `fsync`, about 19 ms with the
  writer open and the removal: about 19 s for 1,000 tables before the replica loop starts. One `syncfs` for every
  table, then the removals, would bring that near the writer opens alone; that batching is left for later.
- **Promote** (`EntCairoEngine.fsyncReplicaStateForPromote`, on every promote path). On an `adaptive` instance, before
  the apply job is set on the tables, one flush makes what the replica applied lazily durable
  (`DurableEpochManifest.fsyncTables`: one `syncfs` of the database root and of each volume on Linux; elsewhere the
  tables one by one, each file flushed, a failure logged per table). Each table then enrols at its writer's first
  batch with a whole-table flush. On Linux the promote flush took 41 ms for 200 tables, and each table's first save
  point after it about 34 ms, once (35 ms against 0.8 ms for its next apply). A batch that began on the replica and is
  still running at the promotion keeps applying lazily after that flush, and the promotion's notification for its busy
  table can be dropped, so `ApplyWal2TableJob` reconciles the writer again at the end of every batch, while it still
  holds the writer. Outside `adaptive` that call is one field compare, and in steady state one volatile read; it skips a
  distressed writer, one a DROP TABLE destroyed included. If such a straddling batch fails, the writer is distressed and
  not reconciled, so the rows it applied after the promotion flush have no flush and no save point until the table is
  resumed or the server restarts; a power loss in between can lose them (very unlikely; no cheap safe fix, since a
  writer the job stopped trusting must not publish a save point).
- **The uploader waits for the durable frontier.** In `adaptive` and `sync` mode the uploader ships a sequencer
  record only once the local durable frontier covers it, for V1 and V2 logs alike, so a power loss can never make
  the primary reuse a transaction number the object store already holds. While it waits it logs `holding uploads
  past the local durable frontier [...]` once. The frontier is read through an up-call on the uploader's thread,
  and an answer that cannot be proven holds the table: a live table the start flush left unproven (its scan skipped
  the table, or its cover failed on an error that is not a flush failure) is proven at its first upload by the same
  flush the start makes (`WalUploader.coverTable`), and if that fails too the table stays held, with a log line
  naming the table and the error, and is asked again after the uploader's retry window. The up-call answers
  "nothing to hold" only for a table that is purged or whose drop is marked after its log was flushed. A table an
  earlier run dropped whose sequencer `_meta` is torn (a crash between the drop's log append and the `_meta`
  update) ships its drop after filesystem-wide `syncfs` on Linux; elsewhere it ships with a critical line,
  without a `sync(2)` fallback. Holding it would resurrect an acknowledged DROP on a fail-over.
- **The uploader start flush** (`WalUploader.coverEarlierRecords`). At boot and at promotion, in `adaptive` and `sync`
  mode, the uploader makes every table's sequencer log durable before its first upload, then moves each table's
  durable frontier over the records the flush covered: one `syncfs` of the database root and of each linked volume on
  Linux, which reaches every part file; elsewhere each table's own flush, `TableSequencerAPI.fdatasyncTxnLog(token,
  0)`, which in a V2 log first flushes every part file it still keeps (listing `_txn_parts`, so a part kept behind
  one the purge deleted is reached), then the open part and the header. A commit before this step moves the
  frontier after flushing only the open part, so the walk starts at part 0. The step runs in
  `PrimaryRoleState.openLoops` before the uploader is installed as the WAL listener: a commit notified during the
  flush could ship the older records of a table the flush has not reached; a commit made meanwhile is shipped by the
  startup scan (`triggerUploadMissed`) right after the install. A dropped table's log is flushed the same way, from
  part 0 where `syncfs` is not filesystem-wide, and the table then reads as dropped at its first upload, with no
  sequencer open. On Linux the `syncfs` also runs when the scan skipped a live table (it logs a critical line with
  the cause), so that table's older V2 parts are durable before its first upload proves the open one. A data sync
  failure stops the engine; any other failure leaves the table to its first upload, above. On Linux, 1,000 tables take
  37-109 ms in `adaptive` and 21-23 ms in `sync` when every table has one new commit (the `syncfs` is 90-98% of it),
  2-8 ms when none has. The step reads each table's last transaction through `TableSequencerAPI.lastTxn`, which opens
  the log: on the first such start after a `nosync` or `async` run that open also reads and flushes each V1 log once
  (section 13), 3-4 s for 1,000 tables, before the first upload. Outside Linux, flushing every kept part costs about
  +50% over the open part and the header alone (1,000 busy tables: 10.0 s against 6.6 s, measured on Linux with that
  path forced).
- **The V1 log on a replica.** The downloader is the only writer of a replica's V1 sequencer log. A Java open leaves
  records past the published tail alone (the downloader wrote them and has not published them yet); a torn tail is
  still cut.
- **Backup restore** (`BackupRestoreAgent`). The restored table's save point is removed before its files are written,
  and the first `adaptive` writer enrols it. In every mode, each restored table is flushed before its commit marker
  (one `syncfs` on Linux; every file and directory of the table elsewhere, files only on Windows), because a resumed
  restore skips a table whose marker matches and nothing else flushes a table without a save point; the root is
  synced once more before the trigger file goes. On Linux that is about 11 ms per table in every mode: 200 small
  tables restore in 5.3-5.6 s against 3.2-3.3 s with the flushes turned off.
- **Default mode.** Without an explicit `cairo.commit.mode`, Enterprise runs `nosync` when replication is configured.
- **Durable acknowledgements.** `DurableUploadRegistry` composes the local and replicated frontiers on an
  eligible primary. The local gate uses the resolved mode and refuses a fenced engine; replicated follows the
  upload frontier. A both-tier client trims only on replicated progress. On a primary demote, the frozen registry
  denies every *new* LOCAL handshake, but it still answers the LOCAL tier for a client whose grant predates the
  freeze from the real engine-backed local frontier, not from the frozen upload snapshot — so an already-connected
  LOCAL client keeps getting real acknowledgements through the demote instead of stalling for over 10 seconds
  waiting on a frontier the snapshot never updates. Frames already collected for a blocked send retain their
  snapshot. The server sends on inbound traffic or PING, not directly from a flush callback. Uploaded progress
  alone is not proof of local durability: ordinary `adaptive`/`sync` uploads are gated on this local frontier, but
  boot-index and carry-forward watermark seeding bypass that gate, so only freshly-uploaded transactions carry the
  proof.

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
- **Version 2 sequencer log: the torn tail (`adaptive` only).** The header keeps, at offset 40, the first transaction
  a completed flush may not have covered. Only an `adaptive` node that keeps the log durable itself moves it, after a
  flush of the part and the header, so every record at or above it was written by this release with a checksum. An
  `adaptive` open scans from it (no lower than the last part's first transaction) to `maxTxn` and cuts before the
  first record whose checksum is 0 or does not match (`dropping torn sequencer txnlog tail [...]`); a record below it
  is never cut (`TableTransactionLogV2.recoverTail`). An open that cuts nothing leaves the bytes past the tail alone,
  so a replica's downloader can write a batch past `maxTxn` before it advances it. Before the first `adaptive` append
  of a log instance into a part, the rest of that part is checked and, if any byte is non-zero, zeroed and flushed
  (`zeroPastTail`), so a stale record can never look valid after the next power loss. A `sync` or `adaptive` log
  flushes the part it leaves before it moves to the next one. This does not reach earlier parts a `nosync` or
  `async` run already left; the `adaptive` reopen's walk below reaches those.
  A field of 0 trusts every record: an older log, a log no `adaptive` append has touched, or one a replica's
  downloader appends to (Enterprise clears the field when its downloader advances `maxTxn`, and a replica's first
  open clears it too). A `nosync`, `async` or `sync` open on a node that keeps the log durable itself stores the field
  negated, as a V1 log does: those modes do not maintain the `adaptive` flushed-through claim, and the slot keeps
  following `maxTxn`. The existing `sync` part flushes are unchanged. The next `adaptive` open scans the last part
  from the field's magnitude, flushes every kept part from the magnitude's part to the tail's (one `fdatasync` each,
  found by listing `_txn_parts`), and only then stores the field at `maxTxn + 1` and the slot at `maxTxn`, so a power
  loss in the kernel's writeback window after that start cannot lose a part an earlier record of the log depends on.
  A power loss before the claim is stored leaves the negative field, and the next `adaptive` open does the same work
  again. A checkpoint restore that lowers `maxTxn` clamps the field's magnitude to at most `maxTxn + 1` and the slot
  to at most `maxTxn` with it, in the same sector (`TableTransactionLogV2.lowerMaxTxn`, also used by the cut), so
  records an older binary appends after a downgrade sit above the slot and are trusted at the next open instead of
  being cut.
  The header slot next to the field (bytes 48-55) holds `maxTxn` as this release last stored it, while the field is
  set: it is stored before a `maxTxn` it raises and after one it lowers. A writer that does not know the field (an
  older binary after a downgrade, the downloader) moves `maxTxn` alone, so a `maxTxn` above the slot means the records
  above it carry no checksum. Every open of a node that keeps the log durable itself then trusts the whole log and
  clears the field (`trusting sequencer txnlog records appended without a flushed-through txn [...]`); the next
  `adaptive` append starts it again above them.
- **Version 1 sequencer log: the zero tail.** A V1 log has no room for a checksum. Instead, an open scans the records
  after a hint kept in the header (offset 40: the first transaction a completed flush may not have covered) and cuts
  the tail at the first record that cannot be whole (zero WAL id, zero timestamp, a gap in the structure version): it
  lowers `maxTxn`, zeroes the bytes after it and flushes (`dropping torn sequencer txnlog tail [...]`). An open that
  cuts nothing leaves the bytes past the tail alone; the first append after it zeroes and flushes them if any are
  non-zero, so a stale record can never look valid after the next power loss. `sync` and `adaptive` commits, every
  structural change (`fullSync`) and a clean close of the sequencer move the hint, each after a flush of the records
  it covers. The clean close (`TableSequencerImpl.closeLocked` -> `TableTransactionLogV1.flushOnCleanClose`) flushes
  the log when the hint is behind the tail, in any mode, and then stores the tail without a flush: a power loss
  that loses that store costs the next open a check of the records. It skips a distressed sequencer and a table
  whose durable frontier is fenced, as a flush after a failed one can succeed without the lost pages; a failed close
  flush fences the frontier and is logged (`could not flush sequencer txnlog on close [...]`), and the close goes
  on. A `nosync` or `async` open scans from the hint, flushes nothing and stores the hint negated: the next `nosync`
  or `async` open scans only the records after it, but a `sync` or `adaptive` open that finds it negative scans the
  whole log from transaction 1, flushes it and stores a positive hint (`TableTransactionLogV1.recoverTail`). So the
  first `sync` or `adaptive` start after a `nosync` or `async` run that did not stop cleanly pays one `fdatasync`
  and a full read of every V1 log, once per table: 3-4 s for 1,000 tables against about 50 ms at the next start, and 0.27 s for a log of
  a million transactions read from the disk (guide, section 9). It runs at each log's first open: in QuestDB the
  `metadata-hydration` thread opens every WAL table's log right after the start
  (`CairoEngine.hydrateRecentWriteTracker`), unless a write gets there first. Damage to a V1 record already written
  (bit rot, a foreign write) is not detected.
- **WAL segment files.** Apply relies on WAL files becoming durable before sequencing under `sync` and
  `adaptive`; it performs no extra per-apply file-length or last-aux-entry validation. `nosync` and `async` make
  no power-loss durability promise, so their lost or torn WAL bytes need not be replayable after a crash.

A torn `_txn` or `_cv` live area makes a reader stay on the previous transaction (`_txn live area is torn, reader
cannot advance past the previous transaction [...]`) and a writer refuse to build on it, rather than read wrong data.
Under `adaptive`, recovery restores these files from the save point before anything reads them.

The `_txn` and `_cv` checks ask the file's length during initial-open verification, retaining verification after
failure or fallback. Ordinary live refreshes, including mapping growth, do not repeat the checks. The first-open
cost is three more `fstat` of `_txn` and one of `_cv`, previously measured at about 9 µs in every mode.

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
| sequencer V2 part files and `_txnlog` | a checksum in the 8 reserved bytes of each record; in the header, the flushed-through field at offset 40 (negated by a `nosync`, `async` or `sync` open, positive again after the next `adaptive` open) and the slot holding `maxTxn` as this release last stored it at offset 48 (both written only once an `adaptive` append has touched the log) |
| sequencer V1 `_txnlog` | the unsynced-from hint at header offset 40 (negated after a `nosync` or `async` open) |
| `_meta` | none: the base layout (format minor version 2). A development build's enrolment field still loads. |
| `_convert` | none |

Not added, though earlier development builds had them: `_snapshot`, `_meta.epoch`, `_cv.epoch`, `_txn.epoch` (and
their `.0`/`.1` forms), `_epoch.enrol`, `_convert.replay`, `_event.c`, `_txnlog.c`, `_chk`, a `_meta` enrolment
field and body checksum. The save point removal deletes the first four when it finds them (section 7).

## 15. Caveats

1. The configured mode is fixed for the process lifetime; Enterprise's resolved apply grade can change with the
   role. There is no per-table mode.
2. `syncfs` flushes the whole file system, so a save point's cost depends on unrelated dirty data on the same file
   system.
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
   process's `syncfs` collected first, or any error on an older kernel, goes unseen. The save point's batched flush
   (for every file but the open partition's columns and the symbol maps, whose write-back its drain waits on and
   checks), the exit, the recovery header flush and the Enterprise uploader start flush rely on that report alike.
8. Downgrade, write with an older binary, then re-upgrade is unsupported. V2's known-tail slot avoids cutting
   records appended by a writer unaware of its hint; that defensive handling does not broaden the compatibility
   contract. A plain upgrade and mode changes between restarts remain supported.
9. Deferred cost changes: Enterprise demote batching, reducing SYNC V2 part flushes, and splitting the macOS
   `F_FULLFSYNC` upgrade out of `fsyncAndClose`. Current barriers remain; the latter two need measurements first.
   Completion/open can also validate full manifests per pending partition; optional batching is deferred and no
   final cost bound has been measured. Rust error classification across JNI is deferred, as section 9 describes.

## 16. Reading validation evidence

A unit test's simulated power loss, a process kill, a fresh-engine restart and a VM block-write replay test
exercise different failure models. None by itself proves behaviour under every physical power loss or storage
failure. See [the durability harnesses](../tools/durability/README.md) for their mechanisms and limits.

Tests cover the pending-frontier race, generation fallback, required DETACH/RESUME/direct-change completion,
resource ownership after failed publication, and fatal-failure fencing. CI configuration is not evidence that a
particular revision passed: retain the exact revisions, command-selected reports, mode and negative controls.
Historical benchmark numbers do not certify the final synchronous-completion overhead.
