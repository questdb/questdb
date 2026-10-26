# Adaptive commit mode

A guide for people who run QuestDB: what the `adaptive` commit mode does, what it promises, what it does not, what it
costs, and what to do when it stops a start. How it works inside is in
[`adaptive-commit-internals.md`](adaptive-commit-internals.md).

The shipped default is `nosync`. `adaptive` is an opt-in:

```properties
cairo.commit.mode=adaptive
```

## 1. The problem

QuestDB writes rows in two stages. A commit first appends the rows to the table's write-ahead log (WAL) and is
acknowledged. A background job then applies the WAL to the table files: the column files, `_txn` (which says which
rows and partitions exist), `_cv` (column versions) and `_meta` (the schema).

In `nosync` mode nothing is flushed to the disk. The operating system writes the pages back when it chooses. A
process crash loses nothing, because the page cache survives it. A power loss or an operating-system crash can lose
recently acknowledged commits, and can leave the table files torn or pointing at data that never reached the disk.

In `sync` mode every commit and every apply pass waits for every mapped file it touched to be written back. That is
slow: about 12 ms for a one-row commit on a fast NVMe disk, and the apply job pays about the same again for every
pass, because it flushes the large table files as well as the small log.

## 2. How adaptive solves it

Adaptive makes the WAL durable and leaves the table files lazy:

- **The WAL becomes durable before the durable frontier advances.** The writer flushes its private WAL files
  before sequencing. With the group-commit window at 0 it also flushes the sequencer log before acknowledging the
  commit. With a window above 0 an ordinary commit acknowledgement can precede that shared flush; only a durable
  acknowledgement confirms stable storage (sections 4 and 5).
- **The apply is lazy.** The WAL is applied to the table files exactly as in `nosync`: no flush per commit. The
  table files are a copy of the WAL that can be rebuilt.
- **A save point bounds the rebuild.** Once a minute, or after 5 million applied rows, whichever comes first, each
  table takes a save point: it flushes the table data it names, then writes a durable copy of `_txn`, `_cv` and
  `_meta` into one of two save point files, `_epoch.manifest.0` or `_epoch.manifest.1`. A clean shutdown takes a
  last one, and a table that stops changing takes one more about one interval after its last change (section 9).
- **Start-up recovery puts the table back at its save point.** After a crash, the start writes the save point's
  copy back over the live `_txn`, `_cv` and `_meta`, and the ordinary WAL apply replays every transaction after
  it. The table keeps its last two save points, so a start can fall back to the older one if the newer one is
  damaged: the WAL after the older save point, and the partition versions either one names, are kept until a newer
  save point replaces it.
- **Damage is detected.** `_txn` and `_cv` carry checksums, and so does every record of the version 2 sequencer
  log. A torn file is noticed instead of being read as data.

After recovery and WAL catch-up, every transaction covered by the durable frontier is back and the table files
are consistent. With a nonzero group window, ordinary acknowledgements ahead of that frontier remain at risk.
These guarantees require storage that honours flushes; the limits in section 7 still apply.

Adaptive covers WAL tables. A table created with `BYPASS WAL` has no WAL to replay and behaves as in `nosync`.

Some operations cannot rely on WAL replay alone: DETACH completion, RESUME and direct logical partition changes
require every selectable save point to cover their effect. They can take a second synchronous save point and fail
if coverage cannot be established. This completion is distinct from an earlier acknowledgement that merely
queued a command in WAL. Ordinary data commits and physical conversions do not acquire a blanket two-save-point
rule; see the internals guide for the individual paths.

## 3. Turning it on

```properties
cairo.commit.mode=adaptive
```

The mode is one setting for the whole instance, read once at start. Every table answers to the same mode, so a
client can learn the durability promise before it names a table. The accepted values are `nosync`, `async`, `sync`
and `adaptive`. Any other value stops the start:

```
invalid configuration value [key=cairo.commit.mode, value=<value>, expected=nosync|async|sync|adaptive]
```

QuestDB Enterprise defaults to `nosync` when replication is configured: a replicated deployment already survives the
loss of a node. An explicit `cairo.commit.mode` always wins.

Existing tables need no migration. After the restart, each WAL table enrols when a writer first opens it: it takes
its first save point, which flushes every partition of the table (one `syncfs` on Linux, a flush per file on macOS
and Windows). The writer takes it before it applies anything, so no table is ever applied lazily without a save
point behind it.

On macOS and Windows a new writer cannot tell what an earlier writer of the table left unflushed, so the first save
point of every writer flushes every file of every partition and of the sequencer again: once after each start, and
again whenever the table's writer was closed after `cairo.inactive.writer.ttl` (10 minutes by default) without use.
The cost grows with the number of partitions and columns. On Linux the one `syncfs` covers it.

## 4. The group-commit window

```properties
cairo.adaptive.commit.group.window=50ms
```

The window trades the recovery point (how much acknowledged data a power loss can take) against commit latency.

- **0: every acknowledged commit is on stable storage.** The commit flushes its own WAL files, then the sequencer
  log, and returns. Nothing acknowledged is lost on a power loss.
- **Above 0 (default 50 ms): the sequencer flush is shared.** The commit still flushes its own WAL files before it
  takes a sequence number, so the sequencer can never name data that is not on the disk. The flush of the shared
  sequencer log waits until the window's oldest commit is one window old, and one flush covers every commit since.
  Most commits return before that flush; the commit that finds the oldest pending one a window old makes the flush
  itself before it returns. A power loss can lose the commits acknowledged in the last window.

  The window shares the flush only among commits made through a writer that stays open between them: ILP over TCP,
  ILP over HTTP on a kept-alive connection, and QWP. A SQL `INSERT` takes a table writer for one statement and hands
  it back when the statement ends, and handing a writer back flushes the sequencer log at once. So each `INSERT`
  through the HTTP `/exec` endpoint or the embedded API, and each PGWire Sync (once per statement, or once per
  batch for drivers that batch), pays that flush before it returns, as with a window of 0. Those commits are on
  stable storage when they are acknowledged. The flush holds the table's sequencer lock, so commits from other
  writers to the same table, ILP and QWP included, wait for it.

When commits stop in the middle of a window, the last flush is made by a background job that runs on the shared
worker pool. Its nominal delay is the window plus the pool's sleep time (`shared.worker.sleep.timeout`), with
scheduling and flush time on top. This is not a hard wall-clock loss bound if workers stall or storage is slow.
Keep that sleep short when the window is very small. A clock that jumps backwards forces the flush at once.

A negative value counts as 0. A client that needs to know when its rows are durable, whatever the window, asks for
the durable acknowledgement (section 5).

`cairo.wal.commit.writeback.drain` (default `true`) starts the write-back of every file of a WAL segment before the
commit's flushes, so the device writes them in parallel. It changes speed, never durability: every flush still
runs.

The cost of a commit is per commit, not per row: a commit flushes each WAL column file it wrote. Batching rows
amortizes it, and a wide table pays more per commit than a narrow one.

## 5. Durable acknowledgements over QWP

A QWP client can ask the server to say when its commits are durable. It sends the handshake header
`X-QWP-Request-Durable-Ack` with the tiers it wants:

| Value | Meaning |
|---|---|
| `local` | the commit's WAL is on this server's stable storage (`adaptive` mode only) |
| `replicated` | the commit reached the object store (QuestDB Enterprise primary only) |
| `local,replicated` | both, as separate acknowledgement streams |
| `true` | the older spelling of `replicated`; the server answers `enabled` |

The server grants the whole request or none of it. A grant echoes the tiers in `X-QWP-Durable-Ack`. A refusal
leaves that header out, and the client fails the connection with a durable-ack mismatch instead of waiting for
acknowledgements that never come. It never grants a weaker tier than the one asked for.

- The `local` tier is served only when the resolved mode is `adaptive` and the engine has not been fenced by a
  fatal flush failure. An Enterprise replica resolves configured `adaptive` to `nosync` and cannot grant it.
  The tier reports the local durable frontier (`STATUS_LOCAL_DURABLE_ACK`, 0x0E). It survives a power loss,
  not loss of the disk.
- The `replicated` tier reports the upload frontier (`STATUS_DURABLE_ACK`). OSS cannot grant it. Enterprise's
  upload-backed registry composes both tiers on an eligible primary; this is implemented, not deferred.
- With both tiers requested, the client trims its store-and-forward copy only on the replicated acknowledgement;
  local acknowledgements are progress signals. `request_durable_ack=on` retains the legacy `true`/`enabled` wire
  exchange and means replicated, never local.

The server sends these frames in response to inbound traffic, including the client's keepalive PING. The Java
client defaults to a 200 ms keepalive while awaiting durable acknowledgements. Idle acknowledgement latency
therefore includes keepalive, scheduling and network delay; it is not the group-commit window.

A store-and-forward client keeps its copy of the rows until the strongest tier it asked for acknowledges them, and
can then replay without duplicates. A table dropped while a connection waits is acknowledged as covered. When a
table name moves to a new table during a connection (dropped and created again, or renamed), the server shifts the
new table's sequence numbers on that connection above every value it reported for the name before, so the client's
watermark never goes backwards; a new connection sees the real values again.

## 6. What adaptive promises

These hold for WAL tables on an instance in `adaptive` mode. On QuestDB Enterprise they hold on the primary.

1. **No acknowledged transaction is lost after a crash.** After a power loss, an operating-system crash or a
   killed process, and once the WAL apply has caught up after the restart, the table holds every transaction the
   sequencer holds. With the window at 0 that is every acknowledged transaction. With a window above 0 it is every
   transaction whose durable acknowledgement was sent (section 5); commits acknowledged within the last window
   before a power loss may be missing. After a killed process (the page cache survives) nothing acknowledged is
   lost, in any mode.
2. **The table is consistent after recovery.** `_txn` names only partitions and rows that are on the disk, and
   `_meta`, `_cv` and `_txn` agree on their versions.
3. **Recovery does not silently trust a damaged save point.** It uses the newer valid generation, or falls back
   to the older one with its retained WAL and partition versions. If neither is trustworthy, or a required read,
   write or flush fails, startup refuses rather than silently using unsafe live files. Section 11 lists recovery
   steps; damage outside the protected files can still require a backup.
4. **Recovery can itself be interrupted.** A crash or power loss at any step of the start, and a second start,
   give the same result as one uninterrupted start.
5. **A clean restart restores nothing.** After a clean shutdown, the start finds the table files equal to the
   save point, writes nothing to them and counts no recovery (with `cairo.adaptive.epoch.flush.on.close` at its default,
   `true`).
6. **The mode can change between any two restarts**, in any direction and any number of times, including the
   restart right after a crash. Section 8 says what each switch does.
7. **Tables from a released version need nothing.** Their files carry no save point and no checksums. They read
   as not yet enrolled and unverified, never as damaged, and the first adaptive writer enrols them.
8. **Other modes retain their durability grades, with explicit changes.** `nosync` and `async` gain no power-loss
   guarantee. Detection, recovery checks and the fixes in section 8.1 also run outside `adaptive`. Section 9 gives
   historical measurements, not a blanket claim that every current path has identical cost or system calls.

## 7. What adaptive does not promise or support

### 7.1 Downgrade, write with the older release, then upgrade again

Not supported. Running this version, going back to an older release, writing with it, and then running this
version again has undefined results: the older release does not maintain the checksums and save points this
version relies on.

**The safe way back to an older release:**

1. Set `cairo.commit.mode=nosync` (the default) and restart this version once. Let the start finish: it makes
   every enrolled table durable and removes its save point files.
2. Stop QuestDB cleanly.
3. Install the older release.

Do not come back to this version after the older release has written to the database.

### 7.2 The old UDP line protocol receiver into WAL tables

The old UDP receiver (`line.udp`, off by default) writes into a table directly, outside the WAL. In `adaptive` mode
the start-up recovery would roll such rows back, so the receiver refuses WAL tables: it logs an error, skips the
lines for that table until the restart, and does not create new WAL tables. Non-WAL tables and the other modes
behave as before. The QWP UDP receiver writes through the WAL and is covered like any other client.

### 7.3 Non-WAL tables

A table created with `BYPASS WAL` has no WAL to replay. In `adaptive` mode it behaves as in `nosync`: a power loss
can lose its recent commits.

A parallel COPY into a new table creates that table as non-WAL, whatever `cairo.wal.enabled.default` says, so it is
in this group too: a power loss soon after COPY returned can lose or damage the table. To keep the promise, create
the table first (`CREATE TABLE ... WAL`) and COPY into it.

### 7.4 `RESUME WAL FROM TXN` on a table with held-back transactions

When the WAL apply stops in the middle of out-of-order data (its time quota, a shutdown, an error), the table holds
some applied transactions back in `_txn` (the WAL lag). `ALTER TABLE ... RESUME WAL FROM TXN` on such a table loses
one good transaction, in every release and in this version
([questdb#5422](https://github.com/questdb/questdb/issues/5422)). This version changes which transaction is lost in
one case and removes an endless apply loop and stray rows in another, but it does not fix the loss. A plain
`RESUME WAL` (without `FROM TXN`) is not affected.

### 7.5 Other limits

- **Storage that ignores flushes.** The promises require flushes to reach stable storage through the filesystem,
  virtual disk, host and physical device. Disabled barriers or a layer that acknowledges flushes without persisting
  the data can break them. A cache-mode name or host OS alone establishes neither safety nor failure. This feature
  adds no startup check that certifies the storage; validate the actual stack.
- **The last window.** With a window above 0, commits acknowledged in the last window before a power loss can be
  lost (section 4). Use a window of 0, or the QWP durable acknowledgement, where that matters.
- **Enterprise replicas.** A replica keeps no new save points; demotion removes inherited ones only after their
  state is durable. The object store is the durable source, but the downloader resumes from the local sequencer:
  this feature does not promise to re-download and repair all local state automatically after a replica power
  loss. ATTACH and DETACH PARTITION finish at once on a replica, as outside `adaptive`. With `adaptive`
  configured, the resolved structural apply-state and downloaded-log gates use `nosync` grade. This does not change other cold-storage flush gates
  or mandatory repair barriers; explicit `sync` replicas, local commits, promotion and boot retain their
  configured grade.
- **A power loss during a `nosync` or `async` run** loses what those modes lose, also after a switch to `adaptive`
  later.
- **Detection is for torn writes.** Damage to an already written record of a version 1 sequencer log (bit rot, a
  foreign write) is not detected. The checksums are not a full integrity check.
- **The save point is not a backup.** It bounds replay, but does not protect against losing the disk or all valid
  copies. Checkpoints and backups are still needed. Their restore paths have additional flushes (section 8.1).
- **A role switch in the middle of an apply batch that then fails** (QuestDB Enterprise). If a node is promoted while
  an apply batch that began on the replica is still running, and that batch then fails, the rows it applied after
  the promotion are neither flushed nor covered by a save point until the table is resumed or the server restarts.
  A power loss in that window can lose them. It needs a promotion, a failing batch and a power loss together.
- **A crash soon after a storage policy step** (QuestDB Enterprise). The storage policy job applies some steps to a
  table directly, without a WAL transaction: a local drop of an uploaded partition, a Parquet commit, rewrite or
  squash, and its bookkeeping steps. A crash within one save point interval of such a step puts the table back at
  the save point before it. The files the step replaced are still there (the save point keeps them), and the policy
  job does the step again at its next pass. No row is lost. A clean shutdown keeps the step.
- **A power loss during a parallel COPY into an existing WAL table** that has not reported it finished can leave
  the table in a state that needs a manual step (section 11.6). Nothing such a COPY reported done is lost. A COPY
  into a new table has no promise in `adaptive` (section 7.3).
- **QuestDB Enterprise: the view audit table on a replica has no working manual recovery step.** The replica
  applies it without a save point and purges its WAL once applied (it is the one table a replica writes itself).
  A power loss can lose its most recent records and leave it suspended. `ALTER TABLE "sys.view_audit" RESUME WAL
  FROM TXN <n>` is refused unconditionally; plain `RESUME WAL` only retries the already-failed transaction, and
  `wal_tables()` does not expose the first transaction whose WAL still exists, so there is no `<n>` to compute
  either. The lost records cannot be recovered; the table stays suspended until this is addressed in a follow-up.
  Replicas keep `nosync` grade even when configured `adaptive`: taking save points on a replica needs the
  downloader to flush every WAL file it downloads, which can slow replication down, so it is deferred.
- **macOS and Windows: a partition directory missing from disk stops the save points.** There the save point
  flushes partition files one by one. A partition that the transaction file lists but the disk lacks (for example,
  removed by hand) makes every save point of that table fail until the partition is dropped. No data is lost, but
  the table reports errors, and a FORCE DROP of another partition takes effect yet still reports the failure.
  Linux's filesystem-wide flush does not notice the missing directory.
- **QuestDB Enterprise: an incremental backup can ship records past the local durable frontier.** The backup
  checkpoint does not consult `localDurableSeqTxn`, so with a group window large enough (or a stalled flush) to
  outlast the backup, the backup can capture a transaction before it is durable. A power loss that then makes the
  primary reissue that transaction number with different content leaves the backup's copy permanently stale for
  that partition; a restore brings back the old content with no error and no suspension. This mechanism exists
  identically in `nosync` today (the shipped default); `adaptive`'s exposure window is much narrower (it needs a
  group window or a flush stall that outlasts the backup).

### 7.6 Known issues in the other modes, outside this change

Found while this mode was built, reproduced on the current release line, and filed:

- `sync` schema changes that can lose data on a power loss right after them: ALTER COLUMN TYPE values
  ([#7843](https://github.com/questdb/questdb/issues/7843)), ALTER COLUMN TYPE to SYMBOL
  ([#7844](https://github.com/questdb/questdb/issues/7844)), ADD INDEX
  ([#7845](https://github.com/questdb/questdb/issues/7845)).
- `sync` WAL ingestion spends most of its time flushing pages ([#7846](https://github.com/questdb/questdb/issues/7846)).
- A process killed in the middle of ATTACH PARTITION loses the partition, except on an enrolled adaptive table
  ([#7840](https://github.com/questdb/questdb/issues/7840)).
- POSTING index files left behind by a process killed in the middle of out-of-order apply
  ([#7841](https://github.com/questdb/questdb/issues/7841)); in `adaptive` mode the start-up recovery cleans them.
- The old UDP receiver: a commit list that loses rows ([#7847](https://github.com/questdb/questdb/issues/7847)), a
  column added to a WAL table ([#7848](https://github.com/questdb/questdb/issues/7848)), and a writer cache that
  sends rows to the wrong table ([#5151](https://github.com/questdb/questdb/issues/5151)).

### 7.7 Deferred work and accepted costs

- The shipped default remains `nosync`; changing it needs a separate decision supported by measurements.
- Enterprise demotion still flushes enrolled tables individually. Batching those flushes is a follow-up, not part
  of this implementation. Batching repeated full-manifest checks per pending partition is also deferred; neither
  that cost nor final synchronous two-generation completion has a fresh measured bound.
- Rust flush errors do not yet carry the Java fatal-durability classification across JNI (section 11.2).
- The extra SYNC V2 part flushes and macOS `F_FULLFSYNC` in `fsyncAndClose` remain. Reducing either cost awaits
  measurement; the Linux figures do not establish their macOS cost.
- There is no database-level marker to skip save-point checks at a `nosync` start. The measured startup cost was
  accepted, as were the reader length checks and SYNC COPY directory flushes (section 9).
- No per-table startup quarantine or general repair of arbitrary corrupt storage is provided. Some failures stop
  startup and require the manual steps in section 11.

## 8. Changing the commit mode

`cairo.commit.mode` can change at any restart, in any direction, any number of times.

- **Into `adaptive`.** Each WAL table enrols at its first writer open (section 3).
- **From `nosync` or `async` into `sync` or `adaptive`.** After a `nosync` or `async` run that did not stop
  cleanly, the first start in the new mode reads each WAL table's version 1 sequencer log (the default) once from
  its first transaction and flushes it, because the records the `nosync` or `async` run wrote were never flushed and
  a durable mode does not trust them unchecked. A clean stop flushes each log the run wrote to, and the start skips
  it (section 8.1). The first open
  of each log pays it, once per table: in QuestDB the background table scan right after the start (or the table's
  first write, if that comes first), in QuestDB Enterprise the uploader start, before any upload. Later starts in a
  durable mode skip it. It costs one `fdatasync` per table and a read of the log: 3-4 s for 1,000 tables in our
  measurement (section 9). A version 2 sequencer log that an `adaptive` run had written is marked at its first
  `nosync`, `async` or `sync` open; the first `adaptive` start after such a run flushes each such table's kept log
  parts (one `fdatasync` per kept part, plus the header) at the log's first open, once per table, before it trusts
  the log again. Later `adaptive` starts skip it.
- **Out of `adaptive`** (to `nosync`, `async` or `sync`). The first start in the new mode makes every enrolled table
  durable (one `syncfs` per file system on Linux, the root and each volume; a flush of each table's files elsewhere),
  then removes its save point files and logs `table left adaptive commit mode, its save point is removed [table=...]`.
  From then on the table behaves as in the new mode, and the WAL is purged as usual. A table with a conversion to
  non-WAL pending is left to the conversion, which removes the save point itself.
- **Right after a crash.** The start recovers first, in any mode: it puts each enrolled table back at its save
  point, and the WAL apply replays the rest. Only then does the switch above take effect. A crash that happened in
  `adaptive` mode is therefore repaired even when the next start runs in another mode.
- **A conversion to non-WAL** (`ALTER TABLE ... SET TYPE BYPASS WAL`, carried out at the next start) applies every
  outstanding WAL transaction first, in every mode, flushes the table, then removes the save point and converts. If
  that replay cannot finish, the conversion is refused and the table stays a WAL table (section 11.4). A start in
  `adaptive` keeps the refused table's save point; a start in another mode flushes the table and removes its save
  point, as the switch above does.

### 8.1 What changes in `nosync`, `async` and `sync`

- **Torn-file detection** runs in every mode (section 2). The historical measurements found no extra per-commit
  system call for the checksum work and about 9 µs on the first reader (four file-size checks). WAL column length
  guards also refresh a cached length when the committed range grows past it. Each V1 sequencer open scans from
  its hint, and a torn tail that it cuts is flushed in every mode; mode-switch rescans are described in section 8.
  In `nosync` and `async` a clean close of a sequencer (shutdown, the release of an idle table's sequencer, DROP
  TABLE) flushes its V1 log once, one `fdatasync`, when the log holds records no flush covered, so the next start
  reads only the log's header. After a crash, a kill or a power loss the next start checks the records written
  since the sequencer was last opened or closed cleanly.
- **Held-back transactions** are applied again from the WAL after a restart instead of being read from `_txn`, in
  every mode. Before, a crash at the wrong moment could lose or duplicate those rows
  ([#7804](https://github.com/questdb/questdb/issues/7804)).
- **A conversion to non-WAL** applies the outstanding WAL first, in every mode, and flushes the table before the WAL
  goes. Before, committed rows that were not yet applied were dropped. A conversion whose replay cannot finish (the
  table is suspended, or a transaction fails to apply) is refused in every mode, also for a table that never ran
  `adaptive`: the table stays a WAL table until the replay can finish (section 11.4). Before, the conversion went
  ahead and dropped the WAL. A failed flush during the conversion stops the server, in every mode
  (section 11.2).
- **DETACH PARTITION** first links the partition into a temporary directory, `<partition>.<txn>.detaching`, and
  gives it its `.detached` name only once the commit is final, in every mode. Before, a process killed between the
  link and the commit left the partition attached next to a `.detached` copy, and a second DETACH was refused.
- **The partition purge job waits for a writer** in `sync`, and for WAL tables in `adaptive`: after a restart, `VACUUM
  TABLE` and the background purge leave a table's old partition versions alone until a writer has opened the table
  (`partition purge deferred to the next writer open`). `VACUUM TABLE` still reports success. The writer open removes
  them itself: in `sync` it first makes `_txn` durable again; in `adaptive` the start has already made the save point
  durable. On a table nobody writes after the restart, the old versions stay on the disk until a writer opens it: its
  next write, or in `adaptive` its idle save point (section 9) if its two save points differ. Before, a purge after a
  failed flush and a restart could remove the version the table on the disk still named.
- **The column purge at start** no longer deletes a column version for a purge log row whose update is past the
  table's committed txn, in every mode. After a power loss in `nosync` or `async`, the log can keep the row of an
  UPDATE whose `_txn` never reached the disk, and the version that row names is the one the table still uses. The
  start now leaves that version alone. Before, it could delete a column version the table still used.
- **A QuestDB Enterprise backup restore** flushes each restored table before it marks the table as restored, in
  every mode, about 11 ms per table (section 9). Before, a restore resumed after a power loss could skip a table
  whose files never reached the disk.
- **A parallel COPY in `sync`** flushes the imported partitions and symbol files before it attaches them, and the
  table again after the attach, before it reports the import finished; a failed flush stops the server (section
  11.2). Before, a power loss soon after COPY returned could lose a table the COPY created, or leave an existing
  table whose `_txn` named partitions or symbol values that never reached the disk. The flushes make a
  5-million-row import 6.7% slower (section 9).
- **REBASE WAL's seed transactions** no longer set the table's minimum timestamp to 1970 when the apply takes them
  in one block with the first insert after them, in every mode
  ([#7842](https://github.com/questdb/questdb/issues/7842)). Before, that happened when the apply job lost a race,
  and in `adaptive` a kill or power loss after the REBASE made it certain. A table that already reads 1970 from
  this keeps that minimum.
- **`sync` uses the fatal Java flush-failure policy**, as `adaptive` does, with the exceptions in section 11.2.
  Additional barriers cover the schema file swap, partition directories, ALTER COLUMN TYPE, partition conversions,
  event files before sequencing (including non-data events), and the WAL segment `_meta` before its first record.
  A version 2 sequencer log flushes the part before its header and flushes a part before closing it. The
  ADD COLUMN SYMBOL event rewrite also flushes its rewritten files in SYNC and requests writeback in ASYNC.
- **Checkpoint restore** flushes restored state in every mode before completing the restore. RENAME TABLE and
  table-registry compaction also flush the database directory around publication. These are not per-row costs.
- **A partition conversion keeps the new partition** once its `_txn` is written, even when the flush of that `_txn`
  fails, and in `sync` a writer open flushes `_txn` again before it removes old partition versions. Before, a
  restart could find the partition missing (CONVERT PARTITION) or its rows gone (the Parquet partition switch).
- **A start** checks each WAL table for its two save point files, in every mode: four `access` calls, about 2 µs per
  table (2.2 ms per 1,000 tables).
- **The first `sync` start after a `nosync` or `async` run** that did not stop cleanly reads and flushes each
  version 1 sequencer log once, as the switch into `adaptive` does (section 8).
- The selected NOSYNC/ASYNC steady-state measurements were within noise. That does not establish unchanged cost
  for every workload, filesystem or final integrated revision (section 9).

## 9. What it costs

Current measurement (2026-10-14, Linux, ext4 on NVMe, production configuration, paired runs): one writer that
waits for each 100-row commit takes 0.33 ms per commit in `nosync` and 4.06 ms in `adaptive`, about 12 times slower
(302,000 against 25,000 rows per second). `adaptive` flushes every WAL file of a commit before it is sequenced, about
11 flushes for a nine-column table, and a lone writer that waits for each commit cannot spread that cost; concurrent
writers and larger commits do. On Windows each flush is much slower (about 200 ms per commit was seen in CI). In
`nosync` a commit makes the same file-system calls as the release before this change.

Historical measurements: OSS `d3b877ccc9` against `8a71ef801f`, and Enterprise `c10e48336` against `d8e549cdb`.
These predate the final repair commits and upstream integration. They used one Linux machine (AMD Ryzen 9 7900,
ext4 on a Samsung 990 PRO NVMe), one process, and apply on the calling thread. Each figure is the median of three
runs; "as before" describes those measured revisions, not a guarantee about every current path.

The commit figures predate the overlapped writeback drain. The structural-operation figures do not measure the
final synchronous completion rule: DETACH, RESUME and direct logical changes can need two save points so every
selectable recovery generation covers the effect. That final overhead has not been remeasured. Other filesystems,
devices, macOS and Windows (which flush file by file) can differ widely. Timings elsewhere in this guide and its
internals companion have the same historical scope unless stated otherwise.

| Mode | One commit | One apply pass | One save point | At start | One schema change |
|---|---|---|---|---|---|
| `nosync` (default) | as before: no system call, 4.5 µs for one row | as before: 101 µs for one row, the same system calls | none | about 2 µs more per WAL table (two file checks), and about 9 µs more for the first reader of each table | as before |
| `async` | as before: 8 µs for one row | as before | none | as `nosync` | as before |
| `sync` | as before: 12.1 ms for one row, 38.8 ms for 100,000 rows; with a version 2 sequencer log, one more `msync` to write the record before the header that names it | as before: 11.6 ms | none | as `nosync` | about 12 ms more (four `fsync` that make the `_meta` swap durable): 32 ms to add a column, 26-28 ms to rename, drop or change a type; a few more `fsync` for ALTER COLUMN TYPE and partition conversions, which could lose data on a power loss before |
| `adaptive`, window 0 | 6.6 ms for one row, 30.5 ms for 100,000 rows (`sync`: 12.1 and 38.8 ms) | 127 µs for one row, the same system calls as `nosync` | 19-24 ms, nearly all of it one `syncfs`; periodic time trigger once a minute, with additional row-count, structural, enrolment, close and idle triggers | clean: 28 ms for 20 tables (`nosync` 9 ms), 0.24 s for 1,000 tables, of which one `syncfs` of 51 ms; after a crash: about 34 ms per table with 20 commits to replay | 25-30 ms; ALTER COLUMN TYPE 48 ms, because it takes a save point, as partition conversions, ATTACH and DETACH do |
| `adaptive`, window 50 ms | 5.2 ms for one row, 28.7 ms for 100,000 rows | 141 µs | as window 0 | as window 0 | as window 0 |

Steady ingestion, 100-row commits from one writer: `nosync` 5.2 million rows/s, `async` 3.9 million, `sync` 5,800,
`adaptive` 13,000 (window 0) and 16,500 (window 50 ms).

More detail:

- **A commit of many rows.** The 100,000 rows of a 30.5 ms `adaptive` commit are written into the WAL in 5.9 ms
  (51 ms in `sync`), so writing and committing them takes 36 ms against 90 ms in `sync`. On Linux, adaptive does not
  wait for each WAL page to be written back when it moves to the next page; the commit's own flush writes it.
- **The save point** costs about the same whatever the table's width or partition count: the fixed flushes
  dominate. 19 ms at 4 columns and one partition, 22 ms at 1,000 partitions or at 256 columns, 24 ms after a
  100,000-row commit. At 4 columns it makes 47 system calls, among them four `fsync`, one or two `msync`, ten
  `sync_file_range` (the write-back of each column file started, then waited on) and one `syncfs`; 168 at 64 columns
  and 552 at 256. `syncfs` flushes the whole file system, so
  its cost also depends on other dirty data on the same file system.
- **Start after a crash.** Each table restored costs its restore plus the replay of the WAL after its save point:
  about 34 ms per table with 20 commits to replay. A table with POSTING indexes also checks each partition's index
  files: about 0.4 s per 10,000 partitions, only after a crash. A table whose apply had held transactions back
  applies them again from the WAL: 25-100 ms per table at the largest held-back size, once.
- **The first reader of a table**, in every mode, asks the size of `_txn` three more times and of `_cv` once, to
  detect a torn file (section 2): about 9 µs per table after a start. It recurs only when `_txn` or `_cv` grows past
  what the reader has mapped, never per query.
- **A conversion to non-WAL** replays the table's WAL first and flushes it once (one `syncfs` on Linux), in every
  mode, when the table had WAL left to apply or a save point. A 2-million-row table converts in 33 ms with 200,000
  rows left to replay (11 ms of it the flush) and in 29 ms in `adaptive` (5 ms flush). With nothing left to apply,
  `nosync` pays nothing. It runs at the start that carries out the conversion, once.
- **DETACH PARTITION**, in every mode, links the partition one more time (into its temporary name, then into
  `.detached`) and removes one more directory. In `sync` it also flushes every file of the detached copy and three
  directories. Not measured; DETACH is a rare statement.
- **An `adaptive` start** rewrites the header of each table's current save point and makes it durable before anything
  relies on it. On Linux that is one write per table and one `syncfs` per file system (the root and each volume) per
  start: 1,000 tables took 244 ms, of which the `syncfs` 51 ms. Elsewhere it is one `fsync` per table (5-8 ms each,
  measured on Linux with that path forced).
- **A parallel COPY** in `sync` flushes twice before COPY returns:
  the imported partitions and symbol files before the attach, then `_txn` and the renamed directories after it. On
  Linux that is two `syncfs` per COPY. In `sync` the import also flushes each partition directory it creates, and
  that directory's parent, as it writes the partition. A 5-million-row import (258 MB of CSV, 30 partitions, four
  workers) takes 2.52 s in `sync` (2.36 s before; the two `syncfs` take 31 ms, the directory `fsync` the rest);
  `nosync` is unchanged (1.3 s). Elsewhere it is every file of
  the imported partitions, the table's root files and directories, before and after the attach (45-50 flushes for
  three partitions of four columns). COPY into an existing WAL table in `adaptive` pays for two save points instead,
  one per generation, so that a start which falls back to the older save point keeps the import; a skipped or failed
  save point fails the COPY (section 11.6).
  `nosync`, `async` and a non-WAL table in `adaptive`, which includes every table a COPY creates (section 7.3): no
  flush.
- **The first `sync` or `adaptive` start after `nosync` or `async`** that did not stop cleanly reads each WAL
  table's version 1 sequencer log from its first transaction and flushes it, once per table (section 8): one `fdatasync` and a read of 28 bytes per
  transaction in the log. Measured while other work loaded the same disk: 1,000 tables with 100 to 1,000 transactions
  each took 3.0 s in `sync` and 4.2 s in `adaptive`, against about 50 ms at the next start; one table with a million
  transactions 7.5 ms against 0.2 ms with its log in the page cache, 0.27 s when the log had to be read from the disk.
  In QuestDB Enterprise the uploader start includes it, so on that start uploads begin that much later. The first
  `adaptive` start after a `nosync`, `async` or `sync` run also flushes the kept parts of each version 2 log an
  earlier `adaptive` run had written: one directory listing and one `fdatasync` per kept part (5,000 transactions
  each by default) plus the header, once per table. The listing and flush do not read records; the torn-tail check
  at that open can still scan the last part.
- **Disk.** Because a start can fall back to the older of the two save points, the WAL, the superseded partition
  versions and the superseded column and index files wait one save point longer before they are removed: one minute
  at the defaults, but with a negative `cairo.adaptive.epoch.interval` that can be days. The WAL purge job reads the
  two save point headers of each `adaptive` table on every run.
- **An idle table.** A table whose two save points differ takes one more save point at its current state about one
  save point interval after its last change (at the defaults 60-90 s: the interval plus up to one 30 s WAL purge
  pass), once. Both save points then stand on the same state, and the purges free what the older one kept: its WAL,
  superseded and dropped partition versions, superseded column and index files. A change made without a WAL
  transaction (on QuestDB Enterprise, a storage policy step) counts as a change. Live views take theirs from their
  refresh worker. The cost is one save point per table each time it goes idle, about 20 ms (two when its last write
  had no save point of its own; the first is the one its writer's close would take anyway), none while it stays
  idle and none extra on a busy table; the save point finds nothing of the table left to flush. The WAL purge job
  asks at most 16 tables per pass, so after a restart with many tables written shortly before it, the extra save
  points spread over passes: 1,000 tables are all done after about 31 minutes at the defaults, at about 0.3 s of
  apply work per pass. Every idle table is asked within about two rounds of tables / 16 passes, however many tables
  keep going idle. A table whose idle save point cannot be taken is asked again one interval later. With a negative
  interval there is no idle save point.
- **Space freed by DROP PARTITION and TTL** comes back once neither save point names the dropped partition: at the
  table's next save point after the drop's own one, which on a table that gets no further write is its idle save
  point. `VACUUM TABLE` alone does not free it earlier, because a start could still fall back to the save point
  that names it. A partition dropped before any save point named it goes at the drop's own save point, or, if the
  table's writer closes without one, at its next open.
- **Outside Linux** a save point, the start's header flush and the COPY flush call `fsync` per file instead of one
  `syncfs`, which costs more with many files; only the figures marked "measured on Linux with that path forced"
  show how much.
- **Enterprise.** The uploader flushes every table's sequencer log once when it starts (boot and promotion, in
  `adaptive` and `sync`), before it listens for commits; a commit made meanwhile is uploaded once the flush is done.
  On Linux that is one `syncfs` plus a read of each table's last transaction (on the first such start after a
  `nosync` or `async` run that did not stop cleanly, also the one-off read and flush of each version 1 log, above): for 1,000 tables 37-109 ms in
  `adaptive` and 21-23 ms in `sync` when every table has one new commit, 2-8 ms when none has. Elsewhere each table
  flushes its log with every version 2 log part it still keeps: one more flush per kept closed part (5,000
  transactions each by default), about +50% on the start flush in the typical case (1,000 busy tables: 10.0 s
  against 6.6 s, measured on Linux with that path forced), and the log of each table an earlier run dropped is
  flushed the same way at that start. A table that kept a long backlog it could not upload pays
  one flush per 5,000 kept transactions at that start. A table the start could not flush (a transient error that is
  not a flush failure) is flushed at its first upload instead, and held until then. The role switch and the restore
  add flushes too:
  - a demotion opens the writer of each enrolled table (when none is pooled) and flushes the table before it removes
    its save point: one `syncfs` and two `fsync` per table on Linux, about 19 ms per table, so about 19 s for 1,000
    enrolled tables before the instance starts replicating;
  - a promotion of an `adaptive` instance flushes once what the replica applied (one `syncfs` of the database root
    and of each volume on Linux, 41 ms for 200 tables; a flush of every table file elsewhere); each table then takes
    its first save point at its next write, about 34 ms once per table; with access control enabled, the promotion
    opens every WAL table's writer, so each table takes that save point there;
  - a backup restore flushes each restored table before it marks it restored, in every mode: one `syncfs` per table
    on Linux, every file and directory of the table elsewhere. 200 small tables restore in 5.3-5.6 s against
    3.2-3.3 s with the flushes turned off, about 11 ms per table, the same in every mode; only on a start that
    restores a backup;
  - a table registered on a primary or at point-in-time recovery takes a save point in `adaptive`: about 18 ms more
    per table (48 against 30 ms); the other modes skip it;
  - the check at the end of each apply batch that a role switch did not happen during it adds no system call while
    the role stays the same.

## 10. Observing it

### 10.1 `wal_tables()`

```sql
SELECT name, sequencerTxn, writerTxn, localDurableSeqTxn, durableEpochSeqTxn, lastEpochTs, recoveryIncarnation
FROM wal_tables();
```

| Column | Type | Meaning |
|---|---|---|
| `localDurableSeqTxn` | LONG | the highest sequence number whose WAL is on stable storage; -1 before the first. Advances in `adaptive` (and `sync`). |
| `durableEpochSeqTxn` | LONG | the sequence number of the table's current save point. A crash replays from here. The WAL is kept from the save point before it, in case the start has to fall back to that one. |
| `lastEpochTs` | TIMESTAMP | the last save-point publication time *this process* recorded; NULL when unknown. Not evidence that a save point is currently on disk: recovery does not restore this timestamp, and removing an epoch pin does not erase it. After a restart it stays NULL until the first WAL-apply batch publishes a save point — the writer-open check that certifies recovered coverage does not itself set it. |
| `recoveryIncarnation` | LONG | how many times this process restored the table from its save point. |

All four columns read the tracker directly, regardless of whether applied-writer coordinates are initialized yet
(for example, a table no writer has touched since the start, or one caught by a notification-queue overflow).
`localDurableSeqTxn` is `-1` until a flush establishes it; overflow can otherwise leave any of the four at a
nonzero value rather than reading 0. `suspended` always reflects the tracker's real state and, for an unreadable
`_txn` row, the genuine read error -- never a value fabricated because the writer was not yet initialized.

Useful differences:

```sql
-- commits not yet covered by the local durable frontier (not a wall-clock loss bound)
SELECT name, sequencerTxn - localDurableSeqTxn AS not_yet_durable FROM wal_tables();

-- how far a crash now would replay (the WAL kept reaches one save point further back)
SELECT name, sequencerTxn - durableEpochSeqTxn AS replay_after_crash FROM wal_tables();
```

The commit mode is not a column: it is one setting for the whole instance.

### 10.2 Metrics

| Metric (scraped name) | Type | Meaning |
|---|---|---|
| `questdb_wal_apply_local_durable_seq_txn` | gauge | the durable frontier summed over tables |
| `questdb_wal_adaptive_epoch_advances_total` | counter | save points taken, idle ones included |
| `questdb_wal_adaptive_recovery_events_total` | counter | tables restored from a save point at a start |

`questdb_wal_apply_seq_txn - questdb_wal_apply_local_durable_seq_txn` is the global durable lag. It reads high after
a switch to `adaptive` until every table has been written once; use `wal_tables()` for a per-table figure.

### 10.3 What to watch

- `not_yet_durable` should stay near 0 with a window of 0 and swing within one window otherwise. A lag that keeps
  growing means the flushes do not keep up: look at the device.
- `lastEpochTs` should move at least once a minute on a table that is being written, and once more about one interval
  after its last write. If it does not, look for `could not advance adaptive durable epoch` in the log. Without new
  save points the WAL is never purged and the replay after a crash grows.
- `recoveryIncarnation` and the recovery counter move only after a crash. After a clean shutdown they stay at 0.
  With `cairo.adaptive.epoch.flush.on.close=false` or a negative `cairo.adaptive.epoch.interval`, a clean restart
  also restores and counts.

## 11. Operations: when a start refuses or a table is stuck

These steps cover the named failure states, not every possible storage failure. Keep the logs and a copy of the
affected files before manual repair. Skipping WAL transactions, restoring a backup, or dropping an interrupted
COPY's target can discard data; the relevant sections state the limits. Do not remove save points merely to make
startup succeed after an unknown crash.

### 11.1 The start refuses

The start stops before it accepts connections and logs the reason with the table name.

| Message | What it means | What to do |
|---|---|---|
| `invalid configuration value [key=cairo.commit.mode, ...]` | The mode is misspelt. | Use `nosync`, `async`, `sync` or `adaptive`. |
| `could not open read-write [file=.../_epoch.manifest.<generation>]` or `could not rewrite adaptive save point header [path=...]` | Every `adaptive` start rewrites the header of each table's current save point and flushes it, so that nothing relies on a save point only the page cache holds. The named file cannot be opened for writing, or the write failed: its owner or permissions changed (a copy made as another user, for example), the file system is read-only, or the disk fails. | Fix the file's owner or permissions, or the disk, and start again. Nothing is lost meanwhile. |

Two more refusals concern the save point files. Each message says what to do; the details follow. Each message is
one line in the log, wrapped here.

#### A damaged save point

```
no trustworthy adaptive epoch generation; refusing unsafe live-state fallback [table=..., dir=...]; after a clean
stop or a process kill, move _epoch.manifest.0 and _epoch.manifest.1 out of the table directory and start again;
after a power loss, restore the table from a backup or a checkpoint
```

Neither save point file of the table passes its checks. QuestDB never leaves this state by itself: a new save point
is always written into the file that is not current, and its header last. It means damage on the disk or a defect.

1. Copy the table directory aside for a report, with both `_epoch.manifest.*` files.
2. If the last stop was clean, or only the QuestDB process was killed, the table files are complete: move
   `_epoch.manifest.0` and `_epoch.manifest.1` out of the table directory and start again. The table starts as not
   enrolled, keeps every row, and takes a new save point at its first writer.
3. After a power loss or an operating-system crash, restore the table from a backup or a checkpoint instead; rows
   written after the backup are lost. If you cannot tell (a container restart can be a host crash), treat it as a
   power loss.

The same message can end differently:

```
no trustworthy adaptive epoch generation; refusing unsafe live-state fallback [table=..., dir=...]; recovery has
rewritten the live table files, restore the table from a backup or a checkpoint
```

Here a save point passed its file checks, was written over the live table files, and only then failed its content
checks, which needs a defect. The table files now hold that save point, so moving the files out does not help:
restore the table from a backup or a checkpoint, and keep the copy for a report.

#### A save point ahead of the table files

```
adaptive epoch post-dates live state; refusing wrong-lineage recovery [table=..., dir=..., epochSeqTxn=...]; move
_epoch.manifest.<generation> out of the table directory and start again
```

The newer save point names a transaction ahead of the table's `_txn`. QuestDB leaves this state in one sequence
only: an earlier start rejected the newer save point on its content (a defect wrote it), fell back to the older one,
and two crashes in a row interrupted that start and the replay after it. The table files are then at the older save
point, and its WAL is still kept.

For that cause, keep a copy of the named file for a report, move it out of the table directory and start again. The
start restores the older save point and replays the WAL after it; no row is lost.

The step is for that cause only. If table files were put back by hand (a `_txn`, `_cv` or partition copied in from
a backup or another copy), restore the whole table from the same backup instead: the WAL after the older save point
may already be purged, and moving the newer file out would lose rows.

#### Anything else

Any other recovery error also names the table and stops the start. Keep the log and a copy of the table directory,
and report it.

### 11.2 The server stops after a failed flush

In resolved `sync` and `adaptive` mode, a failed Java durability barrier reported through the flush-failure factory
stops the server at once: the data it was flushing, or other data on the same disk, may not be on the disk. This
covers `fsync`, `fdatasync`, `syncfs`, `sync_file_range` and synchronous `msync`. The Rust and final checkpoint
`sync()` exceptions are described below. A write-back kick that returns ENOMEM, EAGAIN or EINTR is retried by the
wait and barrier after it, rather than immediately stopping the server. A failed mandatory flush before conversion
to non-WAL stops it in every mode. A parallel COPY's work files count as table data: in a durable mode, a failed
flush of its index chunks or staging tables stops the server too, even if `cairo.sql.copy.work.root` is on another disk. A
restart clears it. The server stops at the failed flush itself, before any clean-up, so a parallel COPY never
reports an import done that is not on the disk, and section 11.6 says what it can leave behind. An Enterprise replica
decides by the mode it runs in: in `adaptive` it follows `nosync` (the object store holds the durable copy), so it
reports the error and keeps running; in `sync` it stops as `sync` does. Until its replica role has started (the
recovery and checkpoint restore at its start), an `adaptive` replica stops as `adaptive` does. In `nosync` and `async`
a failed flush fails only the operation that ran it; during the start (a table that leaves `adaptive`, or a
checkpoint restore) that stops the start, with the flush error in the message and exit code 55: fix the cause and
start again.

```
fatal durability barrier failure [operation=..., errno=..., message=could not fsync [fd=...] [path=...]]
QuestDB fatal durability failure; hard-halting without writer cleanup [...]
```

The message names the file that failed to flush where the operating system can tell (Linux), so the disk at fault is
known.

The process exits with code 55. No acknowledgement was sent for anything the failed flush covered. Fix the cause
(a full disk, a failing device, an I/O error in the system log), then start again: recovery puts every table back
at its save point and the WAL apply replays the rest. While the disk keeps failing, every start stops the same way.

An embedded engine, built without `ServerMain`, does not halt: after the failure it refuses every further write,
acknowledgement and `CHECKPOINT CREATE` with `engine is poisoned by a failed durability barrier`. Close it, fix the
cause and construct a new one; nothing past the failure was acknowledged, and the new engine recovers as a restart
does.

A flush that the Rust code runs does not stop the server yet: the Parquet metadata (`_pm`) barriers of an
out-of-order merge into a Parquet partition or of an ATTACH, and Enterprise cold storage. Its failure fails the
statement or suspends the table (section 11.3) instead; no acknowledged data is lost, because the `_pm` footer is
written past the committed size and the retry rewrites it. Also, the final `sync()` of `CHECKPOINT CREATE`
checks its return code outside the Java failure factory and fails the statement rather than poisoning the engine.
Neither exception is a claim that all Rust or checkpoint failures are harmless.

### 11.3 A table is suspended or its save point is late

| Message | What it means | What to do |
|---|---|---|
| `could not take the durable epoch the table owes, the table stays suspended; retry RESUME WAL [table=...]` | RESUME could not establish coverage of the live state in every selectable save point. It checks this even when no save point is marked owed. | Fix the logged cause, which can be an I/O error or a busy epoch pin, then retry `ALTER TABLE ... RESUME WAL`. Do not delete a save point to bypass the refusal. |
| `could not complete durable epoch after <operation>; retry the operation or RESUME WAL [table=...]` | DETACH or a direct logical change could not finish the required save points. The effect may already be visible, but completion was refused. | Fix the logged cause and retry the operation or RESUME. Keep the temporary partition files; the writer finishes them when every selectable generation covers the effect. |
| `could not advance adaptive durable epoch [table=..., error=...]` | A periodic save point failed. The table keeps its previous save point and its WAL. | Fix the cause. The next apply pass tries again; nothing is lost meanwhile, but the WAL grows and the replay after a crash gets longer. |
| `durable epoch skipped after <operation>, a crash before the next one undoes it [table=...]` | A best-effort save point, for example after a physical conversion or WAL ATTACH, was skipped. The change stays visible and a save point is owed. | Check the cause and that a later publication succeeds. This is not a successful completion of DETACH, RESUME or a direct logical change: those require coverage of every selectable generation before completion. |

A suspended table is otherwise resumed as in every release: `ALTER TABLE ... RESUME WAL`. Section 7.4 explains why
`RESUME WAL FROM TXN` loses a transaction on a table with held-back transactions.

### 11.4 A conversion to non-WAL is refused

`ALTER TABLE ... SET TYPE BYPASS WAL` is carried out at the next start, after the outstanding WAL is applied. If
that replay cannot finish (the table is suspended, or a transaction fails to apply), the conversion is refused, in
every mode and for every WAL table, also one that never ran `adaptive`. The start logs a critical
line:

```
table conversion failed, the table keeps its type [path=..., e=... could not apply the WAL before the conversion to
non-WAL, the table stays a WAL table; resume it, let it catch up and restart [table=..., seqTxn=..., lastSeqTxn=...,
suspended=...]]
```

The table stays a WAL table with its WAL and its pending conversion, and the rest of the database starts. A start in
`adaptive` keeps the table's save point; a start in another mode flushes the table and removes its save point. No
row is lost, and the next start tries the conversion again.

To finish the conversion:

- **The cause has passed** (a full disk that was cleared, for example): `ALTER TABLE ... RESUME WAL`, let the apply
  catch up, restart.
- **A transaction fails for good**: `ALTER TABLE ... RESUME WAL FROM TXN <n>`, with `<n>` the transaction after the
  failing one, let the apply catch up, restart. This drops the failing transaction's rows. First check that the
  failing transaction is the one right after the `seqTxn` in the message (the error is in `wal_tables()`, columns
  `errorTag` and `errorMessage`, and in the log): if it is a later one, the skip would drop the transactions in
  between too. That happens when the failing transaction's whole WAL directory is missing: the apply then stops
  before the good transactions ahead of it, on any WAL table, as in every release. Section 7.4 explains what
  `FROM TXN` does on a table with held-back transactions.

If the critical line names a sequencer file instead (`could not open ... [file=.../txn_seq/...]`), the table's
sequencer could not be read at the start, and the table has a save point that the conversion needs to replay from.
Fix the cause (file permissions, the open-file limit, the disk) and restart; the conversion runs again with every
row. If the sequencer files are damaged and cannot be repaired, cancel the conversion with `ALTER TABLE ... SET TYPE
WAL` and restart: the start then skips it (`skipping conversion, table already has the expected type`), and the table
stays a WAL table with the rows of its save point. The rows after the save point needed the sequencer, on any path.

### 11.5 Other messages

| Message | What to do |
|---|---|
| `UDP line protocol cannot write a WAL table in ADAPTIVE commit mode, skipping its lines until restart [table=...]` | Send those rows over ILP on HTTP or TCP, or over QWP, or write them into a non-WAL table (section 7.2). |
| A QWP client fails its connection with a durable-ack mismatch | Check the whole requested tier set and the server version, role and resolved mode. OSS cannot grant replicated; local needs an unfenced adaptive primary or standalone server. Older servers may not understand tier tokens. Choose a server that supports the required guarantee; do not silently weaken the request. |
| `partition source next to the attached partition may share its files, do not attach it again [table=..., partition=...]` (at a writer open) or `could not remove attached partition source, it shares files with the attached partition and is unsafe to attach again [path=..., errno=...]` | On an enrolled table ATTACH PARTITION links the files of the `.attachable` directory instead of moving them, and deletes the directory once both save point generations cover the attach. A crash in between, or a failed deletion, leaves it behind. The partition is attached. Do not attach the source again: its files are the attached partition's files. Prefer letting the writer remove it. Manual deletion is safe only after both selectable generations cover the ATTACH (two successful save points covering it); a restart alone does not establish this. Before then, fallback can need the source to replay the ATTACH. |
| `adaptive save point failed its checksums, trying the other generation [table=..., generation=..., epochSeqTxn=...]` or `removing adaptive save point that failed its content checks [table=..., generation=..., ...]` | The newer save point of the table is damaged. When the start goes on, it used the older one, whose WAL is still kept, so the replay brings back every row: nothing to do for the data. It means damage on the disk or a defect: keep the log for a report. If neither save point passes, the start refuses instead (section 11.1). |
| `adaptive epoch roll-forward restored durable cut [table=..., epochSeqTxn=...]` | Information: the start restored the table after a crash. No action. |
| `adaptive epoch belongs to a superseded WAL lineage, discarding anchor [table=..., epochSeqTxn=..., seqLastTxn=...]` | Information: the save point named transactions the sequencer no longer has (the table's WAL was replaced). The start removes it, and in `adaptive` the table takes a new one at its first writer. No action. |

### 11.6 A parallel COPY did not finish

A parallel COPY that has not reported its import finished has promised nothing about the rows it was importing, and
an existing table it imports into must be empty. The steps below therefore never touch rows a table held before.

**After a failed flush stopped the server** (section 11.2), start it again. The server stopped at the failed flush,
before the COPY could clean up, so the table is as the COPY left it:

- A table the COPY created (always a non-WAL table) is still there and can hold some imported partitions: run
  `DROP TABLE`, then COPY again.
- An existing table can hold some imported partitions: run `TRUNCATE TABLE`.
- Imported partitions that were not attached yet are left as `<partition>.attachable` directories in the table's
  directory, and COPY again fails with `could not copy partition file [to=.../<partition>.attachable]`: delete the
  `.attachable` directories the failed COPY left, then run COPY again.

**After a power loss during the COPY** (in `sync`; in `adaptive` a table the COPY created has no promise, section
7.3):

- **A non-WAL table the COPY created, or any existing table.** The table can hold some imported partitions, each
  whole, and a retry is refused with `target table must be empty [table=...]`; or reading the table fails with
  `Partition '...' does not exist in table '...' directory. Run [ALTER TABLE ... FORCE DROP PARTITION LIST '...'] to
  repair the table or the database from the backup.` (a partition whose move into place did not reach the disk).
  Run `DROP TABLE`, then `CREATE TABLE` again if the table existed before the COPY (it was empty), then run COPY
  again.

**COPY failed with `import is not durable against a save point fallback: a save point after the attach was skipped
or failed [...]`** (into an existing WAL table in `adaptive`). The import was attached, but the save point that makes
it survive a start which falls back to the older save point was skipped or failed, and the log says why (a
`durable epoch skipped after attach partition [...]` or `could not advance durable epoch after attach partition
[...]` line, both critical). Nothing in the WAL redoes an import, so COPY did not report it done: its clean-up
truncated the target table, which was empty before the COPY. Fix the cause the log names (the skip is transient: a
structural change in flight, a busy pin slot; a failure is an I/O error on the save point file) and run COPY again.

## Appendix A. Configuration

| Key | Default | Meaning |
|---|---|---|
| `cairo.commit.mode` | `nosync` | `nosync`, `async`, `sync` or `adaptive`, for the whole instance. QuestDB Enterprise defaults to `nosync` when replication is configured; an explicit value wins. |
| `cairo.adaptive.commit.group.window` | `50ms` | The group-commit window (section 4). `0`: every acknowledged commit is on stable storage. Negative counts as 0. |
| `cairo.adaptive.epoch.interval` | `60s` | The periodic time trigger. `0`: a save point after every apply pass. Negative disables the periodic, row-count and idle triggers, not enrolment, required completion or retry of owed save points. DETACH, RESUME and direct logical changes still require complete coverage. WAL retention and replay can grow without a time bound. Not for production. |
| `cairo.adaptive.epoch.max.rows` | `5000000` | A periodic save point once this many rows were applied since the last one, while the interval is nonnegative. `0` or negative disables the row trigger. |
| `cairo.adaptive.epoch.flush.on.close` | `true` | A clean writer close takes a save point over any transactions after the last one, so a clean restart restores nothing. |
| `cairo.adaptive.epoch.column.sync.batched` | `true` | On Linux, flush a save point's column files in one batch ending in one `syncfs`. `false`: a flush per column file, then the same `syncfs`. |
| `cairo.wal.commit.writeback.drain` | `true` | Start the write-back of every file of a WAL segment, then wait for it, before the commit's flushes, so the files are written back in parallel. Speed only. |

## Appendix B. Files

Each enrolled WAL table holds two save point files in its directory, `_epoch.manifest.0` and `_epoch.manifest.1`.
Each is one save point: a 120-byte header and copies of `_txn`, `_cv` and `_meta`. Do not edit, copy or delete them
while QuestDB runs. A switch out of `adaptive` removes them (section 8).

Changed existing formats: a checksum and stamp in each `_txn` record, a trailer after each `_cv` area, and a
checksum in each V2 sequencer record. V1 `_txnlog` has a scan hint at offset 40; V2 has an adaptive scan hint at
40 and a known-tail slot at 48. Their sign and update rules differ by mode. No existing format version changes;
a plain upgrade accepts older files as unverified. `_meta` keeps the base layout: save-point presence, not a
`_meta` enrolment field, records enrolment. `_event.c`, `_txnlog.c` and partition `_chk` files are not written.
[`adaptive-commit-internals.md`](adaptive-commit-internals.md), section 14, describes the layouts and removed
intermediate-build files. This is not downgrade-after-write support (section 7.1).
