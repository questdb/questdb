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

- **The commit is durable.** Before a commit is acknowledged, the writer's own WAL files (column data, the event
  log) are flushed to the device with `fdatasync`, and then the shared sequencer log that orders the commits is
  flushed too. With the group-commit window at 0 every acknowledged commit is on stable storage when the call
  returns. With a window above 0 the sequencer flush is shared by the commits of one window (section 4).
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

The result: after a crash of any kind, every acknowledged transaction is back once the WAL apply catches up, and
the table files are consistent.

Adaptive covers WAL tables. A table created with `BYPASS WAL` has no WAL to replay and behaves as in `nosync`.

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
its first save point, which flushes every partition of the table once (one `syncfs` on Linux, a flush per file
elsewhere). The writer takes it before it applies anything, so no table is ever applied lazily without a save point
behind it.

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

When commits stop in the middle of a window, the last flush is made by a background job that runs on the shared
worker pool, so the bound for that last batch is the window plus the pool's sleep time
(`shared.worker.sleep.timeout`). Keep that sleep short when the window is very small. A clock that jumps backwards
forces the flush at once.

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

- The `local` tier is served only when the instance runs in `adaptive` mode and no flush has failed. It reports,
  per table, the highest sequence number whose WAL is on stable storage (`STATUS_LOCAL_DURABLE_ACK`).
- The `replicated` tier reports the replicated frontier (`STATUS_DURABLE_ACK`).

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
3. **An ordinary crash never stops the start.** A start refuses only when a save point is damaged, which needs
   damage on the disk or a defect, or when a save point file cannot be written (section 11 says what to do). When
   only the newer of a table's two save points is damaged, the start falls back to the older one and loses nothing.
4. **Recovery can itself be interrupted.** A crash or power loss at any step of the start, and a second start,
   give the same result as one uninterrupted start.
5. **A clean restart restores nothing.** After a clean shutdown, the start finds the table files equal to the
   save point, writes nothing to them and counts no recovery (with `cairo.adaptive.epoch.flush.on.close` at its default,
   `true`).
6. **The mode can change between any two restarts**, in any direction and any number of times, including the
   restart right after a crash. Section 8 says what each switch does.
7. **Tables from a released version need nothing.** Their files carry no save point and no checksums. They read
   as not yet enrolled and unverified, never as damaged, and the first adaptive writer enrols them.
8. **The other modes keep their behaviour.** `nosync`, `async` and `sync` make the same file system calls on a
   commit and a one-row apply pass as before, except that `sync` flushes the directories of a partition an apply
   opens (section 9 lists the measured differences). Section 8.1 lists what changes in them.

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

### 7.4 `RESUME WAL FROM TXN` on a table with held-back transactions

When the WAL apply stops in the middle of out-of-order data (its time quota, a shutdown, an error), the table holds
some applied transactions back in `_txn` (the WAL lag). `ALTER TABLE ... RESUME WAL FROM TXN` on such a table loses
one good transaction, in every release and in this version
([questdb#5422](https://github.com/questdb/questdb/issues/5422)). This version changes which transaction is lost in
one case and removes an endless apply loop and stray rows in another, but it does not fix the loss. A plain
`RESUME WAL` (without `FROM TXN`) is not affected.

### 7.5 Other limits

- **Storage that ignores flushes.** The promises need `fsync` and `fdatasync` to reach stable storage. A file
  system mounted without write barriers, a network file system, a virtual disk set to write-through on the guest, or
  a Linux guest on a macOS host can break them. QuestDB does not check the storage: make sure it honours flushes.
- **The last window.** With a window above 0, commits acknowledged in the last window before a power loss can be
  lost (section 4). Use a window of 0, or the QWP durable acknowledgement, where that matters.
- **Enterprise replicas.** A replica keeps no save points; its source of truth is the object store. A power loss on
  a replica behaves as in `nosync`, and the replica catches up from the object store. ATTACH and DETACH PARTITION
  finish at once on a replica, as outside `adaptive`.
- **A power loss during a `nosync` or `async` run** loses what those modes lose, also after a switch to `adaptive`
  later.
- **Detection is for torn writes.** Damage to an already written record of a version 1 sequencer log (bit rot, a
  foreign write) is not detected. The checksums are not a full integrity check.
- **The save point is not a backup.** It bounds the replay after a crash. `CHECKPOINT` and backups are unchanged and
  still needed.
- **A role switch in the middle of an apply batch that then fails** (QuestDB Enterprise). If a node is promoted while
  an apply batch that began on the replica is still running, and that batch then fails, the rows it applied after
  the promotion are neither flushed nor covered by a save point until the table is resumed or the server restarts.
  A power loss in that window can lose them. It needs a promotion, a failing batch and a power loss together.
- **A crash soon after a storage policy step** (QuestDB Enterprise). The storage policy job applies some steps to a
  table directly, without a WAL transaction: a local drop of an uploaded partition, a Parquet commit, rewrite or
  squash, and its bookkeeping steps. A crash within one save point interval of such a step puts the table back at
  the save point before it. The files the step replaced are still there (the save point keeps them), and the policy
  job does the step again at its next pass. No row is lost. A clean shutdown keeps the step.
- **A power loss during a parallel COPY** that has not reported it finished can leave the table in a state that
  needs a manual step (section 11.6). Nothing COPY reported done is lost.

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

Found while this mode was built, not yet filed:

- A parallel COPY into a new table on an instance that creates WAL tables by default (`cairo.wal.enabled.default`,
  `true` by default) creates the table without its sequencer, in every mode. After any restart the table can be
  read but not written, altered or dropped. Create the table first (`CREATE TABLE ... WAL`) and COPY into it;
  section 11.6 says how to get out of the state.

## 8. Changing the commit mode

`cairo.commit.mode` can change at any restart, in any direction, any number of times.

- **Into `adaptive`.** Each WAL table enrols at its first writer open (section 3).
- **From `nosync` or `async` into `sync` or `adaptive`.** The first start in the new mode reads each WAL table's
  version 1 sequencer log (the default) once from its first transaction and flushes it, because the records the
  `nosync` or `async` run wrote were never flushed and a durable mode does not trust them unchecked. The first open
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

- **Torn-file detection** runs in every mode (section 2): it costs nanoseconds per commit and no system call, and
  about 9 µs on the first reader of each table (four more file size checks). Each open of a version 1 sequencer log
  reads the records added since its previous open (no system call), and a torn tail it cuts after a crash is
  flushed, in every mode.
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
- **`sync` stops the server on a failed flush**, as `adaptive` does (section 11.2), and makes a few more files
  durable: the schema file swap, the directories of a partition an apply opens (two `fsync`), and the directories
  of ALTER COLUMN TYPE, CONVERT PARTITION and the Parquet partition switch. A version 2 sequencer log writes the
  record before the header that names it.
- **A partition conversion keeps the new partition** once its `_txn` is written, even when the flush of that `_txn`
  fails, and in `sync` a writer open flushes `_txn` again before it removes old partition versions. Before, a
  restart could find the partition missing (CONVERT PARTITION) or its rows gone (the Parquet partition switch).
- **A start** checks each WAL table for its two save point files, in every mode: four `access` calls, about 2 µs per
  table (2.2 ms per 1,000 tables).
- **The first `sync` start after a `nosync` or `async` run** reads and flushes each version 1 sequencer log once, as
  the switch into `adaptive` does (section 8).
- `nosync` and `async` otherwise behave and cost as before (section 9).

## 9. What it costs

Measured on one Linux machine (AMD Ryzen 9 7900, ext4 on a Samsung 990 PRO NVMe disk), in one process with the
apply run on the calling thread, against the release this change starts from. Each figure is the median of three
runs; "as before" means within the spread of runs of the unchanged release. Other file systems, other devices,
macOS and Windows (where a save point flushes file by file) can differ widely.

| Mode | One commit | One apply pass | One save point | At start | One schema change |
|---|---|---|---|---|---|
| `nosync` (default) | as before: no system call, 4.5 µs for one row | as before: 101 µs for one row, the same system calls | none | about 2 µs more per WAL table (two file checks), and about 9 µs more for the first reader of each table | as before |
| `async` | as before: 8 µs for one row | as before | none | as `nosync` | as before |
| `sync` | as before: 12.1 ms for one row, 38.8 ms for 100,000 rows; with a version 2 sequencer log, one more `msync` to write the record before the header that names it | as before: 11.6 ms | none | as `nosync` | about 12 ms more (four `fsync` that make the `_meta` swap durable): 32 ms to add a column, 26-28 ms to rename, drop or change a type; a few more `fsync` for ALTER COLUMN TYPE and partition conversions, which could lose data on a power loss before |
| `adaptive`, window 0 | 6.6 ms for one row, 30.5 ms for 100,000 rows (`sync`: 12.1 and 38.8 ms) | 127 µs for one row, the same system calls as `nosync` | 19-24 ms, nearly all of it one `syncfs`; at most once a minute per table by default (about 0.03% of one core), and once at each table's first apply after a start | clean: 28 ms for 20 tables (`nosync` 9 ms), 0.24 s for 1,000 tables, of which one `syncfs` of 51 ms; after a crash: about 34 ms per table with 20 commits to replay | 25-30 ms; ALTER COLUMN TYPE 48 ms, because it takes a save point, as partition conversions, ATTACH and DETACH do |
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
- **A parallel COPY** in `sync`, and into a WAL table it creates in `adaptive`, flushes twice before COPY returns:
  the imported partitions and symbol files before the attach, then `_txn` and the renamed directories after it. On
  Linux that is two `syncfs` per COPY. In `sync` the import also flushes each partition directory it creates, and
  that directory's parent, as it writes the partition. A 5-million-row import (258 MB of CSV, 30 partitions, four
  workers) takes 2.52 s in `sync` (2.36 s before; the two `syncfs` take 31 ms, the directory `fsync` the rest) and
  1.83 s in `adaptive` (the two `syncfs` take 30 ms); `nosync` is unchanged (1.3 s). Elsewhere it is every file of
  the imported partitions, the table's root files and directories, before and after the attach (45-50 flushes for
  three partitions of four columns). COPY into an existing WAL table in `adaptive` pays for two save points instead,
  one per generation, so that a start which falls back to the older save point keeps the import; a skipped or failed
  save point fails the COPY (section 11.6).
  `nosync`, `async` and a non-WAL table in `adaptive`: no flush.
- **The first `sync` or `adaptive` start after `nosync` or `async`** reads each WAL table's version 1 sequencer log
  from its first transaction and flushes it, once per table (section 8): one `fdatasync` and a read of 28 bytes per
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
  that names it.
- **Outside Linux** a save point, the start's header flush and the COPY flush call `fsync` per file instead of one
  `syncfs`, which costs more with many files; only the figures marked "measured on Linux with that path forced"
  show how much.
- **Enterprise.** The uploader flushes every table's sequencer log once when it starts (boot and promotion, in
  `adaptive` and `sync`), before it listens for commits; a commit made meanwhile is uploaded once the flush is done.
  On Linux that is one `syncfs` plus a read of each table's last transaction (on the first such start after a
  `nosync` or `async` run, also the one-off read and flush of each version 1 log, above): for 1,000 tables 37-109 ms in
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
| `lastEpochTs` | TIMESTAMP | when the current save point was taken; NULL when there is none. |
| `recoveryIncarnation` | LONG | how many times this process restored the table from its save point. |

The three numbers read 0 for a table no writer has touched since the start.

Useful differences:

```sql
-- acknowledged commits not yet on stable storage (at most one window's worth with a window above 0)
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

Every state below either repairs itself or has a manual step, named in its message or in this section, that clears
it, and following the step keeps every acknowledged row. Two steps say where they cannot: skipping a transaction
that can never be applied (section 11.4), and a damaged save point after a power loss, which needs damage on the
disk or a defect and is cleared by a restore from a backup (section 11.1).

### 11.1 The start refuses

The start stops before it accepts connections and logs the reason with the table name.

| Message | What it means | What to do |
|---|---|---|
| `adaptive recovery is disabled; refusing unsafe startup [table=...]` | `cairo.adaptive.recovery.roll.forward.enabled` is `false`. That setting exists for tests. | Remove the setting (the default is `true`) and start again. |
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

In `sync` and `adaptive` mode, a failed `fsync`, `fdatasync`, `syncfs`, `sync_file_range` or synchronous `msync` stops
the server at once, wherever it happens (a table, its WAL, a checkpoint), because the data it was flushing, or other
data on the same disk, may not be on the disk. (A write-back that could not start for lack of memory, or was
interrupted, is retried by the wait and the flush after it and does not stop the server.) So does a failed flush of a
conversion to non-WAL, in every mode. A parallel COPY's work files count as table data: a failed flush of its index chunks or of the staging tables that
become the table's partitions stops the server too, also when `cairo.sql.copy.work.root` is on another disk, and a
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
written past the committed size and the retry rewrites it.

### 11.3 A table is suspended or its save point is late

| Message | What it means | What to do |
|---|---|---|
| `could not take the durable epoch the table owes, the table stays suspended; retry RESUME WAL [table=...]` | `ALTER TABLE ... RESUME WAL` takes the save point the table owes before it resumes, and that save point failed. | Fix the cause in the log line before it (usually the disk), then run `ALTER TABLE ... RESUME WAL` again. |
| `could not advance adaptive durable epoch [table=..., error=...]` | A periodic save point failed. The table keeps its previous save point and its WAL. | Fix the cause. The next apply pass tries again; nothing is lost meanwhile, but the WAL grows and the replay after a crash gets longer. |
| `durable epoch skipped after <operation>, a crash before the next one undoes it [table=...]` | A save point that should follow a schema change or a partition operation did not run. The change stays in effect. | Nothing: the save point is owed and taken at the next apply pass, `RESUME WAL` or clean close. A crash before then replays the WAL from the previous save point. |

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
| A QWP client fails its connection with a durable-ack mismatch | The client asked for the `local` tier and the server is not in `adaptive` mode (or a flush failed earlier). Switch the server to `adaptive`, or stop asking for `local`. |
| `partition source next to the attached partition may share its files, do not attach it again [table=..., partition=...]` (at a writer open) or `could not remove attached partition source, it shares files with the attached partition and is unsafe to attach again [path=..., errno=...]` | On an enrolled table ATTACH PARTITION links the files of the `.attachable` directory instead of moving them, and deletes the directory once both save point generations cover the attach. A crash in between, or a failed deletion, leaves it behind. The partition is attached. Delete the `.attachable` directory of that partition; do not attach it again, its files are the attached partition's files. Delete it only after a restart, or after the table has taken two save points since the ATTACH (two `adaptive durable epoch [table=...]` lines for the table in the log): until then a start that falls back to the older save point can still replay the ATTACH from it, and the writer deletes it itself once both save points cover the attach. |
| `adaptive save point failed its checksums, trying the other generation [table=..., generation=..., epochSeqTxn=...]` or `removing adaptive save point that failed its content checks [table=..., generation=..., ...]` | The newer save point of the table is damaged. When the start goes on, it used the older one, whose WAL is still kept, so the replay brings back every row: nothing to do for the data. It means damage on the disk or a defect: keep the log for a report. If neither save point passes, the start refuses instead (section 11.1). |
| `adaptive epoch roll-forward restored durable cut [table=..., epochSeqTxn=...]` | Information: the start restored the table after a crash. No action. |
| `adaptive epoch belongs to a superseded WAL lineage, discarding anchor [table=..., epochSeqTxn=..., seqLastTxn=...]` | Information: the save point named transactions the sequencer no longer has (the table's WAL was replaced). The start removes it, and in `adaptive` the table takes a new one at its first writer. No action. |

### 11.6 A parallel COPY did not finish

A parallel COPY that has not reported its import finished has promised nothing about the rows it was importing, and
an existing table it imports into must be empty. The steps below therefore never touch rows a table held before.

**After a failed flush stopped the server** (section 11.2), start it again. The server stopped at the failed flush,
before the COPY could clean up, so the table is as the COPY left it:

- A table the COPY created is still there and can hold some imported partitions. If it is a non-WAL table, run
  `DROP TABLE`, then COPY again. If it is a WAL table, follow "A WAL table the COPY created" below.
- An existing table can hold some imported partitions: run `TRUNCATE TABLE`.
- Imported partitions that were not attached yet are left as `<partition>.attachable` directories in the table's
  directory, and COPY again fails with `could not copy partition file [to=.../<partition>.attachable]`: delete the
  `.attachable` directories the failed COPY left, then run COPY again.

**After a power loss during the COPY** (in `sync`, or into a WAL table it created in `adaptive`):

- **A non-WAL table the COPY created, or any existing table.** The table can hold some imported partitions, each
  whole, and a retry is refused with `target table must be empty [table=...]`; or reading the table fails with
  `Partition '...' does not exist in table '...' directory. Run [ALTER TABLE ... FORCE DROP PARTITION LIST '...'] to
  repair the table or the database from the backup.` (a partition whose move into place did not reach the disk).
  Run `DROP TABLE`, then `CREATE TABLE` again if the table existed before the COPY (it was empty), then run COPY
  again.
- **A WAL table the COPY created.** If the table is not there after the restart, create it with
  `CREATE TABLE ... WAL` and run COPY into it. If it is there, no SQL step works on it, because of the missing
  sequencer (section 7.6), and the
  last imported partition can read as one row of zero and empty values. Do not run `DROP TABLE` on it: that fails,
  hides the table from then on and keeps its name taken. Stop the server, delete the table's directory
  (`<table>~<id>` under the database root), start, create the table with `CREATE TABLE ... WAL`, and run COPY into
  it.

**COPY failed with `import is not durable against a save point fallback: a save point after the attach was skipped
or failed [...]`** (into an existing WAL table in `adaptive`). The import was attached, but the save point that makes
it survive a start which falls back to the older save point was skipped or failed, and the log says why (a
`durable epoch skipped after attach partition [...]` or `could not advance durable epoch after attach partition
[...]` line, both critical). Nothing in the WAL redoes an import, so COPY did not report it done: its clean-up
truncated the target table, which was empty before the COPY. Fix the cause the log names (the skip is transient: a
structural change in flight, a busy pin slot; a failure is an I/O error on the save point file) and run COPY again.

**A WAL table the COPY created, after any restart** (no crash needed, section 7.6). The table can be read, but
INSERT and ALTER TABLE fail with `could not open read-write [file=.../txn_seq/_wal_index.d]`, and `DROP TABLE` fails
and hides the table as above. To keep its rows: copy them into a new table while it is still visible (for example
`CREATE TABLE t2 AS (SELECT * FROM t) TIMESTAMP(ts) PARTITION BY DAY WAL`), stop the server, delete the old table's
directory (`<table>~<id>` under the database root), and start. The copy takes writes.

## Appendix A. Configuration

| Key | Default | Meaning |
|---|---|---|
| `cairo.commit.mode` | `nosync` | `nosync`, `async`, `sync` or `adaptive`, for the whole instance. QuestDB Enterprise defaults to `nosync` when replication is configured; an explicit value wins. |
| `cairo.adaptive.commit.group.window` | `50ms` | The group-commit window (section 4). `0`: every acknowledged commit is on stable storage. Negative counts as 0. |
| `cairo.adaptive.epoch.interval` | `60s` | The time between periodic save points of a table. `0`: a save point after every apply pass. Negative: no periodic save points, no row cap and no idle save point; save points are then taken only after ALTER COLUMN TYPE, partition conversions, ATTACH and DETACH, and at a clean close only when one of those failed or was skipped; the WAL is kept from the older of the last two, which can be days old, and a crash replays all of it. Not for production. |
| `cairo.adaptive.epoch.max.rows` | `5000000` | A save point once this many rows were applied since the last one, whatever the interval. `0` or negative: off. |
| `cairo.adaptive.epoch.flush.on.close` | `true` | A clean writer close takes a save point over any transactions after the last one, so a clean restart restores nothing. |
| `cairo.adaptive.epoch.column.sync.batched` | `true` | On Linux, flush a save point's column files in one batch ending in one `syncfs`. `false`: a flush per column file, then the same `syncfs`. |
| `cairo.wal.commit.writeback.drain` | `true` | Start the write-back of every file of a WAL segment, then wait for it, before the commit's flushes, so the files are written back in parallel. Speed only. |
| `cairo.adaptive.recovery.roll.forward.enabled` | `true` | For tests. `false` stops the start when an adaptive table needs recovery. |

## Appendix B. Files

Each enrolled WAL table holds two save point files in its directory, `_epoch.manifest.0` and `_epoch.manifest.1`.
Each is one save point: a 120-byte header and copies of `_txn`, `_cv` and `_meta`. Do not edit, copy or delete them
while QuestDB runs. A switch out of `adaptive` removes them (section 8).

This version also changes three existing formats, in every mode, using space that older releases leave unused: a
checksum in each `_txn` record, a trailer after each `_cv` area, and a checksum in each record of the version 2
sequencer log (plus a hint in the version 1 log's header). No format version changes. Files an older release wrote
read as unverified. [`adaptive-commit-internals.md`](adaptive-commit-internals.md), section 14, has the details.
