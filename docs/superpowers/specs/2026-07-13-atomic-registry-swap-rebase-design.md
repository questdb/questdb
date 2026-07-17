# Crash-atomic registry swap for REBASE WAL — Design

**Goal:** Make the `tables.d` **drop-old + register-new** step of `ALTER TABLE … REBASE WAL` a
single crash-atomic durable operation, so a power loss during the swap can never leave a
half-swapped registry that recovery must paper over with best-effort startup orphan-adoption. This
closes the last un-crash-safe window in REBASE WAL (the `dropTable → registerName` gap), completing
the crash-safe-rebase work whose clone-durability half is already done.

## Background: what's already fixed vs. what remains

The crash-fuzz sweep `RandomizedAdaptiveCrashFuzzTest#testRebaseWalCrashSafeW0` proved two distinct
bugs in REBASE WAL under a durability-faulting facade:

1. **Clone not durable before publish** (the `_meta size=0 → suspend` bug). Fixed by
   `WalUtils.cloneTableDirForRebase` syncing the staging tree before `CairoEngine`'s
   `ff.rename` (`syncStagingTreeDurable`), plus the test-facade `rename` durability re-key. With
   those, the sweep's entire **clone + rename** portion is crash-safe (crash points k=1..38 pass).

2. **Drop/register not atomic** (this design). The first surviving failure, k=39, crashes inside
   `CairoEngine.rebaseWalTable0` at `dropTable(oldToken)` (`CairoEngine.java:2847`) — the window
   *after* the new dir `…~N` is renamed into place but *before* `registerName`. The old table's DROP
   reaches `tables.d` durably while the new table's ADD never does; recovery then finds the new dir
   durable on disk but absent from the name registry and leans on `reloadFromRootDirectory`
   orphan-adoption to bring it back. `rebaseWalTable0`'s own comment (`CairoEngine.java:2843-2846`)
   admits this: *"The drop and the register are NOT atomic … startup's reloadFromRootDirectory
   adopts it (without the empty seeds) … the table comes back, just unseeded."*

The two bugs are independent and both fixes are required: (1) guarantees the published dir's contents
are durable; (2) guarantees the name swap that publishes it is all-or-nothing. This design is (2).

This is **not** ADAPTIVE-specific. The registry always `msync`s its own log regardless of commit
mode (`GrowOnlyTableNameRegistryStore.logAddTable/logDropTable` each call `sync(false)`), so the
drop-then-register two-sync window exists in NOSYNC and SYNC too. The fix is therefore
mode-independent.

## Root cause (code-confirmed)

`tables.d` is an append log of fixed-shape records: `operation:int (ADD=0 / REMOVE=-1)`,
`tableName:Str`, `dirName:Str`, `tableId:int`, `tableType:int`, and (ADD only) 8 reserved longs. The
private `GrowOnlyTableNameRegistryStore.writeEntry(token, op)` (`:71-89`) appends one record and
rewrites the 8-byte header@0 to the new append offset — the header is the commit pointer replay
trusts (`reloadFromTablesFile` reads records `while offset < header`). The **public** mutators each
force their own durability:

```java
public synchronized void logAddTable (TableToken t){ writeEntry(t, OPERATION_ADD);    tableNameMemory.sync(false); }
public synchronized void logDropTable(TableToken t){ writeEntry(t, OPERATION_REMOVE); tableNameMemory.sync(false); }
```

So a rebase performs **two** independent durable steps and a crash can persist the first without the
second. The batch primitive already exists privately: `compactTableNameFile` writes N `writeEntry`s
— including a back-to-back ADD+REMOVE for one token — followed by a **single** `sync(false)`
(`TableNameRegistryStore.java:231-242`). The facade arms a crash *at* an `msync` op and skips it
(the op never partially applies), so **one** sync over both records is genuinely all-or-nothing at
the crash-point granularity.

## Design: one durable swap

### 1. Log primitive — `GrowOnlyTableNameRegistryStore.logSwapTable(old, new)`

```java
public synchronized void logSwapTable(TableToken oldToken, TableToken newToken) {
    writeEntry(oldToken, OPERATION_REMOVE);   // append DROP (no sync)
    writeEntry(newToken, OPERATION_ADD);      // append ADD  (no sync)
    tableNameMemory.sync(false);              // ONE durable step for both
}
```

REMOVE is written first so replay folds it before the ADD (matching today's order and
`reloadFromTablesFile`'s same-name repoint logic at `:476-508`). Reuses the exact append+header
mechanics of `logAddTable`/`logDropTable`; the RW `writeEntry` override still enforces the registry
lock.

### 2. Registry composite — `TableNameRegistryRW.swapTable(oldToken, tableName, newDirName, newTableId, isView, isMatView, isWal)` → `TableToken`

Performs the in-memory effects of today's `dropTable(old)` + `lockTableName` + `registerName(new)`
but routes the log write through `logSwapTable` (one sync). Mirrors the existing lock protocol:

- reserve: `tableNameToTableTokenMap.replace(tableName, oldToken, LOCKED_DROP_TOKEN)`; return `null`
  on failure (lost the race — caller aborts as today's `lockTableName == null` path does);
- build the authoritative `newToken` exactly as `lockTableName` does (flag resolver + dbLogName);
- metadata cache, unsafe-first (as `registerName`): `metadataRW.dropTable(oldToken)` then, if not a
  view, `metadataRW.hydrateTable(newToken)`;
- **`nameStore.logSwapTable(oldToken, newToken)`** — the single durable step;
- reverse map: `dirNameToTableTokenMap.put(oldDir, ofDropped(oldToken))` (old dir marked dropped so
  the purge job reclaims it) and `put(newDir, of(newToken))` (new dir live);
- publish: `replace(tableName, LOCKED_DROP_TOKEN, newToken)` — table queryable as the new dir;
- return `newToken`.

### 3. Rewire `CairoEngine.rebaseWalTable0`

Replace the `dropTable(oldToken)` / `lockTableName` / `registerName(newToken)` / `unlockTableName`
block (`CairoEngine.java:2847-2869`) with a single `swapTable(...)` call returning the live
`newToken` used for `commitRebaseSeed`. The `catch`/rollback logic keys off `oldTableDropped`; set
that flag from the `swapTable` result (the swap is the point of no return, exactly as the old drop
was). Update the now-stale "NOT atomic … reloadFromRootDirectory adopts it" comment to describe the
atomic swap.

## Crash analysis (post-fix)

The dir rename (`CairoEngine.java:2835`) still precedes the registry swap; the two live in different
durability domains (FS metadata vs. `tables.d`) and cannot be one op, so the safe ordering is
dir-first — an unregistered orphan dir is recoverable; a registered dirless table is not.

- **Crash at/before the swap sync** → header@0 unchanged; replay sees neither record → **old table
  intact** (name still maps to old dir, old dir present). The renamed new dir `…~N` is an orphan
  whose `_name` says the same table name; `reloadFromRootDirectory` hits the existing "duplicate
  table name found, table will not be available" branch and keeps the old table. No suspend, no data
  loss. (Orphan-dir/sequencer cleanup + the tolerated `CheckWalTransactionsJob` "could not process
  table sequencer" log line for that orphan are pre-existing recovery noise — see Non-goals.)
- **Crash after the swap sync** → both records durable → replay repoints the name to the new dir →
  **new table live**. `commitRebaseSeed`'s own durability is the separate, already-fixed torn-seq
  concern.

No intermediate "old dropped, new not yet registered" durable state exists any more.

## Non-goals / scope

- **Registry log page-ordering / torn-record integrity.** A single `sync` msyncs `[0, size)` with no
  barrier ordering the header@0 page after the record bodies and no per-record checksum. This is a
  pre-existing property of *every* registry op (single add/drop today), not introduced here, and
  belongs to a separate registry-hardening effort. This design matches the existing durability model
  and only removes the *two-sync* gap.
- **Orphan-dir cleanup + recovery-log noise.** When a crash lands in the dir-rename→swap window the
  renamed dir is left on disk as a duplicate-name orphan and its sequencer files make
  `CheckWalTransactionsJob` log a tolerated (non-fatal, recovery still completes) error. Reclaiming
  that orphan and silencing the log is polish, tracked separately; it does not affect crash-safety
  (old table survives).
- **Other drop+register pairers.** `renameToNew` (ALTER RENAME) and non-WAL `rename0` have the same
  two-sync shape but different semantics (same dirName / register-then-drop) and are not part of this
  change; they can adopt `logSwapTable` later if desired.
- The clone-durability sync (`syncStagingTreeDurable`) and the test-facade `rename` re-key from the
  first half stay; this design is additive.

## Verification

- Un-`@Ignore` `testRebaseWalCrashSafeW0`; the full ADAPTIVE W=0 crash sweep passes — every crash
  point recovers `cf_rbs` un-suspended with its 4 rows (fingerprint match), old-table-wins or
  new-table-live depending on which side of the swap sync the crash falls.
- **Negative control:** reverting `logSwapTable` back to two syncs (or pointing `swapTable` at the
  old `dropTable`/`registerName` pair) must reproduce the k=39 "rebased table vanished" suspend.
- `testConvertPartitionCrashSafeW0` and the other adaptive crash sweeps stay green (shared infra
  untouched by the product change; the facade re-key is unchanged).
- A focused unit assertion that `logSwapTable` emits DROP-before-ADD and both survive a replay, and
  that a single armed crash at its sync leaves the pre-swap state.
