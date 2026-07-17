# Crash-safe ALTER TABLE ... REBASE WAL (ADAPTIVE) — Design

**Goal:** Make `ALTER TABLE ... REBASE WAL` crash-safe under ADAPTIVE commit mode so a power
loss during the rebase never publishes a non-durable table that recovery suspends. Acceptance
oracle: `RandomizedAdaptiveCrashFuzzTest#testRebaseWalCrashSafeW0` (currently `@Ignore`d) goes
green — every durability-op crash point recovers the rebased table un-suspended, data preserved.

## Root cause (confirmed empirically + by code read)

`WalUtils.cloneTableDirForRebase` builds the new table in a staging dir (`ff.copy` of root files,
hard-linked partitions, mmap-reset `_meta`/`_txn`, `createSequencerFiles`) and `CairoEngine`
atomically renames it into place, then drops the old table + registers the new one, then
`commitRebaseSeed`. The clone **never msyncs/fdatasyncs** the staging files, and the atomic rename
makes only the *directory entry* durable, not the file *contents*. A power loss after the rename
(before OS writeback) publishes a table whose `_meta` is size 0 → `RecoveryCoordinator ->
validateMeta -> "File is too small"` → table suspended. Real Linux would lose the mmap-dirty pages
just the same, so this is a real production bug, not a facade artifact.

The `commitRebaseSeed` events-before-sequencer torn order is a *separate* already-fixed issue
(commit d8ea2daf84); it sits at the tail of the rebase and is only reached once the clone is durable.

## Two coupled fixes

### 1. Product: durably publish the staging table before the rename (ADAPTIVE-gated)

The correct atomic-publish pattern is *sync-before-publish*: everything the rename makes adoptable
must already be durable, because startup's `reloadFromRootDirectory` adopts the new dir by its
presence at the final path (a crash between drop and register still adopts it). Syncing *after* the
rename leaves a window where the dir is adoptable but not durable.

- In `cloneTableDirForRebase`, after the staging table is fully built and before returning (before
  `CairoEngine`'s rename), under `configuration.getCommitMode() == CommitMode.ADAPTIVE`: recursively
  make every staging file durable and fsync every directory. Files are already closed by this point,
  so each file is made durable by *re-mapping* it and issuing a full-range `MS_SYNC` msync +
  `fdatasync` (the append-narrowed `MemoryMARW.sync` cannot be used post-close; and `fsync` alone
  cannot advance the durable extent of mmap-written content). Hard-linked partition columns must be
  synced too: the durability model is per-path, so an unsynced new path is truncated to 0 on crash
  even though it shares the source's durable inode.
- In `CairoEngine`, after `ff.rename(...)` under ADAPTIVE, fsync the parent (db root) dir so the
  rename itself is durable. (Real-world correctness; a no-op under the facade, which walks the real
  post-rename FS.)

### 2. Test facade fidelity: `rename` must carry durability across paths

`CrashFaultFilesFacade` tracks durability per absolute path and does **not** override `rename`, so a
staging-path sync is orphaned when the dir is renamed to its final path and `crash()` truncates the
final file to 0. Real POSIX `rename` carries the inode's durable data to the new name, so the facade
is unfaithful here. Fix: override `rename` to re-key every path-keyed durability map
(`durableContent`, `deviceCacheContent`, `durableSize`, `writtenDataEnd`, `syncedDataEnd`,
`journaledDataEnd`, `pteFlushed`, `tornTails`, `trackedFiles`) from the old path — or old-dir prefix,
for a directory rename — to the new one. This is a fidelity fix (renames DO carry durability), not a
weakening of the oracle.

## Non-goals / scope

- Only ADAPTIVE is hardened here (matches the branch + the existing WAL-writer guards). NOSYNC keeps
  its fast, non-durable behavior; SYNC's DDL durability is a pre-existing separate concern.
- The broader engine-wide "DDL builds metadata files without msync/fsync" gap (normal CREATE TABLE,
  WAL conversion) is out of scope; this change is confined to the REBASE WAL clone path.

## Verification

Un-`@Ignore` `testRebaseWalCrashSafeW0`; the full ADAPTIVE W=0 crash sweep must pass (no suspend,
data preserved) and stay untruncated. Negative control: reverting either the clone sync or the facade
re-key must reproduce a suspend. `testConvertPartitionCrashSafeW0` and the other adaptive crash tests
must stay green (the facade re-key only makes renames more faithful).
