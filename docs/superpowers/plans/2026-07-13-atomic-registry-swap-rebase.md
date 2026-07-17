# Crash-atomic registry swap for REBASE WAL — Implementation Plan

> **For agentic workers:** executed inline (tightly-coupled single change). Steps use checkbox syntax.

**Goal:** Make the `tables.d` drop-old + register-new step of `ALTER TABLE … REBASE WAL` a single
crash-atomic durable operation, removing the last un-crash-safe window (the `dropTable → registerName`
gap that fails `testRebaseWalCrashSafeW0` at k=39).

**Architecture:** Add a batched `logSwapTable` (two `writeEntry` appends + one `sync`) to the
registry store; add a `swapTable` composite to `TableNameRegistryRW` that performs today's
drop+lock+register in-memory effects but routes the log write through `logSwapTable`; rewire
`CairoEngine.rebaseWalTable0` to call it.

**Tech Stack:** QuestDB OSS core, JDK25, JUnit4 fluent house style, branch `nw_adaptive_commit`.

## Global Constraints

- Do NOT weaken the crash oracle (D2 Step 4). Green must come from the product change, proven by a
  negative control.
- Mode-independent: the swap applies in all commit modes (the registry always syncs its own log).
- Keep the existing clone-durability sync (`WalUtils.syncStagingTreeDurable`) and the test-facade
  `rename` re-key — additive, both still required.
- `git` trailer on commits: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- JDK: `/usr/lib/jvm/java-25-openjdk-amd64`.

---

### Task 1: `logSwapTable` log primitive + `swapTable` registry composite

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/GrowOnlyTableNameRegistryStore.java` (add `logSwapTable`)
- Modify: `core/src/main/java/io/questdb/cairo/TableNameRegistry.java` (add `swapTable` decl)
- Modify: `core/src/main/java/io/questdb/cairo/TableNameRegistryRW.java` (impl `swapTable`)
- Modify: `core/src/main/java/io/questdb/cairo/TableNameRegistryRO.java` (read-only stub)
- Test: an existing `TableNameRegistry*Test` (locate; mirror its setup) — assert a swap replays to the new dir.

**Interfaces:**
- Produces: `void GrowOnlyTableNameRegistryStore.logSwapTable(TableToken old, TableToken new)`;
  `TableToken TableNameRegistryRW.swapTable(TableToken old, String name, String newDir, int newId, boolean isView, boolean isMatView, boolean isWal)` (returns live new token, or `null` on race).

- [ ] **Step 1 — `logSwapTable`** (`GrowOnlyTableNameRegistryStore`, after `logDropTable`):

```java
public synchronized void logSwapTable(final TableToken oldToken, final TableToken newToken) {
    // Atomic drop-old + register-new: append REMOVE then ADD and make BOTH durable in a single
    // sync, so a crash can never persist the drop without the add (see rebaseWalTable0). REMOVE is
    // written first to match reloadFromTablesFile's same-name repoint order.
    writeEntry(oldToken, OPERATION_REMOVE);
    writeEntry(newToken, OPERATION_ADD);
    tableNameMemory.sync(false);
}
```

- [ ] **Step 2 — interface decl** (`TableNameRegistry.java`, near `registerName`), with javadoc noting
  atomic replace of `oldToken`'s name→dir binding with `newToken`, returns `null` if the name is no
  longer bound to `oldToken`:

```java
TableToken swapTable(TableToken oldToken, String tableName, String newDirName, int newTableId, boolean isView, boolean isMatView, boolean isWal);
```

- [ ] **Step 3 — RO stub** (`TableNameRegistryRO.java`):

```java
@Override
public TableToken swapTable(TableToken oldToken, String tableName, String newDirName, int newTableId, boolean isView, boolean isMatView, boolean isWal) {
    throw CairoException.critical(0).put("instance is read only");
}
```

- [ ] **Step 4 — `swapTable` impl** (`TableNameRegistryRW.java`). WAL-only (rebase is WAL); reserves
  the shared name for the whole swap, one durable log step, restores the name on mid-swap failure:

```java
@Override
public TableToken swapTable(TableToken oldToken, String tableName, String newDirName, int newTableId, boolean isView, boolean isMatView, boolean isWal) {
    // Reserve the (shared) logical name for the duration of the swap so no concurrent create/drop
    // can grab it while old is being repointed to new.
    final ReverseTableMapItem oldReverse = dirNameToTableTokenMap.get(oldToken.getDirName());
    if (oldReverse == null || !tableNameToTableTokenMap.replace(tableName, oldToken, LOCKED_DROP_TOKEN)) {
        return null;
    }
    boolean published = false;
    try {
        // Build the authoritative new token (mirror lockTableName's flag resolution).
        final boolean isProtected = tableFlagResolver.isProtected(tableName);
        final boolean isSystem = tableFlagResolver.isSystem(tableName);
        final boolean isPublic = tableFlagResolver.isPublic(tableName);
        final String dbLogName = engine.getConfiguration().getDbLogName();
        final TableToken newToken = new TableToken(tableName, newDirName, dbLogName, newTableId, isView, isMatView, isWal, isSystem, isProtected, isPublic);

        // Metadata cache first (unsafe, can throw) — mirrors registerName ordering.
        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.dropTable(oldToken);
            if (!newToken.isView()) {
                metadataRW.hydrateTable(newToken);
            }
        }

        // Single durable step: DROP old + ADD new.
        nameStore.logSwapTable(oldToken, newToken);

        // Reverse map: old dir dropped (purge reclaims it), new dir live.
        dirNameToTableTokenMap.put(oldToken.getDirName(), ReverseTableMapItem.ofDropped(oldToken));
        dirNameToTableTokenMap.put(newDirName, ReverseTableMapItem.of(newToken));

        // Publish: the logical name now resolves to the new dir. Queryable from here.
        published = tableNameToTableTokenMap.replace(tableName, LOCKED_DROP_TOKEN, newToken);
        assert published;
        return newToken;
    } finally {
        if (!published) {
            // Mid-swap failure before the durable commit: restore the name to the old table.
            tableNameToTableTokenMap.replace(tableName, LOCKED_DROP_TOKEN, oldToken);
        }
    }
}
```

- [ ] **Step 5 — locate a registry test** (`rg "class TableNameRegistr.*Test" core/src/test`), read
  its setup, add a test: create WAL table `t` (dir `t~1`), `swapTable(t~1, "t", "t~2", 2, …)`, force a
  registry `reload()`, assert `getTableToken("t").getDirName() == "t~2"` and old dir is `ofDropped`.
- [ ] **Step 6 — compile + run the new unit test**: `mvn -q -pl core test -Dtest=<TestClass>#<method>`
  under JDK25. Expected: PASS.
- [ ] **Step 7 — commit** (`feat: atomic logSwapTable + TableNameRegistryRW.swapTable`).

---

### Task 2: Rewire `rebaseWalTable0` to the atomic swap

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/CairoEngine.java:2840-2869`

- [ ] **Step 1** — replace the drop/lock/register/unlock block with:

```java
// Commit the swap in the registry as ONE crash-atomic durable step: drop the old table and
// register the rebuilt dir together (single tables.d sync), so a power loss can never leave the
// old table dropped with the new one unregistered. The dir was already renamed into place above;
// a crash before this swap leaves the new dir as a duplicate-name orphan and keeps the old table.
final TableToken swapped = tableNameRegistry.swapTable(
        oldToken, tableName, newDirName, newTableId, oldToken.isView(), oldToken.isMatView(), true
);
if (swapped == null) {
    throw CairoException.nonCritical()
            .put("rebase target name was taken concurrently [table=").put(tableName).put(']');
}
oldTableDropped = true;
newToken = swapped;
```

- [ ] **Step 2** — confirm the surrounding `renamed` / `oldTableDropped` / `catch` rollback still
  reads correctly (the swap is the point of no return, as the old `dropTable` was). Remove the stale
  "NOT atomic … reloadFromRootDirectory adopts it (without the empty seeds)" wording; keep the
  seed-purpose comment below.
- [ ] **Step 3 — compile** `mvn -q -pl core -am compile` (JDK25). Expected: success.
- [ ] **Step 4 — commit** (`refactor: rebaseWalTable0 uses atomic registry swap`).

---

### Task 3: Prove crash-safety — un-`@Ignore` sweep + negative control

**Files:**
- Modify: `core/src/test/java/io/questdb/test/cairo/crash/RandomizedAdaptiveCrashFuzzTest.java`
  (remove `@Ignore` + its import; keep the deep-dive oracle changes)

- [ ] **Step 1** — remove `@Ignore` from `testRebaseWalCrashSafeW0` and the unused `import org.junit.Ignore;`.
- [ ] **Step 2 — run the sweep**:
  `mvn -q -pl core test -Dtest=RandomizedAdaptiveCrashFuzzTest#testRebaseWalCrashSafeW0` (JDK25,
  background + done-marker; watch for spurious "completed"). Expected: PASS, full N-point sweep, no
  suspend, `cf_rbs` recovered with 4 rows at every k.
- [ ] **Step 3 — assert orphan tolerance**: confirm from the run log that at pre-swap crash points
  the old table wins (no "vanished") and any `CheckWalTransactionsJob` orphan-sequencer line is
  non-fatal (recovery completes). If it is fatal, STOP — the design's tolerance assumption is wrong.
- [ ] **Step 4 — negative control**: temporarily point `swapTable` at the old two-sync pair
  (`nameStore.logDropTable(oldToken); nameStore.logAddTable(newToken);` instead of `logSwapTable`),
  re-run the sweep, confirm it reproduces the k=39 "rebased table vanished" suspend, then revert.
- [ ] **Step 5 — commit** (`test: un-ignore testRebaseWalCrashSafeW0 (atomic swap crash-safe)`).

---

### Task 4: Regression + lint + finish

- [ ] **Step 1 — regression**: run the sibling adaptive crash sweeps that share the facade/infra
  (`testConvertPartitionCrashSafeW0` + the other `RandomizedAdaptiveCrashFuzzTest` methods and the
  D1 `AbstractAdaptiveCrashSweepTest` subclasses). Expected: all green.
- [ ] **Step 2 — java-lint / import order**: ensure new imports (`MetadataCacheWriter`,
  `ReverseTableMapItem` already present) are ordered per IntelliJ formatter; run the repo's format
  check if quick, else eyeball against neighbours.
- [ ] **Step 3 — update ledger** (`.superpowers/sdd/progress.md`) + memory
  (`project_adaptive_applynonstructural_torn_seq.md` / the rebase entry).
- [ ] **Step 4 — final commit** if any lint/doc changes remain; keep everything on `nw_adaptive_commit`.
