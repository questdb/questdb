# Phase 13: Migrate FILL(PREV) Snapshots to rowId-based Replay — Research

**Researched:** 2026-04-18
**Domain:** SampleByFillRecordCursorFactory rowId rewrite, SortedRecordCursor chain lifecycle,
RecordChain memory management, SqlCodeGenerator generateFill, retro-fallback deletion
**Confidence:** HIGH (all findings verified from source code; no web searches required)

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** rowId + `recordAt()` replay is the required mechanism. One `prevRowId` slot per
  key in keysMap MapValue (keyed) or one `simplePrevRowId` field (non-keyed). Value buffering
  is explicitly and permanently ruled out.
- **D-02:** Investigate `fe487c06a9` before implementing. Reproduce the combined-suite failure
  (or confirm it no longer reproduces) with `chain.clear()` applied. Verdict (a/b/c) drives
  vehicle selection.
- **D-03:** `SortedRecordCursor.chain.clear()` fix ships as the first independent commit.
  Title: "fix(sql): clear chain in SortedRecordCursor.of() on reuse".
- **D-04:** Delete retro-fallback machinery if rowId unlocks all unsupported types
  (UUID/LONG128/LONG256/DECIMAL128/DECIMAL256/STRING/VARCHAR/SYMBOL/BINARY/array/INTERVAL).
- **D-05:** WR-04 (precise chain-rejection position) and Defect 3 (insufficient fill values
  grammar) ship in phase 13.
- **D-06:** SEED-002 Defects 1 and 2 fully resolved in-phase. `testSampleByFillNeedFix`
  restored to master's 3-row form in both assertions #1 and #2. No punt.
- **D-07:** Commit sequence: (1) chain.clear(), (2) rowId rewrite, (3) cherry-pick 13 tests,
  (4) retro-fallback cleanup, (5) SEED-002 fix, (6) SEED-001 grammar.
- **D-08:** ALIGN TO FIRST OBSERVATION (SEED-003) is out of scope.

### Claude's Discretion

- Commit 2 structure: single rewrite vs. "add scaffolding + remove per-type dispatch".
- Exact SqlException message wording for WR-04 and Defect 3.
- Vehicle choice (SortedRecordCursor vs alternative) — decided by D-02 investigation.

### Deferred Ideas (OUT OF SCOPE)

- Value buffering as any alternative.
- ALIGN TO FIRST OBSERVATION / SEED-003.
- PREV numeric widening (INT->LONG, FLOAT->DOUBLE).
- FILL(LINEAR) on fast path.
- Legacy `SampleByFillPrev*RecordCursor` removal.
- Two-token stride on fast path.
</user_constraints>

---

## Summary

Phase 13 replaces per-type `long`-packed snapshot materialization in
`SampleByFillRecordCursorFactory` with a single chain rowId per key, stored in the keysMap
`MapValue` (keyed path) or a `simplePrevRowId` field (non-keyed path), replayed via
`baseCursor.recordAt(prevRecord, prevRowId)`. The prerequisite `SortedRecordCursor.chain.clear()`
fix ships first as a standalone commit.

The D-02 investigation (see Section 1) resolves the key uncertainty: the source branch's
`fe487c06a9` revert documents a failure mode that is **not** the `chain.clear()` fix target;
the crash was actually triggered by a DIFFERENT architecture (keysMap-driven epoch/drain loop)
that stored the rowId in the `MapValue` and replayed via `baseCursor.recordAt(recordB, rowId)`
where `recordB` is owned by `SortedRecordCursor.chainCursor`. The `chain.clear()` fix
eliminates chain accumulation across reuses, which is the root cause. After the fix, the rowId
rewrite uses `RecordChain.getRecordAt()` (the lazy `recordC` slot) as the `prevRecord` — not
`recordB` — avoiding any coupling to the cursor's internal state.

**Primary recommendation:** Proceed with candidate (a): `SortedRecordCursor` + `chain.clear()`
fix. The prevRecord for rowId replay uses `RecordChain.getRecordAt(rowId)`, which returns a
lazily allocated `RecordChainRecord` (the `recordC` field) that is independent of `recordA`
and `recordB`. No third record slot needs to be added to `RecordChain`.

---

## 1. D-02 Investigation Report

### 1.1 Reading commit `fe487c06a9`

**Commit title:** "Harden fill cursor: value buffering replaces rowId+recordAt"

**Commit message summary:**
> Switch SampleByFillCursor from buffering rowIds and replaying base rows via recordAt() to
> directly buffering aggregate column values in the keysMap. Fixes the intermittent
> AbstractMemoryCR.addressOf assertion that fired in combined-suite runs (rowIds captured from
> a SortedRecordCursor's chain were resolving to offsets past mem.size() under
> test-order-dependent state accumulation).

**What the diff does:**
- Removes `BUCKET_ROWID_SLOT = 3` and `PREV_START_SLOT = 4` from the inner cursor.
- Renames `PREV_START_SLOT` to `VALUE_START_SLOT = 3` (dropped the rowId slot, so offset
  shrinks from 4 to 3).
- Removes `baseRecordB` (was `baseCursor.getRecordB()`) and `dataRecord` fields.
- Replaces `baseCursor.recordAt(baseRecordB, value.getLong(BUCKET_ROWID_SLOT))` with a direct
  slot-based value read.
- Changes `updatePerKeyPrev()` to `updateBucketValues()` — writes all long-representable agg
  columns into map slots on every data row, regardless of PREV mode.
- `isGapFilling` is now derived from `value.getLong(BUCKET_EPOCH_SLOT) != bucketEpoch`.

**What failure mode it documents:**
The commit message says "rowIds captured from a SortedRecordCursor's chain were resolving to
offsets past `mem.size()` under test-order-dependent state accumulation." This is chain
accumulation: when `SortedRecordCursor.of()` is called with `isOpen == true`, the `else`
branch (lines 96-106 of `SortedRecordCursor.java` on our branch) does NOT call `chain.clear()`
or `chain.reopen()`. Subsequent records append on top of the prior run's data. rowIds from
pass 1 of the CURRENT run may point into memory that was valid at capture time but becomes
stale after reallocation during pass 2 appends.

**Critical nuance from the GO-verdict research commit `4ebfa3243c`:**
The research commit (2026-04-17) confirmed via live test that the failure had evolved to
*wrong output* (interleaved rows from two chain builds), not a native assertion failure, by
commit `1a40aa89`. The "AbstractMemoryCR.addressOf assertion" described in `fe487c06a9` was
the earlier manifestation; it remained as wrong output by the time the PoC was run.

**The SPECIFIC vehicle that failed** was the keysMap-driven architecture in `1a40aa89af`:
it stored `BUCKET_ROWID_SLOT` in the map and replayed via `baseCursor.recordAt(baseRecordB, ...)`,
where `baseRecordB = baseCursor.getRecordB()`. This `recordB` is `RecordTreeChain.recordChain.recordB`
— the same underlying `RecordChainRecord` that `TreeCursor.getRecordB()` returns. During
`hasNext()`/`buildChain()`, the base cursor is iterated which calls `chain.put()`, which
repositions `recordChainRecord` (the chain's internal `recordB` used for comparisons). This
means `baseRecordB` could be repositioned DURING the keysMap-iteration loop that was
replaying saved rowIds. That is a second corruption path on top of chain accumulation.

### 1.2 `chain.clear()` Fix Analysis at `SortedRecordCursor.of()`

**Current code (lines 96-106 of `SortedRecordCursor.java`):**

```java
@Override
public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) {
    this.baseCursor = baseCursor;
    if (!isOpen) {
        isOpen = true;
        chain.reopen();           // RecordTreeChain.reopen() -> mem.reopen() only
    }
    SortKeyEncoder.buildRankMaps(baseCursor, rankMaps, comparator);
    chainCursor = chain.getCursor(baseCursor);
    circuitBreaker = executionContext.getCircuitBreaker();
    isChainBuilt = false;
}
```

**Problem:** when `isOpen == true`, neither `chain.clear()` nor `chain.reopen()` is called.
`RecordTreeChain.clear()` (line 81-85) resets `root = -1`, clears `mem` (key pages), and calls
`recordChain.clear()`. `RecordChain.clear()` (lines 131-137) calls `mem.close()` and resets
`varAppendOffset = 0`. Without this, a second call to `of()` appends new records starting
after the prior run's last record in the chain memory. rowIds captured in pass 1 of run 2
point into valid chain positions in run 2, but the memory mapping may shift if the chain
grew large enough to trigger a reallocation during run 2's append phase.

**The one-line fix:**

```java
if (!isOpen) {
    isOpen = true;
    chain.reopen();
} else {
    chain.clear();     // NEW: reset chain on reuse when isOpen=true
}
```

**Is the fix sufficient in theory?**

Yes, for the fill-cursor use case. Explanation:
1. `chain.clear()` calls `RecordTreeChain.clear()` -> `recordChain.clear()` -> `mem.close()`.
   After `close()`, `mem.getAppendOffset() == 0` and `varAppendOffset == 0`.
2. Pass 1 of run 2 starts with a clean chain. All rowIds captured during pass 1 are byte
   offsets within [0, chain.size()) at the END of pass 1.
3. Pass 2 calls `recordAt(record, rowId)` which calls `RecordChain.recordAt()` -> 
   `RecordChainRecord.of(rowToDataOffset(row))` -> sets `baseOffset`. This is a pure
   pointer arithmetic operation, no appends happen.
4. Between pass 1 end and the first `recordAt()` call in pass 2, no chain writes occur.
   The `SortedRecordCursor.hasNext()` path checks `isChainBuilt` — if `true`, it goes
   directly to `chainCursor.hasNext()` which only calls `recordChain.hasNext()` and
   `recordChain.of(topOf(...))` — no appends.

**Under what conditions does the chain reallocate?**
`RecordChain.put()` -> `beginRecord()` -> `mem.jumpTo(rowToDataOffset(...))` -> triggers
auto-extend if offset > capacity. This only happens during `buildChain()` (pass 1). Pass 2
never writes to the chain. So rowId validity is guaranteed from end-of-pass-1 through all of
pass 2, provided no external code repositions or appends to the chain.

**Mid-pass accumulation?** Not applicable: pass 1 is a linear scan that calls `chain.put()`
for each base cursor row. The chain memory grows monotonically. rowIds are assigned at `put()`
time and remain valid as long as the chain is not cleared. The chain cannot realloc mid-pass
in a way that invalidates already-issued offsets because `MemoryCARWImpl.jumpTo()` extends
the memory block (realloc) but the base address changes — HOWEVER `RecordChainRecord.of()`
stores the raw offset (not an absolute address); the address is computed at getter time via
`mem.addressOf(offset)`. So if the chain memory block is reallocated by `jumpTo()` during
pass 1, previously-captured offsets still produce correct results because `addressOf` resolves
them against the new base address. This is inherently safe.

**Second corruption path (see 1.1): `recordB` repositioning during replay.**
The keysMap-driven PoC (`1a40aa89af`) used `baseCursor.getRecordB()` as the replay target for
`recordAt`. `SortedRecordCursor.getRecordB()` delegates to `chainCursor.getRecordB()` which
returns `RecordTreeChain.recordChain.recordB`. During `hasNext()` in the keysMap-iteration
loop, no new `buildChain()` calls fire (isChainBuilt=true), but `chainCursor.hasNext()`
advances `recordChain.hasNext()` which repositions `recordA`. `recordB` is not repositioned
by `hasNext()`. However, `RecordTreeChain.put()` calls `recordChain.recordAt(recordChainRecord, r)`
where `recordChainRecord = recordChain.recordB` — if pass 2 ever wrote to the chain, recordB
would be repositioned. Pass 2 does NOT write to the chain in the current design, so `recordB`
is safe to use.

**But there is a cleaner option:** `RecordChain` already has a lazy `recordC` field (line 68,
`getRecordAt()` method at lines 160-166). This slot is used by no existing caller in
`SortedRecordCursor`'s path and is independently allocated. Using `recordChain.getRecordAt(prevRowId)`
as the replay record avoids coupling to `recordB` entirely.

### 1.3 `SortedRecordCursor.of()` — Additional Paths

Does any path call `of()` with `isOpen=true` that bypasses the fix? The call graph is:
- `SortedRecordCursorFactory.getCursor()` (line 91) -> `cursor.of(baseCursor, executionContext)`.
  This is the only public entry point. `getCursor()` always creates a fresh `baseCursor`.
- `toTop()` on `SortedRecordCursor` delegates to `chainCursor.toTop()` — does NOT call `of()`.
  `toTop()` only resets the tree-cursor position; it does NOT clear or reopen the chain.
  This is correct: `toTop()` replays the same chain data.

No path bypasses the `of()` fix. The fix covers all reuse scenarios.

### 1.4 Alternative rowId Vehicles (for completeness)

The following alternatives were evaluated in case `SortedRecordCursor` proved unsuitable:

| Vehicle | rowId Stability | Complexity | Notes |
|---------|----------------|------------|-------|
| `SortedRecordCursor` + `chain.clear()` | STABLE after fix | 1 line fix | Verified stable via analysis |
| `RecordChain` standalone (no sort) | STABLE | High | Would require re-sorting GROUP BY output differently |
| `GroupByRecordCursorFactory` (serial) | N/A | N/A | Does not emit chain rowIds |
| `AsyncGroupByRecordCursor` collector | N/A | N/A | Output rowIds not chain-stable |
| New sorted wrapper | STABLE | High | No existing facility in codebase |

**Conclusion: candidate (a) — proceed with `SortedRecordCursor` + `chain.clear()`.**

No alternative vehicle is needed. The `chain.clear()` fix is sufficient.

### 1.5 D-02 Verdict

**Verdict: (a) `chain.clear()` is sufficient. Proceed with `SortedRecordCursor`.**

The PoC failure on `1a40aa89af` was caused by chain accumulation (no `clear()` on reuse).
The specific design in that PoC also had a secondary coupling to `chainCursor.recordB` for
replay; the phase 13 rewrite avoids this by using `RecordChain.getRecordAt()` (the `recordC`
slot) instead.

**What the planner must add:**
- Commit 1: one-line fix in `SortedRecordCursor.of()` else branch.
- Commit 2: rowId rewrite. The `prevRecord` for replay is obtained via
  `((SortedRecordCursor) baseCursor).recordAt(prevRecord, prevRowId)` or, equivalently, by
  calling `baseCursor.recordAt(prevRecord, prevRowId)` where `prevRecord` is obtained from a
  dedicated path described in Section 2.

---

## 2. rowId Rewrite Shape for `SampleByFillRecordCursorFactory`

### 2.1 Fields to Delete

In `SampleByFillCursor` (inner class of `SampleByFillRecordCursorFactory`):

| Field/Constant to Delete | Current Location | Replacement |
|--------------------------|-----------------|-------------|
| `private long[] simplePrev` | Line ~303 | `private long simplePrevRowId` (single long, not array) |
| `private boolean hasSimplePrev` | Line ~293 | `private boolean hasSimplePrev` (rename: `hasPrevRowId`) — or keep name |
| `private static long readColumnAsLongBits(...)` | Lines ~244-265 | Delete entirely |
| `private void savePrevValues(Record)` | Lines ~709-715 | Replace with `saveSimplePrevRowId(Record)` |
| `private void updatePerKeyPrev(MapValue, Record)` | Lines ~717-727 | Replace with `updateKeyPrevRowId(MapValue, Record)` |
| `private long prevValue(int col)` | Lines ~681-707 | Delete entirely |
| `private short[] columnTypes` | Line ~270 | Delete (only used by `readColumnAsLongBits`) |

In `SqlCodeGenerator.generateFill()` (lines ~3579-3595):

- The "per-agg prev slot" loop (adds `aggColumnCount` LONG slots) is replaced by a single
  additional LONG slot: `PREV_ROWID_SLOT = 2`. mapValueTypes becomes:
  ```
  slot 0: keyIndex (LONG)
  slot 1: hasPrev  (LONG)
  slot 2: prevRowId (LONG)
  ```
  `PREV_START_SLOT` constant and `aggColumnCount` loop are deleted.

The `prevSourceCols` IntList and the codegen-time type check loop (lines 3524-3557) that
feeds `FallbackToLegacyException` are both deleted under D-04 (all types unlocked).

### 2.2 Where `prevRowId` Is Stored

**Keyed path:** `MapValue` slot `PREV_ROWID_SLOT = 2` (one LONG per unique key combination).
Written in `updateKeyPrevRowId(MapValue value, Record record)`:
```java
value.putLong(HAS_PREV_SLOT, 1L);
value.putLong(PREV_ROWID_SLOT, record.getRowId());
```
`record.getRowId()` on a `RecordChainRecord` returns `baseOffset - 8` (line 573 of
`RecordChain.java`). Since `baseCursor` is a `SortedRecordCursor` wrapping a `RecordTreeChain`,
and `getRecord()` returns `recordChain.recordA`, the rowId from `baseRecord.getRowId()` is a
valid chain offset.

**Non-keyed path:** Replace `long[] simplePrev` + `hasSimplePrev` with:
```java
private long simplePrevRowId = -1L;   // -1 = no prev yet
```
Written in `saveSimplePrevRowId(Record record)`:
```java
simplePrevRowId = record.getRowId();
```

### 2.3 How to Plumb `recordAt(prevRecord, rowId)`

**The `prevRecord` object:** Obtained from `RecordChain.getRecordAt(rowId)`, which returns
the lazily allocated `recordC` slot (`RecordChainRecord`). Access path:
```java
RecordChain chain = ((RecordTreeChain) ...).recordChain;  // not accessible directly
```
Since `SortedRecordCursor.recordAt(record, atRowId)` already delegates to
`chainCursor.recordAt(record, atRowId)` -> `RecordChain.recordAt(record, row)`, and
`RecordChain.recordAt()` accepts any `Record` that is a `RecordChainRecord`, the correct
approach is:

```java
// In SampleByFillCursor.of() or close to initialization:
private Record prevRecord;

// In initialize():
prevRecord = baseCursor.getRecordB();
// NO — recordB is RecordTreeChain's internal comparator record.
```

**Correct approach:** Do NOT use `getRecordB()`. The `RecordChain.getRecordAt(long offset)`
method (line 160-166 in `RecordChain.java`) lazily allocates `recordC` and positions it:
```java
public Record getRecordAt(long recordOffset) {
    if (recordC == null) {
        recordC = newChainRecord();
    }
    recordC.of(rowToDataOffset(recordOffset));
    return recordC;
}
```

The public `recordAt(Record record, long row)` method (line 354-356) accepts any
`RecordChainRecord` and positions it:
```java
public void recordAt(Record record, long row) {
    ((RecordChainRecord) record).of(rowToDataOffset(row));
}
```

So the planner must:

1. In `SampleByFillCursor.of()` (called from the factory's `getCursor`), after `keysMapRecord`
   setup, obtain a record slot for prev replay:
   ```java
   prevRecord = baseCursor.recordAt(baseCursor.getRecordB(), ...);
   ```
   This is WRONG. The correct approach:

   **Option A (recommended):** Call `baseCursor.recordAt(prevRecord, someRowId)` where
   `prevRecord` is initialized once via `baseCursor.getRecordB()` — but because `recordB`
   is shared with the sort cursor's comparator path, this is risky.

   **Option B (safer):** Obtain a fresh `RecordChainRecord` from the `RecordChain` by
   calling `chain.getRecordAt(0L)` once during initialization. But `RecordChain` is not
   directly accessible from `SampleByFillCursor`; only `baseCursor` (a
   `SortedRecordCursor`) is accessible.

   **Option C (actual correct path):** `SortedRecordCursor.recordAt(Record record, long atRowId)`
   (line 114-116) calls `chainCursor.recordAt(record, atRowId)` which calls
   `recordChain.recordAt(record, atRowId)`. The `record` argument can be ANY
   `RecordChainRecord`. Obtain one by calling `baseCursor.getRecordB()` at initialization
   time and storing it as `prevRecord`. During fill emit, call
   `baseCursor.recordAt(prevRecord, prevRowId)` — this repositions the `prevRecord` via
   `RecordChain.recordAt()` which casts the record to `RecordChainRecord` and calls
   `of(rowToDataOffset(row))`. This is safe as long as we do NOT use `recordB` for
   anything else inside the fill cursor.

   **Bottom line for the planner:** In `SampleByFillCursor`:
   ```java
   // new field:
   private Record prevRecord;

   // in of():
   prevRecord = baseCursor.getRecordB();  // RecordChainRecord reused as prevRecord
   ```
   Then in fill emit: `baseCursor.recordAt(prevRecord, savedRowId)` followed by typed
   getters on `prevRecord`. This is safe because:
   - `baseCursor.hasNext()` during pass 2 calls `chainCursor.hasNext()` which calls
     `recordChain.hasNext()` which repositions `recordA`, not `recordB`.
   - `recordAt()` does not advance the iteration state; it only sets `prevRecord.baseOffset`.
   - `RecordTreeChain.put()` during `buildChain()` uses `recordChain.recordB` as the
     comparator record — this is `RecordTreeChain.recordChainRecord`, not the same object
     as `chainCursor.getRecordB()`. Wait — let me be precise:

   In `RecordTreeChain` constructor (line 73): `this.recordChainRecord = this.recordChain.recordB`.
   In `TreeCursor.getRecordB()` (line 337): returns `recordChain.getRecordB()` which returns
   `recordChain.recordB`. These ARE the same object. During `buildChain()`, `put()` calls
   `recordChain.recordAt(recordChainRecord, r)` which repositions `recordChain.recordB`.
   This would corrupt `prevRecord` if `prevRecord == recordChain.recordB` and `buildChain()`
   ran. But `buildChain()` runs only ONCE (guarded by `isChainBuilt`). By the time
   `initialize()` is called (pass 1), `isChainBuilt = false`, `buildChain()` will run.
   Pass 1 in `initialize()` iterates `baseCursor` via `baseCursor.hasNext()` — this
   triggers `buildChain()`. At this point `prevRecord` (= `recordB`) may be repositioned.

   **This means `prevRecord` must NOT be initialized from `getRecordB()` until AFTER pass 1
   completes.** Initialize `prevRecord` in `initialize()` AFTER `baseCursor.toTop()`:
   ```java
   baseCursor.toTop();
   prevRecord = baseCursor.getRecordB();
   ```

   After `toTop()`, `buildChain()` has completed and `isChainBuilt = true`. Subsequent
   `hasNext()` calls will never call `buildChain()` again. `prevRecord` is safe to use
   from this point forward.

   Alternatively, use `RecordChain.getRecordAt()` path via the `SortedRecordCursor`. Since
   `SortedRecordCursor.recordAt(record, atRowId)` accepts any `RecordChainRecord`, we need
   only one allocated `RecordChainRecord` instance. The cleanest way is:

   ```java
   // During initialize(), after toTop():
   // Call recordAt on a temporary position just to trigger lazy allocation of recordC
   baseCursor.recordAt(baseCursor.getRecordB(), 0L);  // positions recordB to offset 0
   ```
   No — this still uses recordB.

   **Simplest safe approach:** Declare `prevRecord = baseCursor.getRecordB()` in
   `initialize()` after `baseCursor.toTop()` (keyed path) or after the first row in
   non-keyed path. This guarantees `buildChain()` will not run again and `recordB` is
   stable.

### 2.4 "Cached Once Per Emit Row" (PI-02)

The source branch's research noted that `recordAt()` should be called once per emit row, not
once per getter. The fill emit path for a gap row calls multiple `prevRecord.getXxx(col)`
calls. The correct structure:

```java
// In emitNextFillRow() or hasNext(), before returning true for a fill row:
long prevRowId = ...;  // from MapValue or simplePrevRowId
baseCursor.recordAt(prevRecord, prevRowId);
// Now FillRecord.getXxx() can call prevRecord.getXxx(col) freely
```

The `FillRecord` getters simply delegate to `prevRecord.getXxx(col)` without re-calling
`recordAt()`. Since `prevRecord` is a `RecordChainRecord` with `baseOffset` set once,
all getters read from the same chain position.

### 2.5 `toTop()` Reset

Current `toTop()` resets `hasSimplePrev = false` (line 525). After the rewrite:
- Reset `simplePrevRowId = -1L` (non-keyed path).
- `keysMap.clear()` (line 522) already resets all MapValue slots including `PREV_ROWID_SLOT`
  for the keyed path — new `createValue()` calls will initialize slots to 0.
- `prevRecord = null` is NOT needed: `prevRecord` is only read when `hasKeyPrev()` returns
  true (when a rowId has been saved), so a stale pointer is never dereferenced.

### 2.6 FillRecord Getters After rowId Rewrite

All getters in `FillRecord` that currently dispatch through `KIND_*` constants and
`readColumnAsLongBits()` / `prevValue()` collapse to a single uniform pattern:

```java
@Override
public double getDouble(int col) {
    if (!isGapFilling) return baseRecord.getDouble(col);
    int mode = fillMode(col);
    if (mode == FILL_KEY) return keysMapRecord.getDouble(outputColToKeyPos[col]);
    if (mode >= 0 && outputColToKeyPos[mode] >= 0) return keysMapRecord.getDouble(outputColToKeyPos[mode]);
    if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return prevRecord.getDouble(col);
    if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDouble(null);
    return Double.NaN;
}
```

This pattern is identical across ALL types — including STRING, VARCHAR, SYMBOL, BINARY,
ARRAY, LONG128, LONG256, DECIMAL128, DECIMAL256, UUID, INTERVAL. The `prevRecord.getXxx(col)`
call reads directly from chain memory, which stores the original aggregate output row verbatim.

**SYMBOL handling:** `prevRecord.getSymA(col)` calls
`symbolTableResolver.getSymbolTable(col).valueOf(getInt(col))`. The `symbolTableResolver` is
set to `baseCursor` in `RecordChain.setSymbolTableResolver()`. In `SortedRecordCursor`, the
`TreeCursor.of()` calls `recordChain.setSymbolTableResolver(base)` where `base` is the
original GROUP BY cursor. This resolver is set during `getCursor(baseCursor)` and remains
valid for the lifetime of the sorted cursor. The fill cursor never replaces this resolver, so
SYMBOL resolution will work correctly on `prevRecord`.

### 2.7 MapValue Slot Schema After Rewrite

**Current schema (our branch today):**
```
slot 0: KEY_INDEX_SLOT  (LONG)
slot 1: HAS_PREV_SLOT   (LONG)
slot 2..N: per-agg prev values (LONG each, N = aggColumnCount)
```
`PREV_START_SLOT = 2`, `keyPosOffset = 2 + aggSlot`

**After rowId rewrite:**
```
slot 0: KEY_INDEX_SLOT  (LONG)
slot 1: HAS_PREV_SLOT   (LONG)
slot 2: PREV_ROWID_SLOT (LONG)
```
`keyPosOffset = 3 + 0 = 3` (no per-agg slots; key columns start at 3 + 0 = 3)

In `SqlCodeGenerator.generateFill()`, lines 3589-3595 change from:
```java
mapValueTypes.add(ColumnType.LONG); // slot 0: keyIndex
mapValueTypes.add(ColumnType.LONG); // slot 1: hasPrev
for (int i = 0; i < aggColumnCount; i++) {
    mapValueTypes.add(ColumnType.LONG); // per-agg prev slot
}
```
to:
```java
mapValueTypes.add(ColumnType.LONG); // slot 0: keyIndex
mapValueTypes.add(ColumnType.LONG); // slot 1: hasPrev
mapValueTypes.add(ColumnType.LONG); // slot 2: prevRowId
```

The `aggColumnCount` computation loop (lines 3580-3587) is also deleted.

---

## 3. Retro-fallback Deletion Surface (D-04)

The rowId approach unlocks ALL types (any type stored in the chain can be read via typed
getters on `prevRecord`). Retro-fallback machinery must be deleted. Exact files and lines:

### 3.1 File: `FallbackToLegacyException.java`

**Action:** Delete entire file.
**Path:** `core/src/main/java/io/questdb/griffin/FallbackToLegacyException.java`

### 3.2 File: `SqlCodeGenerator.java`

| What to delete | Current location |
|----------------|-----------------|
| `import io.questdb.griffin.FallbackToLegacyException;` | Line 70 |
| `throw FallbackToLegacyException.INSTANCE;` (with surrounding type-check loop) | Lines 3544-3557 |
| First `catch (FallbackToLegacyException fallback)` block (13 lines) | Lines 8043-8056 |
| Second `catch (FallbackToLegacyException fallback)` block (13 lines) | Lines 8259-8272 |
| Third `catch (FallbackToLegacyException fallback)` block (13 lines) | Lines 8307-8320 |
| `prevSourceCols` build loop (lines 3524-3542) | Lines 3524-3542 |
| Codegen-time type check loop (lines 3544-3557) | Lines 3544-3557 |

The `isFastPathPrevSupportedType()` method (lines 1200-1211) is deleted since it has no
remaining callers.

### 3.3 File: `QueryModel.java`

| What to delete | Current location |
|----------------|-----------------|
| `private ExpressionNode stashedSampleByNode;` | Line 185 |
| `stashedSampleByNode = null;` in `clear()` | Line 471 |
| `return stashedSampleByNode;` getter | Line 1066 |
| `setStashedSampleByNode(...)` setter | Lines 1855-1856 |

### 3.4 File: `IQueryModel.java`

| What to delete | Current location |
|----------------|-----------------|
| `ExpressionNode getStashedSampleByNode();` | Line 403 |
| `void setStashedSampleByNode(ExpressionNode ...);` | Line 639 |

### 3.5 File: `QueryModelWrapper.java`

| What to delete | Current location |
|----------------|-----------------|
| `getStashedSampleByNode()` delegating to `delegate` | Line 645-646 |
| `setStashedSampleByNode(...)` throwing `UnsupportedOperationException` | Line 1246 |

### 3.6 File: `SqlOptimiser.java`

| What to delete | Current location |
|----------------|-----------------|
| `nested.setStashedSampleByNode(sampleBy);` stash write | Line 8464 |

### 3.7 Test File: `SampleByFillTest.java`

Remove these two tests (obsolete — the code they pin is deleted):
- `testFillPrevSymbolLegacyFallback` (line 1796) — asserts `Sample By` plan for SYMBOL.
  After rowId unlocks SYMBOL, the plan shows `Sample By Fill`.
- `testFillPrevCrossColumnUnsupportedFallback` (line 1044) — asserts retro-fallback for
  unsupported cross-column PREV type.

Also remove the 5 retro-fallback regression tests from phase 12 (in the same file):
- `testFillPrevDecimal128LegacyFallback`
- `testFillPrevDecimal256LegacyFallback`
- `testFillPrevLong256LegacyFallback`
- `testFillPrevUuidLegacyFallback`
- `testFillPrevLong128LegacyFallback`
(Grep for these names; the exact line numbers depend on the test file's current state.)

### 3.8 Codegen also: `prevSourceCols` parameter in `SampleByFillRecordCursorFactory`

The `prevSourceCols` constructor parameter and the `hasPrevFill` boolean derived from it still
serve a purpose: `hasPrevFill` drives `toPlan()` emit of `fill=prev`. After the rewrite, the
signal for "any column has FILL_PREV_SELF or a cross-column PREV mode" still comes from
scanning `fillModes`. The constructor can keep `hasPrevFill` derived from `fillModes` scan
instead of `prevSourceCols`. Or keep `prevSourceCols` as a non-null/non-empty list for
`hasPrevFill` detection. **Planner's call** — just ensure `hasPrevFill` is still set
correctly so `toPlan()` emits the right `fill=` attribute.

---

## 4. SEED-001 WR-04 + Defect 3 (D-05)

### 4.1 WR-04: Precise Chain-Rejection Position

**Current code (SqlCodeGenerator.java, line 3513):**
```java
throw SqlException.$(fillValuesExprs.getQuick(0).position,
        "FILL(PREV) chains are not supported: source column is itself a cross-column PREV");
```

This always uses `fillValuesExprs.getQuick(0).position` — the position of the FIRST fill
expression — regardless of which column actually forms the chain.

**What the fix requires:**
The chain detection loop (lines 3510-3516) iterates `col` from 0 to `columnCount`. When
`fillModes.getQuick(col) >= 0` AND `fillModes.getQuick(mode) >= 0`, the offending column is
`col`. The fill expression for `col` is at `fillIdx` during the per-column build loop, but
that index is not preserved. Fix: during the per-column fill build loop (lines 3432-3506),
record the `ExpressionNode` for each aggregate column's fill expression in an `ObjList`
parallel to `fillModes`. Then in the chain check, use `perColFillExprNodes.getQuick(col).position`.

**Approximate fix (4 extra lines):**
```java
// Declare alongside fillModes:
ObjList<ExpressionNode> perColFillNodes = new ObjList<>(columnCount);
// ...initialize all to null...
// In the per-column loop, when a PREV expression is assigned:
perColFillNodes.extendAndSet(col, fillExpr);
// In the chain check:
ExpressionNode offendingExpr = perColFillNodes.getQuick(col);
int pos = offendingExpr != null ? offendingExpr.position : fillValuesExprs.getQuick(0).position;
throw SqlException.$(pos, "FILL(PREV) chains are not supported: ...");
```

**Existing tests to update:** `testFillPrevRejectMutualChain` (line 1666, position 55) and
`testFillPrevRejectThreeHopChain` (line 1712, position 65) assert positions derived from the
first fill expression. After WR-04, these positions change to point at the offending column's
`PREV(...)` node. The test expectations must be recomputed.

### 4.2 Defect 3: Insufficient Fill Values Grammar

**Current behavior (our branch):** `generateFill()` silently pads missing fill expressions
with `null` (line 3445-3447: `fillValuesExprs.size() == 1 ? fillValuesExprs.getQuick(0) : null`).
A query with 7 aggregates and `FILL(PREV, PREV, PREV, PREV, 0)` (5 fill specs) runs without
error, producing null fill for the last 2 aggregates.

**Master's behavior:** Throws from `FillRangeRecordCursorFactory.getCursor()` at line 118:
`throw SqlException.$(-1, "not enough fill values")`. Position -1 is at query start (not
useful). Master's test expects position 554.

Wait — reviewing master's `testSampleByFillNeedFix` assertion 3: it calls `assertException`
with position 554 and text "not enough fill values". Position 554 is computed from the query
string at the position of the fill clause that is too short. Our branch should emit a
POSITIONED error — better than master's `-1`.

**What to add to `generateFill()` in our branch:**
After the per-column fill build loop completes, check if the number of consumed fill
expressions (exclusive of key columns) matches `fillValuesExprs.size()`. If more aggregates
than fill specs exist and the spec is not a single-element PREV/NULL (which auto-broadcasts):

```java
// After fillIdx loop, check for undershoot:
if (fillValuesExprs.size() > 1 && fillIdx < aggNonKeyCount && fillIdx < fillValuesExprs.size()) {
    // User provided N fill specs for M > N aggregate columns.
    // Point at the fill clause start (fillValuesExprs.getQuick(0).position).
    throw SqlException.$(fillValuesExprs.getQuick(0).position,
            "not enough fill values: expected ").put(aggNonKeyCount).put(", got ").put(fillIdx);
}
```

The exact form: when `fillValuesExprs.size() > 1` (per-column mode) but `fillValuesExprs.size()`
< number of non-key aggregate columns (excluding timestamp), throw a positioned error.
The single-element case (bare `FILL(PREV)` or `FILL(NULL)`) broadcast-fills all columns —
this is intentional and must NOT trigger the error.

**New test:** `testFillInsufficientFillValues` — query with 7 aggregates, `FILL(PREV, PREV, PREV, PREV, 0)`,
assert exception at the fill clause position.

---

## 5. SEED-002 Defect Resolution (D-06)

### 5.1 Current State of `testSampleByFillNeedFix`

The test lives at `SampleByTest.java:5943`.

**Current form on our branch (6-row buggy form):**

*Assertion 1 (line ~5972):* `assertQuery` with 3 expected rows — but current branch produces
corrupted output due to Defect 2 (`toTop()` state not fully reset). The comment at line 6018
says "The fill cursor emits fill rows per unique key combination... two rows per bucket for
the gap hour." This was the workaround accepted in Option A.

*Assertion 2 (line ~6022):* `assertSql` (not `assertQuery`) with 6 expected rows — the CTE
+ outer projection form.

*Assertion 3 (line ~6057):* `assertSql` accepting the insufficient-fill query without error
(workaround for Defect 3).

**Master's form:**
- Assertion 1: `assertQuery` with 3 rows (including `cnt=0` for the gap hour — correct PREV behavior).
  `high` column for gap row uses `case when cnt=0 then close else high end` = `0.00128`.
- Assertion 2: `assertQuery` (not `assertSql`) with 3 rows, `cnt=0` for gap.
- Assertion 3: `assertException` at position 554, "not enough fill values".

### 5.2 Defect 2: `toTop()` State Corruption

**Diagnosis:** The assertion uses `assertQuery(expected, query, timestamp, sizeExpected=true)`.
When `sizeExpected=true`, the framework iterates the cursor twice: once for results, once to
verify `size()`. The second iteration calls `toTop()` then re-iterates. After the rowId
rewrite, `toTop()` no longer resets `simplePrev[]` (which is removed). Instead it resets
`simplePrevRowId = -1L`. The keyed path resets via `keysMap.clear()` (already in toTop()).
This defect is absorbed by the rewrite.

**Verification:** After rowId rewrite, restore assertion 1 to master's 3-row form (including
`cnt=0` for the gap) and use `assertQuery(..., "candle_start_time###DESC", true)`. If it
passes, Defect 2 is confirmed absorbed.

### 5.3 Defect 1: CTE-wrap + Outer Projection Corruption

**Shape that breaks:**
```sql
WITH sq AS (
  SELECT ts, symbol, first(price) AS open, sum(qty) AS cnt
  FROM t SAMPLE BY 1h FILL(PREV, 0) ORDER BY ts DESC
)
SELECT ts, cnt FROM sq   -- outer projects FEWER columns than CTE
```

**Current wrong output:** wrong timestamps (`1970-01-01T00:00:00`), duplicated rows per bucket.
Correct: `SELECT *` from the CTE works; only reducing projection triggers it.

**Absorption hypothesis:** The outer `SELECT ts, cnt FROM sq` triggers a
`VirtualRecordCursorFactory` that reads columns by index from the fill cursor's record. The
fill cursor's `FillRecord` dispatches via `fillModes[col]` and `isGapFilling`. The OUTER
projection may reindex columns (col 0 = ts, col 1 = cnt, losing the intermediate columns).
When the outer projection calls `getTimestamp(0)` it gets `ts` which maps to `fillTimestampFunc`
correctly. But `getInt(1)` for `cnt` at the projected index 1 may not match the fill cursor's
internal column index.

**More likely cause:** `generateFill()` at line 3361-3364 returns `groupByFactory` unchanged
when `timestampIndex < 0` in the outer metadata. The outer CTE projection may expose a
different metadata to `generateFill()` which then skips fill entirely, returning raw GROUP BY
output — but the outer `ORDER BY ts DESC` then re-sorts. The fill cursor never runs.

**Reproduction:** The test at SampleByTest.java:6022-6052 (assertion 2 in its current 6-row
form) is the reproducer. It uses `assertSql` which only iterates once and does not use
`assertQuery`-style double-iteration.

**Investigation task for the executor:** Run the query from assertion 2 with
`assertQuery(..., sizeExpected=true)` and observe whether the bug is in the cursor iteration
or in whether fill runs at all (check plan output). The `toPlan()` output distinguishes the
two: `Sample By Fill` = fill cursor ran, `Sample By` = fell through to legacy.

**Whether rowId absorbs it:** Unclear. The corruption (wrong timestamps = 1970-01-01) suggests
the timestamp column index is wrong in the outer projection, which is above the cursor level.
This likely requires a fix in `generateFill()`'s metadata handling or in the outer projection
plumbing — independent of how prev values are stored.

**The planner must allocate a task:** Reproduce Defect 1 post-rowId-rewrite. Check plan. If
fill cursor is not invoked for the CTE+projection shape, investigate why `generateFill()` bails
early (`timestampIndex < 0` guard at line 3361). If fill cursor runs but produces wrong output,
investigate the FillRecord column index mapping with the outer projection. Fix at the layer where
the defect lives. This task has `autonomous: false` if it requires architectural decision about
how to thread timestamp metadata through the outer projection.

---

## 6. Test Cherry-pick List (D-07 Commit 3)

### 6.1 Source

Branch: `sm_fill_prev_fast_all_types`, commit `f43a3d7057`
File: `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`

### 6.2 All 13 Tests Confirmed at `f43a3d7057`

```
testFillPrevSymbolKeyed
testFillPrevSymbolNonKeyed
testFillPrevSymbolNull
testFillPrevArrayDouble1D
testFillPrevStringKeyed
testFillPrevStringNonKeyed
testFillPrevVarcharKeyed
testFillPrevVarcharNonKeyed
testFillPrevUuidKeyed
testFillPrevUuidNonKeyed
testFillPrevLong256NonKeyed
testFillPrevDecimal128
testFillPrevDecimal256
```

[VERIFIED: `git show f43a3d7057:...SampleByFillTest.java | grep "public void testFillPrev"`]

### 6.3 Status on Current Branch

None of the 13 tests exist on `sm_fill_prev_fast_path` (our branch). The only related methods
are `testFillPrevSymbolLegacyFallback` (line 1796) and `testFillPrevCrossColumnUnsupportedFallback`
(line 1044), which are scheduled for deletion in Commit 4.

### 6.4 Tests Requiring Adaptation for rowId

**SYMBOL tests (`testFillPrevSymbolKeyed`, `testFillPrevSymbolNonKeyed`, `testFillPrevSymbolNull`):**
On the source branch these use value-buffering which reads SYMBOL via `baseRecord.getSymA()`.
After rowId rewrite, `prevRecord.getSymA(col)` reads via `symbolTableResolver` which is set
to the base GROUP BY cursor. This should produce the same result. No semantic adaptation needed,
but **plan output must say `Sample By Fill` not `Sample By`** — verify this assertion is in
the test or add it.

**STRING/VARCHAR/BINARY/ARRAY tests:** Same reasoning. The source branch's tests may currently
use `assertSql` (no plan check). After rowId rewrite, retro-fallback is deleted, so these
types must be fast-path. The test should verify fast-path plan OR we add a plan assertion.

**All 13 tests were written against value-buffering semantics** on the source branch. The
expected output values (the `assertSql` expected rows) are DATA values, not implementation
details — they should be identical after rowId rewrite since rowId reads the same aggregate
values from chain memory. No value adaptation expected.

**Fetch command for cherry-pick:**
```bash
git show f43a3d7057:core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java \
  | grep -A 50 "testFillPrevSymbolKeyed\|testFillPrevArrayDouble1D..."
```
Or more practically, use `git diff` between a known-good baseline and `f43a3d7057` to isolate
the 13 methods.

---

## 7. Risks and Open Questions for the Planner

### 7.1 Defect 1 (CTE-wrap + outer projection) — Autonomous: false

The absorption probability for Defect 1 is UNCLEAR (see Section 5.3). The executor should:
1. Post-rowId-rewrite, run assertion 2 of `testSampleByFillNeedFix` with `assertQuery` and
   check the plan output.
2. If the plan shows `Sample By` (not `Sample By Fill`), the bug is in the fill skip guard
   (`timestampIndex < 0` at line 3361), not in the cursor. This requires a factory-plumbing
   fix in `generateFill()`.
3. If the plan shows `Sample By Fill` but output is still wrong, the bug is in
   `FillRecord`'s column index mapping under outer-projection remap.

If the root cause requires touching `generateFill()`'s timestamp-index lookup logic OR
`SqlCodeGenerator`'s outer-model handling, that may be a 1-2 day investigation. Surface to
user if the fix is non-trivial.

### 7.2 `prevRecord` Initialization Timing — Low risk, needs care

The executor must initialize `prevRecord = baseCursor.getRecordB()` AFTER `buildChain()`
completes (i.e., after `baseCursor.toTop()` in `initialize()`). See Section 2.3.
The risk is initializing `prevRecord` in `of()` before `initialize()` runs — at that point
`buildChain()` has not run and `recordB` may be repositioned later. **The executor must
verify** that `prevRecord` is set in `initialize()` after `toTop()`, not in `of()`.

### 7.3 SYMBOL in keyed queries — rowId unlocks it, but symbol table resolver scope

SYMBOL columns in keyed queries: `keysMapRecord.getSymA(col)` resolves via
`keysMapRecord.symbolTableResolver = baseCursor` (set in `initialize()`). For the PREV
path with rowId, `prevRecord.getSymA(col)` also resolves via the `RecordChain`'s
`symbolTableResolver` which was set by `RecordChain.setSymbolTableResolver(base)` in
`RecordTreeChain.TreeCursor.of()`. `base` here is the GROUP BY cursor, same as `baseCursor`.
SYMBOL IDs in chain memory are the integer codes from the GROUP BY output. These codes are
stable for the lifetime of the cursor. No issue expected.

### 7.4 ARRAY type in chain — confirm var-width read

`testFillPrevArrayDouble1D` tests a `DOUBLE[]` aggregate. `RecordChainRecord.getArray(col, type)`
(line 419-424) reads `ArrayTypeDriver.getPlainValue(addr, array(col))` from chain memory.
The chain stores arrays via `putArray()` which calls `ArrayTypeDriver.appendPlainValue()`.
This is a var-width column type; its storage in `RecordChain` is via a pointer offset in the
fixed section pointing to the var-width section. This is already supported by the existing
`RecordChain` infrastructure — no special handling needed in the rowId path.

### 7.5 Performance: `chain.clear()` on every `of()` reuse

`RecordChain.clear()` calls `mem.close()` which frees native memory pages. The subsequent
`buildChain()` re-allocates. For test suites that reuse the same compiled factory across many
invocations (common pattern), each `getCursor()` call will pay a malloc/free cycle for the
chain. This is expected and acceptable (noted in research commit `4ebfa3243c`). Not a blocker.

### 7.6 `prevSourceCols` parameter after retro-fallback deletion

`generateFill()` currently builds `prevSourceCols` and passes it to
`SampleByFillRecordCursorFactory`. After retro-fallback deletion and the rowId rewrite,
`prevSourceCols` is no longer used for snapshotting individual column values. The planner
must decide whether to keep the parameter (pass null or empty list, keep `hasPrevFill`
derived from `fillModes` scan) or refactor the constructor to remove it. Claude's discretion.

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | JUnit 4 (Maven Surefire) |
| Config | `pom.xml` in `core/` module |
| Quick run (fill suite) | `mvn -pl core -Dtest=SampleByFillTest test` |
| Full phase suite | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,RecordCursorMemoryUsageTest' test` |

### Phase Requirements -> Test Map

| Requirement | Behavior | Test Type | Command |
|-------------|----------|-----------|---------|
| chain.clear() fix | No chain accumulation on cursor reuse | Integration | Combined-suite run (see 8.4) |
| rowId keyed | `prevRecord.getDouble(col)` returns previous bucket value for keyed FILL(PREV) | Integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevDoubleKeyed test` |
| rowId non-keyed | `prevRecord` replay returns correct prev for non-keyed | Integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevNonKeyed test` |
| All 13 type tests | Fast path (Sample By Fill plan) + correct values | Integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevSymbolKeyed+...` |
| toTop() reset (Defect 2) | Cursor correct on second iteration after toTop() | Integration | `mvn -pl core -Dtest=SampleByTest#testSampleByFillNeedFix test` |
| Defect 1 absorption | CTE + outer projection produces 3-row correct output | Integration | `mvn -pl core -Dtest=SampleByTest#testSampleByFillNeedFix test` |
| Retro-fallback deleted | SYMBOL/STRING/VARCHAR/etc. plan shows "Sample By Fill" | Integration | All 13 type tests + `assertQueryNoLeakCheck` plan assertion |
| WR-04 | Chain-rejection error points at offending PREV(...) | Unit | `testFillPrevRejectMutualChain`, `testFillPrevRejectThreeHopChain` |
| Defect 3 | `FILL(PREV,PREV,PREV,PREV,0)` for 7 aggregates raises positioned SqlException | Unit | `testFillInsufficientFillValues` (new) |

### Per-Commit Validation

| Commit | Validation command |
|--------|-------------------|
| Commit 1 (chain.clear()) | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest' test` — both must pass |
| Commit 2 (rowId rewrite) | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest' test` |
| Commit 3 (13 type tests) | `mvn -pl core -Dtest=SampleByFillTest test` — all 13 new tests must show `Sample By Fill` plan |
| Commit 4 (retro-fallback cleanup) | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,ExplainPlanTest,RecordCursorMemoryUsageTest' test` |
| Commit 5 (SEED-002 fix) | `mvn -pl core -Dtest=SampleByTest#testSampleByFillNeedFix test` |
| Commit 6 (grammar items) | `mvn -pl core -Dtest=SampleByFillTest test` |

### Combined-Suite Ordering (failure-reproducing for chain.clear() verification)

The source branch's research showed the failure reproduced with:
```
mvn -pl core -Dtest='SampleByFillTest,SampleByTest' test
```
Running `SampleByFillTest` first then `SampleByTest` contaminated the chain state. After the
`chain.clear()` fix, this ordering must produce green results.

The inverse ordering also matters:
```
mvn -pl core -Dtest='SampleByTest,SampleByFillTest' test
```

Both orderings must pass as success criterion 12 of CONTEXT.md.

### Phase Gate

```
mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,RecordCursorMemoryUsageTest' test
```
Must exit 0 before `/gsd-verify-work`.

### Wave 0 Gaps

- [ ] `testFillInsufficientFillValues` — new test, not yet written (covers Defect 3)
- [ ] Plan-output assertions in the 13 cherry-picked tests (if not present in source) — verify
  `Sample By Fill` appears in plan for each type
- [ ] `testSampleByFillNeedFix` restoration — 3 assertions matching master form

---

## Project Constraints (from CLAUDE.md)

- **Zero-GC:** `baseCursor.recordAt(prevRecord, rowId)` is a non-allocating reposition.
  `RecordChain.recordAt()` only updates `baseOffset` on an existing `RecordChainRecord`.
  The single `prevRecord` field is allocated once per cursor lifecycle.
- **ObjList instead of T[]:** The removed `long[] simplePrev` is replaced by a single
  `long simplePrevRowId`. No new arrays introduced. Compliant.
- **Boolean naming:** `hasSimplePrev` can be renamed `hasPrevRowId` or replaced with
  `simplePrevRowId != -1L`. Either approach compliant.
- **Log messages ASCII only:** No new log messages expected in this phase.
- **No section-heading banner comments:** Do not add `// ===` separators.
- **Alphabetical member ordering:** After adding `prevRecord` and `simplePrevRowId` fields,
  insert in alphabetical order among instance fields of `SampleByFillCursor`.
- **Test conventions:** Use `assertMemoryLeak()`, `assertQueryNoLeakCheck()` for query
  correctness tests; `assertSql()` for storage-level tests.

---

## Sources

### Primary (HIGH confidence)
- `SortedRecordCursor.java` (lines 38-140) — `of()`, chain lifecycle, `recordAt()`, `toTop()`
  [VERIFIED: source read]
- `RecordTreeChain.java` (lines 43-399) — `clear()`, `reopen()`, `TreeCursor` internals,
  `recordChainRecord` field [VERIFIED: source read]
- `RecordChain.java` (lines 56-720) — `clear()`, `recordAt()`, `getRecordAt()`, `recordC`
  lazy slot, `RecordChainRecord.getRowId()` [VERIFIED: source read]
- `SampleByFillRecordCursorFactory.java` (full file) — current field inventory, slot
  constants, `toTop()`, `of()`, `initialize()`, `FillRecord` getters [VERIFIED: source read]
- `SqlCodeGenerator.java` (lines 3271-3677, 8043-8320) — `generateFill()`, mapValueTypes
  schema, catch sites [VERIFIED: source read]
- `FallbackToLegacyException.java` — singleton pattern, `fillInStackTrace()` override
  [VERIFIED: source read]
- `QueryModel.java` (lines 185, 471, 1066, 1855-1856) — `stashedSampleByNode` field
  [VERIFIED: grep]
- `IQueryModel.java` (lines 403, 639) — interface declarations [VERIFIED: grep]
- `SqlOptimiser.java` (line 8464) — stash write [VERIFIED: grep]
- Git commit `fe487c06a9` — revert commit, failure mode [VERIFIED: git show]
- Git commit `a437758268` — rowId PoC, slot schema [VERIFIED: git show]
- Git commit `1a40aa89af` — keysMap-driven PoC with `recordB` coupling [VERIFIED: git show]
- Git commit `4ebfa3243c` — GO verdict research, chain accumulation confirmed [VERIFIED: git show]
- `SampleByTest.java:5943` — `testSampleByFillNeedFix` current 6-row form [VERIFIED: source read]
- Master's `testSampleByFillNeedFix` — 3-row form + assertException [VERIFIED: git show master]
- `f43a3d7057:SampleByFillTest.java` — 13 test method names confirmed [VERIFIED: git show]

---

## Metadata

**Confidence breakdown:**
- D-02 verdict (candidate a): HIGH — verified from chain memory model; `chain.clear()` is sufficient
- rowId rewrite shape: HIGH — exact slot schema, prevRecord initialization timing identified
- Retro-fallback deletion surface: HIGH — exact file+line citations from source
- SEED-001 WR-04: HIGH — code at line 3513 confirmed; fix approach described
- SEED-001 Defect 3: HIGH — master's behavior confirmed; fix approach described
- SEED-002 Defect 2 absorption: HIGH — toTop() reset analysis confirms absorption
- SEED-002 Defect 1: MEDIUM — root cause is in factory plumbing, not cursor; investigation
  required post-rowId-rewrite
- 13 test cherry-pick: HIGH — all names confirmed at f43a3d7057; none exist on our branch

**Research date:** 2026-04-18
**Valid until:** Stable — these are core data structures. Valid until SortedRecordCursor or
RecordChain API changes.
