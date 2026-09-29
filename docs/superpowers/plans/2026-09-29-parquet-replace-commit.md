# Parquet Replace Commits Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let replace-range WAL commits (`WAL_DEDUP_MODE_REPLACE_RANGE`) apply to Parquet partitions instead of suspending the table.

**Architecture:** The Parquet O3 merge strategy learns the replace window: row groups that intersect it become filter-only `MERGE`s, and row groups it fully covers with no new rows become a new `DROP` action. `mergeRowGroup` builds a three-segment merge index (existing rows before the range, new rows, existing rows after the range). `processParquetPartition` updates in place unless a `DROP` occurs, in which case it takes the existing crash-safe rewrite path. No-op and full-removal cases return early without writing.

**Tech Stack:** Java 25 (core enforces `java.enforce.version=25`; local JDK GraalVM 25.0.1), QuestDB core (zero-GC), JUnit 4, `AbstractCairoTest`, Maven.

Spec: `docs/superpowers/specs/2026-09-29-parquet-replace-commit-design.md`

## Global Constraints

- Worktree `~/claude/wt/oss/parquet-replace-commit`, branch `feat/parquet-replace-commit`, based on OSS `origin/master` f3788a1b64 (latest as of 2026-09-29). Never push.
- JDK 25. Modern language features are fine (enhanced `switch`, text blocks, `instanceof` patterns, records where the codebase already uses them); keep zero-GC on data paths.
- Before starting, and again before Task 5, run `git -C ~/claude/hub/questdb fetch origin master` and merge `origin/master` if it moved (merge, don't rebase). ENT `origin/main` pins OSS 92926cb70, an ancestor of master with no changes to the files this plan touches; nothing in ENT depends on the Parquet replace restriction.
- Run Maven sequentially, never two `mvn` invocations at once. Test command form: `mvn -pl core -Dtest=ClassName#method test -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false` from the worktree root.
- Log messages: ASCII only.
- Boolean names use `is...`/`has...`.
- Tests: wrap in `assertMemoryLeak(...)` (except pure-Java unit tests with no native allocation); assert query output with `assertQuery(sql).returns(expected)`; UPPERCASE SQL keywords; `_` thousands separators in numbers of 5+ digits; multiline strings for multi-row SQL/expected output.
- Commit titles: plain English, ≤50 chars, no Conventional-Commit prefix, long-form body wrapped at 72, ending with
  `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- Replace mode and dedup mode are mutually exclusive (`TableWriter.dedupMode` is one byte). Never combine replace filtering with dedup merging.
- Sentinel for "no replace range": `replaceLo = Long.MAX_VALUE`, `replaceHi = Long.MIN_VALUE` (so `replaceLo > replaceHi`). Ranges are inclusive at both ends.

---

## File map

| File | Change |
|---|---|
| `core/src/main/java/io/questdb/cairo/O3ParquetMergeStrategy.java` | replace-range parameters, `DROP` action, `setDrop`, `setFilter` |
| `core/src/main/java/io/questdb/cairo/O3ParquetMergeContext.java` | `mergeFirstTimestamp` scratch field |
| `core/src/main/java/io/questdb/cairo/O3PartitionJob.java` | `createReplaceMergeIndex`, `mergeRowGroup` replace branch, `processParquetPartition` restructure, dispatch arguments |
| `core/src/main/java/io/questdb/cairo/TableWriter.java` | remove the Parquet replace throw |
| `core/src/main/java/io/questdb/cairo/CLAUDE.md` | document `DROP` and replace mode |
| `core/src/test/java/io/questdb/test/cairo/parquet/O3ParquetMergeStrategyTest.java` | strategy unit tests |
| `core/src/test/java/io/questdb/test/cairo/parquet/O3ReplaceMergeIndexTest.java` | new: merge-index unit tests |
| `core/src/test/java/io/questdb/test/cairo/parquet/ParquetReplaceCommitTest.java` | new: end-to-end differential tests |
| `core/src/test/java/io/questdb/test/cairo/wal/WalWriterReplaceRangeTest.java` | flip `testReplaceRangeNotSupportedParquetPartition` |
| `core/src/test/java/io/questdb/test/cairo/fuzz/ReplaceInsertFuzzTest.java` | Parquet fuzz variants |

---

### Task 1: Replace-aware merge strategy

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/O3ParquetMergeStrategy.java`
- Test: `core/src/test/java/io/questdb/test/cairo/parquet/O3ParquetMergeStrategyTest.java`

**Interfaces:**
- Produces:
  - `O3ParquetMergeStrategy.ActionType.DROP`
  - `MergeAction.setDrop(int rowGroupIndex)`: sets `type=DROP`, `rowGroupIndex=rowGroupIndexHi=rowGroupIndex`, `rgLo=rgHi=o3Lo=o3Hi=-1`
  - `MergeAction.setFilter(int rowGroupIndex, long rgRowCount)`: a `MERGE` with an empty O3 slice, `o3Lo=0, o3Hi=-1`, so `o3Hi - o3Lo + 1 == 0` and `getO3RowCount() == 0`
  - New overload `computeMergeActions(LongList rowGroupBounds, long sortedTimestampsAddr, long srcOooLo, long srcOooHi, int smallRowGroupThreshold, int maxRowGroupSize, ObjList<MergeAction> actionsBuf, LongList rgO3Ranges, LongList gapO3Ranges, boolean coalesceBoundaryTies, long replaceLo, long replaceHi)`. The existing 10-arg overload delegates with `Long.MAX_VALUE, Long.MIN_VALUE`.

- [ ] **Step 1: Write the failing tests** (append to `O3ParquetMergeStrategyTest`, before the private helpers)

```java
    @Test
    public void testReplaceCoveredRowGroupWithO3IsMerge() throws Exception {
        assertMemoryLeak(() -> {
            LongList rowGroupBounds = new LongList();
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 100, 200, 4);
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 300, 400, 4);
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 500, 600, 4);
            ObjList<MergeAction> actionsBuf = new ObjList<>();
            long addr = allocateSortedTimestamps(320);
            try {
                int n = O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds, addr, 0, 0, 1, Integer.MAX_VALUE,
                        actionsBuf, new LongList(), new LongList(), false, 250, 450
                );
                Assert.assertEquals(3, n);
                Assert.assertEquals("COPY_ROW_GROUP_SLICE(rg=0[0,3])", actionsBuf.get(0).toString());
                Assert.assertEquals("MERGE(rg=1[0,3], o3=[0,0])", actionsBuf.get(1).toString());
                Assert.assertEquals("COPY_ROW_GROUP_SLICE(rg=2[0,3])", actionsBuf.get(2).toString());
            } finally {
                freeSortedTimestamps(addr, 1);
            }
        });
    }

    @Test
    public void testReplaceDropsFullyCoveredRowGroup() {
        LongList rowGroupBounds = new LongList();
        O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 100, 200, 4);
        O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 300, 400, 4);
        O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 500, 600, 4);
        ObjList<MergeAction> actionsBuf = new ObjList<>();
        // empty O3 batch: srcOooLo > srcOooHi, the address is never read
        int n = O3ParquetMergeStrategy.computeMergeActions(
                rowGroupBounds, 0, 0, -1, 1, Integer.MAX_VALUE,
                actionsBuf, new LongList(), new LongList(), false, 250, 450
        );
        Assert.assertEquals(3, n);
        Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actionsBuf.get(0).type);
        Assert.assertEquals(ActionType.DROP, actionsBuf.get(1).type);
        Assert.assertEquals(1, actionsBuf.get(1).rowGroupIndex);
        Assert.assertEquals(0, actionsBuf.get(1).getTotalRowCount());
        Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actionsBuf.get(2).type);
    }

    @Test
    public void testReplaceFiltersPartiallyCoveredRowGroups() {
        LongList rowGroupBounds = new LongList();
        O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 100, 200, 4);
        O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 300, 400, 4);
        O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 500, 600, 4);
        ObjList<MergeAction> actionsBuf = new ObjList<>();
        int n = O3ParquetMergeStrategy.computeMergeActions(
                rowGroupBounds, 0, 0, -1, 1, Integer.MAX_VALUE,
                actionsBuf, new LongList(), new LongList(), false, 150, 350
        );
        Assert.assertEquals(3, n);
        Assert.assertEquals("MERGE(rg=0[0,3], o3=[0,-1])", actionsBuf.get(0).toString());
        Assert.assertEquals(0, actionsBuf.get(0).getO3RowCount());
        Assert.assertEquals("MERGE(rg=1[0,3], o3=[0,-1])", actionsBuf.get(1).toString());
        Assert.assertEquals("COPY_ROW_GROUP_SLICE(rg=2[0,3])", actionsBuf.get(2).toString());
    }

    @Test
    public void testReplaceO3InGapBetweenDroppedRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            LongList rowGroupBounds = new LongList();
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 100, 200, 4);
            O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 300, 400, 4);
            ObjList<MergeAction> actionsBuf = new ObjList<>();
            long addr = allocateSortedTimestamps(250);
            try {
                // threshold 1: neither row group is "small", so the gap row stays COPY_O3
                int n = O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds, addr, 0, 0, 1, Integer.MAX_VALUE,
                        actionsBuf, new LongList(), new LongList(), false, 100, 400
                );
                Assert.assertEquals(3, n);
                Assert.assertEquals("DROP(rg=0)", actionsBuf.get(0).toString());
                Assert.assertEquals("COPY_O3(o3=[0,0])", actionsBuf.get(1).toString());
                Assert.assertEquals("DROP(rg=1)", actionsBuf.get(2).toString());
            } finally {
                freeSortedTimestamps(addr, 1);
            }
        });
    }

    @Test
    public void testReplaceRangeMissingAllRowGroupsCopiesEverything() {
        LongList rowGroupBounds = new LongList();
        O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 100, 200, 4);
        O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, 300, 400, 4);
        ObjList<MergeAction> actionsBuf = new ObjList<>();
        int n = O3ParquetMergeStrategy.computeMergeActions(
                rowGroupBounds, 0, 0, -1, 1, Integer.MAX_VALUE,
                actionsBuf, new LongList(), new LongList(), false, 201, 299
        );
        Assert.assertEquals(2, n);
        Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actionsBuf.get(0).type);
        Assert.assertEquals(ActionType.COPY_ROW_GROUP_SLICE, actionsBuf.get(1).type);
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `mvn -pl core -Dtest=O3ParquetMergeStrategyTest test -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAILURE (`computeMergeActions` has no 12-arg overload; `ActionType.DROP` missing).

- [ ] **Step 3: Implement**

In `ActionType`, after `COPY_O3` add:

```java
        /**
         * Discard an existing row group whose rows all fall inside a replace-commit
         * range that brings no rows for it. rowGroupIndex is valid; rgLo, rgHi,
         * o3Lo, o3Hi are -1. Update mode cannot remove a row group, so a DROP
         * forces the partition rewrite.
         */
        DROP
```

In `MergeAction`, add after `setCopyRowGroupSlice`:

```java
        /**
         * Set this action to discard a row group fully covered by a replace range.
         */
        public void setDrop(int rowGroupIndex) {
            this.type = ActionType.DROP;
            this.rowGroupIndex = rowGroupIndex;
            this.rowGroupIndexHi = rowGroupIndex;
            this.rgLo = -1;
            this.rgHi = -1;
            this.o3Lo = -1;
            this.o3Hi = -1;
        }

        /**
         * Set this action to rewrite a row group partially covered by a replace range
         * that brings no rows for it: a MERGE with an empty O3 slice (o3Hi < o3Lo),
         * which drops the covered rows and keeps the rest.
         */
        public void setFilter(int rowGroupIndex, long rgRowCount) {
            setMerge(rowGroupIndex, 0, rgRowCount - 1, 0, -1);
        }
```

Fix `getO3RowCount()` so the empty slice counts as zero rows:

```java
        public long getO3RowCount() {
            return o3Hi >= o3Lo && o3Hi >= 0 ? o3Hi - o3Lo + 1 : 0;
        }
```

Add a `DROP` arm to `toString()`:

```java
                case DROP -> "DROP(rg=" + rowGroupIndex + ")";
```

Rename the current 10-arg `computeMergeActions(..., boolean coalesceBoundaryTies)` body into the new 12-arg overload with `long replaceLo, long replaceHi` appended. Keep the 10-arg overload as a delegate:

```java
    public static int computeMergeActions(
            LongList rowGroupBounds,
            long sortedTimestampsAddr,
            long srcOooLo,
            long srcOooHi,
            int smallRowGroupThreshold,
            int maxRowGroupSize,
            ObjList<MergeAction> actionsBuf,
            LongList rgO3Ranges,
            LongList gapO3Ranges,
            boolean coalesceBoundaryTies
    ) {
        return computeMergeActions(
                rowGroupBounds, sortedTimestampsAddr, srcOooLo, srcOooHi, smallRowGroupThreshold,
                maxRowGroupSize, actionsBuf, rgO3Ranges, gapO3Ranges, coalesceBoundaryTies,
                Long.MAX_VALUE, Long.MIN_VALUE
        );
    }
```

Add the Javadoc `@param` lines for the new overload:

```java
     * @param replaceLo              Inclusive start of the replace-commit range, or Long.MAX_VALUE
     *                               when the commit is not a replace commit.
     * @param replaceHi              Inclusive end of the replace-commit range, or Long.MIN_VALUE
     *                               when the commit is not a replace commit. Every O3 row must
     *                               lie inside [replaceLo, replaceHi].
```

At the top of the 12-arg body add:

```java
        final boolean isReplace = replaceLo <= replaceHi;
        // Replace commits never dedup, and coalescing exists only for dedup.
        assert !(isReplace && coalesceBoundaryTies);
```

In the "Single row group" emission at the end of the loop, replace:

```java
            if (getRangeLo(rgO3Ranges, rg) >= 0) {
                nextAction(actionsBuf, actionCount++).setMerge(rg, 0, rgRowCount - 1, getRangeLo(rgO3Ranges, rg), getRangeHi(rgO3Ranges, rg));
            } else {
                nextAction(actionsBuf, actionCount++).setCopyRowGroupSlice(rg, 0, rgRowCount - 1);
            }
```

with:

```java
            if (getRangeLo(rgO3Ranges, rg) >= 0) {
                nextAction(actionsBuf, actionCount++).setMerge(rg, 0, rgRowCount - 1, getRangeLo(rgO3Ranges, rg), getRangeHi(rgO3Ranges, rg));
            } else if (isReplace
                    && getRowGroupMin(rowGroupBounds, rg) <= replaceHi
                    && getRowGroupMax(rowGroupBounds, rg) >= replaceLo) {
                // The replace range intersects this row group but brings no rows for it.
                if (getRowGroupMin(rowGroupBounds, rg) >= replaceLo && getRowGroupMax(rowGroupBounds, rg) <= replaceHi) {
                    nextAction(actionsBuf, actionCount++).setDrop(rg);
                } else {
                    // min or max row lies outside the range, so at least one row survives
                    nextAction(actionsBuf, actionCount++).setFilter(rg, rgRowCount);
                }
            } else {
                nextAction(actionsBuf, actionCount++).setCopyRowGroupSlice(rg, 0, rgRowCount - 1);
            }
```

- [ ] **Step 4: Run tests**

Run: `mvn -pl core -Dtest='O3ParquetMergeStrategyTest,O3ParquetMergeStrategyFuzzTest' test -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
Expected: all PASS. If `O3ParquetMergeStrategyFuzzTest` has an exhaustive `switch` over `ActionType` that fails to compile, add a `case DROP -> throw new AssertionError("DROP without replace range");` arm. Without a replace range the strategy never emits `DROP`.

- [ ] **Step 5: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/O3ParquetMergeStrategy.java core/src/test/java/io/questdb/test/cairo/parquet/O3ParquetMergeStrategyTest.java core/src/test/java/io/questdb/test/cairo/parquet/O3ParquetMergeStrategyFuzzTest.java
git commit -m "Teach parquet merge strategy replace ranges

computeMergeActions takes an optional inclusive replace range. A row
group the range intersects without bringing rows for it becomes a
filter-only MERGE (empty O3 slice), or a DROP when the range covers it
fully. Callers without a range keep the old behaviour via the existing
overload.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Replace merge index

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/O3PartitionJob.java` (next to `createMergeIndex`, ~line 1806)
- Create: `core/src/test/java/io/questdb/test/cairo/parquet/O3ReplaceMergeIndexTest.java`

**Interfaces:**
- Produces: `public static long O3PartitionJob.createReplaceMergeIndex(long srcTimestampAddr, long srcRowCount, long sortedTimestampsAddr, long o3Lo, long o3Hi, long replaceLo, long replaceHi, long destAddr)`. It writes 16-byte `(ts, i)` entries to `destAddr` and returns the entry count. An existing row's `i` is `(1L << 63) | rowIndex`; a new row's entry is copied verbatim from `sortedTimestampsAddr`. `destAddr` must have room for `(srcRowCount + max(0, o3Hi - o3Lo + 1)) * 16` bytes.

- [ ] **Step 1: Write the failing test**

```java
/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo.parquet;

import io.questdb.cairo.O3PartitionJob;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class O3ReplaceMergeIndexTest extends AbstractCairoTest {
    private static final long DATA_BIT = 1L << 63;

    @Test
    public void testEmptyO3DropsRangeFromMiddle() throws Exception {
        assertIndex(
                new long[]{10, 20, 30, 40, 50},
                new long[]{},
                20, 40,
                "10:d0,50:d4"
        );
    }

    @Test
    public void testO3OnlyWhenRangeCoversAllRows() throws Exception {
        assertIndex(
                new long[]{20, 30},
                new long[]{25},
                10, 40,
                "25:o0"
        );
    }

    @Test
    public void testPrefixO3Suffix() throws Exception {
        assertIndex(
                new long[]{10, 20, 30, 40, 50},
                new long[]{25, 35},
                20, 40,
                "10:d0,25:o0,35:o1,50:d4"
        );
    }

    @Test
    public void testRangeAtHeadKeepsSuffix() throws Exception {
        assertIndex(
                new long[]{10, 10, 20, 30},
                new long[]{5},
                5, 10,
                "5:o0,20:d2,30:d3"
        );
    }

    @Test
    public void testRangeAtTailKeepsDuplicatePrefix() throws Exception {
        assertIndex(
                new long[]{10, 20, 20, 30},
                new long[]{35},
                21, 40,
                "10:d0,20:d1,20:d2,35:o0"
        );
    }

    private static void assertIndex(long[] data, long[] o3, long replaceLo, long replaceHi, String expected) throws Exception {
        assertMemoryLeak(() -> {
            final long dataSize = Math.max(1, data.length) * 8L;
            final long o3Size = Math.max(1, o3.length) * 16L;
            final long destSize = (data.length + o3.length) * 16L + 16;
            final long dataAddr = Unsafe.malloc(dataSize, MemoryTag.NATIVE_O3);
            final long o3Addr = Unsafe.malloc(o3Size, MemoryTag.NATIVE_O3);
            final long destAddr = Unsafe.malloc(destSize, MemoryTag.NATIVE_O3);
            try {
                for (int i = 0; i < data.length; i++) {
                    Unsafe.putLong(dataAddr + i * 8L, data[i]);
                }
                for (int i = 0; i < o3.length; i++) {
                    Unsafe.putLong(o3Addr + i * 16L, o3[i]);
                    Unsafe.putLong(o3Addr + i * 16L + 8, i);
                }
                final long n = O3PartitionJob.createReplaceMergeIndex(
                        dataAddr, data.length, o3Addr, 0, o3.length - 1, replaceLo, replaceHi, destAddr
                );
                StringBuilder sb = new StringBuilder();
                for (long i = 0; i < n; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    long ts = Unsafe.getLong(destAddr + i * 16);
                    long idx = Unsafe.getLong(destAddr + i * 16 + 8);
                    sb.append(ts).append(':');
                    if ((idx & DATA_BIT) != 0) {
                        sb.append('d').append(idx & ~DATA_BIT);
                    } else {
                        sb.append('o').append(idx);
                    }
                }
                Assert.assertEquals(expected, sb.toString());
            } finally {
                Unsafe.free(dataAddr, dataSize, MemoryTag.NATIVE_O3);
                Unsafe.free(o3Addr, o3Size, MemoryTag.NATIVE_O3);
                Unsafe.free(destAddr, destSize, MemoryTag.NATIVE_O3);
            }
        });
    }
}
```

(Copy the licence header verbatim from any neighbouring test file if it differs from the one above.)

- [ ] **Step 2: Run to verify failure**

Run: `mvn -pl core -Dtest=O3ReplaceMergeIndexTest test -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAILURE, `createReplaceMergeIndex` not found.

- [ ] **Step 3: Implement** (in `O3PartitionJob`, public statics are alphabetical, so place it among the public static methods)

```java
    /**
     * Builds the merge index for a replace-commit merge of one row group: existing rows
     * with ts &lt; replaceLo, then the O3 slice [o3Lo, o3Hi] (every O3 row lies inside
     * the range), then existing rows with ts &gt; replaceHi. Existing rows inside the
     * range are dropped. Entries use the layout of {@link Vect#mergeTwoLongIndexesAsc}:
     * an existing row carries its row index with bit 63 set.
     *
     * @return the number of entries written to destAddr
     */
    public static long createReplaceMergeIndex(
            long srcTimestampAddr,
            long srcRowCount,
            long sortedTimestampsAddr,
            long o3Lo,
            long o3Hi,
            long replaceLo,
            long replaceHi,
            long destAddr
    ) {
        assert replaceLo <= replaceHi;
        final long prefixCount = replaceLo == Long.MIN_VALUE
                ? 0
                : Vect.boundedBinarySearch64Bit(srcTimestampAddr, replaceLo - 1, 0, srcRowCount - 1, Vect.BIN_SEARCH_SCAN_DOWN) + 1;
        final long suffixLo = Vect.boundedBinarySearch64Bit(srcTimestampAddr, replaceHi, 0, srcRowCount - 1, Vect.BIN_SEARCH_SCAN_DOWN) + 1;
        long written = 0;
        if (prefixCount > 0) {
            // An empty O3 index makes the native merge emit the existing rows only;
            // destAddr doubles as a valid, unread index pointer.
            Vect.mergeTwoLongIndexesAsc(srcTimestampAddr, 0, prefixCount, destAddr, 0, destAddr);
            written = prefixCount;
        }
        final long o3Count = o3Hi - o3Lo + 1;
        if (o3Count > 0) {
            Vect.memcpy(
                    destAddr + written * TIMESTAMP_MERGE_ENTRY_BYTES,
                    sortedTimestampsAddr + o3Lo * TIMESTAMP_MERGE_ENTRY_BYTES,
                    o3Count * TIMESTAMP_MERGE_ENTRY_BYTES
            );
            written += o3Count;
        }
        final long suffixCount = srcRowCount - suffixLo;
        if (suffixCount > 0) {
            final long suffixDest = destAddr + written * TIMESTAMP_MERGE_ENTRY_BYTES;
            Vect.mergeTwoLongIndexesAsc(srcTimestampAddr, suffixLo, suffixCount, suffixDest, 0, suffixDest);
            written += suffixCount;
        }
        return written;
    }
```

Confirm that `TIMESTAMP_MERGE_ENTRY_BYTES` is already statically imported in `O3PartitionJob` (it is used at line ~519). If it isn't, import it from `TableWriter`.

- [ ] **Step 4: Run tests**

Run: `mvn -pl core -Dtest=O3ReplaceMergeIndexTest test -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 5 PASS.

- [ ] **Step 5: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/O3PartitionJob.java core/src/test/java/io/questdb/test/cairo/parquet/O3ReplaceMergeIndexTest.java
git commit -m "Add replace-commit merge index builder

createReplaceMergeIndex emits the existing rows before the replace
range, the O3 slice, and the existing rows after the range, in the
entry layout mergeCopy consumes. The parquet row-group merge uses it
to drop replaced rows.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Apply replace commits to Parquet partitions end to end

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (~line 10376, the `if (isParquet)` throw in the replace branch)
- Modify: `core/src/main/java/io/questdb/cairo/O3PartitionJob.java` (`processPartition` dispatch ~line 769-813, `processParquetPartition` ~94-733, `mergeRowGroup` ~2139)
- Modify: `core/src/main/java/io/questdb/cairo/O3ParquetMergeContext.java`
- Create: `core/src/test/java/io/questdb/test/cairo/parquet/ParquetReplaceCommitTest.java`
- Modify: `core/src/test/java/io/questdb/test/cairo/wal/WalWriterReplaceRangeTest.java` (`testReplaceRangeNotSupportedParquetPartition`, ~line 978)

**Interfaces:**
- Consumes: `computeMergeActions(..., replaceLo, replaceHi)`, `ActionType.DROP`, `MergeAction.getO3RowCount()` (Task 1); `createReplaceMergeIndex` (Task 2).
- Produces: `O3ParquetMergeContext.getMergeFirstTimestamp()` / `setMergeFirstTimestamp(long)`; `processParquetPartition(..., long oldPartitionSize, long replaceLo, long replaceHi)`.

- [ ] **Step 1: Write the failing end-to-end tests**

Create `ParquetReplaceCommitTest.java` (same licence header as Task 2). Each test builds twin tables: `nat` stays native and `pq` has partitions converted to Parquet. It applies the identical replace commit to both, then asserts `pq` matches `nat` row for row. It also pins row count, min/max and the update-vs-rewrite choice, which it reads from the partition name txn.

```java
package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.datetime.microtime.MicrosTimestampDriver;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

public class ParquetReplaceCommitTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // super.setUp() resets per-node cairo state, so set overrides after it
        // (existing parquet tests set them in the test body, which runs later still).
        super.setUp();
        // 12 rows per day at 2h spacing -> 3 row groups of 4 per parquet partition.
        // Disable the dead-bytes rewrite triggers so only DROP / schema changes rewrite.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }

    @Test
    public void testAcrossRowGroupBoundaryUpdatesInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth("2024-01-01T05:00:00.000000Z", "2024-01-01T09:00:00.000000Z", "2024-01-01T07:00:00.000000Z");
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            assertQuery("SELECT count() FROM pq WHERE ts IN '2024-01-01'").returns("""
                    count
                    11
                    """);
        });
    }

    @Test
    public void testAfterAddColumnRewrites() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            execute("ALTER TABLE nat ADD COLUMN extra INT");
            execute("ALTER TABLE pq ADD COLUMN extra INT");
            drainWalQueue();
            long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth("2024-01-01T08:30:00.000000Z", "2024-01-01T11:30:00.000000Z", "2024-01-01T09:00:00.000000Z");
            assertTwinsEqual();
            Assert.assertNotEquals(txnBefore, partitionNameTxn("pq", 0));
        });
    }

    @Test
    public void testCoveredRowGroupWithNewRowsUpdatesInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth(
                    "2024-01-01T07:00:00.000000Z", "2024-01-01T15:00:00.000000Z",
                    "2024-01-01T09:30:00.000000Z", "2024-01-01T11:30:00.000000Z"
            );
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
        });
    }

    @Test
    public void testDropCoveredRowGroupRewrites() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth("2024-01-01T07:00:00.000000Z", "2024-01-01T15:00:00.000000Z");
            assertTwinsEqual();
            Assert.assertNotEquals(txnBefore, partitionNameTxn("pq", 0));
            assertQuery("SELECT count() FROM pq WHERE ts IN '2024-01-01'").returns("""
                    count
                    8
                    """);
        });
    }

    @Test
    public void testFirstPartitionHeadUpdatesMinTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            replaceBoth("2024-01-01T00:00:00.000000Z", "2024-01-01T03:00:00.000000Z");
            assertTwinsEqual();
            assertQuery("SELECT min(ts) FROM pq").returns("""
                    min
                    2024-01-01T04:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testFormatParquetLastPartitionTailUpdatesMaxTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(true);
            replaceBoth("2024-01-03T19:00:00.000000Z", "2024-01-03T23:59:59.999999Z");
            assertTwinsEqual();
            assertQuery("SELECT max(ts) FROM pq").returns("""
                    max
                    2024-01-03T18:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testFormatParquetReplaceCreatesNewPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(true);
            replaceBoth(
                    "2024-01-03T08:30:00.000000Z", "2024-01-04T02:00:00.000000Z",
                    "2024-01-03T09:00:00.000000Z", "2024-01-04T01:00:00.000000Z"
            );
            assertTwinsEqual();
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('pq')").returns("""
                    name\tnumRows\tisParquet
                    2024-01-01\t12\ttrue
                    2024-01-02\t12\ttrue
                    2024-01-03\t6\ttrue
                    2024-01-04\t1\ttrue
                    """);
        });
    }

    @Test
    public void testInsideRowGroupNoRowsUpdatesInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth("2024-01-01T08:30:00.000000Z", "2024-01-01T11:30:00.000000Z");
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
        });
    }

    @Test
    public void testInsideRowGroupUpdatesInPlace() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth(
                    "2024-01-01T08:30:00.000000Z", "2024-01-01T11:30:00.000000Z",
                    "2024-01-01T09:00:00.000000Z", "2024-01-01T10:30:00.000000Z"
            );
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            assertQuery("SELECT id, ts, v FROM pq WHERE ts BETWEEN '2024-01-01T08:00' AND '2024-01-01T12:00'").returns("""
                    id\tts\tv
                    5\t2024-01-01T08:00:00.000000Z\tv5
                    1000\t2024-01-01T09:00:00.000000Z\tr0
                    1001\t2024-01-01T10:30:00.000000Z\tr1
                    7\t2024-01-01T12:00:00.000000Z\tv7
                    """);
        });
    }

    @Test
    public void testMissingDataIsNoop() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            long txnBefore = partitionNameTxn("pq", 0);
            replaceBoth("2024-01-01T22:30:00.000000Z", "2024-01-01T23:30:00.000000Z");
            assertTwinsEqual();
            Assert.assertEquals(txnBefore, partitionNameTxn("pq", 0));
            assertQuery("SELECT count() FROM pq").returns("""
                    count
                    36
                    """);
        });
    }

    @Test
    public void testRemovesWholeParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            replaceBoth("2024-01-02T00:00:00.000000Z", "2024-01-02T23:59:59.999999Z");
            assertTwinsEqual();
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('pq')").returns("""
                    name\tnumRows\tisParquet
                    2024-01-01\t12\ttrue
                    2024-01-03\t12\tfalse
                    """);
        });
    }

    @Test
    public void testSpansNativeAndParquetPartitions() throws Exception {
        assertMemoryLeak(() -> {
            createTwins(false);
            replaceBoth(
                    "2024-01-02T20:00:00.000000Z", "2024-01-03T03:00:00.000000Z",
                    "2024-01-02T21:00:00.000000Z", "2024-01-03T01:00:00.000000Z"
            );
            assertTwinsEqual();
        });
    }

    private static void appendRows(TableToken token, String rangeLo, String rangeHi, String... rowTs) throws Exception {
        try (WalWriter ww = engine.getWalWriter(token)) {
            for (int i = 0; i < rowTs.length; i++) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor(rowTs[i]));
                row.putInt(0, 1000 + i);
                row.putVarchar(2, new Utf8String("r" + i));
                row.putSym(3, "n");
                row.putDouble(4, -i);
                row.append();
            }
            ww.commitWithParams(
                    MicrosTimestampDriver.floor(rangeLo),
                    MicrosTimestampDriver.floor(rangeHi) + 1,
                    WAL_DEDUP_MODE_REPLACE_RANGE
            );
        }
    }

    private static long partitionNameTxn(String table, int partitionIndex) {
        try (TableReader reader = engine.getReader(table)) {
            return reader.getTxFile().getPartitionNameTxn(partitionIndex);
        }
    }

    private static void replaceBoth(String rangeLo, String rangeHi, String... rowTs) throws Exception {
        appendRows(engine.verifyTableName("nat"), rangeLo, rangeHi, rowTs);
        appendRows(engine.verifyTableName("pq"), rangeLo, rangeHi, rowTs);
        drainWalQueue();
    }

    private void assertTwinsEqual() throws Exception {
        Assert.assertFalse("nat suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("nat")));
        Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
        assertSqlCursors("nat", "pq");
        assertSqlCursors("SELECT count(), min(ts), max(ts) FROM nat", "SELECT count(), min(ts), max(ts) FROM pq");
    }

    /**
     * Creates nat (native) and pq with 3 daily partitions of 12 rows at 2h spacing.
     * With formatParquet, pq is a FORMAT PARQUET table (every partition parquet,
     * including the last). Otherwise pq's first two days are converted to parquet
     * and the last stays native.
     */
    private void createTwins(boolean formatParquet) throws Exception {
        final String ddl = "CREATE TABLE %s (id INT, ts TIMESTAMP, v VARCHAR, sym SYMBOL, d DOUBLE) TIMESTAMP(ts) PARTITION BY DAY%s WAL";
        execute(String.format(ddl, "nat", ""));
        execute(String.format(ddl, "pq", formatParquet ? " FORMAT PARQUET" : ""));
        final String insert = """
                INSERT INTO %s
                SELECT x::INT, '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L, 'v' || x, 's' || (x % 3), x * 1.5
                FROM long_sequence(36)
                """;
        execute(String.format(insert, "nat"));
        execute(String.format(insert, "pq"));
        drainWalQueue();
        if (!formatParquet) {
            execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
            drainWalQueue();
        }
        assertQuery("SELECT count() FROM table_partitions('pq') WHERE isParquet").returns(
                formatParquet ? "count\n3\n" : "count\n2\n"
        );
    }
}
```

In `WalWriterReplaceRangeTest`, rename `testReplaceRangeNotSupportedParquetPartition` → `testReplaceRangeParquetPartition` and replace its body so it asserts success against a native twin:

```java
    @Test
    public void testReplaceRangeParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");
            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            execute("ALTER TABLE rg CONVERT PARTITION TO PARQUET LIST '2022-02-24'");
            drainWalQueue();

            insertRowWithReplaceRange(
                    "2022-02-24T17", "2022-02-24T14", "2022-02-25T18", tableToken,
                    false, false, "rg", "expected", false, false
            );
            assertQuery("select isParquet from table_partitions('rg') limit 1").returns("""
                    isParquet
                    true
                    """);
        });
    }
```

`insertRowWithReplaceRange` builds `expected` as a native `CREATE TABLE ... AS (... where ts not between lo and hi)`, applies the same rows with a plain commit, asserts `rg` is not suspended, and compares cursors.

- [ ] **Step 2: Run to verify failure**

Run: `mvn -pl core -Dtest='ParquetReplaceCommitTest,WalWriterReplaceRangeTest#testReplaceRangeParquetPartition' test -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL. Every test that touches a Parquet partition fails with `pq suspended` (the writer throws `commit replace mode is not supported for Parquet partitions`). `testSpansNativeAndParquetPartitions` fails the same way.

If `createTwins` itself fails, fix the fixture before touching production code:
- CONVERT rejected for the active partition: the `WHERE ts < '2024-01-03'` bound already excludes it.
- The expected partition listing differs, e.g. `table_partitions` column names.

- [ ] **Step 3: Remove the TableWriter throw**

In `TableWriter.java`, in the O3 partition loop replace branch, delete the whole `if (isParquet) { ... throw ... }` block, including its `o3PartitionUpdRemaining.decrementAndGet(); latchCount--; pressureControl...` bookkeeping, so the branch reads:

```java
                        if (isCommitReplaceMode()) {
                            o3TimestampLo = (partitionTimestamp == minO3PartitionTimestamp) ? o3TimestampMin : partitionTimestamp;
                            o3TimestampHi = (partitionTimestamp == maxO3PartitionTimestamp) ? o3TimestampMax :
                                    txWriter.getCurrentPartitionMaxTimestamp(partitionTimestamp);
                        } else {
```

- [ ] **Step 4: Thread the replace window through the dispatch**

In `O3PartitionJob.processPartition`, inside `if (isParquet) {`:

(a) In the `srcDataMax < 1` branch (`writeFreshParquetFromO3`), in replace mode `o3TimestampMin` is the replace-range start, not the first row. Before the call add:

```java
                if (tableWriter.isCommitReplaceMode()) {
                    // o3TimestampMin is replaceRangeLo in replace mode; the new
                    // partition starts at its first row.
                    assert srcOooLo <= srcOooHi;
                    o3TimestampMin = getTimestampIndexValue(sortedTimestampsAddr, srcOooLo);
                }
```

(b) Append two arguments to the `processParquetPartition(...)` call:

```java
                    oldPartitionSize,
                    tableWriter.isCommitReplaceMode() ? o3TimestampLo : Long.MAX_VALUE,
                    tableWriter.isCommitReplaceMode() ? o3TimestampHi : Long.MIN_VALUE
            );
```

and append `long replaceLo, long replaceHi` to `processParquetPartition`'s parameter list after `long oldPartitionSize`.

- [ ] **Step 5: Add the context scratch field**

In `O3ParquetMergeContext`, next to the other private fields (alphabetical), add `private long mergeFirstTimestamp = Long.MAX_VALUE;`. Add alphabetically placed accessors:

```java
    public long getMergeFirstTimestamp() {
        return mergeFirstTimestamp;
    }

    public void setMergeFirstTimestamp(long mergeFirstTimestamp) {
        this.mergeFirstTimestamp = mergeFirstTimestamp;
    }
```

In `clear()` add `mergeFirstTimestamp = Long.MAX_VALUE;`.

- [ ] **Step 6: Replace branch in `mergeRowGroup`**

Append `long replaceLo, long replaceHi` to `mergeRowGroup`'s parameters, after `O3ParquetMergeContext ctx`.

Replace the line `if (!tableWriter.isCommitDedupMode()) {` that opens the merge-index selection with:

```java
            if (replaceLo <= replaceHi) {
                // Replace commit: drop existing rows inside the range, keep the rest,
                // and splice the O3 slice between them. Replace and dedup are exclusive.
                timestampMergeIndexAddr = Unsafe.malloc(timestampMergeIndexSize, MemoryTag.NATIVE_O3);
                final long replaceRows = createReplaceMergeIndex(
                        timestampDataPtr,
                        rowGroupSize,
                        sortedTimestampsAddr,
                        mergeRangeLo,
                        mergeRangeHi,
                        replaceLo,
                        replaceHi,
                        timestampMergeIndexAddr
                );
                // A merge that empties its row group is a DROP, never a MERGE.
                assert replaceRows > 0;
                timestampMergeIndexAddr = Unsafe.realloc(
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        replaceRows * TIMESTAMP_MERGE_ENTRY_BYTES,
                        MemoryTag.NATIVE_O3
                );
                timestampMergeIndexSize = replaceRows * TIMESTAMP_MERGE_ENTRY_BYTES;
                duplicateCount = mergeRowCount - replaceRows;
                mergeRowCount = replaceRows;
            } else if (!tableWriter.isCommitDedupMode()) {
```

(the rest of the existing `if/else` stays as is).

After the existing `assert timestampMergeIndexAddr != 0;` add:

```java
            ctx.setMergeFirstTimestamp(Unsafe.getLong(timestampMergeIndexAddr));
```

In Phase 1b, guard the O3 var-data estimate against the empty slice of a filter-only merge. Replace:

```java
                    long neededDataSize = ctd.getDataVectorSize(srcOooFixAddr, mergeRangeLo, mergeRangeHi)
                            + ctd.getDataVectorSizeAt(columnAuxPtr, rowGroupSize - 1);
```

with:

```java
                    final long o3DataSize = mergeBatchRowCount > 0 ? ctd.getDataVectorSize(srcOooFixAddr, mergeRangeLo, mergeRangeHi) : 0;
                    long neededDataSize = o3DataSize + ctd.getDataVectorSizeAt(columnAuxPtr, rowGroupSize - 1);
```

Update the method's comment on the return value, i.e. the low 32 bits of the packed result, to say "rows removed by dedup or by the replace range".

- [ ] **Step 7: Restructure `processParquetPartition`**

(a) Rename the local `long duplicateCount = 0;` to `long removedRowCount = 0;` and every use in this method (`duplicateCount += mergeDuplicates;` → `removedRowCount += mergeDuplicates;`, and the sink `newPartitionSize - duplicateCount` → `newPartitionSize - removedRowCount`). Add, next to it:

```java
        final boolean isReplace = replaceLo <= replaceHi;
        // Set when a replace commit leaves the partition untouched or empties it; the
        // outer finally then publishes the result without any file having been written.
        boolean isReplaceNoop = false;
        boolean isReplaceRemoval = false;
        long resultMinTimestamp = Long.MAX_VALUE;
```

(b) Move the block that starts at the comment `// Build row group bounds for merge strategy computation.` and ends at the `computeMergeActions(...)` call (ending `isCommitDedup\n );`) so it sits immediately **before** the comment `// Decide whether to rewrite the file or update in-place.` It only needs `partitionDecoder`, `tableToParquetIdx`, `timestampIndex`, `timestampColumnType`, `rowGroupCount`, `isCommitDedup` and `ctx`, which are all defined earlier. Change the call to pass the range:

```java
                final int actionCount = O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds,
                        sortedTimestampsAddr,
                        srcOooLo,
                        srcOooHi,
                        rowGroupSize / 4,
                        rowGroupSize,
                        actionsBuf,
                        ctx.getRgO3Ranges(),
                        ctx.getGapO3Ranges(),
                        isCommitDedup,
                        replaceLo,
                        replaceHi
                );
```

(`rowGroupSize` is read from configuration earlier in the method, above the moved block. Keep it above.)

(c) Right after the moved block, and before the rewrite decision, classify the replace outcome:

```java
                boolean hasDrop = false;
                if (isReplace) {
                    boolean isAllDropped = true;
                    boolean isAllCopied = true;
                    for (int i = 0; i < actionCount; i++) {
                        final O3ParquetMergeStrategy.ActionType type = actionsBuf.getQuick(i).type;
                        hasDrop |= type == O3ParquetMergeStrategy.ActionType.DROP;
                        isAllDropped &= type == O3ParquetMergeStrategy.ActionType.DROP;
                        isAllCopied &= type == O3ParquetMergeStrategy.ActionType.COPY_ROW_GROUP_SLICE;
                    }
                    if (isAllCopied) {
                        // The range misses every row and brings no rows (COPY_O3 would
                        // break isAllCopied): leave the partition as it is.
                        isReplaceNoop = true;
                        resultMinTimestamp = O3ParquetMergeStrategy.getRowGroupMin(rowGroupBounds, 0);
                        LOG.info().$("parquet replace commit leaves partition unchanged [table=").$(tableWriter.getTableToken())
                                .$(", partition=").$ts(partitionTimestamp)
                                .I$();
                        return;
                    }
                    if (isAllDropped) {
                        isReplaceRemoval = true;
                        LOG.info().$("parquet replace commit removes partition [table=").$(tableWriter.getTableToken())
                                .$(", partition=").$ts(partitionTimestamp)
                                .I$();
                        return;
                    }
                }
```

Both `return`s run the inner `finally` (munmap) and the outer `finally` (sink + latch). No writer fd has been opened yet.

(d) Add the DROP gate to the rewrite decision:

```java
                isRewrite = hasSchemaChange
                        || forceFullReencode
                        || rowGroupCount == 1
                        || hasCoalescableTie
                        // update mode has no primitive to remove a row group
                        || hasDrop
                        || (parquetSize > 0 && ...
```

and add `.$(", hasDrop=").$(hasDrop)` to the `parquet o3 partition rewrite` log line.

(e) In the action loop:

- `MERGE`:
  - Pass `replaceLo, replaceHi` as the new trailing `mergeRowGroup` arguments.
  - Replace `(action.o3Hi - action.o3Lo + 1)` with `action.getO3RowCount()`, both in the log (`o3Rows`) and in `addPhysicallyWrittenRows`.
  - After the call add:
    ```java
                                if (resultMinTimestamp == Long.MAX_VALUE) {
                                    resultMinTimestamp = ctx.getMergeFirstTimestamp();
                                }
    ```
- `COPY_ROW_GROUP_SLICE`: at the top of the case add
  ```java
                                if (resultMinTimestamp == Long.MAX_VALUE) {
                                    resultMinTimestamp = O3ParquetMergeStrategy.getRowGroupMin(rowGroupBounds, action.rowGroupIndex);
                                }
  ```
- `COPY_O3`: at the top of the case add
  ```java
                                if (resultMinTimestamp == Long.MAX_VALUE) {
                                    resultMinTimestamp = getTimestampIndexValue(sortedTimestampsAddr, action.o3Lo);
                                }
  ```
- New case:
  ```java
                            case DROP -> {
                                // hasDrop forced rewrite mode: skipping the row group removes it.
                                assert isRewrite;
                                final long droppedRows = partitionDecoder.metadata().getRowGroupSize(action.rowGroupIndex);
                                LOG.info()
                                        .$("parquet drop row group [table=").$(tableWriter.getTableToken())
                                        .$(", partition=").$ts(partitionTimestamp)
                                        .$(", rg=").$(action.rowGroupIndex)
                                        .$(", rows=").$(droppedRows)
                                        .I$();
                                removedRowCount += droppedRows;
                            }
  ```
  `metadataPosition` does **not** advance for `DROP`.

(f) In the outer `finally`, replace the block from `final long fileSize = Files.length(path.$());` through the `partitionUpdateSinkAddr + 7 * Long.BYTES` put with:

```java
            if (isReplaceNoop) {
                updatePartitionSink(partitionUpdateSinkAddr, partitionTimestamp, resultMinTimestamp, oldPartitionSize, oldPartitionSize, 0);
            } else if (isReplaceRemoval) {
                updatePartitionSink(partitionUpdateSinkAddr, partitionTimestamp, Long.MAX_VALUE, 0, oldPartitionSize, 1);
            } else {
                final long fileSize = Files.length(path.$());
                Unsafe.putLong(partitionUpdateSinkAddr, partitionTimestamp);
                Unsafe.putLong(partitionUpdateSinkAddr + Long.BYTES, isReplace ? resultMinTimestamp : o3TimestampMin);
                Unsafe.putLong(partitionUpdateSinkAddr + 2 * Long.BYTES, newPartitionSize - removedRowCount);
                Unsafe.putLong(partitionUpdateSinkAddr + 3 * Long.BYTES, oldPartitionSize);
                // flags: lowInt = partitionMutates (0 when rewritten, 1 when mutated in place)
                Unsafe.putLong(partitionUpdateSinkAddr + 4 * Long.BYTES, Numbers.encodeLowHighInts(isRewrite ? 0 : 1, 0));
                Unsafe.putLong(partitionUpdateSinkAddr + 5 * Long.BYTES, 0); // o3SplitPartitionSize
                Unsafe.putLong(partitionUpdateSinkAddr + 7 * Long.BYTES, fileSize); // update parquet partition file size
            }
```

Keep the `path.of(pathToTable); setPathForParquetPartition(...)` lines above it and the latch countdown below it unchanged. The removal sink mirrors native's full-partition removal (`updatePartition(..., Long.MAX_VALUE, 0, oldPartitionSize, 1)`). `o3ConsumePartitionUpdateSink` then removes the partition and queues its directory by name txn.

A failure after (c) still goes through the existing error path. `isReplaceNoop`/`isReplaceRemoval` are false there, so behaviour is unchanged.

- [ ] **Step 8: Run the new tests**

Run: `mvn -pl core -Dtest='ParquetReplaceCommitTest,WalWriterReplaceRangeTest#testReplaceRangeParquetPartition,O3ReplaceMergeIndexTest,O3ParquetMergeStrategyTest' test -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
Expected: all PASS. If one fails, debug from its log lines (`parquet merge row group`, `parquet drop row group`, `o3 partition update`) before changing assertions. The differential `assertSqlCursors` against the native twin is the oracle; only hand-written expected values may be wrong.

- [ ] **Step 9: Run the regression neighbourhood**

Run sequentially:
1. `mvn -pl core -Dtest='WalWriterReplaceRangeTest,O3Parquet*Test,WalParquetO3BlockApplyTest,TableFormatTest,ParquetColumnTypeConversionTest' test -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
2. `mvn -pl core -Dtest='MatViewIdenticalReplaceTest,LiveViewBaseReplaceRangeTest' test -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`

Expected: all PASS. Investigate any failure as a live bug (repo `CLAUDE.md`: prove before calling anything pre-existing, by checking the same test on `origin/master` in the hub).

- [ ] **Step 10: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/TableWriter.java core/src/main/java/io/questdb/cairo/O3PartitionJob.java core/src/main/java/io/questdb/cairo/O3ParquetMergeContext.java core/src/test/java/io/questdb/test/cairo/parquet/ParquetReplaceCommitTest.java core/src/test/java/io/questdb/test/cairo/wal/WalWriterReplaceRangeTest.java
git commit -m "Support replace commits on parquet partitions

TableWriter no longer rejects replace-range commits that reach a
parquet partition. processParquetPartition passes the per-partition
replace window to the merge strategy; mergeRowGroup drops replaced
rows through createReplaceMergeIndex. The partition updates in place
unless a row group must disappear, which forces the crash-safe
rewrite. Replace commits that miss all data or remove every row
return before writing and publish a no-op or partition removal.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Fuzz coverage and docs

**Files:**
- Modify: `core/src/test/java/io/questdb/test/cairo/fuzz/ReplaceInsertFuzzTest.java`
- Modify: `core/src/main/java/io/questdb/cairo/CLAUDE.md`

- [ ] **Step 1: Add fuzz variants** (alphabetical among the tests). They use the 19-argument `setFuzzProbabilities(cancelRows, notSet, nullSet, rollback, colAdd, colRemove, colRename, colTypeChange, dataAdd, equalTsRows, partitionDrop, partitionToParquet, partitionToNative, truncate, tableDrop, setTtl, replace, symbolAccess, queryProb)` overload in `AbstractFuzzTest`:

```java
    @Test
    public void testReplaceOnFormatParquetTable() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setCreateWalAsParquet(true);
        setFuzzProbabilities(
                0.01, 0.2, 0.1, 0.01,
                0.02, 0.02, 0.02, 0,
                1.0, 0.01, 0.01,
                0, 0, 0, 0, 0,
                0.5, 0.05, 0
        );
        setFuzzCounts(
                rnd.nextBoolean(), 1000, 5 + rnd.nextInt(100),
                20, 10, 200, rnd.nextInt(100), 1
        );
        runFuzz(rnd);
    }

    @Test
    public void testReplaceWithParquetConversions() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(
                0.01, 0.2, 0.1, 0.01,
                0.02, 0.02, 0.02, 0,
                1.0, 0.01, 0.01,
                0.2, 0.02, 0, 0, 0,
                0.5, 0.05, 0
        );
        setFuzzCounts(
                rnd.nextBoolean(), 1000, 5 + rnd.nextInt(100),
                20, 10, 200, rnd.nextInt(100), 1
        );
        runFuzz(rnd);
    }
```

If the 19-argument overload's parameter order differs from the list above when read at execution time, match the real order: `partitionToParquetProb` and `replaceProb` must be the non-zero values shown.

- [ ] **Step 2: Run each fuzz test 5 times** (a single green fuzz run proves little)

Run: `for i in 1 2 3 4 5; do mvn -q -pl core -Dtest='ReplaceInsertFuzzTest#testReplaceWithParquetConversions+testReplaceOnFormatParquetTable' test -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false || break; done`
Expected: 5× PASS. On a failure, rerun with the seeds printed in the log (`generateRandom` logs them) to reproduce deterministically before fixing.

Before trusting the green runs, confirm that the fuzz actually reaches the new path. Grep the test log (`core/target/surefire-reports/*ReplaceInsertFuzzTest-output.txt` or console) for `parquet merge row group`, `parquet drop row group`, or `parquet replace commit`. If none appear, the generator never lands a replace on a Parquet partition; raise the conversion probability until it does.

- [ ] **Step 3: Update the cairo `CLAUDE.md`**

In the merge-action table add a row:

```markdown
| `DROP` | a row group fully inside a replace-commit range that brings no rows for it | nothing written (forces `isRewrite`) | no |
```

Add `|| hasDrop                // replace commit removes a row group; update mode cannot` to the `isRewrite` block. Add a section:

```markdown
## Replace commits

A replace commit (`WAL_DEDUP_MODE_REPLACE_RANGE`) passes its per-partition window
`[replaceLo, replaceHi]` to `computeMergeActions`. A row group the window intersects
without bringing O3 rows becomes a filter-only `MERGE` (empty O3 slice, `o3Hi < o3Lo`)
or, when fully covered, a `DROP`. `mergeRowGroup` builds its index with
`createReplaceMergeIndex` (existing rows before the window, O3 rows, existing rows after
it) instead of `createMergeIndex`/dedup; replace and dedup never combine. A window that
misses every row with no O3 rows publishes a no-op; one that drops every row group with
no O3 rows publishes a size-0 removal. Neither writes a file.
```

Add `DROP` to the `O3ParquetMergeStrategy.java` row of Key Files.

- [ ] **Step 4: Commit**

```bash
git add core/src/test/java/io/questdb/test/cairo/fuzz/ReplaceInsertFuzzTest.java core/src/main/java/io/questdb/cairo/CLAUDE.md
git commit -m "Fuzz replace commits over parquet partitions

ReplaceInsertFuzzTest gains variants that convert partitions to
parquet and that create FORMAT PARQUET tables, so replace-range
commits land on parquet row groups. The cairo CLAUDE.md documents the
DROP action and the replace-commit path.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Final verification

- [ ] **Step 1: Broader suite**

Run sequentially:
1. `mvn -pl core -Dtest='*Parquet*Test,*ReplaceRange*Test,ReplaceInsertFuzzTest,MatView*Test' test -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
2. `mvn -pl core -Dtest='WalWriterFuzzTest,O3*Test' test -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`

Expected: all PASS. Record counts (run/failures/errors/skipped) from the surefire summary. A skipped test proves nothing, so list any skips.

- [ ] **Step 2: Report**

Summarise:
- what changed
- test counts
- rewrite-vs-update behaviour confirmed by tests
- the out-of-scope items: native's identical-data short-circuit; no `remove_row_group` primitive
