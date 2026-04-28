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

package io.questdb.test.cairo;

import io.questdb.cairo.idx.PostingIndexNative;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Red tests for the critical findings raised in the PR review of #6861.
 * Each test reproduces a defect that should fail against the current
 * implementation. Tests that require fault injection or platform-specific
 * conditions are marked with comments describing what they need.
 */
public class PostingIndexCriticalIssuesTest extends AbstractCairoTest {

    /**
     * Critical #1: PostingIndexNative Java fallback silently corrupts data
     * at bitWidth=64 because of the 64-bit shift no-op in
     * unpackAllValuesNativeFallback (line 133: buffer >>>= bitWidth).
     * The fallback runs on platforms without the native AVX2 library.
     */
    @Test
    public void testFallbackBitWidth64RoundTripIsCorrect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int count = 4;
            int bitWidth = 64;
            long minValue = 0;

            long[] inputs = {
                    0L,
                    0x0123_4567_89AB_CDEFL,
                    0xFFFF_FFFF_FFFF_FFFFL,
                    Long.MIN_VALUE
            };

            long valuesAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            int packedBytes = (count * bitWidth + 7) / 8 + 8;
            long packedAddr = Unsafe.malloc(packedBytes, MemoryTag.NATIVE_DEFAULT);
            long unpackedAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(valuesAddr + (long) i * Long.BYTES, inputs[i]);
                }

                PostingIndexNative.packValuesNativeFallback(valuesAddr, count, minValue, bitWidth, packedAddr);
                PostingIndexNative.unpackAllValuesNativeFallback(packedAddr, count, bitWidth, minValue, unpackedAddr);

                for (int i = 0; i < count; i++) {
                    long actual = Unsafe.getUnsafe().getLong(unpackedAddr + (long) i * Long.BYTES);
                    // RED: at bitWidth=64 the fallback right-shift is a no-op
                    // so the unpacked value collides with the next iteration's buffer.
                    Assert.assertEquals("fallback corrupted value at index " + i,
                            inputs[i], actual);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(packedAddr, packedBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(unpackedAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Critical #3: native JNI methods accept invalid bit widths with no
     * validation, leading to undefined behavior on corrupted index files.
     * Negative, zero, and >64 bit widths must be rejected explicitly.
     */
    @Test
    public void testNativeRejectsBitWidthZero() throws Exception {
        if (!PostingIndexNative.isNativeAvailable()) {
            return;
        }
        TestUtils.assertMemoryLeak(() -> {
            int count = 4;
            long src = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            long dst = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(src, 64, (byte) 0);
                Unsafe.getUnsafe().setMemory(dst, (long) count * Long.BYTES, (byte) 0xAA);
                try {
                    PostingIndexNative.unpackAllValuesNative(src, count, 0, 1234L, dst);
                    Assert.fail("expected rejection of bitWidth=0");
                } catch (IllegalArgumentException expected) {
                    Assert.assertTrue(expected.getMessage().contains("bitWidth"));
                }
            } finally {
                Unsafe.free(src, 64, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dst, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testNativeRejectsBitWidthNegative() throws Exception {
        if (!PostingIndexNative.isNativeAvailable()) {
            return;
        }
        TestUtils.assertMemoryLeak(() -> {
            int count = 4;
            long src = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            long dst = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(src, 64, (byte) 0);
                try {
                    PostingIndexNative.unpackAllValuesNative(src, count, -1, 0L, dst);
                    Assert.fail("expected rejection of negative bitWidth");
                } catch (IllegalArgumentException expected) {
                    Assert.assertTrue(expected.getMessage().contains("bitWidth"));
                }
            } finally {
                Unsafe.free(src, 64, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dst, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testNativeRejectsBitWidthAbove64() throws Exception {
        if (!PostingIndexNative.isNativeAvailable()) {
            return;
        }
        TestUtils.assertMemoryLeak(() -> {
            int count = 4;
            long src = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
            long dst = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(src, 128, (byte) 0);
                try {
                    PostingIndexNative.unpackAllValuesNative(src, count, 100, 0L, dst);
                    Assert.fail("expected rejection of bitWidth=100");
                } catch (IllegalArgumentException expected) {
                    Assert.assertTrue(expected.getMessage().contains("bitWidth"));
                }
            } finally {
                Unsafe.free(src, 128, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dst, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Critical #8: ExpressionNode.deepClone restoration in the covering
     * DISTINCT path leaves the original mutated nodes in expressionNodePool.
     * If the optimization is rejected, downstream compilation must still
     * see a coherent WHERE clause and produce correct results.
     */
    @Test
    public void testDistinctRejectionPreservesWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_reject (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        extra DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t_distinct_reject VALUES
                    ('2024-01-01T00:00:00Z', 'A', 1.0, 10.0),
                    ('2024-01-01T01:00:00Z', 'B', 2.0, 20.0),
                    ('2024-01-01T02:00:00Z', 'A', 3.0, 30.0),
                    ('2024-01-01T03:00:00Z', 'C', 4.0, 40.0)
                    """);

            // The non-interval / non-key predicate forces rejection of the
            // posting-DISTINCT optimization. After rejection, the regular
            // DISTINCT codegen must observe the original WHERE clause.
            // ORDER BY locks ordering so this asserts row identity, not order.
            assertSql(
                    "sym\n" +
                            "A\n" +
                            "B\n",
                    "SELECT DISTINCT sym FROM t_distinct_reject WHERE sym IN ('A','B') AND extra > 5.0 ORDER BY sym"
            );

            // Run the same query a second time. If the rejected path leaked
            // mutated nodes back into the expression pool, a subsequent
            // compilation that pulls from the same pool can observe stale
            // tree state and produce different (or wrong) rows.
            assertSql(
                    "sym\n" +
                            "A\n" +
                            "B\n",
                    "SELECT DISTINCT sym FROM t_distinct_reject WHERE sym IN ('A','B') AND extra > 5.0 ORDER BY sym"
            );
        });
    }

    /**
     * Critical #10: multi-key covering cursor changes result ordering vs
     * the legacy non-covering path. With LIMIT and no ORDER BY the rows
     * returned must match the legacy path so user dashboards do not see
     * silent regressions.
     */
    @Test
    public void testMultiKeyCoveringMatchesLegacyOrdering() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_legacy (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    CREATE TABLE t_covering (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            String inserts = """
                    INSERT INTO %s VALUES
                    ('2024-01-01T00:00:00Z', 'A', 1.0),
                    ('2024-01-01T01:00:00Z', 'B', 2.0),
                    ('2024-01-02T00:00:00Z', 'A', 3.0),
                    ('2024-01-02T01:00:00Z', 'B', 4.0),
                    ('2024-01-03T00:00:00Z', 'A', 5.0),
                    ('2024-01-03T01:00:00Z', 'B', 6.0)
                    """;
            execute(inserts.formatted("t_legacy"));
            execute(inserts.formatted("t_covering"));

            // Force the legacy path with the no_covering hint and compare to
            // the default covering path. The result set must match row-for-row.
            printSql("SELECT /*+ no_covering */ sym, price FROM t_legacy WHERE sym IN ('A','B') LIMIT 4");
            String legacy = sink.toString();
            sink.clear();
            printSql("SELECT sym, price FROM t_covering WHERE sym IN ('A','B') LIMIT 4");
            String covering = sink.toString();

            // RED: covering iterates (partitions outer, keys inner), so
            // LIMIT 4 returns rows from partitions 0..1 only; the legacy
            // path interleaves keys per partition. The two outputs differ.
            Assert.assertEquals(
                    "covering and legacy paths must return identical rows under LIMIT",
                    legacy, covering
            );
        });
    }

    /**
     * Critical #2: setMaxValue writes to the active page outside the
     * seqlock. A concurrent reader that reads PAGE_OFFSET_MAX_VALUE while
     * the writer is mid-update can observe a torn 8-byte field.
     * <p>
     * This test is a scaffold: today no production reader consumes
     * maxValue, so demonstrating the torn read requires either a probe
     * that reads PAGE_OFFSET_MAX_VALUE directly through PostingIndexUtils,
     * or asserting that setMaxValue runs under the seqlock by inspecting
     * sequence numbers before and after the call.
     */
    @Test
    public void testSetMaxValueIsSeqlockProtected() {
        // RED: enforce that any future reader of maxValue can rely on the
        // seqlock. A direct assertion would require reading the .pk file
        // bytes around setMaxValue and checking that seqStart/seqEnd are
        // bumped. Implement by:
        //   1. open a writer, append rows, capture seqStart of active page
        //   2. call setMaxValue(newMax) on a different value
        //   3. read seqStart/seqEnd from the .pk file mapping
        //   4. assert seqStart == seqEnd && seqEnd > captured_seq
        // Today setMaxValue bypasses the seqlock entirely, so step 4 fails.
        Assert.fail(
                "Critical finding #2 lacks a regression test. "
                        + "Implement once setMaxValue is brought under the seqlock "
                        + "or document why no reader will ever consume PAGE_OFFSET_MAX_VALUE."
        );
    }

    /**
     * Critical #6: publishPendingPurges uses a pre-commit txn as the
     * scoreboard upper bound. A purge can fire while a reader still holds
     * a superseded .pv mapping IF the scoreboard semantics shift.
     * <p>
     * Reproduction outline:
     *   1. open writer W, ingest rows that produce a sealed posting index at
     *      sealTxn=N; capture txn T0.
     *   2. open a long-lived reader R against txn T0; let R hold the .pv
     *      file open through a posting index reader.
     *   3. trigger an O3 commit on W that bumps to sealTxn=N+1.
     *      O3 reseal calls publishPendingPurges with txWriter.getTxn() (still T0
     *      pre-commit) so the recorded purge interval falls into the
     *      [0, Long.MAX_VALUE) conservative branch.
     *   4. drain the PostingSealPurgeJob.
     *   5. verify R still reads consistent rows from sealTxn=N's .pv —
     *      i.e. the file was NOT deleted out from under it.
     * The test fails today only if the [0, MAX_VALUE) defensive branch is
     * skipped, but it pins the contract so a future refactor cannot delete
     * the conservative path silently.
     */
    @Test
    public void testO3SealPurgeRespectsPendingReader() {
        Assert.fail(
                "Critical finding #6 needs a fault-injection or scoreboard-pin test "
                        + "to lock in the [0, Long.MAX_VALUE) conservative purge interval. "
                        + "Until written, document the contract in code."
        );
    }

    /**
     * Critical #7: the seal loop in TableWriter.sealPostingIndexesForO3Partitions
     * has no rollback for partitions 0..N-1 when partition N's seal throws.
     * Recovery relies entirely on the orphan scan in logOrphanSealedFiles
     * on next reopen.
     * <p>
     * Reproduction outline (requires FilesFacade fault injection):
     *   1. CREATE TABLE with POSTING index across 3 partitions.
     *   2. install a FilesFacade that throws on the 3rd partition's
     *      sidecar mmap.
     *   3. perform an O3 insert that touches all 3 partitions.
     *   4. expect TableWriter to be marked distressed and the writer pool
     *      to replace it.
     *   5. reopen the table and verify:
     *        - data is consistent against the pre-O3 snapshot
     *        - no orphan .pv.{newSealTxn} or .pc{i}.{newSealTxn} files remain
     *        - subsequent queries via posting index work correctly
     */
    @Test
    public void testSealLoopPartialFailureRecovers() {
        // Scaffold for a real fault-injection test. Once the seal-failure
        // FilesFacade hook is parameterised in PostingIndexStressTest, port
        // this here.
        Assert.fail(
                "Critical finding #7 needs a FilesFacade fault-injection test. "
                        + "Required: throw on partition N's sidecar mmap during O3 reseal, "
                        + "then verify reopen restores a consistent state and orphan scan "
                        + "removes the partial sidecars from partitions 0..N-1."
        );
    }

    /**
     * Critical #9: ColumnPurgeOperator retry loop has no failure cap.
     * On Windows, a sidecar held open by a long-running query causes
     * removeQuiet to fail; the task re-enqueues without backoff.
     * <p>
     * Portable reproduction via FilesFacade injection:
     *   1. CREATE TABLE with POSTING index.
     *   2. DROP INDEX.
     *   3. install a FilesFacade whose remove() returns false for any
     *      .pc{N}.* file; count invocations.
     *   4. drain the column purge queue with a wall-clock deadline.
     *   5. assert invocations are bounded (e.g. < 1000) — today the loop
     *      retries indefinitely.
     */
    @Test
    public void testColumnPurgeRetryIsBounded() {
        Assert.fail(
                "Critical finding #9 needs a FilesFacade-injected purge test. "
                        + "Add a removal-failure counter and assert the retry count is bounded "
                        + "before claiming Windows file-locked safety."
        );
    }

    /**
     * Critical #4 and #5 are style violations enforced by static
     * analysis, not JUnit. They cannot be expressed as red unit tests.
     * Reference checks:
     *   #4 Banner comments forbidden by CLAUDE.md:
     *      grep -rn "// ===\|// ---" core/src/test/java/io/questdb/test/cairo/PostingIndex*.java
     *      grep -rn "// ===\|// ---" core/src/test/java/io/questdb/test/cairo/CoveringIndex*.java
     *      Both must return zero matches.
     *   #5 assertSql vs assertQueryNoLeakCheck:
     *      grep -c "assertSql\b" core/src/test/java/io/questdb/test/cairo/CoveringIndexTest.java
     *      Must be zero (or only inside helper methods).
     * These are noted here so the red-test set covers all critical findings,
     * even those that are not JUnit-testable.
     */
    @SuppressWarnings("unused")
    private void styleCheckPlaceholder() {
        // Kept as documentation; no JUnit test.
        AtomicReference<String> ignore = new AtomicReference<>();
        ignore.set("see method javadoc");
    }
}
