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

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.BitpackUtils;
import io.questdb.cairo.idx.PostingIndexNative;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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

    // Critical findings #2 (setMaxValue seqlock), #6 ([0, Long.MAX_VALUE)
    // conservative purge interval) and #7 (seal-loop partial failure
    // recovery) were RED placeholders during the v1 era. They are now
    // addressed structurally by the v2 chain redesign:
    //   #2 — chain.updateHeadMaxValue publishes via the chain header
    //        seqlock, so any reader of MAX_VALUE goes through the same
    //        consistency protocol as keyCount/genCount.
    //   #6 — recordPostingSealPurge derives [fromTxn, toTxn) from the
    //        chain entries themselves; the residual [0, MAX) branch only
    //        fires for the empty-chain edge case, which the
    //        writer-open recovery walk picks up on the next reopen.
    //   #7 — recoveryDropAbandoned (run from PostingIndexWriter.of after
    //        setCurrentTableTxn) drops every chain entry whose txnAtSeal
    //        was published before the encompassing txWriter.commit
    //        landed.
    // Critical finding #9 (ColumnPurgeOperator retry cap on Windows) is
    // orthogonal to the posting-index chain rewrite and remains tracked
    // separately.

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

    // =========================================================================
    // Red tests for the v2 review of PR #6861 (current pass).
    //
    // Each test below maps to a finding from the review report. Tests that
    // require fault injection use TestFilesFacadeImpl; tests that require
    // concurrency simulate the race by mutating ff.length() between mmap
    // setup and chain access. Findings that can only manifest from sources
    // FilesFacade does not see (Unsafe.realloc OOM, queue-pool exhaustion)
    // are documented in trailing comments.
    // =========================================================================

    // Review finding #1 (sidecar mem fd leak in openSidecarFiles) was
    // dropped after verification: MemoryCMARWImpl.extend0 (line 403) and
    // map0 (line 417) both close the fd on mmap/mremap failure inside
    // jumpTo(). The "orphan mem" identified in the review never holds an
    // open fd by the time the outer catch fires — it is already closed
    // internally by the memory-mapping helper.
    // Documented for traceability; no JUnit red test.

    /**
     * Review finding #3 — reader reads past keyMem mmap when the chain
     * header points to an entry beyond the mapped region.
     * <p>
     * {@code AbstractPostingIndexReader.of(...)} (line 250-258) maps the
     * {@code .pk} key file with {@code size=-1}, which {@code MemoryCMRImpl}
     * resolves via {@code ff.length(fd)} at the moment of mmap. If a
     * concurrent writer publishes a new chain entry past that snapshotted
     * length, the picker reads at {@code headEntryOffset} which is past
     * {@code keyMem.size()}. The picker's bound check (PostingIndexChainPicker.java:95)
     * compares against the header's {@code regionLimit} only — not against
     * {@code keyMem.size()}. With assertions enabled (CLAUDE.md says
     * QuestDB runs {@code -ea} in production), this fires
     * {@code AbstractMemoryCR}'s {@code assert checkOffsetMapped(offset)}
     * and crashes the reader.
     * <p>
     * To make the race deterministic without thread interleaving, the
     * test uses a FilesFacade that returns a stale length (just above
     * KEY_FILE_RESERVED) for the {@code .pk} file when the reader opens
     * it. The on-disk file has been extended past that point by a
     * preceding seal, mimicking the post-publish state the reader would
     * see if it raced an extendHead.
     */
    @Test
    public void testReviewFinding3_PostingReaderHandlesStaleKeyFileLength() throws Exception {
        final AtomicBoolean staleLength = new AtomicBoolean(false);
        // Track the .pk fds opened by the reader so length() only clips
        // those, leaving other files (.pv, .d, etc.) reporting actual size.
        final java.util.concurrent.ConcurrentHashMap<Long, Boolean> pkFds =
                new java.util.concurrent.ConcurrentHashMap<>();
        final AtomicLong clippedReadCount = new AtomicLong(0);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long length(long fd) {
                long actual = super.length(fd);
                // Simulate the TOCTOU window: the first length() query for
                // a .pk fd returns a stale value (smaller than the file
                // actually is on disk). Subsequent queries return truth,
                // mimicking what happens after a writer's publish settles.
                // The extend-and-retry loop in readIndexMetadataFromChain
                // must observe the new length on retry and grow the mmap.
                if (staleLength.get() && pkFds.containsKey(fd) && actual > 8192) {
                    if (clippedReadCount.getAndIncrement() == 0) {
                        return 8192;
                    }
                }
                return actual;
            }

            @Override
            public long openRO(LPSZ name) {
                long fd = super.openRO(name);
                if (fd != -1 && isPkFile(name)) {
                    pkFds.put(fd, Boolean.TRUE);
                }
                return fd;
            }

            @Override
            public long openRONoCache(LPSZ name) {
                long fd = super.openRONoCache(name);
                if (fd != -1 && isPkFile(name)) {
                    pkFds.put(fd, Boolean.TRUE);
                }
                return fd;
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (fd != -1 && isPkFile(name)) {
                    pkFds.put(fd, Boolean.TRUE);
                }
                return fd;
            }

            @Override
            public boolean close(long fd) {
                pkFds.remove(fd);
                return super.close(fd);
            }

            private boolean isPkFile(LPSZ name) {
                // Path is "{...}/{sym}.pk" optionally followed by ".{txn}".
                return name != null && Utf8s.containsAscii(name, ".pk");
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_stale_pk (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Insert enough rows so the chain entry lives past 8KB.
            StringBuilder sb = new StringBuilder("INSERT INTO t_stale_pk VALUES");
            for (int i = 0; i < 1_000; i++) {
                sb.append(i == 0 ? "\n" : ",\n")
                        .append("('2024-01-01T")
                        .append(String.format("%02d:%02d:00", (i / 60) % 24, i % 60))
                        .append("', '")
                        .append("k").append(i % 50)
                        .append("')");
            }
            execute(sb.toString());
            engine.releaseAllWriters();

            staleLength.set(true);
            try {
                // The query opens a TableReader, which opens a posting-
                // index reader, which mmaps .pk at the (now stale) length.
                // The picker then walks the chain. The chain entry lives
                // past 8KB on disk, but the mmap stops at 8KB.
                //
                // RED: with the missing bound check (PostingIndexChainPicker:91-95
                // does not compare entryOffset against keyMem.size()), the
                // reader either returns wrong rows (silent corruption) or
                // raises AssertionError from AbstractMemoryCR's
                // checkOffsetMapped under -ea. The fix must produce a
                // clean CairoException with a descriptive message, so the
                // test asserts that the query either returns the correct
                // count (50 distinct keys) or throws CairoException —
                // anything else (AssertionError, NPE, wrong count) is
                // a confirmed bug.
                // SELECT DISTINCT sym dispatches to the posting-index
                // distinct factory which walks every key in the chain,
                // forcing the picker to read past the (clipped) mmap.
                long count = -1;
                AssertionError jvmAssertionFromPicker = null;
                try {
                    try (RecordCursorFactory factory = select(
                            "SELECT count(*) AS c FROM (SELECT DISTINCT sym FROM t_stale_pk)")) {
                        try (io.questdb.cairo.sql.RecordCursor c = factory.getCursor(sqlExecutionContext)) {
                            if (c.hasNext()) {
                                count = c.getRecord().getLong(0);
                            }
                        }
                    }
                } catch (io.questdb.cairo.CairoException ok) {
                    // Acceptable post-fix outcome: clean error surfaced.
                    return;
                } catch (AssertionError jvmAssert) {
                    jvmAssertionFromPicker = jvmAssert;
                }

                if (jvmAssertionFromPicker != null) {
                    Assert.fail(
                            "RED: posting-index picker raised raw AssertionError "
                                    + "(checkOffsetMapped fired) when key-file mmap is smaller "
                                    + "than the chain head. The picker must compare entryOffset "
                                    + "against keyMem.size(), not just regionLimit. Stack: "
                                    + jvmAssertionFromPicker.getClass().getName()
                    );
                }
                Assert.assertEquals(
                        "stale-mmap reader returned wrong distinct count "
                                + "(picker may have walked off the mapped region "
                                + "and read junk)",
                        50L, count
                );
                Assert.assertTrue(
                        "test setup gap: length() override never fired for .pk; "
                                + "pkFds tracked=" + pkFds.size()
                                + ", clipped=" + clippedReadCount.get(),
                        clippedReadCount.get() > 0
                );
            } finally {
                staleLength.set(false);
            }
        });
    }

    /**
     * Review finding #4 — {@code BitpackUtils.unpackValuesFrom} Java
     * pre-fill loop drops bits when {@code skipBits + bitWidth > 64}.
     * <p>
     * BitpackUtils.java:264-271:
     * <pre>
     *     while (bufferBits &lt; skipBits + bitWidth && srcOffset &lt; totalBytes) {
     *         buffer |= ((Unsafe.getByte(...) & 0xFFL) &lt;&lt; bufferBits);
     *         bufferBits += 8;
     *     }
     * </pre>
     * With {@code bitWidth=63} and {@code startIndex=1}, {@code skipBits=7}
     * and the loop wants {@code bufferBits >= 70}. After the 8th byte
     * {@code bufferBits=64}; the 9th iteration evaluates {@code b << 64},
     * which Java treats as {@code b << 0} (the JVM masks the shift count
     * to 6 bits), OR-ing the byte into {@code buffer}'s low bits and
     * silently corrupting them. The main loop's spill mechanism is not
     * mirrored in the pre-fill.
     * <p>
     * Pure-Java reproducer: pack 5 distinct values that span 63 bits with
     * the fallback packer, then unpack from {@code startIndex=1} and
     * compare against the input.
     */
    @Test
    public void testReviewFinding4_BitpackPrefillSkipBitsBoundary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int count = 5;
            int bitWidth = 63;
            long minValue = 0L;

            // Corruption analysis: the bug ORs byte[15] bit 7 into the
            // unpacked value[1]'s bit 0. byte[15] bit 7 = stream bit 127
            // = value[2] bit 1. To make the bug observable, set
            // value[1] bit 0 = 0 and value[2] bit 1 = 1 so the corrupted
            // unpack produces a 1 where 0 is expected.
            // The (1L<<62) entry forces the packer to use the full 63-bit
            // range so bw=63 stays meaningful.
            long[] values = {
                    0L,
                    0L,
                    2L,
                    0L,
                    1L << 62
            };

            long valuesAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            int packedBytes = (count * bitWidth + 7) / 8 + 16;
            long packedAddr = Unsafe.malloc(packedBytes, MemoryTag.NATIVE_DEFAULT);
            long destAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(valuesAddr + (long) i * Long.BYTES, values[i]);
                }
                Unsafe.getUnsafe().setMemory(packedAddr, packedBytes, (byte) 0);

                PostingIndexNative.packValuesNativeFallback(
                        valuesAddr, count, minValue, bitWidth, packedAddr);

                // Sanity: unpack from index 0 (skipBits=0) — the aligned
                // path is unaffected by the bug.
                BitpackUtils.unpackValuesFrom(
                        packedAddr, 0, count, bitWidth, minValue, destAddr);
                for (int i = 0; i < count; i++) {
                    long actual = Unsafe.getUnsafe().getLong(destAddr + (long) i * Long.BYTES);
                    Assert.assertEquals(
                            "aligned unpack must round-trip at index " + i,
                            values[i], actual);
                }

                // The buggy path: startIndex=1 produces skipBits=7,
                // skipBits + bitWidth = 70 > 64.
                Unsafe.getUnsafe().setMemory(destAddr, (long) count * Long.BYTES, (byte) 0);
                BitpackUtils.unpackValuesFrom(
                        packedAddr, 1, count - 1, bitWidth, minValue, destAddr);
                for (int i = 0; i < count - 1; i++) {
                    long actual = Unsafe.getUnsafe().getLong(destAddr + (long) i * Long.BYTES);
                    Assert.assertEquals(
                            "BitpackUtils.unpackValuesFrom corrupted value at startIndex+" + i
                                    + " (bw=63, skipBits=7, target=70 exceeds 64-bit buffer)",
                            values[1 + i], actual);
                }
            } finally {
                Unsafe.free(valuesAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(packedAddr, packedBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destAddr, (long) count * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Review finding #6 — {@code PostingIndexWriter.truncate()} leaves the
     * writer in a half-open state when the new {@code valueMem.of(...)}
     * fails after {@code closeSidecarMems()} and {@code valueMem.close(...)}
     * already ran (PostingIndexWriter.java:1238-1279).
     * <p>
     * Currently passes because the {@code WriterPool} recreates a fresh
     * {@code TableWriter} after the failed TRUNCATE — the half-open state
     * exists inside the failed {@code PostingIndexWriter} but is not
     * reachable from SQL. To exercise the bug a caller must hold a
     * {@code TableWriter} reference across the truncate failure and keep
     * using it directly. Left here as documentation; the SQL-level test
     * confirms the cleanup at the table layer works.
     * <p>
     * Several methods (e.g. {@code flushAllPending}, {@code seal}) read
     * {@code valueMem.addressOf(...)} without checking
     * {@code valueMem.isOpen()}. After a failed truncate, calling those
     * methods produces a SIGSEGV / NPE instead of a clean error.
     * <p>
     * The fault-injection FilesFacade fails {@code openRW} on the new
     * {@code .pv.{N}} file the truncate would create, so the fresh
     * {@code valueMem.of(...)} throws inside truncate. The test then
     * exercises the writer via the engine again — if the writer is in a
     * coherent state the engine sees a CairoException; if it's
     * half-open, the test surfaces the symptom.
     */
    @Test
    public void testReviewFinding6_TruncatePvOpenFailureDoesNotLeaveHalfOpenWriter() throws Exception {
        final AtomicBoolean failArmed = new AtomicBoolean(false);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failArmed.get() && name != null && Utf8s.containsAscii(name, ".pv.")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_truncate_half (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_truncate_half VALUES
                    ('2024-01-01T00:00:00', 'A'),
                    ('2024-01-01T01:00:00', 'B')
                    """);
            engine.releaseAllWriters();

            failArmed.set(true);
            try {
                // TRUNCATE goes through TableWriter which forwards to
                // PostingIndexWriter.truncate() per indexed column. The
                // .pv reopen inside truncate fails — the writer must
                // either fully back out (best) or mark itself distressed.
                // What it must NOT do is leave valueMem closed while
                // keyMem is still open and accept further writes.
                try {
                    execute("TRUNCATE TABLE t_truncate_half");
                } catch (Throwable expected) {
                    // expected
                }

                // Now try to insert again. With a clean failure path, the
                // engine either rejects the call (writer distressed) or
                // accepts it (writer fully recovered). With the bug, the
                // writer's flushAllPending hits a closed valueMem and
                // throws something internal (NPE / Unsafe assertion).
                try {
                    execute("""
                            INSERT INTO t_truncate_half VALUES
                            ('2024-01-02T00:00:00', 'C')
                            """);
                } catch (io.questdb.cairo.CairoException expected) {
                    // OK — this is the clean failure mode.
                } catch (NullPointerException | AssertionError leaked) {
                    Assert.fail(
                            "writer left in half-open state after truncate failure: "
                                    + leaked.getClass().getName() + ": " + leaked.getMessage()
                    );
                }
            } finally {
                failArmed.set(false);
            }
        });
    }

    /**
     * Review finding #8 — {@code SqlCodeGenerator} line 7666-7667 only
     * checks {@code hasNoIndexHint(model)}, not {@code hasNoCoveringHint(model)},
     * when picking the {@code PostingIndexDistinctRecordCursorFactory} fast
     * path. So {@code /*+ no_covering * / SELECT DISTINCT sym FROM t}
     * still routes through the posting-distinct factory even though the
     * user explicitly opted out of the covering paths.
     * <p>
     * Asserts on {@code factory.toPlan(...)} text: the default DISTINCT
     * plan must include {@code "PostingIndex"} + {@code "distinct"}; the
     * hinted version must not.
     */
    @Test
    public void testReviewFinding8_NoCoveringHintDisablesPostingDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_hint (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_hint VALUES
                    ('2024-01-01T00:00:00', 'A'),
                    ('2024-01-01T01:00:00', 'B'),
                    ('2024-01-01T02:00:00', 'A')
                    """);
            engine.releaseAllWriters();

            // Baseline: default DISTINCT must use the posting-distinct
            // fast path. This locks the symptom — if the codegen ever
            // stops dispatching to PostingIndexDistinct, the red test
            // becomes a false negative.
            try (RecordCursorFactory factory = select(
                    "SELECT DISTINCT sym FROM t_dist_hint")) {
                planSink.clear();
                factory.toPlan(planSink);
                String defaultPlan = planSink.getSink().toString();
                Assert.assertTrue(
                        "default DISTINCT must use PostingIndex distinct, got:\n" + defaultPlan,
                        defaultPlan.contains("PostingIndex") && defaultPlan.contains("distinct")
                );
            }

            // The hinted query must NOT use PostingIndex distinct.
            try (RecordCursorFactory factory = select(
                    "SELECT /*+ no_covering */ DISTINCT sym FROM t_dist_hint")) {
                planSink.clear();
                factory.toPlan(planSink);
                String hintedPlan = planSink.getSink().toString();
                Assert.assertFalse(
                        "/*+ no_covering */ must disable PostingIndex distinct, got:\n" + hintedPlan,
                        hintedPlan.contains("PostingIndex") && hintedPlan.contains("distinct")
                );
            }

            // Result correctness must match across paths.
            assertSql(
                    "sym\nA\nB\n",
                    "SELECT DISTINCT sym FROM t_dist_hint ORDER BY sym"
            );
            assertSql(
                    "sym\nA\nB\n",
                    "SELECT /*+ no_covering */ DISTINCT sym FROM t_dist_hint ORDER BY sym"
            );
        });
    }

    // -------------------------------------------------------------------------
    // Findings that cannot be expressed as JUnit red tests with the available
    // injection surface:
    //
    // Finding #2 — PostingSealPurgeJob.processInQueue queue slot leak on
    //   taskPool.pop() failure. The pool is private to the job and the
    //   throw-on-OOM path requires injecting an exception inside SimpleObjectPool,
    //   which has no FilesFacade hook. A test would need to subclass the job
    //   and override taskPool, which couples the test to private internals.
    //   Code review verification only.
    //
    // Finding #5 — Unsafe.realloc dangling pointer in spillKey / flushAllPending.
    //   Triggering this requires injecting an OOM into Unsafe.realloc, which
    //   the test framework does not expose. The structural defect (no
    //   address/capacity zero-out before the realloc call) is a static-analysis
    //   finding; the only way to red-test it is via a dedicated mock Unsafe,
    //   which would not exercise the real code path.
    // -------------------------------------------------------------------------
}
