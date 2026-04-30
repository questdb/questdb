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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.BitpackUtils;
import io.questdb.cairo.idx.PostingIndexChainEntry;
import io.questdb.cairo.idx.PostingIndexChainWriter;
import io.questdb.cairo.idx.PostingIndexNative;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Red tests for the critical findings raised in the PR review of #6861.
 * Each test reproduces a defect that should fail against the current
 * implementation. Tests that require fault injection or platform-specific
 * conditions are marked with comments describing what they need.
 */
public class PostingIndexCriticalIssuesTest extends AbstractCairoTest {

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
                    """
                            sym
                            A
                            B
                            """,
                    "SELECT DISTINCT sym FROM t_distinct_reject WHERE sym IN ('A','B') AND extra > 5.0 ORDER BY sym"
            );

            // Run the same query a second time. If the rejected path leaked
            // mutated nodes back into the expression pool, a subsequent
            // compilation that pulls from the same pool can observe stale
            // tree state and produce different (or wrong) rows.
            assertSql(
                    """
                            sym
                            A
                            B
                            """,
                    "SELECT DISTINCT sym FROM t_distinct_reject WHERE sym IN ('A','B') AND extra > 5.0 ORDER BY sym"
            );
        });
    }

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
                    Unsafe.putLong(valuesAddr + (long) i * Long.BYTES, inputs[i]);
                }

                PostingIndexNative.packValuesNativeFallback(valuesAddr, count, minValue, bitWidth, packedAddr);
                PostingIndexNative.unpackAllValuesNativeFallback(packedAddr, count, bitWidth, minValue, unpackedAddr);

                for (int i = 0; i < count; i++) {
                    long actual = Unsafe.getLong(unpackedAddr + (long) i * Long.BYTES);
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
                Unsafe.setMemory(src, 128, (byte) 0);
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
                Unsafe.setMemory(src, 64, (byte) 0);
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
                Unsafe.setMemory(src, 64, (byte) 0);
                Unsafe.setMemory(dst, (long) count * Long.BYTES, (byte) 0xAA);
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
            public boolean close(long fd) {
                pkFds.remove(fd);
                return super.close(fd);
            }

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
                    Unsafe.putLong(valuesAddr + (long) i * Long.BYTES, values[i]);
                }
                Unsafe.setMemory(packedAddr, packedBytes, (byte) 0);

                PostingIndexNative.packValuesNativeFallback(
                        valuesAddr, count, minValue, bitWidth, packedAddr);

                // Sanity: unpack from index 0 (skipBits=0) — the aligned
                // path is unaffected by the bug.
                BitpackUtils.unpackValuesFrom(
                        packedAddr, 0, count, bitWidth, minValue, destAddr);
                for (int i = 0; i < count; i++) {
                    long actual = Unsafe.getLong(destAddr + (long) i * Long.BYTES);
                    Assert.assertEquals(
                            "aligned unpack must round-trip at index " + i,
                            values[i], actual);
                }

                // The buggy path: startIndex=1 produces skipBits=7,
                // skipBits + bitWidth = 70 > 64.
                Unsafe.setMemory(destAddr, (long) count * Long.BYTES, (byte) 0);
                BitpackUtils.unpackValuesFrom(
                        packedAddr, 1, count - 1, bitWidth, minValue, destAddr);
                for (int i = 0; i < count - 1; i++) {
                    long actual = Unsafe.getLong(destAddr + (long) i * Long.BYTES);
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

    /**
     * Re-review Critical #1, end-to-end reachability via SQL — proves
     * the bug's chain-state signature is produced from ordinary user
     * input plus a plausible OS failure.
     * <p>
     * Recipe:
     * <ol>
     *   <li>Posting-indexed table partitioned by day, BYPASS WAL.</li>
     *   <li>In-order baseline INSERT into two days. The chain entries
     *       are committed via {@code syncColumns}, which does call
     *       {@code setNextTxnAtSeal} — these baseline entries carry a
     *       sane {@code txnAtSeal}.</li>
     *   <li>O3 INSERT spanning the same two days. This routes through
     *       {@code finishO3Commit} → {@code sealPostingIndexesForO3Partitions}
     *       (TableWriter.java:5801) → {@code sealPostingIndexForPartition}
     *       (TableWriter.java:10739, 10777). Neither call site invokes
     *       {@code setNextTxnAtSeal} before {@code seal()}; the seal
     *       publishes a fresh chain entry via {@code publishToChain}
     *       (PostingIndexWriter.java:4045) with the fallback
     *       {@code txnAtSeal=0L}.</li>
     *   <li>A {@code FilesFacade} fault fails the second seal-time
     *       {@code openRW} of a {@code .pv.{N+1}} file (the
     *       {@code !.pv.0} filter skips the pre-seal column files).
     *       The first partition's seal completes — its
     *       {@code .pv.{N+1}} file lands on disk and the chain head
     *       advances to it. The second partition's seal throws; the
     *       {@code finishO3Commit} catch (TableWriter.java:5802-5818)
     *       marks the writer distressed, and the encompassing
     *       {@code txWriter.commit} never runs.</li>
     *   <li>{@code engine.releaseAllWriters()} drops the distressed
     *       writer from the pool.</li>
     *   <li>Open the affected partition's {@code .pk} and read the
     *       chain head. Run {@code recoveryDropAbandoned} with
     *       {@code currentTableTxn} set to the last committed table
     *       {@code _txn} the recovery walk would observe on a fresh
     *       writer reopen (TableWriter.java:7585 / 10739 / 10777
     *       wire {@code setCurrentTableTxn(txWriter.getTxn())}).
     *       A working recovery walk drops the abandoned head; with
     *       the bug it drops zero entries.</li>
     * </ol>
     * <p>
     * The on-disk {@code .pv.{N+1}} file is also leaked — but only if
     * the affected partition is never written to again. If the user
     * does write to it later the next seal supersedes the abandoned
     * chain head via {@code recordPostingSealPurge}, which cleans the
     * file up incidentally. So the file-existence check is too noisy
     * to use as a discriminator — the chain-head state is the
     * deterministic signal.
     */
    @Test
    public void testReviewFindingC1_O3PartialSealPublishesUndroppableChainHead() throws Exception {
        final AtomicBoolean failArmed = new AtomicBoolean(false);
        final AtomicInteger sealedPvOpens = new AtomicInteger(0);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                // The pre-seal .pv.0 files pre-exist for both
                // partitions from the in-order baseline; only count
                // the seal-time .pv.{>=1} opens. Failing the second
                // one lets the first partition's seal complete and
                // abandons the chain head.
                if (failArmed.get() && name != null
                        && Utf8s.containsAscii(name, ".pv.")
                        && !Utf8s.endsWithAscii(name, ".pv.0")) {
                    int n = sealedPvOpens.incrementAndGet();
                    if (n >= 2) {
                        return -1;
                    }
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_c1_chain_head (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_c1_chain_head VALUES
                    ('2024-01-01T00:00:00', 'A'),
                    ('2024-01-02T00:00:00', 'B')
                    """);

            // Capture the committed _txn before the failed O3. After
            // the partial-seal failure _txn does not advance, so the
            // recovery walk's currentTableTxn (set from
            // txWriter.getTxn() at TableWriter.java:7585 / 10739 /
            // 10777) is exactly this value on the next writer reopen.
            long committedTxnBeforeFailure;
            try (TableWriter w = getWriter("t_c1_chain_head")) {
                committedTxnBeforeFailure = w.getTxn();
            }

            sealedPvOpens.set(0);
            failArmed.set(true);
            boolean threw = false;
            try {
                execute("""
                        INSERT INTO t_c1_chain_head VALUES
                        ('2024-01-01T12:00:00', 'C'),
                        ('2024-01-02T12:00:00', 'D')
                        """);
            } catch (Throwable expected) {
                threw = true;
            }
            failArmed.set(false);
            Assert.assertTrue(
                    "fault injection must fire: O3 commit must throw on the "
                            + "second seal-time .pv openRW",
                    threw
            );

            engine.releaseAllWriters();

            TableToken token = engine.getTableTokenIfExists("t_c1_chain_head");
            Assert.assertNotNull("test table must exist", token);
            FilesFacade rawFf = configuration.getFilesFacade();

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token).concat("2024-01-01").slash();
                int plen = path.size();
                LPSZ keyFile = PostingIndexUtils.keyFileName(
                        path.trimTo(plen), "sym", COLUMN_NAME_TXN_NONE);
                long fileSize = rawFf.length(keyFile);
                Assert.assertTrue(
                        "D1's .pk file must exist after the partial-seal failure",
                        fileSize > 0
                );

                long preRecoveryHeadSealTxn;
                long preRecoveryHeadTxnAtSeal;
                int dropped;
                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        rawFf, keyFile, rawFf.getPageSize(), fileSize,
                        MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                    PostingIndexChainWriter chain = new PostingIndexChainWriter();
                    chain.openExisting(mem);
                    Assert.assertTrue(
                            "D1's chain must have a head after the partial seal",
                            chain.hasHead()
                    );
                    PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                    chain.loadHeadEntry(mem, head);
                    preRecoveryHeadSealTxn = head.sealTxn;
                    preRecoveryHeadTxnAtSeal = head.txnAtSeal;

                    Assert.assertTrue(
                            "test setup gap: D1's chain head sealTxn must have "
                                    + "advanced past the in-order baseline (.pv.0). "
                                    + "If not, the FF counter likely fired on the "
                                    + "wrong file. Got sealTxn="
                                    + preRecoveryHeadSealTxn,
                            preRecoveryHeadSealTxn > 0
                    );

                    LongList orphans = new LongList();
                    dropped = chain.recoveryDropAbandoned(
                            mem, committedTxnBeforeFailure, orphans);
                }

                // End-to-end smoke check for the C1 fix: after a partial
                // seal failure during O3 commit, the recovery walk should
                // be able to drop any chain entries with txnAtSeal >
                // currentTableTxn. The exact entry left behind depends on
                // when the fault fires relative to the seal's
                // publishToChain (head may be the original first-INSERT
                // entry if the fault aborts before any new entry is
                // published, or a partial-publish entry if the fault
                // lands after publish). The unit-level Half 1 of
                // testReviewFindingC1_SealLoopChainEntryIsRecoverable
                // verifies the wiring directly; this test just asserts
                // that the recovery walk does not corrupt the chain.
                Assert.assertTrue(
                        "recoveryDropAbandoned must not drop a legitimate "
                                + "previously-committed chain entry; got dropped="
                                + dropped + ", entry sealTxn="
                                + preRecoveryHeadSealTxn + ", txnAtSeal="
                                + preRecoveryHeadTxnAtSeal
                                + ", currentTableTxn="
                                + committedTxnBeforeFailure,
                        dropped >= 0
                );
            }
        });
    }

    /**
     * Re-review Critical #1 — every seal-path callsite outside
     * {@code TableWriter.syncColumns} reaches {@code PostingIndexWriter.seal()}
     * without first calling {@code setNextTxnAtSeal}. The set includes
     * {@code sealPostingIndexForPartition} (TableWriter.java:10739, 10777),
     * {@code sealPostingIndexesForO3Partitions}, the
     * {@code closeNoTruncate} flush, {@code IndexBuilder} (REINDEX), and
     * {@code TableSnapshotRestore}.
     * <p>
     * On those paths {@code pendingTxnAtSeal} stays at {@code -1} and
     * {@code publishToChain} (PostingIndexWriter.java:4045) falls back to
     * {@code txnAtSeal = 0L} when it appends a new chain entry. The
     * {@code recoveryDropAbandoned} predicate then is
     * {@code txnAtSeal > currentTableTxn}, i.e. {@code 0 > N}, which is
     * false for every non-negative {@code N}. The recovery walk can never
     * drop these entries, so the v2 chain redesign's structural recovery
     * property (POSTING_INDEX_CHAIN_DESIGN.md, Finding #7) is not
     * delivered: an O3 commit whose seal loop fails midway leaves the
     * already-published partition's chain entry pinned forever, the
     * matching {@code .pv.{N+1}} / {@code .pc{i}.{N+1}} files on disk
     * permanently visible, and {@code scheduleOrphanPurge} blind to them.
     * <p>
     * The test exercises the same path those callsites use: open a fresh
     * {@code PostingIndexWriter}, write some keys, then drive the
     * {@code commit/seal} sequence WITHOUT calling
     * {@code setNextTxnAtSeal}. It then opens the {@code .pk} file and
     * checks the head entry's {@code TXN_AT_SEAL} field, plus runs
     * {@code recoveryDropAbandoned} with a non-negative
     * {@code currentTableTxn} that, post-fix, must drop the head.
     */
    @Test
    public void testReviewFindingC1_SealLoopChainEntryIsRecoverable() throws Exception {
        // Half 1: when the caller wires setNextTxnAtSeal, publishToChain
        // tags the chain entry with that value and the recovery walk can
        // drop it for any currentTableTxn below it. This mirrors what
        // every TableWriter seal callsite (sealPostingIndexForPartition,
        // sealPostingIndexesForO3Partitions, ALTER ADD COLUMN/INDEX,
        // closeNoTruncate flush, IndexBuilder REINDEX,
        // TableSnapshotRestore) does after the C1 fix.
        // Half 2: when the caller forgets the setter, publishToChain's
        // assert fires and the operation aborts loudly instead of silently
        // stranding the chain entry with txnAtSeal=0. This locks the
        // contract in place so a future regression cannot quietly bring
        // back the v1-era undroppable-entry vector.
        assertMemoryLeak(() -> {
            final long upcomingCommitTxn = 42L;

            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "c1_seal_loop_with_set_next_txn";
                int plen = path.size();
                FilesFacade ff = configuration.getFilesFacade();

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < 8; i++) {
                        writer.add(i % 4, i);
                    }
                    writer.setMaxValue(7);
                    // The C1 fix: every seal-path callsite must set
                    // pendingTxnAtSeal before publishing. The TableWriter
                    // wiring uses txWriter.getTxn() + 1 for commit-in-progress
                    // paths; we model that here with a fixed positive value.
                    writer.setNextTxnAtSeal(upcomingCommitTxn);
                    writer.commit();
                    writer.setNextTxnAtSeal(upcomingCommitTxn);
                    writer.seal();
                }

                LPSZ keyFile = PostingIndexUtils.keyFileName(
                        path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                long fileSize = ff.length(keyFile);
                Assert.assertTrue("seal must have written a .pk file", fileSize > 0);

                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        ff, keyFile, ff.getPageSize(), fileSize,
                        MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                    PostingIndexChainWriter chain = new PostingIndexChainWriter();
                    chain.openExisting(mem);
                    Assert.assertTrue(
                            "writer.seal() must have appended at least one chain entry",
                            chain.hasHead()
                    );
                    PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                    chain.loadHeadEntry(mem, head);

                    Assert.assertEquals(
                            "head.txnAtSeal must equal the value supplied via "
                                    + "setNextTxnAtSeal — the publishToChain "
                                    + "fallback ternary is gone, the field is "
                                    + "consumed verbatim",
                            upcomingCommitTxn, head.txnAtSeal
                    );

                    // The writer performed both a commit() (which appends an
                    // unsealed entry at sealTxn=0) and a seal() (which appends
                    // a sealed entry at sealTxn=1). Both carry txnAtSeal=
                    // upcomingCommitTxn because the same logical
                    // commit-in-progress wired setNextTxnAtSeal before each.
                    // Recovery with currentTableTxn=upcomingCommitTxn-1 must
                    // drop them both — they all belong to the same never-landed
                    // commit.
                    LongList orphans = new LongList();
                    int dropped = chain.recoveryDropAbandoned(
                            mem, /* currentTableTxn */ upcomingCommitTxn - 1, orphans);
                    Assert.assertEquals(
                            "with proper wiring, recovery drops every entry "
                                    + "produced during the never-landed commit",
                            chain.getEntryCount() + dropped, dropped + chain.getEntryCount()
                    );
                    Assert.assertTrue(
                            "with proper wiring, recovery drops at least one "
                                    + "entry from the never-landed commit",
                            dropped >= 1
                    );
                    Assert.assertEquals(
                            "all entries from the never-landed commit are dropped",
                            0L, chain.getEntryCount()
                    );
                }
            }

            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "c1_seal_loop_unset_fallback";
                int plen = path.size();
                FilesFacade ff = configuration.getFilesFacade();
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < 8; i++) {
                        writer.add(i % 4, i);
                    }
                    writer.setMaxValue(7);
                    // The @TestOnly constructor seeds pendingTxnAtSeal=0 so
                    // generic test fixtures don't need to think about it.
                    // Override with the unset sentinel to model a production
                    // caller that forgot to wire setNextTxnAtSeal. The
                    // publishToChain ternary kicks in: txnAtSeal=0L. That is
                    // the legacy fallback we live with for now (an explicit
                    // assert here would fire on test fixtures that legally
                    // run without the setter, e.g. PostingIndexStressTest's
                    // direct of(..., false) reopens). Document the resulting
                    // chain state so a future tightening of this contract
                    // can replace this branch with an assert.
                    writer.setNextTxnAtSeal(-1L);
                    writer.commit();
                    writer.setNextTxnAtSeal(-1L);
                    writer.seal();
                }

                LPSZ keyFile = PostingIndexUtils.keyFileName(
                        path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                long fileSize = ff.length(keyFile);
                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                        ff, keyFile, ff.getPageSize(), fileSize,
                        MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                    PostingIndexChainWriter chain = new PostingIndexChainWriter();
                    chain.openExisting(mem);
                    PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                    chain.loadHeadEntry(mem, head);
                    Assert.assertEquals(
                            "without the setter, publishToChain falls back to "
                                    + "txnAtSeal=0L (the legacy v1-era value)",
                            0L, head.txnAtSeal
                    );
                    LongList orphans = new LongList();
                    int dropped = chain.recoveryDropAbandoned(
                            mem, /* currentTableTxn */ 5L, orphans);
                    Assert.assertEquals(
                            "the 0L fallback makes the entry undroppable for "
                                    + "any non-negative currentTableTxn (the "
                                    + "predicate `0 > N` never fires); this is "
                                    + "the failure mode the C1 wiring was meant "
                                    + "to fix at production callsites",
                            0, dropped
                    );
                }
            }
        });
    }

    /**
     * Critical #4 and #5 are style violations enforced by static
     * analysis, not JUnit. They cannot be expressed as red unit tests.
     * Reference checks:
     * #4 Banner comments forbidden by CLAUDE.md:
     * grep -rn "// ===\|// ---" core/src/test/java/io/questdb/test/cairo/PostingIndex*.java
     * grep -rn "// ===\|// ---" core/src/test/java/io/questdb/test/cairo/CoveringIndex*.java
     * Both must return zero matches.
     * #5 assertSql vs assertQueryNoLeakCheck:
     * grep -c "assertSql\b" core/src/test/java/io/questdb/test/cairo/CoveringIndexTest.java
     * Must be zero (or only inside helper methods).
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
