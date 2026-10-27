/*******************************************************************************
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

package io.questdb.test.cairo.sql;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * A late-materialized Parquet frame (navigateTo with the filter's column subset) leaves the
 * columns outside the subset undecoded until the filter has run. Those columns are not column
 * tops: the compiled filter reads only the filter's columns, so {@link PageFrameMemory#hasColumnTops()}
 * must judge the decoded subset only. Before, every undecoded column counted as a column top, so
 * every async factory skipped its compiled filter on every late-materialized Parquet frame and ran
 * the Java filter instead.
 * <p>
 * Because a partial decode is now trusted by the compiled filter, it must never be served to a
 * caller that needs more: a full-frame navigate, another column subset, or a subset navigate after a
 * partial row window must each get the columns and rows they read.
 */
public class PageFrameMemoryLateMaterializationTest extends AbstractCairoTest {

    @Test
    public void testBudgetedPoolAccountsSubsetRedecodes() throws Exception {
        // A subset re-decode maps other columns to the buffer's decode slots. Each slot keeps the
        // peak capacity of every column it has held, so the cache must account those peaks, not
        // the bytes of the latest decode only.
        assertFrame(1L << 30, (pool, ref, p, metadata, frameRows) -> {
            final int qty = metadata.getColumnIndex("qty");
            final int note = metadata.getColumnIndex("note");
            final long tagBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            final IntHashSet noteOnly = set(note);
            final IntHashSet qtyAndNote = set(qty, note);
            long previous = 0;
            for (IntHashSet subset : new IntHashSet[]{noteOnly, qtyAndNote, noteOnly, null}) {
                final PageFrameMemory memory = subset != null ? pool.navigateTo(p, subset) : pool.navigateTo(p);
                if (subset != qtyAndNote) {
                    assertNotEquals(0, memory.getAuxPageAddress(note));
                }
                final long cached = pool.getCachedBytes();
                final long allocated = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER) - tagBefore;
                assertTrue("the accounted bytes never shrink while the buffer holds its memory", cached >= previous);
                // Rust vectors may over-allocate, so allow a little slack, but no under-count of a
                // whole column's slot
                assertTrue(
                        "cached bytes " + cached + " under-count the decoder's allocation " + allocated,
                        cached >= allocated * 9 / 10
                );
                previous = cached;
            }
            assertEquals(1, pool.getCachedFrameCount());
        });
    }

    @Test
    public void testBudgetedPoolReusesSubsetBuffersAcrossFrames() throws Exception {
        // a budgeted pool keeps a frame's subset decode while the frame memory visits another frame,
        // then must serve each later request from it, or re-decode it, by what it holds
        assertFrames(1L << 30, (pool, ref, frames, metadata, addressCache) -> {
            final int sym = metadata.getColumnIndex("sym");
            final int qty = metadata.getColumnIndex("qty");
            final int p0 = frames.getQuick(0);
            final int p1 = frames.getQuick(1);
            final IntHashSet symOnly = set(sym);
            final IntHashSet symAndQty = set(sym, qty);

            assertEquals(0, pool.navigateTo(p0, symOnly).getPageAddress(qty));
            assertEquals(0, pool.navigateTo(p1, symOnly).getPageAddress(qty));
            assertEquals(2, pool.getCachedFrameCount());

            // p0's cached {sym} decode lacks qty: re-decode it
            PageFrameMemory memory = pool.navigateTo(p0, symAndQty);
            assertQtyEquals(ref.navigateTo(p0), memory, qty, (int) addressCache.getFrameSize(p0));
            final long symAddress = memory.getPageAddress(sym);
            final long qtyAddress = memory.getPageAddress(qty);

            // p1's cached {sym} decode cannot serve a full-frame navigate
            memory = pool.navigateTo(p1);
            assertFalse(memory.hasColumnTops());
            assertQtyEquals(ref.navigateTo(p1), memory, qty, (int) addressCache.getFrameSize(p1));

            // p0's {sym, qty} decode is a superset of {sym}: served without a re-decode
            final long generation = pool.getBindGeneration();
            memory = pool.navigateTo(p0, symOnly);
            assertEquals(symAddress, memory.getPageAddress(sym));
            assertEquals(qtyAddress, memory.getPageAddress(qty));
            assertEquals(generation, pool.getBindGeneration());
            assertFalse(memory.hasColumnTops());
            assertEquals(2, pool.getCachedFrameCount());
        });
    }

    @Test
    public void testBudgetedPoolVictimReuseDropsColumnSubset() throws Exception {
        // MONOTONIC caps the cache at 4 buffers, so later frames reuse the buffers of earlier subset
        // decodes in place; a reused buffer must serve its new frame in full
        assertFrames(1L << 30, ParquetDecodeHint.MONOTONIC, (pool, ref, frames, metadata, addressCache) -> {
            final int sym = metadata.getColumnIndex("sym");
            final int qty = metadata.getColumnIndex("qty");
            final IntHashSet symOnly = set(sym);
            assertTrue("expected more Parquet frames than cached buffers", frames.size() > 4);
            for (int i = 0, n = frames.size(); i < n; i++) {
                assertEquals(0, pool.navigateTo(frames.getQuick(i), symOnly).getPageAddress(qty));
            }
            assertTrue(pool.getCachedFrameCount() <= 4);
            for (int i = 0, n = frames.size(); i < n; i++) {
                final int p = frames.getQuick(i);
                final PageFrameMemory memory = pool.navigateTo(p);
                assertFalse(memory.hasColumnTops());
                assertQtyEquals(ref.navigateTo(p), memory, qty, (int) addressCache.getFrameSize(p));
            }
        });
    }

    @Test
    public void testColumnAddedAfterConversionIsColumnTopInSubset() throws Exception {
        assertMemoryLeak(() -> {
            createAndConvert();
            execute("ALTER TABLE t ADD COLUMN extra INT");
            try (RecordCursorFactory factory = select("SELECT * FROM t");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 PageFrameAddressCache addressCache = new PageFrameAddressCache();
                 PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L)) {
                final int frameCount = fill(factory, cursor, addressCache);
                pool.of(addressCache);
                final RecordMetadata metadata = factory.getMetadata();
                final int sym = metadata.getColumnIndex("sym");
                final int extra = metadata.getColumnIndex("extra");

                int parquetFrames = 0;
                for (int i = 0; i < frameCount; i++) {
                    if (addressCache.getFrameFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    parquetFrames++;
                    assertTrue(
                            "a filter column the Parquet file does not hold is a column top",
                            pool.navigateTo(i, set(sym, extra)).hasColumnTops()
                    );
                    // A different frame first, so the next navigateTo decodes afresh.
                    pool.of(addressCache);
                    assertFalse(
                            "the column top lies outside the filter's columns",
                            pool.navigateTo(i, set(sym)).hasColumnTops()
                    );
                    pool.of(addressCache);
                    assertTrue("a full decode still sees the column top", pool.navigateTo(i).hasColumnTops());
                    pool.of(addressCache);
                }
                assertTrue("expected a Parquet frame", parquetFrames > 0);
            }
        });
    }

    @Test
    public void testCompiledFilterMatchesJavaFilterOnLateMaterializedFrames() throws Exception {
        // Small frames and row groups, so the selectivity stats see enough frames to switch late
        // materialization on, and the compiled filter now runs on those frames.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createAndConvert();
            execute("ALTER TABLE t ADD COLUMN extra INT");
            execute("""
                    INSERT INTO t (ts, sym, ex, price, qty, note, extra)
                    SELECT '2024-01-03T03:00:00'::TIMESTAMP + x * 60_000_000L, 'B', 'L', x, x, 'n' || x, x::INT
                    FROM long_sequence(200)
                    """);
            final String[] queries = {
                    // selective filters: late materialization on the Parquet partition
                    "SELECT * FROM t WHERE sym = 'A' AND ex = 'K' AND qty > 1200",
                    "SELECT ts, note, extra FROM t WHERE qty >= 700 AND qty <= 760",
                    "SELECT ts, price FROM t WHERE ex IN ('K', 'z1', 'z2', 'z3', 'z4', 'z5', 'z6', 'z7', 'z8', 'z9', 'z10') AND price < 0.05",
                    // a filter column the Parquet partition does not hold (a column top): the Java filter
                    "SELECT ts, qty, extra FROM t WHERE extra IS NULL AND qty < 40",
                    "SELECT ts, qty FROM t WHERE extra > 150",
                    "SELECT count() FROM t WHERE sym = 'C' AND price > 0.97",
                    "SELECT sym, count(), sum(price) FROM t WHERE ex = 'M' AND price < 0.1 ORDER BY sym",
            };
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            for (String query : queries) {
                if (query.startsWith("SELECT ts") && !query.contains(" IN (")) {
                    // the factory under test: a compiled async filter (an over-threshold IN list
                    // compiles only with a native library that has the symbol IN set opcode)
                    sink.clear();
                    printSql("EXPLAIN " + query, sink);
                    TestUtils.assertContains(sink, "Async JIT Filter");
                }
            }
            final StringSink expected = new StringSink();
            for (String query : queries) {
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                expected.clear();
                printSql(query, expected);
                for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR}) {
                    sqlExecutionContext.setJitMode(mode);
                    // twice: the second execution runs with the selectivity stats of the first
                    for (int run = 0; run < 2; run++) {
                        sink.clear();
                        printSql(query, sink);
                        TestUtils.assertEquals(query + " [jit mode " + mode + ", run " + run + "]", expected, sink);
                    }
                }
            }
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        });
    }

    @Test
    public void testCompiledFilterRunsOnLateMaterializedFrames() throws Exception {
        // With JIT null checks off, "i < 0" matches INT NULL (Integer.MIN_VALUE) under the compiled
        // filter but not under the Java filter. So on the Parquet partition the compiled filter's
        // answer equals the Java answer of "(i < 0 OR i IS NULL)" only if the compiled filter ran on
        // every frame, late-materialized ones included. Before the fix, late-materialized frames
        // fell back to the Java filter and dropped the NULL rows.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG, i INT
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 30_000_000L,
                        rnd_symbol('A', 'B', 'C'),
                        rnd_double(),
                        x,
                        rnd_int(-5, 100, 3)
                    FROM long_sequence(3_000)
                    """);
            // 3000 rows * 30s = 25h: day 1 is Parquet, a sliver of day 2 stays native
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            execute("CREATE TABLE u (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO u
                    SELECT '2024-01-01T00:00:00'::TIMESTAMP + x * 30_000_000L, rnd_symbol('A', 'B', 'C'), rnd_double()
                    FROM long_sequence(6_000)
                    """);
            drainWalQueue();

            // {plan factory, query}; $P is the predicate, every query reads the Parquet partition only
            final String[][] shapes = {
                    // count-only: always late-materialized on Parquet
                    {"Async JIT Filter", "SELECT count() FROM t WHERE ts IN '2024-01-01' AND $P AND t.qty < 300"},
                    {"Async JIT Filter", "SELECT * FROM t WHERE ts IN '2024-01-01' AND $P AND t.qty < 300"},
                    {"Async JIT Group By", "SELECT sym, count(), sum(qty) FROM t WHERE ts IN '2024-01-01' AND $P AND t.qty < 300 ORDER BY sym"},
                    {"Async JIT Top K", "SELECT * FROM t WHERE ts IN '2024-01-01' AND $P AND t.qty < 300 ORDER BY price DESC LIMIT 1000"},
                    {"Async JIT Horizon Join", "SELECT h.offset, count() c FROM t HORIZON JOIN u ON (sym) RANGE FROM 0s TO 60s STEP 30s AS h"
                            + " WHERE t.ts IN '2024-01-01' AND $P AND t.qty < 300 GROUP BY h.offset ORDER BY h.offset"},
                    {"Async JIT Horizon Join", "SELECT t.sym, h.offset, count() c FROM t HORIZON JOIN u ON (sym) RANGE FROM 0s TO 60s STEP 30s AS h"
                            + " WHERE t.ts IN '2024-01-01' AND $P AND t.qty < 300 GROUP BY t.sym, h.offset ORDER BY t.sym, h.offset"},
            };
            final StringSink mismatches = new StringSink();
            for (String[] shape : shapes) {
                final String jitQuery = shape[1].replace("$P", "t.i < 0");
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                final String javaPlain = runSql(jitQuery, true);
                final String javaWithNulls = runSql(shape[1].replace("$P", "(t.i < 0 OR t.i IS NULL)"), true);
                assertNotEquals("the Parquet partition must hold INT NULLs in i: " + jitQuery, javaPlain, javaWithNulls);

                for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR}) {
                    sqlExecutionContext.setJitMode(mode);
                    sink.clear();
                    printSql("EXPLAIN " + jitQuery, sink);
                    TestUtils.assertContains(sink, shape[0]);
                    // several runs: the later ones run with the selectivity stats of the earlier ones,
                    // which switch late materialization on for the filtered (non count-only) shapes
                    for (int run = 0; run < 3; run++) {
                        if (!javaWithNulls.equals(runSql(jitQuery, false))) {
                            mismatches.put(jitQuery).put(" [jit mode ").put(mode).put(", run ").put(run).put("]\n");
                        }
                    }
                }
            }
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertEquals("the compiled filter dropped INT NULLs (it did not run) on:\n" + mismatches, 0, mismatches.length());
        });
    }

    @Test
    public void testFullDecodeReusedBySubsetIsNotPopulated() throws Exception {
        // A full decode serves a subset navigate as is. Populating its remaining columns for the
        // filtered rows only would hollow it out, and a later full-frame navigate would serve it.
        assertLateMaterializedFrame((pool, ref, p, sym, qty, frameRows) -> {
            pool.navigateTo(p);
            final IntHashSet filterColumns = set(sym);
            final PageFrameMemory memory = pool.navigateTo(p, filterColumns);
            assertNotEquals("precondition: the full decode serves the subset", 0, memory.getPageAddress(qty));
            final boolean populated;
            try (DirectLongList rows = new DirectLongList(4, MemoryTag.NATIVE_DEFAULT)) {
                rows.add(0);
                rows.add(frameRows / 2);
                populated = memory.populateRemainingColumns(filterColumns, rows, true);
            }
            assertQtyEquals(ref.navigateTo(p), pool.navigateTo(p), qty, frameRows);
            assertFalse("every column is decoded already", populated);
        });
    }


    @Test
    public void testFullNavigateAfterLateMaterializationDecodesEveryColumn() throws Exception {
        assertLateMaterializedFrame((pool, ref, p, sym, qty, frameRows) -> {
            final PageFrameMemory partial = pool.navigateTo(p, set(sym));
            assertEquals("precondition: qty is outside the decoded subset", 0, partial.getPageAddress(qty));

            final PageFrameMemory full = pool.navigateTo(p);
            assertNotEquals("a full-frame navigate must not serve the partial decode", 0, full.getPageAddress(qty));
            assertFalse(full.hasColumnTops());
            assertQtyEquals(ref.navigateTo(p), full, qty, frameRows);
        });
    }

    @Test
    public void testFullNavigateAfterPopulateRemainingColumnsRedecodes() throws Exception {
        assertLateMaterializedFrame((pool, ref, p, sym, qty, frameRows) -> {
            final IntHashSet filterColumns = set(sym);
            final PageFrameMemory memory = pool.navigateTo(p, filterColumns);
            try (DirectLongList rows = new DirectLongList(4, MemoryTag.NATIVE_DEFAULT)) {
                rows.add(0);
                rows.add(frameRows / 2);
                assertTrue(memory.populateRemainingColumns(filterColumns, rows, true));
            }
            // every column now has an address: none of them is a column top
            assertFalse(memory.hasColumnTops());

            // the remaining columns hold only the filtered rows; a full-frame navigate must re-decode
            final PageFrameMemory full = pool.navigateTo(p);
            assertQtyEquals(ref.navigateTo(p), full, qty, frameRows);
        });
    }

    @Test
    public void testFullNavigateKeepsBufferOfPinnedRecord() throws Exception {
        // record B pins a populated subset decode; a full-frame navigate must not re-decode it in
        // place, or every raw pointer taken into it (JIT address arrays, varchar views) goes stale
        assertLateMaterializedFrame((pool, ref, p, sym, qty, frameRows) -> {
            final IntHashSet filterColumns = set(sym);
            final PageFrameMemory memory = pool.navigateTo(p, filterColumns);
            populateEveryRow(memory, filterColumns, frameRows);
            final PageFrameMemoryRecord recordB = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
            pool.navigateTo(p, recordB);
            final long symAddress = recordB.getPageAddress(sym);
            final int lastSym = Unsafe.getInt(symAddress + 4L * (frameRows - 1));
            final long qtyAddress = recordB.getPageAddress(qty);

            final PageFrameMemory full = pool.navigateTo(p);
            assertQtyEquals(ref.navigateTo(p), full, qty, frameRows);
            assertNotEquals("the full decode must go to another buffer", symAddress, full.getPageAddress(sym));
            assertEquals(symAddress, recordB.getPageAddress(sym));
            assertEquals(qtyAddress, recordB.getPageAddress(qty));
            assertEquals("record B's memory was overwritten", lastSym, Unsafe.getInt(symAddress + 4L * (frameRows - 1)));
            pool.navigateTo(p, recordB);
            assertRecordQtyEquals(ref.navigateTo(p), recordB, qty, frameRows);
        });
    }

    @Test
    public void testPopulateTwiceOnOneBindingFails() throws Exception {
        // the second call would find the remaining columns populated for the first call's rows
        assertLateMaterializedFrame((pool, ref, p, sym, qty, frameRows) -> {
            final IntHashSet filterColumns = set(sym);
            final PageFrameMemory memory = pool.navigateTo(p, filterColumns);
            try (DirectLongList rows = new DirectLongList(4, MemoryTag.NATIVE_DEFAULT)) {
                rows.add(0);
                assertTrue(memory.populateRemainingColumns(filterColumns, rows, true));
                try {
                    memory.populateRemainingColumns(filterColumns, rows, true);
                    fail("a second populate on one binding must fail");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "late-materialized frame populated twice");
                }
            }
        });
    }

    @Test
    public void testRecordNavigateOnCompactedSubsetDecodesEveryColumn() throws Exception {
        // populateRemainingColumns(fillWithNulls = false) stores the remaining columns in
        // filtered-row order, which a record reading by frame row index cannot use
        assertLateMaterializedFrame((pool, ref, p, sym, qty, frameRows) -> {
            final IntHashSet filterColumns = set(sym);
            final PageFrameMemory memory = pool.navigateTo(p, filterColumns);
            try (DirectLongList rows = new DirectLongList(4, MemoryTag.NATIVE_DEFAULT)) {
                rows.add(1);
                rows.add(frameRows / 2);
                assertTrue(memory.populateRemainingColumns(filterColumns, rows, false));
            }
            final PageFrameMemoryRecord recordB = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
            pool.navigateTo(p, recordB);
            assertRecordQtyEquals(ref.navigateTo(p), recordB, qty, frameRows);
        });
    }

    @Test
    public void testRecordNavigateOnUnpopulatedSubsetDecodesEveryColumn() throws Exception {
        // the record path reuses a late-materialized decode only after populateRemainingColumns();
        // before it, the columns outside the subset have no addresses
        assertLateMaterializedFrame((pool, ref, p, sym, qty, frameRows) -> {
            final PageFrameMemory memory = pool.navigateTo(p, set(sym));
            final long symAddress = memory.getPageAddress(sym);
            assertEquals(0, memory.getPageAddress(qty));
            final PageFrameMemoryRecord recordB = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
            pool.navigateTo(p, recordB);
            assertRecordQtyEquals(ref.navigateTo(p), recordB, qty, frameRows);
            // the frame memory keeps its subset decode
            assertEquals(symAddress, memory.getPageAddress(sym));
        });
    }

    @Test
    public void testRedecodeBumpsBindGeneration() throws Exception {
        // a record bound through init(PageFrameMemory) does not pin the buffer; an in-place
        // re-decode must fail its fast path so it rebinds
        assertLateMaterializedFrame((pool, ref, p, sym, qty, frameRows) -> {
            final PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            record.init(pool.navigateTo(p, set(sym)));
            final long generation = pool.getBindGeneration();
            assertEquals(generation, record.getBoundGeneration());
            pool.navigateTo(p);
            assertNotEquals("the in-place re-decode must bump the bind generation", generation, pool.getBindGeneration());
            pool.navigateTo(p, record);
            assertEquals(pool.getBindGeneration(), record.getBoundGeneration());
            assertRecordQtyEquals(ref.navigateTo(p), record, qty, frameRows);
        });
    }

    @Test
    public void testSubsetNavigateAfterPopulateRedecodes() throws Exception {
        // populateRemainingColumns() rewrote the other columns for the first caller's rows; the next
        // subset binding of the frame must start from a clean subset decode and populate its own rows
        assertLateMaterializedFrame((pool, ref, p, sym, qty, frameRows) -> {
            final IntHashSet filterColumns = set(sym);
            PageFrameMemory memory = pool.navigateTo(p, filterColumns);
            try (DirectLongList rows = new DirectLongList(4, MemoryTag.NATIVE_DEFAULT)) {
                rows.add(0);
                rows.add(frameRows / 2);
                assertTrue(memory.populateRemainingColumns(filterColumns, rows, false));
            }
            memory = pool.navigateTo(p, filterColumns);
            assertEquals("a clean subset decode", 0, memory.getPageAddress(qty));
            try (DirectLongList rows = new DirectLongList(4, MemoryTag.NATIVE_DEFAULT)) {
                rows.add(1);
                rows.add(3);
                assertTrue(memory.populateRemainingColumns(filterColumns, rows, false));
            }
            // compacted: the k-th filtered row is at index k
            final long expected = ref.navigateTo(p).getPageAddress(qty);
            assertEquals(Unsafe.getLong(expected + 8), Unsafe.getLong(memory.getPageAddress(qty)));
            assertEquals(Unsafe.getLong(expected + 24), Unsafe.getLong(memory.getPageAddress(qty) + 8));
        });
    }

    @Test
    public void testSubsetNavigateAfterWindowedDecodeCoversFrame() throws Exception {
        assertLateMaterializedFrame((pool, ref, p, sym, qty, frameRows) -> {
            final PageFrameMemory windowed = pool.navigateTo(p, 0, 10);
            assertTrue("precondition: a partial window", windowed.getPageSize(qty) < 8L * frameRows);

            final PageFrameMemory memory = pool.navigateTo(p, set(sym, qty));
            assertTrue("a subset navigate must not serve a partial window", memory.getPageSize(qty) >= 8L * frameRows);
            assertQtyEquals(ref.navigateTo(p), memory, qty, frameRows);
        });
    }

    @Test
    public void testSubsetNavigateKeepsBufferOfPinnedRecord() throws Exception {
        // record B pins a populated subset decode (Top-K's recordB does); another subset navigate of
        // the frame must decode into a fresh buffer and leave record B's columns in place
        assertLateMaterializedFrame((pool, ref, p, sym, qty, frameRows) -> {
            final IntHashSet symAndQty = set(sym, qty);
            final PageFrameMemory memory = pool.navigateTo(p, symAndQty);
            populateEveryRow(memory, symAndQty, frameRows);
            final PageFrameMemoryRecord recordB = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
            pool.navigateTo(p, recordB);
            final long qtyAddress = recordB.getPageAddress(qty);
            assertNotEquals(0, qtyAddress);

            final PageFrameMemory symOnly = pool.navigateTo(p, set(sym));
            assertNotEquals(0, symOnly.getPageAddress(sym));
            assertEquals("record B lost qty under a re-decode", qtyAddress, recordB.getPageAddress(qty));
            pool.navigateTo(p, recordB);
            assertEquals(qtyAddress, recordB.getPageAddress(qty));
            assertRecordQtyEquals(ref.navigateTo(p), recordB, qty, frameRows);
        });
    }

    @Test
    public void testSubsetNavigateKeepsColumnsOfRecordOnSameFrame() throws Exception {
        // record B navigates to a frame the frame memory holds as a {sym, qty} decode; a {sym}
        // navigate then must neither narrow record B's buffer nor leave record B on a stale binding
        assertLateMaterializedFrame((pool, ref, p, sym, qty, frameRows) -> {
            pool.navigateTo(p, set(sym, qty));
            final PageFrameMemoryRecord recordB = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
            pool.navigateTo(p, recordB);
            final long qtyAddress = recordB.getPageAddress(qty);
            assertNotEquals(0, qtyAddress);

            pool.navigateTo(p, set(sym));
            assertEquals(qtyAddress, recordB.getPageAddress(qty));
            pool.navigateTo(p, recordB);
            assertNotEquals("record B lost qty under an in-place narrowing re-decode", 0, recordB.getPageAddress(qty));
            assertRecordQtyEquals(ref.navigateTo(p), recordB, qty, frameRows);
        });
    }

    @Test
    public void testSubsetNavigateWithAnotherSubsetDecodesIt() throws Exception {
        assertLateMaterializedFrame((pool, ref, p, sym, qty, frameRows) -> {
            final IntHashSet symOnly = set(sym);
            final long symAddress = pool.navigateTo(p, symOnly).getPageAddress(sym);
            assertNotEquals(0, symAddress);
            // the same subset again is served from the bound frame, without a decode
            assertEquals(symAddress, pool.navigateTo(p, symOnly).getPageAddress(sym));

            final PageFrameMemory memory = pool.navigateTo(p, set(qty));
            assertNotEquals("a different subset must be decoded", 0, memory.getPageAddress(qty));
            assertFalse(memory.hasColumnTops());
            assertQtyEquals(ref.navigateTo(p), memory, qty, frameRows);
        });
    }

    @Test
    public void testUndecodedColumnsAreNotColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            createAndConvert();
            try (RecordCursorFactory factory = select("SELECT * FROM t");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 PageFrameAddressCache addressCache = new PageFrameAddressCache();
                 PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L)) {
                final int frameCount = fill(factory, cursor, addressCache);
                pool.of(addressCache);
                final RecordMetadata metadata = factory.getMetadata();
                final IntHashSet filterColumns = set(metadata.getColumnIndex("sym"), metadata.getColumnIndex("ex"));

                int parquetFrames = 0;
                for (int i = 0; i < frameCount; i++) {
                    final boolean parquet = addressCache.getFrameFormat(i) == PartitionFormat.PARQUET;
                    parquetFrames += parquet ? 1 : 0;
                    final PageFrameMemory memory = pool.navigateTo(i, filterColumns);
                    assertFalse(
                            "frame " + i + (parquet ? " (parquet)" : " (native)") + ": no column has a top",
                            memory.hasColumnTops()
                    );
                }
                assertTrue("expected a Parquet frame", parquetFrames > 0);

                // The full-frame overload sees every column decoded.
                pool.of(addressCache);
                for (int i = 0; i < frameCount; i++) {
                    assertFalse(pool.navigateTo(i).hasColumnTops());
                }
            }
        });
    }

    @Test
    public void testZeroWindowNavigateResetsDecodedColumns() throws Exception {
        // An empty subset decodes no column and judges none for column tops. A zero-window navigate
        // publishes NULL addresses for every column, so it must judge them all again.
        assertLateMaterializedFrame((pool, ref, p, sym, qty, frameRows) -> {
            assertFalse(pool.navigateTo(p, new IntHashSet()).hasColumnTops());
            assertTrue(pool.navigateTo(p, 5, 5).hasColumnTops());
        });
    }

    private static void assertQtyEquals(PageFrameMemory expected, PageFrameMemory actual, int qty, int frameRows) {
        final long expectedAddress = expected.getPageAddress(qty);
        final long actualAddress = actual.getPageAddress(qty);
        assertNotEquals(0, expectedAddress);
        assertNotEquals(0, actualAddress);
        for (int r = 0; r < frameRows; r++) {
            assertEquals("qty at frame row " + r, Unsafe.getLong(expectedAddress + 8L * r), Unsafe.getLong(actualAddress + 8L * r));
        }
    }

    private static void assertRecordQtyEquals(PageFrameMemory expected, PageFrameMemoryRecord actual, int qty, int frameRows) {
        final long expectedAddress = expected.getPageAddress(qty);
        assertNotEquals(0, expectedAddress);
        assertNotEquals(0, actual.getPageAddress(qty));
        for (int r = 0; r < frameRows; r++) {
            actual.setRowIndex(r);
            assertEquals("record qty at frame row " + r, Unsafe.getLong(expectedAddress + 8L * r), actual.getLong(qty));
        }
    }

    private static void populateEveryRow(PageFrameMemory memory, IntHashSet filterColumns, int frameRows) {
        try (DirectLongList rows = new DirectLongList(frameRows, MemoryTag.NATIVE_DEFAULT)) {
            for (int r = 0; r < frameRows; r++) {
                rows.add(r);
            }
            assertTrue(memory.populateRemainingColumns(filterColumns, rows, true));
        }
    }

    private static String runSql(String query, boolean jitNullChecks) throws Exception {
        final StringSink out = new StringSink();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setEnableJitNullChecks(jitNullChecks);
            try {
                TestUtils.printSql(compiler, sqlExecutionContext, query, out);
            } finally {
                compiler.setEnableJitNullChecks(true);
            }
        }
        return out.toString();
    }

    private static IntHashSet set(int... columns) {
        final IntHashSet set = new IntHashSet();
        for (int c : columns) {
            set.add(c);
        }
        return set;
    }

    // one frame per partition: the Parquet day is a single 1440-row frame
    private void assertFrame(long budget, FrameCheck check) throws Exception {
        assertFrames(budget, ParquetDecodeHint.SCATTERED, 1, (pool, ref, frames, metadata, addressCache) -> {
            final int p = frames.getQuick(0);
            check.run(pool, ref, p, metadata, (int) addressCache.getFrameSize(p));
        });
    }

    private void assertFrames(long budget, FramesCheck check) throws Exception {
        assertFrames(budget, ParquetDecodeHint.SCATTERED, check);
    }

    // 200-row frames: the Parquet day holds 8 of them
    private void assertFrames(long budget, ParquetDecodeHint hint, FramesCheck check) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 200);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 200);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 200);
        assertFrames(budget, hint, 2, check);
    }

    private void assertFrames(long budget, ParquetDecodeHint hint, int minParquetFrames, FramesCheck check) throws Exception {
        assertMemoryLeak(() -> {
            createAndConvert();
            try (RecordCursorFactory factory = select("SELECT * FROM t");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 PageFrameAddressCache addressCache = new PageFrameAddressCache();
                 PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, budget);
                 PageFrameMemoryPool ref = new PageFrameMemoryPool(configuration, 0L)) {
                final int frameCount = fill(factory, cursor, addressCache);
                final IntList parquetFrames = new IntList();
                for (int p = 0; p < frameCount; p++) {
                    if (addressCache.getFrameFormat(p) == PartitionFormat.PARQUET) {
                        parquetFrames.add(p);
                    }
                }
                assertTrue("expected " + minParquetFrames + " Parquet frame(s)", parquetFrames.size() >= minParquetFrames);
                pool.of(addressCache, hint);
                ref.of(addressCache);
                check.run(pool, ref, parquetFrames, factory.getMetadata(), addressCache);
            }
        });
    }


    private void assertLateMaterializedFrame(LateMaterializedFrameCheck check) throws Exception {
        assertMemoryLeak(() -> {
            createAndConvert();
            try (RecordCursorFactory factory = select("SELECT * FROM t");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 PageFrameAddressCache addressCache = new PageFrameAddressCache();
                 PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L);
                 PageFrameMemoryPool ref = new PageFrameMemoryPool(configuration, 0L)) {
                final int frameCount = fill(factory, cursor, addressCache);
                final RecordMetadata metadata = factory.getMetadata();
                final int sym = metadata.getColumnIndex("sym");
                final int qty = metadata.getColumnIndex("qty");
                int parquetFrames = 0;
                for (int p = 0; p < frameCount; p++) {
                    if (addressCache.getFrameFormat(p) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    parquetFrames++;
                    pool.of(addressCache);
                    ref.of(addressCache);
                    check.run(pool, ref, p, sym, qty, (int) addressCache.getFrameSize(p));
                }
                assertTrue("expected a Parquet frame", parquetFrames > 0);
            }
        });
    }

    private void createAndConvert() throws Exception {
        execute("""
                CREATE TABLE t (
                    ts TIMESTAMP, sym SYMBOL, ex SYMBOL, price DOUBLE, qty LONG, note VARCHAR
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("""
                INSERT INTO t
                SELECT
                    '2024-01-01T00:00:00'::TIMESTAMP + x * 60_000_000L,
                    rnd_symbol('A', 'B', 'C'),
                    rnd_symbol('K', 'L', 'M', 'N'),
                    rnd_double(),
                    x,
                    rnd_varchar(1, 40, 1)
                FROM long_sequence(3_000)
                """);
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
    }

    private int fill(RecordCursorFactory factory, PageFrameCursor cursor, PageFrameAddressCache addressCache) {
        addressCache.of(factory.getMetadata(), cursor.getColumnMapping(), cursor.isExternal());
        int frameCount = 0;
        PageFrame f;
        while ((f = cursor.next(0)) != null) {
            addressCache.add(frameCount++, f);
        }
        return frameCount;
    }

    @FunctionalInterface
    private interface FrameCheck {
        void run(PageFrameMemoryPool pool, PageFrameMemoryPool ref, int frameIndex, RecordMetadata metadata, int frameRows);
    }

    @FunctionalInterface
    private interface FramesCheck {
        void run(PageFrameMemoryPool pool, PageFrameMemoryPool ref, IntList parquetFrames, RecordMetadata metadata, PageFrameAddressCache addressCache);
    }

    @FunctionalInterface
    private interface LateMaterializedFrameCheck {
        void run(PageFrameMemoryPool pool, PageFrameMemoryPool ref, int frameIndex, int sym, int qty, int frameRows);
    }
}
