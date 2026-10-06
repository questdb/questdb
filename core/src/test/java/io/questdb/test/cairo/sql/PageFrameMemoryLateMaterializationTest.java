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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.IntHashSet;
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
 */
public class PageFrameMemoryLateMaterializationTest extends AbstractCairoTest {

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

    private static IntHashSet set(int... columns) {
        final IntHashSet set = new IntHashSet();
        for (int c : columns) {
            set.add(c);
        }
        return set;
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
}
