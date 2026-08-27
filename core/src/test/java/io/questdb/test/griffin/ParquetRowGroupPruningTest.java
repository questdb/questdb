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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;

import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;

import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ParquetRowGroupPruningTest extends AbstractCairoTest {
    // Rows createRepeatedFloatPartialParquet() writes into the parquet partition. The value is large
    // enough that the compiled filter runs its AVX2 loop rather than handling everything in the
    // scalar tail - a tiny table leaves jit/impl/avx2.h out of the picture entirely, which is the
    // one code path the tolerance-boundary witnesses exist to cover.
    private static final int REPEATED_FLOAT_ROW_COUNT = 1_000;

    @Before
    public void setUp() {
        ParquetRowGroupFilter.resetRowGroupsSkipped();
        super.setUp();
    }

    @Test
    public void testRowGroupPruningSurvivesEmptyMetadataCacheWindow() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 100_000)
                    FROM long_sequence(5000)
                    """);
            // Second partition makes 2024-01-01 a non-active partition so it converts.
            execute("""
                    INSERT INTO x VALUES
                    (8000, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            // Reproduce the registered-but-not-yet-cached window: evict the table from
            // the metadata cache so the query below is the first reader to need it.
            try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                metadataRW.clearCache();
            }
            final TableToken token = engine.getTableTokenIfExists("x");
            try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
                Assert.assertNull(metadataRO.getTable(token));
            }

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -991 ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);
        });
    }

    @Test
    public void testCalculateSizeAndSkipRowsOnPushdownPrunedPageFrameCursor() throws Exception {
        // Pushdown pruning drops whole non-matching row groups at the page-frame
        // level, but the metadata-only size/skip fast paths account at the
        // PARTITION-frame level, which is blind to that pruning: they would count
        // physical rows the cursor never yields. All three of PageFrameRecordCursorImpl
        // .calculateSize(), .skipRows() and .size() therefore gate on
        // hasActivePushdownFilter().
        //
        // No gate is reachable through a plain SELECT: a residual filter factory
        // always wraps the pushdown-carrying page-frame factory, and the wrapper's own
        // calculateSize()/skipRows() walk rows. The live view refresh job unwraps it -
        // filterFactory.getBaseFactory(), re-applying the WHERE above - and drives the
        // raw cursor, whose own filter is therefore null while pushdown is active. That
        // job calls skipRows() (covered end-to-end by LiveViewParquetBaseTest); it does
        // not call calculateSize(), so the calculateSize() gate is a contract-level
        // sibling with no production caller yet. This test builds that same raw-cursor
        // shape and pins BOTH gates, so neither can be dropped unnoticed.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 100_000)
                    FROM long_sequence(5000)
                    """);
            // Second partition makes 2024-01-01 a non-active partition so it converts.
            execute("""
                    INSERT INTO x VALUES
                    (8000, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            try (RecordCursorFactory filterFactory = select("SELECT * FROM x WHERE val = 42")) {
                // The residual filter lives in the wrapper; the pushdown conditions sit
                // on the page-frame factory beneath it. Do not close the base separately -
                // the wrapper owns it.
                // Strip the QueryProgress wrapper that engine.select() adds; the refresh
                // job compiles its own factory and never sees it.
                RecordCursorFactory residualFilterFactory = filterFactory;
                if (residualFilterFactory instanceof QueryProgress) {
                    residualFilterFactory = residualFilterFactory.getBaseFactory();
                }
                // The residual WHERE lives in the wrapper, so unwrapping it once lands on
                // the page-frame factory whose cursor carries no filter of its own while
                // pushdown pruning is active. (Only the wrapper's getFilter() is load
                // bearing here: PageFrameRecordCursorFactory does not override getFilter(),
                // so asserting null on the base would prove nothing.)
                Assert.assertNotNull(residualFilterFactory.getFilter());
                final RecordCursorFactory pageFrameFactory = residualFilterFactory.getBaseFactory();
                Assert.assertNotNull(pageFrameFactory);

                // Walk the raw cursor to establish what it actually yields.
                long walked = 0;
                try (RecordCursor cursor = pageFrameFactory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        walked++;
                    }
                }
                // Pruning really dropped row groups, so the scan yields far fewer than the
                // 5001 physical rows. Without this the size assertion below would be vacuous.
                Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);
                Assert.assertTrue("pruned scan must yield a strict subset [walked=" + walked + ']', walked > 0 && walked < 5_001);

                // calculateSize() must agree with the walk. Without the pushdown gate the
                // metadata-only path reports every physical row, including the pruned ones.
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                try (RecordCursor cursor = pageFrameFactory.getCursor(sqlExecutionContext)) {
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                }
                Assert.assertEquals(walked, counter.get());

                // The same gate in skipRows(). The skip target must exceed the parquet
                // partition's PHYSICAL row count (5000), because the pruning-blind
                // accounting only kicks in when FullFwdPartitionFrameCursor.next(skipTarget)
                // takes its whole-partition skeleton branch (hi <= skipTarget), which
                // reports the physical count without decoding or pruning. A smaller target
                // would decode and prune on the way past, making both paths agree and the
                // assertion vacuous.
                counter.set(5_001);
                try (RecordCursor cursor = pageFrameFactory.getCursor(sqlExecutionContext)) {
                    cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
                    // Gated: the walk skips only the rows the cursor yields, leaving the
                    // rest of the request unspent. Un-gated: the skeleton branch charges
                    // all 5001 physical rows and the counter lands on 0.
                    Assert.assertEquals(5_001 - walked, counter.get());
                    Assert.assertFalse(cursor.hasNext());
                }

                // size() is the third member of the family. It is an entity cursor here, so
                // un-gated it would hand back the partition-frame physical count (5001) -
                // the same pruning-blind number the two gates above reject. A size() must
                // either be exact or -1 (unknown); the pruned scan cannot know it up front,
                // so -1 is the only sound answer.
                try (RecordCursor cursor = pageFrameFactory.getCursor(sqlExecutionContext)) {
                    final long size = cursor.size();
                    Assert.assertTrue(
                            "size() must be exact or unknown, never the pruning-blind physical count [size=" + size + ", walked=" + walked + ']',
                            size == -1 || size == walked
                    );
                }
            }
        });
    }

    @Test
    public void testBloomFilterBackwardScan() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 100_000)
                    FROM long_sequence(5000)
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (8000, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = -991 ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 42 ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("""
                            val
                            42
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IN (-1, -2, -3) ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IN (1, 2001) ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("""
                            val
                            2001
                            1
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt("v", -991);
            assertQuery("SELECT val FROM x WHERE val = :v ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt("v", 42);
            assertQuery("SELECT val FROM x WHERE val = :v ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("val\n42\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);
        });
    }

    @Test
    public void testBloomFilterByte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (10, '2024-01-01T01:00:00.000000Z'),
                    (100, '2024-01-01T02:00:00.000000Z'),
                    (101, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");
            assertQuery("SELECT val FROM x WHERE val = 50::byte")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 10::byte")
                    .noLeakCheck()
                    .returns("""
                            val
                            10
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 50::SHORT")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 10::SHORT")
                    .noLeakCheck()
                    .returns("val\n10\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 50")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 10")
                    .noLeakCheck()
                    .returns("val\n10\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 50::LONG")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 10::LONG")
                    .noLeakCheck()
                    .returns("val\n10\n");
        });
    }

    @Test
    public void testBloomFilterChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('A', '2024-01-01T00:00:00.000000Z'),
                    ('M', '2024-01-01T01:00:00.000000Z'),
                    ('Z', '2024-01-01T02:00:00.000000Z'),
                    ('X', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 'G'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 'M'")
                    .noLeakCheck()
                    .returns("""
                            val
                            M
                            """);
        });
    }

    @Test
    public void testBloomFilterDate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('2020-01-01'::DATE, '2024-01-01T00:00:00.000000Z'),
                    ('2020-06-15'::DATE, '2024-01-01T01:00:00.000000Z'),
                    ('2020-10-31'::DATE, '2024-01-01T02:00:00.000000Z'),
                    ('2020-12-01'::DATE, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '2020-03-15'::DATE")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '2020-06-15'::DATE")
                    .noLeakCheck()
                    .returns("""
                            val
                            2020-06-15T00:00:00.000Z
                            """);
            assertQuery("SELECT val FROM x WHERE val = '2020-06-15T00:00:00.000000001Z'")
                    .noLeakCheck()
                    .returns("""
                            val
                            2020-06-15T00:00:00.000Z
                            """);
        });
    }

    @Test
    public void testBloomFilterDecimal128() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(30,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999999999.98', '2024-01-01T02:00:00.000000Z'),
                    ('9999999999999.99', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '2500000000000.25'::DECIMAL(30,2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '5000000000000.50'::DECIMAL(30,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5000000000000.50
                            """);
        });
    }

    @Test
    public void testBloomFilterDecimal16() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(4,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('10.10', '2024-01-01T00:00:00.000000Z'),
                    ('50.50', '2024-01-01T01:00:00.000000Z'),
                    ('99.99', '2024-01-01T02:00:00.000000Z'),
                    ('10.11', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '30.30'::DECIMAL(4,2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '50.50'::DECIMAL(4,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            50.50
                            """);
        });
    }

    @Test
    public void testBloomFilterDecimal256() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(50,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('100000000000000000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('500000000000000000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('999999999999999999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('999999999999999999999.98', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '250000000000000000000.25'::DECIMAL(50,2)")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '500000000000000000000.50'::DECIMAL(50,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            500000000000000000000.50
                            """);
        });
    }

    @Test
    public void testBloomFilterDecimal32() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(8,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                     INSERT INTO x VALUES
                     ('1000.10', '2024-01-01T00:00:00.000000Z'),
                     ('50000.50', '2024-01-01T01:00:00.000000Z'),
                     ('99999.99', '2024-01-01T02:00:00.000000Z'),
                     ('99998.99', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '25000.25'::DECIMAL(8,2)")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '50000.50'::DECIMAL(8,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            50000.50
                            """);
        });
    }

    @Test
    public void testBloomFilterDecimal64() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '2500000.25'::DECIMAL(15,2)")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '5000000.50'::DECIMAL(15,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5000000.50
                            """);
        });
    }

    @Test
    public void testBloomFilterDecimal8() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(2,1), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1.1', '2024-01-01T00:00:00.000000Z'),
                    ('5.5', '2024-01-01T01:00:00.000000Z'),
                    ('9.9', '2024-01-01T02:00:00.000000Z'),
                    ('7.9', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '3.3'::DECIMAL(2,1)")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '5.5'::DECIMAL(2,1)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5.5
                            """);
        });
    }

    @Test
    public void testBloomFilterDouble() throws Exception {
        // A DOUBLE equality no longer prunes on the bloom filter at this magnitude: the bloom filter
        // hashes the exact bits of the bound, while the row-level filter keeps every row within
        // DOUBLE_TOLERANCE of it, and a group holding 3.3300000000499 would report 3.33 as absent.
        // The bound pushes as the BETWEEN spanning its tolerance band instead, which prunes on the
        // min/max stats only (both values here fall inside them). testDoubleColumnExactEqStillPrunes-
        // OnBloomFilter covers the magnitude at which the band holds the bound alone and the bloom
        // filter comes back.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1.11, '2024-01-01T00:00:00.000000Z'),
                    (5.55, '2024-01-01T01:00:00.000000Z'),
                    (9.99, '2024-01-01T02:00:00.000000Z'),
                    (9.79, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 3.33")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            assertQuery("SELECT val FROM x WHERE val = 5.55")
                    .noLeakCheck()
                    .returns("""
                            val
                            5.55
                            """);

            // A bound clear of the group's min/max still prunes it, through the band.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 99.99")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testBloomFilterDroppedColumnO3Rewrite() throws Exception {
        // Regression: after DROP COLUMN, an O3 insert rewrites the parquet
        // partition and raw-copies the row groups the O3 data does not touch.
        // The updater extracts bloom bitsets in source-file order but writes
        // the survivors in target order; without the remap, dropping the
        // leading bloomed column shifted every bloom one slot (a's bloom onto
        // b, b's onto c), so an equality filter probed the wrong column's
        // bloom and silently pruned row groups holding matching rows.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, b INT, c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Disjoint, gapped value ranges per column: a misattributed bloom
            // reports every value of its neighbor as absent. 12 rows make 3
            // row groups of 4.
            execute("""
                    INSERT INTO x
                    SELECT (1000 + 10 * x)::INT, (2000 + 10 * x)::INT, (3000 + 10 * x)::INT,
                           timestamp_sequence('2024-01-01', 3_600_000_000)
                    FROM long_sequence(12)
                    """);
            // Second partition makes 2024-01-01 a non-active partition so it converts.
            execute("INSERT INTO x VALUES (1, 9999, 9999, '2024-01-02T00:00:00.000000Z')");
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01' WITH (bloom_filter_columns = 'a,b,c')");
            drainWalQueue();

            // Drop the leading bloomed column: survivors b, c, ts shift down one slot.
            execute("ALTER TABLE x DROP COLUMN a");
            drainWalQueue();
            // The O3 row lands in the first row group only; the schema change
            // forces a rewrite that copies row groups 1 and 2 with their blooms.
            execute("INSERT INTO x(b, c, ts) VALUES (5000, 6000, '2024-01-01T00:30:00.000000Z')");
            drainWalQueue();

            // Equality on values that live only in the copied row groups must
            // return their rows instead of pruning on a misattributed bloom.
            assertQuery("SELECT b, c FROM x WHERE b = 2100")
                    .noLeakCheck()
                    .returns("""
                            b\tc
                            2100\t3100
                            """);
            assertQuery("SELECT b, c FROM x WHERE c = 3060")
                    .noLeakCheck()
                    .returns("""
                            b\tc
                            2060\t3060
                            """);
            // The merged row group re-encodes its bloom and must find the O3 row.
            assertQuery("SELECT b, c FROM x WHERE b = 5000")
                    .noLeakCheck()
                    .returns("""
                            b\tc
                            5000\t6000
                            """);

            // A value inside a copied row group's min/max range but absent from
            // its data must still prune, proving the copied blooms stay live.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b = 2095")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testBloomFilterFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1.0, '2024-01-01T00:00:00.000000Z'),
                    (5.0, '2024-01-01T01:00:00.000000Z'),
                    (10.0, '2024-01-01T02:00:00.000000Z'),
                    (12.0, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 3.0")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 5.0::FLOAT")
                    .noLeakCheck()
                    .returns("""
                            val
                            5.0
                            """);
        });
    }

    @Test
    public void testBloomFilterIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val IPv4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1.1.1.1', '2024-01-01T00:00:00.000000Z'),
                    ('10.0.0.1', '2024-01-01T01:00:00.000000Z'),
                    (NULL, '2024-01-01T02:00:00.000000Z'),
                    ('192.168.1.1', '2024-01-01T03:00:00.000000Z'),
                    ('127.0.0.1', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '5.5.5.5'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '10.0.0.1'")
                    .noLeakCheck()
                    .returns("""
                            val
                            10.0.0.1
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val in (NULL)")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testBloomFilterInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 25_000")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 50_000")
                    .noLeakCheck()
                    .returns("""
                            val
                            50000
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 25_000::LONG")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 50_000::LONG")
                    .noLeakCheck()
                    .returns("val\n50000\n");
        });
    }

    @Test
    public void testBloomFilterIntBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            bindVariableService.clear();
            bindVariableService.setInt("v", 25_000);
            assertQuery("SELECT val FROM x WHERE val = :v")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt("v", 50_000);
            assertQuery("SELECT val FROM x WHERE val = :v")
                    .noLeakCheck()
                    .returns("val\n50000\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt(0, 25_000);
            assertQuery("SELECT val FROM x WHERE val = $1")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testBloomFilterIntInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val IN (2, 3, 4)")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val IN (1, 25_000)")
                    .noLeakCheck()
                    .returns("""
                            val
                            1
                            """);
        });
    }

    @Test
    public void testBloomFilterLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (500_000, '2024-01-01T01:00:00.000000Z'),
                    (1_000_000, '2024-01-01T02:00:00.000000Z'),
                    (1_000_001, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 250_000")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 500_000")
                    .noLeakCheck()
                    .returns("""
                            val
                            500000
                            """);
        });
    }

    @Test
    public void testBloomFilterLong128() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG128, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (to_long128(0, 1), '2024-01-01T00:00:00.000000Z'),
                    (to_long128(0, 50), '2024-01-01T01:00:00.000000Z'),
                    (to_long128(0, 100), '2024-01-01T02:00:00.000000Z'),
                    (to_long128(0, 101), '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = to_long128(0, 25)")
                    .noLeakCheck()
                    .returns("val\n");


            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = to_long128(0, 50)")
                    .noLeakCheck()
                    .returns("""
                            val
                            00000000-0000-0032-0000-000000000000
                            """);
        });
    }

    @Test
    public void testBloomFilterMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, b VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, 'aaa', '2024-01-01T00:00:00.000000Z'),
                    (50, 'mmm', '2024-01-01T01:00:00.000000Z'),
                    (100, 'zzz', '2024-01-01T02:00:00.000000Z'),
                    (101, 'xxx', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'a,b')");

            assertQuery("SELECT a, b FROM x WHERE a = 25 AND b = 'ggg'")
                    .noLeakCheck()
                    .returns("a\tb\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT a, b FROM x WHERE a = 1 AND b = 'ggg'")
                    .noLeakCheck()
                    .returns("a\tb\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT a, b FROM x WHERE a = 50 AND b = 'mmm'")
                    .noLeakCheck()
                    .returns("""
                            a\tb
                            50\tmmm
                            """);
        });
    }

    @Test
    public void testBloomFilterShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (100, '2024-01-01T00:00:00.000000Z'),
                    (200, '2024-01-01T01:00:00.000000Z'),
                    (1000, '2024-01-01T02:00:00.000000Z'),
                    (1010, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 501")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 200")
                    .noLeakCheck()
                    .returns("""
                            val
                            200
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 501")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 200")
                    .noLeakCheck()
                    .returns("val\n200\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 501::LONG")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 200::LONG")
                    .noLeakCheck()
                    .returns("val\n200\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 501::SHORT")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 200::SHORT")
                    .noLeakCheck()
                    .returns("val\n200\n");
        });
    }

    @Test
    public void testBloomFilterString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('aaa', '2024-01-01T00:00:00.000000Z'),
                    ('mmm', '2024-01-01T01:00:00.000000Z'),
                    ('zzz', '2024-01-01T02:00:00.000000Z'),
                    ('xxx', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 'ggg'")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 'mmm'")
                    .noLeakCheck()
                    .returns("""
                            val
                            mmm
                            """);
        });
    }

    @Test
    public void testBloomFilterSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('alpha', '2024-01-01T00:00:00.000000Z'),
                    ('gamma', '2024-01-01T01:00:00.000000Z'),
                    ('zeta', '2024-01-01T02:00:00.000000Z'),
                    ('zeta1', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 'delta'")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 'gamma'")
                    .noLeakCheck()
                    .returns("""
                            val
                            gamma
                            """);
        });
    }

    @Test
    public void testBloomFilterSymbolRenamedColumn() throws Exception {
        // Regression: the row-group bloom-filter pushdown resolved the filtered column
        // by its parquet name. Parquet column names are frozen at conversion time, so a
        // rename leaves them stale. When another column already bears the query's current
        // name, the pushdown checked the WRONG column's bloom filter and wrongly skipped
        // row groups, silently dropping valid rows. The fix resolves by stable column id.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a SYMBOL, b SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // 'gamma' lives only in column a; column b never holds it.
            execute("""
                    INSERT INTO x VALUES
                    ('gamma', 'p', '2024-01-01T00:00:00.000000Z'),
                    ('delta', 'q', '2024-01-01T01:00:00.000000Z'),
                    ('gamma', 'r', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'a,b')");

            // Swap names: free 'b', then rename a -> b. Now the live column 'b' is the
            // original column 'a' (holds 'gamma'), while the parquet still carries a
            // frozen column literally named 'b' (the original b, which never held 'gamma').
            execute("ALTER TABLE x RENAME COLUMN b TO c");
            execute("ALTER TABLE x RENAME COLUMN a TO b");

            // Equality on the renamed column must find its rows, not be pruned away.
            assertQuery("SELECT b FROM x WHERE b = 'gamma' ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b
                            gamma
                            gamma
                            """);

            // A value genuinely absent from the renamed column must still prune.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b = 'nope'")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testBloomFilterTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('2020-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    ('2020-06-15T12:00:00.000000Z', '2024-01-01T01:00:00.000000Z'),
                    ('2020-12-31T23:59:59.999999Z', '2024-01-01T02:00:00.000000Z'),
                    ('2020-12-30T23:59:59.999999Z', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '2020-03-15T00:00:00.000000Z'::TIMESTAMP")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '2020-06-15T12:00:00.000000Z'::TIMESTAMP")
                    .noLeakCheck()
                    .returns("""
                            val
                            2020-06-15T12:00:00.000000Z
                            """);

            assertQuery("SELECT val FROM x WHERE val = '2020-06-15T12:00:00.000000Z'::TIMESTAMP_NS")
                    .noLeakCheck()
                    .returns("""
                            val
                            2020-06-15T12:00:00.000000Z
                            """);

            assertQuery("SELECT val FROM x WHERE val = '2020-01-01'::Date")
                    .noLeakCheck()
                    .returns("""
                            val
                            2020-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testBloomFilterUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:00.000000Z'),
                    ('55555555-5555-5555-5555-555555555555', '2024-01-01T01:00:00.000000Z'),
                    ('99999999-9999-9999-9999-999999999999', '2024-01-01T02:00:00.000000Z'),
                    ('99999999-9999-9999-9999-999999999998', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '33333333-3333-3333-3333-333333333334'")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '55555555-5555-5555-5555-555555555555'")
                    .noLeakCheck()
                    .returns("""
                            val
                            55555555-5555-5555-5555-555555555555
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '33333333-3333-3333-3333-333333333334'::UUID")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQuery("SELECT val FROM x WHERE val = '55555555-5555-5555-5555-555555555555'::UUID")
                    .noLeakCheck()
                    .returns("""
                            val
                            55555555-5555-5555-5555-555555555555
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = NULL::UUID")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IN ('33333333-3333-3333-3333-333333333334', '44444444-4444-4444-4444-444444444444')")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQuery("SELECT val FROM x WHERE val IN ('11111111-1111-1111-1111-111111111111', '99999999-9999-9999-9999-999999999999')")
                    .noLeakCheck()
                    .returns("""
                            val
                            11111111-1111-1111-1111-111111111111
                            99999999-9999-9999-9999-999999999999
                            """);
        });
    }

    @Test
    public void testBloomFilterVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('abc', '2024-01-01T00:00:00.000000Z'),
                    ('❤️', '2024-01-01T01:00:00.000000Z'),
                    ('xyz', '2024-01-01T02:00:00.000000Z'),
                    ('xxx', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 'ghi'")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '❤️'")
                    .noLeakCheck()
                    .returns("""
                            val
                            ❤️
                            """);
        });
    }

    @Test
    public void testBloomFilterVarcharBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('abc', '2024-01-01T00:00:00.000000Z'),
                    ('❤️', '2024-01-01T01:00:00.000000Z'),
                    ('xyz', '2024-01-01T02:00:00.000000Z'),
                    ('xxx', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            bindVariableService.clear();
            bindVariableService.setStr("v", "ghi");
            assertQuery("SELECT val FROM x WHERE val = :v")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setStr("v", "❤️");
            assertQuery("SELECT val FROM x WHERE val = :v")
                    .noLeakCheck()
                    .returns("val\n❤️\n");
        });
    }

    @Test
    public void testBloomFilterWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x ADD COLUMN val INT");
            execute("""
                    INSERT INTO x VALUES
                    (3, '2024-01-01T02:00:00.000000Z', 10),
                    (4, '2024-01-01T03:00:00.000000Z', 100),
                    (5, '2024-01-01T04:00:00.000000Z', 1000),
                    (6, '2024-01-02T01:00:00.000000Z', 10_000)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT id, val FROM x WHERE val = 50")
                    .noLeakCheck()
                    .returns("id\tval\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT id, val FROM x WHERE val = 100")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            4\t100
                            """);
            assertQuery("SELECT id, val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\tnull
                            2\tnull
                            """);
        });
    }

    @Test
    public void testByteAndShortColumnTopRowsSurvivePruning() throws Exception {
        // BYTE and SHORT have no NULL sentinel, so a column top reads back as a perfectly ordinary
        // 0. The parquet writer marks those rows definition-level 0, which keeps them out of the
        // min/max statistics and the bloom set - and the pruning code only consults null_count when
        // the FILTER value is the type's null sentinel, which for these two types does not exist.
        // So a row group whose real values are 5 and 6 reports min=5/max=6 and gets skipped for
        // "= 0" and "< 1", losing the column-top rows that native storage returns.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x ADD COLUMN b BYTE");
            execute("ALTER TABLE x ADD COLUMN s SHORT");
            // The 2024-01-02 row keeps 2024-01-01 out of the active slot so it can convert.
            execute("""
                    INSERT INTO x VALUES
                    (3, '2024-01-01T02:00:00.000000Z', 5, 5),
                    (4, '2024-01-01T03:00:00.000000Z', 6, 6),
                    (5, '2024-01-02T01:00:00.000000Z', 7, 7)
                    """);

            final String eqByte = """
                    id\tb
                    1\t0
                    2\t0
                    """;
            final String eqShort = """
                    id\ts
                    1\t0
                    2\t0
                    """;

            // Native storage: the column-top rows read as 0 and match.
            assertQuery("SELECT id, b FROM x WHERE b = 0").noLeakCheck().returns(eqByte);
            assertQuery("SELECT id, b FROM x WHERE b < 1").noLeakCheck().returns(eqByte);
            assertQuery("SELECT id, s FROM x WHERE s = 0").noLeakCheck().returns(eqShort);
            assertQuery("SELECT id, s FROM x WHERE s < 1").noLeakCheck().returns(eqShort);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            assertHasParquetPartitions("x", true);

            // Parquet must agree with native on every one of them.
            assertQuery("SELECT id, b FROM x WHERE b = 0").noLeakCheck().returns(eqByte);
            assertQuery("SELECT id, b FROM x WHERE b < 1").noLeakCheck().returns(eqByte);
            assertQuery("SELECT id, s FROM x WHERE s = 0").noLeakCheck().returns(eqShort);
            assertQuery("SELECT id, s FROM x WHERE s < 1").noLeakCheck().returns(eqShort);

            // A bound that genuinely excludes both the stored values and the column-top 0 must
            // still prune, so the fix does not simply switch pruning off for these columns.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT id, b FROM x WHERE b = 9").noLeakCheck().returns("id\tb\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // The bloom filter is the third pruning mechanism and needs its own guard: statistics
            // can be widened to cover the implicit 0, a set of exact hashes cannot. The column-top
            // rows were never hashed in, so a 0 probe must not come back "absent".
            execute("CREATE TABLE y (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO y VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE y ADD COLUMN b BYTE");
            execute("""
                    INSERT INTO y VALUES
                    (3, '2024-01-01T02:00:00.000000Z', 5),
                    (4, '2024-01-01T03:00:00.000000Z', 6),
                    (5, '2024-01-02T01:00:00.000000Z', 7)
                    """);
            execute("ALTER TABLE y CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02' " +
                    "WITH (bloom_filter_columns = 'b')");
            assertHasParquetPartitions("y", true);

            assertQuery("SELECT id, b FROM y WHERE b = 0").noLeakCheck().returns(eqByte);
            assertQuery("SELECT id, b FROM y WHERE b = 5")
                    .noLeakCheck()
                    .returns("""
                            id\tb
                            3\t5
                            """);
            // 3 sits INSIDE the widened [0,6] statistics, so only the bloom set can prune it.
            // This is what proves the bloom path above is genuinely consulted, and therefore that
            // the 0 probe needs its own guard rather than riding on the widened statistics.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT id, b FROM y WHERE b = 3").noLeakCheck().returns("id\tb\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // A value outside both the statistics and the bloom set still prunes.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT id, b FROM y WHERE b = 9").noLeakCheck().returns("id\tb\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testByteColumnTopRowsSurviveIsNotNullPruning() throws Exception {
        // The same family as testByteAndShortColumnTopRowsSurvivePruning, on the fourth pruning
        // path. BYTE, SHORT and BOOLEAN have no NULL sentinel, so "b IS NOT NULL" is a constant
        // TRUE over them - a column-top row reads as 0/false and 0 IS NOT NULL. The parquet writer
        // marks those rows definition-level 0 though, so a row group lying wholly inside the column
        // top reports null_count == num_values, which a decoder reading that count as "every value
        // is null" would skip, and the rows would vanish. Only a partition entirely predating the
        // ADD COLUMN produces that shape.
        //
        // What the assertions below pin is the JAVA half of the defence: isNullOpPushable() refuses
        // to emit a null op for these three types, so nothing reaches the row group pruner at all.
        // They do NOT cover the native ParquetDecoder::is_null_free_type guard - with no condition
        // pushed down there is nothing for it to gate, so they pass whether or not it exists. Its
        // coverage lives in the Rust tests its doc comment names.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE z (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO z VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE z ADD COLUMN b BYTE");
            execute("ALTER TABLE z ADD COLUMN s SHORT");
            execute("ALTER TABLE z ADD COLUMN f BOOLEAN");
            // 2024-01-02 is the only partition with data for these, so 2024-01-01 is wholly top.
            execute("INSERT INTO z VALUES (3, '2024-01-02T00:00:00.000000Z', 7, 7, true)");

            final String expectedByte = """
                    id\tb
                    1\t0
                    2\t0
                    3\t7
                    """;
            final String expectedShort = """
                    id\ts
                    1\t0
                    2\t0
                    3\t7
                    """;
            final String expectedBool = """
                    id\tf
                    1\tfalse
                    2\tfalse
                    3\ttrue
                    """;
            assertQuery("SELECT id, b FROM z WHERE b IS NOT NULL").noLeakCheck().expectSize().returns(expectedByte);
            assertQuery("SELECT id, s FROM z WHERE s IS NOT NULL").noLeakCheck().expectSize().returns(expectedShort);
            assertQuery("SELECT id, f FROM z WHERE f IS NOT NULL").noLeakCheck().expectSize().returns(expectedBool);

            execute("ALTER TABLE z CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            assertHasParquetPartitions("z", true);

            assertQuery("SELECT id, b FROM z WHERE b IS NOT NULL").noLeakCheck().expectSize().returns(expectedByte);
            assertQuery("SELECT id, s FROM z WHERE s IS NOT NULL").noLeakCheck().expectSize().returns(expectedShort);
            assertQuery("SELECT id, f FROM z WHERE f IS NOT NULL").noLeakCheck().expectSize().returns(expectedBool);

            // The mirror image stays prunable: IS NULL is a constant FALSE over these types.
            assertQuery("SELECT id, b FROM z WHERE b IS NULL").noLeakCheck().returns("id\tb\n");
        });
    }

    @Test
    public void testInfinityRowsSurviveIsNullPruning() throws Exception {
        // Numbers.isNull(double) is an exponent-bits test, so QuestDB calls every non-finite value
        // NULL - NaN and +/-Infinity alike - while the parquet writer's Nullable for f32/f64 reports
        // is_nan() alone. An infinity is therefore written as an ordinary value and left out of
        // null_count, so the IS_NULL pushdown skipped the whole row group on "null_count == 0" and
        // dropped a row native storage returns.
        //
        // isNullOpPushable() now refuses IS NULL for DOUBLE, so the IS NULL arm below pins that
        // gate rather than the native writer_undercounts_nulls guard, which nothing pushes a
        // condition to any more. The "d > 1e308" and IS NOT NULL arms are the live pushdown here.
        //
        // The infinity has to arrive through a NON-CONSTANT expression: FunctionParser folds a
        // constant one through DoubleConstant#newInstance, which maps every non-finite value onto
        // NULL, so "1e308 * 10" stores a NaN and the writer and reader agree on it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (m DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO src VALUES (10.0, '2024-01-01T01:00:00.000000Z')");
            execute("CREATE TABLE inf (d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO inf VALUES (1.0, '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO inf SELECT 1e308 * m, ts FROM src");
            execute("INSERT INTO inf VALUES (2.0, '2024-01-02T00:00:00.000000Z')");

            // A genuine infinity, not a NaN: only an infinity is greater than the largest finite
            // double, and a NaN would fail this comparison.
            assertQuery("SELECT count() c FROM inf WHERE d > 1e308")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");

            final String nulls = """
                    d
                    null
                    """;
            final String nonNulls = """
                    d
                    1.0
                    2.0
                    """;
            assertQuery("SELECT d FROM inf WHERE d IS NULL").noLeakCheck().returns(nulls);
            assertQuery("SELECT d FROM inf WHERE d IS NOT NULL").noLeakCheck().returns(nonNulls);

            execute("ALTER TABLE inf CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            assertHasParquetPartitions("inf", true);

            assertQuery("SELECT d FROM inf WHERE d IS NULL").noLeakCheck().returns(nulls);
            assertQuery("SELECT d FROM inf WHERE d IS NOT NULL").noLeakCheck().returns(nonNulls);
        });
    }

    @Test
    public void testInfinityRowsSurviveNullEqualityPruning() throws Exception {
        // The same hole as testInfinityRowsSurviveIsNullPruning, on the EQ path rather than the
        // IS NULL one. `d = null::double` is not a bare NULL keyword, so it compiles to an ordinary
        // OP_EQ with a NaN bound; isExactEqDouble certifies NaN as exact because the native side
        // decides a NULL bound from the null count rather than the statistics. That count came from
        // a writer that calls only is_nan() null, so a row group holding an infinity - which
        // Numbers.equals calls EQUAL to NULL, and which EqDoubleFunctionFactory therefore matches
        // natively - reported has_nulls == false and was pruned away.
        //
        // Both signs, and both the statistics and the bloom paths: the bloom arm is the one that
        // runs when the group carries a bloom filter for the column, and it reads the same
        // has_nulls.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src2 (m DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO src2 VALUES (10.0, '2024-01-01T01:00:00.000000Z')");
            execute("CREATE TABLE inf2 (d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO inf2 VALUES (1.0, '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO inf2 SELECT 1e308 * m, ts FROM src2");
            execute("INSERT INTO inf2 SELECT -1e308 * m, '2024-01-01T02:00:00.000000Z'::TIMESTAMP FROM src2");
            execute("INSERT INTO inf2 VALUES (2.0, '2024-01-02T00:00:00.000000Z')");

            // Genuine infinities, not NaNs: only an infinity compares outside the finite range,
            // and a NaN would fail both comparisons.
            assertQuery("SELECT count() c FROM inf2 WHERE d > 1e308")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");
            assertQuery("SELECT count() c FROM inf2 WHERE d < -1e308")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");

            final String nulls = """
                    d
                    null
                    null
                    """;
            assertQuery("SELECT d FROM inf2 WHERE d = null::double").noLeakCheck().returns(nulls);

            execute("ALTER TABLE inf2 CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            assertHasParquetPartitions("inf2", true);

            assertQuery("SELECT d FROM inf2 WHERE d = null::double").noLeakCheck().returns(nulls);
            // IS NULL already took this branch before the EQ path did; asserting it here keeps the
            // two spellings of one predicate pinned to the same answer.
            assertQuery("SELECT d FROM inf2 WHERE d IS NULL").noLeakCheck().returns(nulls);
        });
    }

    @Test
    public void testCharColumnTopRowsSurviveEqualityPruning() throws Exception {
        // CHAR's column top decodes to (char) 0, which IS its NULL - but `c = null::char` is not
        // rewritten to IS NULL. It compiles to an ordinary equality against 0, and CHAR equality is
        // a raw comparison, so native storage matches those rows. The statistics never recorded the
        // zeros, so parquet pruned the row group and lost them. CHAR therefore needs the statistics
        // widening but not the IS NOT NULL guard BYTE and SHORT need.
        //
        // IS NULL is unaffected in THIS shape only: a column top is definition level 0, which does
        // reach null_count. A CHAR NULL stored as a value does not - see the sibling
        // testCharNullRowsSurviveIsNullPruning, which is the shape that broke IS NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cc (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO cc VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE cc ADD COLUMN c CHAR");
            execute("""
                    INSERT INTO cc VALUES
                    (3, '2024-01-01T02:00:00.000000Z', 'x'),
                    (4, '2024-01-02T00:00:00.000000Z', 'y')
                    """);

            final String expected = """
                    id
                    1
                    2
                    """;
            assertQuery("SELECT id FROM cc WHERE c = null::char").noLeakCheck().returns(expected);

            execute("ALTER TABLE cc CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            assertHasParquetPartitions("cc", true);

            assertQuery("SELECT id FROM cc WHERE c = null::char").noLeakCheck().returns(expected);
        });
    }

    @Test
    public void testCharNullRowsSurviveIsNullPruning() throws Exception {
        // A CHAR NULL is (char) 0 - an in-domain value the writer stores at definition level 1 like
        // any other, because `impl Nullable for u16` reports false unconditionally. Only a column
        // top reaches null_count for CHAR, so a row group whose NULLs were all stored as values
        // reports null_count == 0, and the IS NULL pushdown skipped it on that count and dropped
        // every row native storage returns.
        //
        // isNullOpPushable() now refuses IS NULL for CHAR, so the IS NULL arm below pins that gate
        // rather than the native writer_undercounts_nulls guard. The "c = null::char" arm is the
        // live pushdown, and it is what the closing assertion exercises.
        //
        // This is the shape the sibling testCharColumnTopRowsSurviveEqualityPruning does NOT cover:
        // its NULLs come from a column top, which IS counted, which is why IS NULL survived there.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cn (id INT, c CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO cn VALUES
                    (1, 'a', '2024-01-01T00:00:00.000000Z'),
                    (2, null, '2024-01-01T01:00:00.000000Z'),
                    (3, 'b', '2024-01-01T02:00:00.000000Z'),
                    (4, null, '2024-01-01T03:00:00.000000Z'),
                    (5, 'c', '2024-01-02T00:00:00.000000Z')
                    """);

            final String nulls = """
                    id
                    2
                    4
                    """;
            final String nonNulls = """
                    id
                    1
                    3
                    5
                    """;
            assertQuery("SELECT id FROM cn WHERE c IS NULL").noLeakCheck().returns(nulls);
            assertQuery("SELECT id FROM cn WHERE c IS NOT NULL").noLeakCheck().returns(nonNulls);
            assertQuery("SELECT id FROM cn WHERE c = null::char").noLeakCheck().returns(nulls);

            execute("ALTER TABLE cn CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            assertHasParquetPartitions("cn", true);

            // IS NOT NULL is the direction that was always correct: an uncounted null keeps
            // null_count below num_values, which declines to skip rather than over-pruning.
            assertQuery("SELECT id FROM cn WHERE c IS NULL").noLeakCheck().returns(nulls);
            assertQuery("SELECT id FROM cn WHERE c IS NOT NULL").noLeakCheck().returns(nonNulls);

            // The equality spelling in the shape that has null_count == 0, which is what makes the
            // two-predicate split load-bearing: CHAR is deliberately OUT of nulls_hidden_from_stats,
            // so has_nulls is false here. These rows survive only because a stored (char) 0 is an
            // ordinary value that lands in the min/max statistics - the premise that lets the value
            // paths keep working without help. If that premise were wrong this arm would lose rows.
            assertQuery("SELECT id FROM cn WHERE c = null::char").noLeakCheck().returns(nulls);
        });
    }

    @Test
    public void testCharValueOnByteColumnMatchesNative() throws Exception {
        // A CHAR bound against a BYTE column compares as the digit it spells: overload
        // resolution picks EqShortFunctionFactory, so the row filter reads '1' through
        // CharFunction.getShort -> castCharToNumber and sees 1, not the code point 49.
        // Two row groups with disjoint stats pin both halves of the contract. Pushing the
        // code point prunes BOTH groups and loses the row; declining the bound outright
        // returns the row but stops pruning the [8,9] group, so each failure mode trips a
        // different assertion below.
        // 4 is the floor PropServerConfiguration applies, so 8 rows give exactly two groups.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z'),
                    (4, '2024-01-01T03:00:00.000000Z'),
                    (6, '2024-01-01T04:00:00.000000Z'),
                    (7, '2024-01-01T05:00:00.000000Z'),
                    (8, '2024-01-01T06:00:00.000000Z'),
                    (9, '2024-01-01T07:00:00.000000Z'),
                    (101, '2024-01-02T02:00:00.000000Z')
                    """);
            // Only 2024-01-01 converts -- CONVERT skips the active partition -- giving two
            // row groups, [1,4] and [6,9].
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '1'")
                    .noLeakCheck()
                    .returns("""
                            val
                            1
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testColumnTopDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x ADD COLUMN val DOUBLE");
            execute("""
                    INSERT INTO x VALUES
                    (3, '2024-01-01T02:00:00.000000Z', 1.11),
                    (4, '2024-01-01T03:00:00.000000Z', 2.22),
                    (5, '2024-01-02T01:00:00.000000Z', 3.33)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT id, val FROM x WHERE val = 1.0")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQuery("SELECT id, val FROM x WHERE val = 1.11")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            3\t1.11
                            """);
            assertQuery("SELECT id, val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\tnull
                            2\tnull
                            """);
        });
    }

    @Test
    public void testColumnTopInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x ADD COLUMN val INT");
            execute("""
                    INSERT INTO x VALUES
                    (4, '2024-01-01T03:00:00.000000Z', 100),
                    (5, '2024-01-01T04:00:00.000000Z', 200),
                    (6, '2024-01-02T01:00:00.000000Z', 300)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT id, val FROM x WHERE val = 100")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            4\t100
                            """);
            assertQuery("SELECT id, val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\tnull
                            2\tnull
                            3\tnull
                            """);

            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
            assertQuery("SELECT id, val FROM x WHERE val = 999")
                    .noLeakCheck()
                    .returns("id\tval\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

        });
    }

    @Test
    public void testColumnTopLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x ADD COLUMN val LONG");
            execute("""
                    INSERT INTO x VALUES
                    (3, '2024-01-01T02:00:00.000000Z', 100_000),
                    (4, '2024-01-01T03:00:00.000000Z', 200_000),
                    (5, '2024-01-02T01:00:00.000000Z', 300_000)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT id, val FROM x WHERE val = 100_000")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            3\t100000
                            """);
            assertQuery("SELECT id, val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\tnull
                            2\tnull
                            """);
        });
    }

    @Test
    public void testColumnTopString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x ADD COLUMN val STRING");
            execute("""
                    INSERT INTO x VALUES
                    (4, '2024-01-01T03:00:00.000000Z', 'hello'),
                    (5, '2024-01-01T04:00:00.000000Z', 'world'),
                    (6, '2024-01-02T01:00:00.000000Z', 'world1')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            assertQuery("SELECT id, val FROM x WHERE val = 'aaa'")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT id, val FROM x WHERE val = 'hello'")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            4\thello
                            """);
            assertQuery("SELECT id, val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\t
                            2\t
                            3\t
                            """);
        });
    }

    @Test
    public void testColumnTopVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x ADD COLUMN val VARCHAR");
            execute("""
                    INSERT INTO x VALUES
                    (3, '2024-01-01T02:00:00.000000Z', 'abc'),
                    (4, '2024-01-01T03:00:00.000000Z', 'def'),
                    (5, '2024-01-02T02:00:00.000000Z', 'def2')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            assertQuery("SELECT id, val FROM x WHERE val = 'aaa'")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQuery("SELECT id, val FROM x WHERE val = 'abc'")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            3\tabc
                            """);
            assertQuery("SELECT id, val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\t
                            2\t
                            """);
        });
    }

    @Test
    public void testCombinedFilters() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val > 100 AND val < 120")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n19\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 10_000 AND val IS NOT NULL")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testHasParquetPartitionsFlagAfterConvertBackToNative() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z'),
                    (3, '2024-01-03T00:00:00.000000Z')
                    """);
            assertHasParquetPartitions("x", false);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            assertHasParquetPartitions("x", true);

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE WHERE ts >= 0");
            assertHasParquetPartitions("x", false);
        });
    }

    @Test
    public void testHasParquetPartitionsFlagFollowsReaderSnapshotCopy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            final TableToken tableToken = engine.verifyTableName("x");
            try (TableReader sourceReader = engine.getReader(tableToken)) {
                Assert.assertTrue(sourceReader.hasParquetPartitions());

                execute("ALTER TABLE x CONVERT PARTITION TO NATIVE WHERE ts >= 0");
                Assert.assertTrue(sourceReader.hasParquetPartitions());

                try (TableReader snapshotCopy = engine.getReaderAtTxn(sourceReader, sqlExecutionContext)) {
                    Assert.assertTrue(snapshotCopy.hasParquetPartitions());
                }
            }

            try (TableReader latestReader = engine.getReader(tableToken)) {
                Assert.assertFalse(latestReader.hasParquetPartitions());
            }
        });
    }

    @Test
    public void testHasParquetPartitionsFlagAfterDetachPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z'),
                    (3, '2024-01-03T00:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts = '2024-01-01'");
            assertHasParquetPartitions("x", true);

            execute("ALTER TABLE x DETACH PARTITION WHERE ts = '2024-01-01'");
            assertHasParquetPartitions("x", false);
        });
    }

    @Test
    public void testHasParquetPartitionsFlagAfterDropPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z'),
                    (3, '2024-01-03T00:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts = '2024-01-01'");
            assertHasParquetPartitions("x", true);

            execute("ALTER TABLE x DROP PARTITION WHERE ts = '2024-01-01'");
            assertHasParquetPartitions("x", false);
        });
    }

    @Test
    public void testHasParquetPartitionsFlagAfterTruncate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts = '2024-01-01'");
            assertHasParquetPartitions("x", true);

            execute("TRUNCATE TABLE x");
            assertHasParquetPartitions("x", false);
        });
    }

    @Test
    public void testHasParquetPartitionsFlagMixedPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z'),
                    (3, '2024-01-03T00:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts = '2024-01-01'");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts = '2024-01-02'");
            assertHasParquetPartitions("x", true);

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE WHERE ts = '2024-01-01'");
            assertHasParquetPartitions("x", true);

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE WHERE ts = '2024-01-02'");
            assertHasParquetPartitions("x", false);
        });
    }

    @Test
    public void testInListWithNullDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1.11, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (3.33, '2024-01-01T02:00:00.000000Z'),
                    (4.44, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val IN (null, 3.34)")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    @Test
    public void testInListWithNullInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z'),
                    (5, '2024-01-01T03:00:00.000000Z'),
                    (6, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val IN (null, 3)")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            3
                            """);
            assertQuery("SELECT val FROM x WHERE val IN (null)")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            """);
            assertQuery("SELECT val FROM x WHERE val IN (null, 99)")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    @Test
    public void testInListWithNullLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (100_000, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (300_000, '2024-01-01T02:00:00.000000Z'),
                    (400_000, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val IN (null, 300_000)")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            300000
                            """);
        });
    }

    @Test
    public void testInListWithNullString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('aaa', '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    ('ccc', '2024-01-01T02:00:00.000000Z'),
                    ('ddd', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val IN (null, 'cccd')")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            """);
        });
    }

    @Test
    public void testInListWithNullVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('hello', '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    ('world', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val IN (null, 'world')")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            world
                            """);
        });
    }

    @Test
    public void testIntervalScanStringMultiBlockPage() throws Exception {
        // Force a large row-group size so all 300 rows land in one row group (one
        // data page) regardless of execution order. The property is a static
        // override that persists across test methods (reset only in @AfterClass),
        // and many sibling tests lower it to 100; a value below 128 would split the
        // rows into single-block row groups whose length stream never spans multiple
        // blocks, silently bypassing the partial multi-block read path this guards.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100_000);
        // A STRING column read over a partial row-group range. 300 rows land in a
        // single row group whose one data page holds a DELTA_LENGTH_BYTE_ARRAY
        // length stream that spans several 128-value blocks. An interval ending
        // inside the row group makes the column read stop before the later blocks
        // (rowGroupHi < the page's value count). The data offset must still skip
        // the whole length stream; it used to under-count the unread blocks and
        // return shifted values (e.g. "v1" decoded as a string with leading NULs).
        // Existing STRING tests miss this: they filter on the value column (a
        // different, immune decode path) and use only a handful of rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT 'v' || x, timestamp_sequence('2024-01-01', 60_000_000)
                    FROM long_sequence(300)
                    """);
            // A row in the next partition so 2024-01-01 is not the active partition
            // and CONVERT actually rewrites it to parquet.
            execute("INSERT INTO x VALUES ('tail', '2024-01-02T00:00:00.000000Z')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE ts < '2024-01-01T00:05:00.000000Z'")
                    .noLeakCheck()
                    .returns("""
                            val
                            v1
                            v2
                            v3
                            v4
                            v5
                            """);
        });
    }

    @Test
    public void testIntervalScanStringMultiBlockPageBackward() throws Exception {
        // Force a large row-group size for the same reason as
        // testIntervalScanStringMultiBlockPage: keep all 300 rows in one multi-block
        // page so a sibling test's lowered override cannot mask the partial-read path.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100_000);
        // As testIntervalScanStringMultiBlockPage but with descending timestamp
        // order, which drives the backward page-frame cursor. It computes the same
        // partial (rowGroupHi < value count) frame and reads the STRING column over
        // it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT 'v' || x, timestamp_sequence('2024-01-01', 60_000_000)
                    FROM long_sequence(300)
                    """);
            // A row in the next partition so 2024-01-01 is not the active partition
            // and CONVERT actually rewrites it to parquet.
            execute("INSERT INTO x VALUES ('tail', '2024-01-02T00:00:00.000000Z')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE ts < '2024-01-01T00:05:00.000000Z' ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("""
                            val
                            v5
                            v4
                            v3
                            v2
                            v1
                            """);
        });
    }

    @Test
    public void testIsNotNullAllNullsRowGroup() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT NULL, timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(100)
                    """);
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01T16:40:00', 600_000_000)
                    FROM long_sequence(50)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val IS NOT NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testIsNotNullFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (NULL, '2024-01-01T00:00:00.000000Z'),
                    (NULL, '2024-01-01T01:00:00.000000Z'),
                    (42, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IS NOT NULL")
                    .noLeakCheck()
                    .returns("""
                            val
                            42
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testIsNotNullOverBooleanColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            createColumnTopParquetTable("BOOLEAN", "true");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt, min(val) AS lo, max(val) AS hi FROM x WHERE c2 IS NOT NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\tlo\thi\n501\t1\t8000\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testIsNotNullOverByteColumnTop() throws Exception {
        // BYTE, SHORT and BOOLEAN carry no null sentinel: EqByteFunctionFactory and its two
        // siblings fold a null constant to BooleanConstant.FALSE, so "c2 IS NOT NULL" holds
        // for every stored row, column-top rows included - the native twin returns all 501.
        //
        // The parquet writer still marks column-top rows with definition level 0, so a row
        // group built entirely from them reports null_count == num_values. Row-group pruning
        // reads that as "no row can match" and discards the group, dropping 500 rows.
        //
        // Parquet nulls encode column tops for these types, not SQL NULLs, so
        // PushdownFilterExtractor must not push a null op for them at all. The CHAR sibling
        // below is the control: CHAR has a real sentinel, its column-top rows genuinely are
        // NULL, and its pruning must survive the gate.
        assertMemoryLeak(() -> {
            createColumnTopParquetTable("BYTE", "7");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt, min(val) AS lo, max(val) AS hi FROM x WHERE c2 IS NOT NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\tlo\thi\n501\t1\t8000\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testIsNotNullOverCharColumnTop() throws Exception {
        // Control for the three tests above. CHAR's null is Numbers.CHAR_NULL, so a
        // column-top row reads back as a genuine SQL NULL and the parquet null bit means
        // exactly what the predicate asks about. Pruning stays correct and must keep firing.
        assertMemoryLeak(() -> {
            createColumnTopParquetTable("CHAR", "'a'");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt, min(val) AS lo, max(val) AS hi FROM x WHERE c2 IS NOT NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\tlo\thi\n1\t8000\t8000\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testIsNotNullOverDoubleColumnTop() throws Exception {
        // Over-broad-gate control for the FLOAT/DOUBLE arm, the counterpart of the CHAR one.
        // Only IS NULL is unsound for these types; a column-top row reads back as a genuine
        // DOUBLE null, so IS NOT NULL still prunes exactly and must keep doing so. Widening the
        // arm to refuse both ops would cost pruning on a very common predicate, silently.
        assertMemoryLeak(() -> {
            createColumnTopParquetTable("DOUBLE", "1.5");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt, min(val) AS lo, max(val) AS hi FROM x WHERE c2 IS NOT NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\tlo\thi\n1\t8000\t8000\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testIsNotNullOverShortColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            createColumnTopParquetTable("SHORT", "7");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt, min(val) AS lo, max(val) AS hi FROM x WHERE c2 IS NOT NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\tlo\thi\n501\t1\t8000\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testIsNullFilter() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IS NULL")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testIsNullNoNullsRowGroup() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(100)
                    """);
            execute("""
                    INSERT INTO x
                    SELECT NULL, timestamp_sequence('2024-01-01T16:40:00', 600_000_000)
                    FROM long_sequence(50)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val IS NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testIsNullOverStoredCharNull() throws Exception {
        // The mirror of the IS NOT NULL column-top hole, in the opposite direction. CHAR's SQL
        // null is Numbers.CHAR_NULL, but the parquet writer's Nullable impl for u16 returns
        // false unconditionally (core/rust/qdbr/src/parquet_write/mod.rs:92-97), so a stored
        // char 0 goes to the file as a NON-null value. A row group full of them reports
        // null_count == 0, which pruning reads as "no row can be null" and discards for
        // IS NULL - dropping the very rows that match.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, c2 CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT x::INT, NULL::CHAR, timestamp_sequence('2024-01-01', 100_000)
                    FROM long_sequence(500)
                    """);
            execute("INSERT INTO x VALUES (8000, 'a', '2024-01-02T02:00:00.000000Z')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt, min(val) AS lo, max(val) AS hi FROM x WHERE c2 IS NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\tlo\thi\n500\t1\t500\n");
        });
    }

    @Test
    public void testIsNullOverStoredDoubleInfinity() throws Exception {
        // FLOAT and DOUBLE have CHAR's shape too, and it is easy to miss because the usual NULL,
        // NaN, behaves. Numbers.isNull(double) masks EXP_BIT_MASK and Numbers.isNull(float)
        // tests isInfinite, so +/-Infinity is SQL NULL - but the writer's Nullable impls for
        // f32/f64 test only is_nan(), and simd.rs compares strictly greater than the infinity
        // bits, so an infinity is stored as an ordinary value. A row group of them reports
        // null_count == 0 and IS NULL pruning discards it, dropping every matching row.
        //
        // The infinity must come from a runtime expression: written as a literal, 'Infinity'
        // and 1e308 * 10 constant-fold to a NULL DoubleConstant and store NaN, which the writer
        // does mark null - so the folded form cannot reproduce this.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (a DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO src
                    SELECT 1e308, timestamp_sequence('2024-01-01', 100_000)
                    FROM long_sequence(500)
                    """);
            execute("CREATE TABLE x (val INT, c2 DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x SELECT 1, a * a, ts FROM src");
            execute("INSERT INTO x VALUES (8000, 1.5, '2024-01-02T02:00:00.000000Z')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE c2 IS NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n500\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testIsNullOverStoredFloatInfinity() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (a FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO src
                    SELECT 1e38::FLOAT, timestamp_sequence('2024-01-01', 100_000)
                    FROM long_sequence(500)
                    """);
            execute("CREATE TABLE x (val INT, c2 FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x SELECT 1, a * a, ts FROM src");
            execute("INSERT INTO x VALUES (8000, 1.5, '2024-01-02T02:00:00.000000Z')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE c2 IS NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n500\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testIsNullOverStoredIPv4Null() throws Exception {
        // IPv4's null is 0, an in-band value like CHAR's. This pins the writer mapping that 0
        // to a parquet null, which is what keeps IPv4 out of the gate: if the writer regressed
        // to storing it as an ordinary value, IS NULL pruning would gain CHAR's hole and this
        // goes red. It is not a control against over-broad gating - blocking IPv4 only costs
        // pruning, so the answer would stay 500 either way.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, c2 IPV4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT x::INT, NULL::IPV4, timestamp_sequence('2024-01-01', 100_000)
                    FROM long_sequence(500)
                    """);
            execute("INSERT INTO x VALUES (8000, '10.0.0.1', '2024-01-02T02:00:00.000000Z')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt, min(val) AS lo, max(val) AS hi FROM x WHERE c2 IS NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\tlo\thi\n500\t1\t500\n");
        });
    }

    @Test
    public void testLatestByNativePlanInvalidatedAfterParquetConversion() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL, val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT 'a', x::VARCHAR, timestamp_sequence('2024-01-01', 100_000)
                    FROM long_sequence(5000)
                    """);
            execute("INSERT INTO x VALUES ('b', 'active', '2024-01-02T00:00:00.000000Z')");

            final String query =
                    "SELECT sym, val, ts FROM x WHERE val = '250' LATEST ON ts PARTITION BY sym";
            try (RecordCursorFactory factory = select(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertFalse(cursor.hasNext());
                }

                execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("expected cached latest-by native plan to be invalidated");
                } catch (TableReferenceOutOfDateException ignored) {
                }
            }

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery(query)
                    .expectSize()
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            sym\tval\tts
                            a\t250\t2024-01-01T00:00:24.900000Z
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testLimitOverConstantFoldedByteNullFilter() throws Exception {
        // The query QueryFuzzTest found for aa809bb54f ("Fix LIMIT under-count on
        // pushdown-pruned skip"), kept as a correctness pin.
        //
        // BYTE carries no null sentinel, so EqByteFunctionFactory folds "val IS NOT NULL" to
        // constant TRUE and the code generator drops the filter, leaving the page-frame factory
        // unwrapped. That used to pair with active pushdown, because PushdownFilterExtractor
        // read the expression node before the compiler folded it. filter == null together with
        // active pushdown is the single state that reaches PageFrameRecordCursorImpl
        // .skipRows()'s slow path with the decode clamp armed, and it under-counted the nested
        // LIMIT: the walk charged its own rows against a clamp of 0, so the skip skipped
        // nothing and calculateSize() reported 0 for a cursor yielding 16.
        //
        // PushdownFilterExtractor.isNullOpPushable() now refuses to push a null op for BYTE,
        // because a parquet null bit denotes a column top there rather than a SQL NULL. That
        // closes the route: pushdown stays inactive, so size() reports the real row count and
        // the outer LIMIT never has to size itself through skipRows(). The skipRows() guard
        // stays and still matters - active pushdown coexists with canClamp for an ordinary
        // non-constant predicate, which is what testSkipRowsWithFiniteBoundUnderActivePushdown
        // drives directly; no query shape was found that reaches it through a LIMIT any more,
        // but that is an absence of evidence rather than a proof.
        //
        // So this pins two things: the folded BYTE predicate leaves pushdown inactive, and the
        // fuzz query still yields its 16 rows with calculateSize() agreeing.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT (x % 100)::BYTE, timestamp_sequence('2024-01-01', 60_000_000)
                    FROM long_sequence(500)
                    """);
            // Second partition makes 2024-01-01 a non-active partition so it converts.
            execute("INSERT INTO x VALUES (99, '2024-01-02T02:00:00.000000Z')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Landing on the page-frame factory rather than a filter wrapper still proves the
            // WHERE folded away, and a real size() on that entity cursor proves the null op no
            // longer activates pushdown - which is what keeps the outer LIMIT off skipRows().
            try (RecordCursorFactory factory = select("SELECT * FROM x WHERE val IS NOT NULL")) {
                RecordCursorFactory scanFactory = factory;
                if (scanFactory instanceof QueryProgress) {
                    scanFactory = scanFactory.getBaseFactory();
                }
                Assert.assertTrue(
                        "the folded WHERE must leave the page-frame factory unwrapped [factory="
                                + scanFactory.getClass().getSimpleName() + ']',
                        scanFactory instanceof PageFrameRecordCursorFactory
                );
                try (RecordCursor cursor = scanFactory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(501, cursor.size());
                }
            }

            // The base scan now reports a real size, so the outer LIMIT knows its size up front
            // instead of deriving it through skipRows(); .expectSize() pins that.
            assertQuery("WITH cte0 AS (SELECT * FROM x WHERE val IS NOT NULL LIMIT 16) SELECT * FROM cte0 LIMIT 40")
                    .timestamp("ts")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            val\tts
                            1\t2024-01-01T00:00:00.000000Z
                            2\t2024-01-01T00:01:00.000000Z
                            3\t2024-01-01T00:02:00.000000Z
                            4\t2024-01-01T00:03:00.000000Z
                            5\t2024-01-01T00:04:00.000000Z
                            6\t2024-01-01T00:05:00.000000Z
                            7\t2024-01-01T00:06:00.000000Z
                            8\t2024-01-01T00:07:00.000000Z
                            9\t2024-01-01T00:08:00.000000Z
                            10\t2024-01-01T00:09:00.000000Z
                            11\t2024-01-01T00:10:00.000000Z
                            12\t2024-01-01T00:11:00.000000Z
                            13\t2024-01-01T00:12:00.000000Z
                            14\t2024-01-01T00:13:00.000000Z
                            15\t2024-01-01T00:14:00.000000Z
                            16\t2024-01-01T00:15:00.000000Z
                            """);
        });
    }

    @Test
    public void testMinMaxPruningByte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z'),
                    (50, '2024-01-01T03:00:00.000000Z'),
                    (100, '2024-01-01T04:00:00.000000Z'),
                    (110, '2024-01-02T03:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 2")
                    .noLeakCheck()
                    .returns("""
                            val
                            2
                            """);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -1")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningByteNegative() throws Exception {
        // BYTE is INT32-backed in parquet. Inline stats round-trip through a
        // u64 slot at INT32 physical width, so a negative min value must read
        // back as the correct i32 in the skip path. Without that, predicates
        // like val = 0 against a row group whose true min is negative drop
        // every row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (-100, '2024-01-01T00:00:00.000000Z'),
                    (-50, '2024-01-01T01:00:00.000000Z'),
                    (-1, '2024-01-01T02:00:00.000000Z'),
                    (0, '2024-01-01T03:00:00.000000Z'),
                    (10, '2024-01-01T04:00:00.000000Z'),
                    (50, '2024-01-02T03:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // val = 0 hits the row group whose true min is -100; before the
            // round-trip fix the inline min read back as 156, dropping the
            // row group. Must not skip.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 0")
                    .noLeakCheck()
                    .returns("""
                            val
                            0
                            """);

            // val = -42 falls inside [-100, 10] but not in the data: must not
            // be skipped, must return empty.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -42")
                    .noLeakCheck()
                    .returns("val\n");

            // val = -127 is outside both row groups; should skip.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -127")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('A', '2024-01-01T00:00:00.000000Z'),
                    ('B', '2024-01-01T01:00:00.000000Z'),
                    ('C', '2024-01-01T02:00:00.000000Z'),
                    ('X', '2024-01-01T03:00:00.000000Z'),
                    ('Y', '2024-01-01T04:00:00.000000Z'),
                    ('Z', '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 'C'")
                    .noLeakCheck()
                    .returns("""
                            val
                            C
                            """);

            assertQuery("SELECT val FROM x WHERE val = 'c'")
                    .noLeakCheck()
                    .returns("""
                            val
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('2020-01-01'::DATE, '2024-01-01T00:00:00.000000Z'),
                    ('2020-06-01'::DATE, '2024-01-01T01:00:00.000000Z'),
                    ('2020-12-31'::DATE, '2024-01-01T02:00:00.000000Z'),
                    ('2021-12-31'::DATE, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '2020-06-01'::DATE")
                    .noLeakCheck()
                    .returns("""
                            val
                            2020-06-01T00:00:00.000Z
                            """);
            assertQuery("SELECT val FROM x WHERE val = '2099-01-01'::DATE")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimal128() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(30,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '5000000000000.50'::DECIMAL(30,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5000000000000.50
                            """);
            assertQuery("SELECT val FROM x WHERE val = '100.10'::DECIMAL(30,2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimal16() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(4,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('10.10', '2024-01-01T00:00:00.000000Z'),
                    ('50.50', '2024-01-01T01:00:00.000000Z'),
                    ('99.99', '2024-01-01T02:00:00.000000Z'),
                    ('99.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '50.50'::DECIMAL(4,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            50.50
                            """);
            assertQuery("SELECT val FROM x WHERE val = '1.01'::DECIMAL(4,2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimal256() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(50,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('100000000000000000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('500000000000000000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('999999999999999999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('999999999999999999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '500000000000000000000.50'::DECIMAL(50,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            500000000000000000000.50
                            """);
            assertQuery("SELECT val FROM x WHERE val = '10.10'::DECIMAL(50,2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimal32() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(8,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000.10', '2024-01-01T00:00:00.000000Z'),
                    ('50000.50', '2024-01-01T01:00:00.000000Z'),
                    ('99999.99', '2024-01-01T02:00:00.000000Z'),
                    ('99999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '50000.50'::DECIMAL(8,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            50000.50
                            """);
            assertQuery("SELECT val FROM x WHERE val = '100.10'::DECIMAL(8,2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimal64() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '5000000.50'::DECIMAL(15,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5000000.50
                            """);
            assertQuery("SELECT val FROM x WHERE val = '100.10'::DECIMAL(15,2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimal8() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(2,1), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1.1', '2024-01-01T00:00:00.000000Z'),
                    ('5.5', '2024-01-01T01:00:00.000000Z'),
                    ('9.9', '2024-01-01T02:00:00.000000Z'),
                    ('9.8', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '5.5'::DECIMAL(2,1)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5.5
                            """);
            assertQuery("SELECT val FROM x WHERE val = '0.1'::DECIMAL(2,1)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimalRescaleScaleUp() throws Exception {
        // PushdownFilterExtractor#rescaleDecimalForPushdown rebuilds the literal at
        // the column's scale when the literal scale is smaller. Pushdown still works
        // because the rescaled raw value matches the column's row group statistics.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // literal scale 1 < column scale 2, value rescales to 50000005 (scale 2)
            assertQuery("SELECT val FROM x WHERE val = '5000000.5'::DECIMAL(15, 1)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5000000.50
                            """);
            // out-of-range literal: rescale produces a value smaller than the row
            // group min, so the second partition's row group is pruned.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '100.1'::DECIMAL(15, 1)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimalRescaleScaleDownLossless() throws Exception {
        // Literal carries trailing zeros beyond the column's scale, so the rescale
        // is lossless; pushdown keeps working with the rebuilt constant.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '5000000.500'::DECIMAL(15, 3)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5000000.50
                            """);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '100.100'::DECIMAL(15, 3)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimalRescaleScaleDownLossy() throws Exception {
        // Literal has non-zero fractional digits beyond the column's scale, so
        // rescaling would lose precision and pushdown is abandoned. The runtime
        // filter still correctly returns no matching rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '5000000.501'::DECIMAL(15, 3)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testMinMaxPruningDecimalRescaleWiderLiteralFits() throws Exception {
        // Literal is DECIMAL128 but its value fits in the column's DECIMAL64
        // storage. The helper rebuilds the constant at the narrower tag so
        // pushdown's getDecimal64 dispatch produces the right value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '5000000.50'::DECIMAL(30, 2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5000000.50
                            """);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '100.10'::DECIMAL(30, 2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimalRescaleWiderLiteralOverflows() throws Exception {
        // Literal value is larger than the column's DECIMAL64 storage can hold,
        // so the helper abandons pushdown. The runtime filter still produces the
        // correct (full) result for `c <= huge_literal`.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val <= '99999999999999999999999999.99'::DECIMAL(30, 2) ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            1000000.10
                            5000000.50
                            9999999.98
                            9999999.99
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testMinMaxPruningDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1.11, '2024-01-01T00:00:00.000000Z'),
                    (2.22, '2024-01-01T01:00:00.000000Z'),
                    (3.33, '2024-01-01T02:00:00.000000Z'),
                    (4.44, '2024-01-01T03:00:00.000000Z'),
                    (5.55, '2024-01-01T04:00:00.000000Z'),
                    (5.56, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 3.33")
                    .noLeakCheck()
                    .returns("""
                            val
                            3.33
                            """);
            assertQuery("SELECT val FROM x WHERE val = 99.99")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1.5, '2024-01-01T00:00:00.000000Z'),
                    (2.5, '2024-01-01T01:00:00.000000Z'),
                    (3.5, '2024-01-01T02:00:00.000000Z'),
                    (4.5, '2024-01-01T03:00:00.000000Z'),
                    (5.5, '2024-01-01T04:00:00.000000Z'),
                    (5.6, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 3.5::FLOAT")
                    .noLeakCheck()
                    .returns("""
                            val
                            3.5
                            """);
            assertQuery("SELECT val FROM x WHERE val = 99.9::FLOAT")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningGeoByte() throws Exception {
        // GeoByte rides the same INT32-physical inline path as BYTE: the _pm
        // slot now holds parquet i32 stats verbatim instead of a 1-byte
        // narrow encoding. The skip path doesn't currently push geohash
        // equality through the row-group filter, but values must still
        // round-trip correctly through the parquet conversion.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val GEOHASH(1c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (#0, '2024-01-01T00:00:00.000000Z'),
                    (#1, '2024-01-01T01:00:00.000000Z'),
                    (#2, '2024-01-01T02:00:00.000000Z'),
                    (#3, '2024-01-01T03:00:00.000000Z'),
                    (#z, '2024-01-02T03:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = #2")
                    .noLeakCheck()
                    .returns("""
                            val
                            2
                            """);

            // #y matches no row in either partition; the query must return
            // empty without misclassifying the row groups.
            assertQuery("SELECT val FROM x WHERE val = #y")
                    .noLeakCheck()
                    .returns("val\n");

            // Full scan with timestamp ordering still has to read every value
            // through the parquet reader, regardless of whether the row group
            // filter fires.
            assertQuery("SELECT val, ts FROM x ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            val\tts
                            0\t2024-01-01T00:00:00.000000Z
                            1\t2024-01-01T01:00:00.000000Z
                            2\t2024-01-01T02:00:00.000000Z
                            3\t2024-01-01T03:00:00.000000Z
                            z\t2024-01-02T03:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testMinMaxPruningGeoShort() throws Exception {
        // GeoShort rides the same INT32-physical inline path as SHORT: the
        // _pm slot now holds parquet i32 stats verbatim instead of a 2-byte
        // narrow encoding. The skip path doesn't currently push geohash
        // equality through the row-group filter, but values must still
        // round-trip correctly through the parquet conversion.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val GEOHASH(3c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (#000, '2024-01-01T00:00:00.000000Z'),
                    (#001, '2024-01-01T01:00:00.000000Z'),
                    (#002, '2024-01-01T02:00:00.000000Z'),
                    (#003, '2024-01-01T03:00:00.000000Z'),
                    (#zzz, '2024-01-02T03:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = #002")
                    .noLeakCheck()
                    .returns("""
                            val
                            002
                            """);

            // #yyy matches no row in either partition; the query must return
            // empty without misclassifying the row groups.
            assertQuery("SELECT val FROM x WHERE val = #yyy")
                    .noLeakCheck()
                    .returns("val\n");

            assertQuery("SELECT val, ts FROM x ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            val\tts
                            000\t2024-01-01T00:00:00.000000Z
                            001\t2024-01-01T01:00:00.000000Z
                            002\t2024-01-01T02:00:00.000000Z
                            003\t2024-01-01T03:00:00.000000Z
                            zzz\t2024-01-02T03:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testMinMaxPruningIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val IPv4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1.1.1.1', '2024-01-01T00:00:00.000000Z'),
                    ('10.0.0.1', '2024-01-01T01:00:00.000000Z'),
                    (NULL, '2024-01-01T02:00:00.000000Z'),
                    ('192.168.1.1', '2024-01-01T03:00:00.000000Z'),
                    ('192.168.1.2', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '10.0.0.1'")
                    .noLeakCheck()
                    .returns("""
                            val
                            10.0.0.1
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '1.1.1.0'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val in (NULL)")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IN ('192.168.1.1', '192.168.1.2') ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            192.168.1.1
                            192.168.1.2
                            """);
        });
    }

    @Test
    public void testMinMaxPruningInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (10_000, '2024-01-01T00:00:00.000000Z'),
                    (20_000, '2024-01-01T01:00:00.000000Z'),
                    (30_000, '2024-01-01T02:00:00.000000Z'),
                    (40_000, '2024-01-01T03:00:00.000000Z'),
                    (50_000, '2024-01-01T04:00:00.000000Z'),
                    (60_000, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 30_000")
                    .noLeakCheck()
                    .returns("""
                            val
                            30000
                            """);
            assertQuery("SELECT val FROM x WHERE val = 99_999")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningIntBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (10_000, '2024-01-01T00:00:00.000000Z'),
                    (20_000, '2024-01-01T01:00:00.000000Z'),
                    (30_000, '2024-01-01T02:00:00.000000Z'),
                    (40_000, '2024-01-01T03:00:00.000000Z'),
                    (50_000, '2024-01-01T04:00:00.000000Z'),
                    (60_000, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            bindVariableService.clear();
            bindVariableService.setInt("v", 30_000);
            assertQuery("SELECT val FROM x WHERE val = :v")
                    .noLeakCheck()
                    .returns("val\n30000\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt("v", 99_999);
            assertQuery("SELECT val FROM x WHERE val = :v")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt(0, 99_999);
            assertQuery("SELECT val FROM x WHERE val = $1")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningIntInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (10_000, '2024-01-01T00:00:00.000000Z'),
                    (20_000, '2024-01-01T01:00:00.000000Z'),
                    (30_000, '2024-01-01T02:00:00.000000Z'),
                    (40_000, '2024-01-01T03:00:00.000000Z'),
                    (50_000, '2024-01-01T04:00:00.000000Z'),
                    (60_000, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val IN (10_000, 50_000)")
                    .noLeakCheck()
                    .returns("""
                            val
                            10000
                            50000
                            """);
            assertQuery("SELECT val FROM x WHERE val IN (99_998, 99_999)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (100_000, '2024-01-01T00:00:00.000000Z'),
                    (200_000, '2024-01-01T01:00:00.000000Z'),
                    (300_000, '2024-01-01T02:00:00.000000Z'),
                    (400_000, '2024-01-01T03:00:00.000000Z'),
                    (500_000, '2024-01-01T04:00:00.000000Z'),
                    (600_000, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 300_000")
                    .noLeakCheck()
                    .returns("""
                            val
                            300000
                            """);
            assertQuery("SELECT val FROM x WHERE val = 999_999")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningLong128() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG128, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (to_long128(0, 1), '2024-01-01T00:00:00.000000Z'),
                    (to_long128(0, 50), '2024-01-01T01:00:00.000000Z'),
                    (to_long128(0, 100), '2024-01-01T02:00:00.000000Z'),
                    (to_long128(0, 101), '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = to_long128(0, 50)")
                    .noLeakCheck()
                    .returns("""
                            val
                            00000000-0000-0032-0000-000000000000
                            """);
            assertQuery("SELECT val FROM x WHERE val = to_long128(0, 999)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (100, '2024-01-01T00:00:00.000000Z'),
                    (200, '2024-01-01T01:00:00.000000Z'),
                    (300, '2024-01-01T02:00:00.000000Z'),
                    (400, '2024-01-01T03:00:00.000000Z'),
                    (500, '2024-01-01T04:00:00.000000Z'),
                    (600, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 300")
                    .noLeakCheck()
                    .returns("""
                            val
                            300
                            """);
            assertQuery("SELECT val FROM x WHERE val = 999")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningShortNegative() throws Exception {
        // SHORT is INT32-backed in parquet. The skip path reads inline stats
        // at INT32 physical width, so a negative min must round-trip through
        // the u64 slot as the correct i32. A SHORT column with min -74 must
        // not appear as an unsigned 65462 to the skip path; otherwise every
        // row group whose true min is negative gets dropped.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (-30_000, '2024-01-01T00:00:00.000000Z'),
                    (-200, '2024-01-01T01:00:00.000000Z'),
                    (-74, '2024-01-01T02:00:00.000000Z'),
                    (0, '2024-01-01T03:00:00.000000Z'),
                    (300, '2024-01-01T04:00:00.000000Z'),
                    (29_000, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // val = 0 must not skip the row group whose min is -30_000.
            // An unsigned-extended inline min of 35536 would be greater than 0
            // and would drop every match.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 0")
                    .noLeakCheck()
                    .returns("""
                            val
                            0
                            """);

            // val = -74 is the literal value from the fuzzer reproduction.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -74")
                    .noLeakCheck()
                    .returns("""
                            val
                            -74
                            """);

            // val = -32_000 sits below the row group min (-30_000); should skip.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -32_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('abc', '2024-01-01T00:00:00.000000Z'),
                    ('bbb', '2024-01-01T01:00:00.000000Z'),
                    ('ccc', '2024-01-01T02:00:00.000000Z'),
                    ('xxx', '2024-01-01T03:00:00.000000Z'),
                    ('zzz', '2024-01-01T04:00:00.000000Z'),
                    ('yyy', '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 'ccc'")
                    .noLeakCheck()
                    .returns("""
                            val
                            ccc
                            """);
            assertQuery("SELECT val FROM x WHERE val = 'aaa'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('alpha', '2024-01-01T00:00:00.000000Z'),
                    ('beta', '2024-01-01T01:00:00.000000Z'),
                    ('gamma', '2024-01-01T02:00:00.000000Z'),
                    ('delta', '2024-01-01T03:00:00.000000Z'),
                    ('epsilon', '2024-01-01T04:00:00.000000Z'),
                    ('epsilon1', '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 'gamma'")
                    .noLeakCheck()
                    .returns("""
                            val
                            gamma
                            """);
            assertQuery("SELECT val FROM x WHERE val = 'aa'")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('2020-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    ('2020-06-01T00:00:00.000000Z', '2024-01-01T01:00:00.000000Z'),
                    ('2020-12-31T00:00:00.000000Z', '2024-01-01T02:00:00.000000Z'),
                    ('2021-12-31T00:00:00.000000Z', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '2020-06-01T00:00:00.000000Z'::TIMESTAMP")
                    .noLeakCheck()
                    .returns("""
                            val
                            2020-06-01T00:00:00.000000Z
                            """);
            assertQuery("SELECT val FROM x WHERE val = '2099-01-01T00:00:00.000000Z'::TIMESTAMP")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:00.000000Z'),
                    ('22222222-2222-2222-2222-222222222222', '2024-01-01T01:00:00.000000Z'),
                    ('33333333-3333-3333-3333-333333333333', '2024-01-01T02:00:00.000000Z'),
                    ('33333333-3333-3333-3333-333333333334', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '22222222-2222-2222-2222-222222222222'")
                    .noLeakCheck()
                    .returns("""
                            val
                            22222222-2222-2222-2222-222222222222
                            """);
            assertQuery("SELECT val FROM x WHERE val = 'ffffffff-ffff-ffff-ffff-ffffffffffff'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('hello', '2024-01-01T00:00:00.000000Z'),
                    ('world', '2024-01-01T01:00:00.000000Z'),
                    ('foo', '2024-01-01T02:00:00.000000Z'),
                    ('bar', '2024-01-01T03:00:00.000000Z'),
                    ('baz', '2024-01-01T04:00:00.000000Z'),
                    ('baz1', '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 'foo'")
                    .noLeakCheck()
                    .returns("""
                            val
                            foo
                            """);
            assertQuery("SELECT val FROM x WHERE val = 'aaa'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMixedParquetAndNativePartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-02T00:00:00.000000Z'),
                    (4, '2024-01-02T01:00:00.000000Z'),
                    (5, '2024-01-03T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");

            assertQuery("SELECT val FROM x WHERE val = 2")
                    .noLeakCheck()
                    .returns("""
                            val
                            2
                            """);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 5")
                    .noLeakCheck()
                    .returns("""
                            val
                            5
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMultipleAndConditions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, b STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, 'aaa', '2024-01-01T00:00:00.000000Z'),
                    (2, 'bbb', '2024-01-01T01:00:00.000000Z'),
                    (3, 'ccc', '2024-01-01T02:00:00.000000Z'),
                    (1, 'bbb', '2024-01-01T03:00:00.000000Z'),
                    (2, 'ccc', '2024-01-01T04:00:00.000000Z'),
                    (4, 'ccc', '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT a, b FROM x WHERE a = 1 AND b = 'bbb'")
                    .noLeakCheck()
                    .returns("""
                            a\tb
                            1\tbbb
                            """);
            assertQuery("SELECT a, b FROM x WHERE a = 99 AND b = 'zzz'")
                    .noLeakCheck()
                    .returns("a\tb\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testNativePlanInvalidatedAfterParquetConversion() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT x::VARCHAR, timestamp_sequence('2024-01-01', 100_000)
                    FROM long_sequence(5000)
                    """);
            execute("INSERT INTO x VALUES ('5001', '2024-01-02T02:00:00.000000Z')");

            try (RecordCursorFactory factory = select("SELECT val FROM x WHERE val = '-1'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse(cursor.hasNext());
                }

                execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("expected cached native plan to be invalidated");
                } catch (TableReferenceOutOfDateException ignored) {
                }
            }

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '-1'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testNativePlanRemainsValidAfterNativePartitionAdded() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES ('match', '2024-01-02T00:00:00.000000Z')");

            final TableToken tableToken = engine.getTableTokenIfExists("x");
            final long partitionTableVersion;
            try (TableReader reader = engine.getReader(tableToken)) {
                Assert.assertFalse(reader.hasParquetPartitions());
                partitionTableVersion = reader.getTxFile().getPartitionTableVersion();
            }

            try (RecordCursorFactory factory = select("SELECT val FROM x WHERE val = 'match'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertFalse(cursor.hasNext());
                }

                execute("INSERT INTO x VALUES ('match', '2024-01-01T00:00:00.000000Z')");

                try (TableReader reader = engine.getReader(tableToken)) {
                    Assert.assertFalse(reader.hasParquetPartitions());
                    Assert.assertNotEquals(partitionTableVersion, reader.getTxFile().getPartitionTableVersion());
                }

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testNullColumnPruning() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (null, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (null, '2024-01-01T02:00:00.000000Z'),
                    (null, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 42")
                    .noLeakCheck()
                    .returns("val\n");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            null
                            null
                            null
                            """);
        });
    }

    @Test
    public void testNullPruningByte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z'),
                    (4, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 0")
                    .noLeakCheck()
                    .returns("""
                            val
                            0
                            """);
        });
    }

    @Test
    public void testNullPruningDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1.11, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (3.33, '2024-01-01T02:00:00.000000Z'),
                    (3.34, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    @Test
    public void testNullPruningFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1.5, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (3.5, '2024-01-01T02:00:00.000000Z'),
                    (3.6, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    @Test
    public void testNonFiniteBoundPruningMatchesNative() throws Exception {
        // Numbers.isNull(double) is an exponent-bits test, so it is true for +/-Infinity as well as
        // NaN, and Numbers.equals(NaN, +Infinity) is therefore true. The Java row filter's negated
        // form for >= is (eq || l > r), so d >= +Infinity KEEPS every NULL row. NULLs are absent from
        // parquet min/max stats, so pruning must decline any bound for which the filter can keep a
        // NULL row - otherwise it drops rows the filter keeps.
        //
        // A CONSTANT bound cannot carry an infinity here - FunctionParser folds 1e308 * 10.0 through
        // DoubleConstant#newInstance, which maps every non-finite value onto NULL, so this arm only
        // ever sees NaN and the certification loop declines it. It stays as a forward guard for the
        // fold. testNonFiniteBindVariableBoundPruningMatchesNative covers the bound that IS
        // reachable as a genuine infinity.
        assertMemoryLeak(() -> {
            createNullMixedPartialParquet("DOUBLE", "6.0", "7.0", "2.0", "9.0");
            assertNativeMatchesPartialParquet("c6 >= 1e308 * 10.0", "c6\nnull\nnull\n");
        });
    }

    @Test
    public void testNonFiniteBindVariableBoundPruningMatchesNative() throws Exception {
        // PushdownFilterExtractor accepts any isConstantOrRuntimeConstant() bound and reads it at
        // scan time, and nothing between the PGWire binder (Double.longBitsToDouble of the raw
        // wire bits) and the pruner normalises it - so a bind variable delivers a genuine
        // +/-Infinity where a constant expression could only deliver NULL.
        //
        // Such a bound is tolerance-equal to NULL (Numbers.isNull is an exponent-bits test), so the
        // filter's inclusive and equality forms keep every NULL row, while NULL rows never appear in
        // a row group's min/max statistics. Pruning used to push it anyway: toleranceBound(+Inf, GE)
        // is Math.nextDown(+Inf - 1e-10) = Double.MAX_VALUE, a FINITE bound that certifies and then
        // prunes every group whose max is finite. The INT arm reached the same place through
        // integralBound and the out-of-range rewrite, which turns into "> INT_MAX" and drops every
        // group. Measured before the fix: the parquet table returned one NULL row where the native
        // table returned two.
        // SHORT and BYTE are excluded: they have no NULL sentinel, so the fixture holds no NULL row
        // for pruning to lose.
        assertMemoryLeak(() -> {
            for (String columnType : new String[]{"DOUBLE", "FLOAT", "INT", "LONG"}) {
                execute("DROP TABLE IF EXISTS tn");
                execute("DROP TABLE IF EXISTS tp");
                createNullMixedPartialParquet(columnType, "6", "7", "2", "9");
                final String suffix = "DOUBLE".equals(columnType) || "FLOAT".equals(columnType) ? ".0" : "";
                // Fixture in ts order: null, 6, 7 (parquet partition), null, 2, 9 (native partition).
                final String allRows = "c6\nnull\n6" + suffix + "\n7" + suffix + "\nnull\n2" + suffix + "\n9" + suffix + "\n";
                final String finiteRows = "c6\n6" + suffix + "\n7" + suffix + "\n2" + suffix + "\n9" + suffix + "\n";
                final String nullRows = "c6\nnull\nnull\n";
                final String noRows = "c6\n";
                for (double bound : new double[]{Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
                    final boolean isPositive = bound == Double.POSITIVE_INFINITY;
                    // With an infinite bound, Numbers.equals(row, bound) reduces to "the row is
                    // NULL", so "eq || ..." keeps the NULLs and "!eq && ..." drops them, while the
                    // raw ordering half orders the infinity as an ordinary extreme.
                    assertBindVarBoundMatchesNative(">=", bound, isPositive ? nullRows : allRows);
                    assertBindVarBoundMatchesNative("<=", bound, isPositive ? allRows : nullRows);
                    assertBindVarBoundMatchesNative(">", bound, isPositive ? noRows : finiteRows);
                    assertBindVarBoundMatchesNative("<", bound, isPositive ? finiteRows : noRows);
                    assertBindVarBoundMatchesNative("=", bound, nullRows);
                }
                // Control: the decline must be narrow. A finite bind-variable bound still prunes,
                // so a degenerate isPushableFloatingBound that rejected everything would fail here
                // rather than pass the parity sweep above. The parquet group holds {null, 6, 7}, so
                // ">= 8" clears it and only the native 9 matches.
                ParquetRowGroupFilter.resetRowGroupsSkipped();
                assertBindVarBoundMatchesNative(">=", 8.0, "c6\n9" + suffix + "\n");
                Assert.assertTrue("finite bind-variable bound must still prune for " + columnType,
                        ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            }
        });
    }

    @Test
    public void testNullPruningInclusiveDouble() throws Exception {
        // Inclusive DOUBLE bounds (<= and >=) push down over a parquet row group that mixes NULL and
        // non-NULL values. NULLs are excluded from the row-group min/max stats and never match an
        // inclusive comparison, so the parquet-pruned result must equal the native result, and a group
        // clear of the bound is still pruned. Existing NULL pruning coverage exercised equality only.
        // The parquet row group holds {null, 6, 7}; the native partition holds {null, 2, 9}.
        assertMemoryLeak(() -> {
            createNullMixedPartialParquet("DOUBLE", "6.0", "7.0", "2.0", "9.0");

            // c6 <= 5.0 clears the parquet group (min 6 > 5); only the native 2.0 matches.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 <= 5.0", "c6\n2.0\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // c6 >= 8.0 clears the parquet group (max 7 < 8); only the native 9.0 matches.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 >= 8.0", "c6\n9.0\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testNullPruningInclusiveFloat() throws Exception {
        // FLOAT counterpart of testNullPruningInclusiveDouble: the inclusive bound narrows through
        // tryPutFloatFromDouble over a parquet row group that mixes NULL and non-NULL values.
        assertMemoryLeak(() -> {
            createNullMixedPartialParquet("FLOAT", "6.5", "7.5", "2.5", "9.5");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 <= 5.0", "c6\n2.5\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 >= 8.0", "c6\n9.5\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testNullPruningInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z'),
                    (4, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    @Test
    public void testNullPruningLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z'),
                    (4, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    @Test
    public void testNullPruningShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (100, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (300, '2024-01-01T02:00:00.000000Z'),
                    (400, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 0")
                    .noLeakCheck()
                    .returns("""
                            val
                            0
                            """);
        });
    }

    @Test
    public void testNullPruningString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('aaa', '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    ('ccc', '2024-01-01T02:00:00.000000Z'),
                    ('ddd', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            """);
        });
    }

    @Test
    public void testNullPruningUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    ('33333333-3333-3333-3333-333333333333', '2024-01-01T02:00:00.000000Z'),
                    ('33333333-3333-3333-3333-333333333334', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            """);
        });
    }

    @Test
    public void testNullPruningVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('hello', '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    ('world', '2024-01-01T02:00:00.000000Z'),
                    ('world1', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            """);
        });
    }

    @Test
    public void testOrConditionNoPruning() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, 10, '2024-01-01T00:00:00.000000Z'),
                    (2, 20, '2024-01-01T01:00:00.000000Z'),
                    (3, 30, '2024-01-01T02:00:00.000000Z'),
                    (4, 40, '2024-01-02T00:00:00.000000Z'),
                    (5, 50, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'a,b')");

            assertQuery("SELECT a, b FROM x WHERE a = 1 OR b = 50")
                    .noLeakCheck()
                    .returns("""
                            a\tb
                            1\t10
                            5\t50
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT a, b FROM x WHERE a = 1 AND (b = 10 OR b = 99)")
                    .noLeakCheck()
                    .returns("""
                            a\tb
                            1\t10
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testOrEqualityFilter() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -1 OR val = -2")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 42 OR val = 43")
                    .noLeakCheck()
                    .returns("""
                            val
                            42
                            43
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -1 OR val = -2 OR val = -3")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -1 OR val = 100 OR val = -3")
                    .noLeakCheck()
                    .returns("""
                            val
                            100
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -1 OR ts = '2099-01-01T00:00:00.000000Z'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val = 1 OR val > 0")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n150\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE (val = -1 OR val = -2) AND val > 0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // OR with IS NULL on non-nullable data → all absent, skip all
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -1 OR val IS NULL")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // OR: matching value + IS NULL on non-nullable data → returns matching rows
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 42 OR val IS NULL")
                    .noLeakCheck()
                    .returns("""
                            val
                            42
                            """);
        });
    }

    @Test
    public void testOrEqualityFilterString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('alice', '2024-01-01T00:00:00.000000Z'),
                    ('bob', '2024-01-01T01:00:00.000000Z'),
                    ('charlie', '2024-01-01T02:00:00.000000Z'),
                    ('diana', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT name FROM x WHERE name = 'xyz' OR name = 'unknown'")
                    .noLeakCheck()
                    .returns("name\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT name FROM x WHERE name = 'xyz' OR name = 'bob'")
                    .noLeakCheck()
                    .returns("""
                            name
                            bob
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT name FROM x WHERE name = 'xyz' OR name IS NULL")
                    .noLeakCheck()
                    .returns("name\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT name FROM x WHERE name = 'bob' OR name IS NULL")
                    .noLeakCheck()
                    .returns("""
                            name
                            bob
                            """);
        });
    }

    @Test
    public void testOrEqualityFilterWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (NULL, '2024-01-01T00:00:00.000000Z'),
                    (NULL, '2024-01-01T01:00:00.000000Z'),
                    (42, '2024-01-02T00:00:00.000000Z'),
                    (43, '2024-01-02T01:00:00.000000Z'),
                    (44, '2024-01-03T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -1 OR val IS NULL")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            null
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 42 OR val IS NULL")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            null
                            42
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -999 OR val IS NULL")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            null
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testParquetPlanInvalidatedAfterNativeConversion() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT x::VARCHAR, timestamp_sequence('2024-01-01', 100_000)
                    FROM long_sequence(5000)
                    """);
            execute("INSERT INTO x VALUES ('5001', '2024-01-02T02:00:00.000000Z')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            try (RecordCursorFactory factory = select("SELECT val FROM x WHERE val = '-1'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse(cursor.hasNext());
                }

                execute("ALTER TABLE x CONVERT PARTITION TO NATIVE WHERE ts >= 0");

                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("expected cached parquet plan to be invalidated");
                } catch (TableReferenceOutOfDateException ignored) {
                }
            }

            assertQuery("SELECT val FROM x WHERE val = '-1'")
                    .noLeakCheck()
                    .returns("val\n");
        });
    }

    @Test
    public void testParquetPlanRemainsValidAfterAnotherParquetConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('match', '2024-01-01T00:00:00.000000Z'),
                    ('miss', '2024-01-02T00:00:00.000000Z'),
                    ('active', '2024-01-03T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01' WITH (bloom_filter_columns = 'val')");

            try (RecordCursorFactory factory = select("SELECT val FROM x WHERE val = 'match'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertFalse(cursor.hasNext());
                }

                execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-02' WITH (bloom_filter_columns = 'val')");

                ParquetRowGroupFilter.resetRowGroupsSkipped();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertFalse(cursor.hasNext());
                }
                Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            }
        });
    }

    @Test
    public void testPruningAllTypesMultipleRowGroups() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 2);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE x (
                        v_byte BYTE,
                        v_short SHORT,
                        v_char CHAR,
                        v_int INT,
                        v_long LONG,
                        v_float FLOAT,
                        v_double DOUBLE,
                        v_string STRING,
                        v_varchar VARCHAR,
                        v_symbol SYMBOL,
                        v_date DATE,
                        v_timestamp TIMESTAMP,
                        v_uuid UUID,
                        v_ipv4 IPv4,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, 100, 'A', 1000, 100_000, 1.5, 1.55, 'aaa', 'alpha', 'sym1', '2024-01-01', '2024-01-01T00:00:00.000000Z', '11111111-1111-1111-1111-111111111111', '1.1.1.1', '2024-01-01T00:00:00.000000Z'),
                    (2, 200, 'B', 2000, 200_000, 2.5, 2.55, 'bbb', 'beta',  'sym2', '2024-01-02', '2024-01-01T01:00:00.000000Z', '22222222-2222-2222-2222-222222222222', '2.2.2.2', '2024-01-01T01:00:00.000000Z'),
                    (3, 300, 'C', 3000, 300_000, 3.5, 3.55, 'ccc', 'gamma', 'sym3', '2024-01-03', '2024-01-01T02:00:00.000000Z', '33333333-3333-3333-3333-333333333333', '3.3.3.3', '2024-01-01T02:00:00.000000Z'),
                    (50, 500, 'X', 50_000, 500_000, 50.5, 50.55, 'xxx', 'xi', 'sym50', '2024-06-01', '2024-01-02T00:00:00.000000Z', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '50.50.50.50', '2024-01-02T00:00:00.000000Z'),
                    (60, 600, 'Y', 60_000, 600_000, 60.5, 60.55, 'yyy', 'upsilon', 'sym60', '2024-07-01', '2024-01-02T01:00:00.000000Z', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '60.60.60.60', '2024-01-02T01:00:00.000000Z'),
                    (70, 700, 'Z', 70_000, 700_000, 70.5, 70.55, 'zzz', 'zeta', 'sym70', '2024-08-01', '2024-01-02T02:00:00.000000Z', 'cccccccc-cccc-cccc-cccc-cccccccccccc', '70.70.70.70', '2024-01-02T02:00:00.000000Z'),
                    (80, 800, 'U', 80_000, 800_000, 80.5, 80.55, 'zzz', 'zeta', 'sym80', '2024-09-01', '2024-01-03T01:00:00.000000Z', 'cccccccc-cccc-cccc-cccc-cccccccccccc', '80.80.80.80', '2024-01-03T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT v_byte FROM x WHERE v_byte = 99::byte")
                    .noLeakCheck()
                    .returns("v_byte\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_short FROM x WHERE v_short = 999::short")
                    .noLeakCheck()
                    .returns("v_short\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_char FROM x WHERE v_char = 'M'")
                    .noLeakCheck()
                    .returns("v_char\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_int FROM x WHERE v_int = 99_999")
                    .noLeakCheck()
                    .returns("v_int\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_long FROM x WHERE v_long = 999_999")
                    .noLeakCheck()
                    .returns("v_long\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_float FROM x WHERE v_float = 99.9")
                    .noLeakCheck()
                    .returns("v_float\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_double FROM x WHERE v_double = 99.99")
                    .noLeakCheck()
                    .returns("v_double\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_string FROM x WHERE v_string = 'nnn'")
                    .noLeakCheck()
                    .returns("v_string\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_varchar FROM x WHERE v_varchar = 'omega'")
                    .noLeakCheck()
                    .returns("v_varchar\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_symbol FROM x WHERE v_symbol = 'sym99'")
                    .noLeakCheck()
                    .returns("v_symbol\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_date FROM x WHERE v_date = '2099-01-01'::DATE")
                    .noLeakCheck()
                    .returns("v_date\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_timestamp FROM x WHERE v_timestamp = '2099-01-01T00:00:00.000000Z'::TIMESTAMP")
                    .noLeakCheck()
                    .returns("v_timestamp\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_uuid FROM x WHERE v_uuid = '99999999-9999-9999-9999-999999999999'::UUID")
                    .noLeakCheck()
                    .returns("v_uuid\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_ipv4 FROM x WHERE v_ipv4 = '99.99.99.99'")
                    .noLeakCheck()
                    .returns("v_ipv4\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_int FROM x WHERE v_int = 3000")
                    .noLeakCheck()
                    .returns("v_int\n3000\n");
            assertQuery("SELECT v_string FROM x WHERE v_string = 'ccc'")
                    .noLeakCheck()
                    .returns("v_string\nccc\n");
            assertQuery("SELECT v_uuid FROM x WHERE v_uuid = '33333333-3333-3333-3333-333333333333'::UUID")
                    .noLeakCheck()
                    .returns("v_uuid\n33333333-3333-3333-3333-333333333333\n");
        });
    }

    @Test
    public void testPruningDisabled() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParquetRowGroupPruningEnabled(false);
            try {
                execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
                execute("""
                        INSERT INTO x VALUES
                        (1, '2024-01-01T00:00:00.000000Z'),
                        (2, '2024-01-01T01:00:00.000000Z'),
                        (3, '2024-01-01T02:00:00.000000Z'),
                        (4, '2024-01-02T02:00:00.000000Z')
                        """);
                execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

                assertQuery("SELECT val FROM x WHERE val = 99")
                        .noLeakCheck()
                        .returns("val\n");
                Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            } finally {
                sqlExecutionContext.setParquetRowGroupPruningEnabled(true);
            }
        });
    }

    @Test
    public void testPruningUnsupportedTypesFallback() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v_bool BOOLEAN, v_geo GEOHASH(4c), v_l256 LONG256, v_int INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (true, #u33d, CAST(1 AS LONG256), 10, '2024-01-01T00:00:00.000000Z'),
                    (false, #u33e, CAST(2 AS LONG256), 20, '2024-01-01T01:00:00.000000Z'),
                    (true, #u33f, CAST(3 AS LONG256), 30, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT v_bool FROM x WHERE v_bool = true")
                    .noLeakCheck()
                    .returns("""
                            v_bool
                            true
                            true
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_int FROM x WHERE v_int = 99")
                    .noLeakCheck()
                    .returns("v_int\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRangeFilterBetween() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val BETWEEN 10_000 AND 20_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val BETWEEN 50 AND 60")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n11\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val BETWEEN 110 AND 101")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n10\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRangeFilterBoundaryConditions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 60_000_000)
                    FROM long_sequence(150)
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (151, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val >= 152")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 150")
                    .noLeakCheck()
                    .returns("""
                            val
                            151
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val <= 0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < 1")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= 1")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n151\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val <= 150")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n150\n");
        });
    }

    @Test
    public void testRangeFilterByte() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 50);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS BYTE), timestamp_sequence('2024-01-01', 1200_000_000)
                    FROM long_sequence(100)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 120")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < 0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val > 50")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");
        });
    }

    @Test
    public void testRangeFilterByteNegative() throws Exception {
        // Range pruning over a BYTE column whose data spans negative and
        // positive values. Without correct sign-extension on the inline u64
        // stat slot, a row group with min = -50 reads back as 206 in the
        // skip path, and `val <= 0` skips every row group.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 50);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // x = 1..100, val = -50..49 (50 negative + 50 non-negative).
            execute("""
                    INSERT INTO x
                    SELECT CAST(x - 51 AS BYTE), timestamp_sequence('2024-01-01', 1200_000_000)
                    FROM long_sequence(100)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val <= 0")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n51\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= -10")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n60\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= -10 AND val <= 10")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n21\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < -50")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 100")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRangeFilterByteNegativeStats() throws Exception {
        // Same _pm sidecar inline-stat sign bug for BYTE: 1 narrow byte read back as 4
        // bytes for the i32 skip comparison.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x - 75 AS BYTE), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT count() AS cnt FROM x WHERE val <= 0::BYTE")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n75\n");
        });
    }

    @Test
    public void testRangeFilterChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('A', '2024-01-01T00:00:00.000000Z'),
                    ('B', '2024-01-01T01:00:00.000000Z'),
                    ('C', '2024-01-01T02:00:00.000000Z'),
                    ('M', '2024-01-01T03:00:00.000000Z'),
                    ('X', '2024-01-01T04:00:00.000000Z'),
                    ('Y', '2024-01-01T05:00:00.000000Z'),
                    ('Z', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 'Y'")
                    .noLeakCheck()
                    .returns("""
                            val
                            Z
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < 'A'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= 'X'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n3\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val <= 'C'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n3\n");
        });
    }

    @Test
    public void testRangeFilterDate() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(timestamp_sequence('2020-01-01', 86400_000_000) AS DATE), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > '2025-01-01'::DATE")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < '2019-01-01'::DATE")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= '2020-02-01'::DATE AND val <= '2020-03-02'::DATE")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n31\n");
        });
    }

    @Test
    public void testRangeFilterDouble() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS DOUBLE) * 0.1, timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 100.0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < 0.0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= 5.0 AND val < 10.0")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");
        });
    }

    @Test
    public void testRangeFilterFloat() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS FLOAT) * 0.1, timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 100.0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < 0.0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= 5.0 AND val < 10.0")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");
        });
    }

    @Test
    public void testRangeFilterIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val IPv4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1.1.1.1', '2024-01-01T00:00:00.000000Z'),
                    ('10.0.0.1', '2024-01-01T01:00:00.000000Z'),
                    ('192.168.1.1', '2024-01-01T02:00:00.000000Z'),
                    ('255.255.255.254', '2024-01-01T03:00:00.000000Z'),
                    (NULL, '2024-01-01T04:00:00.000000Z'),
                    (NULL, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > '10.0.0.1' ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            192.168.1.1
                            255.255.255.254
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < '192.168.1.1' ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            1.1.1.1
                            10.0.0.1
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val >= '192.168.1.1' ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            192.168.1.1
                            255.255.255.254
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val <= '10.0.0.1' ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            1.1.1.1
                            10.0.0.1
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > '255.255.255.254'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < '1.1.1.1'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > '0.0.0.1' ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            1.1.1.1
                            10.0.0.1
                            192.168.1.1
                            255.255.255.254
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IS NULL")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IS NOT NULL ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            1.1.1.1
                            10.0.0.1
                            192.168.1.1
                            255.255.255.254
                            """);
        });
    }

    @Test
    public void testRangeFilterIPv4HighValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val IPv4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('200.0.0.1', '2024-01-01T00:00:00.000000Z'),
                    ('210.0.0.1', '2024-01-01T01:00:00.000000Z'),
                    ('220.0.0.1', '2024-01-01T02:00:00.000000Z'),
                    ('230.0.0.1', '2024-01-01T03:00:00.000000Z'),
                    ('240.0.0.1', '2024-01-01T04:00:00.000000Z'),
                    ('250.0.0.1', '2024-01-01T05:00:00.000000Z'),
                    ('250.0.0.1', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val > '200.0.0.1'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n6\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > '250.0.0.1'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < '200.0.0.1'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= '210.0.0.1' AND val <= '230.0.0.1'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n3\n");
        });
    }

    @Test
    public void testRangeFilterInt() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 10_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < -1")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val > 50")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            cnt
                            100
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val >= 151")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val <= -1")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRangeFilterLong() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT x, timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 10_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < 0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= 100")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n51\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val <= 100")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n100\n");
        });
    }

    @Test
    public void testRangeFilterNegativeToPositive() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x - 101 AS INT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(300)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val > 50")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n149\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val < -50")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 199")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < -100")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= -10 AND val <= 10")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n21\n");
        });
    }

    @Test
    public void testRangeFilterShort() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS SHORT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 10_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < 0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val > 100")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");
        });
    }

    @Test
    public void testRangeFilterShortNegative() throws Exception {
        // Direct reproduction of the fuzzer-surfaced bug. SHORT data spans
        // negative and positive values across multiple row groups. Without
        // a parquet-physical-width round trip on the inline u64 stat slot,
        // a row group with min = -100 reads back as 65436 in the skip
        // path, and predicates like `val <= 0` skip every row group whose
        // true min was negative.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // x = 1..300, val = -100..199 (100 negative + 200 non-negative).
            execute("""
                    INSERT INTO x
                    SELECT CAST(x - 101 AS SHORT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(300)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val <= 0")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n101\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val > 50")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n149\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val < -50")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= -10 AND val <= 10")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n21\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < -100")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 199")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRangeFilterShortNegativeStats() throws Exception {
        // Regression for the _pm sidecar inline-stat sign bug: SHORT min/max stored as
        // 2 narrow bytes, then read back as 4 bytes for the i32 skip comparison. Without
        // sign extension a negative min like -74 reads as 65462 and the row group is
        // wrongly skipped. See ParquetRowGroupFilter.prepareFilterList SHORT branch and
        // the convert_stat_to_qdb narrow-INT32 path.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x - 75 AS SHORT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT count() AS cnt FROM x WHERE val <= 0::SHORT")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n75\n");

            assertQuery("SELECT count() AS cnt FROM x WHERE 0::SHORT >= val")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n75\n");

            assertQuery("SELECT count() AS cnt FROM x WHERE 0.147451::FLOAT >= val")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n75\n");
        });
    }

    @Test
    public void testRangeFilterString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('alice', '2024-01-01T00:00:00.000000Z'),
                    ('bob', '2024-01-01T01:00:00.000000Z'),
                    ('charlie', '2024-01-01T02:00:00.000000Z'),
                    ('diana', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT name FROM x WHERE name > 'zzz'")
                    .noLeakCheck()
                    .returns("name\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT name FROM x WHERE name < 'A'")
                    .noLeakCheck()
                    .returns("name\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRangeFilterTimestamp() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT timestamp_sequence('2020-01-01', 600_000_000), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > '2025-01-01'::timestamp")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < '2019-01-01'::timestamp")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= '2020-01-02'::timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n6\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val <= '2020-01-02'::timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n145\n");
        });
    }

    @Test
    public void testUnmaterialisableConditionKeepsPruningForOtherConditions() throws Exception {
        // PushdownFilterExtractor compiles each bound standalone with no target type, so a
        // string literal against a non-designated TIMESTAMP column arrives as a StrConstant
        // and getLong() raises ImplicitCastException. That one bound must decline on its own;
        // it must not cost every other condition in the same query its pruning.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts2 TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT),
                           timestamp_sequence('2024-01-01', 100_000),
                           timestamp_sequence('2024-01-01', 100_000)
                    FROM long_sequence(5000)
                    """);
            // A second partition keeps 2024-01-01 non-active so it converts.
            execute("INSERT INTO x VALUES (8000, '2024-01-02T02:00:00.000000Z', '2024-01-02T02:00:00.000000Z')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Control: the INT bound alone prunes.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -991")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // Same bound, now sharing the query with one the filter list cannot materialise.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -991 AND ts2 < '2024-06-01'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testBloomFilterSkippedAfterAlterColumnTypeIntToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            execute("ALTER TABLE x ALTER COLUMN val TYPE LONG");
            drainWalQueue();

            // After type change INT->LONG, pushdown must be disabled for val.
            // Otherwise the bloom filter bytes (stored as i32) would be probed
            // with an i64 hash, producing false-negative skips and missing rows.
            assertQuery("SELECT val FROM x WHERE val = 50_000").noLeakCheck().returns(
                    """
                            val
                            50000
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 25_000").noLeakCheck().returns(
                    "val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testRenamedColumnBetween() throws Exception {
        // Companion to testBloomFilterSymbolRenamedColumn for the min/max-stats path: a BETWEEN
        // range on a renamed column must resolve to the right parquet column by stable id, not
        // by the frozen (now stale) parquet name.
        assertMemoryLeak(() -> {
            createRenamedNumericParquetTable();

            // The match lives in the renamed column's low row group; the stale name maps to the
            // high-range column whose stats do not overlap [15000, 25000], wrongly pruning it.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b BETWEEN 15_000 AND 25_000 ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b
                            15000
                            25000
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // A range genuinely absent from the renamed column must still prune.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b BETWEEN 1_000 AND 5_000")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testBloomFilterSkippedAfterAlterColumnTypeLongToInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            execute("ALTER TABLE x ALTER COLUMN val TYPE INT");
            drainWalQueue();

            // After type change LONG->INT, pushdown must be disabled for val.
            // The parquet file stores i64 bloom filters / min-max stats but the
            // filter serializes i32 values — different element sizes cause wrong
            // hashes and comparisons.
            assertQuery("SELECT val FROM x WHERE val = 50_000").noLeakCheck().returns(
                    """
                            val
                            50000
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 25_000").noLeakCheck().returns(
                    "val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testRenamedColumnInList() throws Exception {
        // Companion to testBloomFilterSymbolRenamedColumn for the min/max-stats path: an IN list
        // on a renamed column must resolve by stable column id.
        assertMemoryLeak(() -> {
            createRenamedNumericParquetTable();

            // 15_000 and 25_000 both live in the renamed column's first row group; the stale
            // name maps to the high-range column whose stats exclude both, so the buggy
            // resolution wrongly pruned that row group and dropped the matches.
            assertQuery("SELECT b FROM x WHERE b IN (15_000, 25_000) ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b
                            15000
                            25000
                            """);

            // Values genuinely absent from the renamed column must still prune.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b IN (1_000, 5_000)")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testBloomFilterSkippedAfterAlterColumnTypeShortToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (500, '2024-01-01T01:00:00.000000Z'),
                    (1000, '2024-01-01T02:00:00.000000Z'),
                    (1001, '2024-01-02T01:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            execute("ALTER TABLE x ALTER COLUMN val TYPE LONG");
            drainWalQueue();

            // After type change SHORT->LONG, pushdown must be disabled for val.
            // The parquet file stores i32 bloom filters but the filter serializes
            // i64 values — wrong hash width causes false-negative skips.
            assertQuery("SELECT val FROM x WHERE val = 500").noLeakCheck().returns(
                    """
                            val
                            500
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 250").noLeakCheck().returns(
                    "val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testMinMaxPruningSkippedAfterAlterColumnTypeIntToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x VALUES
                    (10_000, '2024-01-01T00:00:00.000000Z'),
                    (20_000, '2024-01-01T01:00:00.000000Z'),
                    (30_000, '2024-01-01T02:00:00.000000Z'),
                    (40_000, '2024-01-01T03:00:00.000000Z'),
                    (50_000, '2024-01-01T04:00:00.000000Z'),
                    (60_000, '2024-01-02T04:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            execute("ALTER TABLE x ALTER COLUMN val TYPE LONG");
            drainWalQueue();

            // After type change INT->LONG, min/max pushdown must be disabled for val.
            // The parquet file stores i32 stats but the filter serializes i64
            // values; cross-width comparisons would produce wrong skip decisions.
            assertQuery("SELECT val FROM x WHERE val = 30_000").noLeakCheck().returns(
                    """
                            val
                            30000
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 100_000").noLeakCheck().returns(
                    "val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testRenamedColumnIsNull() throws Exception {
        // Companion to testBloomFilterSymbolRenamedColumn for the null-count path: IS [NOT] NULL
        // pushdown resolved the filtered column by its frozen parquet name, so after a rename it
        // consulted the wrong column's null-count stats and wrongly pruned. The fix resolves by
        // stable column id.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // Column a (the future 'b') is non-null in the first partition and null in the second;
            // column b is the mirror image. The third partition keeps the first two non-active so
            // they convert to parquet.
            execute("""
                    INSERT INTO x VALUES
                    (100, NULL, '2024-01-01T00:00:00.000000Z'),
                    (NULL, 200, '2024-01-02T00:00:00.000000Z'),
                    (300, 400, '2024-01-03T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            execute("ALTER TABLE x RENAME COLUMN b TO c");
            execute("ALTER TABLE x RENAME COLUMN a TO b");

            // IS NOT NULL must return the renamed column's non-null rows. The stale name maps to
            // the all-null frozen column in the first partition, which wrongly pruned row 100.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b IS NOT NULL ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b
                            100
                            300
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // IS NULL must return the renamed column's null row. The stale name maps to the
            // no-nulls frozen column in the second partition, which wrongly pruned that null.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE b IS NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n1\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningSkippedAfterAlterColumnTypeTimestampToTimestampNs() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x VALUES
                    ('2020-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    ('2020-06-01T00:00:00.000000Z', '2024-01-01T01:00:00.000000Z'),
                    ('2021-01-01T00:00:00.000000Z', '2024-01-01T02:00:00.000000Z'),
                    ('2021-06-01T00:00:00.000000Z', '2024-01-01T03:00:00.000000Z'),
                    ('2022-01-01T00:00:00.000000Z', '2024-01-01T04:00:00.000000Z'),
                    ('2022-06-01T00:00:00.000000Z', '2024-01-02T04:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            execute("ALTER TABLE x ALTER COLUMN val TYPE TIMESTAMP_NS");
            drainWalQueue();

            // After TIMESTAMP(us)->TIMESTAMP_NS, the parquet min/max stats are in
            // microseconds but the filter values are in nanoseconds (1000x larger).
            // The skip path must compare precision in addition to the TIMESTAMP
            // tag; otherwise cross-precision min/max comparisons drop matching
            // row groups.
            assertQuery("SELECT val FROM x WHERE val = '2021-01-01'::TIMESTAMP_NS").noLeakCheck().returns(
                    """
                            val
                            2021-01-01T00:00:00.000000000Z
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > '2025-01-01'::TIMESTAMP_NS").noLeakCheck().returns(
                    "val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testRenamedColumnMinMaxEquality() throws Exception {
        // Companion to testBloomFilterSymbolRenamedColumn: that test covers the bloom-filter
        // equality path; this one covers equality resolved through min/max statistics (no bloom
        // filter on the column). Both share the same prepareFilterList column resolution.
        assertMemoryLeak(() -> {
            createRenamedNumericParquetTable();

            // 25_000 lives in the renamed column's first row group; the stale name maps to the
            // high-range column whose min/max excludes it, which wrongly pruned the match away.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b = 25_000")
                    .noLeakCheck()
                    .returns("""
                            b
                            25000
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // A value genuinely absent from the renamed column must still prune.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b = 5_000")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRenamedColumnOrEquality() throws Exception {
        // Companion to testBloomFilterSymbolRenamedColumn for the min/max-stats path: an
        // OR-of-equalities on a renamed column must resolve by stable column id.
        assertMemoryLeak(() -> {
            createRenamedNumericParquetTable();

            // 15_000 and 25_000 both live in the renamed column's first row group; the stale
            // name maps to the high-range column whose stats exclude both, so the buggy
            // resolution wrongly pruned that row group and dropped the matches.
            assertQuery("SELECT b FROM x WHERE b = 15_000 OR b = 25_000 ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b
                            15000
                            25000
                            """);

            // Values genuinely absent from the renamed column must still prune.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b = 1_000 OR b = 5_000")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRenamedColumnRange() throws Exception {
        // Companion to testBloomFilterSymbolRenamedColumn for the min/max-stats path: range
        // predicates on a renamed column must resolve by stable column id.
        assertMemoryLeak(() -> {
            createRenamedNumericParquetTable();

            // '< 30_000' matches only the renamed column's low row group; the stale name maps to
            // the high-range column whose min is above 30_000, which wrongly pruned the match.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b < 30_000 ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b
                            15000
                            25000
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b <= 25_000 ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b
                            15000
                            25000
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // A range below every value in the renamed column must still prune.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b < 10_000")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testDoubleColumnExactEqStillPrunesOnBloomFilter() throws Exception {
        // The counterpart of testDoubleColumnToleranceEqPushdownNotFalsePruned: a bound whose
        // neighbouring doubles fall outside the tolerance band matches the exact value and nothing
        // else, so it keeps pushing as an equality and keeps consulting the bloom filter. The group's
        // [min, max] spans the bound, so the bloom filter is the only thing that can prune it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1000000.5, '2024-01-01T00:00:00.000000Z'),
                    (3000000.5, '2024-01-01T01:00:00.000000Z'),
                    (5000000.5, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 2000000.5")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQuery("SELECT val FROM x WHERE val = 3000000.5")
                    .noLeakCheck()
                    .returns("""
                            val
                            3000000.5
                            """);
        });
    }

    @Test
    public void testDoubleColumnToleranceEqPushdownNotFalsePruned() throws Exception {
        // Row-level DOUBLE equality is tolerance-based (Numbers.DOUBLE_TOLERANCE, 1e-10), so
        // "c6 = 1.0" keeps the row 1.00000000005. Pushing the exact bound into the row group filter
        // is not: the value falls outside the group's [min, max] (and its bits are absent from the
        // bloom filter), so pruning drops the group before the row filter ever sees it.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquetTyped("DOUBLE", "1.00000000005", "0.0");

            assertNativeMatchesPartialParquet("c6 = 1.0", "c6\n1.00000000005\n");
            assertNativeMatchesPartialParquet("c6 IN (1.0, 5.0)", "c6\n1.00000000005\n");

            // The tolerance band is the only thing the bound gives up: a bound clear of the group
            // still prunes it.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 = 99.0", "c6\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testDoubleColumnToleranceRangePushdownNotFalsePruned() throws Exception {
        // The same tolerance holds for the ops that include equality: "c6 <= 1.0" keeps the row
        // 1.00000000005 and "c6 >= 1.0" keeps 0.99999999995, while the native side prunes on
        // "min > bound" and "max < bound" - the exact bound drops both groups.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquetTyped("DOUBLE", "1.00000000005", "0.0");
            assertNativeMatchesPartialParquet("c6 <= 1.0", "c6\n1.00000000005\n0.0\n");

            // The strict ops need no widening (the tolerance makes the row filter stricter than the
            // pruner), and they still prune the group they exclude.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.0", "c6\n0.0\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            createBoundarySaturatedPartialParquetTyped("DOUBLE", "0.99999999995", "2.0");
            assertNativeMatchesPartialParquet("c6 >= 1.0", "c6\n0.99999999995\n2.0\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 > 1.0", "c6\n2.0\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testDeclinedPushdownConditionIsRederivedForBindVariables() throws Exception {
        // A bound the value arms cannot materialise declines the whole condition, and
        // prepareFilterList runs once per parquet partition, so the same answer was re-derived - and
        // for the routine cause, an ImplicitCastException re-thrown - for every one of them. The
        // decline is now recorded on the condition, but ONLY when every value is a compile-time
        // constant: a bind variable is a runtime constant and the next execution may bind a value
        // that does serialize. Both halves are pinned here, the second by reusing ONE compiled
        // factory across two bindings - a cached decline would silently cost the second execution
        // its pruning while leaving its rows correct, which no result assertion can see.
        assertMemoryLeak(() -> {
            // A LONG column with a DOUBLE bound: the two sides compare at different widths, so the
            // pushdown declines a bound above 2^53 (see tryPutLongFromDouble) and accepts one below
            // it. That gives one decline and one prune from the same condition shape.
            createBoundarySaturatedPartialParquetTyped("LONG", "1", "2");

            // Constant, permanently declined: every row must survive, on both partitions, i.e. across
            // two calls with the flag set by the first.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT c6 FROM tp WHERE c6 < 1e300 ORDER BY ts").noLeakCheck().returns("c6\n1\n2\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            // Same shape through a bind variable, on ONE compiled factory. The first binding declines
            // exactly as the constant did; the second is pushable and prunes the parquet group, which
            // can only happen if the decline was not cached.
            bindVariableService.clear();
            bindVariableService.setDouble("b", 1e300);
            try (RecordCursorFactory factory = select("SELECT c6 FROM tp WHERE c6 < :b ORDER BY ts")) {
                final StringSink sink = new StringSink();
                ParquetRowGroupFilter.resetRowGroupsSkipped();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), sink);
                }
                TestUtils.assertEquals("c6\n1\n2\n", sink);
                Assert.assertEquals("an unpushable bind value must not prune", 0, ParquetRowGroupFilter.getRowGroupsSkipped());

                bindVariableService.setDouble("b", -5.0);
                ParquetRowGroupFilter.resetRowGroupsSkipped();
                sink.clear();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), sink);
                }
                TestUtils.assertEquals("c6\n", sink);
                Assert.assertTrue(
                        "a pushable bind value must still prune after an unpushable one",
                        ParquetRowGroupFilter.getRowGroupsSkipped() > 0
                );
            }
        });
    }

    @Test
    public void testDoubleColumnNearToleranceMagnitudeKeepsJitModeForLaterTests() throws Exception {
        // The tolerance test needs JIT off for its own queries and must hand the mode back
        // afterwards. setJitMode() is what outlives a method: the context is shared by the whole
        // class and reset() does not touch the mode, so a test that switches it and then throws
        // would cost JIT coverage for the other ~160 tests in this class - including the ones this
        // PR added - and show up as nothing at all. Assert the INVARIANT (the callee restores what
        // it found) rather than a literal mode, so the pin survives a change of the
        // test-configuration default.
        final int configJitMode = configuration.getSqlJitMode();
        final int contextJitMode = sqlExecutionContext.getJitMode();
        // The assertions belong in a finally, because the path that leaves the mode switched is
        // exactly the path where the callee threw - asserting after an unguarded call would skip
        // the check on the only run that needs it. Nothing is lost by letting an assertion here
        // replace the callee's exception: the callee is a @Test of its own, so JUnit reports that
        // failure under its own name either way.
        try {
            testDoubleColumnNearToleranceMagnitudePushdownNotFalsePruned();
        } finally {
            Assert.assertEquals(
                    "the tolerance test must not change the configured JIT mode",
                    configJitMode,
                    configuration.getSqlJitMode()
            );
            Assert.assertEquals(
                    "the tolerance test must restore the JIT mode on the execution context",
                    contextJitMode,
                    sqlExecutionContext.getJitMode()
            );
        }
    }

    @Test
    public void testDoubleColumnNearToleranceMagnitudePushdownNotFalsePruned() throws Exception {
        // Near |bound| == DOUBLE_TOLERANCE the tolerance widening cancels: "1e-10 - DOUBLE_TOLERANCE"
        // is exactly 0.0 and nextDown(0.0) is a single subnormal ulp, nowhere near the tolerance edge.
        // The inclusive row filter still keeps a tiny value on the far side of zero -- |(-1e-30) -
        // 1e-10| rounds to exactly DOUBLE_TOLERANCE, which Numbers.equals calls equal -- so pushing
        // that bound prunes the group and loses the row (the DOUBLE arm never had the FLOAT arm's
        // certify-or-decline guard). Pruning runs before ANY row filter, so the fix must decline the
        // pushdown; the surviving rows pin the data-integrity contract and getRowGroupsSkipped() pins
        // the pruning signal directly.
        //
        // These kept rows sit exactly on the strict/inclusive filter boundary (in real arithmetic no
        // negative value is within DOUBLE_TOLERANCE of a positive 1e-10 bound; only the rounding of
        // Numbers.equals keeps them), so only the inclusive Java filter keeps them and the compiled
        // JIT filter would drop them. Disabling the JIT below is what makes the oracle stable, and it
        // covers the assertQuery(...).returns(...) battery too: AbstractCairoTest.assertQuery() hands
        // QueryAssertion this very sqlExecutionContext, and QueryAssertion never touches the JIT mode,
        // so every pass the builder runs -- including the second cursor pass -- runs the Java filter.
        //
        // Switch the mode on the execution context, NOT with setProperty(). setProperty() does not
        // even switch this test: setUp() primes the shared context before the body runs, and
        // SqlCodeGenerator reads the mode from the context alone, so these queries used to run with
        // JIT on regardless of the override. It does not reach a later test either -
        // Cairo#tearDown() resets the overrides after every method - so it is a no-op that reads
        // like a guarantee. testDoubleColumnNearToleranceMagnitudeKeepsJitModeForLaterTests pins
        // the handback.
        final int callerJitMode = sqlExecutionContext.getJitMode();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try {
            assertDoubleColumnNearToleranceMagnitudePushdownNotFalsePruned();
        } finally {
            sqlExecutionContext.setJitMode(callerJitMode);
        }
    }

    @Test
    public void testDoubleConstantAlmostIntegralPushdownNotFalsePruned() throws Exception {
        // The integer stats slots widen the column to double and compare with the same tolerance, so
        // a bound within 1e-10 of an integer keeps that integer's rows: "c6 <= 0.99999999999" keeps
        // the row 1. Rounding the bare bound floors it to "c6 <= 0", which prunes the group holding
        // that row; rounding the tolerance-widened bound lands on "c6 <= 1" and keeps it.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquet(1, -1);
            assertNativeMatchesPartialParquet("c6 <= 0.99999999999", "c6\n1\n-1\n");
            assertNativeMatchesPartialParquet("c6 >= 1.00000000001", "c6\n1\n");

            // The same bound in the 64-bit slot.
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");
            createBoundarySaturatedPartialParquetTyped("LONG", "1", "-1");
            assertNativeMatchesPartialParquet("c6 <= 0.99999999999", "c6\n1\n-1\n");
        });
    }

    @Test
    public void testDoubleConstantFractionalPushdownNotFalsePruned() throws Exception {
        // A fractional DOUBLE bound truncates toward zero in the INT stats slot. Truncation
        // is not pruning-safe: "c6 < 1.5" becomes "c6 < 1", which prunes a group whose INT
        // stat is exactly 1 even though that row satisfies 1 < 1.5. Pruning runs before the
        // row filter, so a false-prune drops the parquet row and the partial-parquet table
        // returns fewer rows than its all-native sibling.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquet(1, 100);

            // Positive fractional bound: strict "<" false-prunes the group at 1 before the fix.
            assertNativeMatchesPartialParquet("c6 < 1.5", "c6\n1\n");
            assertNativeMatchesPartialParquet("c6 <= 1.5", "c6\n1\n");

            // Integral DOUBLE bound stays pushdown-safe: the group at 1 is pruned correctly
            // (row 1 fails 1 < 1.0) and the pushdown still fires -- the fix is surgical.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.0", "c6\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // Negative fractional bound truncates the other way: "c6 > -1.5" becomes "c6 > -1",
            // which false-prunes a group at -1 even though -1 > -1.5.
            createBoundarySaturatedPartialParquet(-1, -100);
            assertNativeMatchesPartialParquet("c6 > -1.5", "c6\n-1\n");
            assertNativeMatchesPartialParquet("c6 >= -1.5", "c6\n-1\n");
        });
    }

    @Test
    public void testDoubleConstantFractionalPushdownNotFalsePrunedLongColumn() throws Exception {
        // The LONG stats slot truncates a fractional DOUBLE bound via (long) getDouble(),
        // the 64-bit twin of the INT/narrow arms. "c6 < 1.5" -> "c6 < 1" false-prunes a
        // group at 1 even though 1 < 1.5.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquetTyped("LONG", "1", "100");

            assertNativeMatchesPartialParquet("c6 < 1.5", "c6\n1\n");

            // Integral DOUBLE bound stays pushdown-safe and still prunes.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.0", "c6\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testDoubleConstantAbove2Pow53PushdownNotFalsePrunedLongColumn() throws Exception {
        // There is no (LONG, DOUBLE) comparison: the row-level filter widens the column to DOUBLE and
        // compares at double width, while row group pruning compares the stats at long width. The two
        // agree only below 2^53, where a double still represents every integer exactly. Above it the
        // pruner is the finer of the two and skips a group whose rows the filter keeps:
        // (double) 10000000000000001 is exactly 1e16, so "c6 <= 1e16" and "c6 = 1e16" both keep that
        // row, while the pushed bound (long) 1e16 == 10000000000000000 excludes the group.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquetTyped("LONG", "10_000_000_000_000_001", "0");

            assertNativeMatchesPartialParquet("c6 <= 1e16", "c6\n10000000000000001\n0\n");
            assertNativeMatchesPartialParquet("c6 = 1e16", "c6\n10000000000000001\n");
            assertNativeMatchesPartialParquet("c6 = 1e16::float", "c6\n10000000000000001\n");

            // A bound below 2^53 stays exact at double width, so it still pushes down and prunes.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1000.0", "c6\n0\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testDoubleConstantOutsideIntRangePushdownNotFalsePruned() throws Exception {
        // An out-of-INT-range DOUBLE bound saturates to INT_MAX in the 32-bit stats slot,
        // the (int) getDouble() twin of the LONG-bound saturation. "c6 < 5e9" saturates to
        // "c6 < INT_MAX" and false-prunes an all-INT_MAX group whose rows satisfy the filter.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquet(2_147_483_647, 0);

            assertNativeMatchesPartialParquet("c6 < 5000000000.0", "c6\n2147483647\n0\n");

            // Control: no INT value exceeds the bound; empty result, group may prune.
            assertNativeMatchesPartialParquet("c6 > 5000000000.0", "c6\n");
        });
    }

    @Test
    public void testDoubleConstantPushdownTimestampAndDateColumns() throws Exception {
        // getLong() throws UnsupportedOperationException on a FLOAT/DOUBLE function, so a double bound
        // against a TIMESTAMP or DATE column threw inside the filter builder. The catch-all swallowed
        // it, logged an error and dropped row group pruning for every condition in the query. Both arms
        // now take the same double guard as the LONG arm: an exact in-range bound prunes, and a bound
        // the column cannot round-trip through DOUBLE declines instead of false-pruning.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquetTyped("TIMESTAMP", "1_704_067_200_000_000", "0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.7e15", "c6\n1970-01-01T00:00:00.000000Z\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            // A FLOAT bound reaches the same arm through getDouble().
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.7e15::float", "c6\n1970-01-01T00:00:00.000000Z\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // Above 2^53 the row-level filter compares at double width and keeps the row, so the
            // pushdown must decline rather than prune the group at long width.
            createBoundarySaturatedPartialParquetTyped("TIMESTAMP", "10_000_000_000_000_001", "0");
            assertNativeMatchesPartialParquet("c6 <= 1e16", "c6\n2286-11-20T17:46:40.000001Z\n1970-01-01T00:00:00.000000Z\n");
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            createBoundarySaturatedPartialParquetTyped("DATE", "1_704_067_200_000", "0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.7e12", "c6\n1970-01-01T00:00:00.000Z\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            createBoundarySaturatedPartialParquetTyped("DATE", "10_000_000_000_000_001", "0");
            assertNativeMatchesPartialParquet("c6 <= 1e16", "c6\n318857-05-20T17:46:40.001Z\n1970-01-01T00:00:00.000Z\n");
        });
    }

    @Test
    public void testFloatColumnPushdownNotFalsePruned() throws Exception {
        // A FLOAT column's stats slot is 32-bit, but there is no (FLOAT, DOUBLE) comparison: the
        // row-level filter widens the column to DOUBLE and compares at double width (the only
        // comparison factories are the double ones, e.g. LtDoubleVVFunctionFactory "<(DD)"). The
        // FLOAT arm narrowed the bound with (float) getDouble(), which rounds to NEAREST - and
        // nearest is not pruning-safe in either direction:
        //   "<"  needs the SMALLEST float >= the bound, but nearest can round DOWN, moving the
        //        bound onto a group's boundary float and pruning rows the filter keeps;
        //   ">"  needs the LARGEST float <= the bound, but nearest can round UP, likewise.
        // ("<=" and ">=" happen to survive nearest: rounding the wrong way only makes them prune
        // less.) Pruning runs before the row filter, so a false-prune drops the parquet rows
        // outright and the partial-parquet table returns fewer rows than its all-native sibling.
        assertMemoryLeak(() -> {
            // The parquet group holds the single float 1.0; the native sibling row holds 100.0.
            createBoundarySaturatedPartialParquetTyped("FLOAT", "1.0", "100.0");

            // (float) 1.00000003 rounds DOWN to 1.0f (the next float up is 1.00000011920928955),
            // so "< 1.00000003" pushed as "< 1.0f" prunes the group whose min stat is 1.0f - yet
            // (double) 1.0f = 1.0 < 1.00000003 keeps the row.
            assertNativeMatchesPartialParquet("c6 < 1.00000003", "c6\n1.0\n");
            // The mirror image: (float) 0.99999998 rounds UP to 1.0f (the next float down is
            // 0.99999994039535522), so "> 0.99999998" pushed as "> 1.0f" prunes a group whose max
            // stat is 1.0f - yet (double) 1.0f = 1.0 > 0.99999998 keeps the row.
            assertNativeMatchesPartialParquet("c6 > 0.99999998", "c6\n1.0\n100.0\n");

            // The two ops nearest already served: they must keep selecting the same rows.
            assertNativeMatchesPartialParquet("c6 <= 1.00000003", "c6\n1.0\n");
            assertNativeMatchesPartialParquet("c6 >= 0.99999998", "c6\n1.0\n100.0\n");

            // An exactly-representable bound loses nothing and must still prune: 1.0 is a float,
            // so "< 1.0" excludes the group at 1.0 outright.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.0", "c6\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 > 100.0", "c6\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // An integer bound has no exact float above 2^24 either: 16777217 sits halfway between
            // 16777216f and 16777218f and rounds to even, i.e. DOWN to 16777216f. "< 16777217"
            // then prunes the group whose min stat is 16777216f, though that row satisfies it.
            createBoundarySaturatedPartialParquetTyped("FLOAT", "16777216.0", "0.0");
            assertNativeMatchesPartialParquet("c6 < 16_777_217", "c6\n1.6777216E7\n0.0\n");
            assertNativeMatchesPartialParquet("c6 >= 16_777_217", "c6\n");

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // The bound must also carry the comparison TOLERANCE. QuestDB compares floating point
            // with Numbers.DOUBLE_TOLERANCE (1e-10), so "c6 >= d" keeps a row that is merely
            // tolerance-equal to d - a row strictly BELOW it. Rounding to the nearest float ignores
            // that, and so does rounding by an ulp: one float ulp near 1.0 is 1.2e-7, over a
            // thousand times the tolerance, so such a bound steps clean over the band and prunes the
            // group holding the row. Every bound below sits inside the tolerance band around 1.0.
            createBoundarySaturatedPartialParquetTyped("FLOAT", "1.0", "100.0");
            assertNativeMatchesPartialParquet("c6 >= 1.00000000005", "c6\n1.0\n100.0\n");
            assertNativeMatchesPartialParquet("c6 <= 0.99999999995", "c6\n1.0\n");
            assertNativeMatchesPartialParquet("c6 > 0.99999999995", "c6\n100.0\n");
            assertNativeMatchesPartialParquet("c6 < 1.00000000005", "c6\n");
            assertNativeMatchesPartialParquet("c6 = 1.00000000005", "c6\n1.0\n");

            // A bound ONE TOLERANCE away from the group's float is the hard case for the strict ops.
            // The row is 1.0000000827e-10 from the bound - just OUTSIDE the tolerance, so the filter
            // keeps it - but "d - tolerance" rounds back onto 1.0 exactly (the residual is far below
            // half a double ulp), so a bound narrowed from that pivot lands on 1.0f, and the native
            // predicate ("<" prunes on min >= bound) drops the group holding it.
            assertNativeMatchesPartialParquet("c6 < 1.0000000001", "c6\n1.0\n");
            assertNativeMatchesPartialParquet("c6 > 0.9999999999", "c6\n1.0\n100.0\n");
            assertNativeMatchesPartialParquet("c6 < 100.0000000001", "c6\n1.0\n100.0\n");
            assertNativeMatchesPartialParquet("c6 > 99.9999999999", "c6\n100.0\n");
            // The non-strict ops prune on min > bound / max < bound, so the bound itself is already
            // excluded from the pruned side, and they must not step the same way.
            assertNativeMatchesPartialParquet("c6 <= 0.9999999999", "c6\n");
            assertNativeMatchesPartialParquet("c6 >= 1.0000000001", "c6\n100.0\n");

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // Near zero the tolerance band spans many floats, so the bound has to be walked out of
            // it - or abandoned. A row of 0.0f is tolerance-equal to a bound of -5e-11, so the
            // filter keeps it for "<=" and the pruner must not skip the group.
            createBoundarySaturatedPartialParquetTyped("FLOAT", "0.0", "5.0");
            assertNativeMatchesPartialParquet("c6 <= -5e-11", "c6\n0.0\n");
            assertNativeMatchesPartialParquet("c6 >= 5e-11", "c6\n0.0\n5.0\n");

            // EQ has no direction to round in: it prunes when the pushed float falls outside
            // [min, max]. Below ~8e-4 the tolerance band holds several floats, so the group can hold
            // a matching row that is not the nearest one - 4.9999997E-4 is tolerance-equal to 0.0005
            // (8.2e-11 away) though 5.0E-4 is what a bound of 0.0005 narrows to. Pushing the nearest
            // float would prune this group; the bound declines instead.
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");
            createBoundarySaturatedPartialParquetTyped("FLOAT", "4.9999997E-4", "5.0");
            assertNativeMatchesPartialParquet("c6 = 0.0005", "c6\n4.9999997E-4\n");
            // ... and a bound whose band holds exactly one float still pushes, and still prunes.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 = 5.0", "c6\n5.0\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testFloatColumnInclusiveOpCertifiesAgainstCompiledF32Filter() throws Exception {
        // The engine has THREE row-level filters, not two, and isRowKept models only the two that
        // compare at DOUBLE width. The third is the compiled filter's f32 arm, which a FLOAT column
        // runs whenever the bound IS exactly a float and the comparison never widens: the
        // subtraction, its absolute value and the tolerance test all happen at single precision, and
        // the tolerance itself is FLOAT_EPSILON = (float) DOUBLE_TOLERANCE, i.e.
        // 1.000000013351432e-10 - a shade LARGER than the 1e-10 the other two use. So the f32 arm
        // has a wider equality band and keeps rows they drop. Pruning is an unconditional drop that
        // no later filter can undo, so a bound certified against the f64 pair alone lands on such a
        // row and the group holding it goes away before any filter runs.
        //
        // The INCLUSIVE ops are where that shows. All three comparators read their epsilon
        // inclusively (Numbers.equals is "|l - r| <= tolerance"; float_cmp_epsilon and
        // double_cmp_epsilon are "epsilon >= |lhs - rhs|" - x86 ucomiss/setae, aarch64 fcmp/cset GE,
        // avx2 vcmpps kLE), so the two tolerances are the only thing left between them, and a row
        // that lands between 1e-10 and FLOAT_EPSILON is EQUAL to the bound for the f32 arm and
        // UNEQUAL for the f64 pair. "c6 >= bound" and "c6 <= bound" then keep it on one and drop it
        // on the other.
        //
        // THIS TEST PINS THE INCLUSIVE COMPARATOR ITSELF, so it reddens on any build whose
        // libquestdb carries the STRICT comparator. The binaries committed under
        // core/src/main/resources/io/questdb/bin/ carry the inclusive one, so a plain checkout
        // passes; an older library, or one built from an earlier source tree, does not, since no
        // Maven profile compiles core/src/main/c/. That failure mode is "the library predates the
        // comparator change", NOT a pruning bug: tn and tp agree in both worlds, so no row is lost
        // either way. The pruning property is pinned separately and binary-independently by
        // testFloatColumnPushdownMatchesNativeUnderEitherComparator.
        //
        // Two oracles, because they fail for different reasons. The ROW assertions are the
        // user-visible half: the row survives on the all-native table and must survive on the
        // partially-parquet one. They deliberately carry no ORDER BY - projecting or ordering by a
        // column the filter does not read turns on parquet late materialization, which leaves the
        // unread column's address at 0, makes the frame report column tops, and drops the query onto
        // the Java f64 filter, which discards the row for its own (separate) reason.
        // getRowGroupsSkipped() is the portable half: it reports the pruning decision itself, so it
        // reddens on a host that runs no compiled filter at all, where the rows cannot.
        assertMemoryLeak(() -> {
            // ">=": the row is BELOW the bound, so the plain comparison drops it and only the
            // equality decides. Their f64 distance is 1.000000013351432e-10, just past
            // DOUBLE_TOLERANCE, so the f64 pair calls them unequal and drops the row; the same
            // subtraction at f32 is exactly FLOAT_EPSILON, which the inclusive f32 test calls equal,
            // so the compiled f32 filter KEEPS it. The row is the row group's max, so a bound
            // certified without the f32 arm prunes the group.
            createBoundarySaturatedPartialParquetTyped("FLOAT", "1.641532049179162e-11", "-100.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquetUnordered("c6 >= 1.1641532182693481e-10", "c6\n1.641532E-11\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            // The mirror image on "<=": the row is ABOVE the bound by the same f64 distance, and it
            // is the row group's min.
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");
            createBoundarySaturatedPartialParquetTyped("FLOAT", "1.1641532182693481e-10", "100.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquetUnordered("c6 <= 1.641532049179162e-11", "c6\n1.1641532E-10\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            // The STRICT ops (LT/GT) are NOT pinned here, and deliberately so. They spell
            // "!isEq && ...", so a WIDER isEq makes the model NARROWER - the opposite direction from
            // the two cases above - and the model therefore reads the comparator strictly on those
            // arms so that "!isEq" covers both readings. Their answer at this boundary depends on
            // which libquestdb is on the classpath, so no literal expectation is valid for them;
            // testFloatColumnPushdownMatchesNativeUnderEitherComparator asserts the property that
            // IS binary-independent - parquet returns exactly what the native table returns.

            // A bound clear of the band still prunes: the extra filter only widens the certification
            // band by about one f32 ulp, it does not disable pushdown.
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");
            createBoundarySaturatedPartialParquetTyped("FLOAT", "1.0", "100.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.0", "c6\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 > 100.0", "c6\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testFloatColumnPushdownMatchesNativeUnderEitherComparator() throws Exception {
        // THE INVARIANT: ParquetRowGroupFilter's certification model must contain every comparator
        // the RUNNING BINARY might carry, not only the one the C++ sources describe.
        //
        // ParquetRowGroupFilter is Java and ships the instant it merges. The native comparators do
        // not: no Maven profile compiles core/src/main/c/, so a checkout runs whichever libquestdb
        // is on the classpath - the committed one under core/src/main/resources/io/questdb/bin/,
        // which carries the INCLUSIVE reading ("epsilon >="), or an older one, or one built from an
        // earlier source tree, which carries the STRICT one ("epsilon > |lhs - rhs|").
        // float_cmp_epsilon / double_cmp_epsilon therefore have two readings and BOTH are reachable.
        //
        // Inclusiveness is not uniformly conservative for the model. The arms that spell
        // "isEq || ..." (LE, GE, EQ) get WIDER as isEq widens - safe. The arms that spell
        // "!isEq && ..." (LT, GT) get NARROWER - and a narrower model certifies bounds it should
        // have rejected, so the pruner drops a row group the running filter would have kept a row
        // from. Pruning runs before any row filter and nothing downstream can undo it, so that is
        // silent data loss, not a wrong-looking number. The model therefore reads the comparator
        // STRICTLY on LT/GT and INCLUSIVELY on LE/GE/EQ: the union contains both.
        //
        // Three witnesses, each of which prunes a group under an inclusive-everywhere model running
        // against a STRICT float_cmp_epsilon. The oracle is DIFFERENTIAL rather than a literal
        // expectation, because the answer itself is comparator-dependent - under the strict binary
        // the compiled filter keeps the row, under the inclusive one it drops it - while
        // "parquet returns what native returns" is true in both worlds and is exactly the property
        // pruning may never break. getRowGroupsSkipped() is the second oracle, and it is what keeps
        // this test from passing vacuously: the model is pure Java, so its decision does not move
        // with the binary, and an inclusive-everywhere model pushes a bound that prunes these groups
        // - "assertEquals(0, getRowGroupsSkipped())" reddens on that model under EITHER binary,
        // while the differential half can only redden on the one that keeps the row.
        //
        // The fixture holds REPEATED_FLOAT_ROW_COUNT rows in the parquet partition rather than one,
        // so the compiled filter's AVX2 loop actually runs - a tiny table is handled entirely by the
        // scalar tail and leaves jit/impl/avx2.h out of the picture.
        assertMemoryLeak(() -> {
            // The guard that stops the differential half from passing by comparing nothing to
            // nothing. Every site below answers with either the whole repeated block or nothing at
            // all, and WHICH of the two moves with the comparator the binary carries - so no single
            // site can pin a count. The SUM over the comparator-dependent sites pins an EXACT one,
            // because the total is BIMODAL: these fixtures sit exactly ON the tolerance boundary,
            // and the LT/GT arms ("!isEq && ...") and the LE/GE arms ("isEq || ...") are mirror
            // images across it. A comparator that calls that distance UNEQUAL feeds the LT/GT
            // family and starves the LE/GE pair; one that calls it EQUAL does the reverse. Exactly
            // one family bears rows under any given binary, so only two totals are reachable:
            //
            //   STRICT    -> 4 * REPEATED_FLOAT_ROW_COUNT - witnesses 1, 2, 4 and 5 each answer
            //                with the whole repeated block, the LE/GE pair with nothing.
            //   INCLUSIVE -> 2 * REPEATED_FLOAT_ROW_COUNT - the two families swap.
            //
            // Witness 3 ("c6 < 2e-10") is the LT/GT site missing from that first count, and it is
            // silent under BOTH comparators by construction, not by regression: 2e-10 is not
            // exactly a float, so that predicate widens to f64 rather than running the f32 arm, and
            // at f64 the distance is 9.99999986648568e-11 - just INSIDE DOUBLE_TOLERANCE - so both
            // comparators call the pair equal and "!isEq" drops the row either way. It still earns
            // its place: its tn-vs-tp differential and its getRowGroupsSkipped() == 0 pin that the
            // Java model declined to prune the group holding that row.
            //
            // Any other total means a site silently stopped bearing rows - exactly the data loss
            // this test exists to catch, and what a bare "> 0" check here would have hidden.
            // assertPartialParquetMatchesNativeUnderEitherComparator() returns the count and
            // separately pins that the compiled filter really ran.
            int comparatorDependentRows = 0;

            // Witness 1: "c6 < 1e-10" with a row at 0.0f. The pivot 1e-10 - 1e-10 is exactly 0.0, so
            // an inclusive-everywhere model certifies the bound 0.0f on step 0 and FILTER_OP_LT then
            // prunes every group with min >= 0.0f. The strict comparator keeps that row:
            // |0.0f - (float) 1e-10| is exactly FLOAT_EPSILON, which a strict test calls UNEQUAL,
            // and 0.0f < (float) 1e-10.
            createRepeatedFloatPartialParquet("0.0", "100.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            comparatorDependentRows += assertPartialParquetMatchesNativeUnderEitherComparator("c6 < 1e-10");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            // Witness 2: the exact mirror. "c6 > -1e-10" pivots on -1e-10 + 1e-10 == 0.0 and
            // certifies the same bound 0.0f, which FILTER_OP_GT uses to prune every group with
            // max <= 0.0f.
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");
            createRepeatedFloatPartialParquet("0.0", "-100.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            comparatorDependentRows += assertPartialParquetMatchesNativeUnderEitherComparator("c6 > -1e-10");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            // Witness 3: "c6 < 2e-10" with a row at (float) 1e-10. Its tn-vs-tp differential is
            // INERT under both comparators: 2e-10 is not exactly representable as a float, so the
            // comparison widens to f64, where the distance is 9.99999986648568e-11 - just INSIDE
            // DOUBLE_TOLERANCE - so both comparators call the pair equal and "!isEq" drops the row
            // either way. The row-group counter below is this site's real and only oracle: an
            // inclusive-everywhere model certifies (float) 1e-10 as the bound and prunes the group
            // holding that very row, so "assertEquals(0, getRowGroupsSkipped())" reddens on that
            // model under EITHER binary.
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");
            createRepeatedFloatPartialParquet("1e-10", "100.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            comparatorDependentRows += assertPartialParquetMatchesNativeUnderEitherComparator("c6 < 2e-10");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            // Two more of the same class, moved here out of
            // testFloatColumnInclusiveOpCertifiesAgainstCompiledF32Filter, where they used to carry
            // a literal expectation that only the inclusive binary satisfies.
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");
            createRepeatedFloatPartialParquet("1.6415322226515094e-11", "100.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            comparatorDependentRows += assertPartialParquetMatchesNativeUnderEitherComparator("c6 < 1.1641532182693481e-10");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");
            createRepeatedFloatPartialParquet("1.1641532182693481e-10", "-100.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            comparatorDependentRows += assertPartialParquetMatchesNativeUnderEitherComparator("c6 > 1.6415322226515094e-11");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            // The two predicates that testFloatColumnInclusiveOpCertifiesAgainstCompiledF32Filter
            // pins with a literal, repeated here differentially. That test reddens on the ALL-NATIVE
            // table whenever the binary carries the strict comparator, so its tp assertion is never
            // reached; these two prove that tp agrees with tn there anyway - i.e. that its failure
            // really is "the library predates the comparator change" and not a lost row group.
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");
            createRepeatedFloatPartialParquet("1.641532049179162e-11", "-100.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            comparatorDependentRows += assertPartialParquetMatchesNativeUnderEitherComparator("c6 >= 1.1641532182693481e-10");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");
            createRepeatedFloatPartialParquet("1.1641532182693481e-10", "100.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            comparatorDependentRows += assertPartialParquetMatchesNativeUnderEitherComparator("c6 <= 1.641532049179162e-11");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            Assert.assertTrue(
                    "the comparator-dependent sites must total " + (4 * REPEATED_FLOAT_ROW_COUNT)
                            + " under the STRICT comparator (witnesses 1, 2, 4 and 5 bear the whole"
                            + " repeated block, the LE/GE pair bears nothing) or "
                            + (2 * REPEATED_FLOAT_ROW_COUNT) + " under the INCLUSIVE one (the two"
                            + " families swap); any other total means a site silently stopped"
                            + " bearing rows [total=" + comparatorDependentRows + "]",
                    comparatorDependentRows == 4 * REPEATED_FLOAT_ROW_COUNT
                            || comparatorDependentRows == 2 * REPEATED_FLOAT_ROW_COUNT
            );

            // The union model costs a handful of declined pushdowns at near-zero / near-tolerance
            // bounds and nothing anywhere else. A bound clear of the band still prunes on the very
            // arms the model tightened, so LT/GT pushdown is not disabled, only deferred.
            //
            // These last two sites are EMPTY under either comparator by construction - the table
            // holds only 1.0f and 100.0f, and neither "< 1.0" nor "> 100.0" can match either of
            // them - so their tn-vs-tp comparison really is empty-vs-empty and proves nothing on its
            // own. That is deliberate: what they exist to pin is the pruning SIGNAL, and
            // getRowGroupsSkipped() > 0 is a genuine binary-independent oracle for it. They are
            // excluded from comparatorDependentRows for exactly that reason.
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");
            createRepeatedFloatPartialParquet("1.0", "100.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            Assert.assertEquals(0, assertPartialParquetMatchesNativeUnderEitherComparator("c6 < 1.0"));
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            Assert.assertEquals(0, assertPartialParquetMatchesNativeUnderEitherComparator("c6 > 100.0"));
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testFloatConstantFractionalPushdownNotFalsePruned() throws Exception {
        // A fractional FLOAT bound truncates in the narrow (SHORT) stats slot via
        // (int) getDouble(), the FLOAT twin of the DOUBLE arm. "c6 < 1.5" -> "c6 < 1"
        // false-prunes a SHORT group at 1.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquetTyped("SHORT", "1", "100");

            assertNativeMatchesPartialParquet("c6 < 1.5::float", "c6\n1\n");
            assertNativeMatchesPartialParquet("c6 <= 1.5::float", "c6\n1\n");
        });
    }

    @Test
    public void testFractionalDoubleBoundStillPrunes() throws Exception {
        // The fix for the fractional false-prune must not surrender pruning: a fractional bound
        // rounds the way its op preserves ("c6 < 1.5" is "c6 < 2", "c6 > 4.5" is "c6 > 4"), which
        // selects the same rows as the double comparison and still skips a group that lies wholly
        // outside it.
        assertMemoryLeak(() -> {
            // Parquet group saturated at 5, native row at 1: "< 1.5" ("< 2") must skip the group.
            createBoundarySaturatedPartialParquet(5, 1);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.5", "c6\n1\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 <= 1.5", "c6\n1\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // Parquet group saturated at 1, native row at 5: "> 4.5" ("> 4") must skip the group.
            createBoundarySaturatedPartialParquet(1, 5);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 > 4.5", "c6\n5\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 >= 4.5", "c6\n5\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // The 64-bit stats slot rounds the same way, below the 2^53 precision ceiling.
            createBoundarySaturatedPartialParquetTyped("LONG", "5", "1");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.5", "c6\n1\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testLongConstantBelowIntRangePushdownNotFalsePruned() throws Exception {
        // Parquet partition saturated at INT_MIN+1 (-2147483647). A below-INT-range
        // LONG bound saturates in the INT stats slot; c > -5e9 matches every row, so
        // the group must NOT prune -- a false-prune would drop the parquet row.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquet(-2_147_483_647, 0);

            assertNativeMatchesPartialParquet("c6 > -5_000_000_000", "c6\n-2147483647\n0\n");
            assertNativeMatchesPartialParquet("c6 >= -5_000_000_000", "c6\n-2147483647\n0\n");

            // The other direction is unsatisfiable for every INT row, so every group prunes: "<"
            // pushes the saturated INT_MIN bound (min >= INT_MIN always holds) and "<=" rewrites
            // to it. The DOUBLE spelling of the bound takes the same route.
            for (String bound : new String[]{"-5_000_000_000", "-5e9"}) {
                for (String op : new String[]{"<", "<="}) {
                    ParquetRowGroupFilter.resetRowGroupsSkipped();
                    assertNativeMatchesPartialParquet("c6 " + op + ' ' + bound, "c6\n");
                    Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

                }
            }
        });
    }

    @Test
    public void testSkipRowsWithFiniteBoundUnderActivePushdown() throws Exception {
        // Direct coverage for aa809bb54f ("Fix LIMIT under-count on pushdown-pruned skip").
        // Under active pushdown skipRows() walks row by row, and that walk must run unclamped:
        // hasNext() charges every row it yields against maxRowsAfterSkip, so a clamp of 0 - the
        // value LimitRecordCursor.calculateSize() passes to size a cursor it will not read - let
        // the first hasNext() report exhaustion, and the skip then skipped nothing.
        //
        // The route that originally reached this state, a BYTE "IS NOT NULL" that folds to TRUE
        // while leaving pushdown active, is gone now that PushdownFilterExtractor refuses null
        // ops on BYTE. So drive the cursor directly instead of through a LIMIT. A non-constant
        // "val >= 0" is extracted as OP_GE and keeps pushdown active on the scan, while
        // SqlCodeGenerator still hands that scan filter == null because the filter lives in the
        // wrapper above it. That reproduces the same filter == null + entity + forward scan +
        // active pushdown state without depending on constant folding.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT x::INT, timestamp_sequence('2024-01-01', 60_000_000)
                    FROM long_sequence(500)
                    """);
            // Second partition makes 2024-01-01 a non-active partition so it converts.
            execute("INSERT INTO x VALUES (501, '2024-01-02T02:00:00.000000Z')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            try (RecordCursorFactory factory = select("SELECT * FROM x WHERE val >= 0")) {
                RecordCursorFactory scan = factory;
                for (int i = 0; i < 8 && scan != null && !(scan instanceof PageFrameRecordCursorFactory); i++) {
                    scan = scan.getBaseFactory();
                }
                Assert.assertTrue(
                        "expected to reach the page-frame scan under the filter wrapper [factory="
                                + (scan == null ? "null" : scan.getClass().getSimpleName()) + ']',
                        scan instanceof PageFrameRecordCursorFactory
                );
                try (RecordCursor cursor = scan.getCursor(sqlExecutionContext)) {
                    // size() == -1 proves pushdown is active, which is what routes skipRows()
                    // into the walk rather than the metadata-only fast path.
                    Assert.assertEquals(-1, cursor.size());
                    RecordCursor.Counter counter = new RecordCursor.Counter();
                    counter.set(16);
                    cursor.skipRows(counter, 0);
                    Assert.assertEquals(0, counter.get());

                }
            }
        });
    }


    @Test
    public void testLongConstantOutsideIntRangePushdownNotFalsePruned() throws Exception {
        // Parquet partition saturated at INT_MAX (2147483647). An above-INT-range LONG
        // bound saturates in the INT stats slot; c < 5e9 matches every row, so the
        // group must NOT prune -- a false-prune would drop the parquet row.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquet(2_147_483_647, 0);

            assertNativeMatchesPartialParquet("c6 < 5_000_000_000", "c6\n2147483647\n0\n");
            assertNativeMatchesPartialParquet("c6 <= 5_000_000_000", "c6\n2147483647\n0\n");

            // The other direction is unsatisfiable for every INT row, so every group prunes: ">"
            // pushes the saturated INT_MAX bound (max <= INT_MAX always holds) and ">=" rewrites
            // to it. The DOUBLE spelling of the bound takes the same route.
            for (String bound : new String[]{"5_000_000_000", "5e9"}) {
                for (String op : new String[]{">", ">="}) {
                    ParquetRowGroupFilter.resetRowGroupsSkipped();
                    assertNativeMatchesPartialParquet("c6 " + op + ' ' + bound, "c6\n");
                    Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
                }
            }
        });
    }

    @Test
    public void testOverflowingIntConstantPushdownWrapsToInt() throws Exception {
        // An overflowing INT constant compared against a narrow column (BYTE/SHORT/INT)
        // must wrap mod 2^32 in the parquet pushdown like the native INT-precision scan.
        // Before the fix the pushdown read the un-wrapped LONG and wrongly pruned a
        // parquet partition, so a partial-parquet table disagreed with its native sibling.
        //   (-2649 * 965_823) wraps to INT +1_736_502_169 (> any row): const>col matches all.
        //   ( 2649 * 965_823) wraps to INT -1_736_502_169 (< any row): col>const matches all.
        assertMemoryLeak(() -> {
            for (String type : new String[]{"BYTE", "SHORT", "INT"}) {
                createNativeAndPartialParquetNarrowColumn(type);

                // Positive wrap: every row passes; wrongly pruned on HEAD.
                assertNativeMatchesPartialParquet("(-2649::SHORT * (965_823)::INT) > c6", "c6\n1\n2\n");
                assertNativeMatchesPartialParquet("c6 < (-2649::SHORT * (965_823)::INT)", "c6\n1\n2\n");
                // Negative wrap: every row passes; wrongly pruned on HEAD from the other side.
                assertNativeMatchesPartialParquet("c6 > (2649::SHORT * (965_823)::INT)", "c6\n1\n2\n");

                // Controls: the wrapped constant excludes every row (pass on HEAD too). The
                // parquet side of the fixture is one converted partition holding one row, so its
                // single row group is the only prunable one - and an unsatisfiable bound must
                // prune it. Without this the differential is decline-blind: a pushdown that
                // refuses the shape outright scans that group in full, still agrees with the
                // all-native tn, and silently degrades the fix to "pruning off".
                ParquetRowGroupFilter.resetRowGroupsSkipped();
                assertNativeMatchesPartialParquet("c6 > (-2649::SHORT * (965_823)::INT)", "c6\n");
                Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
                ParquetRowGroupFilter.resetRowGroupsSkipped();
                assertNativeMatchesPartialParquet("c6 < (2649::SHORT * (965_823)::INT)", "c6\n");
                Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

                execute("DROP TABLE tn");
                execute("DROP TABLE tp");
            }
        });
    }

    private void assertDoubleColumnNearToleranceMagnitudePushdownNotFalsePruned() throws Exception {
        // getRowGroupsSkipped() counts cumulatively until resetRowGroupsSkipped() zeroes it, so the
        // extra cursor passes the assertQuery(...) battery runs cannot flip either counter verdict:
        // a declined pushdown adds nothing on any pass, and a firing one only adds more.
        assertMemoryLeak(() -> {
            // GE: pre-fix pushes nextDown(0.0) and prunes the -1e-30 group; the fix declines the pushdown.
            createBoundarySaturatedPartialParquetTyped("DOUBLE", "-1e-30", "5.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT c6 FROM tp WHERE c6 >= 1e-10 ORDER BY ts")
                    .noLeakCheck()
                    .returns("c6\n-1.0E-30\n5.0\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            // EQ collapses the same way: the BETWEEN lo = nextDown(1e-10 - DOUBLE_TOLERANCE) = nextDown(0.0).
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT c6 FROM tp WHERE c6 = 1e-10 ORDER BY ts")
                    .noLeakCheck()
                    .returns("c6\n-1.0E-30\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // LE mirrors GE: "c6 <= -1e-10" keeps 1e-30; pre-fix prunes on "min > nextUp(0.0)".
            createBoundarySaturatedPartialParquetTyped("DOUBLE", "1e-30", "-5.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT c6 FROM tp WHERE c6 <= -1e-10 ORDER BY ts")
                    .noLeakCheck()
                    .returns("c6\n1.0E-30\n-5.0\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            // A bound a clear tolerance away from zero still prunes a group that lies wholly outside it.
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");
            createBoundarySaturatedPartialParquetTyped("DOUBLE", "-1e-30", "5.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT c6 FROM tp WHERE c6 >= 1.0 ORDER BY ts")
                    .noLeakCheck()
                    .returns("c6\n5.0\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    // All-native tn and a partial-parquet sibling tp with identical data: two single-row
    // daily partitions, the first parquet so the row-group pushdown decides to scan it.
    private void createNativeAndPartialParquetNarrowColumn(String columnType) throws Exception {
        execute("CREATE TABLE tn (c6 " + columnType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE tp (c6 " + columnType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO tn VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-02T00:00:00.000000Z')");
        execute("INSERT INTO tp VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-02T00:00:00.000000Z')");
        execute("ALTER TABLE tp CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
    }

    @Test
    public void testIntWidthProductInAndCastMatchAcrossPartialParquet() throws Exception {
        // The INT-width IN / cast family (an overflowing a*b compared or IN-tested) had no partial
        // parquet coverage. Pruning runs before the row filter, so a mixed-storage table could drop a
        // row a single-storage table keeps if the pushdown disagreed with the row filter on the
        // widened value. The native partition runs the compiled/JIT filter, the parquet partition the
        // Java filter, so this differential pins that they agree for the width-sensitive shapes.
        //
        // Each assertion pins the plan node so the differential cannot silently degrade to
        // Java-vs-Java. The IN shapes compile to the JIT ("Async JIT Filter"); QueryAssertion rewrites
        // that expectation to "Async Filter" on a JIT-unsupported host, so on such hosts the check
        // becomes native-Java vs parquet-Java (still a valid pruning/width-agreement check). The
        // ::long-cast-of-arithmetic shapes are NOT compiled today (the JIT declines that cast, so both
        // tables run the Java filter): the "Async Filter" pin states that plainly and flags if the JIT
        // ever starts compiling them, at which point the compiled-vs-Java width agreement would need
        // re-verifying. The native frames are small, so the compiled filter runs its scalar tail here;
        // the four-lane AVX2 SIMD-body width class is covered by
        // CompiledFilterRegressionTest#testWideLaneIntColumnVsLongColumn.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tn (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE tp (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final String rows = """
                    INSERT INTO %s VALUES
                    (2_000_000_000, 2, '2024-01-01T00:00:00.000000Z'),
                    (100, 3, '2024-01-01T01:00:00.000000Z'),
                    (null, 5, '2024-01-02T00:00:00.000000Z'),
                    (7, 11, '2024-01-02T01:00:00.000000Z')
                    """;
            execute(rows.formatted("tn"));
            execute(rows.formatted("tp"));
            execute("ALTER TABLE tp CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");

            // (a*b) overflows INT for the first row and wraps to -294_967_296, in the pushdown, the
            // row filter and the cast alike. The parquet partition (day 1) runs the Java filter, the
            // native partition (day 2) the compiled filter, so each differential exercises both
            // within one query. The 4e9 bound now matches nothing, which is the point: the pushdown
            // must not keep a row the filter drops.
            assertIntWidthNativeMatchesParquet("(a * b) IN (4_000_000_000)", "Async JIT Filter", "a\n");
            assertIntWidthNativeMatchesParquet("(a * b) IN (-294_967_296)", "Async JIT Filter", "a\n2000000000\n");
            assertIntWidthNativeMatchesParquet("(a * b) IN (null, 300)", "Async JIT Filter", "a\n100\nnull\n");
            // JIT declines the ::long cast of an arithmetic subtree, so these run the Java filter on
            // both tables (see the method comment). The "Async Filter" pin keeps that honest.
            assertIntWidthNativeMatchesParquet("(a * b)::long > 1_000_000_000", "Async Filter", "a\n");
            assertIntWidthNativeMatchesParquet("(a * b)::long = -294_967_296", "Async Filter", "a\n2000000000\n");
        });
    }

    @Test
    public void testBeyondFloatRangeBoundDeclinesPushdown() throws Exception {
        // A finite DOUBLE bound beyond the FLOAT range narrows to +/-Infinity when pushed into a FLOAT
        // stats slot ((float) 1e40 == +Infinity). QuestDB records an overflowing FLOAT as +/-Infinity
        // in the stats, so pruning on the infinite bound is safe here; but an external read_parquet()
        // file may keep an infinite row out of its stats and be false-pruned. The FLOAT arm therefore
        // declines the pushdown for such a bound - no row group is skipped - and the query still
        // returns the correct rows via a superset scan.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquetTyped("FLOAT", "1.0", "2.0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT c6 FROM tp WHERE c6 >= 1e40 ORDER BY ts")
                    .noLeakCheck()
                    .returns("c6\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    // Asserts the all-native tn and the partial-parquet tp return the SAME rows for the width-sensitive
    // whereClause, and pins the filter plan node so the differential cannot silently become
    // Java-vs-Java. planNode is "Async JIT Filter" for shapes the JIT compiles (QueryAssertion rewrites
    // it to "Async Filter" on a JIT-unsupported host) and "Async Filter" for shapes it declines.
    private void assertIntWidthNativeMatchesParquet(String whereClause, String planNode, String expected) throws Exception {
        assertQuery("SELECT a FROM tn WHERE " + whereClause + " ORDER BY ts")
                .noLeakCheck().withPlanContaining(planNode).returns(expected);
        assertQuery("SELECT a FROM tp WHERE " + whereClause + " ORDER BY ts")
                .noLeakCheck().withPlanContaining(planNode).returns(expected);
    }

    // All-native tn and a partial-parquet sibling tp with identical data. The first daily
    // partition (single row = parquetValue) converts to parquet, so its INT stats are
    // min == max == parquetValue -- a group saturated at that exact value. The second row
    // (nativeValue) stays native so the pushdown actually scans the parquet partition.
    private void createBoundarySaturatedPartialParquet(int parquetValue, int nativeValue) throws Exception {
        createBoundarySaturatedPartialParquetTyped("INT", Integer.toString(parquetValue), Integer.toString(nativeValue));
    }

    // Typed variant of createBoundarySaturatedPartialParquet: the column type and the two row
    // values are supplied as text so the same single-row-parquet-group setup can exercise the
    // INT, narrow (BYTE/SHORT) and LONG stats slots.
    private void createBoundarySaturatedPartialParquetTyped(String columnType, String parquetValue, String nativeValue) throws Exception {
        execute("CREATE TABLE tn (c6 " + columnType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE tp (c6 " + columnType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO tn VALUES (" + parquetValue + ", '2024-01-01T00:00:00.000000Z'), (" + nativeValue + ", '2024-01-02T00:00:00.000000Z')");
        execute("INSERT INTO tp VALUES (" + parquetValue + ", '2024-01-01T00:00:00.000000Z'), (" + nativeValue + ", '2024-01-02T00:00:00.000000Z')");
        execute("ALTER TABLE tp CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
    }

    // Builds a native tn and a partial-parquet tp for inclusive-bound NULL pruning tests. Both tables
    // hold the same two partitions; the first (2024-01-01, values pq1/pq2 plus a NULL) is converted to
    // parquet while the second (2024-01-02, values nat1/nat2 plus a NULL) stays native. Only the first
    // partition is a parquet row group the pruner can skip, so a bound clear of pq1/pq2 exercises the
    // skip path, while the differential tn-vs-tp check confirms NULLs (absent from the min/max stats
    // and never matching an inclusive comparison) do not leak into the result.
    private void createNullMixedPartialParquet(String columnType, String pq1, String pq2, String nat1, String nat2) throws Exception {
        execute("CREATE TABLE tn (c6 " + columnType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE tp (c6 " + columnType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        final String rows = " VALUES"
                + " (null, '2024-01-01T00:00:00.000000Z'),"
                + " (" + pq1 + ", '2024-01-01T01:00:00.000000Z'),"
                + " (" + pq2 + ", '2024-01-01T02:00:00.000000Z'),"
                + " (null, '2024-01-02T00:00:00.000000Z'),"
                + " (" + nat1 + ", '2024-01-02T01:00:00.000000Z'),"
                + " (" + nat2 + ", '2024-01-02T02:00:00.000000Z')";
        execute("INSERT INTO tn" + rows);
        execute("INSERT INTO tp" + rows);
        execute("ALTER TABLE tp CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
    }

    // Binds :b to bound and asserts the all-native and the partially-parquet table return the SAME
    // rows. Pruning may only remove row groups that hold nothing the row filter keeps, so any
    // difference between the two tables is a pruning bug.
    private void assertBindVarBoundMatchesNative(String op, double bound, String expected) throws Exception {
        bindVariableService.clear();
        bindVariableService.setDouble("b", bound);
        assertQuery("SELECT c6 FROM tn WHERE c6 " + op + " :b ORDER BY ts")
                .noLeakCheck()
                .returns(expected);
        bindVariableService.clear();
        bindVariableService.setDouble("b", bound);
        assertQuery("SELECT c6 FROM tp WHERE c6 " + op + " :b ORDER BY ts")
                .noLeakCheck()
                .returns(expected);
    }

    // Same differential as assertNativeMatchesPartialParquet, minus the ORDER BY ts. Ordering by a
    // column the filter does not read enables parquet late materialization, and the frame then
    // reports column tops and falls back to the Java filter - which changes which rows survive at a
    // tolerance boundary, independently of pruning. Use this when the assertion is about a boundary
    // row; use the ordered variant when it is about ordering or about more than one row.
    private void assertNativeMatchesPartialParquetUnordered(String whereClause, String expected) throws Exception {
        assertQuery("SELECT c6 FROM tn WHERE " + whereClause)
                .noLeakCheck()
                .returns(expected);
        assertQuery("SELECT c6 FROM tp WHERE " + whereClause)
                .noLeakCheck()
                .returns(expected);
    }

    private void assertNativeMatchesPartialParquet(String whereClause, String expected) throws Exception {
        assertQuery("SELECT c6 FROM tn WHERE " + whereClause + " ORDER BY ts")
                .noLeakCheck()
                .returns(expected);
        assertQuery("SELECT c6 FROM tp WHERE " + whereClause + " ORDER BY ts")
                .noLeakCheck()
                .returns(expected);
    }

    // Asserts that the partially-parquet table returns exactly what the all-native table returns,
    // WITHOUT pinning what that is, and RETURNS the row count the two agreed on so the caller can
    // prove the comparison was not empty-vs-empty. The fluent assertQuery(...).returns(...) form
    // cannot be used for these predicates: they sit exactly on the floating-point tolerance boundary,
    // where the answer depends on whether the libquestdb on the classpath carries the STRICT
    // float_cmp_epsilon (an older library, or one built from an earlier source tree) or the
    // INCLUSIVE one (the C++ sources and the committed binaries). Only one of the two literals could
    // ever be written, so a literal expectation pins the comparator instead of the pruning - and the
    // pruning is what row-group pushdown may never get wrong. tn == tp is true under both
    // comparators and is false exactly when a row group holding a matching row was pruned.
    //
    // Parity on its own is not an oracle - two empty sinks match - so this helper adds two guards
    // that are themselves comparator-independent:
    //
    // 1. usesCompiledFilter() pins that the COMPILED filter really ran on both arms. Every witness's
    //    premise is "the f32 compiled arm keeps a row the f64 pair drops"; if the shape ever stopped
    //    compiling, both arms would run the Java filter, whose answer does not move with the binary,
    //    and the differential would compare one Java answer to another and prove nothing.
    // 2. The row count pins the SHAPE of the answer. createRepeatedFloatPartialParquet() writes one
    //    repeated value into the parquet partition and a single far-off value into the native one, so
    //    the only answers these predicates can have are "the whole repeated block" or "nothing at
    //    all". Anything in between is a row group half-lost, under either comparator.
    //
    // Neither guard can pin a row count outright, because the answer legitimately MOVES with the
    // comparator. The caller closes that gap: testFloatColumnPushdownMatchesNativeUnderEitherComparator
    // sums the counts its comparator-dependent sites return and asserts an EXACT total: either
    // 4 * REPEATED_FLOAT_ROW_COUNT under the STRICT comparator or 2 * REPEATED_FLOAT_ROW_COUNT under
    // the INCLUSIVE one. Only those two totals are reachable under BOTH binaries because FOUR of the
    // five LT/GT witnesses bear rows exactly when the LE/GE pair does not. The fifth ("c6 < 2e-10")
    // bears rows under neither, because its bound is not exactly representable as a float, so the
    // comparison widens to f64 and lands INSIDE DOUBLE_TOLERANCE rather than on the f32 boundary.
    //
    // No ORDER BY, deliberately, and the projection reads only the filtered column: ordering or
    // projecting by a column the filter does not read turns on parquet late materialization, which
    // leaves the unread column's address at 0, makes the frame report column tops and drops the
    // query onto the Java filter - which would defeat guard 1 above. Both tables hold two partitions
    // with distinct timestamps, so the scan order is deterministic without one.
    private int assertPartialParquetMatchesNativeUnderEitherComparator(String whereClause) throws Exception {
        final StringSink nativeSink = new StringSink();
        final int nativeRows = printCompiledFilterQuery("SELECT c6 FROM tn WHERE " + whereClause, nativeSink);
        final StringSink parquetSink = new StringSink();
        printCompiledFilterQuery("SELECT c6 FROM tp WHERE " + whereClause, parquetSink);
        TestUtils.assertEquals(
                "row group pruning changed the result of: " + whereClause,
                nativeSink,
                parquetSink
        );
        Assert.assertTrue(
                "the all-native table answered with a partial repeated block, so the fixture no longer"
                        + " bounds the answer to 0 or " + REPEATED_FLOAT_ROW_COUNT + " rows [rows=" + nativeRows
                        + "] for: " + whereClause,
                nativeRows == 0 || nativeRows == REPEATED_FLOAT_ROW_COUNT
        );
        return nativeRows;
    }

    // Runs a query with the compiled filter pinned ON, prints it into sink and returns the number of
    // DATA rows printed - CursorPrinter emits a header line first, so the row count is one less than
    // the number of newlines.
    private int printCompiledFilterQuery(String query, StringSink sink) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertTrue(
                    "the compiled filter did not run, so this query no longer exercises the native"
                            + " comparator: " + query,
                    factory.usesCompiledFilter()
            );
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                CursorPrinter.println(cursor, factory.getMetadata(), sink);
            }
        }
        int newlines = 0;
        for (int i = 0, n = sink.length(); i < n; i++) {
            if (sink.charAt(i) == '\n') {
                newlines++;
            }
        }
        return Math.max(0, newlines - 1);
    }

    // Builds the FLOAT pair the tolerance-boundary pruning tests need: tn all native, tp with its
    // first partition converted to parquet, both holding REPEATED_FLOAT_ROW_COUNT copies of
    // parquetValue on 2024-01-01 and one nativeValue row on 2024-01-02. The repeated value makes the
    // row group's min and max the boundary value itself.
    private void createRepeatedFloatPartialParquet(String parquetValue, String nativeValue) throws Exception {
        execute("CREATE TABLE tn (c6 FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE tp (c6 FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        final String rows = " SELECT " + parquetValue + "::FLOAT, timestamp_sequence('2024-01-01', 1_000)"
                + " FROM long_sequence(" + REPEATED_FLOAT_ROW_COUNT + ")";
        execute("INSERT INTO tn" + rows);
        execute("INSERT INTO tp" + rows);
        execute("INSERT INTO tn VALUES (" + nativeValue + ", '2024-01-02T00:00:00.000000Z')");
        execute("INSERT INTO tp VALUES (" + nativeValue + ", '2024-01-02T00:00:00.000000Z')");
        execute("ALTER TABLE tp CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
    }

    private void assertHasParquetPartitions(String tableName, boolean expected) {
        TableToken tableToken = engine.verifyTableName(tableName);
        try (TableReader tableReader = engine.getReader(tableToken)) {
            Assert.assertEquals(expected, tableReader.hasParquetPartitions());
        }
        try (MetadataCacheReader reader = engine.getMetadataCache().readLock()) {
            CairoTable table = reader.getTable(tableToken);
            Assert.assertNotNull(table);
            Assert.assertEquals(expected, table.hasParquetPartitions());
        }
    }

    // Builds a parquet table where 'c2' carries a column top: the 500 rows on 2024-01-01 predate
    // the ADD COLUMN and hold no stored value, so the parquet writer marks them with definition
    // level 0 and the row group reports null_count == num_values. The single 2024-01-02 row keeps
    // that partition active so only 2024-01-01 converts.
    private void createColumnTopParquetTable(String columnType, String lastValue) throws Exception {
        execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO x
                SELECT x::INT, timestamp_sequence('2024-01-01', 100_000)
                FROM long_sequence(500)
                """);
        execute("ALTER TABLE x ADD COLUMN c2 " + columnType);
        execute("INSERT INTO x VALUES (8000, '2024-01-02T02:00:00.000000Z', " + lastValue + ")");
        execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");
    }

    // Builds a parquet table whose live column 'b' is the original low-range column 'a', while
    // the frozen parquet column still literally named 'b' is the original high-range column.
    // Resolving a filter by the stale parquet name targets the wrong column's stats and wrongly
    // prunes; resolving by stable column id keeps it correct. The third partition stays active so
    // the first two convert to parquet.
    private void createRenamedNumericParquetTable() throws Exception {
        execute("CREATE TABLE x (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO x VALUES
                (15_000, 75_000, '2024-01-01T00:00:00.000000Z'),
                (25_000, 85_000, '2024-01-01T01:00:00.000000Z'),
                (35_000, 95_000, '2024-01-02T00:00:00.000000Z'),
                (45_000, 99_000, '2024-01-02T01:00:00.000000Z'),
                (55_000, 65_000, '2024-01-03T00:00:00.000000Z')
                """);
        execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");
        execute("ALTER TABLE x RENAME COLUMN b TO c");
        execute("ALTER TABLE x RENAME COLUMN a TO b");
    }
}
