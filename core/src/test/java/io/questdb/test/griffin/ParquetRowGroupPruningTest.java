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
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ParquetRowGroupPruningTest extends AbstractCairoTest {
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

            assertQuery("SELECT val FROM x WHERE val = 3.33")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 5.55")
                    .noLeakCheck()
                    .returns("""
                            val
                            5.55
                            """);
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
                    (6, '2024-01-02T01:00:00.000000Z', 10000)
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
                    (-30000, '2024-01-01T00:00:00.000000Z'),
                    (-200, '2024-01-01T01:00:00.000000Z'),
                    (-74, '2024-01-01T02:00:00.000000Z'),
                    (0, '2024-01-01T03:00:00.000000Z'),
                    (300, '2024-01-01T04:00:00.000000Z'),
                    (29_000, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // val = 0 must not skip the row group whose min is -30000.
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

            // val = -32000 sits below the row group min (-30000); should skip.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -32000")
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
            assertQuery("SELECT val FROM x WHERE val > 10000")
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
            assertQuery("SELECT val FROM x WHERE val > 10000")
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

    private void assertHasParquetPartitions(String tableName, boolean expected) {
        TableToken tableToken = engine.verifyTableName(tableName);
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
