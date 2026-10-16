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

package io.questdb.test.cutlass.qwp;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpEgressConnSymbolDict;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

public class QwpEgressParquetPageFrameTest extends AbstractQwpBootstrapTest {

    private static final String[] SMALL_FRAMES_AND_ROW_GROUPS = new String[]{
            PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "64",
            PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS.getEnvVarName(), "32",
            PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE.getEnvVarName(), "32"
    };

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAllTypesParityAllParquet() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startFragmented(SMALL_FRAMES_AND_ROW_GROUPS)) {
                server.execute("""
                        CREATE TABLE allp(
                            ts TIMESTAMP,
                            l LONG,
                            d DOUBLE,
                            i INT,
                            ip IPv4,
                            f FLOAT,
                            sh SHORT,
                            ch CHAR,
                            by BYTE,
                            bo BOOLEAN,
                            sym SYMBOL,
                            vc VARCHAR,
                            s STRING,
                            bin BINARY,
                            u UUID,
                            l256 LONG256,
                            d64 DECIMAL(18,4),
                            d128 DECIMAL(38,6),
                            d256 DECIMAL(76,10),
                            gh GEOHASH(12b),
                            a DOUBLE[],
                            tsn TIMESTAMP_NS
                        ) TIMESTAMP(ts) PARTITION BY DAY WAL
                        """);
                server.execute("""
                        INSERT INTO allp
                        SELECT
                            CAST(86_400_000_000L * ((x - 1) / 150) + x * 1_000L AS TIMESTAMP),
                            CASE WHEN x % 17 = 0 THEN CAST(NULL AS LONG) ELSE x END,
                            CASE WHEN x % 19 = 0 THEN CAST(NULL AS DOUBLE) ELSE x * 1.5 END,
                            CASE WHEN x % 23 = 0 THEN CAST(NULL AS INT) ELSE x::INT END,
                            CAST('192.168.1.1' AS IPv4),
                            x::FLOAT,
                            x::SHORT,
                            'Q',
                            x::BYTE,
                            x % 2 = 0,
                            CASE WHEN x % 29 = 0 THEN CAST(NULL AS SYMBOL) ELSE 'sym_' || (x % 41)::STRING END,
                            CASE WHEN x % 31 = 0 THEN CAST(NULL AS VARCHAR) ELSE 'vc_' || x::STRING END,
                            CASE WHEN x % 37 = 0 THEN CAST(NULL AS STRING) ELSE 'str_' || x::STRING END,
                            rnd_bin(8, 8, 0),
                            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
                            CAST('0x0123456789ABCDEF' AS LONG256),
                            1234.5678m,
                            123456789.123456m,
                            123456789.1234567890m,
                            #012,
                            ARRAY[1.0, 2.0, 3.0],
                            x::TIMESTAMP_NS
                        FROM long_sequence(300)
                        """);
                server.awaitTable("allp");

                byte[] nativePayload = snapshotPayload("SELECT * FROM allp");
                server.execute("ALTER TABLE allp CONVERT PARTITION TO PARQUET WHERE ts >= 0");
                awaitTxnAndAssertParquetPartitions(server, "allp", 2, "1970-01-01\n1970-01-02\n");
                byte[] parquetPayload = snapshotPayload("SELECT * FROM allp");

                Assert.assertArrayEquals(nativePayload, parquetPayload);
            }
        });
    }

    @Test
    public void testColumnTopsAcrossParquetAndNative() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startFragmented(SMALL_FRAMES_AND_ROW_GROUPS)) {
                server.execute("CREATE TABLE tops(id LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("INSERT INTO tops SELECT x, dateadd('s', x::INT, '2024-01-01') FROM long_sequence(130)");
                server.awaitTable("tops");
                server.execute("ALTER TABLE tops CONVERT PARTITION TO PARQUET WHERE ts >= 0");
                awaitTxnAndAssertParquetPartitions(server, "tops", 2, "2024-01-01\n");
                server.execute("ALTER TABLE tops ADD COLUMN l LONG");
                server.execute("ALTER TABLE tops ADD COLUMN i INT");
                server.execute("ALTER TABLE tops ADD COLUMN d DOUBLE");
                server.execute("ALTER TABLE tops ADD COLUMN sh SHORT");
                server.execute("ALTER TABLE tops ADD COLUMN by BYTE");
                server.execute("ALTER TABLE tops ADD COLUMN ch CHAR");
                server.execute("ALTER TABLE tops ADD COLUMN bo BOOLEAN");
                TestUtils.drainWalQueue(server.getEngine());
                server.execute("""
                        INSERT INTO tops(id, ts, l, i, d, sh, by, ch, bo)
                        VALUES (3, '2024-01-02T00:00:00Z', 30, 31, 32.5, 33, 34, 'X', true)
                        """);
                server.awaitTable("tops");

                final int[] rows = {0};
                try (QwpQueryClient client = newClient()) {
                    client.execute("SELECT l, i, d, sh, by, ch, bo FROM tops", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++, rows[0]++) {
                                if (rows[0] < 130) {
                                    Assert.assertTrue(batch.isNull(0, r));
                                    Assert.assertTrue(batch.isNull(1, r));
                                    Assert.assertTrue(batch.isNull(2, r));
                                    Assert.assertFalse(batch.isNull(3, r));
                                    Assert.assertFalse(batch.isNull(4, r));
                                    Assert.assertFalse(batch.isNull(5, r));
                                    Assert.assertFalse(batch.isNull(6, r));
                                    Assert.assertEquals(0, batch.getShortValue(3, r));
                                    Assert.assertEquals(0, batch.getByteValue(4, r));
                                    Assert.assertEquals(0, batch.getCharValue(5, r));
                                    Assert.assertFalse(batch.getBoolValue(6, r));
                                } else {
                                    Assert.assertEquals(30, batch.getLongValue(0, r));
                                    Assert.assertEquals(31, batch.getIntValue(1, r));
                                    Assert.assertEquals(32.5, batch.getDoubleValue(2, r), 0.0);
                                    Assert.assertEquals(33, batch.getShortValue(3, r));
                                    Assert.assertEquals(34, batch.getByteValue(4, r));
                                    Assert.assertEquals('X', batch.getCharValue(5, r));
                                    Assert.assertTrue(batch.getBoolValue(6, r));
                                }
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.assertEquals(131, totalRows);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(131, rows[0]);
            }
        });
    }

    @Test
    public void testCountAndFilterFallbacks() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startFragmented(SMALL_FRAMES_AND_ROW_GROUPS)) {
                server.execute("CREATE TABLE fallback_t(x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("INSERT INTO fallback_t SELECT x, x::TIMESTAMP FROM long_sequence(500)");
                server.awaitTable("fallback_t");
                server.execute("ALTER TABLE fallback_t CONVERT PARTITION TO PARQUET WHERE ts >= 0");
                awaitTxnAndAssertParquetPartitions(server, "fallback_t", 2, "1970-01-01\n");

                final long[] count = {-1};
                final long[] filteredCount = {0};
                final long[] filteredSum = {0};
                try (QwpQueryClient client = newClient()) {
                    client.execute("SELECT count() FROM fallback_t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            count[0] = batch.getLongValue(0, 0);
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                    client.execute("SELECT x FROM fallback_t WHERE x % 10 = 0", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                filteredCount[0]++;
                                filteredSum[0] += batch.getLongValue(0, r);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(500, count[0]);
                Assert.assertEquals(50, filteredCount[0]);
                Assert.assertEquals(12_750, filteredSum[0]);
            }
        });
    }

    @Test
    public void testCoveringIndexIncludedFixedWidthValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startFragmented(SMALL_FRAMES_AND_ROW_GROUPS)) {
                server.execute("""
                        CREATE TABLE qwp_cov(
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (l, d),
                            l LONG,
                            d DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY DAY WAL
                        """);
                server.execute("""
                        INSERT INTO qwp_cov
                        SELECT
                            dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                            CASE WHEN x % 2 = 1 THEN 'A' ELSE 'B' END,
                            x,
                            x * 1.5
                        FROM long_sequence(300)
                        """);
                server.awaitTxn("qwp_cov", 1);

                final String sql = "SELECT l, d FROM qwp_cov WHERE sym = 'A'";
                final StringBuilder plan = new StringBuilder();
                final int[] rows = {0};
                try (QwpQueryClient client = newClient()) {
                    client.execute("EXPLAIN " + sql, new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                plan.append(batch.getString(0, r));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                    Assert.assertTrue(
                            "plan must use covering index: " + plan,
                            plan.indexOf("CoveringIndex on: sym") >= 0
                    );

                    client.execute(sql, new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++, rows[0]++) {
                                long expected = rows[0] * 2L + 1;
                                Assert.assertFalse("LONG row " + rows[0], batch.isNull(0, r));
                                Assert.assertFalse("DOUBLE row " + rows[0], batch.isNull(1, r));
                                Assert.assertEquals(expected, batch.getLongValue(0, r));
                                Assert.assertEquals(expected * 1.5, batch.getDoubleValue(1, r), 0.0);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.assertEquals(150, totalRows);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(150, rows[0]);
            }
        });
    }

    @Test
    public void testLazyAlterColumnTypeDoesNotBecomeNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startFragmented(SMALL_FRAMES_AND_ROW_GROUPS)) {
                server.execute("""
                        CREATE TABLE casts(
                            a LONG,
                            l VARCHAR,
                            d VARCHAR,
                            i VARCHAR,
                            ip VARCHAR,
                            f VARCHAR,
                            sh VARCHAR,
                            byt VARCHAR,
                            bo VARCHAR,
                            tsc VARCHAR,
                            dec VARCHAR,
                            ts TIMESTAMP
                        ) TIMESTAMP(ts) PARTITION BY DAY WAL
                        """);
                server.execute("""
                        INSERT INTO casts VALUES
                            (10, '100', '1.5', '11', '192.168.1.1', '2.5', '12', '13', 'true', '1970-01-01T00:00:00.000010Z', '12.34', 1::TIMESTAMP),
                            (20, '200', '-3.5', '-21', '10.0.0.1', '-4.5', '-22', '-23', 'false', '1970-01-01T00:00:00.000020Z', '-56.78', 2::TIMESTAMP),
                            (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 3::TIMESTAMP)
                        """);
                server.awaitTable("casts");
                server.execute("ALTER TABLE casts CONVERT PARTITION TO PARQUET WHERE ts >= 0");
                awaitTxnAndAssertParquetPartitions(server, "casts", 2, "1970-01-01\n");
                server.execute("ALTER TABLE casts ALTER COLUMN a TYPE VARCHAR");
                TestUtils.drainWalQueue(server.getEngine());
                server.execute("ALTER TABLE casts ALTER COLUMN l TYPE LONG");
                TestUtils.drainWalQueue(server.getEngine());
                server.execute("ALTER TABLE casts ALTER COLUMN d TYPE DOUBLE");
                TestUtils.drainWalQueue(server.getEngine());
                server.execute("ALTER TABLE casts ALTER COLUMN i TYPE INT");
                TestUtils.drainWalQueue(server.getEngine());
                server.execute("ALTER TABLE casts ALTER COLUMN ip TYPE IPv4");
                TestUtils.drainWalQueue(server.getEngine());
                server.execute("ALTER TABLE casts ALTER COLUMN f TYPE FLOAT");
                TestUtils.drainWalQueue(server.getEngine());
                server.execute("ALTER TABLE casts ALTER COLUMN sh TYPE SHORT");
                TestUtils.drainWalQueue(server.getEngine());
                server.execute("ALTER TABLE casts ALTER COLUMN byt TYPE BYTE");
                TestUtils.drainWalQueue(server.getEngine());
                server.execute("ALTER TABLE casts ALTER COLUMN bo TYPE BOOLEAN");
                TestUtils.drainWalQueue(server.getEngine());
                server.execute("ALTER TABLE casts ALTER COLUMN tsc TYPE TIMESTAMP");
                TestUtils.drainWalQueue(server.getEngine());
                server.execute("ALTER TABLE casts ALTER COLUMN dec TYPE DECIMAL(18,2)");
                TestUtils.drainWalQueue(server.getEngine());
                server.awaitTxn("casts", 13);

                final int[] rows = {0};
                try (QwpQueryClient client = newClient()) {
                    client.execute("SELECT a, l, d, i, ip, f, sh, byt, bo, tsc, dec FROM casts", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++, rows[0]++) {
                                if (rows[0] == 0) {
                                    Assert.assertFalse("VARCHAR", batch.isNull(0, r));
                                    Assert.assertFalse("LONG", batch.isNull(1, r));
                                    Assert.assertFalse("DOUBLE", batch.isNull(2, r));
                                    Assert.assertFalse("INT", batch.isNull(3, r));
                                    Assert.assertFalse("IPv4", batch.isNull(4, r));
                                    Assert.assertFalse("FLOAT", batch.isNull(5, r));
                                    Assert.assertFalse("SHORT", batch.isNull(6, r));
                                    Assert.assertFalse("BYTE", batch.isNull(7, r));
                                    Assert.assertFalse("BOOLEAN", batch.isNull(8, r));
                                    Assert.assertFalse("TIMESTAMP", batch.isNull(9, r));
                                    Assert.assertFalse("DECIMAL64", batch.isNull(10, r));
                                    Assert.assertEquals("VARCHAR", "10", batch.getString(0, r));
                                    Assert.assertEquals("LONG", 100, batch.getLongValue(1, r));
                                    Assert.assertEquals("DOUBLE", 1.5, batch.getDoubleValue(2, r), 0.0);
                                    Assert.assertEquals("INT", 11, batch.getIntValue(3, r));
                                    Assert.assertEquals("FLOAT", 2.5f, batch.getFloatValue(5, r), 0.0f);
                                    Assert.assertEquals("SHORT", 12, batch.getShortValue(6, r));
                                    Assert.assertEquals("BYTE", 13, batch.getByteValue(7, r));
                                    Assert.assertTrue("BOOLEAN", batch.getBoolValue(8, r));
                                    Assert.assertEquals("TIMESTAMP", 10, batch.getLongValue(9, r));
                                    Assert.assertEquals("DECIMAL64", 1_234, batch.getLongValue(10, r));
                                    Assert.assertEquals("IPv4", 0xC0A80101L, batch.getIntValue(4, r) & 0xFFFF_FFFFL);
                                } else if (rows[0] == 1) {
                                    Assert.assertEquals("20", batch.getString(0, r));
                                    Assert.assertEquals(200, batch.getLongValue(1, r));
                                    Assert.assertEquals(-3.5, batch.getDoubleValue(2, r), 0.0);
                                    Assert.assertEquals(-21, batch.getIntValue(3, r));
                                    Assert.assertEquals(0x0A000001L, batch.getIntValue(4, r) & 0xFFFF_FFFFL);
                                    Assert.assertEquals(-4.5f, batch.getFloatValue(5, r), 0.0f);
                                    Assert.assertEquals(-22, batch.getShortValue(6, r));
                                    Assert.assertEquals(-23, batch.getByteValue(7, r));
                                    Assert.assertFalse(batch.getBoolValue(8, r));
                                    Assert.assertEquals(20, batch.getLongValue(9, r));
                                    Assert.assertEquals(-5_678, batch.getLongValue(10, r));
                                } else {
                                    for (int c = 0; c < 11; c++) {
                                        if (c == 6 || c == 7 || c == 8) {
                                            Assert.assertFalse("column " + c, batch.isNull(c, r));
                                        } else {
                                            Assert.assertTrue("column " + c, batch.isNull(c, r));
                                        }
                                    }
                                    Assert.assertEquals(0, batch.getShortValue(6, r));
                                    Assert.assertEquals(0, batch.getByteValue(7, r));
                                    Assert.assertFalse(batch.getBoolValue(8, r));
                                }
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.assertEquals(3, totalRows);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(3, rows[0]);
            }
        });
    }

    @Test
    public void testMixedNativeAndParquetSymbolDictionary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startFragmented(SMALL_FRAMES_AND_ROW_GROUPS)) {
                server.execute("CREATE TABLE mixed(s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("""
                        INSERT INTO mixed
                        SELECT
                            CASE WHEN x % 37 = 0 THEN CAST(NULL AS SYMBOL) ELSE 's_' || (x % 41)::STRING END,
                            CAST(86_400_000_000L * ((x - 1) / 200) + x AS TIMESTAMP)
                        FROM long_sequence(400)
                        """);
                server.awaitTable("mixed");
                server.execute("ALTER TABLE mixed CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
                awaitTxnAndAssertParquetPartitions(server, "mixed", 2, "1970-01-01\n");

                final int[] counts = new int[41];
                final int[] nulls = {0};
                final int[] rows = {0};
                try (QwpQueryClient client = newClient()) {
                    client.execute("SELECT s FROM mixed", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++, rows[0]++) {
                                String value = batch.getSymbol(0, r);
                                if (value == null) {
                                    nulls[0]++;
                                } else {
                                    counts[Integer.parseInt(value.substring(2))]++;
                                }
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.assertEquals(400, totalRows);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(400, rows[0]);
                Assert.assertEquals(10, nulls[0]);
                for (int i = 0; i < counts.length; i++) {
                    Assert.assertTrue("symbol bucket " + i, counts[i] >= 8 && counts[i] <= 10);
                }
            }
        });
    }

    @Test
    public void testParquetColumnTopUsesBulkFill() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ObjList<QwpEgressColumnDef> columns = new ObjList<>();
            QwpEgressColumnDef longColumn = new QwpEgressColumnDef();
            longColumn.of("l", ColumnType.LONG);
            columns.add(longColumn);
            QwpEgressColumnDef shortColumn = new QwpEgressColumnDef();
            shortColumn.of("sh", ColumnType.SHORT);
            columns.add(shortColumn);
            QwpEgressColumnDef booleanColumn = new QwpEgressColumnDef();
            booleanColumn.of("bo", ColumnType.BOOLEAN);
            columns.add(booleanColumn);

            try (
                    QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict();
                    QwpResultBatchBuffer batch = new QwpResultBatchBuffer();
                    TestColumnTopRecord record = new TestColumnTopRecord()
            ) {
                batch.beginBatch(columns, null, dict);
                record.ofColumnTop();
                batch.appendPageFrame(new TestParquetPageFrame(), record, 0, 64);
                Assert.assertEquals(64, batch.getRowCount());
                Assert.assertEquals("LONG column top must not call the record accessor", 0, record.getLongCalls);
                Assert.assertEquals("SHORT column top must not call the record accessor", 0, record.getShortCalls);
                Assert.assertEquals("BOOLEAN column top must not call the record accessor", 0, record.getBoolCalls);

                batch.reset();
                batch.beginBatch(columns, null, dict);
                record.ofLazyCast();
                batch.appendPageFrame(new TestParquetPageFrame(), record, 0, 64);
                Assert.assertEquals(64, batch.getRowCount());
                Assert.assertEquals("LONG lazy cast must keep the record fallback", 64, record.getLongCalls);
                Assert.assertEquals("SHORT lazy cast must keep the record fallback", 64, record.getShortCalls);
                Assert.assertEquals("BOOLEAN lazy cast must keep the record fallback", 64, record.getBoolCalls);
            }
        });
    }

    @Test
    public void testMultipleRowGroupsAndFragmentedResume() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startFragmented(SMALL_FRAMES_AND_ROW_GROUPS)) {
                server.execute("CREATE TABLE resume_t(x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("INSERT INTO resume_t SELECT x, x::TIMESTAMP FROM long_sequence(1_000)");
                server.awaitTable("resume_t");
                server.execute("ALTER TABLE resume_t CONVERT PARTITION TO PARQUET WHERE ts >= 0");
                awaitTxnAndAssertParquetPartitions(server, "resume_t", 2, "1970-01-01\n");

                final long[] next = {1};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";max_batch_rows=37;")) {
                    client.connect();
                    client.execute("SELECT x FROM resume_t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                Assert.assertEquals(next[0]++, batch.getLongValue(0, r));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.assertEquals(1_000, totalRows);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(1_001, next[0]);
            }
        });
    }

    @Test
    public void testSymbolNullsAndHighCardinality() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startFragmented(SMALL_FRAMES_AND_ROW_GROUPS)) {
                server.execute("CREATE TABLE high_sym(s SYMBOL CAPACITY 4096, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("""
                        INSERT INTO high_sym
                        SELECT CASE WHEN x % 101 = 0 THEN CAST(NULL AS SYMBOL) ELSE 's_' || x::STRING END, x::TIMESTAMP
                        FROM long_sequence(2_000)
                        """);
                server.awaitTable("high_sym");
                server.execute("ALTER TABLE high_sym CONVERT PARTITION TO PARQUET WHERE ts >= 0");
                awaitTxnAndAssertParquetPartitions(server, "high_sym", 2, "1970-01-01\n");

                final int[] nulls = {0};
                final int[] rows = {0};
                try (QwpQueryClient client = newClient()) {
                    client.execute("SELECT s FROM high_sym", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++, rows[0]++) {
                                if (batch.isNull(0, r)) {
                                    nulls[0]++;
                                } else {
                                    Assert.assertEquals("s_" + (rows[0] + 1), batch.getSymbol(0, r));
                                }
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.assertEquals(2_000, totalRows);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(2_000, rows[0]);
                Assert.assertEquals(19, nulls[0]);
            }
        });
    }

    private static class TestColumnTopRecord extends PageFrameMemoryRecord {
        private int getBoolCalls;
        private int getLongCalls;
        private int getShortCalls;
        private final DirectLongList testAuxPageAddresses = new DirectLongList(1, MemoryTag.NATIVE_DEFAULT);
        private final DirectLongList testPageAddresses = new DirectLongList(1, MemoryTag.NATIVE_DEFAULT);

        @Override
        public void close() {
            testAuxPageAddresses.close();
            testPageAddresses.close();
            super.close();
        }

        @Override
        public boolean getBool(int columnIndex) {
            getBoolCalls++;
            return true;
        }

        @Override
        public long getLong(int columnIndex) {
            getLongCalls++;
            return 42;
        }

        @Override
        public short getShort(int columnIndex) {
            getShortCalls++;
            return 42;
        }

        private void of(boolean isLazyCast) {
            testAuxPageAddresses.clear();
            testPageAddresses.clear();
            sourceColumnTypes = new IntList();
            for (int i = 0; i < 3; i++) {
                testAuxPageAddresses.add(0);
                testPageAddresses.add(0);
                sourceColumnTypes.add(isLazyCast ? -ColumnType.VARCHAR : -1);
            }
            auxPageAddresses = testAuxPageAddresses;
            pageAddresses = testPageAddresses;
            columnOffset = 0;
            frameFormat = PartitionFormat.PARQUET;
            getBoolCalls = 0;
            getLongCalls = 0;
            getShortCalls = 0;
            hasTypeCasts = isLazyCast;
        }

        private void ofColumnTop() {
            of(false);
        }

        private void ofLazyCast() {
            of(true);
        }
    }

    private static class TestParquetPageFrame implements PageFrame {
        @Override
        public long getAuxPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public int getColumnCount() {
            return 3;
        }

        @Override
        public byte getFormat() {
            return PartitionFormat.PARQUET;
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public int getParquetRowGroup() {
            return 0;
        }

        @Override
        public int getParquetRowGroupHi() {
            return 64;
        }

        @Override
        public int getParquetRowGroupLo() {
            return 0;
        }

        @Override
        public long getPartitionHi() {
            return 64;
        }

        @Override
        public int getPartitionIndex() {
            return 0;
        }

        @Override
        public long getPartitionLo() {
            return 0;
        }
    }

    private static void awaitTxnAndAssertParquetPartitions(
            TestServerMain server,
            String tableName,
            long txn,
            String expectedPartitionNames
    ) {
        server.awaitTxn(tableName, txn);
        server.assertSql(
                "SELECT name FROM table_partitions('" + tableName + "') WHERE isParquet ORDER BY name",
                "name\n" + expectedPartitionNames
        );
    }

    private static QwpQueryClient newClient() throws Exception {
        QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";");
        client.connect();
        return client;
    }

    private static byte[] snapshotPayload(String sql) throws Exception {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        try (QwpQueryClient client = newClient()) {
            client.execute(sql, new QwpColumnBatchHandler() {
                @Override
                public void onBatch(QwpColumnBatch batch) {
                    long lo = batch.payloadAddr();
                    long hi = batch.payloadLimit();
                    for (long p = lo; p < hi; p++) {
                        payload.write(Unsafe.getByte(p));
                    }
                }

                @Override
                public void onEnd(long totalRows) {
                    Assert.assertEquals(300, totalRows);
                }

                @Override
                public void onError(byte status, String message) {
                    Assert.fail(message);
                }
            });
        }
        return payload.toByteArray();
    }
}
