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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.jit.JitUtil;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Deterministic LATEST ON coverage over parquet partitions. {@link ParallelLatestByTest}
 * converts to parquet only on a random boolean, so any given run may skip the parquet arm;
 * here every test converts deterministically, and the latest row for one key ('c') lives
 * only inside a parquet partition, so the backward scan must decode parquet rather than
 * stop in the native tail. Each test asserts the same expected rows before and after the
 * conversion, pinning that the format switch does not change latest-by results.
 */
public class LatestByParquetTest extends AbstractCairoTest {

    private static final String EXPECTED_ALL = """
            sym\tv\tts
            a\t8\t2020-01-03T00:00:00.000000Z
            b\t9\t2020-01-03T01:00:00.000000Z
            c\t5\t2020-01-01T04:00:00.000000Z
            """;
    private static final String EXPECTED_C = """
            sym\tv\tts
            c\t5\t2020-01-01T04:00:00.000000Z
            """;
    private static final String QUERY_ALL = "SELECT sym, v, ts FROM x LATEST ON ts PARTITION BY sym ORDER BY sym";
    private static final String QUERY_C = "SELECT sym, v, ts FROM x WHERE sym = 'c' LATEST ON ts PARTITION BY sym";

    @Test
    public void testLatestOnAllIndexedOverMixedPartitions() throws Exception {
        // Indexed symbol routes through LatestByAllIndexedRecordCursorFactory, whose worker
        // tasks (LatestByTask) construct the parquet decoder from the configuration.
        assertMemoryLeak(() -> {
            createMixedTable(true);
            assertQuery(QUERY_ALL).expectSize().returns(EXPECTED_ALL);
            assertPlanContains(QUERY_ALL, "LatestByAllIndexed");

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
            assertParquetPartitionCount(2);

            assertPlanContains(QUERY_ALL, "LatestByAllIndexed");
            assertQuery(QUERY_ALL).expectSize().returns(EXPECTED_ALL);
        });
    }

    @Test
    public void testLatestOnAllOverMixedPartitions() throws Exception {
        assertMemoryLeak(() -> {
            createMixedTable(false);
            assertQuery(QUERY_ALL).expectSize().returns(EXPECTED_ALL);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
            assertParquetPartitionCount(2);

            assertPlanContains(QUERY_ALL, "LatestBy");
            assertQuery(QUERY_ALL).expectSize().returns(EXPECTED_ALL);
        });
    }

    @Test
    public void testLatestOnAllOverParquetOnlyScanRange() throws Exception {
        // CONVERT skips the active partition, so scope the scan to the converted range:
        // every frame the latest-by reads is parquet.
        assertMemoryLeak(() -> {
            createMixedTable(false);
            final String query = "SELECT sym, v, ts FROM x WHERE ts < '2020-01-03' LATEST ON ts PARTITION BY sym ORDER BY sym";
            final String expected = """
                    sym\tv\tts
                    a\t6\t2020-01-02T00:00:00.000000Z
                    b\t7\t2020-01-02T01:00:00.000000Z
                    c\t5\t2020-01-01T04:00:00.000000Z
                    """;
            assertQuery(query).expectSize().returns(expected);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
            assertParquetPartitionCount(2);

            assertQuery(query).expectSize().returns(expected);
        });
    }

    @Test
    public void testLatestOnJitAcrossFrameFormatsAndKeyShapes() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(1, 17);
            try {
                setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 31);
                execute("""
                        CREATE TABLE jit_frames AS (
                          SELECT x id, (x % 11)::STRING::SYMBOL s, (x % 7)::STRING::SYMBOL t, (x % 5)::STRING::SYMBOL u,
                            (x % 13)::INT k, (x % 10)::INT v,
                            (CASE WHEN x % 19 = 0 THEN NULL ELSE 'pass' END)::VARCHAR payload,
                            (x / 3 * 500_000_000)::TIMESTAMP ts
                          FROM long_sequence(2000)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
                execute("CREATE TABLE jit_keys (s STRING)");
                execute("INSERT INTO jit_keys VALUES ('1'), ('3'), ('1'), (NULL), ('missing')");
                bindVariableService.setStr("key", "1");
                String[] keys = {"s", "s,t", "s,t,u", "k", "s,k", "s", "s", "s", "s", "s"};
                String[] predicates = {"", "", "", "", "", " AND s='1'", " AND s=:key",
                        " AND s IN ('1','3',NULL,'missing')", " AND s NOT IN ('1','3',NULL)",
                        " AND s IN (SELECT s FROM jit_keys)"};
                StringSink expected = new StringSink();
                for (int format = 0; format < 3; format++) {
                    if (format == 1) {
                        execute("ALTER TABLE jit_frames CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-04'");
                    } else if (format == 2) {
                        execute("ALTER TABLE jit_frames ALTER COLUMN v TYPE DOUBLE");
                    }
                    for (int i = 0; i < keys.length; i++) {
                        String query = "SELECT id FROM jit_frames WHERE v > 2 AND v < 8 AND payload != NULL"
                                + " AND ts >= 4_000_000_000::TIMESTAMP AND ts < 300_000_000_000::TIMESTAMP"
                                + predicates[i] + " LATEST ON ts PARTITION BY " + keys[i];
                        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                        expected.clear();
                        TestUtils.printSql(engine, sqlExecutionContext, query, expected);
                        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                        try (RecordCursorFactory factory = select(query)) {
                            Assert.assertTrue("format=" + format + ": " + query, factory.usesCompiledFilter());
                            assertFactory(factory).withContext(sqlExecutionContext).sizeMayVary().returns(expected.toString());
                        }
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testLatestOnJitBatchesAcrossFrameFormats() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(8193, 8193);
            try {
                setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8193);
                execute("""
                        CREATE TABLE jit_batches AS (
                          SELECT x id, (x % 7)::STRING::SYMBOL s, (x % 5)::STRING::SYMBOL t, (x % 3)::STRING::SYMBOL u,
                            (x % 11)::INT k, (CASE WHEN x % 17 = 0 THEN NULL ELSE x::STRING END)::VARCHAR p,
                            (((x-1) / 8193) * 86_400_000_000L + (x-1) % 8193)::TIMESTAMP ts
                          FROM long_sequence(24_577)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
                execute("CREATE TABLE batch_keys (s STRING)");
                execute("INSERT INTO batch_keys VALUES ('1'), ('3'), ('1'), (NULL), ('missing')");
                bindVariableService.setStr("key", "1");
                String[] keys = {"s", "s,t", "s,t,u", "k", "s,k", "s", "s", "s", "s", "s"};
                String[] predicates = {"", "", "", "", "", " AND s='1'", " AND s=:key",
                        " AND s IN ('1','3',NULL,'missing')", " AND s NOT IN ('1','3',NULL)",
                        " AND s IN (SELECT s FROM batch_keys)"};
                StringSink expected = new StringSink();
                for (int format = 0; format < 3; format++) {
                    if (format == 1) {
                        execute("ALTER TABLE jit_batches CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-03'");
                    } else if (format == 2) {
                        // The Parquet VARCHAR-to-LONG conversion requires the Java frame fallback.
                        execute("ALTER TABLE jit_batches ALTER COLUMN p TYPE LONG");
                    }
                    for (int i = 0; i < keys.length; i++) {
                        for (String interval : new String[]{"", " AND ts >= '1970-01-01T00:00:00.000100' AND ts < '1970-01-02T00:00:00.007001'"}) {
                            String query = "SELECT id FROM jit_batches WHERE id < 12_291 AND p != NULL"
                                    + interval + predicates[i] + " LATEST ON ts PARTITION BY " + keys[i];
                            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                            expected.clear();
                            TestUtils.printSql(engine, sqlExecutionContext, query, expected);
                            for (int mode : new int[]{SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_ENABLED}) {
                                sqlExecutionContext.setJitMode(mode);
                                try (RecordCursorFactory factory = select(query)) {
                                    Assert.assertTrue("format=" + format + ": " + query, factory.usesCompiledFilter());
                                    for (int attempt = 0; attempt < 2; attempt++) {
                                        assertFactory(factory).withContext(sqlExecutionContext).sizeMayVary().returns(expected.toString());
                                    }
                                }
                            }
                        }
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testLatestOnSingleIndexedKeyInParquetPartition() throws Exception {
        // sym='c' with an index routes through the indexed single-value factory; the only
        // 'c' rows live in the parquet partition.
        assertMemoryLeak(() -> {
            createMixedTable(true);
            assertQuery(QUERY_C).timestamp("ts").returns(EXPECTED_C);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
            assertParquetPartitionCount(2);

            assertPlanContains(QUERY_C, "Index");
            assertQuery(QUERY_C).timestamp("ts").returns(EXPECTED_C);
        });
    }

    @Test
    public void testLatestOnSingleKeyInParquetPartition() throws Exception {
        // sym='c' never appears after 2020-01-01, so the backward scan must walk through
        // the native partitions and decode the parquet one to find the latest 'c' row.
        assertMemoryLeak(() -> {
            createMixedTable(false);
            assertQuery(QUERY_C).timestamp("ts").returns(EXPECTED_C);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
            assertParquetPartitionCount(2);

            assertQuery(QUERY_C).timestamp("ts").returns(EXPECTED_C);
        });
    }

    @Test
    public void testLatestOnJitOverParquetKeepsOneDecodedFrame() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 0);
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100_000);
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 3 * 1024 * 1024L);
            execute("""
                    CREATE TABLE t AS (
                      SELECT x id, (x % 13)::INT k, (x % 10)::INT v, x::TIMESTAMP ts FROM long_sequence(300_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t VALUES (0, 0, 0, '1970-01-02')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            assertQuery("SELECT count() FROM table_partitions('t') WHERE isParquet")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n1\n");
            final String query = "SELECT * FROM t WHERE v > 2 AND v < 8 LATEST ON ts PARTITION BY k";
            final int jitMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            try {
                assertPlanContains(query, "jit: true");
                assertQuery(query).noLeakCheck().timestamp("ts").sizeMayVary().returns("""
                        id\tk\tv\tts
                        299965\t3\t5\t1970-01-01T00:00:00.299965Z
                        299966\t4\t6\t1970-01-01T00:00:00.299966Z
                        299975\t0\t5\t1970-01-01T00:00:00.299975Z
                        299976\t1\t6\t1970-01-01T00:00:00.299976Z
                        299977\t2\t7\t1970-01-01T00:00:00.299977Z
                        299985\t10\t5\t1970-01-01T00:00:00.299985Z
                        299986\t11\t6\t1970-01-01T00:00:00.299986Z
                        299987\t12\t7\t1970-01-01T00:00:00.299987Z
                        299993\t5\t3\t1970-01-01T00:00:00.299993Z
                        299994\t6\t4\t1970-01-01T00:00:00.299994Z
                        299995\t7\t5\t1970-01-01T00:00:00.299995Z
                        299996\t8\t6\t1970-01-01T00:00:00.299996Z
                        299997\t9\t7\t1970-01-01T00:00:00.299997Z
                        """);
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    private void assertParquetPartitionCount(int expected) throws Exception {
        assertQuery("SELECT count() FROM table_partitions('x') WHERE isParquet")
                .noLeakCheck().noRandomAccess().expectSize()
                .returns("count\n" + expected + "\n");
    }

    private void assertPlanContains(String query, String factoryMarker) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            planSink.clear();
            planSink.of(factory, sqlExecutionContext);
            final StringBuilder plan = new StringBuilder();
            for (int i = 1, n = planSink.getLineCount(); i <= n; i++) {
                final CharSequence line = planSink.getLine(i);
                if (Chars.contains(line, factoryMarker)) {
                    return;
                }
                plan.append(line).append('\n');
            }
            Assert.fail("expected a " + factoryMarker + " factory in the plan, got:\n" + plan);
        }
    }

    private void createMixedTable(boolean indexed) throws Exception {
        execute("CREATE TABLE x (sym SYMBOL" + (indexed ? " INDEX" : "") + ", v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        // 2020-01-01 holds the latest (and only) 'c' rows; later partitions carry only 'a'/'b'.
        execute("""
                INSERT INTO x VALUES
                ('a', 1, '2020-01-01T00:00:00'),
                ('b', 2, '2020-01-01T01:00:00'),
                ('c', 3, '2020-01-01T02:00:00'),
                ('a', 4, '2020-01-01T03:00:00'),
                ('c', 5, '2020-01-01T04:00:00')
                """);
        execute("INSERT INTO x VALUES ('a', 6, '2020-01-02T00:00:00'), ('b', 7, '2020-01-02T01:00:00')");
        execute("INSERT INTO x VALUES ('a', 8, '2020-01-03T00:00:00'), ('b', 9, '2020-01-03T01:00:00')");
    }
}
