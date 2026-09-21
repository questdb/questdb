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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
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
