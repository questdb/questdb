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

package io.questdb.test.griffin.engine.table;

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterMetrics;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.metrics.QueryTracingJob;
import io.questdb.tasks.TelemetryTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TableWriterMetricsRecordCursorFactoryTest extends AbstractCairoTest {

    @Test
    public void testCursor() throws Exception {
        assertMetricsCursorEquals(snapshotMetrics());
    }

    @Test
    public void testDisabled() throws Exception {
        Metrics.ENABLED.disable();
        assertMemoryLeak(() -> {
            try (
                    CairoEngine localEngine = new CairoEngine(configuration);
                    SqlCompiler localCompiler = localEngine.getSqlCompiler();
                    SqlExecutionContext localSqlExecutionContext = TestUtils.createSqlExecutionCtx(localEngine)
            ) {
                MetricsSnapshot metricsWhenDisabled = new MetricsSnapshot(-1, -1, -1, -1, -1);
                assertQuery("select * from table_writer_metrics()")
                        .withCompiler(localCompiler)
                        .withContext(localSqlExecutionContext)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(toExpectedTableContent(metricsWhenDisabled));
            }
        });
    }

    @Test
    public void testMakingProgress() throws Exception {
        MetricsSnapshot metricsBefore = snapshotMetrics();
        assertMetricsCursorEquals(metricsBefore);

        TableModel tm = new TableModel(configuration, "tab1", PartitionBy.NONE);
        tm.timestamp("ts").col("ID", ColumnType.INT);
        createPopulateTable(tm, 1, "2020-01-01", 1);
        MetricsSnapshot metricsAfter = snapshotMetrics();
        assertNotEquals(metricsBefore, metricsAfter);

        assertMetricsCursorEquals(metricsAfter);
    }

    @Test
    public void testOneFactoryToMultipleCursors() throws Exception {
        // assertQuery exercises a single factory across several cursor opens (two result reads
        // plus a calculate-size pass), covering the one-factory-to-many-cursors path.
        assertMetricsCursorEquals(snapshotMetrics());
    }

    @Test
    public void testSanity() throws Exception {
        // we want to make sure metrics in tests are enabled by default
        assertTrue(engine.getMetrics().isEnabled());

        MetricsSnapshot metricsSnapshot = snapshotMetrics();
        assertMetricsCursorEquals(metricsSnapshot);
    }

    @Test
    public void testSql() throws Exception {
        assertQuery("select * from table_writer_metrics()")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns(toExpectedTableContent(snapshotMetrics()));
    }

    @Test
    public void testSystemTableWritesAreNotCounted() throws Exception {
        assertMemoryLeak(() -> {
            final MetricsSnapshot before = snapshotMetrics();
            // System tables by prefix and by fixed name: in-order and O3 commits, and a rollback.
            for (String tableName : new String[]{"sys.writer_metrics_test", TelemetryTask.TABLE_NAME, QueryTracingJob.TABLE_NAME}) {
                execute("CREATE TABLE \"" + tableName + "\" (ts TIMESTAMP, id INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO \"" + tableName + "\" VALUES ('2020-01-02', 1), ('2020-01-03', 2)");
                execute("INSERT INTO \"" + tableName + "\" VALUES ('2020-01-01', 3)");
                try (TableWriter writer = getWriter(tableName)) {
                    writer.newRow(0).append();
                    writer.rollback();
                }
            }
            Assert.assertEquals(before, snapshotMetrics());

            execute("CREATE TABLE user_tab (ts TIMESTAMP, id INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO user_tab VALUES ('2020-01-02', 1), ('2020-01-03', 2)");
            execute("INSERT INTO user_tab VALUES ('2020-01-01', 3)");
            final MetricsSnapshot after = snapshotMetrics();
            Assert.assertEquals(before.commitCount + 2, after.commitCount);
            Assert.assertEquals(before.o3CommitCount + 1, after.o3CommitCount);
            Assert.assertEquals(before.committedRows + 3, after.committedRows);
        });
    }

    private static MetricsSnapshot snapshotMetrics() {
        TableWriterMetrics writerMetrics = engine.getMetrics().tableWriterMetrics();
        return new MetricsSnapshot(writerMetrics.getCommitCount(),
                writerMetrics.getCommittedRows(),
                writerMetrics.getO3CommitCount(),
                writerMetrics.getRollbackCount(),
                writerMetrics.getPhysicallyWrittenRows()
        );
    }

    private static String toExpectedTableContent(MetricsSnapshot metricsSnapshot) {
        return "name\tvalue\n" +
                "total_commits" + '\t' + metricsSnapshot.commitCount + '\n' +
                "o3commits" + '\t' + metricsSnapshot.o3CommitCount + '\n' +
                "rollbacks" + '\t' + metricsSnapshot.rollbackCount + '\n' +
                "committed_rows" + '\t' + metricsSnapshot.committedRows + '\n' +
                "physically_written_rows" + '\t' + metricsSnapshot.physicallyWrittenRows + '\n';
    }

    private void assertMetricsCursorEquals(MetricsSnapshot metricsSnapshot) throws Exception {
        // table_writer_metrics() compiles to TableWriterMetricsRecordCursorFactory; the builder
        // re-opens the cursor several times from that single factory.
        assertQuery("select * from table_writer_metrics()")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns(toExpectedTableContent(metricsSnapshot));
    }

    private record MetricsSnapshot(long commitCount, long committedRows, long o3CommitCount, long rollbackCount,
                                   long physicallyWrittenRows) {

        @Override
        public @NotNull String toString() {
            return "MetricsSnapshot{" +
                    "commitCount=" + commitCount +
                    ", committedRows=" + committedRows +
                    ", o3CommitCount=" + o3CommitCount +
                    ", rollbackCount=" + rollbackCount +
                    ", physicallyWrittenRows=" + physicallyWrittenRows +
                    '}';
        }
    }
}
