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
import io.questdb.cairo.TableWriterMetrics;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.TableWriterMetricsRecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TableWriterMetricsRecordCursorFactoryTest extends AbstractCairoTest {

    @Test
    public void testCursor() {
        try (TableWriterMetricsRecordCursorFactory factory = new TableWriterMetricsRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertCursor(
                    toExpectedTableContent(snapshotMetrics()),
                    false,
                    true,
                    false,
                    cursor,
                    factory.getMetadata(),
                    factory.fragmentedSymbolTables()
            );
        }
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
                TestUtils.assertSql(localCompiler, localSqlExecutionContext, "select * from table_writer_metrics()", new StringSink(), toExpectedTableContent(metricsWhenDisabled));
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
        int cursorCount = 10;
        assertMemoryLeak(() -> {
            try (TableWriterMetricsRecordCursorFactory factory = new TableWriterMetricsRecordCursorFactory()) {
                for (int i = 0; i < cursorCount; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertCursor(
                                toExpectedTableContent(snapshotMetrics()),
                                false,
                                true,
                                false,
                                cursor, factory.getMetadata(),
                                factory.fragmentedSymbolTables()
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testSanity() {
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

    private void assertMetricsCursorEquals(MetricsSnapshot metricsSnapshot) {
        try (TableWriterMetricsRecordCursorFactory factory = new TableWriterMetricsRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertCursor(toExpectedTableContent(metricsSnapshot), cursor, factory.getMetadata(), true);
        }
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
