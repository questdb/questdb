/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableModel;
import io.questdb.cairo.TableWriterMetrics;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TableWriterMetricsRecordCursorFactoryTest extends AbstractGriffinTest {

    @Test
    public void testSanity() throws Exception {
        // we want to make sure metrics in tests are enabled by default
        assertTrue(metrics.isEnabled());
        assertTrue(engine.getMetrics().isEnabled());

        MetricsSnapshot metricsSnapshot = snapshotMetrics();
        assertMetricsCursorEquals(metricsSnapshot);
    }

    @Test
    public void testMakingProgress() throws Exception {
        MetricsSnapshot metricsBefore = snapshotMetrics();
        assertMetricsCursorEquals(metricsBefore);

        try (TableModel tm = new TableModel(configuration, "tab1", PartitionBy.NONE)) {
            tm.timestamp("ts").col("ID", ColumnType.INT);
            createPopulateTable(tm, 1, "2020-01-01", 1);
        }
        MetricsSnapshot metricsAfter = snapshotMetrics();
        assertNotEquals(metricsBefore, metricsAfter);

        assertMetricsCursorEquals(metricsAfter);
    }

    @Test
    public void testSql() throws Exception{
        printSqlResult(() -> toExpectedTableContent(snapshotMetrics()), "select * from table_writer_metrics()", null, null, null, false, true, true, false, null);
    }

    @Test
    public void testDisabled() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine localEngine = new CairoEngine(configuration, Metrics.disabled());
                SqlCompiler localCompiler = new SqlCompiler(localEngine, null, snapshotAgent);
                SqlExecutionContextImpl localSqlExecutionContext = new SqlExecutionContextImpl(localEngine, 1)
                         .with(AllowAllCairoSecurityContext.INSTANCE,
                                 new BindVariableServiceImpl(configuration),
                                 null,
                                 -1,
                                 null)) {
                MetricsSnapshot metricsWhenDisabled = new MetricsSnapshot(-1, -1, -1, -1, -1);
                TestUtils.assertSql(localCompiler, localSqlExecutionContext, "select * from table_writer_metrics()", new StringSink(), toExpectedTableContent(metricsWhenDisabled));
            }
        });
    }

    @Test
    public void testOneFactoryToMultipleCursors() throws Exception {
        int cursorCount = 10;
        assertMemoryLeak(() -> {
            try (TableWriterMetricsRecordCursorFactory factory = new TableWriterMetricsRecordCursorFactory()) {
                for (int i = 0; i < cursorCount; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertCursor(toExpectedTableContent(snapshotMetrics()), false, true, true, false, cursor, factory.getMetadata(), false);
                    }
                }
            }
        });
    }

    @Test
    public void testCursor() throws Exception{
        try (TableWriterMetricsRecordCursorFactory factory = new TableWriterMetricsRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertCursor(toExpectedTableContent(snapshotMetrics()), false, true, true, false, cursor, factory.getMetadata(), false);
        }
    }

    private void assertMetricsCursorEquals(MetricsSnapshot metricsSnapshot) throws Exception {
        try (TableWriterMetricsRecordCursorFactory factory = new TableWriterMetricsRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertCursor(toExpectedTableContent(metricsSnapshot), cursor, factory.getMetadata(), true);
        }
    }

    private static String toExpectedTableContent(MetricsSnapshot metricsSnapshot) {
        StringBuilder sb = new StringBuilder("name\tvalue\n")
                .append("total_commits").append('\t').append(metricsSnapshot.commitCount).append('\n')
                .append("o3commits").append('\t').append(metricsSnapshot.o3CommitCount).append('\n')
                .append("rollbacks").append('\t').append(metricsSnapshot.rollbackCount).append('\n')
                .append("committed_rows").append('\t').append(metricsSnapshot.committedRows).append('\n')
                .append("physically_written_rows").append('\t').append(metricsSnapshot.physicallyWrittenRows).append('\n');
        return sb.toString();
    }

    private static class MetricsSnapshot {
        private final long commitCount;
        private final long committedRows;
        private final long o3CommitCount;
        private final long rollbackCount;
        private final long physicallyWrittenRows;

        private MetricsSnapshot(long commitCount, long committedRows, long o3CommitCount, long rollbackCount, long physicallyWrittenRows) {
            this.commitCount = commitCount;
            this.committedRows = committedRows;
            this.o3CommitCount = o3CommitCount;
            this.rollbackCount = rollbackCount;
            this.physicallyWrittenRows = physicallyWrittenRows;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricsSnapshot that = (MetricsSnapshot) o;
            return commitCount == that.commitCount && committedRows == that.committedRows && o3CommitCount == that.o3CommitCount && rollbackCount == that.rollbackCount && physicallyWrittenRows == that.physicallyWrittenRows;
        }

        @Override
        public int hashCode() {
            return Objects.hash(commitCount, committedRows, o3CommitCount, rollbackCount, physicallyWrittenRows);
        }

        @Override
        public String toString() {
            return "MetricsSnapshot{" +
                    "commitCount=" + commitCount +
                    ", committedRows=" + committedRows +
                    ", o3CommitCount=" + o3CommitCount +
                    ", rollbackCount=" + rollbackCount +
                    ", physicallyWrittenRows=" + physicallyWrittenRows +
                    '}';
        }
    }

    private static MetricsSnapshot snapshotMetrics() {
        TableWriterMetrics writerMetrics = engine.getMetrics().tableWriter();
        return new MetricsSnapshot(writerMetrics.getCommitCount(),
                writerMetrics.getCommittedRows(),
                writerMetrics.getO3CommitCount(),
                writerMetrics.getRollbackCount(),
                writerMetrics.getPhysicallyWrittenRows()
        );
    }
}
