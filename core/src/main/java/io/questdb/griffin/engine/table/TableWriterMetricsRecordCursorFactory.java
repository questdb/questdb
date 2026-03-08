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

package io.questdb.griffin.engine.table;

import io.questdb.Metrics;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableWriterMetrics;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;

public final class TableWriterMetricsRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final int COMMITTED_ROWS_COLUMN_INDEX = 3;
    private static final RecordMetadata METADATA;
    private static final long METRICS_DISABLED_VALUE = -1;
    private static final int O3_COMMITS_COLUMN_INDEX = 1;
    private static final int PHYSICALLY_WRITTEN_ROWS_COLUMN_INDEX = 4;
    private static final int ROLLBACKS_COLUMN_INDEX = 2;
    private static final int TOTAL_COMMITS_COLUMN_INDEX = 0;
    private static final int TOTAL_NUMBER_OF_METRIC = PHYSICALLY_WRITTEN_ROWS_COLUMN_INDEX + 1;
    private static final String[] KEYS = new String[TOTAL_NUMBER_OF_METRIC];
    private final StringLongTuplesRecordCursor cursor = new StringLongTuplesRecordCursor();
    private final long[] values = new long[TOTAL_NUMBER_OF_METRIC];

    public TableWriterMetricsRecordCursorFactory() {
        super(METADATA);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        Metrics metrics = executionContext.getCairoEngine().getMetrics();
        if (metrics.isEnabled()) {
            TableWriterMetrics tableWriterMetrics = metrics.tableWriterMetrics();
            values[TOTAL_COMMITS_COLUMN_INDEX] = tableWriterMetrics.getCommitCount();
            values[O3_COMMITS_COLUMN_INDEX] = tableWriterMetrics.getO3CommitCount();
            values[ROLLBACKS_COLUMN_INDEX] = tableWriterMetrics.getRollbackCount();
            values[COMMITTED_ROWS_COLUMN_INDEX] = tableWriterMetrics.getCommittedRows();
            values[PHYSICALLY_WRITTEN_ROWS_COLUMN_INDEX] = tableWriterMetrics.getPhysicallyWrittenRows();
        } else {
            values[TOTAL_COMMITS_COLUMN_INDEX] = METRICS_DISABLED_VALUE;
            values[O3_COMMITS_COLUMN_INDEX] = METRICS_DISABLED_VALUE;
            values[ROLLBACKS_COLUMN_INDEX] = METRICS_DISABLED_VALUE;
            values[COMMITTED_ROWS_COLUMN_INDEX] = METRICS_DISABLED_VALUE;
            values[PHYSICALLY_WRITTEN_ROWS_COLUMN_INDEX] = METRICS_DISABLED_VALUE;
        }
        cursor.of(KEYS, values);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("table_writer_metrics");
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(0, new TableColumnMetadata("name", ColumnType.STRING));
        metadata.add(1, new TableColumnMetadata("value", ColumnType.LONG));
        METADATA = metadata;

        KEYS[TOTAL_COMMITS_COLUMN_INDEX] = "total_commits";
        KEYS[O3_COMMITS_COLUMN_INDEX] = "o3commits";
        KEYS[ROLLBACKS_COLUMN_INDEX] = "rollbacks";
        KEYS[COMMITTED_ROWS_COLUMN_INDEX] = "committed_rows";
        KEYS[PHYSICALLY_WRITTEN_ROWS_COLUMN_INDEX] = "physically_written_rows";
    }
}
