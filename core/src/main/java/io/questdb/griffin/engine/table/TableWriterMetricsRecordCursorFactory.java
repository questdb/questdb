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
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableWriterMetrics;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

public final class TableWriterMetricsRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final RecordMetadata METADATA;
    private static final int TOTAL_COMMITS_COLUMN_INDEX = 0;
    private static final int O3_COMMITS_COLUMN_INDEX = 1;
    private static final int ROLLBACKS_COLUMN_INDEX = 2;
    private static final int COMMITTED_ROWS_COLUMN_INDEX = 3;
    private static final int PHYSICALLY_WRITTEN_ROWS_COLUMN_INDEX = 4;
    private static final int TOTAL_NUMBER_OF_METRIC = PHYSICALLY_WRITTEN_ROWS_COLUMN_INDEX + 1;
    private static final long METRICS_DISABLED_VALUE = -1;
    private final SingleRowRecordCursor cursor = new SingleRowRecordCursor();
    private final Object[] collectedMetrics = new Object[TOTAL_NUMBER_OF_METRIC];

    public TableWriterMetricsRecordCursorFactory() {
        super(METADATA);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        Metrics metrics = executionContext.getCairoEngine().getMetrics();
        if (metrics.isEnabled()) {
            TableWriterMetrics tableWriterMetrics = metrics.tableWriter();
            collectedMetrics[TOTAL_COMMITS_COLUMN_INDEX] = tableWriterMetrics.getCommitCount();
            collectedMetrics[O3_COMMITS_COLUMN_INDEX] = tableWriterMetrics.getO3CommitCount();
            collectedMetrics[ROLLBACKS_COLUMN_INDEX] = tableWriterMetrics.getRollbackCount();
            collectedMetrics[COMMITTED_ROWS_COLUMN_INDEX] = tableWriterMetrics.getCommittedRows();
            collectedMetrics[PHYSICALLY_WRITTEN_ROWS_COLUMN_INDEX] = tableWriterMetrics.getPhysicallyWrittenRows();
        } else {
            collectedMetrics[TOTAL_COMMITS_COLUMN_INDEX] = METRICS_DISABLED_VALUE;
            collectedMetrics[O3_COMMITS_COLUMN_INDEX] = METRICS_DISABLED_VALUE;
            collectedMetrics[ROLLBACKS_COLUMN_INDEX] = METRICS_DISABLED_VALUE;
            collectedMetrics[COMMITTED_ROWS_COLUMN_INDEX] = METRICS_DISABLED_VALUE;
            collectedMetrics[PHYSICALLY_WRITTEN_ROWS_COLUMN_INDEX] = METRICS_DISABLED_VALUE;
        }
        cursor.of(collectedMetrics);
        return cursor;
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(TOTAL_COMMITS_COLUMN_INDEX, new TableColumnMetadata("total-commits", 1, ColumnType.LONG));
        metadata.add(O3_COMMITS_COLUMN_INDEX, new TableColumnMetadata("o3commits", 2, ColumnType.LONG));
        metadata.add(ROLLBACKS_COLUMN_INDEX, new TableColumnMetadata("rollbacks", 3, ColumnType.LONG));
        metadata.add(COMMITTED_ROWS_COLUMN_INDEX, new TableColumnMetadata("committed-rows", 4, ColumnType.LONG));
        metadata.add(PHYSICALLY_WRITTEN_ROWS_COLUMN_INDEX, new TableColumnMetadata("physically-written-rows", 5, ColumnType.LONG));
        METADATA = metadata;
    }
}
