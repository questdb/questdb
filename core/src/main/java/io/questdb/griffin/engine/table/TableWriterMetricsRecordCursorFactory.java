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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableWriterMetrics;
import io.questdb.cairo.sql.Record;
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
    private static final int METRICS_DISABLED = -1;
    private final TableWriterMetricsRecordCursor cursor;

    public TableWriterMetricsRecordCursorFactory() {
        super(METADATA);
        this.cursor = new TableWriterMetricsRecordCursor();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(executionContext.getCairoEngine().getMetrics());
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

    private static class TableWriterMetricsRecordCursor implements RecordCursor {
        private final TableWriterMetricsRecord record = new TableWriterMetricsRecord();
        private boolean recordEmitted;

        private long totalCommits;
        private long o3commits;
        private long rollbacks;
        private long committedRows;
        private long physicallyWrittenRows;

        public void of(Metrics metrics) {
            TableWriterMetrics tableWriterMetrics = metrics.tableWriter();
            recordEmitted = false;
            if (metrics.isEnabled()) {
                totalCommits = tableWriterMetrics.getCommitCount();
                o3commits = tableWriterMetrics.getO3CommitCount();
                rollbacks = tableWriterMetrics.getRollbackCount();
                committedRows = tableWriterMetrics.getCommittedRows();
                physicallyWrittenRows = tableWriterMetrics.getPhysicallyWrittenRows();
            } else {
                totalCommits = METRICS_DISABLED;
                o3commits = METRICS_DISABLED;
                rollbacks = METRICS_DISABLED;
                committedRows = METRICS_DISABLED;
                physicallyWrittenRows = METRICS_DISABLED;
            }
        }

        @Override
        public void close() {

        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            return recordEmitted ? false : (recordEmitted = true);
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException("RecordB not supported");
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException("random access not supported");
        }

        @Override
        public void toTop() {
            recordEmitted = false;
        }

        @Override
        public long size() {
            return 1;
        }

        private class TableWriterMetricsRecord implements Record {
            @Override
            public long getLong(int col) {
                switch (col) {
                    case TOTAL_COMMITS_COLUMN_INDEX:
                        return totalCommits;
                    case O3_COMMITS_COLUMN_INDEX:
                        return o3commits;
                    case ROLLBACKS_COLUMN_INDEX:
                        return rollbacks;
                    case COMMITTED_ROWS_COLUMN_INDEX:
                        return committedRows;
                    case PHYSICALLY_WRITTEN_ROWS_COLUMN_INDEX:
                        return physicallyWrittenRows;
                    default: throw CairoException.instance(0).put("unsupported column number. [column=").put(col).put("]");
                }
            }
        }
    }
}
