/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class CheckpointStatusFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;
    private static final String SIGNATURE = "checkpoint_status()";
    private static final int inProgressColumn;
    private static final int startedAtColumn;

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new CursorFunction(new CheckpointStatusCursorFactory()) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    private static class CheckpointStatusCursorFactory extends AbstractRecordCursorFactory {
        private final CheckpointStatusRecordCursor cursor = new CheckpointStatusRecordCursor();
        private CairoEngine engine;

        public CheckpointStatusCursorFactory() {
            super(METADATA);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            engine = executionContext.getCairoEngine();
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }

        private class CheckpointStatusRecordCursor implements NoRandomAccessRecordCursor {
            private final CheckpointStatusRecord record = new CheckpointStatusRecord();
            private boolean rowPending = true;

            @Override
            public void close() {
                rowPending = true;
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                if (rowPending) {
                    final long startedAt = engine.getCheckpointStatus().startedAtTimestamp();
                    record.of(startedAt > -1, startedAt);
                    rowPending = false;
                    return true;
                }
                return false;
            }

            @Override
            public long preComputedStateSize() {
                return 0;
            }

            @Override
            public long size() {
                return 1;
            }

            @Override
            public void toTop() {
                close();
            }

            public class CheckpointStatusRecord implements Record {
                private boolean inProgress;
                private long startedAt;

                @Override
                public boolean getBool(int col) {
                    if (col == inProgressColumn) {
                        return inProgress;
                    }
                    return false;
                }

                @Override
                public long getTimestamp(int col) {
                    if (col == startedAtColumn) {
                        return startedAt;
                    }
                    return Numbers.LONG_NULL;
                }

                private void of(boolean inProgress, long startedAt) {
                    this.inProgress = inProgress;
                    this.startedAt = startedAt;
                }
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("in_progress", ColumnType.BOOLEAN));
        inProgressColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("started_at", ColumnType.TIMESTAMP_MICRO));
        startedAtColumn = metadata.getColumnCount() - 1;
        METADATA = metadata;
    }
}
