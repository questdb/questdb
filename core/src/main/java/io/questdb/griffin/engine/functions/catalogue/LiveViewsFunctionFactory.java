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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.MergeBuffer;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class LiveViewsFunctionFactory implements FunctionFactory {

    static String getIntervalUnit(char unit) {
        return switch (unit) {
            case 's' -> "SECOND";
            case 'm' -> "MINUTE";
            case 'h' -> "HOUR";
            case 'd' -> "DAY";
            default -> null;
        };
    }

    @Override
    public String getSignature() {
        return "live_views()";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new CursorFunction(new LiveViewsCursorFactory()) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    private static class LiveViewsCursorFactory implements RecordCursorFactory {
        private static final int COLUMN_BASE_TABLE_NAME = 1;
        private static final int COLUMN_BUFFERED_ROW_COUNT = 9;
        private static final int COLUMN_INVALIDATION_REASON = 7;
        private static final int COLUMN_LAG = 2;
        private static final int COLUMN_LAG_UNIT = 3;
        private static final int COLUMN_LATE_ROW_COUNT = 10;
        private static final int COLUMN_RETENTION = 4;
        private static final int COLUMN_RETENTION_UNIT = 5;
        private static final int COLUMN_VIEW_NAME = 0;
        private static final int COLUMN_VIEW_SQL = 8;
        private static final int COLUMN_VIEW_STATUS = 6;
        private static final RecordMetadata METADATA;
        private final LiveViewsListCursor cursor = new LiveViewsListCursor();

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.toTop(executionContext.getCairoEngine());
            return cursor;
        }

        @Override
        public RecordMetadata getMetadata() {
            return METADATA;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("live_views()");
        }

        private static class LiveViewsListCursor implements NoRandomAccessRecordCursor {
            private final LiveViewsRecord record = new LiveViewsRecord();
            private final ObjList<LiveViewInstance> viewInstances = new ObjList<>();
            private CairoEngine engine;
            private int viewIndex = 0;

            @Override
            public void close() {
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                if (viewIndex < viewInstances.size()) {
                    record.of(viewInstances.getQuick(viewIndex++));
                    return true;
                }
                return false;
            }

            @Override
            public long preComputedStateSize() {
                return viewInstances.size();
            }

            @Override
            public long size() {
                return -1;
            }

            @Override
            public void toTop() {
                viewInstances.clear();
                engine.getLiveViewRegistry().getViews(viewInstances);
                viewIndex = 0;
            }

            private void toTop(CairoEngine engine) {
                this.engine = engine;
                toTop();
            }

            private static class LiveViewsRecord implements Record {
                private LiveViewDefinition definition;
                private LiveViewInstance instance;

                @Override
                public long getLong(int col) {
                    return switch (col) {
                        case COLUMN_LAG -> definition.getLagValue();
                        case COLUMN_RETENTION -> definition.getRetentionValue();
                        case COLUMN_BUFFERED_ROW_COUNT -> {
                            MergeBuffer mb = instance.getMergeBuffer();
                            yield mb != null ? mb.size() : 0;
                        }
                        case COLUMN_LATE_ROW_COUNT -> {
                            MergeBuffer mb = instance.getMergeBuffer();
                            yield mb != null ? mb.getLateRowCount() : 0;
                        }
                        default -> 0;
                    };
                }

                @Override
                public CharSequence getStrA(int col) {
                    return switch (col) {
                        case COLUMN_VIEW_NAME -> definition.getViewName();
                        case COLUMN_BASE_TABLE_NAME -> definition.getBaseTableName();
                        case COLUMN_LAG_UNIT -> getIntervalUnit(definition.getLagUnit());
                        case COLUMN_RETENTION_UNIT -> getIntervalUnit(definition.getRetentionUnit());
                        case COLUMN_VIEW_SQL -> definition.getViewSql();
                        case COLUMN_VIEW_STATUS -> instance.isInvalid() ? "invalid" : "valid";
                        case COLUMN_INVALIDATION_REASON -> instance.getInvalidationReason();
                        default -> null;
                    };
                }

                @Override
                public CharSequence getStrB(int col) {
                    return getStrA(col);
                }

                @Override
                public int getStrLen(int col) {
                    return TableUtils.lengthOf(getStrA(col));
                }

                public void of(LiveViewInstance instance) {
                    this.instance = instance;
                    this.definition = instance.getDefinition();
                }
            }
        }

        static {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("view_name", ColumnType.STRING));              // 0
            metadata.add(new TableColumnMetadata("base_table_name", ColumnType.STRING));        // 1
            metadata.add(new TableColumnMetadata("lag", ColumnType.LONG));                      // 2
            metadata.add(new TableColumnMetadata("lag_unit", ColumnType.STRING));                // 3
            metadata.add(new TableColumnMetadata("retention", ColumnType.LONG));                 // 4
            metadata.add(new TableColumnMetadata("retention_unit", ColumnType.STRING));          // 5
            metadata.add(new TableColumnMetadata("view_status", ColumnType.STRING));             // 6
            metadata.add(new TableColumnMetadata("invalidation_reason", ColumnType.STRING));     // 7
            metadata.add(new TableColumnMetadata("view_sql", ColumnType.STRING));                // 8
            metadata.add(new TableColumnMetadata("buffered_row_count", ColumnType.LONG));        // 9
            metadata.add(new TableColumnMetadata("late_row_count", ColumnType.LONG));            // 10
            METADATA = metadata;
        }
    }
}
