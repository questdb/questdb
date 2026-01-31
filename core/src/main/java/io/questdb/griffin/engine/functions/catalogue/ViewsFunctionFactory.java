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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.view.ViewState;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class ViewsFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "views()";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new CursorFunction(new ViewsCursorFactory()) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    private static class ViewsCursorFactory implements RecordCursorFactory {
        private static final int COLUMN_VIEW_NAME = 0;
        private static final int COLUMN_VIEW_SQL = COLUMN_VIEW_NAME + 1;
        private static final int COLUMN_TABLE_DIR_NAME = COLUMN_VIEW_SQL + 1;
        private static final int COLUMN_INVALIDATION_REASON = COLUMN_TABLE_DIR_NAME + 1;
        private static final int COLUMN_VIEW_STATUS = COLUMN_INVALIDATION_REASON + 1;
        private static final int COLUMN_VIEW_STATUS_UPDATE_TIME = COLUMN_VIEW_STATUS + 1;
        private static final RecordMetadata METADATA;
        private final ViewsListCursor cursor = new ViewsListCursor();

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
            sink.val("views()");
        }

        private static class ViewsListCursor implements NoRandomAccessRecordCursor {
            private final ViewsRecord record = new ViewsRecord();
            private final ObjList<TableToken> viewTokens = new ObjList<>();
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
                final int n = viewTokens.size();
                for (; viewIndex < n; viewIndex++) {
                    final TableToken viewToken = viewTokens.get(viewIndex);
                    if (viewToken.isSystem()) {
                        continue;
                    }
                    if (engine.getTableTokenIfExists(viewToken.getTableName()) != null) {
                        final ViewDefinition viewDefinition = engine.getViewGraph().getViewDefinition(viewToken);
                        if (viewDefinition == null) {
                            continue; // view was dropped concurrently
                        }

                        final ViewState viewState = engine.getViewStateStore().getViewState(viewToken);
                        if (viewState == null) {
                            continue; // view was dropped concurrently
                        }

                        record.of(viewDefinition, viewState);
                        viewIndex++;
                        return true;
                    }
                }
                return false;
            }

            @Override
            public long preComputedStateSize() {
                return viewTokens.size();
            }

            @Override
            public long size() {
                return -1;
            }

            @Override
            public void toTop() {
                viewTokens.clear();
                engine.getViewGraph().getViews(viewTokens);
                viewIndex = 0;
            }

            private void toTop(CairoEngine engine) {
                this.engine = engine;
                toTop();
            }

            private static class ViewsRecord implements Record {
                private final StringSink invalidationReasonSink = new StringSink();
                private boolean invalid;
                private long lastUpdatedTimestamp;
                private ViewDefinition viewDefinition;

                @Override
                public long getLong(int col) {
                    return col == COLUMN_VIEW_STATUS_UPDATE_TIME ? lastUpdatedTimestamp : 0;
                }

                @Override
                public CharSequence getStrA(int col) {
                    return switch (col) {
                        case COLUMN_VIEW_NAME -> viewDefinition.getViewToken().getTableName();
                        case COLUMN_VIEW_SQL -> viewDefinition.getViewSql();
                        case COLUMN_TABLE_DIR_NAME -> viewDefinition.getViewToken().getDirName();
                        case COLUMN_VIEW_STATUS -> invalid ? "invalid" : "valid";
                        case COLUMN_INVALIDATION_REASON ->
                                invalidationReasonSink.isEmpty() ? null : invalidationReasonSink;
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

                public void of(ViewDefinition viewDefinition, ViewState viewState) {
                    this.viewDefinition = viewDefinition;

                    try {
                        viewState.lockForRead();
                        lastUpdatedTimestamp = viewState.getUpdateTimestamp();
                        invalid = viewState.isInvalid();
                        viewState.getInvalidationReason(invalidationReasonSink);
                    } finally {
                        viewState.unlockAfterRead();
                    }
                }
            }
        }

        static {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("view_name", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("view_sql", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("view_table_dir_name", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("invalidation_reason", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("view_status", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("view_status_update_time", ColumnType.TIMESTAMP));
            METADATA = metadata;
        }
    }
}
