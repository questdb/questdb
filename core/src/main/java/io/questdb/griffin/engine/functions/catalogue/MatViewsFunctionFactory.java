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
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshState;
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

public class MatViewsFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "mat_views()";
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
        private static final int COLUMN_REFRESH_TYPE = COLUMN_VIEW_NAME + 1;
        private static final int COLUMN_BASE_TABLE_NAME = COLUMN_REFRESH_TYPE + 1;
        private static final int COLUMN_LAST_REFRESH_TIMESTAMP = COLUMN_BASE_TABLE_NAME + 1;
        private static final int COLUMN_VIEW_SQL = COLUMN_LAST_REFRESH_TIMESTAMP + 1;
        private static final int COLUMN_TABLE_DIR_NAME = COLUMN_VIEW_SQL + 1;
        private static final int COLUMN_INVALIDATION_REASON = COLUMN_TABLE_DIR_NAME + 1;
        private static final int COLUMN_VIEW_STATUS = COLUMN_INVALIDATION_REASON + 1;
        private static final int COLUMN_LAST_REFRESH_BASE_TABLE_TXN = COLUMN_VIEW_STATUS + 1;
        private static final int COLUMN_LAST_APPLIED_BASE_TABLE_TXN = COLUMN_LAST_REFRESH_BASE_TABLE_TXN + 1;
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
            sink.val("mat_views()");
        }

        private static class ViewsListCursor implements NoRandomAccessRecordCursor {
            private final ViewsListRecord record = new ViewsListRecord();
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
            public Record getRecordB() {
                return record;
            }

            @Override
            public boolean hasNext() throws DataUnavailableException {
                int n = viewTokens.size();
                while (viewIndex < n) {
                    final TableToken viewToken = viewTokens.get(viewIndex);
                    final MatViewRefreshState viewState = engine.getMatViewGraph().getViewRefreshState(viewToken);
                    if (viewState != null && !viewState.isDropped()) {
                        TableToken baseTableToken = engine.getTableTokenIfExists(viewState.getViewDefinition().getBaseTableName());
                        final long lastRefreshedBaseTxn = viewState.getLastRefreshBaseTxn();
                        final long lastRefreshTimestamp = viewState.getLastRefreshTimestamp();
                        // Read base table txn after mat view's last refreshed txn to avoid
                        // showing obsolete base table txn.
                        final long lastAppliedBaseTxn = baseTableToken != null
                                ? engine.getTableSequencerAPI().getTxnTracker(baseTableToken).getWriterTxn() : -1;

                        record.of(
                                viewState.getViewDefinition(),
                                lastRefreshTimestamp,
                                lastRefreshedBaseTxn,
                                lastAppliedBaseTxn,
                                viewState.getInvalidationReason(),
                                viewState.isInvalid()
                        );
                        viewIndex++;
                        return true;
                    }
                    viewIndex++;
                }
                return false;
            }

            @Override
            public long size() throws DataUnavailableException {
                return -1;
            }

            @Override
            public void toTop() {
                viewTokens.clear();
                engine.getMatViewGraph().getViews(viewTokens);
                viewIndex = 0;
            }

            public void toTop(CairoEngine engine) {
                this.engine = engine;
                toTop();
            }

            private static class ViewsListRecord implements Record {
                private boolean invalid;
                private String invalidationReason;
                private long lastAppliedBaseTxn;
                private long lastRefreshTimestamp;
                private long lastRefreshTxn;
                private MatViewDefinition viewDefinition;

                @Override
                public long getLong(int col) {
                    switch (col) {
                        case COLUMN_LAST_REFRESH_TIMESTAMP:
                            return lastRefreshTimestamp;
                        case COLUMN_LAST_REFRESH_BASE_TABLE_TXN:
                            return lastRefreshTxn;
                        case COLUMN_LAST_APPLIED_BASE_TABLE_TXN:
                            return lastAppliedBaseTxn;
                        default:
                            return 0;
                    }
                }

                @Override
                public CharSequence getStrA(int col) {
                    switch (col) {
                        case COLUMN_VIEW_NAME:
                            return viewDefinition.getMatViewToken().getTableName();
                        case COLUMN_REFRESH_TYPE:
                            // For now, incremental refresh is the only supported strategy.
                            return "incremental";
                        case COLUMN_BASE_TABLE_NAME:
                            return viewDefinition.getBaseTableName();
                        case COLUMN_VIEW_SQL:
                            return viewDefinition.getMatViewSql();
                        case COLUMN_TABLE_DIR_NAME:
                            return viewDefinition.getMatViewToken().getDirName();
                        case COLUMN_VIEW_STATUS:
                            return invalid ? "invalid" : "valid";
                        case COLUMN_INVALIDATION_REASON:
                            return invalidationReason != null && !invalidationReason.isEmpty() ? invalidationReason : null;
                        default:
                            return null;
                    }
                }

                @Override
                public CharSequence getStrB(int col) {
                    return getStrA(col);
                }

                public void of(
                        MatViewDefinition viewDefinition,
                        long lastRefreshTimestamp,
                        long lastRefreshTxn,
                        long lastAppliedBaseTxn,
                        String lastError,
                        boolean invalid
                ) {
                    this.viewDefinition = viewDefinition;
                    this.lastRefreshTimestamp = lastRefreshTimestamp;
                    this.lastRefreshTxn = lastRefreshTxn;
                    this.lastAppliedBaseTxn = lastAppliedBaseTxn;
                    this.invalidationReason = lastError;
                    this.invalid = invalid;
                }
            }
        }

        static {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("view_name", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("refresh_type", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("base_table_name", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("last_refresh_timestamp", ColumnType.TIMESTAMP));
            metadata.add(new TableColumnMetadata("view_sql", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("view_table_dir_name", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("invalidation_reason", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("view_status", ColumnType.STRING));
            metadata.add(new TableColumnMetadata("base_table_txn", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("applied_base_table_txn", ColumnType.LONG));
            METADATA = metadata;
        }
    }
}
