/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.functions.activity;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;

public class QueryActivityFunctionFactory implements FunctionFactory {

    private static final RecordMetadata METADATA;

    private static final String NAME = "query_activity";
    private static final String SIGNATURE = NAME + "()";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return new CursorFunction(new QueryActivityCursorFactory(METADATA, sqlExecutionContext));
    }

    private static class QueryActivityCursor implements NoRandomAccessRecordCursor {

        private final LongList entryIds = new LongList();
        private final QueryRegistry queryRegistry;
        private final QueryActivityRecord record = new QueryActivityRecord();
        private QueryRegistry.Entry entry;
        private int entryIndex;

        private boolean isAdmin;

        private CharSequence principal;

        private QueryActivityCursor(SqlExecutionContext executionContext) {
            queryRegistry = executionContext.getCairoEngine().getQueryRegistry();
        }

        @Override
        public void close() {
            entryIds.clear();
            isAdmin = false;
            principal = null;
            toTop();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() throws DataUnavailableException {
            while (++entryIndex < entryIds.size()) {
                entry = queryRegistry.getEntry(entryIds.get(entryIndex));
                if (entry != null) {
                    if (isAdmin || entry.getPrincipal().equals(principal)) {
                        return true;
                    }
                }
            }

            return false;
        }

        public void of(SqlExecutionContext executionContext) {
            try {
                executionContext.getSecurityContext().authorizeAdminAction();
                isAdmin = true;
            } catch (CairoException e) {
                isAdmin = false;
                principal = executionContext.getSecurityContext().getPrincipal();
            }

            queryRegistry.getEntryIds(entryIds);
            toTop();
        }

        @Override
        public long size() throws DataUnavailableException {
            return -1;
        }

        @Override
        public void toTop() {
            entryIndex = -1;
            entry = null;
        }

        private class QueryActivityRecord implements Record {

            @Override
            public long getLong(int col) {
                if (col == 0) {
                    return entryIds.getQuick(entryIndex);
                } else if (col == 1) {
                    return entry.getWorkerId();
                }

                return Record.super.getLong(col);
            }

            @Override
            public CharSequence getStr(int col) {
                if (col == 2) {
                    return entry.getPoolName();
                } else if (col == 3) {
                    return entry.getPrincipal();
                } else if (col == 6) {
                    return entry.getStateText();
                } else if (col == 7) {
                    return entry.getQuery();
                }

                return Record.super.getStr(col);
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStr(col);
            }

            @Override
            public int getStrLen(int col) {
                CharSequence str = getStr(col);
                return str != null ? str.length() : -1;
            }

            @Override
            public long getTimestamp(int col) {
                if (col == 4) {
                    return entry.getRegisteredAtNs();
                } else if (col == 5) {
                    return entry.getChangedAtNs();
                }

                return Record.super.getTimestamp(col);
            }
        }
    }

    private static class QueryActivityCursorFactory extends AbstractRecordCursorFactory {

        private final QueryActivityCursor cursor;

        public QueryActivityCursorFactory(RecordMetadata metadata, SqlExecutionContext sqlExecutionContext) {
            super(metadata);
            cursor = new QueryActivityCursor(sqlExecutionContext);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.of(executionContext);
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
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("query_id", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("worker_id", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("worker_pool", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("username", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("query_start", ColumnType.TIMESTAMP));
        metadata.add(new TableColumnMetadata("state_change", ColumnType.TIMESTAMP));
        metadata.add(new TableColumnMetadata("state", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("query", ColumnType.STRING));
        METADATA = metadata;
    }
}
