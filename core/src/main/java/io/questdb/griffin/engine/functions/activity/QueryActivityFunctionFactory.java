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

package io.questdb.griffin.engine.functions.activity;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

import java.util.Objects;

@SuppressWarnings("unused")
public class QueryActivityFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;
    private static final String SIGNATURE = "query_activity()";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new CursorFunction(new QueryActivityCursorFactory(METADATA, sqlExecutionContext));
    }

    private static class QueryActivityCursor implements NoRandomAccessRecordCursor {

        private final LongList entryIds = new LongList();
        private final QueryRegistry queryRegistry;
        private final QueryActivityRecord record = new QueryActivityRecord();
        private int entryIndex;

        private boolean isAdmin;

        private CharSequence principal;

        private QueryActivityCursor(SqlExecutionContext executionContext) {
            queryRegistry = executionContext.getCairoEngine().getQueryRegistry();
        }

        @Override
        public void close() {
            entryIds.clear();
            record.clear();
            isAdmin = false;
            principal = null;
            toTop();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            while (++entryIndex < entryIds.size()) {
                final long queryId = entryIds.get(entryIndex);
                final QueryRegistry.Entry entry = queryRegistry.getEntry(queryId);
                if (entry != null) {
                    if (record.of(queryId, entry) && (isAdmin || Objects.equals(record.getPrincipal(), principal))) {
                        return true;
                    }
                }
            }
            return false;
        }

        public void of(SqlExecutionContext executionContext) {
            try {
                executionContext.getSecurityContext().authorizeSqlEngineAdmin();
                isAdmin = true;
            } catch (CairoException e) {
                isAdmin = false;
                principal = executionContext.getSecurityContext().getPrincipal();
            }

            queryRegistry.getEntryIds(entryIds);
            toTop();
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            entryIndex = -1;
        }

        private static class QueryActivityRecord implements Record {
            private final StringSink poolName = new StringSink();
            private final StringSink principal = new StringSink();
            private final StringSink query = new StringSink();
            private long changedAtNs;
            private boolean isWAL;
            private boolean poolNameIsNull;
            private boolean principalIsNull;
            private long queryId;
            private long registeredAtNs;
            private byte state;
            private long workerId;

            private QueryActivityRecord() {
                clear();
            }

            @Override
            public boolean getBool(int col) {
                if (col == 7) {
                    return isWAL;
                }

                return false;
            }

            @Override
            public long getLong(int col) {
                if (col == 0) {
                    return queryId;
                } else if (col == 1) {
                    return workerId;
                }

                return Record.super.getLong(col);
            }

            @Override
            public CharSequence getStrA(int col) {
                if (col == 2) {
                    return poolNameIsNull ? null : poolName;
                } else if (col == 3) {
                    return getPrincipal();
                } else if (col == 6) {
                    return QueryRegistry.Entry.State.getText(state);
                } else if (col == 8) {
                    return query;
                }

                return Record.super.getStrA(col);
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStrA(col);
            }

            @Override
            public int getStrLen(int col) {
                return TableUtils.lengthOf(getStrA(col));
            }

            @Override
            public long getTimestamp(int col) {
                if (col == 4) {
                    return registeredAtNs;
                } else if (col == 5) {
                    return changedAtNs;
                }

                return Record.super.getTimestamp(col);
            }

            private void clear() {
                poolName.clear();
                principal.clear();
                query.clear();
                poolNameIsNull = true;
                principalIsNull = true;
                queryId = -1;
                workerId = -1;
                registeredAtNs = 0;
                changedAtNs = 0;
                state = QueryRegistry.Entry.State.IDLE;
                isWAL = false;
            }

            private CharSequence getPrincipal() {
                return principalIsNull ? null : principal;
            }

            // Entry is a moving target: unregister() can retire and recycle it while
            // query_activity() reads. Copy a row optimistically, then accept it only
            // if the lifecycle still points to the same active query.
            private boolean of(long queryId, QueryRegistry.Entry entry) {
                final long lifecycle = entry.getLifecycle();
                if (!QueryRegistry.Entry.isActiveLifecycle(queryId, lifecycle)) {
                    return false;
                }

                this.queryId = queryId;
                this.workerId = entry.getWorkerId();
                this.registeredAtNs = entry.getRegisteredAtNs();
                this.changedAtNs = entry.getChangedAtNs();
                this.state = entry.getState();
                this.isWAL = entry.isWAL();

                final CharSequence entryPoolName = entry.getPoolName();
                if (!copy(entryPoolName, poolName)) {
                    return false;
                }
                poolNameIsNull = entryPoolName == null;

                final CharSequence entryPrincipal = entry.getPrincipal();
                if (!copy(entryPrincipal, principal)) {
                    return false;
                }
                principalIsNull = entryPrincipal == null;

                if (!copy(entry.getQuery(), query)) {
                    return false;
                }

                return entry.getLifecycle() == lifecycle && entry.getState() == state;
            }

            private static boolean copy(CharSequence source, StringSink target) {
                target.clear();
                if (source != null) {
                    final int len = source.length();
                    try {
                        // The source may shrink while we copy; reject the row instead of exposing a torn string.
                        target.put(source, 0, len);
                    } catch (IndexOutOfBoundsException e) {
                        target.clear();
                        return false;
                    }
                    return source.length() == len;
                }
                return true;
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
        metadata.add(new TableColumnMetadata("query_start", ColumnType.TIMESTAMP_MICRO));
        metadata.add(new TableColumnMetadata("state_change", ColumnType.TIMESTAMP_MICRO));
        metadata.add(new TableColumnMetadata("state", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("is_wal", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("query", ColumnType.STRING));
        METADATA = metadata;
    }
}
