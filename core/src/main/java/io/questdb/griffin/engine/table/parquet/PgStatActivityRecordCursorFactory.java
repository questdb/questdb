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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.metrics.DatabaseActivityRegistry;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Os;
import io.questdb.std.str.CharSink;

public final class PgStatActivityRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final RecordMetadata METADATA;
    private final PgStatActivityRecordCursor cursor = new PgStatActivityRecordCursor();

    public PgStatActivityRecordCursorFactory() {
        super(METADATA);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        DatabaseActivityRegistry registry = DatabaseActivityRegistry.getInstance();
        DatabaseActivityRegistry.ConnectionInfo[] connections = registry.getActiveConnections();
        cursor.of(connections);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("pg_stat_activity");
    }

    private static class PgStatActivityRecordCursor implements RecordCursor {
        private final PgStatActivityRecord record = new PgStatActivityRecord();
        private DatabaseActivityRegistry.ConnectionInfo[] connections;
        private int currentIndex = 0;

        public void of(DatabaseActivityRegistry.ConnectionInfo[] connections) {
            this.connections = connections;
            this.currentIndex = 0;
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
            if (currentIndex < connections.length) {
                record.of(connections[currentIndex]);
                currentIndex++;
                return true;
            }
            return false;
        }

        @Override
        public Record getRecordB() {
            return record;
        }

        @Override
        public void recordAt(long rowId) {
            if (rowId >= 0 && rowId < connections.length) {
                record.of(connections[(int) rowId]);
            }
        }

        @Override
        public long size() {
            return connections.length;
        }

        @Override
        public void toTop() {
            currentIndex = 0;
        }
    }

    private static class PgStatActivityRecord implements Record {
        private DatabaseActivityRegistry.ConnectionInfo connectionInfo;

        public void of(DatabaseActivityRegistry.ConnectionInfo connectionInfo) {
            this.connectionInfo = connectionInfo;
        }

        @Override
        public BinarySequence getBin(int col) {
            return null;
        }

        @Override
        public long getBinLen(int col) {
            return -1;
        }

        @Override
        public boolean getBool(int col) {
            return false;
        }

        @Override
        public byte getByte(int col) {
            return 0;
        }

        @Override
        public char getChar(int col) {
            return 0;
        }

        @Override
        public long getDate(int col) {
            return switch (col) {
                case 5 -> connectionInfo.connectionStartTime / 1000; // backend_start (convert micros to millis)
                case 6 -> connectionInfo.queryStartTime > 0 ? connectionInfo.queryStartTime / 1000 : -1; // query_start
                default -> -1;
            };
        }

        @Override
        public double getDouble(int col) {
            return Double.NaN;
        }

        @Override
        public float getFloat(int col) {
            return Float.NaN;
        }

        @Override
        public int getInt(int col) {
            return switch (col) {
                case 0 -> connectionInfo.connectionId; // pid
                default -> Integer.MIN_VALUE;
            };
        }

        @Override
        public long getLong(int col) {
            return switch (col) {
                case 0 -> connectionInfo.connectionId; // pid
                case 5 -> connectionInfo.connectionStartTime / 1000; // backend_start
                case 6 -> connectionInfo.queryStartTime > 0 ? connectionInfo.queryStartTime / 1000 : -1; // query_start
                default -> Long.MIN_VALUE;
            };
        }

        @Override
        public long getLong128Hi(int col) {
            return Long.MIN_VALUE;
        }

        @Override
        public long getLong128Lo(int col) {
            return Long.MIN_VALUE;
        }

        @Override
        public void getLong256(int col, CharSink sink) {
        }

        @Override
        public Long256 getLong256A(int col) {
            return null;
        }

        @Override
        public Long256 getLong256B(int col) {
            return null;
        }

        @Override
        public short getShort(int col) {
            return 0;
        }

        @Override
        public CharSequence getStr(int col) {
            return switch (col) {
                case 1 -> connectionInfo.database; // datname
                case 2 -> connectionInfo.username; // usename
                case 3 -> connectionInfo.remoteAddress; // client_addr
                case 4 -> connectionInfo.state; // state
                case 7 -> connectionInfo.currentQuery != null ? connectionInfo.currentQuery : ""; // query
                default -> null;
            };
        }

        @Override
        public int getStrLen(int col) {
            CharSequence str = getStr(col);
            return str != null ? str.length() : -1;
        }

        @Override
        public CharSequence getStrB(int col) {
            return getStr(col);
        }

        @Override
        public int getStrLenB(int col) {
            return getStrLen(col);
        }

        @Override
        public long getTimestamp(int col) {
            return switch (col) {
                case 5 -> connectionInfo.connectionStartTime; // backend_start
                case 6 -> connectionInfo.queryStartTime > 0 ? connectionInfo.queryStartTime : -1; // query_start
                default -> Long.MIN_VALUE;
            };
        }

        @Override
        public CharSequence getVarchar(int col) {
            return getStr(col);
        }

        @Override
        public int getVarcharLen(int col) {
            return getStrLen(col);
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(0, new TableColumnMetadata("pid", ColumnType.LONG));
        metadata.add(1, new TableColumnMetadata("datname", ColumnType.STRING));
        metadata.add(2, new TableColumnMetadata("usename", ColumnType.STRING));
        metadata.add(3, new TableColumnMetadata("client_addr", ColumnType.STRING));
        metadata.add(4, new TableColumnMetadata("state", ColumnType.STRING));
        metadata.add(5, new TableColumnMetadata("backend_start", ColumnType.TIMESTAMP));
        metadata.add(6, new TableColumnMetadata("query_start", ColumnType.TIMESTAMP));
        metadata.add(7, new TableColumnMetadata("query", ColumnType.STRING));
        METADATA = metadata;
    }
}