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


public final class PgStatDatabaseRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final RecordMetadata METADATA;
    private final PgStatDatabaseRecordCursor cursor = new PgStatDatabaseRecordCursor();


    public PgStatDatabaseRecordCursorFactory() {
        super(METADATA);
    }


    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        cursor.of();
        return cursor;
    }


    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }


    @Override
    public void toPlan(PlanSink sink) {
        sink.type("pg_stat_database");
    }


    private static class PgStatDatabaseRecordCursor implements RecordCursor {
        private final PgStatDatabaseRecord record = new PgStatDatabaseRecord();
        private boolean hasData = false;


        public void of() {
            this.hasData = true;
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
            if (hasData) {
                hasData = false;
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
            // Single row result
        }


        @Override
        public long size() {
            return 1;
        }


        @Override
        public void toTop() {
            hasData = true;
        }
    }


    private static class PgStatDatabaseRecord implements Record {
        private final DatabaseActivityRegistry registry = DatabaseActivityRegistry.getInstance();


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
            return -1;
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
            return Integer.MIN_VALUE;
        }


        @Override
        public long getLong(int col) {
            return switch (col) {
                case 1 -> registry.getConnectionCount(); // numbackends
                case 2 -> registry.getTotalCommits(); // xact_commit
                case 3 -> registry.getTotalRollbacks(); // xact_rollback
                case 4 -> registry.getTotalQueries(); // tup_fetched (using total queries as proxy)
                case 5 -> registry.getTotalQueries(); // tup_returned (using total queries as proxy)
                case 6 -> (Os.currentTimeMicros() - registry.getServerStartTime()) / 1_000_000; // uptime_seconds
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
                case 0 -> "questdb"; // datname
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
            return Long.MIN_VALUE;
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
        metadata.add(0, new TableColumnMetadata("datname", ColumnType.STRING));
        metadata.add(1, new TableColumnMetadata("numbackends", ColumnType.LONG));
        metadata.add(2, new TableColumnMetadata("xact_commit", ColumnType.LONG));
        metadata.add(3, new TableColumnMetadata("xact_rollback", ColumnType.LONG));
        metadata.add(4, new TableColumnMetadata("tup_fetched", ColumnType.LONG));
        metadata.add(5, new TableColumnMetadata("tup_returned", ColumnType.LONG));
        metadata.add(6, new TableColumnMetadata("uptime_seconds", ColumnType.LONG));
        METADATA = metadata;
    }
}