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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class PgDatabaseFunctionFactory implements FunctionFactory {

    private final static RecordMetadata METADATA;
    private final static String SIGNATURE = "pg_database()";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public boolean isCursor() {
        return true;
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
        return new CursorFunction(new PgDatabaseRecordCursorFactory());
    }

    private static class PgDatabaseRecord implements Record {

        @Override
        public boolean getBool(int col) {
            // datistemplate
            // If true, then this database can be cloned by any user with CREATEDB privileges; if false, then only superusers or the owner of the database can clone it.
            // datallowconn
            // If false then no one can connect to this database. This is used to protect the template0 database from being altered.
            return col != 6;
        }

        @Override
        public int getInt(int col) {
            switch (col) {
                case 0:
                    // oid
                    return 1;
                case 2:
                    // datdba
                    return 2;
                case 3:
                    // encoding
                    return 0;
                case 8:
                    // datconnlimit
                    return -1;
                case 9:
                    // datlastsysoid
                    return 1; // same as oid
                default:
                    // dattablespace
                    // pg_tablespace.oid
                    return 3;
            }
        }

        @Override
        public long getLong(int col) {
            if (col == 10) {
                // datfrozenxid
                return -1;
            } else {
                // datminmxid
                return 0;
            }
        }

        @Override
        public long getRowId() {
            return 0;
        }

        @Override
        public CharSequence getStr(int col) {
            switch (col) {
                case 1:
                    // datname
                    return Constants.DB_NAME;
                case 13:
                    // datacl
                    return "";
                default:
                    // datcollate
                    // datctype
                    return "en_US.UTF-8";
            }
        }

        @Override
        public CharSequence getStrB(int col) {
            return getStr(col);
        }

        @Override
        public int getStrLen(int col) {
            return getStr(col).length();
        }
    }

    private static class PgDatabaseRecordCursor implements RecordCursor {

        private static final PgDatabaseRecord RECORD = new PgDatabaseRecord();
        private boolean hasNext = true;

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return RECORD;
        }

        @Override
        public Record getRecordB() {
            return RECORD;
        }

        @Override
        public boolean hasNext() {
            if (hasNext) {
                hasNext = false;
                return true;
            }
            return false;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            hasNext = true;
        }
    }

    private static class PgDatabaseRecordCursorFactory extends AbstractRecordCursorFactory {
        private final PgDatabaseRecordCursor cursor = new PgDatabaseRecordCursor();

        public PgDatabaseRecordCursorFactory() {
            super(METADATA);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type(SIGNATURE);
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("oid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("datname", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("datdba", ColumnType.INT));
        metadata.add(new TableColumnMetadata("encoding", ColumnType.INT));
        metadata.add(new TableColumnMetadata("datcollate", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("datctype", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("datistemplate", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("datallowconn", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("datconnlimit", ColumnType.INT));
        metadata.add(new TableColumnMetadata("datlastsysoid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("datfrozenxid", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("datminmxid", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("dattablespace", ColumnType.INT));
        metadata.add(new TableColumnMetadata("datacl", ColumnType.STRING));
        METADATA = metadata;
    }
}
