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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.duckdb.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.GcUtf8String;

public class DuckDBQuackFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "quack(S)";
    }

    @Override
    public boolean isCursor() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        assert args.size() == 1;
        assert args.getQuick(0).isConstant();
        CharSequence str = args.getQuick(0).getStr(null);
        DuckDBInstance db = sqlExecutionContext.getCairoEngine().getDuckDBInstance();
        DuckDBConnection connection = db.getConnection();
        DirectUtf8Sequence n = new GcUtf8String("CREATE TABLE integers(i INTEGER)");
        DuckDBResult result = new DuckDBResult();
        connection.query(n, result);
        DirectUtf8Sequence i = new GcUtf8String("LOAD parquet;");
        connection.query(i, result);

        DirectUtf8Sequence q = new GcUtf8String(str.toString());
        long stmt = connection.prepare(q);
        DuckDBPreparedStatement ps = new DuckDBPreparedStatement(stmt);
        DuckDBRecordCursorFactory factory = new DuckDBRecordCursorFactory(ps, connection);
        return new CursorFunction(factory) {
            @Override
            public boolean isRuntimeConstant() {
                return false;
            }
        };
    }

    public static class QuackRecordCursorFactory implements RecordCursorFactory {
        public static final String COLUMN_NAME = "say-it";
        private final QuackRecordCursor cursor;

        public QuackRecordCursorFactory(CharSequence str) {
//            DuckDBResult result = new DuckDBResult();
//            try(DuckDBInstance duckDBInstance = new DuckDBInstance()) {
//                try(DuckDBConnection connection = duckDBInstance.getConnection()) {
//                    DirectUtf8Sequence n = new GcUtf8String("CREATE TABLE integers(i INTEGER)");
//                    connection.execute(n);
//                    DirectUtf8Sequence i = new GcUtf8String("INSERT INTO integers VALUES (1), (2), (3), (4), (5)");
//                    connection.execute(i);
//                    DirectUtf8Sequence q = new GcUtf8String(str.toString());
//                    connection.query(q, result);
//                }
//            }
//            final GenericRecordMetadata metadata = new GenericRecordMetadata();
//            if (!result.isClosed()) {
//                long columnCount = result.getColumnCount();
//                DirectUtf8StringZ name = new DirectUtf8StringZ();
//                for (int i = 0; i < columnCount; i++) {
//                    boolean ok = result.getColumnName(i, name);
//                    if (ok) {
//                        metadata.add(new TableColumnMetadata(name, ColumnType.INT))
//                    }
//                }
//            }
            this.cursor = new QuackRecordCursor();
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            return cursor.of();
        }

        @Override
        public RecordMetadata getMetadata() {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata(COLUMN_NAME, ColumnType.STRING));
            return metadata;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("quack");
        }

        private static class QuackRecordCursor implements RecordCursor {
            private final QuackRecord record = new QuackRecord();
            private int rowCounter = 0;
            private String quack = "quack";

            @Override
            public void close() {
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public Record getRecordB() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext() {
                if (rowCounter < 100) {
                    rowCounter++;
                    return true;
                }
                return false;
            }

            @Override
            public void recordAt(Record record, long atRowId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long size() {
                return 100;
            }

            @Override
            public void toTop() {
                rowCounter = 0;
            }

            private QuackRecordCursor of() {
                toTop();
                return this;
            }

            public class QuackRecord implements Record {
                @Override
                public CharSequence getStr(int col) {
                    if (col == 0) {
                        return quack;
                    }
                    return null;
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
        }

    }
}
