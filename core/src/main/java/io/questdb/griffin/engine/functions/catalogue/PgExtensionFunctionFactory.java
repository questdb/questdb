/*******************************************************************************
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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

// pg_extension stub implementation returning a single row with questdb 'extension' metadata
// used in grafana meta queries
public class PgExtensionFunctionFactory implements FunctionFactory {
    private static final int COLUMN_EXTVERSION = 5;

    private static final RecordMetadata METADATA;

    @Override
    public String getSignature() {
        return "pg_extension()";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new CursorFunction(new PgExtensionCursorFactory(configuration));
    }

    private static class PgExtensionCursorFactory extends AbstractRecordCursorFactory {
        private final PgExtensionRecordCursor cursor;

        public PgExtensionCursorFactory(CairoConfiguration configuration) {
            super(METADATA);
            cursor = new PgExtensionRecordCursor(configuration.getBuildInformation().getSwVersion());
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("pg_extension()");
        }

        private static class PgExtensionRecordCursor implements NoRandomAccessRecordCursor {

            private static final String[][] EXTENSIONS = {{"1", "questdb", "1", "1", "false", null, null, null}};
            private final PgExtensionRecord record = new PgExtensionRecord();
            private final CharSequence version;
            private int index = -1;

            public PgExtensionRecordCursor(CharSequence version) {
                this.version = version;
            }

            @Override
            public void close() {
                // no-op
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                return ++index < EXTENSIONS.length;
            }

            @Override
            public long size() {
                return EXTENSIONS.length;
            }

            @Override
            public long preComputedStateSize() {
                return 0;
            }

            @Override
            public void toTop() {
                index = -1;
            }

            public class PgExtensionRecord implements Record {
                @Override
                public boolean getBool(int col) {
                    return false;
                }

                @Override
                public CharSequence getStrA(int col) {
                    if (col == COLUMN_EXTVERSION) {
                        return version;
                    } else {
                        return EXTENSIONS[0][col];
                    }
                }

                @Override
                public CharSequence getStrB(int col) {
                    return getStrA(col);
                }

                @Override
                public int getStrLen(int col) {
                    return TableUtils.lengthOf(getStrA(col));
                }
            }
        }
    }


    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("oid", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("extname", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("extowner", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("extnamespace", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("extrelocatable", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("extversion", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("extconfig", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("extcondition", ColumnType.STRING));
        METADATA = metadata;
    }

}
