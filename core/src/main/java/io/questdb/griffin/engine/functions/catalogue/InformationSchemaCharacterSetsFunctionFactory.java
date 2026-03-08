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

public class InformationSchemaCharacterSetsFunctionFactory implements FunctionFactory {
    public static final RecordMetadata METADATA;
    public static final String SIGNATURE = "information_schema.character_sets()";

    @Override
    public String getSignature() {
        return SIGNATURE;
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
        return new CursorFunction(new CharSetsCursorFactory());
    }

    private static class CharSetsCursorFactory extends AbstractRecordCursorFactory {
        private final CharSetsRecordCursor cursor = new CharSetsRecordCursor();

        private CharSetsCursorFactory() {
            super(METADATA);
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
            sink.type(SIGNATURE);
        }

        private static class CharSetsRecordCursor implements NoRandomAccessRecordCursor {
            private final CharSetsRecord record = new CharSetsRecord();
            private int rowIndex = -1;

            @Override
            public void close() {
                rowIndex = -1;
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                return rowIndex++ < 0;
            }

            @Override
            public long preComputedStateSize() {
                return 0;
            }

            @Override
            public long size() {
                return 1;
            }

            @Override
            public void toTop() {
                rowIndex = -1;
            }

            private static class CharSetsRecord implements Record {
                @Override
                public CharSequence getStrA(int col) {
                    switch (col) {
                        case 0:
                            // character_set_catalog
                            return null;
                        case 1:
                            // character_set_schema
                            return null;
                        case 2:
                            // character_set_name
                            return "UTF8";
                        case 3:
                            // character_repertoire
                            return "UCS";
                        case 4:
                            // form_of_use
                            return "UTF8";
                        case 5:
                            // default_collate_catalog
                            return Constants.PUBLIC_SCHEMA;
                        case 6:
                            // default_collate_schema
                            return Constants.PUBLIC_SCHEMA;
                        case 7:
                            // default_collate_name
                            return "en_US.utf8";
                    }
                    return null;
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
        metadata.add(new TableColumnMetadata("character_set_catalog", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("character_set_schema", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("character_set_name", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("character_repertoire", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("form_of_use", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("default_collate_catalog", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("default_collate_schema", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("default_collate_name ", ColumnType.STRING));
        METADATA = metadata;
    }
}
