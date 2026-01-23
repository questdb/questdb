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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class FunctionListFunctionFactory implements FunctionFactory {

    private static final RecordMetadata METADATA;
    private static final int NAME_COLUMN;
    private static final int RT_CONSTANT_COLUMN;
    private static final int SIGNATURE_COLUMN;
    private static final int SIGNATURE_TRANSLATED_COLUMN;
    private static final int TYPE_COLUMN;
    private static final ObjHashSet<CharSequence> excludeSet = new ObjHashSet<>();

    public static boolean isExcluded(CharSequence funcName) {
        return excludeSet.contains(funcName) || Chars.startsWith(funcName, "test_");
    }

    @Override
    public String getSignature() {
        return "functions()";
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
        FunctionFactoryCache ffCache = sqlExecutionContext.getCairoEngine().getFunctionFactoryCache();
        return new CursorFunction(new FunctionsCursorFactory(ffCache.getFactories())) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    public enum FunctionFactoryType {
        PREDICATE, CURSOR, GROUP_BY, WINDOW, STANDARD;

        public static FunctionFactoryType getType(FunctionFactory factory) {
            if (factory.isBoolean()) {
                return PREDICATE;
            }
            if (factory.isCursor()) {
                return CURSOR;
            }
            if (factory.isGroupBy()) {
                return GROUP_BY;
            }
            if (factory.isWindow()) {
                return WINDOW;
            }
            return STANDARD;
        }
    }

    private static class FunctionsCursorFactory extends AbstractRecordCursorFactory {
        private final FunctionsRecordCursor cursor = new FunctionsRecordCursor();
        private final LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> factories;
        private final ObjList<CharSequence> funcNames;

        public FunctionsCursorFactory(LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> factories) {
            super(METADATA);
            this.factories = factories;
            this.funcNames = factories.keys();
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
            sink.type("functions()");
        }


        private class FunctionsRecordCursor implements NoRandomAccessRecordCursor {
            private final FunctionRecord record = new FunctionRecord();
            ObjList<FunctionFactoryDescriptor> funcDescriptors;
            private int descriptorIndex = -1;
            private int funcNameIndex = -1;

            @Override
            public void close() {
                funcNameIndex = -1;
                descriptorIndex = -1;
                funcDescriptors = null;
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                if (funcNameIndex < funcNames.size() - 1) {
                    if (funcDescriptors == null) {
                        funcNameIndex++;
                        CharSequence funcName = funcNames.get(funcNameIndex);
                        if (isExcluded(funcName)) {
                            return hasNext();
                        }
                        funcDescriptors = factories.get(funcName);
                        descriptorIndex++;
                        record.init(funcName, funcDescriptors.get(descriptorIndex).getFactory());
                        return true;
                    } else {
                        descriptorIndex++;
                        if (descriptorIndex < funcDescriptors.size()) {
                            record.init(
                                    funcNames.get(funcNameIndex),
                                    funcDescriptors.get(descriptorIndex).getFactory()
                            );
                            return true;
                        } else {
                            descriptorIndex = -1;
                            funcDescriptors = null;
                            return hasNext();
                        }
                    }
                }
                return false;
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
                close();
            }

            private class FunctionRecord implements Record {
                private final StringSink sink = new StringSink();
                private FunctionFactory funcFactory;
                private CharSequence funcName;

                @Override
                public boolean getBool(int col) {
                    if (col == RT_CONSTANT_COLUMN) {
                        return funcFactory.isRuntimeConstant();
                    }
                    throw new IllegalArgumentException("offending: " + col);
                }

                @Override
                @NotNull
                public CharSequence getStrA(int col) {
                    if (col == NAME_COLUMN) {
                        return funcName;
                    }
                    if (col == SIGNATURE_COLUMN) {
                        return funcFactory.getSignature();
                    }
                    if (col == SIGNATURE_TRANSLATED_COLUMN) {
                        sink.clear();
                        return FunctionFactoryDescriptor.translateSignature(funcName, funcFactory.getSignature(), sink);
                    }
                    if (col == TYPE_COLUMN) {
                        return FunctionFactoryType.getType(funcFactory).name();
                    }
                    throw new IndexOutOfBoundsException("offending: " + col);
                }

                @Override
                public CharSequence getStrB(int col) {
                    return getStrA(col);
                }

                @Override
                public int getStrLen(int col) {
                    return getStrA(col).length();
                }

                private void init(CharSequence funcName, FunctionFactory factory) {
                    this.funcName = funcName;
                    this.funcFactory = factory;
                    sink.clear();
                }
            }
        }
    }

    static {
        NAME_COLUMN = 0;
        SIGNATURE_COLUMN = 1;
        SIGNATURE_TRANSLATED_COLUMN = 2;
        RT_CONSTANT_COLUMN = 3;
        TYPE_COLUMN = 4;
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("name", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("signature", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("signature_translated", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("runtime_constant", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("type", ColumnType.STRING));
        METADATA = metadata;

        excludeSet.add("&");
        excludeSet.add("|");
        excludeSet.add("^");
        excludeSet.add("~");
        excludeSet.add("[]");
        excludeSet.add("!=");
        excludeSet.add("!~");
        excludeSet.add("%");
        excludeSet.add("*");
        excludeSet.add("+");
        excludeSet.add("-");
        excludeSet.add(".");
        excludeSet.add("/");
        excludeSet.add("<");
        excludeSet.add("<=");
        excludeSet.add("<>");
        excludeSet.add("<>all");
        excludeSet.add("=");
        excludeSet.add(">");
        excludeSet.add(">=");
        excludeSet.add("VARCHAR");
    }
}
