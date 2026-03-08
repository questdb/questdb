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

import io.questdb.ConfigPropertyKey;
import io.questdb.ConfigPropertyValue;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjObjHashMap;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public class ShowParametersCursorFactory extends AbstractRecordCursorFactory {
    private static final GenericRecordMetadata METADATA = new GenericRecordMetadata();
    private final ShowParametersRecordCursor cursor = new ShowParametersRecordCursor();

    public ShowParametersCursorFactory() {
        super(METADATA);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        return cursor.of(executionContext.getCairoEngine().getConfiguration().getAllPairs());
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show parameters");
    }

    private static final class EmptyIterator implements Iterator<ObjObjHashMap.Entry<ConfigPropertyKey, ConfigPropertyValue>> {
        private static final EmptyIterator INSTANCE = new EmptyIterator();

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public ObjObjHashMap.Entry<ConfigPropertyKey, ConfigPropertyValue> next() {
            return null;
        }
    }

    private static class ShowParametersRecordCursor implements NoRandomAccessRecordCursor {
        private ObjObjHashMap<ConfigPropertyKey, ConfigPropertyValue> allPairs;
        private ObjObjHashMap.Entry<ConfigPropertyKey, ConfigPropertyValue> entry;
        private final Record record = new Record() {
            @Override
            public boolean getBool(int col) {
                return switch (col) {
                    case 4 -> entry.key.isSensitive();
                    case 5 -> entry.value.isDynamic();
                    default -> false;
                };
            }

            @Override
            public CharSequence getStrA(int col) {
                return switch (col) {
                    case 0 -> entry.key.getPropertyPath();
                    case 1 -> entry.key.getEnvVarName();
                    case 2 -> {
                        if (entry.key.isSensitive()) {
                            yield "****";
                        }
                        yield entry.value.getValue();
                    }
                    case 3 -> switch (entry.value.getValueSource()) {
                        case ConfigPropertyValue.VALUE_SOURCE_DEFAULT -> "default";
                        case ConfigPropertyValue.VALUE_SOURCE_CONF -> "conf";
                        case ConfigPropertyValue.VALUE_SOURCE_ENV -> "env";
                        case ConfigPropertyValue.VALUE_SOURCE_FILE -> "file";
                        default -> "unknown";
                    };
                    default -> null;
                };
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStrA(col);
            }

            @Override
            public int getStrLen(int col) {
                return TableUtils.lengthOf(getStrA(col));
            }
        };
        @NotNull
        private Iterator<ObjObjHashMap.Entry<ConfigPropertyKey, ConfigPropertyValue>> iterator = EmptyIterator.INSTANCE;

        @Override
        public void close() {
            iterator = EmptyIterator.INSTANCE;
            allPairs = null;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (iterator.hasNext()) {
                entry = iterator.next();
                return true;
            }
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return -1L;
        }

        @Override
        public void toTop() {
            iterator = allPairs != null ? allPairs.iterator() : EmptyIterator.INSTANCE;
            entry = null;
        }

        private ShowParametersRecordCursor of(ObjObjHashMap<ConfigPropertyKey, ConfigPropertyValue> allPairs) {
            this.allPairs = allPairs;
            toTop();
            return this;
        }
    }

    static {
        METADATA.add(new TableColumnMetadata("property_path", ColumnType.STRING));
        METADATA.add(new TableColumnMetadata("env_var_name", ColumnType.STRING));
        METADATA.add(new TableColumnMetadata("value", ColumnType.STRING));
        METADATA.add(new TableColumnMetadata("value_source", ColumnType.STRING));
        METADATA.add(new TableColumnMetadata("sensitive", ColumnType.BOOLEAN));
        METADATA.add(new TableColumnMetadata("reloadable", ColumnType.BOOLEAN));
    }
}
