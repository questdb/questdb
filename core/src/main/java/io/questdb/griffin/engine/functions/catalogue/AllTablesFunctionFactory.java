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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoMetadataRW;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AllTablesFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;

    @Override
    public String getSignature() {
        return "all_tables()";
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
        return new CursorFunction(new AllTablesCursorFactory()) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    public static class AllTablesCursorFactory extends AbstractRecordCursorFactory {
        public static final Log LOG = LogFactory.getLog(AllTablesCursorFactory.class);
        private final AllTablesRecordCursor cursor;
        private final HashMap<CharSequence, CairoTable> tableCache = new HashMap<>();
        private long tableCacheVersion = -1;

        public AllTablesCursorFactory() {
            super(METADATA);
            cursor = new AllTablesRecordCursor(tableCache);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            final CairoEngine engine = executionContext.getCairoEngine();
            try (CairoMetadataRW metadataRW = engine.getCairoMetadata().write()) {
                if (tableCache.isEmpty()) {
                    // initialise the first time this factory is created
                    metadataRW.snapshotCreate(tableCache);
                    tableCacheVersion = metadataRW.getVersion();
                } else {
                    // otherwise check if we need to refresh any values
                    tableCacheVersion = metadataRW.snapshotRefresh(tableCache, tableCacheVersion);
                }
                metadataRW.filterVisibleTables(tableCache);
            } catch (IOException ignore) {
            }
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("all_tables()");
        }

        @Override
        protected void _close() {
            cursor.close();
        }

        private static class AllTablesRecordCursor implements NoRandomAccessRecordCursor {
            private final AllTablesRecord record = new AllTablesRecord();
            private final HashMap<CharSequence, CairoTable> tableCache;
            private Iterator<Map.Entry<CharSequence, CairoTable>> iterator;


            public AllTablesRecordCursor(HashMap<CharSequence, CairoTable> tableCache) {
                this.tableCache = tableCache;
                this.iterator = tableCache.entrySet().iterator();
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
                if (iterator.hasNext()) {
                    record.of(iterator.next().getValue());
                    return true;
                }

                return false;
            }

            @Override
            public long size() {
                return -1;
            }

            @Override
            public void toTop() {
                this.iterator = tableCache.entrySet().iterator();
            }


            private static class AllTablesRecord implements Record {
                private CairoTable table;

                @Override
                public CharSequence getStrA(int col) {
                    return table.getTableName();
                }

                @Override
                public CharSequence getStrB(int col) {
                    return getStrA(col);
                }

                @Override
                public int getStrLen(int col) {
                    return getStrA(col).length();
                }

                public void of(CairoTable table) {
                    this.table = table;
                }

            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("table_name", ColumnType.STRING));
        METADATA = metadata;
    }
}
