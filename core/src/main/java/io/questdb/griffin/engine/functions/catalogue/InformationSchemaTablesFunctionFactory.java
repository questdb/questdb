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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
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
import io.questdb.metrics.QueryTracingJob;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.tasks.TelemetryTask;

import static io.questdb.TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME;

// information_schema.tables basic implementation
// used in grafana meta queries
public class InformationSchemaTablesFunctionFactory implements FunctionFactory {

    private static final int COLUMN_CATALOG = 0;
    private static final int COLUMN_IS_INSERTABLE_INTO = 9;
    private static final int COLUMN_IS_TYPED = 10;
    private static final int COLUMN_NAME = 2;
    private static final int COLUMN_SCHEMA = 1;
    private static final int COLUMN_TYPE = 3;
    private static final RecordMetadata METADATA;

    @Override
    public String getSignature() {
        return "information_schema.tables()";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext) {
        return new CursorFunction(new InformationSchemaTablesCursorFactory(configuration, METADATA)) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    public static class InformationSchemaTablesCursorFactory extends AbstractRecordCursorFactory {
        private final TableListRecordCursor cursor = new TableListRecordCursor();
        private final boolean hideTelemetryTable;
        private final CharSequence sysTablePrefix;
        private final CharSequence tempPendingRenameTablePrefix;
        private CairoEngine engine;
        private TableToken tableToken;

        public InformationSchemaTablesCursorFactory(CairoConfiguration configuration, RecordMetadata metadata) {
            super(metadata);
            tempPendingRenameTablePrefix = configuration.getTempRenamePendingTablePrefix();
            sysTablePrefix = configuration.getSystemTableNamePrefix();
            hideTelemetryTable = configuration.getTelemetryConfiguration().hideTables();
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            engine = executionContext.getCairoEngine();
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("information_schema.tables");
        }

        @Override
        protected void _close() {
            cursor.close();
            engine = null;
        }

        private class TableListRecordCursor implements NoRandomAccessRecordCursor {
            private final TableListRecord record = new TableListRecord();
            private final ObjHashSet<TableToken> tableBucket = new ObjHashSet<>();
            private int tableIndex = -1;

            @Override
            public void close() {
                tableIndex = -1;
            }

            @Override
            public io.questdb.cairo.sql.Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                if (tableIndex < 0) {
                    engine.getTableTokens(tableBucket, false);
                    tableIndex = -1;
                }
                tableIndex++;
                int n = tableBucket.size();
                for (; tableIndex < n; tableIndex++) {
                    tableToken = tableBucket.get(tableIndex);
                    if (TableUtils.isFinalTableName(tableToken.getTableName(), tempPendingRenameTablePrefix) &&
                            !isSystemTable(tableToken)) {
                        break;
                    }
                }
                return tableIndex < n;
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
                tableIndex = -1;
            }

            private boolean isSystemTable(TableToken tableToken) {
                String tableName = tableToken.getTableName();
                return (hideTelemetryTable &&
                        (Chars.equals(tableName, TelemetryTask.TABLE_NAME) ||
                                Chars.equals(tableName, TELEMETRY_CONFIG_TABLE_NAME)))
                        || Chars.startsWith(tableName, sysTablePrefix)
                        || Chars.equals(tableName, QueryTracingJob.TABLE_NAME);
            }

            private class TableListRecord implements Record {
                @Override
                public boolean getBool(int col) {
                    if (col == COLUMN_IS_TYPED) {
                        return false;
                    }
                    return col == COLUMN_IS_INSERTABLE_INTO;
                }

                @Override
                public CharSequence getStrA(int col) {
                    if (col == COLUMN_NAME) {
                        return tableToken.getTableName();
                    }
                    if (col == COLUMN_CATALOG) {
                        return Constants.DB_NAME;
                    }
                    if (col == COLUMN_SCHEMA) {
                        return Constants.PUBLIC_SCHEMA;
                    }
                    if (col == COLUMN_TYPE) {
                        return "BASE TABLE";
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
        metadata.add(new TableColumnMetadata("table_catalog", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("table_schema", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("table_name", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("table_type", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("self_referencing_column_name", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("reference_generation", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("user_defined_type_catalog", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("user_defined_type_schema", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("user_defined_type_name", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("is_insertable_into", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("is_typed", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("commit_action", ColumnType.STRING));

        METADATA = metadata;
    }
}
