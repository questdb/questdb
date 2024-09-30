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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cutlass.pgwire.PGOids;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;

import static io.questdb.cutlass.pgwire.PGOids.PG_TYPE_TO_SIZE_MAP;

public class PgAttributeFunctionFactory implements FunctionFactory {

    public static final int N_ATTRELID_COL = 0;
    public static final int N_ATTNAME_COL = N_ATTRELID_COL + 1;
    private static final int N_ATTNUM_COL = N_ATTNAME_COL + 1;
    private static final int N_ATTTYPID_COL = N_ATTNUM_COL + 1;
    private static final int N_ATTNOTNULL_COL = N_ATTTYPID_COL + 1;
    private static final int N_ATTTYPMOD_COL = N_ATTNOTNULL_COL + 1;
    private static final int N_ATTLEN_COL = N_ATTTYPMOD_COL + 1;
    private static final int N_ATTIDENTITY_COL = N_ATTLEN_COL + 1;
    private static final int N_ATTISDROPPED_COL = N_ATTIDENTITY_COL + 1;
    private static final int N_ATTHASDEF_COL = N_ATTISDROPPED_COL + 1;

    private static final RecordMetadata METADATA;
    private static final String SIGNATURE = "pg_attribute()";

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
        return new CursorFunction(
                new AttributeCatalogueCursorFactory(
                        sqlExecutionContext.getCairoEngine(),
                        METADATA
                )
        ) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    private static class AttributeCatalogueCursorFactory extends AbstractRecordCursorFactory {

        private final AttributeClassCatalogueCursor cursor;

        public AttributeCatalogueCursorFactory(CairoEngine engine, RecordMetadata metadata) {
            super(metadata);
            this.cursor = new AttributeClassCatalogueCursor(engine);
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

    }

    private static class AttributeClassCatalogueCursor implements NoRandomAccessRecordCursor {
        private final PgAttributeRecord record = new PgAttributeRecord();
        private final ObjHashSet<TableToken> tableTokenSet;
        CairoEngine engine;
        private int columnCount;
        private int columnIndex = 0;
        private CairoTable nextTable;
        private int pos = -1;
        private int tableId = 1000;
        private ObjList<TableToken> tableTokens;

        public AttributeClassCatalogueCursor(CairoEngine engine) {
            this.engine = engine;
            tableTokenSet = new ObjHashSet<>(engine.getTableTokenCount(false));
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
            if (columnIndex == columnCount) {
                nextTable = null;
                columnIndex = 0;
            }

            if (nextTable == null) {
                if (pos < tableTokens.size() - 1) {
                    do {
                        pos++;
                        nextTable = engine.metadataCacheGetTable(tableTokens.getQuiet(pos));
                    } while (nextTable == null);
                    columnCount = (int) nextTable.getColumnCount();
                    tableId = nextTable.getId();
                }
            }

            if (nextTable != null) {
                if (columnIndex < columnCount) {
                    final CairoColumn nextColumn = nextTable.columns.getQuick(columnIndex);
                    final int type = PGOids.getTypeOid(nextColumn.getType());
                    record.intValues[N_ATTTYPID_COL] = type;
                    record.name = nextColumn.getName();
                    record.shortValues[N_ATTNUM_COL] = (short) (columnIndex + 1);
                    record.shortValues[N_ATTLEN_COL] = (short) PG_TYPE_TO_SIZE_MAP.get(type);
                    record.intValues[N_ATTRELID_COL] = tableId;
                    columnIndex++;
                    return true;
                }
            }

            return false;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            engine.getTableTokens(tableTokenSet, false);
            tableTokens = tableTokenSet.getList();
            pos = -1;
            nextTable = null;
            columnCount = 0;
            columnIndex = 0;
        }

        static class PgAttributeRecord implements Record {
            public final int[] intValues = new int[9];
            public final short[] shortValues = new short[9];
            public CharSequence name = null;

            @Override
            public boolean getBool(int col) {
                return col == N_ATTHASDEF_COL;
            }

            @Override
            public char getChar(int col) {
                //from the PG docs:
                // attidentity ->	If a zero byte (''), then not an identity column. Otherwise, a = generated always, d = generated by default.
                return Character.MIN_VALUE;
            }

            @Override
            public int getInt(int col) {
                return intValues[col];
            }

            @Override
            public short getShort(int col) {
                return shortValues[col];
            }

            @Override
            public CharSequence getStrA(int col) {
                return name;
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

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("attrelid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("attname", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("attnum", ColumnType.SHORT));
        metadata.add(new TableColumnMetadata("atttypid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("attnotnull", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("atttypmod", ColumnType.INT));
        metadata.add(new TableColumnMetadata("attlen", ColumnType.SHORT));
        metadata.add(new TableColumnMetadata("attidentity", ColumnType.CHAR));
        metadata.add(new TableColumnMetadata("attisdropped", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("atthasdef", ColumnType.BOOLEAN));
        METADATA = metadata;
    }
}
