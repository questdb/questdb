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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cutlass.pgwire.PGOids;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.util.Collection;
import java.util.Iterator;

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
        private final MemoryMR metaMem = Vm.getCMRInstance();
        private final Path path = new Path();

        public AttributeCatalogueCursorFactory(CairoEngine engine, RecordMetadata metadata) {
            super(metadata);
            this.cursor = new AttributeClassCatalogueCursor(engine, path, metaMem);
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

        @Override
        protected void _close() {
            Misc.free(path);
            Misc.free(metaMem);
        }
    }

    private static class AttributeClassCatalogueCursor implements NoRandomAccessRecordCursor {
        private final Collection<CairoTable> cairoTables;
        private final DiskReadingRecord diskReadingRecord = new DiskReadingRecord();
        private Iterator<CairoTable> cairoTablesIterator;
        private int columnCount;
        private int columnIndex = 0;
        private CairoColumn nextColumn;
        private CairoTable nextTable;
        private int tableId = 1000;

        public AttributeClassCatalogueCursor(CairoEngine engine, Path path, MemoryMR metaMem) {
            this.cairoTables = engine.metadataCacheGetTableList();
        }

        @Override
        public void close() {

        }

        @Override
        public Record getRecord() {
            return diskReadingRecord;
        }


        @Override
        public boolean hasNext() {
            if (columnIndex == columnCount) {
                nextTable = null;
                nextColumn = null;
                columnIndex = 0;
            }

            if (nextTable == null) {
                if (cairoTablesIterator.hasNext()) {
                    nextTable = cairoTablesIterator.next();
                    columnCount = (int) nextTable.getColumnCount();
                    tableId = nextTable.getId();
                }
            }

            if (nextTable != null) {

                if (columnIndex < columnCount) {
                    nextColumn = nextTable.columns.getQuick(columnIndex);
                    final int type = PGOids.getTypeOid(nextColumn.getType());
                    diskReadingRecord.intValues[N_ATTTYPID_COL] = type;
                    diskReadingRecord.name = nextColumn.getName();
                    diskReadingRecord.shortValues[N_ATTNUM_COL] = (short) (columnIndex + 1);
                    diskReadingRecord.shortValues[N_ATTLEN_COL] = (short) PG_TYPE_TO_SIZE_MAP.get(type);
                    diskReadingRecord.intValues[N_ATTRELID_COL] = tableId;
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
            this.cairoTablesIterator = cairoTables.iterator();
        }

        static class DiskReadingRecord implements Record {
            public final int[] intValues = new int[9];
            public final short[] shortValues = new short[9];
            private final StringSink strBSink = new StringSink();
            public CharSequence name = null;

            @Override
            public boolean getBool(int col) {
                return col == 9;
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
                if (name != null) {
                    strBSink.clear();
                    strBSink.put(name);
                    return strBSink;
                }
                return null;
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
