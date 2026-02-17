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
import io.questdb.cairo.CairoColumn;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.pgwire.PGOids;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.IntList;
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
        return new CursorFunction(new AttributeCatalogueCursorFactory()) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    private static class AttributeCatalogueCursorFactory extends AbstractRecordCursorFactory {
        private final AttributeClassCatalogueCursor cursor;
        private final CharSequenceObjHashMap<CairoTable> tableCache = new CharSequenceObjHashMap<>();
        private long tableCacheVersion = -1;

        public AttributeCatalogueCursorFactory() {
            super(METADATA);
            this.cursor = new AttributeClassCatalogueCursor(tableCache);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            final CairoEngine engine = executionContext.getCairoEngine();
            try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
                tableCacheVersion = metadataRO.snapshot(tableCache, tableCacheVersion);
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
            sink.type(SIGNATURE);
        }
    }

    private static class AttributeClassCatalogueCursor implements NoRandomAccessRecordCursor {
        private final PgAttributeRecord record = new PgAttributeRecord();
        private final CharSequenceObjHashMap<CairoTable> tableCache;
        private int columnIdx = -1;
        private int iteratorIdx = -1;
        private CairoTable table;
        private int tableId;

        public AttributeClassCatalogueCursor(CharSequenceObjHashMap<CairoTable> tableCache) {
            super();
            this.tableCache = tableCache;
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
            if (table == null) {
                if (!nextTable()) {
                    return false;
                }
                tableId = table.getId();
            }

            assert table != null;
            // we have a table

            if (columnIdx < table.getColumnCount() - 1) {
                columnIdx++;
            } else {
                columnIdx = -1;
                table = null;
                return hasNext();
            }

            CairoColumn column = table.getColumnQuiet(columnIdx);
            assert column != null;

            final int type = PGOids.getTypeOid(column.getType());
            record.intValues[N_ATTTYPID_COL] = type;
            record.name = column.getName();
            record.shortValues[N_ATTNUM_COL] = (short) (columnIdx + 1);
            record.shortValues[N_ATTLEN_COL] = PG_TYPE_TO_SIZE_MAP.get(type);
            record.intValues[N_ATTRELID_COL] = tableId;
            record.intValues[N_ATTTYPMOD_COL] = PGOids.getAttTypMod(type);

            return true;
        }

        public boolean nextTable() {
            assert table == null;

            if (iteratorIdx < tableCache.size() - 1) {
                table = tableCache.getAt(++iteratorIdx);
            } else return false;

            return true;
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
            columnIdx = -1;
            table = null;
            iteratorIdx = -1;
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
