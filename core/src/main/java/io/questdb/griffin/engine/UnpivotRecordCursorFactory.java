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

package io.questdb.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BinarySequence;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Long128;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;


/**
 * UNPIVOTs data i.e rotate columns into rows.
 * <p>
 * FROM monthly_sales UNPIVOT (
 * sales
 * FOR month IN (jan, feb, mar, apr, may, jun)
 * );
 */
public class UnpivotRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final RecordMetadata baseMetadata;
    private final UnpivotRecordCursor cursor;
    private final int inColumnIndex;
    private final boolean includeNulls;
    private final IntIntHashMap passthroughIndicesMap;
    private final IntList unpivotForIndices;
    private final ObjList<CharSequence> unpivotForNames;
    private final RecordMetadata unpivotMetadata;
    private final int valueColumnIndex;
    private final int valueColumnType;

    public UnpivotRecordCursorFactory(RecordCursorFactory base, RecordMetadata unpivotMetadata, int inColumnIndex, int valueColumnIndex, IntList unpivotForIndices, ObjList<CharSequence> unpivotForNames, IntIntHashMap passthroughIndicesMap, boolean includeNulls) {
        super(unpivotMetadata);
        this.unpivotMetadata = unpivotMetadata;
        this.baseMetadata = base.getMetadata();
        this.base = base;
        this.inColumnIndex = inColumnIndex;
        this.valueColumnIndex = valueColumnIndex;
        this.valueColumnType = unpivotMetadata.getColumnType(valueColumnIndex);
        this.unpivotForIndices = unpivotForIndices;
        this.passthroughIndicesMap = passthroughIndicesMap;
        this.unpivotForNames = unpivotForNames;
        this.includeNulls = includeNulls;
        this.cursor = new UnpivotRecordCursor(includeNulls);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(executionContext);
        return cursor;
    }

    @Override
    public int getScanDirection() {
        return SCAN_DIRECTION_OTHER;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Unpivot");
        sink.attr("into").val(unpivotMetadata.getColumnName(valueColumnIndex));
        sink.attr("for").val(unpivotMetadata.getColumnName(inColumnIndex));
        sink.attr("in");
        sink.val('[');

        for (int i = 0, n = unpivotForIndices.size(); i < n; i++) {
            sink.val(baseMetadata.getColumnName(unpivotForIndices.getQuick(i)));

            if (i + 1 < n) {
                sink.val(',');
            }
        }

        sink.val(']');
        sink.attr("nulls");

        if (includeNulls) {
            sink.val("included");
        } else {
            sink.val("excluded");
        }

        sink.child(base);
    }

    @Override
    protected void _close() {
        passthroughIndicesMap.clear();
        unpivotForIndices.clear();
        unpivotForNames.clear();
        base.close();
        super._close();
    }

    private class UnpivotExcludeNullsRecord extends UnpivotRecord {

        @Override
        boolean mustSkipUnpivotValueColumn(int col) {
            int trueColumnIndex = unpivotForIndices.getQuick(cursor.columnPosition);
            switch (valueColumnType) {
                case ColumnType.INT:
                    return baseRecord.getInt(trueColumnIndex) == Numbers.INT_NULL;
                case ColumnType.LONG:
                    return baseRecord.getLong(trueColumnIndex) == Numbers.LONG_NULL;
                case ColumnType.FLOAT:
                    return Numbers.isNull(baseRecord.getFloat(trueColumnIndex));
                case ColumnType.DOUBLE:
                    return Numbers.isNull(baseRecord.getDouble(trueColumnIndex));
                case ColumnType.IPv4:
                    return baseRecord.getIPv4(trueColumnIndex) == Numbers.IPv4_NULL;
                case ColumnType.STRING:
                    return baseRecord.getStrA(trueColumnIndex) == null;
                case ColumnType.SYMBOL:
                    return baseRecord.getSymA(trueColumnIndex) == null;
                case ColumnType.VARCHAR:
                    return baseRecord.getVarcharA(trueColumnIndex) == null;
                case ColumnType.DATE:
                    return baseRecord.getDate(trueColumnIndex) == Numbers.LONG_NULL;
                case ColumnType.TIMESTAMP:
                    return baseRecord.getTimestamp(trueColumnIndex) == Numbers.LONG_NULL;
                case ColumnType.UUID:
                    return Long128.isNull(baseRecord.getLong128Lo(trueColumnIndex), baseRecord.getLong128Hi(trueColumnIndex));
                case ColumnType.BINARY:
                    return baseRecord.getBin(trueColumnIndex) == null;
                case ColumnType.LONG256:
                    return Long256Impl.isNull(baseRecord.getLong256A(trueColumnIndex));
                case ColumnType.GEOBYTE:
                    return baseRecord.getGeoByte(trueColumnIndex) == GeoHashes.INT_NULL;
                case ColumnType.GEOSHORT:
                    return baseRecord.getGeoShort(trueColumnIndex) == GeoHashes.INT_NULL;
                case ColumnType.GEOINT:
                    return baseRecord.getGeoInt(trueColumnIndex) == GeoHashes.INT_NULL;
                case ColumnType.GEOLONG:
                    return baseRecord.getGeoLong(trueColumnIndex) == GeoHashes.INT_NULL;
                default:
                    return false;
            }
        }
    }

    private class UnpivotIncludeNullsRecord extends UnpivotRecord {
        @Override
        boolean mustSkipUnpivotValueColumn(int col) {
            return false;
        }
    }

    /*
        Handles the mapping of passthrough and unpivoted columns.
        Three cases:
            1. col == valueColumnIndex. This is the case where the parent cursor is trying to retrieve an unpivoted value.
                We look up the right column in our base record and then retrieve the value.
            2. col == inColumnIndex. This means that the parent cursor is trying to retrieve
                the name of the unpivoted column. We look up the name from our names list.
            3. Otherwise, column is a passthrough column. We look up its value in the base record and return it.
     */
    private abstract class UnpivotRecord implements Record {
        Record baseRecord;

        @Override
        public BinarySequence getBin(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getBin(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getBin(passthroughIndicesMap.get(col));
        }

        @Override
        public long getBinLen(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getBinLen(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getBinLen(passthroughIndicesMap.get(col));
        }

        @Override
        public boolean getBool(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getBool(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getBool(passthroughIndicesMap.get(col));
        }

        @Override
        public byte getByte(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getByte(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getByte(passthroughIndicesMap.get(col));
        }

        @Override
        public char getChar(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getChar(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getChar(passthroughIndicesMap.get(col));
        }

        @Override
        public double getDouble(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getDouble(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getDouble(passthroughIndicesMap.get(col));
        }

        @Override
        public float getFloat(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getFloat(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getFloat(passthroughIndicesMap.get(col));
        }

        @Override
        public byte getGeoByte(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getGeoByte(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getGeoByte(passthroughIndicesMap.get(col));
        }

        @Override
        public int getGeoInt(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getGeoInt(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getGeoInt(passthroughIndicesMap.get(col));
        }

        @Override
        public long getGeoLong(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getGeoLong(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getGeoLong(passthroughIndicesMap.get(col));
        }

        @Override
        public short getGeoShort(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getGeoShort(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getGeoShort(passthroughIndicesMap.get(col));
        }

        @Override
        public int getIPv4(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getIPv4(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getIPv4(passthroughIndicesMap.get(col));
        }

        @Override
        public int getInt(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getInt(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getInt(passthroughIndicesMap.get(col));
        }

        @Override
        public long getLong(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getLong(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            return baseRecord.getLong(passthroughIndicesMap.get(col));
        }

        @Override
        public long getLong128Hi(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getLong128Hi(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getLong128Hi(passthroughIndicesMap.get(col));
        }

        @Override
        public long getLong128Lo(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getLong128Lo(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getLong128Lo(passthroughIndicesMap.get(col));
        }

        @Override
        public void getLong256(int col, CharSink<?> sink) {
            if (col == valueColumnIndex) {
                baseRecord.getLong256(unpivotForIndices.getQuick(cursor.columnPosition), sink);
            }
            baseRecord.getLong256(passthroughIndicesMap.get(col), sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getLong256A(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getLong256A(passthroughIndicesMap.get(col));
        }

        @Override
        public Long256 getLong256B(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getLong256B(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getLong256B(passthroughIndicesMap.get(col));
        }

        @Override
        public long getLongIPv4(int col) {
            return getLong(col);
        }

        @Override
        public short getShort(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getShort(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getShort(passthroughIndicesMap.get(col));
        }

        @Override
        public @Nullable CharSequence getStrA(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getStrA(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            if (col == inColumnIndex) {
                return unpivotForNames.getQuick(cursor.columnPosition);
            }
            return baseRecord.getStrA(passthroughIndicesMap.get(col));
        }

        @Override
        public CharSequence getStrB(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getStrB(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            if (col == inColumnIndex) {
                return unpivotForNames.getQuick(cursor.columnPosition);
            }

            return baseRecord.getStrB(passthroughIndicesMap.get(col));
        }

        @Override
        public int getStrLen(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getStrLen(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            if (col == inColumnIndex) {
                return unpivotForNames.getQuick(cursor.columnPosition).length();
            }

            return baseRecord.getStrLen(passthroughIndicesMap.get(col));
        }

        @Override
        public CharSequence getSymA(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getSymA(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getSymA(passthroughIndicesMap.get(col));
        }

        @Override
        public CharSequence getSymB(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getSymB(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getSymB(passthroughIndicesMap.get(col));
        }

        @Override
        public long getTimestamp(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getTimestamp(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getTimestamp(passthroughIndicesMap.get(col));
        }

        @Override
        public @Nullable Utf8Sequence getVarcharA(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getVarcharA(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getVarcharA(passthroughIndicesMap.get(col));
        }

        @Override
        public @Nullable Utf8Sequence getVarcharB(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getVarcharB(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getVarcharB(passthroughIndicesMap.get(col));
        }

        @Override
        public int getVarcharSize(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getVarcharSize(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getVarcharSize(passthroughIndicesMap.get(col));
        }

        public void of(Record baseRecord) {
            this.baseRecord = baseRecord;
        }

        abstract boolean mustSkipUnpivotValueColumn(int col);
    }

    public class UnpivotRecordCursor implements RecordCursor {
        private final UnpivotRecord unpivotRecord;
        private RecordCursor baseCursor;
        private Record baseRecord = null;
        private int columnPosition = -1;

        public UnpivotRecordCursor(boolean includeNulls) {
            if (includeNulls) {
                unpivotRecord = new UnpivotIncludeNullsRecord();
            } else {
                unpivotRecord = new UnpivotExcludeNullsRecord();
            }
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
        }

        @Override
        public Record getRecord() {
            return unpivotRecord;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() throws DataUnavailableException {
            return getNextRecord();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        public void of(SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = base.getCursor(executionContext);
            this.toTop();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() throws DataUnavailableException {
            return -1;
        }

        @Override
        public void toTop() {
            columnPosition = -1;
            baseRecord = null;
            baseCursor.toTop();
        }

        private boolean getNextColumn() {
            // loop over the columns, checking for nulls
            while (columnPosition + 1 < unpivotForIndices.size()) {
                columnPosition++;

                // if we have a non-null column
                if (!unpivotRecord.mustSkipUnpivotValueColumn(columnPosition)) {
                    // then we should return to signal we have a valid record
                    return true;
                }
            }

            // otherwise, null out the record, so the outer loop knows to keep iterating
            baseRecord = null;
            columnPosition = -1;

            return false;
        }

        private boolean getNextRecord() {
            while (true) {
                // if there is no current record
                if (baseRecord == null) {

                    // whilst we still have records to read
                    if (!baseCursor.hasNext()) {
                        return false;
                    }

                    // pull the base record out
                    baseRecord = baseCursor.getRecord();

                    // make it available to our cursor
                    unpivotRecord.of(baseRecord);
                }

                // check if there is a valid column
                if (getNextColumn()) {
                    // if so, we have found our next output record, so return true
                    return true;
                }
            }
        }
    }
}
