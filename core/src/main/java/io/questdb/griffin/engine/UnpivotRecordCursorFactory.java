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
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BinarySequence;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

public class UnpivotRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final RecordMetadata baseMetadata;
    private final UnpivotRecordCursor cursor;
    private final RecordMetadata unpivotMetadata;
    private final int inColumnIndex;
    private final int valueColumnIndex;
    private final IntList unpivotForIndices;
    private final IntIntHashMap passthroughIndicesMap;
    private final ObjList<CharSequence> unpivotForNames;

    public UnpivotRecordCursorFactory(RecordCursorFactory base, RecordMetadata unpivotMetadata, int inColumnIndex, int valueColumnIndex, IntList unpivotForIndices, ObjList<CharSequence> unpivotForNames, IntIntHashMap passthroughIndicesMap) {
        super(unpivotMetadata);
        this.unpivotMetadata = unpivotMetadata;
        this.baseMetadata = base.getMetadata();
        this.base = base;
        this.inColumnIndex = inColumnIndex;
        this.valueColumnIndex = valueColumnIndex;
        this.unpivotForIndices = unpivotForIndices;
        this.passthroughIndicesMap = passthroughIndicesMap;
        this.unpivotForNames = unpivotForNames;
        this.cursor = new UnpivotRecordCursor();
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
        return SCAN_DIRECTION_OTHER; // we are generating new rows, ordering is not guaranteed
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Unpivot")
                .attr("into").val(unpivotMetadata.getColumnName(valueColumnIndex))
                .attr("for").val(unpivotMetadata.getColumnName(inColumnIndex))
                .attr("in");

        sink.val('[');

        for (int i = 0, n = unpivotForIndices.size(); i < n; i++) {
            sink.val(baseMetadata.getColumnName(unpivotForIndices.getQuick(i)));

            if (i + 1 < n) {
                sink.val(',');
            }
        }

        sink.val(']');
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        base.close();
    }

    public class UnpivotRecordCursor implements RecordCursor {
        private SqlExecutionContext executionContext;
        private RecordCursor baseCursor;
        private Record baseRecord = null;
        private final UnpivotRecord unpivotRecord;
        private int columnPosition = -1;

        public UnpivotRecordCursor() {
            unpivotRecord = new UnpivotRecord();
        }

        public void of(SqlExecutionContext executionContext) throws SqlException {
            this.executionContext = executionContext;
            this.baseCursor = base.getCursor(executionContext);
            this.toTop();
        }

        @Override
        public void close() {

        }

        @Override
        public Record getRecord() {
            return unpivotRecord;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }


        // We are iterating over each column we need to map.
        @Override
        public boolean hasNext() throws DataUnavailableException {

            // if we aren't on a record yet
            if (baseRecord == null) {
                assert columnPosition == -1;
                if (!baseCursor.hasNext()) {
                    return false;
                }
                baseRecord = baseCursor.getRecord();
                unpivotRecord.of(baseRecord);
                columnPosition++;
                return true;
            }

            if (columnPosition + 1 < unpivotForIndices.size()) {
                columnPosition++;
                return true;
            } else {
                baseRecord = null;
                columnPosition = -1;
                return hasNext();
            }
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() throws DataUnavailableException {
            return 0;
        }

        @Override
        public void toTop() {
            columnPosition = -1;
            baseRecord = null;
            baseCursor.toTop();
        }
    }

    private class UnpivotRecord implements Record {
        Record baseRecord;

        public void of(Record baseRecord) {
            this.baseRecord = baseRecord;
        }

        // if it is a regular column, we simply pass it through

        // logic - if it is a value column, we look

        @Override
        public BinarySequence getBin(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getBin(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getBin(passthrough);
        }

        @Override
        public long getBinLen(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getBinLen(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getBinLen(passthrough);
        }

        @Override
        public boolean getBool(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getBool(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getBool(passthrough);
        }

        @Override
        public byte getByte(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getByte(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getByte(passthrough);
        }

        @Override
        public char getChar(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getChar(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getChar(passthrough);
        }

        @Override
        public double getDouble(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getDouble(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getDouble(passthrough);
        }

        @Override
        public float getFloat(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getFloat(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getFloat(passthrough);
        }

        @Override
        public byte getGeoByte(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getGeoByte(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getGeoByte(passthrough);
        }

        @Override
        public int getGeoInt(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getGeoInt(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getGeoInt(passthrough);
        }

        @Override
        public long getGeoLong(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getGeoLong(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getGeoLong(passthrough);
        }

        @Override
        public short getGeoShort(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getGeoShort(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getGeoShort(passthrough);
        }

        @Override
        public int getIPv4(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getIPv4(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getIPv4(passthrough);
        }

        @Override
        public int getInt(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getInt(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getInt(passthrough);
        }

        @Override
        public long getLong(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getLong(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getLong(passthrough);
        }

        @Override
        public long getLong128Hi(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getLong128Hi(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getLong128Hi(passthrough);
        }

        @Override
        public long getLong128Lo(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getLong128Lo(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getLong128Lo(passthrough);
        }

        @Override
        public void getLong256(int col, CharSink<?> sink) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                baseRecord.getLong256(unpivotForIndices.getQuick(cursor.columnPosition), sink);
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            baseRecord.getLong256(passthrough, sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getLong256A(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getLong256A(passthrough);
        }

        @Override
        public Long256 getLong256B(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getLong256B(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getLong256B(passthrough);
        }

        @Override
        public long getLongIPv4(int col) {
            return getLong(col);
        }

        @Override
        public short getShort(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getShort(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getShort(passthrough);
        }


        @Override
        public @Nullable CharSequence getStrA(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getStrA(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getStrA(passthrough);
        }

        @Override
        public CharSequence getStrB(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getStrB(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getStrB(passthrough);
        }

        @Override
        public int getStrLen(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getStrLen(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getStrLen(passthrough);
        }

        @Override
        public CharSequence getSymA(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getSymA(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is the name column
            if (col == inColumnIndex) {
                return unpivotForNames.getQuick(cursor.columnPosition);
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getSymA(passthrough);
        }

        @Override
        public CharSequence getSymB(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getSymB(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is the name column
            if (col == inColumnIndex) {
                return unpivotForNames.getQuick(cursor.columnPosition);
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getSymB(passthrough);
        }

        @Override
        public long getTimestamp(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getTimestamp(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getTimestamp(passthrough);
        }

        @Override
        public @Nullable Utf8Sequence getVarcharA(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getVarcharA(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getVarcharA(passthrough);
        }

        @Override
        public @Nullable Utf8Sequence getVarcharB(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getVarcharB(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getVarcharB(passthrough);
        }

        @Override
        public int getVarcharSize(int col) {
            // if it is the value column, look up the column position in the base record
            if (col == valueColumnIndex) {
                return baseRecord.getVarcharSize(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            // if it is a passthrough
            int passthrough = passthroughIndicesMap.get(col);

            assert col != -1;

            return baseRecord.getVarcharSize(passthrough);
        }
    }
}
