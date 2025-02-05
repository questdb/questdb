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

/**
 * Rewrite PIVOT statements.
 * SELECT *
 * FROM cities
 * PIVOT (
 * sum(population)
 * FOR
 * year IN (2000, 2010, 2020)
 * GROUP BY country
 * );
 * -- becomes
 * SELECT
 * country,
 * SUM(CASE WHEN year = 2000 THEN population ELSE 0 END) AS 2000
 * SUM(CASE WHEN year = 2010 THEN population ELSE 0 END) AS 2010
 * SUM(CASE WHEN year = 2020 THEN population ELSE 0 END) AS 2020
 * FROM cities
 * GROUP BY country;
 */
// target:
// select-choose country, SUM(switch(year,2000,population,0)) population_2000, SUM(switch(year,2010,population,0)) population_2010, SUM(switch(year,2020,population,0)) population_2020 from (cities group by country)
// ast -> sum (function), paramCount 1
// rhs is -> switch, (function), paramCount 4
// arg0 -> '0', type 2
// arg1 -> 'population', type 4
// arg2 -> '2000', type 2
// arg3 -> 'year', type 4

// number_of_pivot_columns
// for_column_lengths
// for_column_indices
public class PivotRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final RecordMetadata baseMetadata;
    private final PivotRecordCursor cursor;
    private final int inColumnIndex;
    private final IntIntHashMap passthroughIndicesMap;
    private final ObjList<CharSequence> pivotForNames;
    private final RecordMetadata pivotMetadata;
    private final IntList unpivotForIndices;
    private final int valueColumnIndex;

    public PivotRecordCursorFactory(RecordCursorFactory base, RecordMetadata pivotMetadata, int inColumnIndex, int valueColumnIndex, IntList pivotForIndices, ObjList<CharSequence> pivotForNames, IntIntHashMap passthroughIndicesMap) {
        super(pivotMetadata);
        this.pivotMetadata = pivotMetadata;
        this.baseMetadata = base.getMetadata();
        this.base = base;
        this.inColumnIndex = inColumnIndex;
        this.valueColumnIndex = valueColumnIndex;
        this.unpivotForIndices = pivotForIndices;
        this.passthroughIndicesMap = passthroughIndicesMap;
        this.pivotForNames = pivotForNames;
        this.cursor = new PivotRecordCursor();
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
                .attr("into").val(pivotMetadata.getColumnName(valueColumnIndex))
                .attr("for").val(pivotMetadata.getColumnName(inColumnIndex))
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

    /*
        Handles the mapping of passthrough and unpivoted columns.
        Three cases:
            1. col == valueColumnIndex. This is the case where the parent cursor is trying to retrieve an unpivoted value.
                We look up the right column in our base record and then retrieve the value.
            2. col == inColumnIndex. This is only for SYMBOL, and means that the parent cursor is trying to retrieve
                the name of the unpivoted column. We look up the name from our names list.
            3. Otherwise, column is a passthrough column. We look up its value in the base record and return it.
     */
    private class PivotRecord implements Record {
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
            return baseRecord.getStrA(passthroughIndicesMap.get(col));
        }

        @Override
        public CharSequence getStrB(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getStrB(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getStrB(passthroughIndicesMap.get(col));
        }

        @Override
        public int getStrLen(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getStrLen(unpivotForIndices.getQuick(cursor.columnPosition));
            }
            return baseRecord.getStrLen(passthroughIndicesMap.get(col));
        }

        @Override
        public CharSequence getSymA(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getSymA(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            if (col == inColumnIndex) {
                return pivotForNames.getQuick(cursor.columnPosition);
            }

            return baseRecord.getSymA(passthroughIndicesMap.get(col));
        }

        @Override
        public CharSequence getSymB(int col) {
            if (col == valueColumnIndex) {
                return baseRecord.getSymB(unpivotForIndices.getQuick(cursor.columnPosition));
            }

            if (col == inColumnIndex) {
                return pivotForNames.getQuick(cursor.columnPosition);
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
    }

    public class PivotRecordCursor implements RecordCursor {
        private final PivotRecord pivotRecord;
        private RecordCursor baseCursor;
        private Record baseRecord = null;
        private int columnPosition = -1;

        public PivotRecordCursor() {
            pivotRecord = new PivotRecord();
        }

        @Override
        public void close() {
            baseCursor.close();
        }

        @Override
        public Record getRecord() {
            return pivotRecord;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() throws DataUnavailableException {

            // if we have no record, we grab one and set column position to 0
            if (baseRecord == null) {
                // columnPosition == -1, baseRecord == null is a pairing we assert
                assert columnPosition == -1;

                if (!baseCursor.hasNext()) {
                    return false;
                }
                // pull the base record
                baseRecord = baseCursor.getRecord();

                // make it available to our cursor
                pivotRecord.of(baseRecord);

                // bump our -1 position to 0, so we are looking at the first column
                columnPosition++;
                return true;
            }

            // on each subsequent call, we shift the column position to the next column we need to unpivot
            if (columnPosition + 1 < unpivotForIndices.size()) {
                columnPosition++;
                return true;
            } else {
                // when we have no columns left, we reset the state and call hasNext() again
                baseRecord = null;
                columnPosition = -1;
                return hasNext();
            }
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
    }
}