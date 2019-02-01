/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.groupby;

import com.questdb.cairo.RecordSink;
import com.questdb.cairo.map.Map;
import com.questdb.cairo.map.MapKey;
import com.questdb.cairo.map.MapRecord;
import com.questdb.cairo.map.MapValue;
import com.questdb.cairo.sql.*;
import com.questdb.griffin.engine.functions.GroupByFunction;
import com.questdb.griffin.engine.functions.TimestampFunction;
import com.questdb.std.IntIntHashMap;
import com.questdb.std.Numbers;
import com.questdb.std.ObjList;

class SampleByFillValueRecordCursor implements DelegatingRecordCursor, NoRandomAccessRecordCursor {
    private final Map map;
    private final RecordSink keyMapSink;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final int timestampIndex;
    private final TimestampSampler timestampSampler;
    private final SplitVirtualRecord record;
    private final Record mapRecord;
    private final IntIntHashMap symbolTableIndex;
    private RecordCursor base;
    private RecordCursor mapCursor;
    private Record baseRecord;
    private long lastTimestamp;
    private long nextTimestamp;

    public SampleByFillValueRecordCursor(
            Map map,
            RecordSink keyMapSink,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            ObjList<Function> placeholderFunctions,
            int timestampIndex, // index of timestamp column in base cursor
            TimestampSampler timestampSampler,
            IntIntHashMap symbolTableIndex) {
        this.map = map;
        this.groupByFunctions = groupByFunctions;
        this.timestampIndex = timestampIndex;
        this.keyMapSink = keyMapSink;
        this.timestampSampler = timestampSampler;
        this.mapRecord = map.getRecord();
        this.record = new SplitVirtualRecord(recordFunctions, placeholderFunctions);
        this.record.of(mapRecord);
        this.symbolTableIndex = symbolTableIndex;
        assert recordFunctions.size() == placeholderFunctions.size();
        final TimestampFunc timestampFunc = new TimestampFunc(0);
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            Function f = recordFunctions.getQuick(i);
            if (f == null) {
                recordFunctions.setQuick(i, timestampFunc);
                placeholderFunctions.setQuick(i, timestampFunc);
            }
        }
        this.mapCursor = map.getCursor();
    }

    @Override
    public void close() {
        base.close();
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return base.getSymbolTable(symbolTableIndex.get(columnIndex));
    }

    @Override
    public boolean hasNext() {
        //
        if (mapCursor.hasNext()) {
            // scroll down the map iterator
            // next() will return record that uses current map position
            return refreshRecord();
        }

        if (baseRecord == null) {
            return false;
        }

        // key map has been flushed
        // before we build another one we need to check
        // for timestamp gaps

        // what is the next timestamp we are expecting?
        long nextTimestamp = timestampSampler.nextTimestamp(lastTimestamp);

        // is data timestamp ahead of next expected timestamp?
        if (this.nextTimestamp > nextTimestamp) {
            this.lastTimestamp = nextTimestamp;
            // reset iterator on map and stream contents
            refreshCursorAndRecord();
            return true;
        }

        this.lastTimestamp = this.nextTimestamp;

        // looks like we need to populate key map

        int n = groupByFunctions.size();
        while (true) {
            long timestamp = timestampSampler.round(baseRecord.getTimestamp(timestampIndex));
            if (lastTimestamp == timestamp) {
                final MapKey key = map.withKey();
                keyMapSink.copy(baseRecord, key);
                final MapValue value = key.findValue();
                assert value != null;

                if (value.getLong(0) != timestamp) {
                    value.putLong(0, timestamp);
                    for (int i = 0; i < n; i++) {
                        groupByFunctions.getQuick(i).computeFirst(value, baseRecord);
                    }
                } else {
                    for (int i = 0; i < n; i++) {
                        groupByFunctions.getQuick(i).computeNext(value, baseRecord);
                    }
                }

                // carry on with the loop if we still have data
                if (base.hasNext()) {
                    continue;
                }

                // we ran out of data, make sure hasNext() returns false at the next
                // opportunity, after we stream map that is.
                baseRecord = null;
            } else {
                // timestamp changed, make sure we keep the value of 'lastTimestamp'
                // unchanged. Timestamp columns uses this variable
                // When map is exhausted we would assign 'nextTimestamp' to 'lastTimestamp'
                // and build another map
                this.nextTimestamp = timestamp;
            }

            refreshCursorAndRecord();
            return true;
        }
    }

    @Override
    public void toTop() {
        this.base.toTop();
        if (base.hasNext()) {
            baseRecord = base.getRecord();
            this.nextTimestamp = timestampSampler.round(baseRecord.getTimestamp(timestampIndex));
            this.lastTimestamp = this.nextTimestamp;

            int n = groupByFunctions.size();
            RecordCursor mapCursor = map.getCursor();
            MapRecord mapRecord = map.getRecord();
            while (mapCursor.hasNext()) {
                MapValue value = mapRecord.getValue();
                // timestamp is always stored in value field 0
                value.putLong(0, Numbers.LONG_NaN);
                // have functions reset their columns to "zero" state
                // this would set values for when keys are not found right away
                for (int i = 0; i < n; i++) {
                    groupByFunctions.getQuick(i).setNull(value);
                }
            }
        }
    }

    @Override
    public void of(RecordCursor base) {
        // factory guarantees that base cursor is not empty
        this.base = base;
        this.baseRecord = base.getRecord();
        this.nextTimestamp = timestampSampler.round(baseRecord.getTimestamp(timestampIndex));
        this.lastTimestamp = this.nextTimestamp;
    }

    private void refreshCursorAndRecord() {
        this.map.getCursor().hasNext();
        refreshRecord();
    }

    private boolean refreshRecord() {
        if (mapRecord.getTimestamp(0) == lastTimestamp) {
            record.setActiveA();
        } else {
            record.setActiveB();
        }
        return true;
    }

    private class TimestampFunc extends TimestampFunction {

        public TimestampFunc(int position) {
            super(position);
        }

        @Override
        public long getTimestamp(Record rec) {
            return lastTimestamp;
        }
    }
}
