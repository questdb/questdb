/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.cairo.AbstractRecordCursorFactory;
import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ColumnTypes;
import com.questdb.cairo.RecordSink;
import com.questdb.cairo.map.*;
import com.questdb.cairo.sql.*;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.griffin.engine.table.EmptyTableRecordCursor;
import com.questdb.std.ObjList;
import com.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public class SampleByRecordCursorFactory extends AbstractRecordCursorFactory {

    private final Map map;
    private final RecordCursorFactory base;
    private final SampleByRecordCursor cursor;
    private final ObjList<GroupByFunction> functions;
    private final RecordSink mapSink;
    private final int functionCount;

    public SampleByRecordCursorFactory(
            RecordMetadata metadata,
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @NotNull ColumnTypes valueTypes,
            RecordCursorFactory base,
            @NotNull ObjList<GroupByFunction> functions,
            @NotNull RecordSink mapSink,
            @NotNull TimestampSampler timestampSampler) {
        super(metadata);
        this.map = MapFactory.createMap(configuration, keyTypes, valueTypes);
        this.base = base;
        this.cursor = new SampleByRecordCursor(map, mapSink, functions, metadata, base.getMetadata().getTimestampIndex(), timestampSampler);
        this.functions = functions;
        this.mapSink = mapSink;
        this.functionCount = functions.size();
    }

    @Override
    public RecordCursor getCursor(BindVariableService bindVariableService) {
        final RecordCursor baseCursor = base.getCursor(bindVariableService);

        map.clear();

        // This factory fills gaps in data. To do that we
        // have to know all possible key values. Essentially, every time
        // we sample we return same set of key values with different
        // aggregation results and timestamp

        while (baseCursor.hasNext()) {
            MapKey key = map.withKey();
            mapSink.copy(baseCursor.next(), key);
            // we do not care about result, new or old
            key.createValue();
        }

        if (map.size() == 0) {
            return EmptyTableRecordCursor.INSTANCE;
        }

        baseCursor.toTop();
        // we know base cursor has value
        boolean next = baseCursor.hasNext();
        assert next;
        cursor.of(baseCursor);

        for (int i = 0; i < functionCount; i++) {
            functions.getQuick(i).init(cursor, bindVariableService);
        }

        return cursor;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return false;
    }

    private static class SampleByRecordCursor implements RecordCursor {
        private final Map map;
        private final RecordSink keyMapSink;
        private final ObjList<GroupByFunction> functions;
        private final int timestampIndex;
        private final TimestampSampler timestampSampler;
        private final SampleByRecord record;
        private RecordCursor base;
        private Iterator<MapRecord> mapIterator;
        private Record baseRecord;
        private long lastTimestamp;

        public SampleByRecordCursor(
                Map map,
                RecordSink keyMapSink,
                ObjList<GroupByFunction> functions,
                RecordMetadata metadata,
                int timestampIndex, // index of timestamp column in base cursor
                TimestampSampler timestampSampler) {
            this.map = map;
            this.functions = functions;
            this.timestampIndex = timestampIndex;
            this.keyMapSink = keyMapSink;
            this.timestampSampler = timestampSampler;
            this.record = new SampleByRecord(map.getRecord(), functions, metadata.getColumnCount());
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record newRecord() {
            return null;
        }

        @Override
        public Record recordAt(long rowId) {
            return null;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
        }

        @Override
        public void toTop() {
        }

        @Override
        public boolean hasNext() {
            if (mapIterator != null && mapIterator.hasNext()) {
                return true;
            }

            // key map has been flushed
            // before we build another one we need to check
            // for timestamp gaps

            // whats the timestamp of next data record?
            long timestamp = timestampSampler.round(baseRecord.getTimestamp(timestampIndex));

            // what is the next timestamp we are expecting?
            long nextTimestamp = timestampSampler.nextTimestamp(lastTimestamp);

            // is data timestamp ahead of next expected timestamp?
            if (timestamp > nextTimestamp) {
                // yep, lets fill
                // todo: set 'record' to 'fill record'
                lastTimestamp = nextTimestamp;
                return true;
            }

            // looks like we need to populate key map

            int n = functions.size();
            while (true) {
                if (lastTimestamp == timestamp) {
                    final MapKey key = map.withKey();
                    keyMapSink.copy(baseRecord, key);
                    final MapValue value = key.findValue();
                    assert value != null;

                    if (value.getLong(0) != timestamp) {
                        value.putLong(0, timestamp);
                        for (int i = 0; i < n; i++) {
                            functions.getQuick(i).computeFirst(value);
                        }
                    } else {
                        for (int i = 0; i < n; i++) {
                            functions.getQuick(i).computeNext(value);
                        }
                    }

                    // carry on if we still have data
                    if (base.hasNext()) {
                        base.next();
                        timestamp = timestampSampler.round(baseRecord.getTimestamp(timestampIndex));
                        continue;
                    }

                    // when out of data we need to rely on map content
                    // that we built so far
                    this.mapIterator = map.iterator();
                    return this.mapIterator.hasNext();
                } else {
                    // timestamp changed
                    // return map content
                    // for those values that have timestamp != last timestamp we would have to exercise "fill" option
                    this.mapIterator = map.iterator();
                    return this.mapIterator.hasNext();
                }
            }
        }

        @Override
        public Record next() {
            return null;
        }

        void of(RecordCursor base) {
            // factory guarantees that base cursor is not empty
            this.base = base;
            this.baseRecord = base.next();
        }
    }

    private static class SampleByRecord implements Record {
        private final MapRecord mapRecord;
        private final int split;
        private final ObjList<GroupByFunction> functions;

        public SampleByRecord(MapRecord mapRecord, ObjList<GroupByFunction> functions, int mapRecordColumnCount) {
            this.mapRecord = mapRecord;
            this.split = mapRecordColumnCount - functions.size();
            this.functions = functions;
        }

        @Override
        public double getDouble(int col) {
            if (col < split) {
                return mapRecord.getDouble(col);
            }
            return getFunc(col).getDouble(mapRecord);
        }

        @Override
        public int getInt(int col) {
            if (col < split) {
                return mapRecord.getInt(col);
            }
            return getFunc(col).getInt(mapRecord);
        }

        private Function getFunc(int col) {
            return functions.getQuick(col - split);
        }
    }
}
