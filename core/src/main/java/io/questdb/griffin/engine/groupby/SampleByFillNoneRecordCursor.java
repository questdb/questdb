/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionInterruptor;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.ObjList;

class SampleByFillNoneRecordCursor implements DelegatingRecordCursor, NoRandomAccessRecordCursor {
    private final Map map;
    private final RecordSink keyMapSink;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final int timestampIndex;
    private final TimestampSampler timestampSampler;
    private final Record record;
    private final RecordCursor mapCursor;
    private final ObjList<Function> recordFunctions;
    private RecordCursor base;
    private Record baseRecord;
    private long lastTimestamp;
    private long nextTimestamp;
    private SqlExecutionInterruptor interruptor;

    public SampleByFillNoneRecordCursor(
            Map map,
            RecordSink keyMapSink,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            int timestampIndex, // index of timestamp column in base cursor
            TimestampSampler timestampSampler
    ) {
        this.map = map;
        this.groupByFunctions = groupByFunctions;
        this.timestampIndex = timestampIndex;
        this.keyMapSink = keyMapSink;
        this.timestampSampler = timestampSampler;
        VirtualRecord rec = new VirtualRecordNoRowid(recordFunctions);
        rec.of(map.getRecord());
        this.record = rec;
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            Function f = recordFunctions.getQuick(i);
            if (f == null) {
                recordFunctions.setQuick(i, new TimestampFunc(0));
            }
        }
        this.mapCursor = map.getCursor();
        this.recordFunctions = recordFunctions;
    }

    @Override
    public void close() {
        base.close();
        interruptor = null;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) recordFunctions.getQuick(columnIndex);
    }

    @Override
    public boolean hasNext() {
        return mapHasNext() || baseRecord != null && computeNextBatch();
    }

    @Override
    public void toTop() {
        MultiArgFunction.toTop(recordFunctions);
        this.base.toTop();
        if (base.hasNext()) {
            baseRecord = base.getRecord();
            this.nextTimestamp = timestampSampler.round(baseRecord.getTimestamp(timestampIndex));
            this.lastTimestamp = this.nextTimestamp;
            map.clear();
        }
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void of(RecordCursor base, SqlExecutionContext executionContext) {
        // factory guarantees that base cursor is not empty
        this.base = base;
        this.baseRecord = base.getRecord();
        this.nextTimestamp = timestampSampler.round(baseRecord.getTimestamp(timestampIndex));
        this.lastTimestamp = this.nextTimestamp;
        interruptor = executionContext.getSqlExecutionInterruptor();
    }

    private boolean computeNextBatch() {
        this.lastTimestamp = this.nextTimestamp;
        this.map.clear();

        // looks like we need to populate key map
        // at the start of this loop 'lastTimestamp' will be set to timestamp
        // of first record in base cursor
        int n = groupByFunctions.size();
        do {
            final long timestamp = timestampSampler.round(baseRecord.getTimestamp(timestampIndex));
            if (lastTimestamp == timestamp) {
                final MapKey key = map.withKey();
                keyMapSink.copy(baseRecord, key);
                GroupByUtils.updateFunctions(groupByFunctions, n, key.createValue(), baseRecord);
            } else {
                // timestamp changed, make sure we keep the value of 'lastTimestamp'
                // unchanged. Timestamp columns uses this variable
                // When map is exhausted we would assign 'nextTimestamp' to 'lastTimestamp'
                // and build another map
                this.nextTimestamp = timestamp;
                return createMapCursor();
            }
            interruptor.checkInterrupted();
        } while (base.hasNext());

        // we ran out of data, make sure hasNext() returns false at the next
        // opportunity, after we stream map that is.
        baseRecord = null;
        return createMapCursor();
    }

    private boolean createMapCursor() {
        // reset map iterator
        map.getCursor();
        // we do not have any more data, let map take over
        return mapHasNext();
    }

    private boolean mapHasNext() {
        return mapCursor.hasNext();
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
