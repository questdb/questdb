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
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

class SampleByFillPrevRecordCursor extends AbstractVirtualRecordSampleByCursor {
    private final Map map;
    private final RecordSink keyMapSink;
    private final RecordCursor mapCursor;

    public SampleByFillPrevRecordCursor(
            Map map,
            RecordSink keyMapSink,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            int timestampIndex, // index of timestamp column in base cursor
            TimestampSampler timestampSampler
    ) {
        super(recordFunctions, timestampIndex, timestampSampler, groupByFunctions);
        this.map = map;
        this.keyMapSink = keyMapSink;
        this.mapCursor = map.getCursor();
        record.of(map.getRecord());
    }

    @Override
    public boolean hasNext() {
        //
        if (mapCursor.hasNext()) {
            // scroll down the map iterator
            // next() will return record that uses current map position
            return true;
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
            return map.getCursor().hasNext();
        }

        this.lastTimestamp = this.nextTimestamp;

        // looks like we need to populate key map

        int n = groupByFunctions.size();
        while (true) {
            interruptor.checkInterrupted();
            final long timestamp = getBaseRecordTimestamp();
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
                GroupByUtils.toTop(groupByFunctions);
            }

            return this.map.getCursor().hasNext();
        }
    }

    @Override
    public void toTop() {
        super.toTop();
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
}
