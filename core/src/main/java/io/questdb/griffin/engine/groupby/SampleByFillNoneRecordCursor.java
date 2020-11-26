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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.ObjList;

class SampleByFillNoneRecordCursor extends AbstractVirtualRecordSampleByCursor {
    private final Map map;
    private final RecordSink keyMapSink;
    private final RecordCursor mapCursor;

    public SampleByFillNoneRecordCursor(
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
        this.record.of(map.getRecord());
        this.mapCursor = map.getCursor();
    }

    @Override
    public boolean hasNext() {
        return mapHasNext() || baseRecord != null && computeNextBatch();
    }

    @Override
    public void toTop() {
        super.toTop();
        if (base.hasNext()) {
            baseRecord = base.getRecord();
            this.nextTimestamp = timestampSampler.round(baseRecord.getTimestamp(timestampIndex));
            this.lastTimestamp = this.nextTimestamp;
            map.clear();
        }
    }

    private boolean computeNextBatch() {
        this.lastTimestamp = this.nextTimestamp;
        this.map.clear();

        // looks like we need to populate key map
        // at the start of this loop 'lastTimestamp' will be set to timestamp
        // of first record in base cursor
        int n = groupByFunctions.size();
        do {
            final long timestamp = getBaseRecordTimestamp();
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

                // get group by function to top to indicate that they have a new pass over the data set
                GroupByUtils.toTop(groupByFunctions);
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
}
