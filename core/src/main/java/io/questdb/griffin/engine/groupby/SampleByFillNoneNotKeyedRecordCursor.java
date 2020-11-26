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

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.ObjList;

class SampleByFillNoneNotKeyedRecordCursor extends AbstractVirtualRecordSampleByCursor {
    private final SimpleMapValue simpleMapValue;

    public SampleByFillNoneNotKeyedRecordCursor(
            SimpleMapValue simpleMapValue,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            int timestampIndex, // index of timestamp column in base cursor
            TimestampSampler timestampSampler
    ) {
        super(recordFunctions, timestampIndex, timestampSampler, groupByFunctions);
        this.simpleMapValue = simpleMapValue;
        this.record.of(simpleMapValue);
    }

    @Override
    public boolean hasNext() {
        if (baseRecord == null) {
            return false;
        }

        this.lastTimestamp = this.nextTimestamp;

        // looks like we need to populate key map
        // at the start of this loop 'lastTimestamp' will be set to timestamp
        // of first record in base cursor
        int n = groupByFunctions.size();
        GroupByUtils.updateNew(groupByFunctions, n, simpleMapValue, baseRecord);

        while (base.hasNext()) {
            final long timestamp = getBaseRecordTimestamp();
            if (lastTimestamp == timestamp) {
                GroupByUtils.updateExisting(groupByFunctions, n, simpleMapValue, baseRecord);
            } else {
                // timestamp changed, make sure we keep the value of 'lastTimestamp'
                // unchanged. Timestamp columns uses this variable
                // When map is exhausted we would assign 'nextTimestamp' to 'lastTimestamp'
                // and build another map
                this.nextTimestamp = timestamp;
                GroupByUtils.toTop(groupByFunctions);
                return true;
            }
            interruptor.checkInterrupted();
        }

        // opportunity, after we stream map that is.
        baseRecord = null;
        return true;
    }

    @Override
    public void toTop() {
        super.toTop();
        if (base.hasNext()) {
            baseRecord = base.getRecord();
            this.nextTimestamp = timestampSampler.round(baseRecord.getTimestamp(timestampIndex));
            this.lastTimestamp = this.nextTimestamp;
        }
    }
}
