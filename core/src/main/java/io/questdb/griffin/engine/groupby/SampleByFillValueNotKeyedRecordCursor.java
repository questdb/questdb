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
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.ObjList;

public class SampleByFillValueNotKeyedRecordCursor extends AbstractSplitVirtualRecordSampleByCursor {
    private final SimpleMapValue simpleMapValue;

    public SampleByFillValueNotKeyedRecordCursor(
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            ObjList<Function> placeholderFunctions,
            int timestampIndex, // index of timestamp column in base cursor
            TimestampSampler timestampSampler,
            SimpleMapValue simpleMapValue
    ) {
        super(recordFunctions, timestampIndex, timestampSampler, groupByFunctions, placeholderFunctions);
        this.simpleMapValue = simpleMapValue;
        this.record.of(simpleMapValue);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        if (baseRecord == null) {
            return false;
        }

        // key map has been flushed
        // before we build another one we need to check
        // for timestamp gaps

        // what is the next timestamp we are expecting?
        final long nextTimestamp = timestampSampler.nextTimestamp(lastTimestamp);

        // is data timestamp ahead of next expected timestamp?
        if (this.nextTimestamp > nextTimestamp) {
            this.lastTimestamp = nextTimestamp;
            record.setActiveB();
            return true;
        }

        // this is new timestamp value
        this.lastTimestamp = timestampSampler.round(baseRecord.getTimestamp(timestampIndex));

        // switch to non-placeholder record
        record.setActiveA();

        int n = groupByFunctions.size();
        // initialize values
        for (int i = 0; i < n; i++) {
            interruptor.checkInterrupted();
            groupByFunctions.getQuick(i).computeFirst(simpleMapValue, baseRecord);
        }

        while (base.hasNext()) {
            final long timestamp = getBaseRecordTimestamp();
            if (lastTimestamp == timestamp) {
                for (int i = 0; i < n; i++) {
                    interruptor.checkInterrupted();
                    groupByFunctions.getQuick(i).computeNext(simpleMapValue, baseRecord);
                }
            } else {
                // timestamp changed, make sure we keep the value of 'lastTimestamp'
                // unchanged. Timestamp columns uses this variable
                // When map is exhausted we would assign 'nextTimestamp' to 'lastTimestamp'
                // and build another map
                this.nextTimestamp = timestamp;
                GroupByUtils.toTop(groupByFunctions);
                return true;
            }
        }

        // no more data from base cursor
        // return what we aggregated so far and stop
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
