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

public class SampleByFillPrevNotKeyedRecordCursor extends AbstractVirtualRecordSampleByCursor {
    private final SimpleMapValue simpleMapValue;

    public SampleByFillPrevNotKeyedRecordCursor(
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            int timestampIndex, // index of timestamp column in base cursor
            TimestampSampler timestampSampler,
            SimpleMapValue simpleMapValue,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos
    ) {
        super(
                recordFunctions,
                timestampIndex,
                timestampSampler,
                groupByFunctions,
                timezoneNameFunc,
                timezoneNameFuncPos,
                offsetFunc,
                offsetFuncPos
        );
        this.simpleMapValue = simpleMapValue;
        this.record.of(simpleMapValue);
    }

    @Override
    public boolean hasNext() {
        if (baseRecord == null) {
            return false;
        }

        // the next sample epoch could be different from current sample epoch due to DST transition,
        // e.g. clock going backward
        // we need to ensure we do not fill time transition
        final long expectedLocalEpoch = timestampSampler.nextTimestamp(nextSampleLocalEpoch);
        // is data timestamp ahead of next expected timestamp?
        if (expectedLocalEpoch < localEpoch) {
            this.sampleLocalEpoch = expectedLocalEpoch;
            this.nextSampleLocalEpoch = expectedLocalEpoch;
            return true;
        }
        return notKeyedLoop(simpleMapValue);
    }

    @Override
    public void toTop() {
        super.toTop();
        if (base.hasNext()) {
            baseRecord = base.getRecord();
        }
    }
}
