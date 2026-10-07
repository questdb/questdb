/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.ObjList;

public class SampleByFillPrevNotKeyedRecordCursor extends AbstractVirtualRecordSampleByCursor {
    private final SimpleMapValue value;
    private boolean isFirstRun = true;

    public SampleByFillPrevNotKeyedRecordCursor(
            CairoConfiguration configuration,
            ObjList<GroupByFunction> groupByFunctions,
            GroupByFunctionsUpdater groupByFunctionsUpdater,
            ObjList<Function> recordFunctions,
            int timestampIndex, // index of timestamp column in base cursor
            int timestampType,
            TimestampSampler timestampSampler,
            SimpleMapValue value,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos,
            Function sampleFromFunc,
            int sampleFromFuncPos,
            Function sampleToFunc,
            int sampleToFuncPos,
            boolean isFromToUtc
    ) {
        super(
                configuration,
                recordFunctions,
                timestampIndex,
                timestampType,
                timestampSampler,
                groupByFunctions,
                groupByFunctionsUpdater,
                timezoneNameFunc,
                timezoneNameFuncPos,
                offsetFunc,
                offsetFuncPos,
                sampleFromFunc,
                sampleFromFuncPos,
                sampleToFunc,
                sampleToFuncPos,
                isFromToUtc
        );
        this.value = value;
        record.of(value);
    }

    @Override
    public boolean hasNext() {
        initTimestamps();

        if (baseRecord == null) {
            return false;
        }

        // the next sample epoch could be different from current sample epoch due to DST transition,
        // e.g. clock going backward
        // we need to ensure we do not fill time transition
        final long expectedLocalEpoch;
        if (isFirstRun) {
            // On the first call, nextSampleLocalEpoch holds the bucket at FROM, see initTimestamps(), and
            // that bucket is the first one to emit, as in SampleByFillValueNotKeyedRecordCursor. Expecting
            // the bucket after it skipped the bucket at FROM whenever the first row came after it, unlike
            // the GROUP BY path (GitHub issue #7764). Without FROM, or with the first row at or before the
            // bucket at FROM, nextSampleLocalEpoch is not before localEpoch, and notKeyedLoop() aggregates the
            // bucket of the first row.
            expectedLocalEpoch = nextSampleLocalEpoch;
            isFirstRun = false;
        } else {
            expectedLocalEpoch = timestampSampler.nextTimestamp(nextSampleLocalEpoch);
        }
        // is data timestamp ahead of next expected timestamp?
        if (expectedLocalEpoch < localEpoch) {
            sampleLocalEpoch = expectedLocalEpoch;
            nextSampleLocalEpoch = expectedLocalEpoch;
            return true;
        }

        return notKeyedLoop(value);
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        super.of(baseCursor, executionContext);
        isFirstRun = true;
        setValueToNull();
    }

    @Override
    public void toTop() {
        super.toTop();
        isFirstRun = true;
        setValueToNull();
    }

    // hasNext() emits the gap buckets between FROM and the first row from the value before
    // notKeyedLoop() aggregates any row into it. Each function writes NULL into its own slots, as the
    // keyed FILL(PREV) cursor does for a key with no row yet. Otherwise those buckets read memory
    // that nothing has written or, after a rewind or a re-execution, the last bucket of the previous
    // pass. After a re-execution, the VARCHAR pointers of that bucket reference allocator memory
    // that close() has freed.
    private void setValueToNull() {
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            groupByFunctions.getQuick(i).setNull(value);
        }
    }
}
