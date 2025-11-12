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

package io.questdb.griffin.model;

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;

public class RuntimeStaticIntervalModel implements RuntimeIntrinsicIntervalModel {
    protected final LongList intervals;
    protected final int partitionBy;
    protected final TimestampDriver timestampDriver;

    public RuntimeStaticIntervalModel(TimestampDriver timestampDriver, int partitionBy, LongList staticIntervals) {
        this.intervals = staticIntervals;
        this.timestampDriver = timestampDriver;
        this.partitionBy = partitionBy;
    }

    @Override
    public boolean allIntervalsHitOnePartition() {
        return !PartitionBy.isPartitioned(partitionBy) || allIntervalsHitOnePartition(timestampDriver.getPartitionFloorMethod(partitionBy));
    }

    @Override
    public LongList calculateIntervals(SqlExecutionContext sqlExecutionContext) throws SqlException {
        return intervals;
    }

    public RuntimeStaticIntervalModel cloneAndFixByTimestampDriver(TimestampDriver timestampDriver) {

        return new RuntimeStaticIntervalModel(
                timestampDriver,
                partitionBy,
                intervals
        );
    }

    @Override
    public void close() {
    }

    @Override
    public TimestampDriver getTimestampDriver() {
        return timestampDriver;
    }

    @Override
    public boolean isStatic() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (intervals != null && intervals.size() > 0) {
            sink.val('[');
            for (int i = 0, n = intervals.size(); i < n; i += 2) {
                if (i > 0) {
                    sink.val(',');
                }
                sink.val("(\"");
                valTs(sink, timestampDriver, intervals.getQuick(i));
                sink.val("\",\"");
                valTs(sink, timestampDriver, intervals.getQuick(i + 1));
                sink.val("\")");
            }
            sink.val(']');
        }
    }

    private boolean allIntervalsHitOnePartition(TimestampDriver.TimestampFloorMethod floorMethod) {
        if (intervals.size() == 0) {
            return true;
        }

        long floor = floorMethod.floor(intervals.getQuick(0));
        for (int i = 1, n = intervals.size(); i < n; i++) {
            if (floor != floorMethod.floor(intervals.getQuick(i))) {
                return false;
            }
        }
        return true;
    }

    protected static void valTs(PlanSink sink, TimestampDriver driver, long l) {
        if (l == Numbers.LONG_NULL) {
            sink.val("MIN");
        } else if (l == Long.MAX_VALUE) {
            sink.val("MAX");
        } else {
            sink.valISODate(driver, l);
        }
    }
}
