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

package io.questdb.griffin.model;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Timestamps;

import static io.questdb.griffin.model.IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL;
import static io.questdb.griffin.model.RuntimeIntervalModelBuilder.DYNAMIC_LO_HI;

public class RuntimeIntervalModel implements RuntimeIntrinsicIntervalModel {
    // These 2 are incoming model
    private final LongList intervals;
    private final ObjList<Function> dynamicRangeList;

    // This used to assemble result
    private LongList outIntervals;

    public RuntimeIntervalModel(LongList intervals) {
        this(intervals, null);
    }

    public RuntimeIntervalModel(LongList staticIntervals, ObjList<Function> dynamicRangeList) {
        this.intervals = staticIntervals;

        this.dynamicRangeList = dynamicRangeList;
    }

    @Override
    public LongList calculateIntervals(SqlExecutionContext sqlContext) {
        if (isStatic()) {
            return intervals;
        }

        if (outIntervals == null) {
            outIntervals = new LongList();
        } else {
            outIntervals.clear();
        }

        // Copy static part
        int dynamicStart = intervals.size() - dynamicRangeList.size() * STATIC_LONGS_PER_DYNAMIC_INTERVAL;
        outIntervals.add(intervals, 0, dynamicStart);

        // Evaluate intervals involving functions
        addEvaluateDynamicIntervals(outIntervals, sqlContext);
        return outIntervals;
    }

    @Override
    public boolean isFocused(Timestamps.TimestampFloorMethod floorMethod) {
        if (!isStatic()) return false;

        long floor = floorMethod.floor(intervals.getQuick(0));
        for (int i = 1, n = intervals.size(); i < n; i++) {
            if (floor != floorMethod.floor(intervals.getQuick(i))) {
                return false;
            }
        }
        return true;
    }

    private void addEvaluateDynamicIntervals(LongList outIntervals, SqlExecutionContext sqlContext) {
        int dynamicStart = intervals.size() - dynamicRangeList.size() * STATIC_LONGS_PER_DYNAMIC_INTERVAL;
        int dynamicIndex = 0;

        for (int i = dynamicStart; i < intervals.size(); i += STATIC_LONGS_PER_DYNAMIC_INTERVAL) {
            Function dynamicFunction = dynamicRangeList.get(dynamicIndex++);
            short operation = IntervalUtils.getEncodedOperation(intervals, i);
            int divider = outIntervals.size();

            if (dynamicFunction == null) {
                // copy 4 longs to output and apply the operation
                outIntervals.add(intervals, i, i + STATIC_LONGS_PER_DYNAMIC_INTERVAL);
                IntervalUtils.applyLastEncodedIntervalEx(outIntervals);
            } else {
                long lo = IntervalUtils.getEncodedPeriodLo(intervals, i);
                long hi = IntervalUtils.getEncodedPeriodHi(intervals, i);
                int adjustment = IntervalUtils.getEncodedAdjustment(intervals, i);
                dynamicFunction.init(null, sqlContext);
                long dynamicValue = dynamicFunction.getTimestamp(null);
                if (dynamicValue == Long.MIN_VALUE) {
                    // function evaluated to null. return empty set
                    outIntervals.clear();
                    return;
                }

                if (operation == IntervalOperation.INTERSECT_EQUALS) {
                    hi = lo = dynamicValue;
                } else if (hi == DYNAMIC_LO_HI) {
                    hi = dynamicValue + adjustment;
                } else {
                    lo = dynamicValue + adjustment;
                }

                outIntervals.extendAndSet(divider + 1, hi);
                outIntervals.extendAndSet(divider, lo);
            }

            if (i > 0) {
                // Do not apply operation (intersect, subtract)
                // if this is first element and no pre-calculated static intervals exist
                switch (operation) {
                    case IntervalOperation.INTERSECT:
                    case IntervalOperation.INTERSECT_EQUALS:
                        IntervalUtils.intersectInplace(outIntervals, divider);
                        break;
                    case IntervalOperation.SUBTRACT:
                        IntervalUtils.subtract(outIntervals, divider);
                        break;
                    default:
                        throw new UnsupportedOperationException("Interval operation " + operation + " is not supported");
                }
            }
        }
    }

    private boolean isStatic() {
        return dynamicRangeList == null || dynamicRangeList.size() == 0;
    }
}
