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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;

import static io.questdb.griffin.model.IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL;

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
    public LongList calculateIntervals(SqlExecutionContext sqlContext) throws SqlException {
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
    public boolean allIntervalsHitOnePartition(int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.DAY:
                return allIntervalsHitOnePartition(Timestamps.FLOOR_DD);
            case PartitionBy.MONTH:
                return allIntervalsHitOnePartition(Timestamps.FLOOR_MM);
            case PartitionBy.YEAR:
                return allIntervalsHitOnePartition(Timestamps.FLOOR_YYYY);
            default:
                return true;
        }
    }

    private boolean allIntervalsHitOnePartition(Timestamps.TimestampFloorMethod floorMethod) {
        if (!isStatic()) {
            return false;
        }
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

    @Override
    public void close() {
        Misc.freeObjList(dynamicRangeList);
    }

    private void addEvaluateDynamicIntervals(LongList outIntervals, SqlExecutionContext sqlContext) throws SqlException {
        int size = intervals.size();
        int dynamicStart = size - dynamicRangeList.size() * STATIC_LONGS_PER_DYNAMIC_INTERVAL;
        int dynamicIndex = 0;

        for (int i = dynamicStart; i < size; i += STATIC_LONGS_PER_DYNAMIC_INTERVAL) {
            Function dynamicFunction = dynamicRangeList.getQuick(dynamicIndex++);
            short operation = IntervalUtils.getEncodedOperation(intervals, i);
            boolean negated = operation > IntervalOperation.NEGATED_BORDERLINE;
            int divider = outIntervals.size();

            if (dynamicFunction == null) {
                // copy 4 longs to output and apply the operation
                outIntervals.add(intervals, i, i + STATIC_LONGS_PER_DYNAMIC_INTERVAL);
                IntervalUtils.applyLastEncodedIntervalEx(outIntervals);
            } else {
                long lo = IntervalUtils.getEncodedPeriodLo(intervals, i);
                long hi = IntervalUtils.getEncodedPeriodHi(intervals, i);
                short adjustment = IntervalUtils.getEncodedAdjustment(intervals, i);
                short dynamicHiLo = IntervalUtils.getEncodedDynamicIndicator(intervals, i);

                dynamicFunction.init(null, sqlContext);

                if (operation != IntervalOperation.INTERSECT_INTERVALS && operation != IntervalOperation.SUBTRACT_INTERVALS) {
                    long dynamicValue = getTimestamp(dynamicFunction);
                    long dynamicValue2 = 0;
                    if (dynamicHiLo == IntervalDynamicIndicator.IS_LO_SEPARATE_DYNAMIC) {
                        // Both ends of BETWEEN are dynamic and different values. Take next dynamic point.
                        i += STATIC_LONGS_PER_DYNAMIC_INTERVAL;
                        dynamicFunction = dynamicRangeList.getQuick(dynamicIndex++);
                        dynamicFunction.init(null, sqlContext);
                        dynamicValue2 = hi = getTimestamp(dynamicFunction);
                        lo = dynamicValue;
                    } else {
                        if ((dynamicHiLo & IntervalDynamicIndicator.IS_HI_DYNAMIC) != 0) {
                            hi = dynamicValue + adjustment;
                        }
                        if ((dynamicHiLo & IntervalDynamicIndicator.IS_LO_DYNAMIC) != 0) {
                            lo = dynamicValue + adjustment;
                        }
                    }

                    if (dynamicValue == Numbers.LONG_NaN || dynamicValue2 == Numbers.LONG_NaN) {
                        // functions evaluated to null.
                        if (!negated) {
                            // return empty set if it's not negated
                            outIntervals.clear();
                            return;
                        } else {
                            // or full set
                            negatedNothing(outIntervals, divider);
                            continue;
                        }
                    }

                    if (operation == IntervalOperation.INTERSECT_BETWEEN || operation == IntervalOperation.SUBTRACT_BETWEEN) {
                        long tempHi = Math.max(hi, lo);
                        lo = Math.min(hi, lo);
                        hi = tempHi;
                    }

                    outIntervals.extendAndSet(divider + 1, hi);
                    outIntervals.setQuick(divider, lo);
                } else {
                    // This is subtract or intersect with a string interval (not a single timestamp)
                    CharSequence strValue = dynamicFunction.getStr(null);
                    if (operation == IntervalOperation.INTERSECT_INTERVALS) {
                        // This is intersect
                        if (parseIntervalFails(outIntervals, strValue)){
                            // return empty set
                            outIntervals.clear();
                            return;
                        }
                    } else {
                        // This is subtract
                        if (parseIntervalFails(outIntervals, strValue)){
                            // full set
                            negatedNothing(outIntervals, divider);
                            continue;
                        }
                        IntervalUtils.invert(outIntervals, divider);
                    }
                }
            }

            // Do not apply operation (intersect, subtract)
            // if this is first element and no pre-calculated static intervals exist
            if (divider > 0) {
                switch (operation) {
                    case IntervalOperation.INTERSECT:
                    case IntervalOperation.INTERSECT_BETWEEN:
                    case IntervalOperation.INTERSECT_INTERVALS:
                    case IntervalOperation.SUBTRACT_INTERVALS:
                        IntervalUtils.intersectInplace(outIntervals, divider);
                        break;
                    case IntervalOperation.SUBTRACT:
                    case IntervalOperation.SUBTRACT_BETWEEN:
                        IntervalUtils.subtract(outIntervals, divider);
                        break;
                    // UNION cannot be the first thing at the moment.
                    default:
                        throw new UnsupportedOperationException("Interval operation " + operation + " is not supported");
                }
            }
        }
    }

    private boolean parseIntervalFails(LongList outIntervals, CharSequence strValue) {
        if (strValue  != null) {
            try {
                IntervalUtils.parseIntervalEx(strValue, 0, strValue.length(), 0, outIntervals, IntervalOperation.INTERSECT);
                IntervalUtils.applyLastEncodedIntervalEx(outIntervals);
            } catch (SqlException e) {
                return true;
            }
            return false;
        }
        return true;
    }

    private void negatedNothing(LongList outIntervals, int divider) {
        outIntervals.setPos(divider);
        if (divider == 0) {
            outIntervals.extendAndSet(1, Long.MAX_VALUE);
            outIntervals.extendAndSet(0, Long.MIN_VALUE);
        }
    }

    private long getTimestamp(Function dynamicFunction) {
        if (dynamicFunction.getType() == ColumnType.STRING) {
            CharSequence value = dynamicFunction.getStr(null);
            try {
                return IntervalUtils.parseFloorPartialDate(value);
            } catch (NumericException e) {
                return Numbers.LONG_NaN;
            }
        }
        return dynamicFunction.getTimestamp(null);
    }

    private boolean isStatic() {
        return dynamicRangeList == null || dynamicRangeList.size() == 0;
    }
}
