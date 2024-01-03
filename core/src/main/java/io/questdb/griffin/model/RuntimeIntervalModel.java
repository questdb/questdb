/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;

import static io.questdb.griffin.model.IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL;

public class RuntimeIntervalModel implements RuntimeIntrinsicIntervalModel {

    private static final Log LOG = LogFactory.getLog(RuntimeIntervalModel.class);
    private final ObjList<Function> dynamicRangeList;
    // These 2 are incoming model
    private final LongList intervals;
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
    public boolean allIntervalsHitOnePartition(int partitionBy) {
        return !PartitionBy.isPartitioned(partitionBy) || allIntervalsHitOnePartition(PartitionBy.getPartitionFloorMethod(partitionBy));
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
    public void close() {
        Misc.freeObjList(dynamicRangeList);
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (intervals != null && intervals.size() > 0) {
            sink.val('[');
            try {
                LongList intervals = calculateIntervals(sink.getExecutionContext());
                for (int i = 0, n = intervals.size(); i < n; i += 2) {
                    if (i > 0) {
                        sink.val(',');
                    }
                    sink.val("(\"");
                    valTs(sink, intervals.getQuick(i));
                    sink.val("\",\"");
                    valTs(sink, intervals.getQuick(i + 1));
                    sink.val("\")");
                }
            } catch (SqlException e) {
                LOG.error().$("Can't calculate intervals: ").$(e.getFlyweightMessage()).$();
            }
            sink.val(']');
        }
    }

    private static void valTs(PlanSink sink, long l) {
        if (l == Numbers.LONG_NaN) {
            sink.val("MIN");
        } else if (l == Long.MAX_VALUE) {
            sink.val("MAX");
        } else {
            sink.valISODate(l);
        }
    }

    private void addEvaluateDynamicIntervals(LongList outIntervals, SqlExecutionContext sqlContext) throws SqlException {
        int size = intervals.size();
        int dynamicStart = size - dynamicRangeList.size() * STATIC_LONGS_PER_DYNAMIC_INTERVAL;
        int dynamicIndex = 0;
        boolean firstFuncApplied = false;

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
                    if (divider == 0 && negated) {
                        // Divider == 0 means it's the first interval applied
                        // Invert the interval, since it will not be applied negated to anything
                        IntervalUtils.invert(outIntervals, divider);
                    }
                } else {
                    // This is subtract or intersect with a string interval (not a single timestamp)
                    CharSequence strValue = dynamicFunction.getStr(null);
                    if (operation == IntervalOperation.INTERSECT_INTERVALS) {
                        // This is intersect
                        if (parseIntervalFails(outIntervals, strValue)) {
                            // return empty set
                            outIntervals.clear();
                            return;
                        }
                    } else {
                        // This is subtract
                        if (parseIntervalFails(outIntervals, strValue)) {
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
            if (firstFuncApplied || divider > 0) {
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
            firstFuncApplied = true;
        }
    }

    private boolean allIntervalsHitOnePartition(PartitionBy.PartitionFloorMethod floorMethod) {
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

    private long getTimestamp(Function dynamicFunction) {
        if (ColumnType.isString(dynamicFunction.getType())) {
            CharSequence value = dynamicFunction.getStr(null);
            try {
                return IntervalUtils.parseFloorPartialTimestamp(value);
            } catch (NumericException e) {
                return Numbers.LONG_NaN;
            }
        }
        return dynamicFunction.getTimestamp(null);
    }

    private boolean isStatic() {
        return dynamicRangeList == null || dynamicRangeList.size() == 0;
    }

    private void negatedNothing(LongList outIntervals, int divider) {
        outIntervals.setPos(divider);
        if (divider == 0) {
            outIntervals.extendAndSet(1, Long.MAX_VALUE);
            outIntervals.extendAndSet(0, Long.MIN_VALUE);
        }
    }

    private boolean parseIntervalFails(LongList outIntervals, CharSequence strValue) {
        if (strValue != null) {
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
}
