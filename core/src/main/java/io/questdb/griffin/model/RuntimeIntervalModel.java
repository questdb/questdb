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

package io.questdb.griffin.model;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.ScalarSubQueryUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

import static io.questdb.griffin.model.IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL;

public class RuntimeIntervalModel implements RuntimeIntrinsicIntervalModel {
    private static final Log LOG = LogFactory.getLog(RuntimeIntervalModel.class);
    // Parse positions of cursor functions, in cursor encounter order. Only cursor scalar functions
    // consume these positions when reporting a multi-row error.
    private final IntList cursorFunctionPositions;
    private final ObjList<Function> dynamicRangeList;
    // These 2 are incoming model
    private final LongList intervals;
    private final int partitionBy;
    private final StringSink sink = new StringSink();
    private final TimestampDriver timestampDriver;
    // This used to assemble the result
    private LongList outIntervals;

    public RuntimeIntervalModel(TimestampDriver timestampDriver, int partitionBy, LongList intervals) {
        this(timestampDriver, partitionBy, intervals, null, null);
    }

    public RuntimeIntervalModel(TimestampDriver timestampDriver, int partitionBy, LongList staticIntervals, ObjList<Function> dynamicRangeList) {
        this(timestampDriver, partitionBy, staticIntervals, dynamicRangeList, null);
    }

    public RuntimeIntervalModel(
            TimestampDriver timestampDriver,
            int partitionBy,
            LongList staticIntervals,
            ObjList<Function> dynamicRangeList,
            IntList cursorFunctionPositions
    ) {
        this.intervals = staticIntervals;
        this.cursorFunctionPositions = cursorFunctionPositions;
        this.dynamicRangeList = dynamicRangeList;
        this.timestampDriver = timestampDriver;
        this.partitionBy = partitionBy;
    }

    @Override
    public boolean allIntervalsHitOnePartition() {
        return !PartitionBy.isPartitioned(partitionBy) || allIntervalsHitOnePartition(timestampDriver.getPartitionFloorMethod(partitionBy));
    }

    @Override
    public LongList calculateIntervals(SqlExecutionContext sqlExecutionContext) throws SqlException {
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
        int oldIntervalType = sqlExecutionContext.getIntervalFunctionType();
        try {
            sqlExecutionContext.setIntervalFunctionType(IntervalUtils.getIntervalType(timestampDriver.getTimestampType()));
            addEvaluateDynamicIntervals(outIntervals, sqlExecutionContext);
        } finally {
            sqlExecutionContext.setIntervalFunctionType(oldIntervalType);
        }

        return outIntervals;
    }

    @Override
    public void close() {
        CairoException.rethrowCleanupFailure(Misc.freeObjListBestEffort(null, dynamicRangeList));
    }

    public ObjList<Function> getDynamicRangeList() {
        return dynamicRangeList;
    }

    public LongList getStaticIntervals() {
        return intervals;
    }

    @Override
    public TimestampDriver getTimestampDriver() {
        return timestampDriver;
    }

    @Override
    public boolean isStatic() {
        return dynamicRangeList == null || dynamicRangeList.size() == 0;
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
                    valTs(sink, timestampDriver, intervals.getQuick(i));
                    sink.val("\",\"");
                    valTs(sink, timestampDriver, intervals.getQuick(i + 1));
                    sink.val("\")");
                }
            } catch (SqlException e) {
                LOG.error().$("Can't calculate intervals: ").$safe(e.getFlyweightMessage()).$();
            }
            sink.val(']');
        }
    }

    private static void valTs(PlanSink sink, TimestampDriver driver, long l) {
        if (l == Numbers.LONG_NULL) {
            sink.val("MIN");
        } else if (l == Long.MAX_VALUE) {
            sink.val("MAX");
        } else {
            sink.valISODate(driver, l);
        }
    }

    private void addEvaluateDynamicIntervals(LongList outIntervals, SqlExecutionContext sqlExecutionContext) throws SqlException {
        int size = intervals.size();
        int dynamicStart = size - dynamicRangeList.size() * STATIC_LONGS_PER_DYNAMIC_INTERVAL;
        int cursorFunctionIndex = 0;
        int dynamicIndex = 0;
        boolean firstFuncApplied = false;

        for (int i = dynamicStart; i < size; i += STATIC_LONGS_PER_DYNAMIC_INTERVAL) {
            Function dynamicFunction = dynamicRangeList.getQuick(dynamicIndex);
            dynamicIndex++;
            short operation = IntervalUtils.getEncodedOperation(intervals, i);
            boolean negated = operation > IntervalOperation.NEGATED_BORDERLINE;
            int divider = outIntervals.size();

            // Get day filter mask (stored in high byte of periodCount)
            int dayFilterMask = IntervalUtils.decodeDayFilterMask(intervals, i);

            switch (dynamicFunction) {
                case null -> {
                    if (operation == IntervalOperation.INTERSECT_INTERVALS) {
                        // A static interval union uses one encoded slot per range. The first slot
                        // carries the range count so the evaluator can append the complete union
                        // before applying one atomic intersection with the preceding expression.
                        final int intervalCount = IntervalUtils.decodePeriod(intervals, i);
                        assert intervalCount > 0;
                        assert i + intervalCount * STATIC_LONGS_PER_DYNAMIC_INTERVAL <= size;
                        for (int k = 0; k < intervalCount; k++) {
                            final int intervalIndex = i + k * STATIC_LONGS_PER_DYNAMIC_INTERVAL;
                            outIntervals.add(
                                    IntervalUtils.decodeIntervalLo(intervals, intervalIndex),
                                    IntervalUtils.decodeIntervalHi(intervals, intervalIndex)
                            );
                        }
                        i += (intervalCount - 1) * STATIC_LONGS_PER_DYNAMIC_INTERVAL;
                        dynamicIndex += intervalCount - 1;
                    } else {
                        // copy 4 longs to output and apply the operation
                        outIntervals.add(intervals, i, i + STATIC_LONGS_PER_DYNAMIC_INTERVAL);
                        IntervalUtils.applyLastEncodedInterval(timestampDriver, outIntervals);
                        // Apply day filter if specified
                        if (dayFilterMask != 0) {
                            // Check if the interval's lo timestamp matches the day filter
                            long lo = outIntervals.getQuick(divider);
                            int dayOfWeek = timestampDriver.getDayOfWeek(lo);
                            if ((dayFilterMask & (1 << (dayOfWeek - 1))) == 0) {
                                // Day doesn't match filter - remove this interval
                                outIntervals.setPos(divider);
                                continue;
                            }
                        }
                    }
                }
                case CompiledTickExpression compiled -> {
                    // Compiled tick expression with date variables ($now, $today, etc.)
                    // Re-evaluates the expression with the current "now" timestamp
                    compiled.init(null, sqlExecutionContext);
                    compiled.evaluate(outIntervals);
                    if (operation == IntervalOperation.SUBTRACT_INTERVALS) {
                        IntervalUtils.invert(outIntervals, divider);
                    }
                }
                case TimestampMonotonicInverter inverter -> {
                    inverter.init(null, sqlExecutionContext);
                    inverter.evaluate(outIntervals);
                }
                default -> {
                    long lo = IntervalUtils.decodeIntervalLo(intervals, i);
                    long hi = IntervalUtils.decodeIntervalHi(intervals, i);
                    short adjustment = IntervalUtils.getEncodedAdjustment(intervals, i);
                    short dynamicHiLo = IntervalUtils.getEncodedDynamicIndicator(intervals, i);

                    dynamicFunction.init(null, sqlExecutionContext);
                    final int functionType = dynamicFunction.getType();

                    if (operation != IntervalOperation.INTERSECT_INTERVALS && operation != IntervalOperation.SUBTRACT_INTERVALS) {
                        long dynamicValue = getTimestamp(dynamicFunction, functionType, sqlExecutionContext, cursorFunctionIndex);
                        if (functionType == ColumnType.CURSOR) {
                            cursorFunctionIndex++;
                        }
                        long dynamicValue2 = 0;
                        if (dynamicHiLo == IntervalDynamicIndicator.IS_LO_SEPARATE_DYNAMIC) {
                            // Both ends of BETWEEN are dynamic and different values. Take the next dynamic point.
                            i += STATIC_LONGS_PER_DYNAMIC_INTERVAL;
                            dynamicFunction = dynamicRangeList.getQuick(dynamicIndex);
                            dynamicIndex++;
                            dynamicFunction.init(null, sqlExecutionContext);
                            final int functionType2 = dynamicFunction.getType();
                            dynamicValue2 = hi = getTimestamp(
                                    dynamicFunction,
                                    functionType2,
                                    sqlExecutionContext,
                                    cursorFunctionIndex
                            );
                            if (functionType2 == ColumnType.CURSOR) {
                                cursorFunctionIndex++;
                            }
                            lo = dynamicValue;
                        } else {
                            if ((dynamicHiLo & IntervalDynamicIndicator.IS_HI_DYNAMIC) != 0) {
                                hi = dynamicValue + adjustment;
                            }
                            if ((dynamicHiLo & IntervalDynamicIndicator.IS_LO_DYNAMIC) != 0) {
                                lo = dynamicValue + adjustment;
                            }
                        }

                        if (dynamicValue == Numbers.LONG_NULL || dynamicValue2 == Numbers.LONG_NULL) {
                            // functions evaluated to null
                            if (!negated) {
                                // return an empty set if it's not negated
                                outIntervals.clear();
                                return;
                            } else {
                                // or full set
                                negatedNothing(outIntervals, divider);
                                continue;
                            }
                        }

                        if (adjustment > 0 && dynamicValue == Long.MAX_VALUE) {
                            // a strict bound just past the timestamp domain matches nothing;
                            // the adjustment would wrap around to Long.MIN_VALUE and select every row
                            if (!negated) {
                                outIntervals.clear();
                                return;
                            } else {
                                negatedNothing(outIntervals, divider);
                                continue;
                            }
                        }

                        if (operation == IntervalOperation.INTERSECT_BETWEEN || operation == IntervalOperation.SUBTRACT_BETWEEN) {
                            long tempHi = Math.max(hi, lo);
                            lo = Math.min(hi, lo);
                            hi = tempHi;
                        }

                        // Apply day filter if specified
                        if (dayFilterMask != 0) {
                            int dayOfWeek = timestampDriver.getDayOfWeek(lo);
                            if ((dayFilterMask & (1 << (dayOfWeek - 1))) == 0) {
                                // Day doesn't match filter - skip this interval
                                continue;
                            }
                        }

                        outIntervals.extendAndSet(divider + 1, hi);
                        outIntervals.setQuick(divider, lo);
                        if (divider == 0 && negated) {
                            // Divider == 0 means it's the first interval applied
                            // Invert the interval, since it will not be applied negated to anything
                            IntervalUtils.invert(outIntervals, divider);
                        }
                    } else {
                        if (ColumnType.isInterval(functionType)) {
                            // This is subtraction or intersection with an Interval (not a single timestamp)
                            final Interval interval = timestampDriver.fixInterval(dynamicFunction.getInterval(null), functionType);
                            applyInterval(outIntervals, interval);
                            if (operation == IntervalOperation.SUBTRACT_INTERVALS) {
                                IntervalUtils.invert(outIntervals, divider);
                            }
                        } else {
                            // This is subtraction or intersection with a string interval (not a single timestamp)
                            final CharSequence strInterval = dynamicFunction.getStrA(null);
                            final CairoConfiguration configuration = sqlExecutionContext.getCairoEngine().getConfiguration();
                            if (operation == IntervalOperation.INTERSECT_INTERVALS) {
                                // This is an intersection
                                if (tryParseInterval(outIntervals, strInterval, configuration)) {
                                    // return an empty set
                                    outIntervals.clear();
                                    return;
                                }
                            } else {
                                // This is a subtraction
                                if (tryParseInterval(outIntervals, strInterval, configuration)) {
                                    // full set
                                    negatedNothing(outIntervals, divider);
                                    continue;
                                }
                                IntervalUtils.invert(outIntervals, divider);
                            }
                        }
                    }
                }
            }

            // Do not apply operation (intersection, subtraction).
            // If this is the first element and no pre-calculated static intervals exist.
            if (firstFuncApplied || divider > 0) {
                switch (operation) {
                    case IntervalOperation.INTERSECT, IntervalOperation.INTERSECT_BETWEEN,
                         IntervalOperation.INTERSECT_INTERVALS, IntervalOperation.SUBTRACT_INTERVALS ->
                            IntervalUtils.intersectInPlace(outIntervals, divider);
                    case IntervalOperation.SUBTRACT, IntervalOperation.SUBTRACT_BETWEEN ->
                            IntervalUtils.subtract(outIntervals, divider);
                    case IntervalOperation.UNION ->
                        // Union with previous intervals (used for bracket expansion)
                            IntervalUtils.unionInPlace(outIntervals, divider);
                    default ->
                            throw new UnsupportedOperationException("Interval operation " + operation + " is not supported");
                }
            }
            firstFuncApplied = true;
        }
    }

    private boolean allIntervalsHitOnePartition(TimestampDriver.TimestampFloorMethod floorMethod) {
        if (!isStatic()) {
            return false;
        }
        if (intervals.size() == 0) {
            return true;
        }

        return floorMethod.floor(intervals.getQuick(0)) == floorMethod.floor(intervals.getLast());
    }

    private void applyInterval(LongList outIntervals, Interval interval) {
        IntervalUtils.encodeInterval(interval, IntervalOperation.INTERSECT, outIntervals);
        IntervalUtils.applyLastEncodedInterval(timestampDriver, outIntervals);
    }

    private int getCursorFunctionPosition(int cursorFunctionIndex) {
        return cursorFunctionPositions != null && cursorFunctionIndex < cursorFunctionPositions.size()
                ? cursorFunctionPositions.getQuick(cursorFunctionIndex)
                : 0;
    }

    private long getTimestamp(
            Function dynamicFunction,
            int functionType,
            SqlExecutionContext sqlExecutionContext,
            int cursorFunctionIndex
    ) throws SqlException {
        if (ColumnType.isString(functionType)) {
            final CharSequence value = dynamicFunction.getStrA(null);
            if (value != null) {
                try {
                    return timestampDriver.parseFloorLiteral(value);
                } catch (NumericException e) {
                    return Numbers.LONG_NULL;
                }
            }
            return Numbers.LONG_NULL;
        } else if (functionType == ColumnType.CURSOR) {
            // special case for ts = (<subquery>) and similar cases, where the designated timestamp
            // column routes ts =/</> (select ...) into an interval intrinsic instead of a cursor-
            // comparison factory. A scalar sub-query must still yield at most one row: read the first
            // row, then enforce there is no second one - otherwise an arbitrary first row would be
            // taken silently, diverging from the cursor-comparison factories that reject it.
            final RecordCursorFactory factory = dynamicFunction.getRecordCursorFactory();
            assert factory != null;
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                if (cursor.hasNext()) {
                    final long timestamp = timestampDriver.from(cursor.getRecord().getTimestamp(0), ColumnType.getTimestampType(factory.getMetadata().getColumnType(0)));
                    ScalarSubQueryUtils.assertNoMoreRows(cursor, getCursorFunctionPosition(cursorFunctionIndex));
                    return timestamp;
                } else {
                    return Numbers.LONG_NULL;
                }
            }
        } else {
            return timestampDriver.from(dynamicFunction.getTimestamp(null), ColumnType.getTimestampType(functionType));
        }
    }

    private void negatedNothing(LongList outIntervals, int divider) {
        outIntervals.setPos(divider);
        if (divider == 0) {
            outIntervals.extendAndSet(1, Long.MAX_VALUE);
            outIntervals.extendAndSet(0, Long.MIN_VALUE);
        }
    }

    private boolean tryParseInterval(LongList outIntervals, CharSequence strInterval, CairoConfiguration configuration) {
        if (strInterval != null) {
            try {
                IntervalUtils.parseTickExprAndIntersect(timestampDriver, configuration, strInterval, outIntervals, 0, sink, true);
            } catch (SqlException e) {
                return true;
            }
            return false;
        }
        return true;
    }
}
