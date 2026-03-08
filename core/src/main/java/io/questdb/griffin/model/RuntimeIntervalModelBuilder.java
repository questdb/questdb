/*******************************************************************************
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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

/**
 * Collects the interval during query parsing
 * and records them as 2 types:
 * - static list of intervals as 2 long points [lo, hi] in staticIntervals list
 * - dynamic list of functions.
 * <p>
 * When the first interval involving function is added, all data starts to be encoded in 4 longs in staticPeriods
 * 0: lo (long)
 * 1: hi (long)
 * 2: operation (short), period type (short), adjustment (short), dynamicIndicator (short)
 * 3: period (int), count (int)
 * <p>
 * and the index when it happens stored in pureStaticCount
 */
public class RuntimeIntervalModelBuilder implements Mutable {
    private final ObjList<Function> dynamicRangeList = new ObjList<>();
    private final StringSink sink = new StringSink();
    // All data needed to re-evaluate intervals
    // is stored in 2 lists - ListLong and List of functions
    // ListLongs has STATIC_LONGS_PER_DYNAMIC_INTERVAL entries per 1 dynamic interval
    // and pairs of static intervals in the end
    private final LongList staticIntervals = new LongList();
    private long betweenBoundary = Numbers.LONG_NULL;
    private Function betweenBoundaryFunc;
    private boolean betweenBoundarySet;
    private boolean betweenNegated;
    private CairoConfiguration configuration;
    private boolean intervalApplied = false;
    private int partitionBy;
    private TimestampDriver timestampDriver;

    public RuntimeIntrinsicIntervalModel build() {
        return new RuntimeIntervalModel(
                timestampDriver,
                partitionBy,
                new LongList(staticIntervals),
                new ObjList<>(dynamicRangeList)
        );
    }

    @Override
    public void clear() {
        staticIntervals.clear();
        dynamicRangeList.clear();
        intervalApplied = false;
        clearBetweenParsing();
    }

    public void clearBetweenParsing() {
        betweenBoundarySet = false;
        betweenBoundaryFunc = null;
        betweenBoundary = Numbers.LONG_NULL;
    }

    public boolean hasIntervalFilters() {
        return intervalApplied;
    }

    public void intersect(long lo, Function hi, short adjustment) {
        if (isEmptySet()) {
            return;
        }

        IntervalUtils.encodeInterval(lo, 0, adjustment, IntervalDynamicIndicator.IS_HI_DYNAMIC, IntervalOperation.INTERSECT, staticIntervals);
        dynamicRangeList.add(hi);
        intervalApplied = true;
    }

    public void intersect(Function lo, long hi, short adjustment) {
        if (isEmptySet()) {
            return;
        }

        IntervalUtils.encodeInterval(0, hi, adjustment, IntervalDynamicIndicator.IS_LO_DYNAMIC, IntervalOperation.INTERSECT, staticIntervals);
        dynamicRangeList.add(lo);
        intervalApplied = true;
    }

    public void intersect(long lo, long hi) {
        if (isEmptySet()) {
            return;
        }

        if (dynamicRangeList.size() == 0) {
            staticIntervals.add(lo, hi);
            if (intervalApplied) {
                IntervalUtils.intersectInPlace(staticIntervals, staticIntervals.size() - 2);
            }
        } else {
            IntervalUtils.encodeInterval(lo, hi, IntervalOperation.INTERSECT, staticIntervals);
            dynamicRangeList.add(null);
        }
        intervalApplied = true;
    }

    public void intersectEmpty() {
        clear();
        intervalApplied = true;
    }

    public void intersectIntervals(CharSequence seq, int lo, int lim, int position) throws SqlException {
        if (isEmptySet()) {
            return;
        }

        // Date variable expressions ($now, $today, etc.) must be evaluated dynamically
        // so that cached queries always use the current time.
        // compileTickExpr() validates the expression at compile time and returns
        // a CompiledTickExpression that re-evaluates on each query execution.
        if (containsDateVariable(seq, lo, lim)) {
            CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                    timestampDriver, configuration, seq, lo, lim, position);
            intersectCompiledTickExpr(compiled);
            return;
        }

        int size = staticIntervals.size();
        boolean noDynamicIntervals = dynamicRangeList.size() == 0;
        IntervalUtils.parseTickExpr(timestampDriver, configuration, seq, lo, lim, position, staticIntervals, IntervalOperation.INTERSECT, sink, noDynamicIntervals);
        if (noDynamicIntervals) {
            if (intervalApplied) {
                IntervalUtils.intersectInPlace(staticIntervals, size);
            }
        } else {
            // Dynamic mode: each interval is encoded as 4 longs, add one null per interval
            int intervalsAdded = (staticIntervals.size() - size) / IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL;
            for (int i = 0; i < intervalsAdded; i++) {
                dynamicRangeList.add(null);
            }
        }
        intervalApplied = true;
    }

    public void intersectRuntimeIntervals(Function intervalFunction) {
        if (isEmptySet()) {
            return;
        }

        IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.INTERSECT_INTERVALS, staticIntervals);
        dynamicRangeList.add(intervalFunction);
        intervalApplied = true;
    }

    public void intersectRuntimeTimestamp(Function function) {
        if (isEmptySet()) {
            return;
        }

        IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_HI_DYNAMIC, IntervalOperation.INTERSECT, staticIntervals);
        dynamicRangeList.add(function);
        intervalApplied = true;
    }

    public void intersectTimestamp(CharSequence seq, int lo, int lim, int position) throws SqlException {
        if (isEmptySet()) {
            return;
        }

        final int intersectDividerIndex = staticIntervals.size();
        long timestamp;
        try {
            timestamp = timestampDriver.parseFloor(seq, lo, lim);
        } catch (NumericException e) {
            try {
                timestamp = Numbers.parseLong(seq);
            } catch (NumericException e2) {
                for (int i = lo; i < lim; i++) {
                    if (seq.charAt(i) == ';') {
                        throw SqlException.$(position, "not a timestamp, use IN keyword with intervals");
                    }
                }
                throw SqlException.$(position, "invalid timestamp");
            }
        }
        IntervalUtils.encodeInterval(timestamp, timestamp, IntervalOperation.INTERSECT, staticIntervals);

        if (dynamicRangeList.size() == 0) {
            IntervalUtils.applyLastEncodedInterval(timestampDriver, staticIntervals);
            if (intervalApplied) {
                IntervalUtils.intersectInPlace(staticIntervals, intersectDividerIndex);
            }
        } else {
            // else - nothing to do, interval already encoded in staticPeriods as 4 longs
            dynamicRangeList.add(null);
        }
        intervalApplied = true;
    }

    public boolean isEmptySet() {
        return intervalApplied && staticIntervals.size() == 0;
    }

    /**
     * Merges intervals from another RuntimeIntervalModel into this builder.
     * Currently only support static intervals.
     *
     * @param model the RuntimeIntervalModel to merge from
     */
    public void merge(RuntimeIntervalModel model, long loOffset, long hiOffset) {
        if (model == null || isEmptySet()) {
            return;
        }
        ObjList<Function> dynamicRangeList = model.getDynamicRangeList();
        LongList modelIntervals = model.getStaticIntervals();
        if (modelIntervals != null && modelIntervals.size() > 0) {
            int dynamicStart = modelIntervals.size() - (dynamicRangeList != null ? dynamicRangeList.size() * IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL : 0);
            TimestampDriver driver = model.getTimestampDriver();

            for (int i = 0; i < dynamicStart; i += 2) {
                long lo = modelIntervals.getQuick(i);
                if (loOffset == Numbers.LONG_NULL || loOffset == Long.MAX_VALUE) {
                    lo = loOffset;
                } else if (lo != Numbers.LONG_NULL && lo != Long.MAX_VALUE) {
                    lo = timestampDriver.from(lo, driver.getTimestampType());
                    lo -= loOffset;
                }
                long hi = modelIntervals.getQuick(i + 1);
                if (hiOffset == Numbers.LONG_NULL || hiOffset == Long.MAX_VALUE) {
                    hi = hiOffset;
                } else if (hi != Numbers.LONG_NULL && hi != Long.MAX_VALUE) {
                    hi = timestampDriver.from(hi, driver.getTimestampType());
                    hi += hiOffset;
                }
                if (lo == Numbers.LONG_NULL && hi == Long.MAX_VALUE) {
                    return;
                } else {
                    intersect(lo, hi);
                }
            }

            // TODO: Add support for dynamic intervals in merge() method
            // When merging RuntimeIntervalModel with dynamic intervals, need to:
            // Extend STATIC_LONGS_PER_DYNAMIC_INTERVAL to include offset metadata
        }
    }

    public void of(int timestampType, int partitionBy, CairoConfiguration configuration) {
        this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
        this.partitionBy = partitionBy;
        this.configuration = configuration;
    }

    public void setBetweenBoundary(long timestamp) {
        if (!betweenBoundarySet) {
            betweenBoundary = timestamp;
            betweenBoundarySet = true;
        } else {
            if (betweenBoundaryFunc == null) {
                // Constant interval
                long lo = Math.min(timestamp, betweenBoundary);
                long hi = Math.max(timestamp, betweenBoundary);
                if (hi == Numbers.LONG_NULL || lo == Numbers.LONG_NULL) {
                    if (!betweenNegated) {
                        intersectEmpty();
                    }
                    // else {
                    // NOT BETWEEN with NULL
                    // to be consistent with non-designated filtering
                    // do no filtering
                    //  }
                } else {
                    if (!betweenNegated) {
                        intersect(lo, hi);
                    } else {
                        subtractInterval(lo, hi);
                    }
                }
            } else {
                intersectBetweenSemiDynamic(betweenBoundaryFunc, timestamp);
            }
            betweenBoundarySet = false;
        }
    }

    public void setBetweenBoundary(Function timestamp) {
        if (!betweenBoundarySet) {
            betweenBoundaryFunc = timestamp;
            betweenBoundarySet = true;
        } else {
            if (betweenBoundaryFunc == null) {
                intersectBetweenSemiDynamic(timestamp, betweenBoundary);
            } else {
                intersectBetweenDynamic(timestamp, betweenBoundaryFunc);
            }
            betweenBoundarySet = false;
        }
    }

    public void setBetweenNegated(boolean isNegated) {
        betweenNegated = isNegated;
    }

    public void subtractEquals(Function function) {
        if (isEmptySet()) {
            return;
        }

        IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_HI_DYNAMIC, IntervalOperation.SUBTRACT, staticIntervals);
        dynamicRangeList.add(function);
        intervalApplied = true;
    }

    public void subtractInterval(long lo, long hi) {
        if (isEmptySet()) {
            return;
        }

        if (dynamicRangeList.size() == 0) {
            int size = staticIntervals.size();
            staticIntervals.add(lo, hi);
            IntervalUtils.invert(staticIntervals, size);
            if (intervalApplied) {
                IntervalUtils.intersectInPlace(staticIntervals, size);
            }
        } else {
            IntervalUtils.encodeInterval(lo, hi, IntervalOperation.SUBTRACT, staticIntervals);
            dynamicRangeList.add(null);
        }
        intervalApplied = true;
    }

    public void subtractIntervals(CharSequence seq, int lo, int lim, int position) throws SqlException {
        if (isEmptySet()) {
            return;
        }

        // Date variable expressions ($now, $today, etc.) must be evaluated dynamically
        // so that cached queries always use the current time.
        if (containsDateVariable(seq, lo, lim)) {
            CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                    timestampDriver, configuration, seq, lo, lim, position);
            subtractCompiledTickExpr(compiled);
            return;
        }

        int size = staticIntervals.size();
        boolean noDynamicIntervals = dynamicRangeList.size() == 0;
        IntervalUtils.parseTickExpr(timestampDriver, configuration, seq, lo, lim, position, staticIntervals, IntervalOperation.SUBTRACT, sink, noDynamicIntervals);
        if (noDynamicIntervals) {
            IntervalUtils.invert(staticIntervals, size);
            if (intervalApplied) {
                IntervalUtils.intersectInPlace(staticIntervals, size);
            }
        } else {
            // Dynamic mode: each interval is encoded as 4 longs, add one null per interval
            int intervalsAdded = (staticIntervals.size() - size) / IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL;
            for (int i = 0; i < intervalsAdded; i++) {
                dynamicRangeList.add(null);
            }
        }
        intervalApplied = true;
    }

    public void subtractRuntimeIntervals(Function intervalFunction) {
        if (isEmptySet()) {
            return;
        }

        IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.SUBTRACT_INTERVALS, staticIntervals);
        dynamicRangeList.add(intervalFunction);
        intervalApplied = true;
    }

    public void union(long lo, long hi) {
        if (isEmptySet()) {
            return;
        }

        if (dynamicRangeList.size() == 0) {
            staticIntervals.add(lo, hi);
            if (intervalApplied) {
                IntervalUtils.unionInPlace(staticIntervals, staticIntervals.size() - 2);
            }
        } else {
            IntervalUtils.encodeInterval(lo, hi, IntervalOperation.UNION, staticIntervals);
            dynamicRangeList.add(null);
        }
        intervalApplied = true;
    }

    public void unionIntervals(CharSequence seq, int lo, int lim, int position) throws SqlException {
        if (isEmptySet()) {
            return;
        }

        // Date variable expressions ($now, $today, etc.) must be evaluated dynamically
        // so that cached queries always use the current time.
        if (containsDateVariable(seq, lo, lim)) {
            CompiledTickExpression compiled = IntervalUtils.compileTickExpr(
                    timestampDriver, configuration, seq, lo, lim, position);
            unionCompiledTickExpr(compiled);
            return;
        }

        // Parse and expand the interval string (may produce multiple pairs for periodic intervals)
        int size = staticIntervals.size();
        boolean noDynamicIntervals = dynamicRangeList.size() == 0;
        IntervalUtils.parseTickExpr(timestampDriver, configuration, seq, lo, lim, position, staticIntervals, IntervalOperation.UNION, sink, noDynamicIntervals);
        if (noDynamicIntervals) {
            if (intervalApplied) {
                IntervalUtils.unionInPlace(staticIntervals, size);
            }
        } else {
            // Dynamic mode: each interval is encoded as 4 longs, add one null per interval
            int intervalsAdded = (staticIntervals.size() - size) / IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL;
            for (int i = 0; i < intervalsAdded; i++) {
                dynamicRangeList.add(null);
            }
        }
        intervalApplied = true;
    }

    public void unionRuntimeTimestamp(Function function) {
        if (isEmptySet()) {
            return;
        }

        IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_HI_DYNAMIC, IntervalOperation.UNION, staticIntervals);
        dynamicRangeList.add(function);
        intervalApplied = true;
    }

    /**
     * Applies the add method with overflow checking.
     * Throws SqlException if the addition would cause timestamp overflow.
     */
    private static long addWithOverflowCheck(TimestampDriver.TimestampAddMethod addMethod, long timestamp, int offset) throws SqlException {
        // For zero offset, no change needed
        if (offset == 0) {
            return timestamp;
        }

        long result = addMethod.add(timestamp, offset);

        // Detect overflow: if offset is positive but result is less than original,
        // or if offset is negative but result is greater than original, overflow occurred.
        if (offset > 0 && result < timestamp) {
            throw SqlException.position(0)
                    .put("timestamp overflow: applying offset ")
                    .put(offset)
                    .put(" to timestamp would exceed maximum value");
        } else if (offset < 0 && result > timestamp) {
            throw SqlException.position(0)
                    .put("timestamp overflow: applying offset ")
                    .put(offset)
                    .put(" to timestamp would exceed minimum value");
        }

        return result;
    }

    private static boolean containsDateVariable(CharSequence seq, int lo, int lim) {
        for (int i = lo; i < lim - 1; i++) {
            if (seq.charAt(i) == '$' && DateExpressionEvaluator.isDateVariable(seq, i, lim)) {
                return true;
            }
        }
        return false;
    }

    private void intersectBetweenDynamic(Function funcValue1, Function funcValue2) {
        if (isEmptySet()) {
            return;
        }

        short operation = betweenNegated ? IntervalOperation.SUBTRACT_BETWEEN : IntervalOperation.INTERSECT_BETWEEN;
        IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_SEPARATE_DYNAMIC, operation, staticIntervals);
        IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_SEPARATE_DYNAMIC, operation, staticIntervals);
        dynamicRangeList.add(funcValue1);
        dynamicRangeList.add(funcValue2);
        intervalApplied = true;
    }

    private void intersectBetweenSemiDynamic(Function funcValue, long constValue) {
        if (constValue == Numbers.LONG_NULL) {
            if (!betweenNegated) {
                intersectEmpty();
            }
            // else {
            // NOT BETWEEN with NULL
            // to be consistent with non-designated filtering
            // do no filtering
            // }
            return;
        }

        if (isEmptySet()) {
            return;
        }

        short operation = betweenNegated ? IntervalOperation.SUBTRACT_BETWEEN : IntervalOperation.INTERSECT_BETWEEN;
        IntervalUtils.encodeInterval(constValue, 0, (short) 0, IntervalDynamicIndicator.IS_HI_DYNAMIC, operation, staticIntervals);
        dynamicRangeList.add(funcValue);
        intervalApplied = true;
    }

    private void intersectCompiledTickExpr(CompiledTickExpression expr) {
        if (isEmptySet()) {
            return;
        }
        IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.INTERSECT_INTERVALS, staticIntervals);
        dynamicRangeList.add(expr);
        intervalApplied = true;
    }

    private void subtractCompiledTickExpr(CompiledTickExpression expr) {
        if (isEmptySet()) {
            return;
        }
        IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.SUBTRACT_INTERVALS, staticIntervals);
        dynamicRangeList.add(expr);
        intervalApplied = true;
    }

    private void unionCompiledTickExpr(CompiledTickExpression expr) {
        if (isEmptySet()) {
            return;
        }
        IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.UNION, staticIntervals);
        dynamicRangeList.add(expr);
        intervalApplied = true;
    }

    /**
     * Merges intervals from another builder with calendar-aware offset adjustment.
     * This avoids allocating an intermediate RuntimeIntervalModel.
     *
     * @param other     the builder to merge from
     * @param addMethod the timestamp add method (from TimestampDriver)
     * @param offset    the offset value to apply
     * @throws SqlException if applying the offset would cause timestamp overflow
     */
    void mergeWithAddMethod(RuntimeIntervalModelBuilder other, TimestampDriver.TimestampAddMethod addMethod, int offset) throws SqlException {
        if (other == null || isEmptySet() || addMethod == null || !other.intervalApplied) {
            return;
        }
        LongList otherIntervals = other.staticIntervals;
        if (otherIntervals.size() > 0) {
            int dynamicStart = otherIntervals.size() - other.dynamicRangeList.size() * IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL;
            TimestampDriver otherDriver = other.timestampDriver;

            for (int i = 0; i < dynamicStart; i += 2) {
                long lo = otherIntervals.getQuick(i);
                if (lo != Numbers.LONG_NULL && lo != Long.MAX_VALUE) {
                    lo = timestampDriver.from(lo, otherDriver.getTimestampType());
                    lo = addWithOverflowCheck(addMethod, lo, offset);
                }
                long hi = otherIntervals.getQuick(i + 1);
                if (hi != Numbers.LONG_NULL && hi != Long.MAX_VALUE) {
                    hi = timestampDriver.from(hi, otherDriver.getTimestampType());
                    hi = addWithOverflowCheck(addMethod, hi, offset);
                }
                if (lo == Numbers.LONG_NULL && hi == Long.MAX_VALUE) {
                    return;
                } else {
                    intersect(lo, hi);
                }
            }
        }
    }
}
