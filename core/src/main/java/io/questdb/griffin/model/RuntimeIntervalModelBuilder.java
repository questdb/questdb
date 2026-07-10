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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

/**
 * Collects intervals during query parsing and records them in two phases within the shared
 * staticIntervals list:
 * <p>
 * While dynamicRangeList is empty, intervals are stored as plain [lo, hi] long pairs and
 * eagerly combined in place according to the pending operation (intersected, unioned, or
 * subtracted).
 * <p>
 * Once the first interval involving a function is added, every subsequent entry is encoded
 * as 4 longs appended to staticIntervals:
 * 0: lo (long)
 * 1: hi (long)
 * 2: operation (short), period type (short), adjustment (short), dynamicIndicator (short)
 * 3: period (int), count (int)
 * <p>
 * Each encoded entry has a parallel slot in dynamicRangeList (the dynamic Function, or null
 * for an encoded static interval) and in dynamicRangePositionList (the expression position
 * for error reporting), so the encoded suffix of staticIntervals always spans
 * dynamicRangeList.size() * 4 longs and needs no separate boundary index.
 */
public class RuntimeIntervalModelBuilder implements Mutable {
    private final ObjList<Function> dynamicRangeList = new ObjList<>();
    // parse positions of the dynamic range functions, parallel to dynamicRangeList;
    // used to point error messages at the offending expression in the query text
    private final IntList dynamicRangePositionList = new IntList();
    private final StringSink sink = new StringSink();
    // All data needed to re-evaluate intervals is stored in 2 lists - a LongList and a list of
    // functions. The LongList starts with plain [lo, hi] static interval pairs and ends with
    // STATIC_LONGS_PER_DYNAMIC_INTERVAL encoded entries per dynamic interval (see the class doc)
    private final LongList staticIntervals = new LongList();
    private long betweenBoundary = Numbers.LONG_NULL;
    private Function betweenBoundaryFunc;
    private int betweenBoundaryFuncPosition;
    private boolean betweenBoundarySet;
    private boolean betweenNegated;
    private CairoConfiguration configuration;
    private boolean intervalApplied = false;
    private boolean isOwnershipTransferred;
    private int partitionBy;
    private TimestampDriver timestampDriver;

    public RuntimeIntrinsicIntervalModel build() {
        // Construct the model before committing the ownership transfer: if any of the copy
        // allocations or the constructor throws, the functions stay owned by this builder and
        // the next clear() closes them instead of dropping the references.
        final RuntimeIntervalModel model = new RuntimeIntervalModel(
                timestampDriver,
                partitionBy,
                new LongList(staticIntervals),
                new ObjList<>(dynamicRangeList),
                new IntList(dynamicRangePositionList)
        );
        isOwnershipTransferred = true;
        return model;
    }

    @Override
    public void clear() {
        if (isOwnershipTransferred) {
            // build() handed the dynamic functions to a RuntimeIntervalModel, which now owns them.
            // Run clearBetweenParsing() while dynamicRangeList still holds the adopted functions,
            // so it does not mistake an adopted boundary function for a pending one and close it.
            isOwnershipTransferred = false;
            clearBetweenParsing();
            staticIntervals.clear();
            dynamicRangeList.clear();
            dynamicRangePositionList.clear();
            intervalApplied = false;
        } else {
            // no build(): the accumulated functions are orphaned, free them here
            freeAndClear();
        }
    }

    /**
     * Frees Functions accumulated in dynamicRangeList before clearing. Use only on rollback paths
     * where ownership has not been transferred to a RuntimeIntervalModel via {@link #build()};
     * otherwise this double-frees Functions still owned by the built model.
     */
    public void freeAndClear() {
        isOwnershipTransferred = false;
        // Run clearBetweenParsing() while dynamicRangeList still holds the adopted functions: it
        // closes a pending boundary function exactly once and leaves adopted ones to the list free
        // below.
        clearBetweenParsing();
        Misc.freeObjListAndClear(dynamicRangeList);
        dynamicRangePositionList.clear();
        staticIntervals.clear();
        intervalApplied = false;
    }

    /**
     * Rolls back an unfinished BETWEEN extraction. WhereClauseParser calls this after every
     * BETWEEN analysis; when the second endpoint failed to become an intrinsic, the first dynamic
     * endpoint is still pending in betweenBoundaryFunc, and this method owns closing it. An
     * endpoint already adopted into dynamicRangeList stays open - the list (or the model built
     * from it) owns it.
     */
    public void clearBetweenParsing() {
        if (betweenBoundaryFunc != null) {
            // setBetweenBoundary drops the rollback reference immediately after a handoff
            // commits, and the handoff itself is atomic (see reserveEncodedIntervals), so a
            // non-null field always denotes a pending, not-yet-adopted function that this
            // rollback owns. Closing it directly avoids an O(n) list scan per rollback; the
            // assert keeps the invariant checked in tests.
            assert dynamicRangeList.indexOf(betweenBoundaryFunc) < 0;
            betweenBoundaryFunc.close();
        }
        betweenBoundarySet = false;
        betweenBoundaryFunc = null;
        betweenBoundaryFuncPosition = 0;
        betweenBoundary = Numbers.LONG_NULL;
    }

    public boolean hasIntervalFilters() {
        return intervalApplied;
    }

    public void intersect(long lo, Function hi, short adjustment, int functionPosition) {
        if (isEmptySet()) {
            // the model is already an empty set, but this builder owns the incoming function
            Misc.free(hi);
            return;
        }

        IntervalUtils.encodeInterval(lo, 0, adjustment, IntervalDynamicIndicator.IS_HI_DYNAMIC, IntervalOperation.INTERSECT, staticIntervals);
        addDynamicFunction(hi, functionPosition);
        intervalApplied = true;
    }

    public void intersect(Function lo, long hi, short adjustment, int functionPosition) {
        if (isEmptySet()) {
            // the model is already an empty set, but this builder owns the incoming function
            Misc.free(lo);
            return;
        }

        IntervalUtils.encodeInterval(0, hi, adjustment, IntervalDynamicIndicator.IS_LO_DYNAMIC, IntervalOperation.INTERSECT, staticIntervals);
        addDynamicFunction(lo, functionPosition);
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
            addDynamicFunction(null, 0);
        }
        intervalApplied = true;
    }

    public void intersectEmpty() {
        // free the runtime functions gathered so far; ownership has not been transferred via build()
        freeAndClear();
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
                addDynamicFunction(null, 0);
            }
        }
        intervalApplied = true;
    }

    public void intersectMonotonicTimestamp(TimestampMonotonicInverter inverter) {
        if (isEmptySet()) {
            Misc.free(inverter);
            return;
        }

        IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.INTERSECT_INTERVALS, staticIntervals);
        addDynamicFunction(inverter, 0);
        intervalApplied = true;
    }

    public void intersectRuntimeIntervals(Function intervalFunction, int functionPosition) {
        if (isEmptySet()) {
            // the model is already an empty set, but this builder owns the incoming function
            Misc.free(intervalFunction);
            return;
        }

        IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.INTERSECT_INTERVALS, staticIntervals);
        addDynamicFunction(intervalFunction, functionPosition);
        intervalApplied = true;
    }

    public void intersectRuntimeTimestamp(Function function, int functionPosition) {
        if (isEmptySet()) {
            // the model is already an empty set, but this builder owns the incoming function
            Misc.free(function);
            return;
        }

        IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_HI_DYNAMIC, IntervalOperation.INTERSECT, staticIntervals);
        addDynamicFunction(function, functionPosition);
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
            // else - nothing to do, interval already encoded in staticIntervals as 4 longs
            addDynamicFunction(null, 0);
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
                // The callee either fully adopts/frees the pending endpoint or throws without
                // touching it, so the rollback reference is dropped only after the handoff
                // commits. On a throw, clearBetweenParsing() still owns and closes the endpoint.
                intersectBetweenSemiDynamic(betweenBoundaryFunc, betweenBoundaryFuncPosition, timestamp);
                betweenBoundaryFunc = null;
            }
            betweenBoundarySet = false;
        }
    }

    public void setBetweenBoundary(Function timestamp, int functionPosition) {
        if (!betweenBoundarySet) {
            betweenBoundaryFunc = timestamp;
            betweenBoundaryFuncPosition = functionPosition;
            betweenBoundarySet = true;
        } else {
            if (betweenBoundaryFunc == null) {
                intersectBetweenSemiDynamic(timestamp, functionPosition, betweenBoundary);
            } else {
                // The callee either fully adopts/frees both endpoints or throws without touching
                // either, so the rollback reference is dropped only after the handoff commits.
                // On a throw, the caller still owns the incoming function and
                // clearBetweenParsing() still owns and closes the pending one.
                intersectBetweenDynamic(timestamp, functionPosition, betweenBoundaryFunc, betweenBoundaryFuncPosition);
                betweenBoundaryFunc = null;
            }
            betweenBoundarySet = false;
        }
    }

    public void setBetweenNegated(boolean isNegated) {
        betweenNegated = isNegated;
    }

    public void subtractEquals(Function function, int functionPosition) {
        if (isEmptySet()) {
            // the model is already an empty set, but this builder owns the incoming function
            Misc.free(function);
            return;
        }

        IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_HI_DYNAMIC, IntervalOperation.SUBTRACT, staticIntervals);
        addDynamicFunction(function, functionPosition);
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
            addDynamicFunction(null, 0);
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
                addDynamicFunction(null, 0);
            }
        }
        intervalApplied = true;
    }

    public void subtractRuntimeIntervals(Function intervalFunction, int functionPosition) {
        if (isEmptySet()) {
            // the model is already an empty set, but this builder owns the incoming function
            Misc.free(intervalFunction);
            return;
        }

        IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.SUBTRACT_INTERVALS, staticIntervals);
        addDynamicFunction(intervalFunction, functionPosition);
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
            addDynamicFunction(null, 0);
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
                addDynamicFunction(null, 0);
            }
        }
        intervalApplied = true;
    }

    public void unionRuntimeTimestamp(Function function, int functionPosition) {
        if (isEmptySet()) {
            // the model is already an empty set, but this builder owns the incoming function
            Misc.free(function);
            return;
        }

        IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_HI_DYNAMIC, IntervalOperation.UNION, staticIntervals);
        addDynamicFunction(function, functionPosition);
        intervalApplied = true;
    }

    /**
     * Reserves capacity for {@code intervalCount} encoded intervals in staticIntervals and for
     * their parallel slots in dynamicRangeList and dynamicRangePositionList before any of them
     * is mutated. Growth is the only failure mode of the appends that follow, so reserving up
     * front makes a multi-entry append effectively atomic: a failure here leaves the lists
     * untouched and aligned, and every involved Function with its previous owner. Overridable
     * so tests can inject an allocation failure at the single fallible point of the append.
     */
    protected void reserveEncodedIntervals(int intervalCount) {
        staticIntervals.checkCapacity(staticIntervals.size() + intervalCount * IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL);
        dynamicRangeList.checkCapacity(dynamicRangeList.size() + intervalCount);
        dynamicRangePositionList.checkCapacity(dynamicRangePositionList.size() + intervalCount);
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

    private void addDynamicFunction(Function function, int functionPosition) {
        // Grow both parallel lists before adopting: growth is the only failure mode of add(),
        // so pre-sizing makes the two adds effectively atomic. A growth failure then leaves the
        // incoming function unadopted (still owned by the caller) and the lists aligned, instead
        // of stranding a half-adopted function or misaligning the parallel position list.
        dynamicRangeList.checkCapacity(dynamicRangeList.size() + 1);
        dynamicRangePositionList.checkCapacity(dynamicRangePositionList.size() + 1);
        dynamicRangeList.add(function);
        dynamicRangePositionList.add(functionPosition);
    }

    private static boolean containsDateVariable(CharSequence seq, int lo, int lim) {
        for (int i = lo; i < lim - 1; i++) {
            if (seq.charAt(i) == '$' && DateExpressionEvaluator.isDateVariable(seq, i, lim)) {
                return true;
            }
        }
        return false;
    }

    private void intersectBetweenDynamic(Function funcValue1, int funcPosition1, Function funcValue2, int funcPosition2) {
        if (isEmptySet()) {
            // the model is already an empty set, but this builder owns both incoming functions
            Misc.free(funcValue1);
            Misc.free(funcValue2);
            return;
        }

        // Reserve capacity for the whole operation before mutating any list or adopting either
        // function: a growth failure here leaves both functions with their previous owners and
        // the lists untouched, instead of adopting one endpoint (double-closed by the caller and
        // this builder) while stranding the other.
        reserveEncodedIntervals(2);

        short operation = betweenNegated ? IntervalOperation.SUBTRACT_BETWEEN : IntervalOperation.INTERSECT_BETWEEN;
        IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_SEPARATE_DYNAMIC, operation, staticIntervals);
        IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_SEPARATE_DYNAMIC, operation, staticIntervals);
        addDynamicFunction(funcValue1, funcPosition1);
        addDynamicFunction(funcValue2, funcPosition2);
        intervalApplied = true;
    }

    private void intersectBetweenSemiDynamic(Function funcValue, int funcPosition, long constValue) {
        if (constValue == Numbers.LONG_NULL) {
            // The caller drops its rollback reference only after this method returns, so the
            // incoming function may still be referenced by betweenBoundaryFunc. Detach it before
            // emptying the model: the freeAndClear() inside intersectEmpty() must not close it
            // ahead of the single Misc.free() below.
            if (betweenBoundaryFunc == funcValue) {
                betweenBoundaryFunc = null;
            }
            if (!betweenNegated) {
                intersectEmpty();
            }
            // else {
            // NOT BETWEEN with NULL
            // to be consistent with non-designated filtering
            // do no filtering
            // }
            // either way the interval never encodes, so this builder owns the incoming function
            Misc.free(funcValue);
            return;
        }

        if (isEmptySet()) {
            // the model is already an empty set, but this builder owns the incoming function
            Misc.free(funcValue);
            return;
        }

        // Reserve capacity for the whole operation before mutating any list or adopting the
        // function: a growth failure here leaves the function with its previous owner and the
        // lists untouched and aligned.
        reserveEncodedIntervals(1);

        short operation = betweenNegated ? IntervalOperation.SUBTRACT_BETWEEN : IntervalOperation.INTERSECT_BETWEEN;
        IntervalUtils.encodeInterval(constValue, 0, (short) 0, IntervalDynamicIndicator.IS_HI_DYNAMIC, operation, staticIntervals);
        addDynamicFunction(funcValue, funcPosition);
        intervalApplied = true;
    }

    private void intersectCompiledTickExpr(CompiledTickExpression expr) {
        if (isEmptySet()) {
            return;
        }
        IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.INTERSECT_INTERVALS, staticIntervals);
        addDynamicFunction(expr, 0);
        intervalApplied = true;
    }

    private void subtractCompiledTickExpr(CompiledTickExpression expr) {
        if (isEmptySet()) {
            return;
        }
        IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.SUBTRACT_INTERVALS, staticIntervals);
        addDynamicFunction(expr, 0);
        intervalApplied = true;
    }

    private void unionCompiledTickExpr(CompiledTickExpression expr) {
        if (isEmptySet()) {
            return;
        }
        IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.UNION, staticIntervals);
        addDynamicFunction(expr, 0);
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
