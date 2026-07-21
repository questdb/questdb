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
 * for an encoded static interval), so the encoded suffix of staticIntervals always spans
 * dynamicRangeList.size() * 4 longs and needs no separate boundary index. Cursor function parse
 * positions use a separate sparse list, in cursor encounter order, because only cursor scalar
 * functions need a position for error reporting.
 */
public class RuntimeIntervalModelBuilder implements Mutable {
    // Parse positions of cursor functions, in cursor encounter order. Other dynamic intervals do
    // not need an error position, so keeping this list sparse avoids O(D) storage when C is zero.
    private final IntList cursorFunctionPositions = new IntList();
    private final ObjList<Function> dynamicRangeList = new ObjList<>();
    private final LongList parsedIntervals = new LongList();
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
    private boolean isBetweenBoundaryFunctionConsumed;
    private boolean isOwnershipTransferred;
    private int partitionBy;
    private TimestampDriver timestampDriver;

    public RuntimeIntrinsicIntervalModel build() {
        // Construct the model before committing the ownership transfer: if any of the copy
        // allocations or the constructor throws, the functions stay owned by this builder and
        // the next clear() closes them instead of dropping the references.
        final RuntimeIntrinsicIntervalModel model = newModel();
        isOwnershipTransferred = true;
        return model;
    }

    @Override
    public void clear() {
        if (isOwnershipTransferred) {
            // build() handed the dynamic functions to a RuntimeIntervalModel, which now owns them.
            // Only a still-pending BETWEEN endpoint remains builder-owned.
            isOwnershipTransferred = false;
            final Throwable failure = clearBetweenParsing(null);
            staticIntervals.clear();
            cursorFunctionPositions.clear();
            dynamicRangeList.clear();
            intervalApplied = false;
            CairoException.rethrowCleanupFailure(failure);
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
        CairoException.rethrowCleanupFailure(freeAndClearBestEffort());
    }

    /**
     * Rolls back an unfinished BETWEEN extraction. WhereClauseParser calls this after every
     * BETWEEN analysis; when the second endpoint failed to become an intrinsic, the first dynamic
     * endpoint is still pending in betweenBoundaryFunc, and this method owns closing it. An
     * endpoint already adopted into dynamicRangeList stays open - the list (or the model built
     * from it) owns it.
     */
    public void clearBetweenParsing() {
        CairoException.rethrowCleanupFailure(clearBetweenParsing(null));
    }

    /**
     * Primary-aware variant for error paths. It detaches all BETWEEN parsing state before close,
     * then suppresses a close failure onto {@code primary} instead of replacing that failure.
     */
    public Throwable clearBetweenParsing(Throwable primary) {
        final Function pendingFunction = betweenBoundaryFunc;
        assert pendingFunction == null || dynamicRangeList.indexOf(pendingFunction) < 0;
        resetBetweenParsingState();
        return Misc.freeBestEffort(primary, pendingFunction);
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

        try {
            final boolean isCursor = hi.getType() == ColumnType.CURSOR;
            reserveEncodedIntervals(1, isCursor ? 1 : 0);
            IntervalUtils.encodeInterval(lo, 0, adjustment, IntervalDynamicIndicator.IS_HI_DYNAMIC, IntervalOperation.INTERSECT, staticIntervals);
            addDynamicFunction(hi, functionPosition, isCursor);
            intervalApplied = true;
        } catch (Throwable th) {
            CairoException.rethrowCleanupFailure(Misc.freeBestEffort(th, hi));
        }
    }

    public void intersect(Function lo, long hi, short adjustment, int functionPosition) {
        if (isEmptySet()) {
            // the model is already an empty set, but this builder owns the incoming function
            Misc.free(lo);
            return;
        }

        try {
            final boolean isCursor = lo.getType() == ColumnType.CURSOR;
            reserveEncodedIntervals(1, isCursor ? 1 : 0);
            IntervalUtils.encodeInterval(0, hi, adjustment, IntervalDynamicIndicator.IS_LO_DYNAMIC, IntervalOperation.INTERSECT, staticIntervals);
            addDynamicFunction(lo, functionPosition, isCursor);
            intervalApplied = true;
        } catch (Throwable th) {
            CairoException.rethrowCleanupFailure(Misc.freeBestEffort(th, lo));
        }
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
            reserveEncodedIntervals(1, 0);
            IntervalUtils.encodeInterval(lo, hi, IntervalOperation.INTERSECT, staticIntervals);
            addDynamicFunction(null, 0, false);
        }
        intervalApplied = true;
    }

    public void intersectEmpty() {
        // Finish the empty-state transition even when an adopted function throws during cleanup.
        final Throwable failure = freeAndClearBestEffort();
        intervalApplied = true;
        CairoException.rethrowCleanupFailure(failure);
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

        final int size = staticIntervals.size();
        final boolean noDynamicIntervals = dynamicRangeList.size() == 0;
        try {
            parsedIntervals.clear();
            IntervalUtils.parseTickExpr(timestampDriver, configuration, seq, lo, lim, position, parsedIntervals, IntervalOperation.INTERSECT, sink, noDynamicIntervals);
            if (noDynamicIntervals) {
                staticIntervals.add(parsedIntervals);
                if (intervalApplied) {
                    IntervalUtils.intersectInPlace(staticIntervals, size);
                }
            } else {
                appendParsedDynamicIntervals();
            }
            intervalApplied = true;
        } finally {
            parsedIntervals.clear();
        }
    }

    public void intersectMonotonicTimestamp(TimestampMonotonicInverter inverter) {
        if (isEmptySet()) {
            Misc.free(inverter);
            return;
        }

        try {
            reserveEncodedIntervals(1, 0);
            IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.INTERSECT_INTERVALS, staticIntervals);
            addDynamicFunction(inverter, 0, false);
            intervalApplied = true;
        } catch (Throwable th) {
            CairoException.rethrowCleanupFailure(Misc.freeBestEffort(th, inverter));
        }
    }

    public void intersectRuntimeIntervals(Function intervalFunction, int functionPosition) {
        if (isEmptySet()) {
            // the model is already an empty set, but this builder owns the incoming function
            Misc.free(intervalFunction);
            return;
        }

        try {
            final boolean isCursor = intervalFunction.getType() == ColumnType.CURSOR;
            reserveEncodedIntervals(1, isCursor ? 1 : 0);
            IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.INTERSECT_INTERVALS, staticIntervals);
            addDynamicFunction(intervalFunction, functionPosition, isCursor);
            intervalApplied = true;
        } catch (Throwable th) {
            CairoException.rethrowCleanupFailure(Misc.freeBestEffort(th, intervalFunction));
        }
    }

    public void intersectRuntimeTimestamp(Function function, int functionPosition) {
        if (isEmptySet()) {
            // the model is already an empty set, but this builder owns the incoming function
            Misc.free(function);
            return;
        }

        try {
            final boolean isCursor = function.getType() == ColumnType.CURSOR;
            reserveEncodedIntervals(1, isCursor ? 1 : 0);
            IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_HI_DYNAMIC, IntervalOperation.INTERSECT, staticIntervals);
            addDynamicFunction(function, functionPosition, isCursor);
            intervalApplied = true;
        } catch (Throwable th) {
            CairoException.rethrowCleanupFailure(Misc.freeBestEffort(th, function));
        }
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
        if (dynamicRangeList.size() == 0) {
            staticIntervals.checkCapacity(staticIntervals.size() + IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL);
        } else {
            reserveEncodedIntervals(1, 0);
        }
        IntervalUtils.encodeInterval(timestamp, timestamp, IntervalOperation.INTERSECT, staticIntervals);

        if (dynamicRangeList.size() == 0) {
            IntervalUtils.applyLastEncodedInterval(timestampDriver, staticIntervals);
            if (intervalApplied) {
                IntervalUtils.intersectInPlace(staticIntervals, intersectDividerIndex);
            }
        } else {
            // else - nothing to do, interval already encoded in staticIntervals as 4 longs
            addDynamicFunction(null, 0, false);
        }
        intervalApplied = true;
    }

    /**
     * Reports the ownership result of the most recent Function boundary handoff. A caller retains
     * ownership when {@link #setBetweenBoundary(Function, int)} throws with this value false. When
     * this value is true, the builder adopted or closed the function, including terminal cleanup
     * paths whose close operation threw.
     */
    public boolean isBetweenBoundaryFunctionConsumed() {
        return isBetweenBoundaryFunctionConsumed;
    }

    public boolean isEmptySet() {
        return intervalApplied && staticIntervals.size() == 0;
    }

    /**
     * Narrows a WINDOW JOIN slave scan to the union of the master's intervals expanded by the
     * window bounds. This method is best-effort: mixed timestamp precision and unsupported dynamic
     * combinations leave the slave scan less constrained rather than risk omitting rows.
     *
     * @param model    the master interval model
     * @param loOffset the window lower offset, subtracted from each master lower bound;
     *                 {@link Numbers#LONG_NULL} opens the lower side
     * @param hiOffset the window upper offset, added to each master upper bound;
     *                 {@link Long#MAX_VALUE} opens the upper side
     */
    public void merge(RuntimeIntervalModel model, long loOffset, long hiOffset) {
        if (model == null || isEmptySet()) {
            return;
        }

        final LongList modelIntervals = model.getStaticIntervals();
        if (modelIntervals == null || modelIntervals.size() == 0) {
            return;
        }
        final ObjList<Function> modelDynamicRangeList = model.getDynamicRangeList();
        if (modelDynamicRangeList != null && modelDynamicRangeList.size() > 0) {
            return;
        }

        final TimestampDriver modelTimestampDriver = model.getTimestampDriver();
        if (timestampDriver.getTimestampType() != modelTimestampDriver.getTimestampType()) {
            return;
        }
        try {
            parsedIntervals.clear();
            for (int i = 0, n = modelIntervals.size(); i < n; i += 2) {
                final long lo = offsetIntervalLo(modelIntervals.getQuick(i), loOffset, modelTimestampDriver);
                final long hi = offsetIntervalHi(modelIntervals.getQuick(i + 1), hiOffset, modelTimestampDriver);
                if (lo == Numbers.LONG_NULL && hi == Long.MAX_VALUE) {
                    // One expanded master interval covers the full domain, so its union adds no
                    // useful constraint to the slave scan.
                    return;
                }
                if (lo > hi) {
                    continue;
                }

                // Master intervals are sorted. Applying the same monotonic, saturating shifts to
                // every pair preserves that order; coalesce overlaps the expansion introduces.
                if (parsedIntervals.size() > 0 && lo <= parsedIntervals.getLast()) {
                    if (hi > parsedIntervals.getLast()) {
                        parsedIntervals.setQuick(parsedIntervals.size() - 1, hi);
                    }
                } else {
                    parsedIntervals.add(lo, hi);
                }
            }

            if (parsedIntervals.size() == 0) {
                intersectEmpty();
                return;
            }

            if (dynamicRangeList.size() > 0) {
                // The encoded dynamic suffix can intersect one static interval atomically, but it
                // cannot express an atomic intersection with a static union. Keep the existing
                // (wider) slave scan for a multi-range union.
                if (parsedIntervals.size() == 2) {
                    intersect(parsedIntervals.getQuick(0), parsedIntervals.getQuick(1));
                }
                return;
            }

            final int divider = staticIntervals.size();
            staticIntervals.add(parsedIntervals);
            if (intervalApplied) {
                IntervalUtils.intersectInPlace(staticIntervals, divider);
            } else {
                intervalApplied = true;
            }
        } finally {
            parsedIntervals.clear();
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
            return;
        }

        if (betweenBoundaryFunc == null) {
            // No Function ownership changes on this branch, so reset temporary parsing state
            // before an empty-model cleanup can throw.
            final long pendingTimestamp = betweenBoundary;
            resetBetweenParsingState();
            final long lo = Math.min(timestamp, pendingTimestamp);
            final long hi = Math.max(timestamp, pendingTimestamp);
            if (hi == Numbers.LONG_NULL || lo == Numbers.LONG_NULL) {
                if (!betweenNegated) {
                    intersectEmpty();
                }
                // NOT BETWEEN with NULL does no filtering, consistent with row filtering.
            } else if (!betweenNegated) {
                intersect(lo, hi);
            } else {
                subtractInterval(lo, hi);
            }
            return;
        }

        final Function pendingFunction = betweenBoundaryFunc;
        final int pendingFunctionPosition = betweenBoundaryFuncPosition;
        if (timestamp == Numbers.LONG_NULL || isEmptySet()) {
            // This terminal handoff consumes the pending endpoint. Detach it before any adopted
            // cleanup or endpoint close can throw, then complete all cleanup best-effort.
            resetBetweenParsingState();
            Throwable failure = null;
            if (timestamp == Numbers.LONG_NULL && !betweenNegated) {
                failure = freeAndClearBestEffort();
                intervalApplied = true;
            }
            failure = Misc.freeBestEffort(failure, pendingFunction);
            CairoException.rethrowCleanupFailure(failure);
            return;
        }

        // Reservation failure is pre-adoption: keep the pending endpoint attached for rollback.
        intersectBetweenSemiDynamic(pendingFunction, pendingFunctionPosition, timestamp);
        resetBetweenParsingState();
    }

    public void setBetweenBoundary(Function timestamp, int functionPosition) {
        isBetweenBoundaryFunctionConsumed = false;
        if (!betweenBoundarySet) {
            betweenBoundaryFunc = timestamp;
            betweenBoundaryFuncPosition = functionPosition;
            betweenBoundarySet = true;
            isBetweenBoundaryFunctionConsumed = true;
            return;
        }

        if (betweenBoundaryFunc == null) {
            if (betweenBoundary == Numbers.LONG_NULL || isEmptySet()) {
                // The incoming endpoint is consumed before cleanup begins. This flag tells the
                // parser not to close it again if a close operation below throws.
                isBetweenBoundaryFunctionConsumed = true;
                final boolean isNullBoundary = betweenBoundary == Numbers.LONG_NULL;
                resetBetweenParsingState();
                Throwable failure = null;
                if (isNullBoundary && !betweenNegated) {
                    failure = freeAndClearBestEffort();
                    intervalApplied = true;
                }
                failure = Misc.freeBestEffort(failure, timestamp);
                CairoException.rethrowCleanupFailure(failure);
                return;
            }

            // Reservation failure is pre-adoption, so the caller still owns timestamp.
            intersectBetweenSemiDynamic(timestamp, functionPosition, betweenBoundary);
            isBetweenBoundaryFunctionConsumed = true;
            resetBetweenParsingState();
            return;
        }

        final Function pendingFunction = betweenBoundaryFunc;
        final int pendingFunctionPosition = betweenBoundaryFuncPosition;
        if (isEmptySet()) {
            // Consume and detach both endpoints before closing either one. Pending-first order
            // defines deterministic primary/suppression topology.
            isBetweenBoundaryFunctionConsumed = true;
            resetBetweenParsingState();
            Throwable failure = Misc.freeBestEffort(null, pendingFunction);
            failure = Misc.freeBestEffort(failure, timestamp);
            CairoException.rethrowCleanupFailure(failure);
            return;
        }

        // Reservation failure is pre-adoption: the incoming endpoint stays caller-owned and the
        // pending endpoint remains attached for clearBetweenParsing().
        intersectBetweenDynamic(timestamp, functionPosition, pendingFunction, pendingFunctionPosition);
        isBetweenBoundaryFunctionConsumed = true;
        resetBetweenParsingState();
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

        try {
            final boolean isCursor = function.getType() == ColumnType.CURSOR;
            reserveEncodedIntervals(1, isCursor ? 1 : 0);
            IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_HI_DYNAMIC, IntervalOperation.SUBTRACT, staticIntervals);
            addDynamicFunction(function, functionPosition, isCursor);
            intervalApplied = true;
        } catch (Throwable th) {
            CairoException.rethrowCleanupFailure(Misc.freeBestEffort(th, function));
        }
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
            reserveEncodedIntervals(1, 0);
            IntervalUtils.encodeInterval(lo, hi, IntervalOperation.SUBTRACT, staticIntervals);
            addDynamicFunction(null, 0, false);
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

        final int size = staticIntervals.size();
        final boolean noDynamicIntervals = dynamicRangeList.size() == 0;
        try {
            parsedIntervals.clear();
            IntervalUtils.parseTickExpr(timestampDriver, configuration, seq, lo, lim, position, parsedIntervals, IntervalOperation.SUBTRACT, sink, noDynamicIntervals);
            if (noDynamicIntervals) {
                staticIntervals.add(parsedIntervals);
                IntervalUtils.invert(staticIntervals, size);
                if (intervalApplied) {
                    IntervalUtils.intersectInPlace(staticIntervals, size);
                }
            } else {
                appendParsedDynamicIntervals();
            }
            intervalApplied = true;
        } finally {
            parsedIntervals.clear();
        }
    }

    public void subtractRuntimeIntervals(Function intervalFunction, int functionPosition) {
        if (isEmptySet()) {
            // the model is already an empty set, but this builder owns the incoming function
            Misc.free(intervalFunction);
            return;
        }

        try {
            final boolean isCursor = intervalFunction.getType() == ColumnType.CURSOR;
            reserveEncodedIntervals(1, isCursor ? 1 : 0);
            IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.SUBTRACT_INTERVALS, staticIntervals);
            addDynamicFunction(intervalFunction, functionPosition, isCursor);
            intervalApplied = true;
        } catch (Throwable th) {
            CairoException.rethrowCleanupFailure(Misc.freeBestEffort(th, intervalFunction));
        }
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
            reserveEncodedIntervals(1, 0);
            IntervalUtils.encodeInterval(lo, hi, IntervalOperation.UNION, staticIntervals);
            addDynamicFunction(null, 0, false);
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

        // Parse and expand the interval string (may produce multiple pairs for periodic intervals).
        final int size = staticIntervals.size();
        final boolean noDynamicIntervals = dynamicRangeList.size() == 0;
        try {
            parsedIntervals.clear();
            IntervalUtils.parseTickExpr(timestampDriver, configuration, seq, lo, lim, position, parsedIntervals, IntervalOperation.UNION, sink, noDynamicIntervals);
            if (noDynamicIntervals) {
                staticIntervals.add(parsedIntervals);
                if (intervalApplied) {
                    IntervalUtils.unionInPlace(staticIntervals, size);
                }
            } else {
                appendParsedDynamicIntervals();
            }
            intervalApplied = true;
        } finally {
            parsedIntervals.clear();
        }
    }

    public void unionRuntimeTimestamp(Function function, int functionPosition) {
        if (isEmptySet()) {
            // the model is already an empty set, but this builder owns the incoming function
            Misc.free(function);
            return;
        }

        try {
            final boolean isCursor = function.getType() == ColumnType.CURSOR;
            reserveEncodedIntervals(1, isCursor ? 1 : 0);
            IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_HI_DYNAMIC, IntervalOperation.UNION, staticIntervals);
            addDynamicFunction(function, functionPosition, isCursor);
            intervalApplied = true;
        } catch (Throwable th) {
            CairoException.rethrowCleanupFailure(Misc.freeBestEffort(th, function));
        }
    }

    /**
     * Returns null when no cursor function recorded a position: null is the "no cursor bounds"
     * sentinel {@code RuntimeIntervalModel.getCursorFunctionPosition()} already handles, so the
     * common no-cursor case skips allocating a dead empty IntList per built model.
     */
    protected IntList copyCursorFunctionPositions() {
        return cursorFunctionPositions.size() > 0 ? new IntList(cursorFunctionPositions) : null;
    }

    protected ObjList<Function> copyDynamicRangeList() {
        return new ObjList<>(dynamicRangeList);
    }

    protected LongList copyStaticIntervals() {
        return new LongList(staticIntervals);
    }

    protected RuntimeIntrinsicIntervalModel createModel(
            LongList staticIntervals,
            ObjList<Function> dynamicRangeList,
            IntList cursorFunctionPositions
    ) {
        return new RuntimeIntervalModel(
                timestampDriver,
                partitionBy,
                staticIntervals,
                dynamicRangeList,
                cursorFunctionPositions
        );
    }

    /**
     * Copies the accumulated state into a new model instance. Everything fallible in build() -
     * the defensive list copies and the model constructor - lives here, so that ownership of the
     * dynamic functions transfers to the model only after this method returns. The protected copy
     * and creation methods let tests fail each allocation stage deterministically.
     */
    protected RuntimeIntrinsicIntervalModel newModel() {
        final LongList staticIntervals = copyStaticIntervals();
        final ObjList<Function> dynamicRangeList = copyDynamicRangeList();
        final IntList cursorFunctionPositions = copyCursorFunctionPositions();
        return createModel(staticIntervals, dynamicRangeList, cursorFunctionPositions);
    }

    /**
     * Reserves capacity for {@code intervalCount} encoded intervals and their functions, plus
     * {@code cursorFunctionCount} sparse error positions, before any list is mutated. Growth is
     * the only failure mode of the appends that follow, so reserving up front makes a multi-entry
     * append effectively atomic: a failure leaves the lists untouched and every involved Function
     * with its previous owner. Overridable so tests can inject an allocation failure at the single
     * fallible point of the append.
     */
    protected void reserveEncodedIntervals(int intervalCount, int cursorFunctionCount) {
        staticIntervals.checkCapacity(staticIntervals.size() + intervalCount * IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL);
        dynamicRangeList.checkCapacity(dynamicRangeList.size() + intervalCount);
        cursorFunctionPositions.checkCapacity(cursorFunctionPositions.size() + cursorFunctionCount);
    }

    private static long addSaturating(long value, long offset) {
        final long result = value + offset;
        if (((value ^ result) & (offset ^ result)) < 0) {
            return value < 0 ? Numbers.LONG_NULL : Long.MAX_VALUE;
        }
        return result;
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

    private static long subtractSaturating(long value, long offset) {
        final long result = value - offset;
        if (((value ^ offset) & (value ^ result)) < 0) {
            return value < 0 ? Numbers.LONG_NULL : Long.MAX_VALUE;
        }
        return result;
    }

    private void addDynamicFunction(Function function, int functionPosition, boolean isCursor) {
        // Callers reserve all applicable lists and snapshot cursor classification before model
        // mutation, so commit performs no user callbacks or capacity growth.
        dynamicRangeList.add(function);
        if (isCursor) {
            cursorFunctionPositions.add(functionPosition);
        }
    }

    private void appendParsedDynamicIntervals() {
        final int intervalCount = parsedIntervals.size() / IntervalUtils.STATIC_LONGS_PER_DYNAMIC_INTERVAL;
        reserveEncodedIntervals(intervalCount, 0);
        staticIntervals.add(parsedIntervals);
        for (int i = 0; i < intervalCount; i++) {
            addDynamicFunction(null, 0, false);
        }
    }

    private void appendStaticIntervalsIntersection() {
        final int intervalCount = parsedIntervals.size() / 2;
        reserveEncodedIntervals(intervalCount, 0);
        for (int i = 0; i < intervalCount; i++) {
            IntervalUtils.encodeInterval(
                    parsedIntervals.getQuick(i * 2),
                    parsedIntervals.getQuick(i * 2 + 1),
                    i == 0 ? intervalCount : 0,
                    PeriodType.NONE,
                    1,
                    i == 0 ? IntervalOperation.INTERSECT_INTERVALS : IntervalOperation.NONE,
                    staticIntervals
            );
            addDynamicFunction(null, 0, false);
        }
    }

    private Throwable freeAndClearBestEffort() {
        isOwnershipTransferred = false;
        // Detach the pending endpoint and every adopted slot before invoking user close methods.
        Throwable failure = clearBetweenParsing(null);
        failure = Misc.freeObjListBestEffort(failure, dynamicRangeList);
        dynamicRangeList.clear();
        cursorFunctionPositions.clear();
        staticIntervals.clear();
        intervalApplied = false;
        return failure;
    }

    private void intersectBetweenDynamic(Function funcValue1, int funcPosition1, Function funcValue2, int funcPosition2) {
        assert !isEmptySet();

        // Reserve capacity for the whole operation before mutating any list or adopting either
        // function: a growth failure here leaves both functions with their previous owners and
        // the lists untouched, instead of adopting one endpoint (double-closed by the caller and
        // this builder) while stranding the other.
        final boolean isCursor1 = funcValue1.getType() == ColumnType.CURSOR;
        final boolean isCursor2 = funcValue2.getType() == ColumnType.CURSOR;
        reserveEncodedIntervals(2, (isCursor1 ? 1 : 0) + (isCursor2 ? 1 : 0));

        short operation = betweenNegated ? IntervalOperation.SUBTRACT_BETWEEN : IntervalOperation.INTERSECT_BETWEEN;
        IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_SEPARATE_DYNAMIC, operation, staticIntervals);
        IntervalUtils.encodeInterval(0, 0, (short) 0, IntervalDynamicIndicator.IS_LO_SEPARATE_DYNAMIC, operation, staticIntervals);
        addDynamicFunction(funcValue1, funcPosition1, isCursor1);
        addDynamicFunction(funcValue2, funcPosition2, isCursor2);
        intervalApplied = true;
    }

    private void intersectBetweenSemiDynamic(Function funcValue, int funcPosition, long constValue) {
        assert constValue != Numbers.LONG_NULL;
        assert !isEmptySet();

        // Reserve capacity for the whole operation before mutating any list or adopting the
        // function: a growth failure here leaves the function with its previous owner and the
        // lists untouched and aligned.
        final boolean isCursor = funcValue.getType() == ColumnType.CURSOR;
        reserveEncodedIntervals(1, isCursor ? 1 : 0);

        short operation = betweenNegated ? IntervalOperation.SUBTRACT_BETWEEN : IntervalOperation.INTERSECT_BETWEEN;
        IntervalUtils.encodeInterval(constValue, 0, (short) 0, IntervalDynamicIndicator.IS_HI_DYNAMIC, operation, staticIntervals);
        addDynamicFunction(funcValue, funcPosition, isCursor);
        intervalApplied = true;
    }

    private void intersectCompiledTickExpr(CompiledTickExpression expr) {
        if (isEmptySet()) {
            Misc.free(expr);
            return;
        }
        try {
            reserveEncodedIntervals(1, 0);
            IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.INTERSECT_INTERVALS, staticIntervals);
            addDynamicFunction(expr, 0, false);
            intervalApplied = true;
        } catch (Throwable th) {
            CairoException.rethrowCleanupFailure(Misc.freeBestEffort(th, expr));
        }
    }

    private long offsetIntervalHi(long hi, long hiOffset, TimestampDriver modelTimestampDriver) {
        if (hi == Long.MAX_VALUE || hiOffset == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        if (hi != Numbers.LONG_NULL) {
            hi = timestampDriver.from(hi, modelTimestampDriver.getTimestampType());
        }
        return addSaturating(hi, hiOffset);
    }

    private long offsetIntervalLo(long lo, long loOffset, TimestampDriver modelTimestampDriver) {
        if (lo == Numbers.LONG_NULL || loOffset == Numbers.LONG_NULL) {
            return Numbers.LONG_NULL;
        }
        lo = timestampDriver.from(lo, modelTimestampDriver.getTimestampType());
        return subtractSaturating(lo, loOffset);
    }

    private void resetBetweenParsingState() {
        betweenBoundarySet = false;
        betweenBoundaryFunc = null;
        betweenBoundaryFuncPosition = 0;
        betweenBoundary = Numbers.LONG_NULL;
    }

    private void subtractCompiledTickExpr(CompiledTickExpression expr) {
        if (isEmptySet()) {
            Misc.free(expr);
            return;
        }
        try {
            reserveEncodedIntervals(1, 0);
            IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.SUBTRACT_INTERVALS, staticIntervals);
            addDynamicFunction(expr, 0, false);
            intervalApplied = true;
        } catch (Throwable th) {
            CairoException.rethrowCleanupFailure(Misc.freeBestEffort(th, expr));
        }
    }

    private void unionCompiledTickExpr(CompiledTickExpression expr) {
        if (isEmptySet()) {
            Misc.free(expr);
            return;
        }
        try {
            reserveEncodedIntervals(1, 0);
            IntervalUtils.encodeInterval(0L, 0L, IntervalOperation.UNION, staticIntervals);
            addDynamicFunction(expr, 0, false);
            intervalApplied = true;
        } catch (Throwable th) {
            CairoException.rethrowCleanupFailure(Misc.freeBestEffort(th, expr));
        }
    }

    /**
     * Merges a static interval union from another builder with calendar-aware offset adjustment.
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
        if (other.dynamicRangeList.size() > 0) {
            // The optimizer keeps runtime-bound predicates above the virtual record. Do not use a
            // partial static prefix as pruning: a later runtime UNION may expand beyond that prefix.
            return;
        }

        final LongList otherIntervals = other.staticIntervals;
        if (otherIntervals.size() == 0) {
            intersectEmpty();
            return;
        }

        final TimestampDriver otherDriver = other.timestampDriver;
        try {
            parsedIntervals.clear();
            for (int i = 0, n = otherIntervals.size(); i < n; i += 2) {
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
                }
                if (lo > hi) {
                    continue;
                }

                // Offset the complete source union before intersecting it with this builder.
                // Calendar shifts remain monotonic but may collapse adjacent dates, so coalesce.
                if (parsedIntervals.size() > 0 && lo <= parsedIntervals.getLast()) {
                    if (hi > parsedIntervals.getLast()) {
                        parsedIntervals.setQuick(parsedIntervals.size() - 1, hi);
                    }
                } else {
                    parsedIntervals.add(lo, hi);
                }
            }

            if (parsedIntervals.size() == 0) {
                intersectEmpty();
            } else if (dynamicRangeList.size() > 0) {
                // Keep the complete existing expression in evaluation order and intersect the
                // shifted union once at the end. In particular, a preceding runtime UNION must
                // not run after this outer constraint and add excluded timestamps back.
                appendStaticIntervalsIntersection();
            } else {
                final int divider = staticIntervals.size();
                staticIntervals.add(parsedIntervals);
                if (intervalApplied) {
                    IntervalUtils.intersectInPlace(staticIntervals, divider);
                } else {
                    intervalApplied = true;
                }
            }
        } finally {
            parsedIntervals.clear();
        }
    }
}
