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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.LttbAlgorithm;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import org.jetbrains.annotations.Nullable;

/**
 * lttb(ts, value, target) window function.
 * <p>
 * Boolean "keep this row?" flag that marks the points selected by the Largest-Triangle-Three-Buckets
 * downsampling algorithm ({@link LttbAlgorithm#select}), re-homed here (like {@link M4FunctionFactory})
 * over a per-partition native buffer of {@code (ordinal, ts, value)} entries built during pass1 rather
 * than SUBSAMPLE's whole-cursor buffer. Reuses {@link M4FunctionFactory.BucketSelectWindowFunction} for
 * the buffering/pass1/pass2 plumbing: LTTB always emits first, one point per bucket, and last in
 * strictly ascending buffer-position order (and, in gap-preserving mode, per segment in ascending
 * segment order), matching the ascending-walk assumption that plumbing relies on. Large inputs run
 * the two-stage MinMaxLTTB variant transparently (same output count, pinned endpoints and ascending
 * order; see {@link LttbAlgorithm}'s class doc).
 * <p>
 * The optional fourth argument (see {@link LttbGapFunctionFactory}, signature {@code lttb(NDls)})
 * supplies a gap threshold interval string (e.g. {@code '1h'}): when present, the buffered points are
 * split into contiguous segments wherever consecutive timestamps are further apart than the threshold,
 * and each segment is downsampled independently with a proportional point budget - see
 * {@link LttbAlgorithm}'s class doc for the soft-target semantics this implies.
 */
public class LttbFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "lttb";
    // Uppercase 'L' (like m4's target): a lowercase constant-only 'l' would make FunctionParser's
    // overload matching reject a bind-variable (runtime-constant) target as MATCH_NO_MATCH before
    // newInstance runs, so bind variables could never reach the accept check below. With 'L', both a
    // non-constant column (rejected with the friendly message in newInstance0) and a bind-variable
    // target reach newInstance, where the accept check keeps constants/bind-variables and rejects the
    // rest.
    private static final String SIGNATURE = NAME + "(NDL)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return newInstance0(position, args, argPositions, configuration, sqlExecutionContext, false, supportNullsDesc());
    }

    // Shared by LttbFunctionFactory (lttb(NDl)) and LttbGapFunctionFactory (lttb(NDls)).
    static Function newInstance0(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext,
            boolean hasGap,
            boolean supportNullsDesc
    ) throws SqlException {
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc);

        if (!windowContext.isOrdered()) {
            throw SqlException.$(position, "lttb() requires ORDER BY");
        }

        // See M4FunctionFactory: ascending pass1 input is the algorithm's precondition (gap
        // segmentation in particular can never fire on descending input); reject the
        // order-dismissed backward-scan shape here, the sorted path in initRecordComparator,
        // and runtime backward steps in pass1.
        if (windowContext.getOrderByScanDirection() == RecordCursorFactory.SCAN_DIRECTION_BACKWARD) {
            throw SqlException.$(position, NAME).put("() requires ascending ORDER BY");
        }

        if (!windowContext.isDefaultFrame()) {
            throw SqlException.$(position, "lttb() does not support framing; remove ROWS/RANGE clause");
        }

        if (windowContext.getPartitionByRecord() != null) {
            throw SqlException.$(position, "lttb() does not support PARTITION BY");
        }

        final Function tsArg = args.getQuick(0);
        final Function valueArg = args.getQuick(1);
        final Function targetArg = args.getQuick(2);

        // Preserve SUBSAMPLE's numeric-column check and message so the SQL clause and direct window
        // function reject the same columns identically.
        final short valueTag = ColumnType.tagOf(valueArg.getType());
        if (valueTag != ColumnType.DOUBLE && valueTag != ColumnType.FLOAT
                && valueTag != ColumnType.INT && valueTag != ColumnType.LONG
                && valueTag != ColumnType.SHORT && valueTag != ColumnType.BYTE) {
            throw SqlException.$(argPositions.getQuick(1), "numeric column expected, got: ")
                    .put(ColumnType.nameOf(valueArg.getType()));
        }

        // A bind-variable target that is unset at compile - and may be re-bound between executions -
        // is read PER-EXECUTION (see BucketSelectWindowFunction.init) rather than frozen here. A
        // constant target is range-validated right below (compile time, matching the
        // pre-bind-var-support factory and the legacy SUBSAMPLE cursor's own constant handling); a
        // constant otherwise reads to the same value at every open, so constant behavior is unchanged.
        final int targetPosition = argPositions.getQuick(2);
        if (!targetArg.isConstant() && !targetArg.isRuntimeConstant()) {
            throw SqlException.$(targetPosition, "target must be a constant or bind variable");
        }
        final long resolvedTarget = M4FunctionFactory.BucketSelectWindowFunction.coerceAndValidateConstantTarget(
                targetArg, targetPosition, sqlExecutionContext);

        long gapThreshold = 0;
        if (hasGap) {
            gapThreshold = parseGapThreshold(args.getQuick(3), argPositions.getQuick(3), tsArg.getType());
        }

        return new LttbBucketSelectWindowFunction(
                tsArg,
                valueArg,
                targetArg,
                targetPosition,
                resolvedTarget,
                new LttbAlgorithm(gapThreshold),
                NAME,
                configuration.getSubsampleMaxRows(),
                position
        );
    }

    // The direct window function receives an already-compiled STRING constant (guaranteed by the
    // lowercase 's' in "lttb(NDls)"), so getStrA() returns unquoted content. The SUBSAMPLE clause
    // validates its raw quoted token through the same parseGapThresholdMicros().
    //
    // The interval is parsed to micros and then scaled into the timestamp column's NATIVE unit.
    // LttbAlgorithm compares the threshold against raw column timestamps, so on a TIMESTAMP_NS
    // column an unscaled micros threshold would be 1000x too small - '1h' would split segments
    // every 3.6 seconds, over-segmenting the series and blowing past target_points (gap mode uses
    // soft targets, so every extra segment adds rows).
    private static long parseGapThreshold(Function gapArg, int gapPosition, int timestampType) throws SqlException {
        return toTimestampUnits(parseGapThresholdMicros(gapArg.getStrA(null), gapPosition), timestampType);
    }

    /**
     * Parses a gap threshold such as '30s', '5m', '1h' or '1d' into microseconds. Both the SUBSAMPLE clause
     * and the window-function form report through this method, so a malformed threshold gets the same
     * diagnostic on either path. {@code position} is the position of the quoted SQL token; unit errors
     * point at the offending character inside the quotes.
     */
    public static long parseGapThresholdMicros(@Nullable CharSequence interval, int position) throws SqlException {
        if (interval == null) {
            throw SqlException.$(position, "gap threshold must be a string constant such as '1h'");
        }
        final int len = interval.length();
        int k = 0;
        while (k < len && interval.charAt(k) >= '0' && interval.charAt(k) <= '9') {
            k++;
        }
        if (k == 0 || k != len - 1) {
            throw SqlException.$(position, "invalid gap threshold '").put(interval)
                    .put("'; expected a number followed by a unit, such as '30s', '5m', '1h' or '1d'");
        }
        final long n;
        try {
            n = Numbers.parseLong(interval, 0, k);
        } catch (NumericException e) {
            throw SqlException.$(position, "gap threshold overflow");
        }
        if (n == 0) {
            throw SqlException.$(position, "gap threshold must be greater than zero");
        }
        final long unitMicros = switch (interval.charAt(k)) {
            case 's' -> Micros.SECOND_MICROS;
            case 'm' -> Micros.MINUTE_MICROS;
            case 'h' -> Micros.HOUR_MICROS;
            case 'd' -> Micros.DAY_MICROS;
            default -> throw SqlException.$(position + 1 + k, "unsupported interval unit: ").put(interval.charAt(k))
                    .put(". Supported: s, m, h, d");
        };
        if (n > Long.MAX_VALUE / unitMicros) {
            throw SqlException.$(position, "gap threshold overflow");
        }
        return n * unitMicros;
    }

    // Scales a micros threshold into the timestamp column's native unit: identity for TIMESTAMP,
    // x1000 for TIMESTAMP_NS. Saturates rather than wrapping - a threshold beyond the column's
    // representable span can never be exceeded by a real gap, which is exactly how LttbAlgorithm
    // already reads a saturated threshold (its `prevTs > Long.MAX_VALUE - gapThreshold` guard
    // reports "no gap"). Saturating here also keeps the "gap threshold overflow" compile error
    // attached to the micros computation above, so the micros-column contract is unchanged.
    private static long toTimestampUnits(long micros, int timestampType) {
        final long unitsPerMicro = ColumnType.getTimestampDriver(timestampType).fromMicros(1);
        if (unitsPerMicro <= 1) {
            return micros;
        }
        return micros > Long.MAX_VALUE / unitsPerMicro ? Long.MAX_VALUE : micros * unitsPerMicro;
    }

    // lttb(ts, value, target[, gap]) over (order by xxx) - no partition by, no framing.
    //
    // Thin subclass of the shared M4FunctionFactory.BucketSelectWindowFunction purely to release
    // LttbAlgorithm's native scratch lists on close(). M4Algorithm and MinMaxAlgorithm are stateless
    // singletons, so the shared base's close() has nothing algorithm-specific to free - but LttbAlgorithm
    // owns native DirectLongList scratch fields (segment/target bookkeeping, lazily allocated in gap
    // mode, plus the MinMaxLTTB preselection candidate list, lazily allocated for large inputs; see
    // LttbAlgorithm.selectGapPreserving and LttbAlgorithm.preselectMinMax) and must be tracker-bound
    // and closed explicitly.
    static class LttbBucketSelectWindowFunction extends M4FunctionFactory.BucketSelectWindowFunction {
        private final LttbAlgorithm lttbAlgorithm;

        LttbBucketSelectWindowFunction(
                Function tsArg,
                Function valueArg,
                Function targetArg,
                int targetPosition,
                long resolvedTarget,
                LttbAlgorithm algorithm,
                String name,
                long maxRows,
                int functionPosition
        ) {
            super(tsArg, valueArg, targetArg, targetPosition, resolvedTarget, algorithm, name, maxRows, functionPosition);
            this.lttbAlgorithm = algorithm;
        }

        @Override
        public void close() {
            super.close();
            lttbAlgorithm.close();
        }

        @Override
        public void reset() {
            super.reset();
            lttbAlgorithm.close();
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            super.setMemoryTracker(tracker);
            lttbAlgorithm.setMemoryTracker(tracker);
        }
    }
}
