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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.engine.table.LttbAlgorithm;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Numbers;
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

        long gapThresholdMicros = 0;
        if (hasGap) {
            gapThresholdMicros = parseGapThreshold(args.getQuick(3), argPositions.getQuick(3));
        }

        return new LttbBucketSelectWindowFunction(
                tsArg,
                valueArg,
                targetArg,
                targetPosition,
                resolvedTarget,
                new LttbAlgorithm(gapThresholdMicros),
                NAME,
                configuration.getSubsampleMaxRows(),
                position
        );
    }

    // Preserves SUBSAMPLE's gap-threshold parsing contract: the same TimestampSamplerFactory calls,
    // supported units, errors, and positions. The optimiser validates the raw (possibly quoted)
    // SUBSAMPLE token; here the direct window function receives an already-compiled STRING constant
    // (guaranteed by the lowercase 's' in "lttb(NDls)"), so getStrA() returns unquoted content.
    private static long parseGapThreshold(Function gapArg, int gapPosition) throws SqlException {
        final CharSequence interval = gapArg.getStrA(null);
        int k = TimestampSamplerFactory.findPositiveIntervalEndIndex(interval, gapPosition, "gap threshold");
        long n = TimestampSamplerFactory.parsePositiveInterval(
                interval, k, gapPosition, "gap threshold", Numbers.INT_NULL, '?'
        );
        return switch (interval.charAt(k)) {
            case 's' -> safeMultiplyMicros(n, Micros.SECOND_MICROS, gapPosition);
            case 'm' -> safeMultiplyMicros(n, Micros.MINUTE_MICROS, gapPosition);
            case 'h' -> safeMultiplyMicros(n, Micros.HOUR_MICROS, gapPosition);
            case 'd' -> safeMultiplyMicros(n, Micros.DAY_MICROS, gapPosition);
            default -> throw SqlException.$(gapPosition + k, "unsupported interval unit: ").put(interval.charAt(k))
                    .put(". Supported: s, m, h, d");
        };
    }

    // Preserve the gap-threshold overflow contract: n * unitMicros can overflow long for large n
    // (e.g. many days), so guard before multiplying.
    private static long safeMultiplyMicros(long n, long unitMicros, int pos) throws SqlException {
        if (n > Long.MAX_VALUE / unitMicros) {
            throw SqlException.$(pos, "gap threshold overflow");
        }
        return n * unitMicros;
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
