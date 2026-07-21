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
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;

/**
 * lttb(ts, value, target) window function.
 * <p>
 * Boolean "keep this row?" flag that marks the points selected by the Largest-Triangle-Three-Buckets
 * downsampling algorithm ({@link LttbAlgorithm#select}), re-homed here (like {@link M4FunctionFactory})
 * over a per-partition native buffer of {@code (ordinal, ts, value)} entries built during pass1 rather
 * than SUBSAMPLE's whole-cursor buffer. Reuses {@link M4FunctionFactory.BucketSelectWindowFunction} for
 * the buffering/pass1/pass2 plumbing: LTTB always emits first, one point per bucket, and last in
 * strictly ascending buffer-position order (and, in gap-preserving mode, per segment in ascending
 * segment order), matching the ascending-walk assumption that plumbing relies on.
 * <p>
 * The optional fourth argument (see {@link LttbGapFunctionFactory}, signature {@code lttb(NDls)})
 * supplies a gap threshold interval string (e.g. {@code '1h'}): when present, the buffered points are
 * split into contiguous segments wherever consecutive timestamps are further apart than the threshold,
 * and each segment is downsampled independently with a proportional point budget - see
 * {@link LttbAlgorithm}'s class doc for the soft-target semantics this implies.
 */
public class LttbFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "lttb";
    // Lowercase 'l': unlike m4's target argument (uppercase 'L', deliberately not constant-enforced by
    // the signature so a non-constant target reaches newInstance for a friendlier message), lttb's
    // target is declared constant-enforced here per the spec for this function; a non-constant target
    // is therefore rejected by FunctionParser's own overload matching (MATCH_NO_MATCH) before
    // newInstance ever runs. The defensive isConstant() check below is kept anyway (dead code for a
    // direct non-constant argument, but cheap insurance and matches the m4 validation shape) in case a
    // future signature change or an edge case in overload resolution ever lets one through.
    private static final String SIGNATURE = NAME + "(NDl)";

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
        return newInstance0(position, args, argPositions, sqlExecutionContext, false, supportNullsDesc());
    }

    // Shared by LttbFunctionFactory (lttb(NDl)) and LttbGapFunctionFactory (lttb(NDls)).
    static Function newInstance0(
            int position,
            ObjList<Function> args,
            IntList argPositions,
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

        // Reproduce SqlCodeGenerator.generateSubsample's numeric-column check (same message) so
        // SUBSAMPLE lttb(...) and this window function reject the same columns identically.
        final short valueTag = ColumnType.tagOf(valueArg.getType());
        if (valueTag != ColumnType.DOUBLE && valueTag != ColumnType.FLOAT
                && valueTag != ColumnType.INT && valueTag != ColumnType.LONG
                && valueTag != ColumnType.SHORT && valueTag != ColumnType.BYTE) {
            throw SqlException.$(argPositions.getQuick(1), "numeric column expected, got: ")
                    .put(ColumnType.nameOf(valueArg.getType()));
        }

        if (!targetArg.isConstant()) {
            throw SqlException.$(argPositions.getQuick(2), "target must be a constant");
        }
        long target = targetArg.getLong(null);
        if (target == Numbers.LONG_NULL || target < 2) {
            throw SqlException.$(argPositions.getQuick(2), "target points must be at least 2");
        }
        if (target > Integer.MAX_VALUE) {
            throw SqlException.$(argPositions.getQuick(2), "target points exceeds maximum of ").put(Integer.MAX_VALUE);
        }

        long gapThresholdMicros = 0;
        if (hasGap) {
            gapThresholdMicros = parseGapThreshold(args.getQuick(3), argPositions.getQuick(3));
        }

        return new LttbBucketSelectWindowFunction(tsArg, valueArg, target, new LttbAlgorithm(gapThresholdMicros), NAME);
    }

    // Reproduces SqlCodeGenerator.generateSubsample's gap-threshold parse (~line 7157): same
    // TimestampSamplerFactory calls, same supported units/errors/positions. The original operates on
    // the raw (possibly quoted) ExpressionNode token from the bespoke SUBSAMPLE grammar and strips
    // quotes by hand; here arg is already a compiled STRING constant Function (guaranteed constant by
    // the lowercase 's' in "lttb(NDls)" - FunctionParser disqualifies non-constant candidates before
    // newInstance runs), so getStrA() hands back the unquoted content directly.
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
            default ->
                    throw SqlException.$(gapPosition + k, "unsupported interval unit: ").put(interval.charAt(k))
                            .put(". Supported: s, m, h, d");
        };
    }

    // Reproduces SqlCodeGenerator.generateSubsample's private safeMultiplyMicros (~line 7312) exactly:
    // n * unitMicros can overflow long for large n (e.g. many days), so guard before multiplying.
    private static long safeMultiplyMicros(long n, long unitMicros, int pos) throws SqlException {
        if (n > Long.MAX_VALUE / unitMicros) {
            throw SqlException.$(pos, "gap threshold overflow");
        }
        return n * unitMicros;
    }

    // lttb(ts, value, target[, gap]) over (order by xxx) - no partition by, no framing.
    //
    // Thin subclass of the shared M4FunctionFactory.BucketSelectWindowFunction purely to release
    // LttbAlgorithm's native scratch lists on close(). M4Algorithm/MinMaxAlgorithm/UniformAlgorithm are
    // stateless singletons, so the shared base's close() has nothing algorithm-specific to free - but
    // LttbAlgorithm owns two DirectLongList/DirectIntList fields (lazily allocated only in gap mode; see
    // LttbAlgorithm.selectGapPreserving) that the old SubsampleRecordCursorFactory.destroy() freed
    // explicitly via LttbAlgorithm.close(). This mirrors that lifecycle without adding
    // algorithm-specific cleanup to the shared base class.
    static class LttbBucketSelectWindowFunction extends M4FunctionFactory.BucketSelectWindowFunction {
        private final LttbAlgorithm lttbAlgorithm;

        LttbBucketSelectWindowFunction(Function tsArg, Function valueArg, long target, LttbAlgorithm algorithm, String name) {
            super(tsArg, valueArg, target, algorithm, name);
            this.lttbAlgorithm = algorithm;
        }

        @Override
        public void close() {
            super.close();
            lttbAlgorithm.close();
        }
    }
}
