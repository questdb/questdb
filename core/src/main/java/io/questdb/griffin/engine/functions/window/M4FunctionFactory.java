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
import io.questdb.griffin.engine.table.M4Algorithm;
import io.questdb.griffin.engine.table.SubsampleAlgorithm;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * m4(ts, value, target) window function.
 * <p>
 * Boolean "keep this row?" flag that marks up to 4 representative points (first, min, max, last) per
 * time bucket, using the same selection rule as SUBSAMPLE's M4 algorithm ({@link M4Algorithm#select}),
 * re-homed here over a per-partition native buffer of {@code (ts, value)} entries built during
 * pass1 rather than SUBSAMPLE's whole-cursor buffer. Unlike the position-only {@code uniform}/{@code
 * cadence} window functions, m4 must inspect the value column to compute per-bucket min/max, so it
 * materializes every row before {@link BucketSelectWindowFunction#preparePass2()} runs the
 * bucketing selection.
 * <p>
 * The buffering/pass1/pass2 plumbing lives in {@link BucketSelectWindowFunction}, shared verbatim
 * with {@code minmax} and {@code lttb}; this factory only validates arguments and injects
 * {@link M4Algorithm#INSTANCE} as the selection strategy.
 */
public class M4FunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "m4";
    // Uppercase 'L' (not the constant-only lowercase 'l') so a non-constant target reaches newInstance
    // and gets the friendly "target must be a constant" message below, matching the
    // UniformFunctionFactory/CadenceFunctionFactory precedent - a lowercase-const-flagged signature
    // char would instead make the overload resolution silently not match, surfacing a generic
    // "there is no matching function" error from FunctionParser instead.
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
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc());

        if (!windowContext.isOrdered()) {
            throw SqlException.$(position, "m4() requires ORDER BY");
        }

        // The bucketing algorithm consumes pass1 input in ascending timestamp order. A window
        // ORDER BY dismissed against a backward base scan traverses descending, so refuse it
        // here; the sorted path is validated in initRecordComparator, and pass1's monotonicity
        // guard backstops order keys that only decay at runtime (mismatched or expression keys).
        if (windowContext.getOrderByScanDirection() == RecordCursorFactory.SCAN_DIRECTION_BACKWARD) {
            throw SqlException.$(position, NAME).put("() requires ascending ORDER BY");
        }

        if (!windowContext.isDefaultFrame()) {
            throw SqlException.$(position, "m4() does not support framing; remove ROWS/RANGE clause");
        }

        if (windowContext.getPartitionByRecord() != null) {
            throw SqlException.$(position, "m4() does not support PARTITION BY");
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
        final long resolvedTarget = BucketSelectWindowFunction.coerceAndValidateConstantTarget(
                targetArg, targetPosition, sqlExecutionContext);

        return new BucketSelectWindowFunction(
                tsArg,
                valueArg,
                targetArg,
                targetPosition,
                resolvedTarget,
                M4Algorithm.INSTANCE,
                NAME,
                configuration.getSubsampleMaxRows(),
                position
        );
    }

}
