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
import io.questdb.griffin.engine.table.MinMaxAlgorithm;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * minmax(ts, value, target) window function.
 * <p>
 * Boolean "keep this row?" flag that marks up to 2 representative points (min, max) per time
 * bucket, using the same selection rule as SUBSAMPLE's MinMax algorithm ({@link
 * MinMaxAlgorithm#select}). Thin wrapper over {@link M4FunctionFactory.BucketSelectWindowFunction}
 * - identical validation and buffered value-inspecting pass1/pass2 plumbing as {@code m4}, differing
 * only in the {@link io.questdb.griffin.engine.table.SubsampleAlgorithm} it drives (see that class'
 * javadoc for why the base is shared rather than duplicated).
 */
public class MinMaxFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "minmax";
    // Uppercase 'L' (not the constant-only lowercase 'l') so a non-constant target reaches newInstance
    // and gets the friendly "target must be a constant" message below - see M4FunctionFactory's
    // SIGNATURE comment for the full rationale.
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
            throw SqlException.$(position, "minmax() requires ORDER BY");
        }

        if (!windowContext.isDefaultFrame()) {
            throw SqlException.$(position, "minmax() does not support framing; remove ROWS/RANGE clause");
        }

        if (windowContext.getPartitionByRecord() != null) {
            throw SqlException.$(position, "minmax() does not support PARTITION BY");
        }

        final Function tsArg = args.getQuick(0);
        final Function valueArg = args.getQuick(1);
        final Function targetArg = args.getQuick(2);

        // Reproduce SqlCodeGenerator.generateSubsample's numeric-column check (same message) so
        // SUBSAMPLE minmax(...) and this window function reject the same columns identically.
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

        return new M4FunctionFactory.BucketSelectWindowFunction(tsArg, valueArg, target, MinMaxAlgorithm.INSTANCE, NAME);
    }
}
