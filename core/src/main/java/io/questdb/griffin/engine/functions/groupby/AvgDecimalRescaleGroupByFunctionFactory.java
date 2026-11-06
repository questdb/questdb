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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class AvgDecimalRescaleGroupByFunctionFactory implements FunctionFactory {

    /**
     * The type avg(value, scale) returns: the decimal type of the value rescaled to the scale, keeping the value's
     * integer digits.
     *
     * @param argType       the type of the value
     * @param scale         the constant scale
     * @param scalePosition the position of the scale
     * @throws SqlException at the scale's position when the scale is negative or above the maximum scale, or the
     *                      rescaled precision is below 1 or above the maximum precision
     */
    public static int getRescaledType(int argType, Function scale, int scalePosition) throws SqlException {
        final int targetScale = scale.getInt(null);
        if (targetScale < 0) {
            throw SqlException.$(scalePosition, "non-negative scale required: ").put(targetScale);
        }
        if (targetScale > Decimals.MAX_SCALE) {
            throw SqlException.$(scalePosition, "scale exceeds maximum of ").put(Decimals.MAX_SCALE).put(": ").put(targetScale);
        }
        final int targetPrecision = getRescaledPrecision(argType, targetScale);
        if (targetPrecision < 1) {
            throw SqlException.$(scalePosition, "rescaled decimal has invalid precision: ").put(targetPrecision);
        }
        if (targetPrecision > Decimals.MAX_PRECISION) {
            throw SqlException.$(scalePosition, "rescaled decimal has precision that exceeds maximum of ").put(Decimals.MAX_PRECISION)
                    .put(": ").put(targetPrecision);
        }
        return ColumnType.getDecimalType(targetPrecision, targetScale);
    }

    public static GroupByFunction newInstance(Function arg, int position, int targetScale) {
        final int argType = arg.getType();
        final int targetType = ColumnType.getDecimalType(getRescaledPrecision(argType, targetScale), targetScale);
        return switch (ColumnType.tagOf(argType)) {
            case ColumnType.DECIMAL8 -> new AvgDecimal8Rescale256GroupByFunction(arg, position, targetType);
            case ColumnType.DECIMAL16 -> new AvgDecimal16Rescale256GroupByFunction(arg, position, targetType);
            case ColumnType.DECIMAL32 -> new AvgDecimal32Rescale256GroupByFunction(arg, position, targetType);
            case ColumnType.DECIMAL64 -> new AvgDecimal64Rescale256GroupByFunction(arg, position, targetType);
            case ColumnType.DECIMAL128 -> new AvgDecimal128Rescale256GroupByFunction(arg, position, targetType);
            default -> new AvgDecimal256Rescale256GroupByFunction(arg, position, targetType);
        };
    }

    @Override
    public String getSignature() {
        return "avg(Ξi)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function arg = args.getQuick(0);
        final int targetType = getRescaledType(arg.getType(), args.getQuick(1), argPositions.getQuick(1));
        return newInstance(arg, position, ColumnType.getDecimalScale(targetType));
    }

    private static int getRescaledPrecision(int argType, int targetScale) {
        return ColumnType.getDecimalPrecision(argType) - ColumnType.getDecimalScale(argType) + targetScale;
    }
}
