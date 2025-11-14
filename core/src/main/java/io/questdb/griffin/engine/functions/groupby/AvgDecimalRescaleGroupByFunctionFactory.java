/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

    public static GroupByFunction newInstance(Function arg, int position, int targetScale) {
        final int argType = arg.getType();
        final int argPrecision = ColumnType.getDecimalPrecision(argType);
        final int argScale = ColumnType.getDecimalScale(argType);
        final int targetPrecision = argPrecision - argScale + targetScale;
        final int targetType = ColumnType.getDecimalType(targetPrecision, targetScale);
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
        final int targetScale = args.getQuick(1).getInt(null);
        final int scalePosition = argPositions.getQuick(1);
        if (targetScale < 0) {
            throw SqlException.$(scalePosition, "non-negative scale required: ").put(targetScale);
        }
        if (targetScale > Decimals.MAX_SCALE) {
            throw SqlException.$(scalePosition, "scale exceeds maximum of ").put(Decimals.MAX_SCALE).put(": ").put(targetScale);
        }
        final Function arg = args.getQuick(0);
        final int argPrecision = ColumnType.getDecimalPrecision(arg.getType());
        final int argScale = ColumnType.getDecimalScale(arg.getType());
        final int targetPrecision = argPrecision - argScale + targetScale;
        if (targetPrecision > Decimals.MAX_PRECISION) {
            throw SqlException.$(scalePosition, "rescaled decimal has precision that exceeds maximum of ").put(Decimals.MAX_PRECISION)
                    .put(": ").put(targetPrecision);
        }
        return newInstance(arg, position, targetScale);
    }
}
