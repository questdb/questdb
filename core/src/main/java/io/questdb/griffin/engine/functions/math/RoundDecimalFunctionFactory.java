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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

import java.math.RoundingMode;

public class RoundDecimalFunctionFactory implements FunctionFactory {
    /**
     * Generates a function that will round the provided value.
     *
     * @param value         to be rounded
     * @param roundingScale is the position relative to the dot in the value that will be used for rounding, it may
     *                      be negative (e.g., 350 -> 300 for a rounding scale of -2 with HALF_DOWN rounding).
     * @param roundingMode  to be used for rounding.
     * @return a function that will round the provided value.
     */
    public static Function newInstance(Function value, Function roundingScale, RoundingMode roundingMode) {
        if (roundingScale.isConstant()) {
            return newConstantInstance(value, roundingScale.getInt(null), roundingMode);
        }

        // If the rounding scale is dynamic, we will keep the same target scale as the original type for representation
        // but the rounding scale might differ.
        // Note: this is unstable
        final int type = value.getType();
        int targetPrecision = ColumnType.getDecimalPrecision(type) + getRoundingModePrecisionImpact(roundingMode);
        int fromScale = ColumnType.getDecimalScale(type);
        int targetType = ColumnType.getDecimalType(targetPrecision, fromScale);

        var transformer = new DynamicTransformer(roundingScale, fromScale, roundingMode);
        return DecimalTransformerFactory.newInstance(value, targetType, transformer);
    }

    @Override
    public String getSignature() {
        return "round(ΞI)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return newInstance(args.getQuick(0), args.getQuick(1), RoundingMode.HALF_UP);
    }

    /**
     * Returns the additional precision that we may add to our target precision depending on the rounding mode.
     */
    private static int getRoundingModePrecisionImpact(RoundingMode roundingMode) {
        return roundingMode == RoundingMode.DOWN ? 0 : 1;
    }

    private static Function newConstantInstance(Function value, int roundingScale, RoundingMode roundingMode) {
        final int type = value.getType();
        final int precision = ColumnType.getDecimalPrecision(type);
        final int scale = ColumnType.getDecimalScale(type);

        if (roundingScale == Numbers.INT_NULL) {
            return DecimalUtil.createNullDecimalConstant(1, 0);
        }

        if (roundingScale >= scale) {
            return value;
        }

        if (roundingScale >= 0) {
            return newConstantPositiveInstance(value, precision, scale, roundingScale, roundingMode);
        } else {
            return newConstantNegativeInstance(value, precision, scale, roundingScale, roundingMode);
        }
    }

    /**
     * Round a decimal by a constant negative scale that is lesser than the decimal's scale.
     */
    private static Function newConstantNegativeInstance(Function value, int precision, int scale, int roundingScale, RoundingMode roundingMode) {
        assert scale > roundingScale;

        // In this case, we will always have a scale of 0 (we don't support a negative scale):
        // floor(120, -2) -> 100
        int targetPrecision = precision - scale + getRoundingModePrecisionImpact(roundingMode);
        int targetType = ColumnType.getDecimalType(targetPrecision, 0);

        var transformer = new ConstantNegativeTransformer(scale, roundingScale, roundingMode);
        return DecimalTransformerFactory.newInstance(value, targetType, transformer);
    }

    /**
     * Round a decimal by a constant positive scale that is greater than the decimal's scale.
     */
    private static Function newConstantPositiveInstance(Function value, int precision, int scale, int roundingScale, RoundingMode roundingMode) {
        assert scale > roundingScale;

        // We will reduce the decimal to take as little space as possible, removing any trailing zeroes:
        // floor(123.456, 2) -> 123.4
        int targetPrecision = precision + roundingScale - scale + getRoundingModePrecisionImpact(roundingMode);
        int targetType = ColumnType.getDecimalType(targetPrecision, roundingScale);

        var transformer = new ConstantPositiveTransformer(roundingScale, roundingMode);
        return DecimalTransformerFactory.newInstance(value, targetType, transformer);
    }

    private record ConstantNegativeTransformer(int fromScale, int scale,
                                               RoundingMode roundingMode) implements DecimalTransformer {
        @Override
        public String getName() {
            return "round";
        }

        @Override
        public boolean transform(Decimal128 value, Record record) {
            value.setScale(fromScale - scale);
            value.round(0, roundingMode);
            value.rescale(-scale);
            value.setScale(0);
            return true;
        }

        @Override
        public boolean transform(Decimal256 value, Record record) {
            value.setScale(fromScale - scale);
            value.round(0, roundingMode);
            value.rescale(-scale);
            value.setScale(0);
            return true;
        }

        @Override
        public boolean transform(Decimal64 value, Record record) {
            // We don't support negative scale in our decimal implementation, instead we can fake
            // the scale, round and rescale to simulate the proper scaling behavior.
            // Example: if we want to round 123.456 to -1
            // 1. fake scale: 123.456 -> 12.3456
            // 2. round: 12.3456 -> 12
            // 3. rescale: 12 -> 12.0
            // 4. fake scale: 12.0 -> 120
            value.setScale(fromScale - scale);
            value.round(0, roundingMode);
            value.rescale(-scale);
            value.setScale(0);
            return true;
        }
    }

    private record ConstantPositiveTransformer(int scale, RoundingMode roundingMode) implements DecimalTransformer {
        @Override
        public String getName() {
            return "round";
        }

        @Override
        public boolean transform(Decimal128 value, Record record) {
            value.round(scale, roundingMode);
            return true;
        }

        @Override
        public boolean transform(Decimal256 value, Record record) {
            value.round(scale, roundingMode);
            return true;
        }

        @Override
        public boolean transform(Decimal64 value, Record record) {
            value.round(scale, roundingMode);
            return true;
        }
    }

    private record DynamicTransformer(Function scale, int fromScale,
                                      RoundingMode roundingMode) implements DecimalTransformer {

        @Override
        public String getName() {
            return "round";
        }

        @Override
        public boolean transform(Decimal128 value, Record record) {
            int roundingScale = scale.getInt(record);
            if (roundingScale == Numbers.INT_NULL) {
                return false;
            }

            if (roundingScale >= fromScale) {
                // We don't need to do any rounding here (floor(123.456, 4) -> 123.456).
                return true;
            }

            // This is a bit complicated because the rounding scale is dynamic, but the value's scale is static.
            // For example, if we want to floor(123.456, 1), we need to end with 123.400.
            if (roundingScale >= 0) {
                value.round(roundingScale, roundingMode);
                value.rescale(fromScale);
            } else {
                // Similarly to the constant negative transformer, we need to fake the scale and round
                // to simulate the proper scaling behavior.
                value.setScale(fromScale - roundingScale);
                value.round(0, roundingMode);
                value.rescale(fromScale - roundingScale);
                value.setScale(fromScale);
            }
            return true;
        }

        @Override
        public boolean transform(Decimal256 value, Record record) {
            int roundingScale = scale.getInt(record);
            if (roundingScale == Numbers.INT_NULL) {
                return false;
            }

            if (roundingScale >= fromScale) {
                // We don't need to do any rounding here (floor(123.456, 4) -> 123.456).
                return true;
            }

            // This is a bit complicated because the rounding scale is dynamic, but the value's scale is static.
            // For example, if we want to floor(123.456, 1), we need to end with 123.400.
            if (roundingScale >= 0) {
                value.round(roundingScale, roundingMode);
                value.rescale(fromScale);
            } else {
                // Similarly to the constant negative transformer, we need to fake the scale and round
                // to simulate the proper scaling behavior.
                value.setScale(fromScale - roundingScale);
                value.round(0, roundingMode);
                value.rescale(fromScale - roundingScale);
                value.setScale(fromScale);
            }
            return true;
        }

        @Override
        public boolean transform(Decimal64 value, Record record) {
            int roundingScale = scale.getInt(record);
            if (roundingScale == Numbers.INT_NULL) {
                return false;
            }

            if (roundingScale >= fromScale) {
                // We don't need to do any rounding here (floor(123.456, 4) -> 123.456).
                return true;
            }

            // This is a bit complicated because the rounding scale is dynamic, but the value's scale is static.
            // For example, if we want to floor(123.456, 1), we need to end with 123.400.
            if (roundingScale >= 0) {
                value.round(roundingScale, roundingMode);
                value.rescale(fromScale);
            } else {
                // Similarly to the constant negative transformer, we need to fake the scale and round
                // to simulate the proper scaling behavior.
                value.setScale(fromScale - roundingScale);
                value.round(0, roundingMode);
                value.rescale(fromScale - roundingScale);
                value.setScale(fromScale);
            }
            return true;
        }
    }
}
