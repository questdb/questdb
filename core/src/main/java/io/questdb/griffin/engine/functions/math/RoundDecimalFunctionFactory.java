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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.decimal.Decimal128Function;
import io.questdb.griffin.engine.functions.decimal.Decimal256Function;
import io.questdb.griffin.engine.functions.decimal.Decimal64Function;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
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
        int targetType = ColumnType.getDecimalType(targetPrecision, ColumnType.getDecimalScale(type));
        switch (ColumnType.tagOf(targetType)) {
            case ColumnType.DECIMAL8:
            case ColumnType.DECIMAL16:
            case ColumnType.DECIMAL32:
            case ColumnType.DECIMAL64:
                return new Dynamic64Func(targetType, value, roundingScale, roundingMode);
            case ColumnType.DECIMAL128:
                return new Dynamic128Func(targetType, value, roundingScale, roundingMode);
            default:
                return new Dynamic256Func(targetType, value, roundingScale, roundingMode);
        }
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

    private static class ConstantNegativeTransformer implements DecimalTransformer {
        private final int fromScale;
        private final RoundingMode roundingMode;
        private final int scale;

        private ConstantNegativeTransformer(int fromScale, int scale, RoundingMode roundingMode) {
            this.scale = scale;
            this.roundingMode = roundingMode;
            this.fromScale = fromScale;
        }

        @Override
        public String getName() {
            return "round";
        }

        @Override
        public void transform(Decimal128 value) {
            value.setScale(fromScale - scale);
            value.round(0, roundingMode);
            value.rescale(-scale);
            value.setScale(0);
        }

        @Override
        public void transform(Decimal256 value) {
            value.setScale(fromScale - scale);
            value.round(0, roundingMode);
            value.rescale(-scale);
            value.setScale(0);

        }

        @Override
        public void transform(Decimal64 value) {
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

        }
    }

    private static class ConstantPositiveTransformer implements DecimalTransformer {
        private final RoundingMode roundingMode;
        private final int scale;

        private ConstantPositiveTransformer(int scale, RoundingMode roundingMode) {
            this.scale = scale;
            this.roundingMode = roundingMode;
        }

        @Override
        public String getName() {
            return "round";
        }

        @Override
        public void transform(Decimal128 value) {
            value.round(scale, roundingMode);

        }

        @Override
        public void transform(Decimal256 value) {
            value.round(scale, roundingMode);

        }

        @Override
        public void transform(Decimal64 value) {
            value.round(scale, roundingMode);

        }
    }

    private static class Dynamic128Func extends Decimal128Function implements BinaryFunction {
        private final Decimal128 decimal128 = new Decimal128();
        private final int fromScale;
        private final RoundingMode roundingMode;
        private final Function scale;
        private final Function value;

        private Dynamic128Func(int type, Function value, Function scale, RoundingMode roundingMode) {
            super(type);
            this.fromScale = ColumnType.getDecimalScale(type);
            this.value = value;
            this.scale = scale;
            this.roundingMode = roundingMode;
        }

        @Override
        public long getDecimal128Hi(Record rec) {
            DecimalUtil.load(decimal128, value, rec);
            if (decimal128.isNull()) {
                return decimal128.getHigh();
            }

            int roundingScale = scale.getInt(rec);
            if (roundingScale == Numbers.INT_NULL) {
                decimal128.ofNull();
                return decimal128.getHigh();
            }

            transform(roundingScale);
            return decimal128.getHigh();
        }

        @Override
        public long getDecimal128Lo(Record rec) {
            return decimal128.getLow();
        }

        @Override
        public Function getLeft() {
            return value;
        }

        @Override
        public String getName() {
            return "round";
        }

        @Override
        public Function getRight() {
            return scale;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        private void transform(int roundingScale) {
            if (roundingScale >= fromScale) {
                // We don't need to do any rounding here (floor(123.456, 4) -> 123.456).
                return;
            }

            // This is a bit complicated because the rounding scale is dynamic, but the value's scale is static.
            // For example, if we want to floor(123.456, 1), we need to end with 123.400.
            if (roundingScale >= 0) {
                decimal128.round(roundingScale, roundingMode);
                decimal128.rescale(fromScale);
            } else {
                // Similarly to the constant negative transformer, we need to fake the scale and round
                // to simulate the proper scaling behavior.
                decimal128.setScale(fromScale - roundingScale);
                decimal128.round(0, roundingMode);
                decimal128.rescale(fromScale - roundingScale);
                decimal128.setScale(fromScale);
            }

        }
    }

    private static class Dynamic256Func extends Decimal256Function implements BinaryFunction {
        private final Decimal256 decimal256 = new Decimal256();
        private final int fromScale;
        private final RoundingMode roundingMode;
        private final Function scale;
        private final Function value;

        private Dynamic256Func(int type, Function value, Function scale, RoundingMode roundingMode) {
            super(type);
            this.fromScale = ColumnType.getDecimalScale(type);
            this.value = value;
            this.scale = scale;
            this.roundingMode = roundingMode;
        }

        @Override
        public long getDecimal256HH(Record rec) {
            DecimalUtil.load(decimal256, value, rec);
            if (decimal256.isNull()) {
                return decimal256.getHh();
            }

            int roundingScale = scale.getInt(rec);
            if (roundingScale == Numbers.INT_NULL) {
                decimal256.ofNull();
                return decimal256.getHh();
            }

            transform(roundingScale);
            return decimal256.getHh();
        }

        @Override
        public long getDecimal256HL(Record rec) {
            return decimal256.getHl();
        }

        @Override
        public long getDecimal256LH(Record rec) {
            return decimal256.getLh();
        }

        @Override
        public long getDecimal256LL(Record rec) {
            return decimal256.getLl();
        }

        @Override
        public Function getLeft() {
            return value;
        }

        @Override
        public String getName() {
            return "round";
        }

        @Override
        public Function getRight() {
            return scale;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        private void transform(int roundingScale) {
            if (roundingScale >= fromScale) {
                // We don't need to do any rounding here (floor(123.456, 4) -> 123.456).
                return;
            }

            // This is a bit complicated because the rounding scale is dynamic, but the value's scale is static.
            // For example, if we want to floor(123.456, 1), we need to end with 123.400.
            if (roundingScale >= 0) {
                decimal256.round(roundingScale, roundingMode);
                decimal256.rescale(fromScale);
            } else {
                // Similarly to the constant negative transformer, we need to fake the scale and round
                // to simulate the proper scaling behavior.
                decimal256.setScale(fromScale - roundingScale);
                decimal256.round(0, roundingMode);
                decimal256.rescale(fromScale - roundingScale);
                decimal256.setScale(fromScale);
            }

        }
    }

    private static class Dynamic64Func extends Decimal64Function implements BinaryFunction {
        private final Decimal64 decimal64 = new Decimal64();
        private final int fromScale;
        private final RoundingMode roundingMode;
        private final Function scale;
        private final Function value;

        private Dynamic64Func(int type, Function value, Function scale, RoundingMode roundingMode) {
            super(type);
            this.fromScale = ColumnType.getDecimalScale(type);
            this.value = value;
            this.scale = scale;
            this.roundingMode = roundingMode;
        }

        @Override
        public short getDecimal16(Record rec) {
            DecimalUtil.load(decimal64, value, rec);
            if (decimal64.isNull()) {
                return Decimals.DECIMAL16_NULL;
            }
            int roundingScale = scale.getInt(rec);
            if (roundingScale == Numbers.INT_NULL) {
                return Decimals.DECIMAL16_NULL;
            }

            transform(roundingScale);
            return (short) decimal64.getValue();
        }

        @Override
        public int getDecimal32(Record rec) {
            DecimalUtil.load(decimal64, value, rec);
            if (decimal64.isNull()) {
                return Decimals.DECIMAL32_NULL;
            }
            int roundingScale = scale.getInt(rec);
            if (roundingScale == Numbers.INT_NULL) {
                return Decimals.DECIMAL32_NULL;
            }

            transform(roundingScale);
            return (int) decimal64.getValue();
        }

        @Override
        public long getDecimal64(Record rec) {
            DecimalUtil.load(decimal64, value, rec);
            if (decimal64.isNull()) {
                return decimal64.getValue();
            }
            int roundingScale = scale.getInt(rec);
            if (roundingScale == Numbers.INT_NULL) {
                return Decimals.DECIMAL64_NULL;
            }

            transform(roundingScale);
            return decimal64.getValue();
        }

        @Override
        public byte getDecimal8(Record rec) {
            DecimalUtil.load(decimal64, value, rec);
            if (decimal64.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }
            int roundingScale = scale.getInt(rec);
            if (roundingScale == Numbers.INT_NULL) {
                return Decimals.DECIMAL8_NULL;
            }

            transform(roundingScale);
            return (byte) decimal64.getValue();
        }

        @Override
        public Function getLeft() {
            return value;
        }

        @Override
        public String getName() {
            return "round";
        }

        @Override
        public Function getRight() {
            return scale;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        private void transform(int roundingScale) {
            if (roundingScale >= fromScale) {
                // We don't need to do any rounding here (floor(123.456, 4) -> 123.456).
                return;
            }

            // This is a bit complicated because the rounding scale is dynamic, but the value's scale is static.
            // For example, if we want to floor(123.456, 1), we need to end with 123.400.
            if (roundingScale >= 0) {
                decimal64.round(roundingScale, roundingMode);
                decimal64.rescale(fromScale);
            } else {
                // Similarly to the constant negative transformer, we need to fake the scale and round
                // to simulate the proper scaling behavior.
                // For example, if we want to floor(123.456, -1), we need to end with 120.000.
                decimal64.setScale(fromScale - roundingScale);
                decimal64.round(0, roundingMode);
                decimal64.rescale(fromScale - roundingScale);
                decimal64.setScale(fromScale);
            }
        }
    }
}
