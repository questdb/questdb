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

package io.questdb.cairo;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.engine.functions.columns.DecimalColumn;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.std.Decimals;
import io.questdb.std.Vect;

/**
 * Type driver for the stored decimal family: DECIMAL8 to DECIMAL256 are one type stored at
 * six widths, so they share one class with one instance per tag. Precision and scale are part
 * of the encoded column type and are passed as an argument where a method needs them. The
 * DECIMAL pseudo tag, which only resolves function overloads, has no driver.
 */
public final class DecimalTypeDriver extends FixedSizeTypeDriver {
    public static final DecimalTypeDriver DECIMAL128 = new DecimalTypeDriver(ColumnTypeTag.DECIMAL128, 4);
    public static final DecimalTypeDriver DECIMAL16 = new DecimalTypeDriver(ColumnTypeTag.DECIMAL16, 1);
    public static final DecimalTypeDriver DECIMAL256 = new DecimalTypeDriver(ColumnTypeTag.DECIMAL256, 5);
    public static final DecimalTypeDriver DECIMAL32 = new DecimalTypeDriver(ColumnTypeTag.DECIMAL32, 2);
    public static final DecimalTypeDriver DECIMAL64 = new DecimalTypeDriver(ColumnTypeTag.DECIMAL64, 3);
    public static final DecimalTypeDriver DECIMAL8 = new DecimalTypeDriver(ColumnTypeTag.DECIMAL8, 0);

    private DecimalTypeDriver(ColumnTypeTag tag, int pow2Width) {
        super(tag, pow2Width);
    }

    /**
     * Typed by the encoded precision and scale.
     */
    @Override
    public ConstantFunction getNullConstant(int columnType) {
        return DecimalUtil.createNullDecimalConstant(
                ColumnType.getDecimalPrecision(columnType),
                ColumnType.getDecimalScale(columnType)
        );
    }

    @Override
    public long getNullLong(int longIndex) {
        return switch (getPow2Width()) {
            case 0 -> Decimals.DECIMAL8_NULL;
            case 1 -> Decimals.DECIMAL16_NULL;
            case 2 -> Decimals.DECIMAL32_NULL;
            case 3 -> Decimals.DECIMAL64_NULL;
            case 4 -> longIndex == 0 ? Decimals.DECIMAL128_HI_NULL : Decimals.DECIMAL128_LO_NULL;
            case 5 -> switch (longIndex) {
                case 0 -> Decimals.DECIMAL256_HH_NULL;
                case 1 -> Decimals.DECIMAL256_HL_NULL;
                case 2 -> Decimals.DECIMAL256_LH_NULL;
                default -> Decimals.DECIMAL256_LL_NULL;
            };
            default -> throw new IllegalStateException("no decimal width " + getPow2Width());
        };
    }

    @Override
    public boolean hasNullSentinel() {
        return true;
    }

    @Override
    public Function newColumnFunction(int columnIndex, int columnType) {
        return DecimalColumn.newInstance(columnIndex, columnType);
    }

    @Override
    public Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem) {
        return switch (getPow2Width()) {
            case 0 -> () -> dataMem.putByte(Decimals.DECIMAL8_NULL);
            case 1 -> () -> dataMem.putShort(Decimals.DECIMAL16_NULL);
            case 2 -> () -> dataMem.putInt(Decimals.DECIMAL32_NULL);
            case 3 -> () -> dataMem.putLong(Decimals.DECIMAL64_NULL);
            case 4 -> () -> dataMem.putDecimal128(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
            case 5 ->
                    () -> dataMem.putDecimal256(Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL, Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL);
            default -> throw new IllegalStateException("no decimal width " + getPow2Width());
        };
    }

    @Override
    public void setNull(long addr, long count) {
        switch (getPow2Width()) {
            case 0 -> Vect.memset(addr, count, Decimals.DECIMAL8_NULL);
            case 1 -> Vect.setMemoryShort(addr, Decimals.DECIMAL16_NULL, count);
            case 2 -> Vect.setMemoryInt(addr, Decimals.DECIMAL32_NULL, count);
            case 3 -> Vect.setMemoryLong(addr, Decimals.DECIMAL64_NULL, count);
            case 4 -> Vect.setMemoryLong128(addr, Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, count);
            case 5 -> Vect.setMemoryLong256(addr, Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                    Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL, count);
            default -> throw new IllegalStateException("no decimal width " + getPow2Width());
        }
    }
}
