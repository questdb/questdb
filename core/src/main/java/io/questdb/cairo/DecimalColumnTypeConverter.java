/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.DecimalUtil;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Unsafe;

public class DecimalColumnTypeConverter {
    private static final io.questdb.std.ThreadLocal<Decimal256> decimalsTL = new ThreadLocal<>(Decimal256::new);
    private static final Loader loaderFromDecimal8 = DecimalColumnTypeConverter::loadDecimalFromDecimal8;
    private static final Loader loaderFromDecimal16 = DecimalColumnTypeConverter::loadDecimalFromDecimal16;
    private static final Loader loaderFromDecimal32 = DecimalColumnTypeConverter::loadDecimalFromDecimal32;
    private static final Loader loaderFromDecimal64 = DecimalColumnTypeConverter::loadDecimalFromDecimal64;
    private static final Loader loaderFromDecimal128 = DecimalColumnTypeConverter::loadDecimalFromDecimal128;
    private static final Loader loaderFromDecimal256 = DecimalColumnTypeConverter::loadDecimalFromDecimal256;
    private static final Loader loaderFromByte = DecimalColumnTypeConverter::loadDecimalFromByte;
    private static final Loader loaderFromShort = DecimalColumnTypeConverter::loadDecimalFromShort;
    private static final Loader loaderFromInt = DecimalColumnTypeConverter::loadDecimalFromInt;
    private static final Loader loaderFromLong = DecimalColumnTypeConverter::loadDecimalFromLong;


    public static boolean convertToDecimal(long srcMem, int srcType, MemoryA dstMem, int dstType, long srcColumnTypeSize, long rowCount) {
        final int srcScale = ColumnType.isDecimal(srcType) ? ColumnType.getDecimalScale(srcType) : 0;
        final int dstScale = ColumnType.getDecimalScale(dstType);
        final int dstPrecision = ColumnType.getDecimalPrecision(dstType);
        final Loader loader = getLoader(srcType);

        Decimal256 decimal = decimalsTL.get();
        long hi = srcMem + rowCount * srcColumnTypeSize;
        try {
            for (long i = srcMem; i < hi; i += srcColumnTypeSize) {
                loader.load(decimal, i, srcScale);
                if (srcScale != dstScale) {
                    decimal.rescale(dstScale);
                }
                if (!decimal.comparePrecision(dstPrecision)) {
                    return false;
                }
                DecimalUtil.store(decimal, dstMem, dstType);
            }
        } catch (NumericException ignored) {
            return false;
        }

        return true;
    }


    private static Loader getLoader(int type) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BYTE:
                return loaderFromByte;
            case ColumnType.SHORT:
                return loaderFromShort;
            case ColumnType.INT:
                return loaderFromInt;
            case ColumnType.LONG:
                return loaderFromLong;
            case ColumnType.DECIMAL8:
                return loaderFromDecimal8;
            case ColumnType.DECIMAL16:
                return loaderFromDecimal16;
            case ColumnType.DECIMAL32:
                return loaderFromDecimal32;
            case ColumnType.DECIMAL64:
                return loaderFromDecimal64;
            case ColumnType.DECIMAL128:
                return loaderFromDecimal128;
            default:
                return loaderFromDecimal256;
        }
    }

    private static void loadDecimalFromDecimal8(Decimal256 decimal, long addr, int scale) {
        byte b = Unsafe.getUnsafe().getByte(addr);
        if (b == Decimals.DECIMAL8_NULL) {
            decimal.ofNull();
        } else {
            decimal.ofLong(b, scale);
        }
    }

    private static void loadDecimalFromDecimal16(Decimal256 decimal, long addr, int scale) {
        short s = Unsafe.getUnsafe().getShort(addr);
        if (s == Decimals.DECIMAL16_NULL) {
            decimal.ofNull();
        } else {
            decimal.ofLong(s, scale);
        }
    }

    private static void loadDecimalFromDecimal32(Decimal256 decimal, long addr, int scale) {
        int i = Unsafe.getUnsafe().getInt(addr);
        if (i == Decimals.DECIMAL32_NULL) {
            decimal.ofNull();
        } else {
            decimal.ofLong(i, scale);
        }
    }

    private static void loadDecimalFromDecimal64(Decimal256 decimal, long addr, int scale) {
        long l = Unsafe.getUnsafe().getLong(addr);
        if (l == Decimals.DECIMAL64_NULL) {
            decimal.ofNull();
        } else {
            decimal.ofLong(l, scale);
        }
    }

    private static void loadDecimalFromDecimal128(Decimal256 decimal, long addr, int scale) {
        long hi = Unsafe.getUnsafe().getLong(addr);
        long lo = Unsafe.getUnsafe().getLong(addr + 8L);
        if (Decimal128.isNull(hi, lo)) {
            decimal.ofNull();
        } else {
            long s = hi < 0 ? -1 : 0;
            decimal.of(s, s, hi, lo, scale);
        }
    }

    private static void loadDecimalFromDecimal256(Decimal256 decimal, long addr, int scale) {
        decimal.of(
                Unsafe.getUnsafe().getLong(addr),
                Unsafe.getUnsafe().getLong(addr + 8L),
                Unsafe.getUnsafe().getLong(addr + 16L),
                Unsafe.getUnsafe().getLong(addr + 24L),
                scale
        );
    }

    private static void loadDecimalFromByte(Decimal256 decimal, long addr, int scale) {
        byte b = Unsafe.getUnsafe().getByte(addr);
        if (b == 0) {
            decimal.ofNull();
        } else {
            decimal.ofLong(b, 0);
        }
    }

    private static void loadDecimalFromShort(Decimal256 decimal, long addr, int scale) {
        short s = Unsafe.getUnsafe().getShort(addr);
        if (s == 0) {
            decimal.ofNull();
        } else {
            decimal.ofLong(s, 0);
        }
    }

    private static void loadDecimalFromInt(Decimal256 decimal, long addr, int scale) {
        int i = Unsafe.getUnsafe().getInt(addr);
        if (i == Numbers.INT_NULL) {
            decimal.ofNull();
        } else {
            decimal.ofLong(i, 0);
        }
    }

    private static void loadDecimalFromLong(Decimal256 decimal, long addr, int scale) {
        long l = Unsafe.getUnsafe().getLong(addr);
        if (l == Numbers.LONG_NULL) {
            decimal.ofNull();
        } else {
            decimal.ofLong(l, 0);
        }
    }


    @FunctionalInterface
    public interface Loader {
        void load(Decimal256 decimal, long addr, int scale);
    }
}
