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

import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.DecimalUtil;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;

import java.math.RoundingMode;

public class DecimalColumnTypeConverter {
    private static final Loader loaderFromByte = DecimalColumnTypeConverter::loadDecimalFromByte;
    private static final Loader loaderFromDecimal128 = DecimalColumnTypeConverter::loadDecimalFromDecimal128;
    private static final Loader loaderFromDecimal16 = DecimalColumnTypeConverter::loadDecimalFromDecimal16;
    private static final Loader loaderFromDecimal256 = DecimalColumnTypeConverter::loadDecimalFromDecimal256;
    private static final Loader loaderFromDecimal32 = DecimalColumnTypeConverter::loadDecimalFromDecimal32;
    private static final Loader loaderFromDecimal64 = DecimalColumnTypeConverter::loadDecimalFromDecimal64;
    private static final Loader loaderFromDecimal8 = DecimalColumnTypeConverter::loadDecimalFromDecimal8;
    private static final Loader loaderFromInt = DecimalColumnTypeConverter::loadDecimalFromInt;
    private static final Loader loaderFromLong = DecimalColumnTypeConverter::loadDecimalFromLong;
    private static final Loader loaderFromShort = DecimalColumnTypeConverter::loadDecimalFromShort;

    public static boolean convertToDecimal(long srcMem, int srcType, MemoryA dstMem, int dstType, long srcColumnTypeSize, long rowCount) {
        final int srcScale = ColumnType.isDecimal(srcType) ? ColumnType.getDecimalScale(srcType) : 0;
        final int dstScale = ColumnType.getDecimalScale(dstType);
        final int dstPrecision = ColumnType.getDecimalPrecision(dstType);
        final Loader loader = getLoader(srcType);
        if (loader == null) {
            return false;
        }

        var decimal = Misc.getThreadLocalDecimal256();
        long hi = srcMem + rowCount * srcColumnTypeSize;
        for (long i = srcMem; i < hi; i += srcColumnTypeSize) {
            loader.load(decimal, i);
            // Align with SQL store-assignment semantics: excess fractional digits are rounded to the
            // target scale (round half away from zero), and only a magnitude overflow - the integer
            // part exceeds the target precision - cannot be represented. The unrepresentable value
            // becomes the target NULL sentinel rather than aborting the whole conversion; aborting
            // would surface as "column is corrupt" and suspend a WAL table. Instead we mirror the
            // fixed->fixed converters, where an out-of-range value becomes the target NULL sentinel
            // (e.g. an out-of-range DOUBLE->FLOAT becomes NaN). So NULL is produced only when the
            // magnitude exceeds the target precision (comparePrecision); a scale reduction rounds.
            // Source NULLs flow through unchanged (the loader already set the decimal to NULL).
            if (!decimal.isNull()) {
                try {
                    if (srcScale != dstScale) {
                        decimal.setScale(srcScale);
                        if (dstScale < srcScale) {
                            // Scale reduction rounds half away from zero, like every mainstream SQL
                            // database storing into DECIMAL(p,s); it never fails on lost fraction.
                            decimal.round(dstScale, RoundingMode.HALF_UP);
                        } else {
                            // Scale increase is lossless.
                            decimal.rescale(dstScale);
                        }
                    }
                    if (!decimal.comparePrecision(dstPrecision)) {
                        decimal.ofNull();
                    }
                } catch (NumericException ignored) {
                    decimal.ofNull();
                }
            }
            DecimalUtil.store(decimal, dstMem, dstType);
        }
        return true;
    }

    static Loader getLoader(int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.BYTE -> loaderFromByte;
            case ColumnType.SHORT -> loaderFromShort;
            case ColumnType.INT -> loaderFromInt;
            case ColumnType.LONG -> loaderFromLong;
            case ColumnType.DECIMAL8 -> loaderFromDecimal8;
            case ColumnType.DECIMAL16 -> loaderFromDecimal16;
            case ColumnType.DECIMAL32 -> loaderFromDecimal32;
            case ColumnType.DECIMAL64 -> loaderFromDecimal64;
            case ColumnType.DECIMAL128 -> loaderFromDecimal128;
            case ColumnType.DECIMAL256 -> loaderFromDecimal256;
            default -> null;
        };
    }

    private static void loadDecimalFromByte(Decimal256 decimal, long addr) {
        byte b = Unsafe.getByte(addr);
        decimal.ofRaw(b);
    }

    private static void loadDecimalFromDecimal128(Decimal256 decimal, long addr) {
        long hi = Unsafe.getLong(addr);
        long lo = Unsafe.getLong(addr + 8L);
        if (Decimal128.isNull(hi, lo)) {
            decimal.ofRawNull();
        } else {
            decimal.ofRaw(hi, lo);
        }
    }

    private static void loadDecimalFromDecimal16(Decimal256 decimal, long addr) {
        short s = Unsafe.getShort(addr);
        if (s == Decimals.DECIMAL16_NULL) {
            decimal.ofRawNull();
        } else {
            decimal.ofRaw(s);
        }
    }

    private static void loadDecimalFromDecimal256(Decimal256 decimal, long addr) {
        decimal.ofRawAddress(addr);
    }

    private static void loadDecimalFromDecimal32(Decimal256 decimal, long addr) {
        int i = Unsafe.getInt(addr);
        if (i == Decimals.DECIMAL32_NULL) {
            decimal.ofRawNull();
        } else {
            decimal.ofRaw(i);
        }
    }

    private static void loadDecimalFromDecimal64(Decimal256 decimal, long addr) {
        long l = Unsafe.getLong(addr);
        if (l == Decimals.DECIMAL64_NULL) {
            decimal.ofRawNull();
        } else {
            decimal.ofRaw(l);
        }
    }

    private static void loadDecimalFromDecimal8(Decimal256 decimal, long addr) {
        byte b = Unsafe.getByte(addr);
        if (b == Decimals.DECIMAL8_NULL) {
            decimal.ofRawNull();
        } else {
            decimal.ofRaw(b);
        }
    }

    private static void loadDecimalFromInt(Decimal256 decimal, long addr) {
        int i = Unsafe.getInt(addr);
        if (i == Numbers.INT_NULL) {
            decimal.ofRawNull();
        } else {
            decimal.ofRaw(i);
        }
    }

    private static void loadDecimalFromLong(Decimal256 decimal, long addr) {
        long l = Unsafe.getLong(addr);
        if (l == Numbers.LONG_NULL) {
            decimal.ofNull();
        } else {
            decimal.ofRaw(l);
        }
    }

    private static void loadDecimalFromShort(Decimal256 decimal, long addr) {
        short s = Unsafe.getShort(addr);
        decimal.ofRaw(s);
    }

    @FunctionalInterface
    public interface Loader {
        void load(Decimal256 decimal, long addr);
    }
}
