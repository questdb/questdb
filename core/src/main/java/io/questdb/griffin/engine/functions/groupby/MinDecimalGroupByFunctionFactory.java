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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class MinDecimalGroupByFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "min(Ξ)";
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
    ) {
        final Function func = args.getQuick(0);
        switch (Decimals.getStorageSizePow2(ColumnType.getDecimalPrecision(func.getType()))) {
            case 0:
                return new Decimal8Func(func);
            case 1:
                return new Decimal16Func(func);
            case 2:
                return new Decimal32Func(func);
            case 3:
                return new Decimal64Func(func);
            case 4:
                return new Decimal128Func(func);
            default:
                return new Decimal256Func(func);
        }
    }

    private static class Decimal128Func extends MinMaxDecimal128Func {

        public Decimal128Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "min";
        }

        @Override
        protected boolean shouldStoreA(long aHigh, long aLow, long bHigh, long bLow) {
            return Decimal128.compare(aHigh, aLow, bHigh, bLow) < 0;
        }
    }

    private static class Decimal16Func extends MinMaxDecimal16Func {

        public Decimal16Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "min";
        }

        @Override
        protected boolean shouldStoreA(short aValue, short bValue) {
            return aValue < bValue;
        }
    }

    private static class Decimal256Func extends MinMaxDecimal256Func {

        public Decimal256Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "min";
        }

        @Override
        protected boolean shouldStoreA(long aHH, long aHL, long aLH, long aLL, long bHH, long bHL, long bLH, long bLL) {
            return Decimal256.compare(aHH, aHL, aLH, aLL, bHH, bHL, bLH, bLL) < 0;
        }
    }

    private static class Decimal32Func extends MinMaxDecimal32Func {

        public Decimal32Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "min";
        }

        @Override
        protected boolean shouldStoreA(int aValue, int bValue) {
            return aValue < bValue;
        }
    }

    private static class Decimal64Func extends MinMaxDecimal64Func {

        public Decimal64Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "min";
        }

        @Override
        protected boolean shouldStoreA(long aValue, long bValue) {
            return aValue < bValue;
        }
    }

    private static class Decimal8Func extends MinMaxDecimal8Func {

        public Decimal8Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "min";
        }

        @Override
        protected boolean shouldStoreA(byte aValue, byte bValue) {
            return aValue < bValue;
        }
    }

    abstract static class MinMaxDecimal128Func extends MinMaxDecimalFunc {

        public MinMaxDecimal128Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final long high = arg.getDecimal128Hi(record);
            final long low = arg.getDecimal128Lo(record);
            if (!Decimal128.isNull(high, low)) {
                mapValue.putDecimal128(valueIndex, high, low);
            } else {
                mapValue.putDecimal128Null(valueIndex);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            final long aHigh = arg.getDecimal128Hi(record);
            final long aLow = arg.getDecimal128Lo(record);
            if (!Decimal128.isNull(aHigh, aLow)) {
                final long bHigh = mapValue.getDecimal128Hi(valueIndex);
                final long bLow = mapValue.getDecimal128Lo(valueIndex);
                if (shouldStoreA(aHigh, aLow, bHigh, bLow) || Decimal128.isNull(bHigh, bLow)) {
                    mapValue.putDecimal128(valueIndex, aHigh, aLow);
                }
            }
        }

        @Override
        public long getDecimal128Hi(Record rec) {
            return rec.getDecimal128Hi(valueIndex);
        }

        @Override
        public long getDecimal128Lo(Record rec) {
            return rec.getDecimal128Lo(valueIndex);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.DECIMAL128);
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcHigh = srcValue.getDecimal128Hi(valueIndex);
            final long srcLow = srcValue.getDecimal128Lo(valueIndex);

            final long destHigh = destValue.getDecimal128Hi(valueIndex);
            final long destLow = destValue.getDecimal128Lo(valueIndex);

            if (!Decimal128.isNull(srcHigh, srcLow)
                    && (shouldStoreA(srcHigh, srcLow, destHigh, destLow) || Decimal128.isNull(destHigh, destLow))) {
                destValue.putDecimal128(valueIndex, srcHigh, srcLow);
            }
        }

        @Override
        public void setDecimal128(MapValue mapValue, long high, long low) {
            mapValue.putDecimal128(valueIndex, high, low);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putDecimal128Null(valueIndex);
        }

        protected abstract boolean shouldStoreA(long aHigh, long aLow, long bHigh, long bLow);
    }

    abstract static class MinMaxDecimal16Func extends MinMaxDecimalFunc {

        public MinMaxDecimal16Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final short value = arg.getDecimal16(record);
            if (value != Decimals.DECIMAL16_NULL) {
                mapValue.putShort(valueIndex, value);
            } else {
                mapValue.putShort(valueIndex, Decimals.DECIMAL16_NULL);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            final short aValue = arg.getDecimal16(record);
            if (aValue != Decimals.DECIMAL16_NULL) {
                final short bValue = mapValue.getDecimal16(valueIndex);
                if (shouldStoreA(aValue, bValue) || bValue == Decimals.DECIMAL16_NULL) {
                    mapValue.putShort(valueIndex, aValue);
                }
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return rec.getDecimal16(valueIndex);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.DECIMAL16);
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final short srcVal = srcValue.getDecimal16(valueIndex);
            final short destVal = destValue.getDecimal16(valueIndex);
            if (srcVal != Decimals.DECIMAL16_NULL && (shouldStoreA(srcVal, destVal) || destVal == Decimals.DECIMAL16_NULL)) {
                destValue.putShort(valueIndex, srcVal);
            }
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putShort(valueIndex, Decimals.DECIMAL16_NULL);
        }

        @Override
        public void setShort(MapValue mapValue, short value) {
            mapValue.putShort(valueIndex, value);
        }

        protected abstract boolean shouldStoreA(short aValue, short bValue);
    }

    abstract static class MinMaxDecimal256Func extends MinMaxDecimalFunc {

        public MinMaxDecimal256Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final long hh = arg.getDecimal256HH(record);
            final long hl = arg.getDecimal256HL(record);
            final long lh = arg.getDecimal256LH(record);
            final long ll = arg.getDecimal256LL(record);
            if (!Decimal256.isNull(hh, hl, lh, ll)) {
                mapValue.putDecimal256(valueIndex, hh, hl, lh, ll);
            } else {
                mapValue.putDecimal256Null(valueIndex);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            final long aHH = arg.getDecimal256HH(record);
            final long aHL = arg.getDecimal256HL(record);
            final long aLH = arg.getDecimal256LH(record);
            final long aLL = arg.getDecimal256LL(record);
            if (!Decimal256.isNull(aHH, aHL, aLH, aLL)) {
                final long bHH = mapValue.getDecimal256HH(valueIndex);
                final long bHL = mapValue.getDecimal256HL(valueIndex);
                final long bLH = mapValue.getDecimal256LH(valueIndex);
                final long bLL = mapValue.getDecimal256LL(valueIndex);
                if (shouldStoreA(aHH, aHL, aLH, aLL, bHH, bHL, bLH, bLL) || Decimal256.isNull(bHH, bHL, bLH, bLL)) {
                    mapValue.putDecimal256(valueIndex, aHH, aHL, aLH, aLL);
                }
            }
        }

        @Override
        public long getDecimal256HH(Record rec) {
            return rec.getDecimal256HH(valueIndex);
        }

        @Override
        public long getDecimal256HL(Record rec) {
            return rec.getDecimal256HL(valueIndex);
        }

        @Override
        public long getDecimal256LH(Record rec) {
            return rec.getDecimal256LH(valueIndex);
        }

        @Override
        public long getDecimal256LL(Record rec) {
            return rec.getDecimal256LL(valueIndex);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.DECIMAL256);
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcHH = srcValue.getDecimal256HH(valueIndex);
            final long srcHL = srcValue.getDecimal256HL(valueIndex);
            final long srcLH = srcValue.getDecimal256LH(valueIndex);
            final long srcLL = srcValue.getDecimal256LL(valueIndex);

            final long destHH = destValue.getDecimal256HH(valueIndex);
            final long destHL = destValue.getDecimal256HL(valueIndex);
            final long destLH = destValue.getDecimal256LH(valueIndex);
            final long destLL = destValue.getDecimal256LL(valueIndex);

            if (!Decimal256.isNull(srcHH, srcHL, srcLH, srcLL)
                    && (shouldStoreA(srcHH, srcHL, srcLH, srcLL, destHH, destHL, destLH, destLL) || Decimal256.isNull(destHH, destHL, destLH, destLL))) {
                destValue.putDecimal256(valueIndex, srcHH, srcHL, srcLH, srcLL);
            }
        }

        @Override
        public void setDecimal256(MapValue mapValue, long hh, long hl, long lh, long ll) {
            mapValue.putDecimal256(valueIndex, hh, hl, lh, ll);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putDecimal256Null(valueIndex);
        }

        protected abstract boolean shouldStoreA(
                long aHH, long aHL, long aLH, long aLL,
                long bHH, long bHL, long bLH, long bLL
        );
    }

    abstract static class MinMaxDecimal32Func extends MinMaxDecimalFunc {

        public MinMaxDecimal32Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final int value = arg.getDecimal32(record);
            if (value != Decimals.DECIMAL32_NULL) {
                mapValue.putInt(valueIndex, value);
            } else {
                mapValue.putInt(valueIndex, Decimals.DECIMAL32_NULL);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            final int aValue = arg.getDecimal32(record);
            if (aValue != Decimals.DECIMAL32_NULL) {
                final int bValue = mapValue.getDecimal32(valueIndex);
                if (shouldStoreA(aValue, bValue) || bValue == Decimals.DECIMAL32_NULL) {
                    mapValue.putInt(valueIndex, aValue);
                }
            }
        }

        @Override
        public int getDecimal32(Record rec) {
            return rec.getDecimal32(valueIndex);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.DECIMAL32);
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final int srcVal = srcValue.getDecimal32(valueIndex);
            final int destVal = destValue.getDecimal32(valueIndex);
            if (srcVal != Decimals.DECIMAL32_NULL && (shouldStoreA(srcVal, destVal) || destVal == Decimals.DECIMAL32_NULL)) {
                destValue.putInt(valueIndex, srcVal);
            }
        }

        @Override
        public void setInt(MapValue mapValue, int value) {
            mapValue.putInt(valueIndex, value);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putInt(valueIndex, Decimals.DECIMAL32_NULL);
        }

        protected abstract boolean shouldStoreA(int aValue, int bValue);
    }

    abstract static class MinMaxDecimal64Func extends MinMaxDecimalFunc {

        public MinMaxDecimal64Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final long value = arg.getDecimal64(record);
            if (!Decimal64.isNull(value)) {
                mapValue.putLong(valueIndex, value);
            } else {
                mapValue.putLong(valueIndex, Decimals.DECIMAL64_NULL);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            final long aValue = arg.getDecimal64(record);
            if (!Decimal64.isNull(aValue)) {
                final long bValue = mapValue.getDecimal64(valueIndex);
                if (shouldStoreA(aValue, bValue) || Decimal64.isNull(bValue)) {
                    mapValue.putLong(valueIndex, aValue);
                }
            }
        }

        @Override
        public long getDecimal64(Record rec) {
            return rec.getDecimal64(valueIndex);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.DECIMAL64);
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcVal = srcValue.getDecimal64(valueIndex);
            final long destVal = destValue.getDecimal64(valueIndex);
            if (!Decimal64.isNull(srcVal) && (shouldStoreA(srcVal, destVal) || Decimal64.isNull(destVal))) {
                destValue.putLong(valueIndex, srcVal);
            }
        }

        @Override
        public void setLong(MapValue mapValue, long value) {
            mapValue.putLong(valueIndex, value);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putLong(valueIndex, Decimals.DECIMAL64_NULL);
        }

        protected abstract boolean shouldStoreA(long aValue, long bValue);
    }

    abstract static class MinMaxDecimal8Func extends MinMaxDecimalFunc {

        public MinMaxDecimal8Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final byte value = arg.getDecimal8(record);
            if (value != Decimals.DECIMAL8_NULL) {
                mapValue.putByte(valueIndex, value);
            } else {
                mapValue.putByte(valueIndex, Decimals.DECIMAL8_NULL);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            final byte aValue = arg.getDecimal8(record);
            if (aValue != Decimals.DECIMAL8_NULL) {
                final byte bValue = mapValue.getDecimal8(valueIndex);
                if (shouldStoreA(aValue, bValue) || bValue == Decimals.DECIMAL8_NULL) {
                    mapValue.putByte(valueIndex, aValue);
                }
            }
        }

        @Override
        public byte getDecimal8(Record rec) {
            return rec.getDecimal8(valueIndex);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.DECIMAL8);
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final byte srcVal = srcValue.getDecimal8(valueIndex);
            final byte destVal = destValue.getDecimal8(valueIndex);
            if (srcVal != Decimals.DECIMAL8_NULL && (shouldStoreA(srcVal, destVal) || destVal == Decimals.DECIMAL8_NULL)) {
                destValue.putByte(valueIndex, srcVal);
            }
        }

        @Override
        public void setByte(MapValue mapValue, byte value) {
            mapValue.putByte(valueIndex, value);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putByte(valueIndex, Decimals.DECIMAL8_NULL);
        }

        protected abstract boolean shouldStoreA(byte aValue, byte bValue);
    }

    private abstract static class MinMaxDecimalFunc extends DecimalFunction implements GroupByFunction, UnaryFunction {
        protected final Function arg;
        protected int valueIndex;

        public MinMaxDecimalFunc(@NotNull Function arg) {
            super(arg.getType());
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getValueIndex() {
            return valueIndex;
        }

        @Override
        public void initValueIndex(int valueIndex) {
            this.valueIndex = valueIndex;
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean isThreadSafe() {
            return UnaryFunction.super.isThreadSafe();
        }

        @Override
        public boolean supportsParallelism() {
            return UnaryFunction.super.supportsParallelism();
        }
    }
}
