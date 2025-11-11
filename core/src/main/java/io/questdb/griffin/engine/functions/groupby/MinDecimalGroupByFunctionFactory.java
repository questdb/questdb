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
        switch (ColumnType.tagOf(func.getType())) {
            case ColumnType.DECIMAL8:
                return new Decimal8Func(func);
            case ColumnType.DECIMAL16:
                return new Decimal16Func(func);
            case ColumnType.DECIMAL32:
                return new Decimal32Func(func);
            case ColumnType.DECIMAL64:
                return new Decimal64Func(func);
            case ColumnType.DECIMAL128:
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
        protected boolean shouldStoreA(Decimal128 decimal128A, Decimal128 decimal128B) {
            return decimal128A.compareTo(decimal128B) < 0;
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
        protected boolean shouldStoreA(Decimal256 decimal256A, Decimal256 decimal256B) {
            return decimal256A.compareTo(decimal256B) < 0;
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
        private final Decimal128 decimal128A = new Decimal128();
        private final Decimal128 decimal128B = new Decimal128();

        public MinMaxDecimal128Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            arg.getDecimal128(record, decimal128A);
            mapValue.putDecimal128(valueIndex, decimal128A);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            arg.getDecimal128(record, decimal128A);
            if (!decimal128A.isNull()) {
                mapValue.getDecimal128(valueIndex, decimal128B);
                if (shouldStoreA(decimal128A, decimal128B) || decimal128B.isNull()) {
                    mapValue.putDecimal128(valueIndex, decimal128A);
                }
            }
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            rec.getDecimal128(valueIndex, sink);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.DECIMAL128);
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            srcValue.getDecimal128(valueIndex, decimal128A);
            destValue.getDecimal128(valueIndex, decimal128B);
            if (!decimal128A.isNull()
                    && (shouldStoreA(decimal128A, decimal128B) || decimal128B.isNull())) {
                destValue.putDecimal128(valueIndex, decimal128A);
            }
        }

        @Override
        public void setDecimal128(MapValue mapValue, Decimal128 value) {
            mapValue.putDecimal128(valueIndex, value);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putDecimal128Null(valueIndex);
        }

        protected abstract boolean shouldStoreA(Decimal128 decimal128A, Decimal128 decimal128B);
    }

    abstract static class MinMaxDecimal16Func extends MinMaxDecimalFunc {

        public MinMaxDecimal16Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final short value = arg.getDecimal16(record);
            mapValue.putShort(valueIndex, value);
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
        private final Decimal256 decimal256A = new Decimal256();
        private final Decimal256 decimal256B = new Decimal256();

        public MinMaxDecimal256Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            arg.getDecimal256(record, decimal256A);
            mapValue.putDecimal256(valueIndex, decimal256A);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            arg.getDecimal256(record, decimal256A);
            if (!decimal256A.isNull()) {
                mapValue.getDecimal256(valueIndex, decimal256B);
                if (shouldStoreA(decimal256A, decimal256B) || decimal256B.isNull()) {
                    mapValue.putDecimal256(valueIndex, decimal256A);
                }
            }
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            rec.getDecimal256(valueIndex, sink);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.DECIMAL256);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            srcValue.getDecimal256(valueIndex, decimal256A);
            destValue.getDecimal256(valueIndex, decimal256B);

            if (!decimal256A.isNull()
                    && (shouldStoreA(decimal256A, decimal256B) || decimal256B.isNull())) {
                destValue.putDecimal256(valueIndex, decimal256A);
            }
        }

        @Override
        public void setDecimal256(MapValue mapValue, Decimal256 value) {
            mapValue.putDecimal256(valueIndex, value);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putDecimal256Null(valueIndex);
        }

        protected abstract boolean shouldStoreA(Decimal256 decimal256A, Decimal256 decimal256B);
    }

    abstract static class MinMaxDecimal32Func extends MinMaxDecimalFunc {

        public MinMaxDecimal32Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final int value = arg.getDecimal32(record);
            mapValue.putInt(valueIndex, value);
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
            mapValue.putLong(valueIndex, value);
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
            mapValue.putByte(valueIndex, value);
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
        public void getDecimal128(Record rec, Decimal128 sink) {
            throw new UnsupportedOperationException();
        }

        @Override
        public short getDecimal16(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getDecimal32(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getDecimal64(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte getDecimal8(Record rec) {
            throw new UnsupportedOperationException();
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
