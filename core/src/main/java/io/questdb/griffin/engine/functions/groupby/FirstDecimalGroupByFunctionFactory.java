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
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class FirstDecimalGroupByFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "first(Ξ)";
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
        return switch (ColumnType.tagOf(func.getType())) {
            case ColumnType.DECIMAL8 -> new Decimal8Func(func);
            case ColumnType.DECIMAL16 -> new Decimal16Func(func);
            case ColumnType.DECIMAL32 -> new Decimal32Func(func);
            case ColumnType.DECIMAL64 -> new Decimal64Func(func);
            case ColumnType.DECIMAL128 -> new Decimal128Func(func);
            default -> new Decimal256Func(func);
        };
    }

    private static class Decimal128Func extends FirstLastDecimal128Func {

        public Decimal128Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "first";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcRowId = srcValue.getLong(valueIndex);
            final long destRowId = destValue.getLong(valueIndex);
            if (srcRowId != Numbers.LONG_NULL && (srcRowId < destRowId || destRowId == Numbers.LONG_NULL)) {
                destValue.putLong(valueIndex, srcRowId);
                srcValue.getDecimal128(valueIndex + 1, decimal128);
                destValue.putDecimal128(valueIndex + 1, decimal128);
            }
        }
    }

    private static class Decimal16Func extends FirstLastDecimal16Func {

        public Decimal16Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "first";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcRowId = srcValue.getLong(valueIndex);
            final long destRowId = destValue.getLong(valueIndex);
            if (srcRowId != Numbers.LONG_NULL && (srcRowId < destRowId || destRowId == Numbers.LONG_NULL)) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putShort(valueIndex + 1, srcValue.getDecimal16(valueIndex + 1));
            }
        }
    }

    private static class Decimal256Func extends FirstLastDecimal256Func {

        public Decimal256Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "first";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcRowId = srcValue.getLong(valueIndex);
            final long destRowId = destValue.getLong(valueIndex);
            if (srcRowId != Numbers.LONG_NULL && (srcRowId < destRowId || destRowId == Numbers.LONG_NULL)) {
                destValue.putLong(valueIndex, srcRowId);
                srcValue.getDecimal256(valueIndex + 1, decimal256);
                destValue.putDecimal256(valueIndex + 1, decimal256);
            }
        }
    }

    private static class Decimal32Func extends FirstLastDecimal32Func {

        public Decimal32Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "first";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcRowId = srcValue.getLong(valueIndex);
            final long destRowId = destValue.getLong(valueIndex);
            if (srcRowId != Numbers.LONG_NULL && (srcRowId < destRowId || destRowId == Numbers.LONG_NULL)) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putInt(valueIndex + 1, srcValue.getDecimal32(valueIndex + 1));
            }
        }
    }

    private static class Decimal64Func extends FirstLastDecimal64Func {

        public Decimal64Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "first";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcRowId = srcValue.getLong(valueIndex);
            final long destRowId = destValue.getLong(valueIndex);
            if (srcRowId != Numbers.LONG_NULL && (srcRowId < destRowId || destRowId == Numbers.LONG_NULL)) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putLong(valueIndex + 1, srcValue.getLong(valueIndex + 1));
            }
        }
    }

    private static class Decimal8Func extends FirstLastDecimal8Func {

        public Decimal8Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "first";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcRowId = srcValue.getLong(valueIndex);
            final long destRowId = destValue.getLong(valueIndex);
            if (srcRowId != Numbers.LONG_NULL && (srcRowId < destRowId || destRowId == Numbers.LONG_NULL)) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putByte(valueIndex + 1, srcValue.getDecimal8(valueIndex + 1));
            }
        }
    }

    abstract static class FirstLastDecimal128Func extends FirstLastDecimalFunc {
        protected final Decimal128 decimal128 = new Decimal128();

        public FirstLastDecimal128Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            arg.getDecimal128(record, decimal128);
            mapValue.putLong(valueIndex, rowId);
            mapValue.putDecimal128(valueIndex + 1, decimal128);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            // empty
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            rec.getDecimal128(valueIndex + 1, sink);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.LONG); // row id
            columnTypes.add(ColumnType.DECIMAL128); // value
        }

        @Override
        public void setDecimal128(MapValue mapValue, Decimal128 value) {
            // This method is used to define interpolated points and to init
            // an empty value, so it's ok to reset the row id field here.
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
            mapValue.putDecimal128(valueIndex + 1, value);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
            mapValue.putDecimal128Null(valueIndex + 1);
        }
    }

    abstract static class FirstLastDecimal16Func extends FirstLastDecimalFunc {

        public FirstLastDecimal16Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final short value = arg.getDecimal16(record);
            mapValue.putLong(valueIndex, rowId);
            mapValue.putShort(valueIndex + 1, value);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            // empty
        }

        @Override
        public short getDecimal16(Record rec) {
            return rec.getDecimal16(valueIndex + 1);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.LONG); // row id
            columnTypes.add(ColumnType.DECIMAL16); // value
        }

        @Override
        public void setNull(MapValue mapValue) {
            // This method is used to define interpolated points and to init
            // an empty value, so it's ok to reset the row id field here.
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
            mapValue.putShort(valueIndex + 1, Decimals.DECIMAL16_NULL);
        }

        @Override
        public void setShort(MapValue mapValue, short value) {
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
            mapValue.putShort(valueIndex + 1, value);
        }
    }

    abstract static class FirstLastDecimal256Func extends FirstLastDecimalFunc {
        protected final Decimal256 decimal256 = new Decimal256();

        public FirstLastDecimal256Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            arg.getDecimal256(record, decimal256);
            mapValue.putLong(valueIndex, rowId);
            mapValue.putDecimal256(valueIndex + 1, decimal256);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            // empty
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            rec.getDecimal256(valueIndex + 1, sink);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.LONG); // row id
            columnTypes.add(ColumnType.DECIMAL256); // value
        }

        @Override
        public void setDecimal256(MapValue mapValue, Decimal256 value) {
            // This method is used to define interpolated points and to init
            // an empty value, so it's ok to reset the row id field here.
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
            mapValue.putDecimal256(valueIndex + 1, value);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
            mapValue.putDecimal256Null(valueIndex + 1);
        }
    }

    abstract static class FirstLastDecimal32Func extends FirstLastDecimalFunc {

        public FirstLastDecimal32Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final int value = arg.getDecimal32(record);
            mapValue.putLong(valueIndex, rowId);
            mapValue.putInt(valueIndex + 1, value);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            // empty
        }

        @Override
        public int getDecimal32(Record rec) {
            return rec.getDecimal32(valueIndex + 1);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.LONG); // row id
            columnTypes.add(ColumnType.DECIMAL32); // value
        }

        @Override
        public void setInt(MapValue mapValue, int value) {
            // This method is used to define interpolated points and to init
            // an empty value, so it's ok to reset the row id field here.
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
            mapValue.putInt(valueIndex + 1, value);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
            mapValue.putInt(valueIndex + 1, Decimals.DECIMAL32_NULL);
        }
    }

    abstract static class FirstLastDecimal64Func extends FirstLastDecimalFunc {

        public FirstLastDecimal64Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final long value = arg.getDecimal64(record);
            mapValue.putLong(valueIndex, rowId);
            mapValue.putLong(valueIndex + 1, value);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            // empty
        }

        @Override
        public long getDecimal64(Record rec) {
            return rec.getDecimal64(valueIndex + 1);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.LONG); // row id
            columnTypes.add(ColumnType.DECIMAL64); // value
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            // empty
        }

        @Override
        public void setLong(MapValue mapValue, long value) {
            // This method is used to define interpolated points and to init
            // an empty value, so it's ok to reset the row id field here.
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
            mapValue.putLong(valueIndex + 1, value);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
            mapValue.putLong(valueIndex + 1, Decimals.DECIMAL64_NULL);
        }
    }

    abstract static class FirstLastDecimal8Func extends FirstLastDecimalFunc {

        public FirstLastDecimal8Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final byte value = arg.getDecimal8(record);
            mapValue.putLong(valueIndex, rowId);
            mapValue.putByte(valueIndex + 1, value);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            // empty
        }

        @Override
        public byte getDecimal8(Record rec) {
            return rec.getDecimal8(valueIndex + 1);
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.LONG); // row id
            columnTypes.add(ColumnType.DECIMAL8); // value
        }

        @Override
        public void setByte(MapValue mapValue, byte value) {
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
            mapValue.putByte(valueIndex + 1, value);
        }

        @Override
        public void setNull(MapValue mapValue) {
            // This method is used to define interpolated points and to init
            // an empty value, so it's ok to reset the row id field here.
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
            mapValue.putByte(valueIndex + 1, Decimals.DECIMAL8_NULL);
        }
    }

    private abstract static class FirstLastDecimalFunc extends DecimalFunction implements GroupByFunction, UnaryFunction {
        protected final Function arg;
        protected int valueIndex;

        public FirstLastDecimalFunc(@NotNull Function arg) {
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
            return false;
        }

        @Override
        public boolean supportsParallelism() {
            return UnaryFunction.super.supportsParallelism();
        }
    }
}
