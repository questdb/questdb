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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Long256Util;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class SumLong256GroupByFunction extends Long256Function implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final Long256Impl long256A = new Long256Impl();
    private final Long256Impl long256B = new Long256Impl();
    private int valueIndex;

    public SumLong256GroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeBatch(MapValue mapValue, long dataAddr, int rowCount, long startRowId) {
        if (rowCount <= 0) {
            return;
        }
        long offset = 0;
        long sum0 = 0, sum1 = 0, sum2 = 0, sum3 = 0;
        boolean hasValue = false;
        for (int i = 0; i < rowCount; i++) {
            final long l0 = Unsafe.getLong(dataAddr + offset);
            final long l1 = Unsafe.getLong(dataAddr + offset + Long.BYTES);
            final long l2 = Unsafe.getLong(dataAddr + offset + Long.BYTES * 2);
            final long l3 = Unsafe.getLong(dataAddr + offset + Long.BYTES * 3);
            offset += 4 * Long.BYTES;
            if (l0 == Numbers.LONG_NULL && l1 == Numbers.LONG_NULL
                    && l2 == Numbers.LONG_NULL && l3 == Numbers.LONG_NULL) {
                continue;
            }
            if (!hasValue) {
                sum0 = l0;
                sum1 = l1;
                sum2 = l2;
                sum3 = l3;
                hasValue = true;
            } else {
                // 256-bit add with carry, see Long256Util.add.
                long carry = 0;
                final long n0 = l0 + sum0 + carry;
                carry = ((l0 & sum0) | ((l0 | sum0) & ~n0)) >>> 63;
                final long n1 = l1 + sum1 + carry;
                carry = ((l1 & sum1) | ((l1 | sum1) & ~n1)) >>> 63;
                final long n2 = l2 + sum2 + carry;
                carry = ((l2 & sum2) | ((l2 | sum2) & ~n2)) >>> 63;
                sum3 = l3 + sum3 + carry;
                sum0 = n0;
                sum1 = n1;
                sum2 = n2;
            }
        }
        if (hasValue) {
            long256A.setAll(sum0, sum1, sum2, sum3);
            final Long256 existing = mapValue.getLong256A(valueIndex);
            if (Long256Impl.isNull(existing)) {
                mapValue.putLong256(valueIndex, long256A);
            } else {
                mapValue.addLong256(valueIndex, long256A);
            }
        }
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final Long256 value = arg.getLong256A(record);
        mapValue.putLong256(valueIndex, value);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final Long256 value = arg.getLong256A(record);
        if (!value.equals(Long256Impl.NULL_LONG256)) {
            final Long256 sum = mapValue.getLong256A(valueIndex);
            if (!sum.equals(Long256Impl.NULL_LONG256)) {
                Long256Util.add(sum, value);
                mapValue.putLong256(valueIndex, sum);
            } else {
                mapValue.putLong256(valueIndex, value);
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        Long256Impl v = (Long256Impl) getLong256A(rec);
        v.toSink(sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        return getLong256(rec, long256A);
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return getLong256(rec, long256B);
    }

    @Override
    public String getName() {
        return "sum";
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
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG256);
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
    public void merge(MapValue destValue, MapValue srcValue) {
        final Long256 src = srcValue.getLong256A(valueIndex);
        if (Long256Impl.isNull(src)) {
            return;
        }
        final Long256 dest = destValue.getLong256A(valueIndex);
        if (Long256Impl.isNull(dest)) {
            destValue.putLong256(valueIndex, src);
        } else {
            destValue.addLong256(valueIndex, src);
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong256(valueIndex, Long256Impl.NULL_LONG256);
    }

    @Override
    public boolean supportsBatchComputation() {
        return true;
    }

    @Override
    public boolean supportsParallelism() {
        return true;
    }

    private Long256 getLong256(Record rec, Long256Impl long256) {
        long256.copyFrom(rec.getLong256A(valueIndex));
        return long256;
    }
}
