/*******************************************************************************
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
    public void setNull(MapValue mapValue) {
        mapValue.putLong256(valueIndex, Long256Impl.NULL_LONG256);
    }

    @Override
    public boolean supportsParallelism() {
        return false;
    }

    private Long256 getLong256(Record rec, Long256Impl long256) {
        long256.copyFrom(rec.getLong256A(valueIndex));
        return long256;
    }
}
