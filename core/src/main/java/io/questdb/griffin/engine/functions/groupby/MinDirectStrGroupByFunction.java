/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Chars;
import io.questdb.std.str.DirectCharSequence;
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.NotNull;

public class MinDirectStrGroupByFunction extends StrFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final DirectString viewA = new DirectString();
    private final DirectString viewB = new DirectString();
    private int valueIndex;

    public MinDirectStrGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final DirectCharSequence val = arg.getDirectStr(record);
        if (val == null) {
            mapValue.putLong(valueIndex, 0);
            mapValue.putInt(valueIndex + 1, TableUtils.NULL_LEN);
        } else {
            mapValue.putLong(valueIndex, val.ptr());
            mapValue.putInt(valueIndex + 1, val.length());
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final DirectCharSequence val = arg.getDirectStr(record);
        if (val != null) {
            final long ptr = mapValue.getLong(valueIndex);
            if (ptr == 0 || Chars.compare(viewA.of(ptr, mapValue.getInt(valueIndex + 1)), val) > 0) {
                mapValue.putLong(valueIndex, val.ptr());
                mapValue.putInt(valueIndex + 1, val.length());
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public CharSequence getStr(Record rec) {
        return getStr(rec, viewA);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return getStr(rec, viewB);
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isReadThreadSafe() {
        return false;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcPtr = srcValue.getLong(valueIndex);
        if (srcPtr == 0) {
            return;
        }
        int srcLen = srcValue.getInt(valueIndex + 1);

        long destPtr = destValue.getLong(valueIndex);
        if (destPtr == 0 || Chars.compare(viewA.of(destPtr, destValue.getInt(valueIndex + 1)), viewA.of(srcPtr, srcLen)) > 0) {
            destValue.putLong(valueIndex, srcPtr);
            destValue.putInt(valueIndex + 1, srcLen);
        }
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG); // direct string pointer
        columnTypes.add(ColumnType.INT);  // string length (in chars), TableUtils.NULL_LEN stands for null string
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
        mapValue.putInt(valueIndex + 1, TableUtils.NULL_LEN);
    }

    @Override
    public void setValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("min(").val(arg).val(')');
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
    }

    private CharSequence getStr(Record rec, DirectString view) {
        final int len = rec.getInt(valueIndex + 1);
        if (len == TableUtils.NULL_LEN) {
            return null;
        }
        final long ptr = rec.getLong(valueIndex);
        return view.of(ptr, len);
    }
}
