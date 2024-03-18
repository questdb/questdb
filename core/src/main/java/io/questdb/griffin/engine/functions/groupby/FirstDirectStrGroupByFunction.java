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
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import io.questdb.std.str.DirectCharSequence;
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.NotNull;

public class FirstDirectStrGroupByFunction extends StrFunction implements GroupByFunction, UnaryFunction {
    protected final Function arg;
    protected final DirectString viewA = new DirectString();
    protected final DirectString viewB = new DirectString();
    protected int valueIndex;

    public FirstDirectStrGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putLong(valueIndex, rowId);
        final DirectCharSequence val = arg.getDirectStr(record);
        if (val == null) {
            mapValue.putLong(valueIndex + 1, 0);
            mapValue.putInt(valueIndex + 2, TableUtils.NULL_LEN);
        } else {
            mapValue.putLong(valueIndex + 1, val.ptr());
            mapValue.putInt(valueIndex + 2, val.length());
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        // empty
    }

    @Override
    public Function getArg() {
        return this.arg;
    }

    @Override
    public String getName() {
        return "first";
    }

    @Override
    public CharSequence getStrA(Record rec) {
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
        long srcRowId = srcValue.getLong(valueIndex);
        long destRowId = destValue.getLong(valueIndex);
        if (srcRowId != Numbers.LONG_NaN && (srcRowId < destRowId || destRowId == Numbers.LONG_NaN)) {
            destValue.putLong(valueIndex, srcRowId);
            destValue.putLong(valueIndex + 1, srcValue.getLong(valueIndex + 1));
            destValue.putInt(valueIndex + 2, srcValue.getInt(valueIndex + 2));
        }
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG); // row id
        columnTypes.add(ColumnType.LONG); // direct string pointer
        columnTypes.add(ColumnType.INT);  // string length (in chars), TableUtils.NULL_LEN stands for null string
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NaN);
        mapValue.putLong(valueIndex + 1, 0);
        mapValue.putInt(valueIndex + 2, TableUtils.NULL_LEN);
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
    public void toTop() {
        UnaryFunction.super.toTop();
    }

    private CharSequence getStr(Record rec, DirectString view) {
        final int len = rec.getInt(valueIndex + 2);
        if (len == TableUtils.NULL_LEN) {
            return null;
        }
        final long ptr = rec.getLong(valueIndex + 1);
        return view.of(ptr, len);
    }
}
