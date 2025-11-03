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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class FirstNonNullArrayGroupByFunction extends FirstArrayGroupByFunction {

    public FirstNonNullArrayGroupByFunction(@NotNull Function arg) {
        super(arg);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putLong(valueIndex, rowId);
        ArrayView array = arg.getArray(record);
        if (array == null || array.isNull()) {
            mapValue.putLong(valueIndex + 1, 0);
            mapValue.putBool(valueIndex + 2, true);
        } else {
            sink.of(0);
            sink.put(array);
            mapValue.putLong(valueIndex + 1, sink.ptr());
            mapValue.putBool(valueIndex + 2, false);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        if (mapValue.getBool(valueIndex + 2)) {
            ArrayView array = arg.getArray(record);
            if (array != null && !array.isNull()) {
                mapValue.putLong(valueIndex, rowId);
                long ptr = mapValue.getLong(valueIndex + 1);
                sink.of(ptr);
                sink.put(array);
                mapValue.putLong(valueIndex + 1, sink.ptr());
                mapValue.putBool(valueIndex + 2, false);
            }
        }
    }

    @Override
    public ArrayView getArray(Record rec) {
        if (rec.getBool(valueIndex + 2)) {
            return ArrayConstant.NULL;
        }
        return super.getArray(rec);
    }

    @Override
    public String getName() {
        return "first_non_null";
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        super.initValueTypes(columnTypes);
        columnTypes.add(ColumnType.BOOLEAN);
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        if (srcValue.getBool(valueIndex + 2)) {
            return;
        }
        long srcRowId = srcValue.getLong(valueIndex);
        long destRowId = destValue.getLong(valueIndex);
        if (srcRowId < destRowId || destRowId == Numbers.LONG_NULL) {
            destValue.putLong(valueIndex, srcRowId);
            destValue.putLong(valueIndex + 1, srcValue.getLong(valueIndex + 1));
            destValue.putBool(valueIndex + 2, false);
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NULL);
        mapValue.putLong(valueIndex + 1, 0);
        mapValue.putBool(valueIndex + 2, true);
    }
}
