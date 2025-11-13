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

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiPredicate;

public abstract class NotNullArrayGroupByFunction extends FirstArrayGroupByFunction {
    private final BiPredicate<Long, Long> mergePred;

    public NotNullArrayGroupByFunction(@NotNull Function arg, BiPredicate<Long, Long> mergePred) {
        super(arg);
        this.mergePred = mergePred;
    }

    protected abstract boolean shouldSkipUpdate(MapValue mapValue);

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putLong(valueIndex, rowId);
        ArrayView array = arg.getArray(record);
        if (array == null || array.isNull()) {
            mapValue.putLong(valueIndex + 1, 0);
        } else {
            sink.of(0);
            sink.put(array);
            mapValue.putLong(valueIndex + 1, sink.ptr());
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        if (shouldSkipUpdate(mapValue)) {
            return;
        }
        updateValue(mapValue, record, rowId);
    }

    @Override
    public ArrayView getArray(Record rec) {
        if (rec.getLong(valueIndex + 1) == 0) {
            return ArrayConstant.NULL;
        }
        return super.getArray(rec);
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        if (srcValue.getLong(valueIndex + 1) == 0) {
            return;
        }
        long srcRowId = srcValue.getLong(valueIndex);
        long destRowId = destValue.getLong(valueIndex);
        if (mergePred.test(srcRowId, destRowId) || destRowId == Numbers.LONG_NULL) {
            destValue.putLong(valueIndex, srcRowId);
            destValue.putLong(valueIndex + 1, srcValue.getLong(valueIndex + 1));
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NULL);
        mapValue.putLong(valueIndex + 1, 0);
    }

    protected void updateValue(MapValue mapValue, Record record, long rowId) {
        ArrayView array = arg.getArray(record);
        if (array != null && !array.isNull()) {
            mapValue.putLong(valueIndex, rowId);
            long ptr = mapValue.getLong(valueIndex + 1);
            sink.of(ptr);
            sink.put(array);
            mapValue.putLong(valueIndex + 1, sink.ptr());
        }
    }
}
