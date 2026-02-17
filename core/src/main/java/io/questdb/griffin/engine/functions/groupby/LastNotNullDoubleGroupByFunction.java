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

import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class LastNotNullDoubleGroupByFunction extends FirstDoubleGroupByFunction {

    public LastNotNullDoubleGroupByFunction(@NotNull Function arg) {
        super(arg);
    }

    @Override
    public void computeBatch(MapValue mapValue, long ptr, int count) {
        if (count > 0) {
            long hi = ptr + (count - 1) * 8L;
            for (; hi >= ptr; hi -= 8L) {
                double value = Unsafe.getUnsafe().getDouble(hi);
                if (!Numbers.isNull(value)) {
                    mapValue.putDouble(valueIndex + 1, value);
                    break;
                }
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        if (Numbers.isFinite(arg.getDouble(record))) {
            computeFirst(mapValue, record, rowId);
        }
    }

    @Override
    public String getName() {
        return "last_not_null";
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        double srcVal = srcValue.getDouble(valueIndex + 1);
        if (Numbers.isNull(srcVal)) {
            return;
        }
        long srcRowId = srcValue.getLong(valueIndex);
        long destRowId = destValue.getLong(valueIndex);
        if (srcRowId > destRowId) {
            destValue.putLong(valueIndex, srcRowId);
            destValue.putDouble(valueIndex + 1, srcVal);
        }
    }
}
