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

import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class FirstNotNullTimestampGroupByFunction extends FirstTimestampGroupByFunction {
    public FirstNotNullTimestampGroupByFunction(@NotNull Function arg, int timestampType) {
        super(arg, timestampType);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        if (mapValue.getTimestamp(valueIndex + 1) == Numbers.LONG_NULL) {
            computeFirst(mapValue, record, rowId);
        }
    }

    @Override
    public String getName() {
        return "first_not_null";
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcVal = srcValue.getTimestamp(valueIndex + 1);
        if (srcVal == Numbers.LONG_NULL) {
            return;
        }
        long srcRowId = srcValue.getLong(valueIndex);
        long destRowId = destValue.getLong(valueIndex);
        // srcRowId is non-null at this point since we know that the value is non-null
        if (srcRowId < destRowId || destRowId == Numbers.LONG_NULL) {
            destValue.putLong(valueIndex, srcRowId);
            destValue.putLong(valueIndex + 1, srcVal);
        }
    }
}
