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

import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class CountLongGroupByFunction extends AbstractCountGroupByFunction {

    public CountLongGroupByFunction(@NotNull Function arg) {
        super(arg);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        final long value = arg.getLong(record);
        if (value != Numbers.LONG_NaN) {
            mapValue.putLong(valueIndex, 1);
        } else {
            mapValue.putLong(valueIndex, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        final long value = arg.getLong(record);
        if (value != Numbers.LONG_NaN) {
            mapValue.addLong(valueIndex, 1);
        }
    }
}
