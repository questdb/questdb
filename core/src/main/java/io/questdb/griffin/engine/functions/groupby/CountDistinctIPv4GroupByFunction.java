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
import io.questdb.griffin.engine.groupby.GroupByIntHashSet;
import io.questdb.griffin.engine.groupby.GroupByLongList;
import io.questdb.std.Numbers;

public class CountDistinctIPv4GroupByFunction extends AbstractCountDistinctIntGroupByFunction {

    public CountDistinctIPv4GroupByFunction(Function arg, int setInitialCapacity, double setLoadFactor, int workerCount) {
        super(
                arg,
                // Numbers.IPv4_NULL is zero which is nice for faster zeroing on rehash.
                new GroupByIntHashSet(setInitialCapacity, setLoadFactor, Numbers.IPv4_NULL),
                new GroupByIntHashSet(setInitialCapacity, setLoadFactor, Numbers.IPv4_NULL),
                new GroupByLongList(Math.max(workerCount, 4))
        );
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final int value = arg.getIPv4(record);
        if (value != Numbers.IPv4_NULL) {
            mapValue.putLong(valueIndex, 1);
            mapValue.putLong(valueIndex + 1, value);
        } else {
            mapValue.putLong(valueIndex, 0);
            mapValue.putLong(valueIndex + 1, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final int value = arg.getIPv4(record);
        if (value != Numbers.IPv4_NULL) {
            final long cnt = mapValue.getLong(valueIndex);
            if (cnt == 0) {
                mapValue.putLong(valueIndex, 1);
                mapValue.putLong(valueIndex + 1, value);
            } else if (cnt == 1) { // inlined value
                final int valueB = (int) mapValue.getLong(valueIndex + 1);
                if (value != valueB) {
                    setA.of(0).add(value);
                    setA.add(valueB);
                    mapValue.putLong(valueIndex, 2);
                    mapValue.putLong(valueIndex + 1, setA.ptr());
                }
            } else { // non-empty set
                final long ptr = mapValue.getLong(valueIndex + 1);
                final long index = setA.of(ptr).keyIndex(value);
                if (index >= 0) {
                    setA.addAt(index, value);
                    mapValue.putLong(valueIndex, cnt + 1);
                    mapValue.putLong(valueIndex + 1, setA.ptr());
                }
            }
        }
    }
}
