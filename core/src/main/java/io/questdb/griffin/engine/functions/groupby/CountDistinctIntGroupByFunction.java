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
import io.questdb.griffin.engine.groupby.GroupByIntHashSet;
import io.questdb.std.Numbers;

public class CountDistinctIntGroupByFunction extends AbstractCountDistinctIntGroupByFunction {

    public CountDistinctIntGroupByFunction(Function arg, int setInitialCapacity, double setLoadFactor) {
        super(
                arg,
                new GroupByIntHashSet(setInitialCapacity, setLoadFactor, Numbers.INT_NULL),
                new GroupByIntHashSet(setInitialCapacity, setLoadFactor, Numbers.INT_NULL)
        );
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final int value = arg.getInt(record);
        if (value != Numbers.INT_NULL) {
            mapValue.putLong(valueIndex, 1);
            mapValue.putLong(valueIndex + 1, value);
            cardinality++;
        } else {
            mapValue.putLong(valueIndex, 0);
            mapValue.putLong(valueIndex + 1, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final int value = arg.getInt(record);
        if (value != Numbers.INT_NULL) {
            final long cnt = mapValue.getLong(valueIndex);
            if (cnt == 0) {
                mapValue.putLong(valueIndex, 1);
                mapValue.putLong(valueIndex + 1, value);
                cardinality++;
            } else if (cnt == 1) { // inlined value
                final int valueB = (int) mapValue.getLong(valueIndex + 1);
                if (value != valueB) {
                    setA.of(0).add(value);
                    setA.add(valueB);
                    mapValue.putLong(valueIndex, 2);
                    mapValue.putLong(valueIndex + 1, setA.ptr());
                    cardinality++;
                }
            } else { // non-empty set
                final long ptr = mapValue.getLong(valueIndex + 1);
                final long index = setA.of(ptr).keyIndex(value);
                if (index >= 0) {
                    setA.addAt(index, value);
                    mapValue.putLong(valueIndex, cnt + 1);
                    mapValue.putLong(valueIndex + 1, setA.ptr());
                    cardinality++;
                }
            }
        }
    }
}
