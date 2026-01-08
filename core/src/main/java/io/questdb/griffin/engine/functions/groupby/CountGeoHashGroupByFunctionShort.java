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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class CountGeoHashGroupByFunctionShort extends AbstractCountGroupByFunction {

    public CountGeoHashGroupByFunctionShort(@NotNull Function arg) {
        super(arg);
    }

    @Override
    public void computeBatch(MapValue mapValue, long ptr, int count) {
        if (count > 0) {
            long nonNullCount = 0;
            final long hi = ptr + count * 2L;
            for (; ptr < hi; ptr += 2) {
                if (Unsafe.getUnsafe().getShort(ptr) != GeoHashes.SHORT_NULL) {
                    nonNullCount++;
                }
            }
            mapValue.putLong(valueIndex, nonNullCount);
        }
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final short value = arg.getGeoShort(record);
        if (value != GeoHashes.SHORT_NULL) {
            mapValue.putLong(valueIndex, 1);
        } else {
            mapValue.putLong(valueIndex, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final short value = arg.getGeoShort(record);
        if (value != GeoHashes.SHORT_NULL) {
            mapValue.addLong(valueIndex, 1);
        }
    }

    @Override
    public int getComputeBatchArgType() {
        return ColumnType.GEOSHORT;
    }

    @Override
    public boolean supportsBatchComputation() {
        return true;
    }
}
