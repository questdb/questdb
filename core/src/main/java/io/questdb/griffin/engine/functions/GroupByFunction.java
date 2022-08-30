/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Mutable;

public interface GroupByFunction extends Function, Mutable {

    @Override
    default void clear() {
    }

    default void interpolateBoundary(MapValue mapValue1,
                                     MapValue mapValue2,
                                     long boundaryTimestamp,
                                     boolean isEndOfBoundary) {
        throw new UnsupportedOperationException();
    }

    default void interpolateGap(MapValue mapValue,
                                MapValue mapValue1,
                                MapValue mapValue2,
                                long x) {
        throw new UnsupportedOperationException();
    }

    void computeFirst(MapValue mapValue, Record record);

    void computeNext(MapValue mapValue, Record record);

    default boolean isScalar() {
        return true;
    }

    void pushValueTypes(ArrayColumnTypes columnTypes);

    default void setByte(MapValue mapValue, byte value) {
        throw new UnsupportedOperationException();
    }

    default void setDouble(MapValue mapValue, double value) {
        throw new UnsupportedOperationException();
    }

    default void setEmpty(MapValue value) {
        setNull(value);
    }

    default void setFloat(MapValue mapValue, float value) {
        throw new UnsupportedOperationException();
    }

    default void setInt(MapValue mapValue, int value) {
        throw new UnsupportedOperationException();
    }

    default void setLong(MapValue mapValue, long value) {
        throw new UnsupportedOperationException();
    }

    void setNull(MapValue mapValue);

    default void setShort(MapValue mapValue, short value) {
        throw new UnsupportedOperationException();
    }
}
