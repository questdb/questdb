/*+*****************************************************************************
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

package io.questdb.griffin.engine.groupby.vect;

import io.questdb.std.Chars;
import io.questdb.std.IntObjHashMap;

import static io.questdb.cairo.ColumnType.*;
import static io.questdb.griffin.SqlKeywords.isCountKeyword;
import static io.questdb.griffin.SqlKeywords.isSumKeyword;

/**
 * The vector implementations of the aggregate functions, by name and argument type.
 */
public final class VectorAggregateConstructors {
    private static final VectorAggregateFunctionConstructor COUNT_CONSTRUCTOR = (keyKind, _, _, _) -> new CountVectorAggregateFunction(keyKind);
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> avgConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> countConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> ksumConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> maxConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> minConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> nsumConstructors = new IntObjHashMap<>();
    private static final IntObjHashMap<VectorAggregateFunctionConstructor> sumConstructors = new IntObjHashMap<>();

    public static VectorAggregateFunctionConstructor of(
            CharSequence name, int argumentType, boolean isCountAll
    ) {
        if (isSumKeyword(name)) {
            return sumConstructors.get(argumentType);
        }
        if (isCountKeyword(name)) {
            return isCountAll ? COUNT_CONSTRUCTOR : countConstructors.get(argumentType);
        }
        if (Chars.equalsIgnoreCase(name, "ksum")) {
            return ksumConstructors.get(argumentType);
        }
        if (Chars.equalsIgnoreCase(name, "nsum")) {
            return nsumConstructors.get(argumentType);
        }
        if (Chars.equalsIgnoreCase(name, "avg")) {
            return avgConstructors.get(argumentType);
        }
        if (Chars.equalsIgnoreCase(name, "min")) {
            return minConstructors.get(argumentType);
        }
        if (Chars.equalsIgnoreCase(name, "max")) {
            return maxConstructors.get(argumentType);
        }
        return null;
    }

    static {
        countConstructors.put(DOUBLE, CountDoubleVectorAggregateFunction::new);
        countConstructors.put(INT, CountIntVectorAggregateFunction::new);
        countConstructors.put(LONG, CountLongVectorAggregateFunction::new);
        countConstructors.put(DATE, CountLongVectorAggregateFunction::new);
        countConstructors.put(TIMESTAMP_MICRO, CountLongVectorAggregateFunction::new);
        countConstructors.put(TIMESTAMP_NANO, CountLongVectorAggregateFunction::new);

        sumConstructors.put(DOUBLE, SumDoubleVectorAggregateFunction::new);
        sumConstructors.put(INT, SumIntVectorAggregateFunction::new);
        sumConstructors.put(LONG, SumLongVectorAggregateFunction::new);
        sumConstructors.put(LONG256, SumLong256VectorAggregateFunction::new);
        sumConstructors.put(SHORT, SumShortVectorAggregateFunction::new);

        ksumConstructors.put(DOUBLE, KSumDoubleVectorAggregateFunction::new);
        nsumConstructors.put(DOUBLE, NSumDoubleVectorAggregateFunction::new);

        avgConstructors.put(DOUBLE, AvgDoubleVectorAggregateFunction::new);
        avgConstructors.put(LONG, AvgLongVectorAggregateFunction::new);
        avgConstructors.put(INT, AvgIntVectorAggregateFunction::new);
        avgConstructors.put(SHORT, AvgShortVectorAggregateFunction::new);

        minConstructors.put(DOUBLE, MinDoubleVectorAggregateFunction::new);
        minConstructors.put(LONG, MinLongVectorAggregateFunction::new);
        minConstructors.put(DATE, MinDateVectorAggregateFunction::new);
        minConstructors.put(TIMESTAMP_MICRO, (int keyKind, int columnIndex, int timestampIndex, int _) -> new MinTimestampVectorAggregateFunction(keyKind, columnIndex, TIMESTAMP_MICRO, timestampIndex));
        minConstructors.put(TIMESTAMP_NANO, (int keyKind, int columnIndex, int timestampIndex, int _) -> new MinTimestampVectorAggregateFunction(keyKind, columnIndex, TIMESTAMP_NANO, timestampIndex));
        minConstructors.put(INT, MinIntVectorAggregateFunction::new);
        minConstructors.put(SHORT, MinShortVectorAggregateFunction::new);

        maxConstructors.put(DOUBLE, MaxDoubleVectorAggregateFunction::new);
        maxConstructors.put(LONG, MaxLongVectorAggregateFunction::new);
        maxConstructors.put(DATE, MaxDateVectorAggregateFunction::new);
        maxConstructors.put(TIMESTAMP_MICRO, (int keyKind, int columnIndex, int timestampIndex, int _) -> new MaxTimestampVectorAggregateFunction(keyKind, columnIndex, TIMESTAMP_MICRO, timestampIndex));
        maxConstructors.put(TIMESTAMP_NANO, (int keyKind, int columnIndex, int timestampIndex, int _) -> new MaxTimestampVectorAggregateFunction(keyKind, columnIndex, TIMESTAMP_NANO, timestampIndex));
        maxConstructors.put(INT, MaxIntVectorAggregateFunction::new);
        maxConstructors.put(SHORT, MaxShortVectorAggregateFunction::new);
    }
}
