/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Mutable;

public interface VectorAggregateFunction extends Function, Mutable {

    void aggregate(long address, long count, int workerId);

    default void aggregate(long pRosti, long keyAddress, long valueAddress, long count, int workerId) {
        throw new UnsupportedOperationException();
    }

    default void pushValueTypes(ArrayColumnTypes types) {
        throw new UnsupportedOperationException();
    }

    int getColumnIndex();

    default void merge(long pRostiA, long pRostiB) {
        throw new UnsupportedOperationException();
    }

    // sets null as result of aggregation of all nulls
    // this typically checks non-null count and replaces 0 with null if all values were null
    default void setNull(long pRosti) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
    }
}
