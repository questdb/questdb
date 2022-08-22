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

package io.questdb.griffin.engine.groupby.vect;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.std.Mutable;

public interface VectorAggregateFunction extends Function, Mutable {

    void aggregate(long address, long addressSize, int columnSizeHint, int workerId);

    boolean aggregate(long pRosti, long keyAddress, long valueAddress, long valueAddressSize, int columnSizeShr, int workerId);

    int getColumnIndex();

    // value offset in map
    int getValueOffset();

    void initRosti(long pRosti);

    //returns true if merge was fine and false if it failed on memory allocation 
    boolean merge(long pRostiA, long pRostiB);

    void pushValueTypes(ArrayColumnTypes types);

    // sets null as result of aggregation of all nulls
    // this typically checks non-null count and replaces 0 with null if all values were null
    void wrapUp(long pRosti);
}
