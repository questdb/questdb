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

    //not-keyed aggregation that doesn't use rosti
    //used either for truly non-keyed aggregation or when key is null in page frame due to column tops  
    void aggregate(long address, long addressSize, int columnSizeHint, int workerId);

    //keyed aggregation that uses rosti 
    //if valueAddress == 0 it means that value page frame is 'empty' (due to column tops) - contains null values only 
    //so only keys should be processed 
    //returns true if processing went fine and false if it failed on memory allocation
    boolean aggregate(long pRosti, long keyAddress, long valueAddress, long valueAddressSize, int columnSizeShr, int workerId);

    int getColumnIndex();

    // value offset in map
    int getValueOffset();

    //set initial/default slot values (e.g. 0 for count())  
    void initRosti(long pRosti);

    //merge pRostiB into pRostiA
    //returns true if merge was fine and false if it failed on memory allocation 
    boolean merge(long pRostiA, long pRostiB);

    void pushValueTypes(ArrayColumnTypes types);

    //used for keyed aggregates only
    //merges value for null key (empty/null key page frames with rosti) and (optionally) replaces null values with constant in rosti      
    //returns true if wrapUp was fine and false if it failed on memory allocation
    boolean wrapUp(long pRosti);
}
