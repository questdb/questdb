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

package io.questdb.griffin.engine.groupby.vect;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.PlanSink;
import io.questdb.std.Mutable;

public interface VectorAggregateFunction extends Function, Mutable {

    /**
     * Non-keyed aggregation that doesn't use rosti.
     * Used either for truly non-keyed aggregation or when key is null in page frame due to column tops.
     *
     * @param address       address
     * @param frameRowCount row count int the frame; this is provided to "count" functions
     * @param workerId      worker id
     */
    void aggregate(long address, long frameRowCount, int workerId);


    /**
     * Keyed aggregation that uses rosti.
     * If valueAddress == 0 it means that value page frame is 'empty' (due to column tops) and contains null values,
     * so only keys should be processed.
     *
     * @param pRosti        pointer to rosti
     * @param keyAddress    key address
     * @param valueAddress  value address
     * @param frameRowCount row count in the frame
     * @return true if processing went fine and false if it failed on memory allocation
     */
    boolean aggregate(long pRosti, long keyAddress, long valueAddress, long frameRowCount);

    /**
     * Returns the column index for this aggregate function.
     *
     * @return the column index
     */
    int getColumnIndex();

    /**
     * Returns the value offset in the map.
     *
     * @return the value offset
     */
    int getValueOffset();

    /**
     * Set initial/default slot values (e.g. 0 for count())
     *
     * @param pRosti pointer to rosti
     */
    void initRosti(long pRosti);

    /**
     * Merge rosti instance pointed to by pRostiB into rosti instance pointed to by pRostiA.
     *
     * @param pRostiA pointer to rosti that will hold result
     * @param pRostiB pointer to other rosti to merge
     * @return true if merge was fine and false if it failed on memory allocation
     */
    boolean merge(long pRostiA, long pRostiB);

    /**
     * Pushes value types to the column types array.
     *
     * @param types the column types array to add value types to
     */
    void pushValueTypes(ArrayColumnTypes types);

    @Override
    default void toPlan(PlanSink sink) {
        sink.val(getName()).val('(').putColumnName(getColumnIndex()).val(')');
    }

    /**
     * Used for keyed aggregates only.
     * Merges value for null key (empty/null key page frames with rosti) and (optionally) replaces null values with constant in rosti.
     *
     * @param pRosti pointer to rosti
     * @return true if wrapUp was fine and false if it failed on memory allocation
     */
    boolean wrapUp(long pRosti);
}
