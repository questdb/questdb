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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.PlanSink;
import io.questdb.std.Mutable;

public interface VectorAggregateFunction extends Function, Mutable {

    /**
     * Non-keyed aggregation that doesn't use rosti.
     * Used for truly non-keyed aggregation and, in a keyed aggregation, for page frames whose key
     * column holds no data: a column top, or a Parquet row group whose key column is all NULL. All
     * rows of such a frame belong to the NULL key; see {@link #initNullKeyRosti(long)}.
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
     * Used for keyed aggregates only. Writes what {@link #aggregate(long, long, int)} accumulated over
     * the page frames whose key column holds no data into the initial slot values of the given rosti,
     * which has the same value columns as the keyed map. The values use the layout that the keyed
     * natives use for a key; if nothing was accumulated, they are the values of a key with no rows.
     * <p>
     * The caller then inserts the NULL key into that rosti, so the key starts with these values, and
     * {@link #merge(long, long)} merges it into the keyed map the same way it merges the NULL key of
     * a worker's map.
     *
     * @param pRosti pointer to rosti
     */
    void initNullKeyRosti(long pRosti);

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
     * Merges the value that {@link #aggregate(long, long, int)} accumulated into the null key, and
     * (optionally) replaces null values with constant in rosti.
     * <p>
     * The keyed GROUP BY does not rely on this merge: it merges the accumulated value through
     * {@link #initNullKeyRosti(long)} and {@link #merge(long, long)}, then calls {@link #clear()}
     * before this method, so there is nothing left to merge here. The natives' null key merge is
     * unsuitable because it adds onto values it has just set to NULL, and the avg() natives merge
     * the value as the wrong type.
     *
     * @param pRosti pointer to rosti
     * @return true if wrapUp was fine and false if it failed on memory allocation
     */
    boolean wrapUp(long pRosti);
}
