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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;

import java.util.concurrent.atomic.LongAdder;


/**
 * Abstract base class for count vector aggregate functions.
 */
public abstract class AbstractCountVectorAggregateFunction extends LongFunction implements VectorAggregateFunction {
    /**
     * Counts number of null key aggregations.
     */
    protected final LongAdder aggCount = new LongAdder();
    /**
     * The main count accumulator.
     */
    protected final LongAdder count = new LongAdder();
    /**
     * The column index.
     */
    private final int columnIndex;
    /**
     * The distinct function for distinct aggregates.
     */
    protected DistinctFunc distinctFunc;
    /**
     * The key-value function.
     */
    protected KeyValueFunc keyValueFunc;
    /**
     * The value offset in the map.
     */
    protected int valueOffset;

    /**
     * Constructs a new count vector aggregate function.
     *
     * @param columnIndex the column index
     */
    public AbstractCountVectorAggregateFunction(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public void clear() {
        count.reset();
        aggCount.reset();
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public long getLong(Record rec) {
        return count.sum();
    }

    @Override
    public int getValueOffset() {
        return valueOffset;
    }

    @Override
    public void initRosti(long pRosti) {
        Unsafe.getUnsafe().putLong(Rosti.getInitialValueSlot(pRosti, valueOffset), 0);
    }

    @Override
    public boolean merge(long pRostiA, long pRostiB) {
        return Rosti.keyedIntCountMerge(pRostiA, pRostiB, valueOffset);
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes types) {
        this.valueOffset = types.getColumnCount();
        types.add(ColumnType.LONG);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("count(").putBaseColumnName(columnIndex).val(')');
    }

    @Override
    public boolean wrapUp(long pRosti) {
        return Rosti.keyedIntCountWrapUp(pRosti, valueOffset, aggCount.sum() > 0 ? count.sum() : Long.MIN_VALUE);
    }
}
