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

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

import java.util.concurrent.atomic.LongAdder;

public class SumIntVectorAggregateFunction extends LongFunction implements VectorAggregateFunction {
    private final LongAdder sum = new LongAdder();
    private final LongAdder count = new LongAdder();

    private final int columnIndex;

    public SumIntVectorAggregateFunction(int position, int columnIndex) {
        super(position);
        this.columnIndex = columnIndex;
    }

    @Override
    public void aggregate(long address, long count) {
        if (address != 0) {
            final long value = Vect.sumInt(address, count);
            if (value != Numbers.LONG_NaN) {
                this.sum.add(value);
                this.count.increment();
            }
        }
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public void clear() {
        this.sum.reset();
        this.count.reset();
    }

    @Override
    public long getLong(Record rec) {
        return this.count.sum() > 0 ? this.sum.sum() : Numbers.LONG_NaN;
    }
}
