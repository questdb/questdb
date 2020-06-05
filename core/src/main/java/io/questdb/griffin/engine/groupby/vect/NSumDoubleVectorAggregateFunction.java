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
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.Misc;
import io.questdb.std.Vect;

import java.util.Arrays;

public class NSumDoubleVectorAggregateFunction extends DoubleFunction implements VectorAggregateFunction {

    private final int columnIndex;
    private final double[] sum;
    private final long[] count;
    private final int workerCount;
    private int valueOffset;

    public NSumDoubleVectorAggregateFunction(int position, int columnIndex, int workerCount) {
        super(position);
        this.columnIndex = columnIndex;
        this.sum = new double[workerCount * Misc.CACHE_LINE_SIZE];
        this.count = new long[workerCount * Misc.CACHE_LINE_SIZE];
        this.workerCount = workerCount;
    }

    @Override
    public int getValueOffset() {
        return valueOffset;
    }

    @Override
    public void aggregate(long address, long count, int workerId) {
        if (address != 0) {
            // Neumaier compensated summation
            final double x = Vect.sumDoubleNeumaier(address, count);
            if (x == x) {
                final int offset = workerId * Misc.CACHE_LINE_SIZE;
                final double sum = this.sum[offset];
                final double t = sum + x;
                double c = this.sum[offset + 1];
                if (Math.abs(sum) >= x) {
                    c += (sum - t) + x;
                } else {
                    c += (x - t) + sum;
                }
                this.sum[offset] = t; // sum = t
                this.sum[offset + 1] = c;
                this.count[offset]++;
            }
        }
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public void clear() {
        Arrays.fill(sum, 0);
        Arrays.fill(count, 0);
    }

    @Override
    public double getDouble(Record rec) {
        double sum = 0;
        long count = 0;
        double c = 0;
        for (int i = 0; i < workerCount; i++) {
            final int offset = i * Misc.CACHE_LINE_SIZE;
            double x = this.sum[offset] + this.sum[offset + 1];
            double t = sum + x;
            if (Math.abs(sum) >= x) {
                c += (sum - t) + x;
            } else {
                c += (x - t) + sum;
            }
            sum = t;
            count += this.count[offset];
        }
        return count > 0 ? sum + c : Double.NaN;
    }
}
