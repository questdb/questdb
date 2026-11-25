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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByBatchKernels;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

/**
 * Runs the batch kernels of several Welford aggregates of one group (stddev, variance and corr, see
 * {@link #isFusable}) in one pass over a batch's rows. Each aggregate's update is a chain of divisions
 * by its running count, one after another; in one loop the chains of different aggregates overlap.
 * Every aggregate's arithmetic is its own kernel's, in the same order, so the states are bit for bit
 * those its {@code computeBatchKernel()} would leave.
 */
public final class WelfordBatchFusion {
    private static final int KIND_CORR = 2;
    private static final int KIND_STDDEV = 1;
    private long[] addresses0 = new long[4];
    private long[] addresses1 = new long[4];
    private int count;
    private long[] counts = new long[4];
    private int[] kinds = new int[4];
    private double[] state = new double[4 * 5];
    private int[] types0 = new int[4];
    private int[] types1 = new int[4];
    private int[] valueIndexes = new int[4];

    public static int countFusable(io.questdb.std.ObjList<GroupByFunction> functions) {
        int n = 0;
        for (int i = 0, size = functions.size(); i < size; i++) {
            if (isFusable(functions.getQuick(i))) {
                n++;
            }
        }
        return n;
    }

    public static boolean isFusable(GroupByFunction function) {
        return function instanceof AbstractStdDevGroupByFunction || function instanceof CorrGroupByFunctionFactory.CorrGroupByFunction;
    }

    /**
     * Takes a fusable aggregate (see {@link #isFusable}) whose kernel arguments are evaluated for the batch.
     */
    public void add(GroupByFunction function, GroupByBatchKernels.Args args) {
        if (count == kinds.length) {
            grow();
        }
        final int j = count++;
        valueIndexes[j] = function.getValueIndex();
        addresses0[j] = args.address(0);
        types0[j] = args.type(0);
        if (function instanceof CorrGroupByFunctionFactory.CorrGroupByFunction) {
            kinds[j] = KIND_CORR;
            addresses1[j] = args.address(1);
            types1[j] = args.type(1);
        } else {
            kinds[j] = KIND_STDDEV;
        }
    }

    public void clear() {
        count = 0;
    }

    public int size() {
        return count;
    }

    /**
     * Updates the taken aggregates' states in the map value over the batch's {@code rowCount} rows, then
     * forgets them.
     */
    public void run(MapValue mapValue, int rowCount) {
        final int n = count;
        final double[] s = state;
        final long[] c = counts;
        for (int j = 0; j < n; j++) {
            final int v = valueIndexes[j];
            final int at = j * 5;
            if (kinds[j] == KIND_STDDEV) {
                // AbstractStdDevGroupByFunction: mean, sum, count
                s[at] = mapValue.getDouble(v);
                s[at + 1] = mapValue.getDouble(v + 1);
                c[j] = mapValue.getLong(v + 2);
            } else {
                // corr: meanY, sumY, meanX, sumX, sumXY, count
                s[at] = mapValue.getDouble(v);
                s[at + 1] = mapValue.getDouble(v + 1);
                s[at + 2] = mapValue.getDouble(v + 2);
                s[at + 3] = mapValue.getDouble(v + 3);
                s[at + 4] = mapValue.getDouble(v + 4);
                c[j] = mapValue.getLong(v + 5);
            }
        }
        for (int i = 0; i < rowCount; i++) {
            for (int j = 0; j < n; j++) {
                final int at = j * 5;
                if (kinds[j] == KIND_STDDEV) {
                    // AbstractStdDevGroupByFunction.computeBatchKernel()
                    final double value = read(addresses0[j], types0[j], i);
                    if (Numbers.isFinite(value)) {
                        final long cnt = ++c[j];
                        double mean = s[at];
                        final double oldMean = mean;
                        mean += (value - mean) / cnt;
                        s[at] = mean;
                        s[at + 1] += (value - mean) * (value - oldMean);
                    }
                } else {
                    // CorrGroupByFunction.computeBatchKernel()
                    final double y = read(addresses0[j], types0[j], i);
                    final double x = read(addresses1[j], types1[j], i);
                    if (Numbers.isFinite(y) && Numbers.isFinite(x)) {
                        final long cnt = ++c[j];
                        double meanY = s[at];
                        final double oldMeanY = meanY;
                        meanY += (y - meanY) / cnt;
                        s[at + 1] += (y - meanY) * (y - oldMeanY);
                        double meanX = s[at + 2];
                        final double oldMeanX = meanX;
                        meanX += (x - meanX) / cnt;
                        s[at + 3] += (x - meanX) * (x - oldMeanX);
                        s[at + 4] += (y - oldMeanY) * (x - meanX);
                        s[at] = meanY;
                        s[at + 2] = meanX;
                    }
                }
            }
        }
        for (int j = 0; j < n; j++) {
            final int v = valueIndexes[j];
            final int at = j * 5;
            if (kinds[j] == KIND_STDDEV) {
                mapValue.putDouble(v, s[at]);
                mapValue.putDouble(v + 1, s[at + 1]);
                mapValue.putLong(v + 2, c[j]);
            } else {
                mapValue.putDouble(v, s[at]);
                mapValue.putDouble(v + 1, s[at + 1]);
                mapValue.putDouble(v + 2, s[at + 2]);
                mapValue.putDouble(v + 3, s[at + 3]);
                mapValue.putDouble(v + 4, s[at + 4]);
                mapValue.putLong(v + 5, c[j]);
            }
        }
        count = 0;
    }

    // a kernel argument's value i, as the aggregates' kernels read it
    private static double read(long address, int type, int i) {
        return type == ColumnType.DOUBLE ? Unsafe.getDouble(address + ((long) i << 3)) : GroupByBatchKernels.rawDouble(address, type, i);
    }

    private void grow() {
        final int capacity = kinds.length * 2;
        addresses0 = java.util.Arrays.copyOf(addresses0, capacity);
        addresses1 = java.util.Arrays.copyOf(addresses1, capacity);
        counts = java.util.Arrays.copyOf(counts, capacity);
        kinds = java.util.Arrays.copyOf(kinds, capacity);
        state = java.util.Arrays.copyOf(state, capacity * 5);
        types0 = java.util.Arrays.copyOf(types0, capacity);
        types1 = java.util.Arrays.copyOf(types1, capacity);
        valueIndexes = java.util.Arrays.copyOf(valueIndexes, capacity);
    }
}
