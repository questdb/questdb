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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByBatchKernels;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class WeightedAvgDoubleGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
    private final Function sampleArg;
    private final Function weightArg;
    private int valueIndex;

    public WeightedAvgDoubleGroupByFunction(@NotNull Function sampleArg, @NotNull Function weightArg) {
        this.sampleArg = sampleArg;
        this.weightArg = weightArg;
    }

    @Override
    public void computeBatchKernel(MapValue mapValue, int rowCount, GroupByBatchKernels.Args args) {
        final long samplesAddr = args.address(0);
        final long weightsAddr = args.address(1);
        final int sampleType = args.type(0);
        final int weightType = args.type(1);
        double sum = mapValue.getDouble(valueIndex);
        double weightSum = mapValue.getDouble(valueIndex + 1);
        if (sampleType == ColumnType.DOUBLE && weightType == ColumnType.DOUBLE) {
            // four independent sums, so that the adds overlap rather than wait on one another
            double s1 = 0, s2 = 0, s3 = 0, w1 = 0, w2 = 0, w3 = 0;
            int i = 0;
            for (final int n4 = rowCount & ~3; i < n4; i += 4) {
                final long o = (long) i << 3;
                final double x0 = Unsafe.getDouble(samplesAddr + o), y0 = Unsafe.getDouble(weightsAddr + o);
                final double x1 = Unsafe.getDouble(samplesAddr + o + 8), y1 = Unsafe.getDouble(weightsAddr + o + 8);
                final double x2 = Unsafe.getDouble(samplesAddr + o + 16), y2 = Unsafe.getDouble(weightsAddr + o + 16);
                final double x3 = Unsafe.getDouble(samplesAddr + o + 24), y3 = Unsafe.getDouble(weightsAddr + o + 24);
                if (Numbers.isFinite(x0) && Numbers.isFinite(y0) && y0 != 0.0) {
                    sum += x0 * y0;
                    weightSum += y0;
                }
                if (Numbers.isFinite(x1) && Numbers.isFinite(y1) && y1 != 0.0) {
                    s1 += x1 * y1;
                    w1 += y1;
                }
                if (Numbers.isFinite(x2) && Numbers.isFinite(y2) && y2 != 0.0) {
                    s2 += x2 * y2;
                    w2 += y2;
                }
                if (Numbers.isFinite(x3) && Numbers.isFinite(y3) && y3 != 0.0) {
                    s3 += x3 * y3;
                    w3 += y3;
                }
            }
            for (; i < rowCount; i++) {
                final double sample = Unsafe.getDouble(samplesAddr + ((long) i << 3));
                final double weight = Unsafe.getDouble(weightsAddr + ((long) i << 3));
                if (Numbers.isFinite(sample) && Numbers.isFinite(weight) && weight != 0.0) {
                    sum += sample * weight;
                    weightSum += weight;
                }
            }
            sum += (s1 + s2) + s3;
            weightSum += (w1 + w2) + w3;
        } else if (sampleType == ColumnType.FLOAT && weightType == ColumnType.INT) {
            // a NULL int weight is NaN, and so not finite; four independent sums, see above
            double s1 = 0, s2 = 0, s3 = 0, w1 = 0, w2 = 0, w3 = 0;
            int i = 0;
            for (final int n4 = rowCount & ~3; i < n4; i += 4) {
                final long o = (long) i << 2;
                final double x0 = Unsafe.getFloat(samplesAddr + o), x1 = Unsafe.getFloat(samplesAddr + o + 4);
                final double x2 = Unsafe.getFloat(samplesAddr + o + 8), x3 = Unsafe.getFloat(samplesAddr + o + 12);
                final int y0 = Unsafe.getInt(weightsAddr + o), y1 = Unsafe.getInt(weightsAddr + o + 4);
                final int y2 = Unsafe.getInt(weightsAddr + o + 8), y3 = Unsafe.getInt(weightsAddr + o + 12);
                if (y0 != Numbers.INT_NULL && y0 != 0 && Numbers.isFinite(x0)) {
                    sum += x0 * y0;
                    weightSum += y0;
                }
                if (y1 != Numbers.INT_NULL && y1 != 0 && Numbers.isFinite(x1)) {
                    s1 += x1 * y1;
                    w1 += y1;
                }
                if (y2 != Numbers.INT_NULL && y2 != 0 && Numbers.isFinite(x2)) {
                    s2 += x2 * y2;
                    w2 += y2;
                }
                if (y3 != Numbers.INT_NULL && y3 != 0 && Numbers.isFinite(x3)) {
                    s3 += x3 * y3;
                    w3 += y3;
                }
            }
            for (; i < rowCount; i++) {
                final double sample = Unsafe.getFloat(samplesAddr + ((long) i << 2));
                final int w = Unsafe.getInt(weightsAddr + ((long) i << 2));
                if (w != Numbers.INT_NULL && w != 0 && Numbers.isFinite(sample)) {
                    final double weight = w;
                    sum += sample * weight;
                    weightSum += weight;
                }
            }
            sum += (s1 + s2) + s3;
            weightSum += (w1 + w2) + w3;
        } else {
            for (int i = 0; i < rowCount; i++) {
                final double sample = GroupByBatchKernels.rawDouble(samplesAddr, sampleType, i);
                final double weight = GroupByBatchKernels.rawDouble(weightsAddr, weightType, i);
                if (Numbers.isFinite(sample) && Numbers.isFinite(weight) && weight != 0.0) {
                    sum += sample * weight;
                    weightSum += weight;
                }
            }
        }
        mapValue.putDouble(valueIndex, sum);
        mapValue.putDouble(valueIndex + 1, weightSum);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final double sample = sampleArg.getDouble(record);
        final double weight = weightArg.getDouble(record);
        if (Numbers.isFinite(sample) && Numbers.isFinite(weight) && weight != 0.0) {
            mapValue.putDouble(valueIndex, sample * weight);
            mapValue.putDouble(valueIndex + 1, weight);
        } else {
            mapValue.putDouble(valueIndex, 0.0);
            mapValue.putDouble(valueIndex + 1, 0.0);
        }
    }

    @Override
    public void computeKeyedBatchKernel(
            FlyweightPackedMapValue mapValue,
            long baseValueAddr,
            long batchAddr,
            int rowCount,
            GroupByBatchKernels.Args args
    ) {
        final long samplesAddr = args.address(0);
        final long weightsAddr = args.address(1);
        final long sumOffset = mapValue.getOffset(valueIndex);
        final long weightOffset = mapValue.getOffset(valueIndex + 1);
        for (int i = 0; i < rowCount; i++) {
            final long encoded = Unsafe.getLong(batchAddr + ((long) i << 3));
            final long valueAddr = baseValueAddr + Map.decodeBatchOffset(encoded);
            final double sample = Unsafe.getDouble(samplesAddr + ((long) i << 3));
            final double weight = Unsafe.getDouble(weightsAddr + ((long) i << 3));
            final boolean valid = Numbers.isFinite(sample) && Numbers.isFinite(weight) && weight != 0.0;
            if (Map.isNewBatchEntry(encoded)) {
                Unsafe.putDouble(valueAddr + sumOffset, valid ? sample * weight : 0.0);
                Unsafe.putDouble(valueAddr + weightOffset, valid ? weight : 0.0);
            } else if (valid) {
                Unsafe.putDouble(valueAddr + sumOffset, Unsafe.getDouble(valueAddr + sumOffset) + sample * weight);
                Unsafe.putDouble(valueAddr + weightOffset, Unsafe.getDouble(valueAddr + weightOffset) + weight);
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double sample = sampleArg.getDouble(record);
        final double weight = weightArg.getDouble(record);
        if (Numbers.isFinite(sample) && Numbers.isFinite(weight) && weight != 0.0) {
            mapValue.addDouble(valueIndex, sample * weight);
            mapValue.addDouble(valueIndex + 1, weight);
        }
    }

    @Override
    public Function getBatchKernelArg(int index) {
        return index == 0 ? sampleArg : weightArg;
    }

    @Override
    public int getBatchKernelArgCount() {
        return 2;
    }

    @Override
    public int getBatchKernelArgType(int index) {
        return ColumnType.DOUBLE;
    }

    @Override
    public double getDouble(Record rec) {
        return rec.getDouble(valueIndex) / rec.getDouble(valueIndex + 1);
    }

    @Override
    public Function getLeft() {
        return sampleArg;
    }

    @Override
    public String getName() {
        return "weighted_avg";
    }

    @Override
    public Function getRight() {
        return weightArg;
    }

    @Override
    public boolean supportsRawKernelArgs() {
        return true;
    }

    @Override
    public int getSampleByFlags() {
        return GroupByFunction.SAMPLE_BY_FILL_ALL;
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.DOUBLE);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isOrderSensitive() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return BinaryFunction.super.isThreadSafe();
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        final double destSum = destValue.getDouble(valueIndex);
        final double destWeight = destValue.getDouble(valueIndex + 1);
        final double srcSum = srcValue.getDouble(valueIndex);
        final double srcWeight = srcValue.getDouble(valueIndex + 1);
        destValue.putDouble(valueIndex, destSum + srcSum);
        destValue.putDouble(valueIndex + 1, destWeight + srcWeight);
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        mapValue.putDouble(valueIndex, value);
        mapValue.putDouble(valueIndex + 1, 1.0);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
        mapValue.putDouble(valueIndex + 1, 0.0);
    }

    @Override
    public boolean supportsParallelism() {
        return BinaryFunction.super.supportsParallelism();
    }
}
