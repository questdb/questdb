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
        final double[] samples = args.doubles(0);
        final double[] weights = args.doubles(1);
        double sum = mapValue.getDouble(valueIndex);
        double weightSum = mapValue.getDouble(valueIndex + 1);
        for (int i = 0; i < rowCount; i++) {
            final double sample = samples[i];
            final double weight = weights[i];
            if (Numbers.isFinite(sample) && Numbers.isFinite(weight) && weight != 0.0) {
                sum += sample * weight;
                weightSum += weight;
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
        final double[] samples = args.doubles(0);
        final double[] weights = args.doubles(1);
        final long sumOffset = mapValue.getOffset(valueIndex);
        final long weightOffset = mapValue.getOffset(valueIndex + 1);
        for (int i = 0; i < rowCount; i++) {
            final long encoded = Unsafe.getLong(batchAddr + ((long) i << 3));
            final long valueAddr = baseValueAddr + Map.decodeBatchOffset(encoded);
            final double sample = samples[i];
            final double weight = weights[i];
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
