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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

/**
 * Streaming aggregation for the central moments needed to compute kurtosis.
 * <p>
 * The state holds five slots: mean, M2, M3, M4, and count, where
 * Mk = sum((x - mean)^k). The per-row update uses Pebay's online algorithm
 * (an extension of Welford's), and {@link #merge} uses the corresponding
 * pairwise combine. Concrete subclasses turn these moments into a kurtosis
 * value via {@code getDouble}.
 *
 * @see <a href="https://www.osti.gov/biblio/1028931">Pebay 2008, Formulas for Robust, One-Pass Parallel Computation of Covariances and Arbitrary-Order Statistical Moments</a>
 */
public abstract class AbstractKurtosisGroupByFunction extends DoubleFunction implements GroupByFunction, UnaryFunction {
    protected final Function arg;
    protected int valueIndex;

    protected AbstractKurtosisGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final double d = arg.getDouble(record);
        mapValue.putDouble(valueIndex, 0);
        mapValue.putDouble(valueIndex + 1, 0);
        mapValue.putDouble(valueIndex + 2, 0);
        mapValue.putDouble(valueIndex + 3, 0);
        mapValue.putLong(valueIndex + 4, 0);
        if (Numbers.isFinite(d)) {
            aggregate(mapValue, d);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double d = arg.getDouble(record);
        if (Numbers.isFinite(d)) {
            aggregate(mapValue, d);
        }
    }

    @Override
    public Function getArg() {
        return arg;
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
        columnTypes.add(ColumnType.DOUBLE); // mean
        columnTypes.add(ColumnType.DOUBLE); // M2 = sum((x - mean)^2)
        columnTypes.add(ColumnType.DOUBLE); // M3 = sum((x - mean)^3)
        columnTypes.add(ColumnType.DOUBLE); // M4 = sum((x - mean)^4)
        columnTypes.add(ColumnType.LONG);   // count
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long nB = srcValue.getLong(valueIndex + 4);
        if (nB == 0) {
            return;
        }
        long nA = destValue.getLong(valueIndex + 4);
        if (nA == 0) {
            destValue.putDouble(valueIndex, srcValue.getDouble(valueIndex));
            destValue.putDouble(valueIndex + 1, srcValue.getDouble(valueIndex + 1));
            destValue.putDouble(valueIndex + 2, srcValue.getDouble(valueIndex + 2));
            destValue.putDouble(valueIndex + 3, srcValue.getDouble(valueIndex + 3));
            destValue.putLong(valueIndex + 4, nB);
            return;
        }

        double meanA = destValue.getDouble(valueIndex);
        double m2A = destValue.getDouble(valueIndex + 1);
        double m3A = destValue.getDouble(valueIndex + 2);
        double m4A = destValue.getDouble(valueIndex + 3);

        double meanB = srcValue.getDouble(valueIndex);
        double m2B = srcValue.getDouble(valueIndex + 1);
        double m3B = srcValue.getDouble(valueIndex + 2);
        double m4B = srcValue.getDouble(valueIndex + 3);

        double nAd = nA;
        double nBd = nB;
        double nd = nAd + nBd;

        double delta = meanB - meanA;
        double delta2 = delta * delta;
        double delta3 = delta2 * delta;
        double delta4 = delta3 * delta;

        double mean = (nAd * meanA + nBd * meanB) / nd;
        double m2 = m2A + m2B + delta2 * nAd * nBd / nd;
        double m3 = m3A + m3B
                + delta3 * nAd * nBd * (nAd - nBd) / (nd * nd)
                + 3 * delta * (nAd * m2B - nBd * m2A) / nd;
        double m4 = m4A + m4B
                + delta4 * nAd * nBd * (nAd * nAd - nAd * nBd + nBd * nBd) / (nd * nd * nd)
                + 6 * delta2 * (nAd * nAd * m2B + nBd * nBd * m2A) / (nd * nd)
                + 4 * delta * (nAd * m3B - nBd * m3A) / nd;

        destValue.putDouble(valueIndex, mean);
        destValue.putDouble(valueIndex + 1, m2);
        destValue.putDouble(valueIndex + 2, m3);
        destValue.putDouble(valueIndex + 3, m4);
        destValue.putLong(valueIndex + 4, nA + nB);
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        mapValue.putDouble(valueIndex, value);
        mapValue.putDouble(valueIndex + 1, 0);
        mapValue.putDouble(valueIndex + 2, 0);
        mapValue.putDouble(valueIndex + 3, 0);
        mapValue.putLong(valueIndex + 4, 1L);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
        mapValue.putDouble(valueIndex + 1, Double.NaN);
        mapValue.putDouble(valueIndex + 2, Double.NaN);
        mapValue.putDouble(valueIndex + 3, Double.NaN);
        mapValue.putLong(valueIndex + 4, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    protected void aggregate(MapValue mapValue, double value) {
        double mean = mapValue.getDouble(valueIndex);
        double m2 = mapValue.getDouble(valueIndex + 1);
        double m3 = mapValue.getDouble(valueIndex + 2);
        double m4 = mapValue.getDouble(valueIndex + 3);
        long n = mapValue.getLong(valueIndex + 4) + 1;

        double nd = n;
        double delta = value - mean;
        double deltaN = delta / nd;
        double deltaN2 = deltaN * deltaN;
        double term1 = delta * deltaN * (nd - 1);

        // Update order matters: M4 reads the old M2 and M3, M3 reads the old M2.
        double newM4 = m4 + term1 * deltaN2 * (nd * nd - 3 * nd + 3) + 6 * deltaN2 * m2 - 4 * deltaN * m3;
        double newM3 = m3 + term1 * deltaN * (nd - 2) - 3 * deltaN * m2;
        double newM2 = m2 + term1;
        double newMean = mean + deltaN;

        mapValue.putDouble(valueIndex, newMean);
        mapValue.putDouble(valueIndex + 1, newM2);
        mapValue.putDouble(valueIndex + 2, newM3);
        mapValue.putDouble(valueIndex + 3, newM4);
        mapValue.putLong(valueIndex + 4, n);
    }
}
