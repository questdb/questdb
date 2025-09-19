/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import org.jetbrains.annotations.NotNull;

abstract class AbstractMinMaxDecimalFunction extends DecimalFunction implements GroupByFunction, UnaryFunction {
    protected final Function arg;
    protected final int storageSizePow2;
    protected int valueIndex;

    public AbstractMinMaxDecimalFunction(@NotNull Function arg) {
        super(arg.getType());
        this.arg = arg;
        this.storageSizePow2 = Decimals.getStorageSizePow2(ColumnType.getDecimalPrecision(type));
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final long hh, hl, lh, ll;
        final boolean isNull;
        switch (storageSizePow2) {
            case 0:
                ll = arg.getDecimal8(record);
                lh = hl = hh = ll < 0 ? -1 : 0;
                isNull = (ll == Decimals.DECIMAL8_NULL);
                break;
            case 1:
                ll = arg.getDecimal16(record);
                lh = hl = hh = ll < 0 ? -1 : 0;
                isNull = (ll == Decimals.DECIMAL16_NULL);
                break;
            case 2:
                ll = arg.getDecimal32(record);
                lh = hl = hh = ll < 0 ? -1 : 0;
                isNull = (ll == Decimals.DECIMAL32_NULL);
                break;
            case 3:
                ll = arg.getDecimal64(record);
                lh = hl = hh = ll < 0 ? -1 : 0;
                isNull = Decimal64.isNull(ll);
                break;
            case 4:
                lh = arg.getDecimal128Hi(record);
                ll = arg.getDecimal128Lo(record);
                hl = hh = lh < 0 ? -1 : 0;
                isNull = Decimal128.isNull(lh, ll);
                break;
            default:
                hh = arg.getDecimal256HH(record);
                hl = arg.getDecimal256HL(record);
                lh = arg.getDecimal256LH(record);
                ll = arg.getDecimal256LL(record);
                isNull = Decimal256.isNull(hh, hl, lh, ll);
                break;
        }

        if (!isNull) {
            mapValue.putDecimal256(valueIndex, hh, hl, lh, ll);
        } else {
            mapValue.putDecimal256Null(valueIndex);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final long aHH, aHL, aLH, aLL;
        final boolean isNull;
        switch (storageSizePow2) {
            case 0:
                aLL = arg.getDecimal8(record);
                aLH = aHL = aHH = aLL < 0 ? -1 : 0;
                isNull = (aLL == Decimals.DECIMAL8_NULL);
                break;
            case 1:
                aLL = arg.getDecimal16(record);
                aLH = aHL = aHH = aLL < 0 ? -1 : 0;
                isNull = (aLL == Decimals.DECIMAL16_NULL);
                break;
            case 2:
                aLL = arg.getDecimal32(record);
                aLH = aHL = aHH = aLL < 0 ? -1 : 0;
                isNull = (aLL == Decimals.DECIMAL32_NULL);
                break;
            case 3:
                aLL = arg.getDecimal64(record);
                aLH = aHL = aHH = aLL < 0 ? -1 : 0;
                isNull = Decimal64.isNull(aLL);
                break;
            case 4:
                aLH = arg.getDecimal128Hi(record);
                aLL = arg.getDecimal128Lo(record);
                aHL = aHH = aLH < 0 ? -1 : 0;
                isNull = Decimal128.isNull(aLH, aLL);
                break;
            default:
                aHH = arg.getDecimal256HH(record);
                aHL = arg.getDecimal256HL(record);
                aLH = arg.getDecimal256LH(record);
                aLL = arg.getDecimal256LL(record);
                isNull = Decimal256.isNull(aHH, aHL, aLH, aLL);
                break;
        }

        if (!isNull) {
            final long bHH = mapValue.getDecimal256HH(valueIndex);
            final long bHL = mapValue.getDecimal256HL(valueIndex);
            final long bLH = mapValue.getDecimal256LH(valueIndex);
            final long bLL = mapValue.getDecimal256LL(valueIndex);
            if (shouldStoreA(aHH, aHL, aLH, aLL, bHH, bHL, bLH, bLL) || Decimal256.isNull(bHH, bHL, bLH, bLL)) {
                mapValue.putDecimal256(valueIndex, aHH, aHL, aLH, aLL);
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public long getDecimal128Hi(Record rec) {
        final long hh = rec.getDecimal256HH(valueIndex);
        final long hl = rec.getDecimal256HL(valueIndex);
        final long lh = rec.getDecimal256LH(valueIndex);
        final long ll = rec.getDecimal256LL(valueIndex);
        if (Decimal256.isNull(hh, hl, lh, ll)) {
            return Decimals.DECIMAL128_HI_NULL;
        }
        return lh;
    }

    @Override
    public long getDecimal128Lo(Record rec) {
        final long hh = rec.getDecimal256HH(valueIndex);
        final long hl = rec.getDecimal256HL(valueIndex);
        final long lh = rec.getDecimal256LH(valueIndex);
        final long ll = rec.getDecimal256LL(valueIndex);
        if (Decimal256.isNull(hh, hl, lh, ll)) {
            return Decimals.DECIMAL128_LO_NULL;
        }
        return ll;
    }

    @Override
    public short getDecimal16(Record rec) {
        final long hh = rec.getDecimal256HH(valueIndex);
        final long hl = rec.getDecimal256HL(valueIndex);
        final long lh = rec.getDecimal256LH(valueIndex);
        final long ll = rec.getDecimal256LL(valueIndex);
        if (Decimal256.isNull(hh, hl, lh, ll)) {
            return Decimals.DECIMAL16_NULL;
        }
        return (short) ll;
    }

    @Override
    public long getDecimal256HH(Record rec) {
        return rec.getDecimal256HH(valueIndex);
    }

    @Override
    public long getDecimal256HL(Record rec) {
        return rec.getDecimal256HL(valueIndex);
    }

    @Override
    public long getDecimal256LH(Record rec) {
        return rec.getDecimal256LH(valueIndex);
    }

    @Override
    public long getDecimal256LL(Record rec) {
        return rec.getDecimal256LL(valueIndex);
    }

    @Override
    public int getDecimal32(Record rec) {
        final long hh = rec.getDecimal256HH(valueIndex);
        final long hl = rec.getDecimal256HL(valueIndex);
        final long lh = rec.getDecimal256LH(valueIndex);
        final long ll = rec.getDecimal256LL(valueIndex);
        if (Decimal256.isNull(hh, hl, lh, ll)) {
            return Decimals.DECIMAL32_NULL;
        }
        return (int) ll;
    }

    @Override
    public long getDecimal64(Record rec) {
        final long hh = rec.getDecimal256HH(valueIndex);
        final long hl = rec.getDecimal256HL(valueIndex);
        final long lh = rec.getDecimal256LH(valueIndex);
        final long ll = rec.getDecimal256LL(valueIndex);
        if (Decimal256.isNull(hh, hl, lh, ll)) {
            return Decimals.DECIMAL64_NULL;
        }
        return ll;
    }

    @Override
    public byte getDecimal8(Record rec) {
        final long hh = rec.getDecimal256HH(valueIndex);
        final long hl = rec.getDecimal256HL(valueIndex);
        final long lh = rec.getDecimal256LH(valueIndex);
        final long ll = rec.getDecimal256LL(valueIndex);
        if (Decimal256.isNull(hh, hl, lh, ll)) {
            return Decimals.DECIMAL8_NULL;
        }
        return (byte) ll;
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
        columnTypes.add(ColumnType.DECIMAL256);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return UnaryFunction.super.isThreadSafe();
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        final long srcHH = srcValue.getDecimal256HH(valueIndex);
        final long srcHL = srcValue.getDecimal256HL(valueIndex);
        final long srcLH = srcValue.getDecimal256LH(valueIndex);
        final long srcLL = srcValue.getDecimal256LL(valueIndex);

        final long destHH = destValue.getDecimal256HH(valueIndex);
        final long destHL = destValue.getDecimal256HL(valueIndex);
        final long destLH = destValue.getDecimal256LH(valueIndex);
        final long destLL = destValue.getDecimal256LL(valueIndex);

        if (!Decimal256.isNull(srcHH, srcHL, srcLH, srcLL)
                && (shouldStoreA(srcHH, srcHL, srcLH, srcLL, destHH, destHL, destLH, destLL) || Decimal256.isNull(destHH, destHL, destLH, destLL))) {
            destValue.putDecimal256(valueIndex, srcHH, srcHL, srcLH, srcLL);
        }
    }

    @Override
    public void setDecimal256(MapValue mapValue, long hh, long hl, long lh, long ll) {
        mapValue.putDecimal256(valueIndex, hh, hl, lh, ll);
        mapValue.putLong(valueIndex + 1, 1);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDecimal256Null(valueIndex);
        mapValue.putLong(valueIndex + 1, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    protected abstract boolean shouldStoreA(
            long aHH, long aHL, long aLH, long aLL,
            long bHH, long bHL, long bLH, long bLL
    );
}
