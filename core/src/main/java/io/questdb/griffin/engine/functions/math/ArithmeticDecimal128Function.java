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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.NumericException;

abstract class ArithmeticDecimal128Function extends DecimalFunction implements BinaryFunction {
    protected final Decimal128 decimal = new Decimal128();
    protected final Function left;
    protected final int precision;
    protected final Function right;
    protected final int scale;
    private final int position;
    private final int rightScale;
    private final int rightTag;
    private boolean isNull;

    public ArithmeticDecimal128Function(Function left, Function right, int targetType, int position) {
        super(targetType);
        this.left = left;
        this.right = right;
        this.position = position;
        this.precision = ColumnType.getDecimalPrecision(targetType);
        this.scale = ColumnType.getDecimalScale(targetType);
        this.rightScale = ColumnType.getDecimalScale(right.getType());
        this.rightTag = ColumnType.tagOf(right.getType());
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        if (!calc(rec)) {
            sink.ofRaw(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
            return;
        }
        sink.copyRaw(decimal);
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getDecimal16(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL16_NULL;
        }
        return (short) decimal.getLow();
    }

    @Override
    public int getDecimal32(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL32_NULL;
        }
        return (int) decimal.getLow();
    }

    @Override
    public long getDecimal64(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL64_NULL;
        }
        return decimal.getLow();
    }

    @Override
    public byte getDecimal8(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL8_NULL;
        }
        return (byte) decimal.getLow();
    }

    @Override
    public Function getLeft() {
        return left;
    }

    @Override
    public Function getRight() {
        return right;
    }

    @Override
    public boolean isOperator() {
        return true;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    /**
     * @return whether the result is not null.
     */
    private boolean calc(Record rec) {
        DecimalUtil.load(decimal, left, rec);
        if (decimal.isNull()) {
            return false;
        }

        final long rightHigh, rightLow;
        switch (rightTag) {
            case ColumnType.DECIMAL8:
                rightLow = right.getDecimal8(rec);
                rightHigh = rightLow < 0 ? -1L : 0L;
                if (rightLow == Decimals.DECIMAL8_NULL) {
                    return false;
                }
                break;
            case ColumnType.DECIMAL16:
                rightLow = right.getDecimal16(rec);
                rightHigh = rightLow < 0 ? -1L : 0L;
                if (rightLow == Decimals.DECIMAL16_NULL) {
                    return false;
                }
                break;
            case ColumnType.DECIMAL32:
                rightLow = right.getDecimal32(rec);
                rightHigh = rightLow < 0 ? -1L : 0L;
                if (rightLow == Decimals.DECIMAL32_NULL) {
                    return false;
                }
                break;
            case ColumnType.DECIMAL64:
                rightLow = right.getDecimal64(rec);
                rightHigh = rightLow < 0 ? -1L : 0L;
                if (Decimal64.isNull(rightLow)) {
                    return false;
                }
                break;
            default: {
                Decimal128 rightDec = new Decimal128();
                right.getDecimal128(rec, rightDec);
                if (rightDec.isNull()) {
                    return false;
                }
                rightHigh = rightDec.getHigh();
                rightLow = rightDec.getLow();
                break;
            }
        }

        try {
            exec(rightHigh, rightLow, rightScale);
        } catch (NumericException e) {
            throw CairoException.nonCritical().position(position)
                    .put('\'').put(getName()).put("' operation failed: ").put(e.getFlyweightMessage());
        }
        return true;
    }

    /**
     * The implementation must execute the operation against the decimal here.
     * The right-hand values are given as inputs to this method.
     */
    protected abstract void exec(long rightHigh, long rightLow, int rightScale);
}
