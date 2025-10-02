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
import io.questdb.griffin.engine.functions.Decimal64Function;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.NumericException;

abstract class ArithmeticDecimal64Function extends Decimal64Function implements BinaryFunction {
    protected final Decimal64 decimal = new Decimal64();
    protected final Function left;
    protected final int precision;
    protected final Function right;
    protected final int scale;
    private final int position;
    private final int rightScale;
    private final int rightTag;

    public ArithmeticDecimal64Function(Function left, Function right, int targetType, int position) {
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
    public short getDecimal16(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL16_NULL;
        }
        return (short) decimal.getValue();
    }

    @Override
    public int getDecimal32(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL32_NULL;
        }
        return (int) decimal.getValue();
    }

    @Override
    public long getDecimal64(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL64_NULL;
        }
        return decimal.getValue();
    }

    @Override
    public byte getDecimal8(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL8_NULL;
        }
        return (byte) decimal.getValue();
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

        final long rightValue;
        switch (rightTag) {
            case ColumnType.DECIMAL8:
                rightValue = right.getDecimal8(rec);
                if (rightValue == Decimals.DECIMAL8_NULL) {
                    return false;
                }
                break;
            case ColumnType.DECIMAL16:
                rightValue = right.getDecimal16(rec);
                if (rightValue == Decimals.DECIMAL16_NULL) {
                    return false;
                }
                break;
            case ColumnType.DECIMAL32:
                rightValue = right.getDecimal32(rec);
                if (rightValue == Decimals.DECIMAL32_NULL) {
                    return false;
                }
                break;
            default:
                rightValue = right.getDecimal64(rec);
                if (Decimal64.isNull(rightValue)) {
                    return false;
                }
                break;
        }

        try {
            exec(rightValue, rightScale);
        } catch (NumericException e) {
            throw CairoException.nonCritical().position(position)
                    .put('\'').put(getName()).put("' operation failed: ").put(e.getFlyweightMessage());
        }
        return true;
    }

    /**
     * The implementation must execute the operation against the decimal here.
     * The right-hand value is given as inputs to this method.
     */
    protected abstract void exec(long rightValue, int rightScale);
}
