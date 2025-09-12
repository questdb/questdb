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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;

public abstract class ArithmeticDecimalFunction extends DecimalFunction implements BinaryFunction {
    protected final Function left;
    protected final Decimal256 leftDecimal = new Decimal256();
    protected final int precision;
    protected final Function right;
    protected final Decimal256 rightDecimal = new Decimal256();
    protected final int scale;
    private boolean isNull;

    public ArithmeticDecimalFunction(Function left, Function right, int targetType) {
        super(targetType);
        this.left = left;
        this.right = right;
        this.precision = ColumnType.getDecimalPrecision(targetType);
        this.scale = ColumnType.getDecimalScale(targetType);
    }

    @Override
    public long getDecimal128Hi(Record rec) {
        if (!calc(rec)) {
            isNull = true;
            return Decimals.DECIMAL128_HI_NULL;
        }
        isNull = false;
        return leftDecimal.getLh();
    }

    @Override
    public long getDecimal128Lo(Record rec) {
        if (isNull) {
            return Decimals.DECIMAL128_LO_NULL;
        }
        return leftDecimal.getLl();
    }

    @Override
    public short getDecimal16(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL16_NULL;
        }
        return (short) leftDecimal.getLl();
    }

    @Override
    public long getDecimal256HH(Record rec) {
        if (!calc(rec)) {
            isNull = true;
            return Decimals.DECIMAL256_HH_NULL;
        }
        isNull = false;
        return leftDecimal.getHh();
    }

    @Override
    public long getDecimal256HL(Record rec) {
        if (isNull) {
            return Decimals.DECIMAL256_HL_NULL;
        }
        return leftDecimal.getHl();
    }

    @Override
    public long getDecimal256LH(Record rec) {
        if (isNull) {
            return Decimals.DECIMAL256_LH_NULL;
        }
        return leftDecimal.getLh();
    }

    @Override
    public long getDecimal256LL(Record rec) {
        if (isNull) {
            return Decimals.DECIMAL256_LL_NULL;
        }
        return leftDecimal.getLl();
    }

    @Override
    public int getDecimal32(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL32_NULL;
        }
        return (int) leftDecimal.getLl();
    }

    @Override
    public long getDecimal64(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL64_NULL;
        }
        return leftDecimal.getLl();
    }

    @Override
    public byte getDecimal8(Record rec) {
        if (!calc(rec)) {
            return Decimals.DECIMAL8_NULL;
        }
        return (byte) leftDecimal.getLl();
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

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(left).val(getName()).val(right);
    }

    /**
     * The implementation must fill the leftDecimal with the arithmetic operation value following
     * the target scale and precision. If the value to cast is null, it must return false
     * without doing additional work.
     *
     * @return whether the result is not null.
     */
    protected abstract boolean calc(Record rec);
}
