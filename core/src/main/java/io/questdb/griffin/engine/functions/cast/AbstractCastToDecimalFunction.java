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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.AbstractDecimalFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;

public abstract class AbstractCastToDecimalFunction extends AbstractDecimalFunction implements UnaryFunction {
    protected final Function arg;
    protected final Decimal256 decimal;
    protected final int position;
    protected final int precision;
    protected final int scale;
    private boolean is_null;

    public AbstractCastToDecimalFunction(Function arg, int targetType, int position) {
        super(targetType);
        this.decimal = new Decimal256();
        this.is_null = false;
        this.arg = arg;
        this.position = position;
        this.precision = ColumnType.getDecimalPrecision(targetType);
        this.scale = ColumnType.getDecimalScale(targetType);
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public long getDecimal128Hi(Record rec) {
        if (!cast(rec)) {
            is_null = true;
            return Decimals.DECIMAL128_HI_NULL;
        }
        is_null = false;
        return decimal.getLh();
    }

    @Override
    public long getDecimal128Lo(Record rec) {
        if (is_null) {
            return Decimals.DECIMAL128_LO_NULL;
        }
        return decimal.getLl();
    }

    @Override
    public short getDecimal16(Record rec) {
        if (!cast(rec)) {
            return Decimals.DECIMAL16_NULL;
        }
        return (short) decimal.getLl();
    }

    @Override
    public long getDecimal256HH(Record rec) {
        if (!cast(rec)) {
            is_null = true;
            return Decimals.DECIMAL256_HH_NULL;
        }
        is_null = false;
        return decimal.getHh();
    }

    @Override
    public long getDecimal256HL(Record rec) {
        if (is_null) {
            return Decimals.DECIMAL256_HL_NULL;
        }
        return decimal.getHl();
    }

    @Override
    public long getDecimal256LH(Record rec) {
        if (is_null) {
            return Decimals.DECIMAL256_LH_NULL;
        }
        return decimal.getLh();
    }

    @Override
    public long getDecimal256LL(Record rec) {
        if (is_null) {
            return Decimals.DECIMAL256_LL_NULL;
        }
        return decimal.getLl();
    }

    @Override
    public int getDecimal32(Record rec) {
        if (!cast(rec)) {
            return Decimals.DECIMAL32_NULL;
        }
        return (int) decimal.getLl();
    }

    @Override
    public long getDecimal64(Record rec) {
        if (!cast(rec)) {
            return Decimals.DECIMAL64_NULL;
        }
        return decimal.getLl();
    }

    @Override
    public byte getDecimal8(Record rec) {
        if (!cast(rec)) {
            return Decimals.DECIMAL8_NULL;
        }
        return (byte) decimal.getLl();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(arg).val("::").val(ColumnType.nameOf(type));
    }

    /**
     * The implementation must fill the decimal with the cast value following the target scale and precision.
     * If the value to cast is null, it must return false without doing additional work.
     *
     * @return whether the result is not null.
     */
    protected abstract boolean cast(Record rec);
}
