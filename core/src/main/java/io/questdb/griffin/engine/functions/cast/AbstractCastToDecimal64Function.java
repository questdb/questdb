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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.decimal.Decimal64Function;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;

/**
 * Abstract base class for functions that cast values to decimal64.
 */
public abstract class AbstractCastToDecimal64Function extends Decimal64Function implements CastFunction {
    protected final Function arg;
    /**
     * Reused buffer that {@link #cast(Record)} fills and the {@code getDecimalNN}
     * accessors read back from. Sharing it across worker threads would race, so
     * {@link #isThreadSafe()} returns false.
     */
    protected final Decimal64 decimal = new Decimal64();
    protected final int position;
    protected final int precision;
    protected final int scale;

    /**
     * Constructs a new cast to decimal64 function.
     *
     * @param arg        the function argument to cast
     * @param targetType the target decimal type
     * @param position   the position in the SQL statement
     */
    public AbstractCastToDecimal64Function(Function arg, int targetType, int position) {
        super(targetType);
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
    public short getDecimal16(Record rec) {
        if (!cast(rec)) {
            return Decimals.DECIMAL16_NULL;
        }
        return (short) decimal.getValue();
    }

    @Override
    public int getDecimal32(Record rec) {
        if (!cast(rec)) {
            return Decimals.DECIMAL32_NULL;
        }
        return (int) decimal.getValue();
    }

    @Override
    public long getDecimal64(Record rec) {
        if (!cast(rec)) {
            return Decimals.DECIMAL64_NULL;
        }
        return decimal.getValue();
    }

    @Override
    public byte getDecimal8(Record rec) {
        if (!cast(rec)) {
            return Decimals.DECIMAL8_NULL;
        }
        return (byte) decimal.getValue();
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(arg).val("::").val(ColumnType.nameOf(type));
    }

    /**
     * The implementation must fill the decimal with the cast value following the target scale and precision.
     * If the value to cast is null, it must return false without doing additional work.
     *
     * @param rec the record to read from
     * @return whether the result is not null
     */
    protected abstract boolean cast(Record rec);
}
