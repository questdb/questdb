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

package io.questdb.griffin.engine.functions.decimal;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;

public abstract class ToDecimalFunction extends DecimalFunction {
    protected final Decimal256 decimal = new Decimal256();

    public ToDecimalFunction(int targetType) {
        super(targetType);
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        if (!store(rec)) {
            sink.ofRawNull();
        } else {
            sink.ofRaw(decimal.getLh(), decimal.getLl());
        }
    }

    @Override
    public short getDecimal16(Record rec) {
        if (!store(rec)) {
            return Decimals.DECIMAL16_NULL;
        }
        return (short) decimal.getLl();
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        if (!store(rec)) {
            sink.ofRawNull();
        } else {
            sink.copyRaw(decimal);
        }
    }

    @Override
    public int getDecimal32(Record rec) {
        if (!store(rec)) {
            return Decimals.DECIMAL32_NULL;
        }
        return (int) decimal.getLl();
    }

    @Override
    public long getDecimal64(Record rec) {
        if (!store(rec)) {
            return Decimals.DECIMAL64_NULL;
        }
        return decimal.getLl();
    }

    @Override
    public byte getDecimal8(Record rec) {
        if (!store(rec)) {
            return Decimals.DECIMAL8_NULL;
        }
        return (byte) decimal.getLl();
    }

    /**
     * The implementation must fill the decimal with the store value following the target scale and precision.
     * If the value to store is null, it must return false without doing additional work.
     *
     * @return whether the result is not null.
     */
    protected abstract boolean store(Record rec);
}
