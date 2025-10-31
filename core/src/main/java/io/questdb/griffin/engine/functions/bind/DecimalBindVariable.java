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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Mutable;

class DecimalBindVariable extends DecimalFunction implements Mutable {
    final Decimal256 value = new Decimal256();

    public DecimalBindVariable() {
        super(ColumnType.getDecimalType(76, 38));
    }

    @Override
    public void clear() {
        this.value.ofNull();
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        if (value.isNull()) {
            sink.ofRawNull();
        } else {
            sink.ofRaw(value.getLh(), value.getLl());
        }
    }

    @Override
    public short getDecimal16(Record rec) {
        return value.isNull() ? Decimals.DECIMAL16_NULL : (short) value.getLl();
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        sink.copyRaw(value);
    }

    @Override
    public int getDecimal32(Record rec) {
        return value.isNull() ? Decimals.DECIMAL32_NULL : (int) value.getLl();
    }

    @Override
    public long getDecimal64(Record rec) {
        return value.isNull() ? Decimals.DECIMAL64_NULL : value.getLl();
    }

    @Override
    public byte getDecimal8(Record rec) {
        return value.isNull() ? Decimals.DECIMAL8_NULL : (byte) value.getLl();
    }

    @Override
    public boolean isNonDeterministic() {
        return true;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("?::decimal");
    }

    void setType(int type) {
        this.type = type;
    }
}
