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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.TypeConstant;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;

public class DecimalTypeConstant extends DecimalFunction implements TypeConstant {
    public DecimalTypeConstant(int precision, int scale) {
        super(ColumnType.getDecimalType(precision, scale));
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        sink.ofRawNull();
    }

    @Override
    public short getDecimal16(Record rec) {
        return Decimals.DECIMAL16_NULL;
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        sink.ofRawNull();
    }

    @Override
    public int getDecimal32(Record rec) {
        return Decimals.DECIMAL32_NULL;
    }

    @Override
    public long getDecimal64(Record rec) {
        return Decimals.DECIMAL64_NULL;
    }

    @Override
    public byte getDecimal8(Record rec) {
        return Decimals.DECIMAL8_NULL;
    }
}
