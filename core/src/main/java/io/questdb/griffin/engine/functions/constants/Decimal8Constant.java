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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.decimal.Decimal8Function;
import io.questdb.std.Decimals;

public class Decimal8Constant extends Decimal8Function implements ConstantFunction {
    private final byte value;

    public Decimal8Constant(byte value, int type) {
        super(type);
        this.value = value;
    }

    @Override
    public byte getDecimal8(Record rec) {
        return value;
    }

    @Override
    public boolean isNullConstant() {
        return value == Decimals.DECIMAL8_NULL;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.valDecimal(
                value == Decimals.DECIMAL8_NULL ? Decimals.DECIMAL64_NULL : value,
                ColumnType.getDecimalPrecision(type),
                ColumnType.getDecimalScale(type)
        );
    }
}
