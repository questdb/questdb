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
import io.questdb.griffin.engine.functions.decimal.Decimal128Function;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;

public class Decimal128Constant extends Decimal128Function implements ConstantFunction {
    private final long hi;
    private final long lo;

    public Decimal128Constant(long hi, long lo, int type) {
        super(type);
        this.hi = hi;
        this.lo = lo;
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        sink.ofRaw(hi, lo);
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNullConstant() {
        return Decimal128.isNull(hi, lo);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.valDecimal(hi, lo, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type));
    }
}
