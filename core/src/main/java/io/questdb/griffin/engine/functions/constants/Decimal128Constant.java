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
import io.questdb.griffin.engine.functions.AbstractDecimalFunction;
import io.questdb.std.Decimals;

public class Decimal128Constant extends AbstractDecimalFunction implements ConstantFunction {

    public static final Decimal128Constant NULL = new Decimal128Constant(
            Decimals.DECIMAL128_HI_NULL,
            Decimals.DECIMAL128_LO_NULL,
            ColumnType.DECIMAL128
    );

    private final long hi;
    private final long lo;

    public Decimal128Constant(long hi, long lo, int typep) {
        super(typep);
        this.hi = hi;
        this.lo = lo;
    }

    @Override
    public long getDecimal128Hi(Record rec) {
        return hi;
    }

    @Override
    public long getDecimal128Lo(Record rec) {
        return lo;
    }

    @Override
    public boolean isNullConstant() {
        return hi == Decimals.DECIMAL128_HI_NULL && lo == Decimals.DECIMAL128_LO_NULL;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (isNullConstant()) {
            sink.valDecimal(
                    Decimals.DECIMAL256_HH_NULL,
                    Decimals.DECIMAL256_HL_NULL,
                    Decimals.DECIMAL256_LH_NULL,
                    Decimals.DECIMAL256_LL_NULL,
                    ColumnType.getDecimalPrecision(type),
                    ColumnType.getDecimalScale(type)
            );
        } else {
            long s = hi < 0 ? -1 : 0;
            sink.valDecimal(s, s, hi, lo, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type));
        }
    }
}
