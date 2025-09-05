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

public class Decimal256Constant extends AbstractDecimalFunction implements ConstantFunction {

    public static final Decimal256Constant NULL = new Decimal256Constant(
            Decimals.DECIMAL256_HH_NULL,
            Decimals.DECIMAL256_HL_NULL,
            Decimals.DECIMAL256_LH_NULL,
            Decimals.DECIMAL256_LL_NULL,
            ColumnType.DECIMAL256
    );

    private final long hh;
    private final long hl;
    private final long lh;
    private final long ll;

    public Decimal256Constant(long hh, long hl, long lh, long ll, int typep) {
        super(typep);
        this.hh = hh;
        this.hl = hl;
        this.lh = lh;
        this.ll = ll;
    }

    @Override
    public long getDecimal256HH(Record rec) {
        return hh;
    }

    @Override
    public long getDecimal256HL(Record rec) {
        return hl;
    }

    @Override
    public long getDecimal256LH(Record rec) {
        return lh;
    }

    @Override
    public long getDecimal256LL(Record rec) {
        return ll;
    }

    @Override
    public boolean isNullConstant() {
        return hh == Decimals.DECIMAL256_HH_NULL && hl == Decimals.DECIMAL256_HL_NULL &&
                lh == Decimals.DECIMAL256_LH_NULL && ll  == Decimals.DECIMAL256_LL_NULL;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.valDecimal(hh, hl, lh, ll, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type));
    }
}
