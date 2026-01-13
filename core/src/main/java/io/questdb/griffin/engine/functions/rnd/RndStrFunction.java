/*******************************************************************************
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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.Rnd;

class RndStrFunction extends StrFunction implements Function {
    private final int lo;
    private final int nullRate;
    private final int range;
    private Rnd rnd;

    public RndStrFunction(int lo, int hi, int nullRate) {
        this.lo = lo;
        this.range = hi - lo + 1;
        this.nullRate = nullRate;
    }

    @Override
    public CharSequence getStrA(Record rec) {
        if ((rnd.nextInt() % nullRate) == 1) {
            return null;
        }
        return rnd.nextChars(lo + rnd.nextPositiveInt() % range);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return getStrA(rec);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        this.rnd = executionContext.getRandom();
    }

    @Override
    public boolean isNonDeterministic() {
        return true;
    }

    @Override
    public boolean isRandom() {
        return true;
    }

    @Override
    public boolean shouldMemoize() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("rnd_str(").val(lo).val(',').val(range + lo - 1).val(',').val(nullRate - 1).val(')');
    }
}
