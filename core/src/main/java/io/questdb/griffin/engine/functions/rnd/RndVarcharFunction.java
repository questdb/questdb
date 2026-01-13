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
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8StringSink;

class RndVarcharFunction extends VarcharFunction implements Function {
    private final int lo;
    private final int nullRate;
    private final int range;
    private final Utf8StringSink utf8sinkA = new Utf8StringSink();
    private final Utf8StringSink utf8sinkB = new Utf8StringSink();
    private Rnd rnd;

    public RndVarcharFunction(int lo, int hi, int nullRate) {
        this.lo = lo;
        this.range = hi - lo + 1;
        this.nullRate = nullRate;
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        if ((rnd.nextInt() % nullRate) == 1) {
            return null;
        }
        utf8sinkA.clear();
        sinkRnd(utf8sinkA);
        return utf8sinkA;
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        if ((rnd.nextInt() % nullRate) == 1) {
            return null;
        }
        utf8sinkB.clear();
        sinkRnd(utf8sinkB);
        return utf8sinkB;
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
        sink.val("rnd_varchar(").val(lo).val(',').val(range + lo - 1).val(',').val(nullRate - 1).val(')');
    }

    private void sinkRnd(Utf8Sink utf8Sink) {
        int len = lo + rnd.nextPositiveInt() % range;
        if (rnd.nextBoolean()) {
            rnd.nextUtf8AsciiStr(len, utf8Sink);
        } else {
            rnd.nextUtf8Str(len, utf8Sink);
        }
    }
}
