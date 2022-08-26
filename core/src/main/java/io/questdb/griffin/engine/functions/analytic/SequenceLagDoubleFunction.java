/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.analytic;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.ScalarFunction;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.IntList;

import java.util.ArrayList;
import java.util.LinkedList;

class SequenceLagDoubleFunction extends DoubleFunction implements ScalarFunction {
    private final DoubleFunction base;
    private final int lag;
    private final LinkedList<Double> queue;

    public SequenceLagDoubleFunction(DoubleFunction base, int lag) {
        this.base = base;
        this.queue = new LinkedList<>();
        this.lag = lag;
    }
    @Override
    public double getDouble(Record rec) {
        double cVal = base.getDouble(rec);
        if(this.queue.size() < this.lag) {
            this.queue.add(cVal);
            return Double.NaN;
        }
        double oVal = this.queue.pop();
        this.queue.add(cVal);
        return oVal;
    }

    @Override
    public boolean supportsRandomAccess() {
        return false;
    }

    @Override
    public void toTop() {
        this.queue.clear();
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        toTop();
    }
}
