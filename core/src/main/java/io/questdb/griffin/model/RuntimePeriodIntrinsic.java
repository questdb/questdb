/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.model;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;

public class RuntimePeriodIntrinsic implements Mutable {
    public static final ObjectFactory<RuntimePeriodIntrinsic> FACTORY = RuntimePeriodIntrinsic::new;
    private int operation;
    private long staticLo;
    private long staticHi;

    private Function dynamicLo;
    private Function dynamicHi;

    private long dynamicIncrement;

    private int count;
    private char periodType;
    private int period;

    @Override
    public void clear() {
        operation = IntervalOperation.NONE;
    }

    public int getCount() {
        return count;
    }

    public long getHi(SqlExecutionContext sqlContext) {
        if (dynamicHi == null) return staticHi;

        dynamicHi.init(null, sqlContext);
        return dynamicHi.getTimestamp(null) + dynamicIncrement;
    }

    public long getLo(SqlExecutionContext sqlContext) {
        if (dynamicLo == null) return staticLo;

        dynamicLo.init(null, sqlContext);
        return dynamicLo.getTimestamp(null) + dynamicIncrement;
    }

    public int getOperation() {
        return operation;
    }

    public int getPeriod() {
        return period;
    }

    public char getPeriodType() {
        return periodType;
    }

    public RuntimePeriodIntrinsic setInterval(int operation, long lo, long hi) {
        this.operation = operation;
        staticLo = lo;
        staticHi = hi;
        return this;
    }

    public RuntimePeriodIntrinsic setInterval(int operation, Interval tempInterval) {
        this.operation = operation;
        staticLo = tempInterval.lo;
        staticHi = tempInterval.hi;
        this.count = tempInterval.count;
        this.periodType = tempInterval.periodType;
        this.period = tempInterval.period;

        return this;
    }

    public RuntimePeriodIntrinsic setLess(int operation, long lo, Function function, long dynamicIncrement) {
        this.operation = operation;
        staticLo = lo;
        dynamicHi = function;
        this.dynamicIncrement = dynamicIncrement;
        return this;
    }

    public RuntimePeriodIntrinsic setGreater(int operation, Function lo, long hi, long dynamicIncrement) {
        this.operation = operation;
        dynamicLo = lo;
        this.staticHi = hi;
        this.dynamicIncrement = dynamicIncrement;
        return this;
    }
}
