/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.Long128Function;
import io.questdb.std.Numbers;

public class Long128Constant extends Long128Function implements ConstantFunction {

    public static final Long128Constant NULL = new Long128Constant(Numbers.LONG_NaN, Numbers.LONG_NaN);

    private final long hi;
    private final long lo;

    public Long128Constant(long lo, long hi) {
        this.lo = lo;
        this.hi = hi;
    }

    @Override
    public long getLong128Hi(Record rec) {
        return hi;
    }

    @Override
    public long getLong128Lo(Record rec) {
        return lo;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(hi).val(lo);
    }
}
