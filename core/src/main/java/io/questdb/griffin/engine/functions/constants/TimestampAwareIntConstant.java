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

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.std.Numbers;

public class TimestampAwareIntConstant extends IntFunction implements ConstantFunction {
    public static final IntConstant NULL = new IntConstant(Numbers.INT_NULL);

    private final int intValue;
    private final long tsValue;

    public TimestampAwareIntConstant(int intValue, long tsValue) {
        this.intValue = intValue;
        this.tsValue = tsValue;
    }

    @Override
    public int getInt(Record rec) {
        return intValue;
    }

    @Override
    public long getTimestamp(Record rec) {
        return tsValue;
    }

    @Override
    public boolean isNullConstant() {
        return intValue == Numbers.INT_NULL && tsValue == Numbers.LONG_NULL;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(intValue).val(tsValue);
    }
}
