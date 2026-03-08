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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.UuidFunction;
import io.questdb.std.Numbers;
import io.questdb.std.Uuid;

public class UuidConstant extends UuidFunction implements ConstantFunction {
    public final static UuidConstant NULL = new UuidConstant(Numbers.LONG_NULL, Numbers.LONG_NULL);
    private final long hi;
    private final long lo;

    public UuidConstant(Uuid that) {
        this(that.getLo(), that.getHi());
    }

    public UuidConstant(long lo, long hi) {
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
    public boolean isEquivalentTo(Function obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof UuidConstant that) {
            return this.lo == that.lo && this.hi == that.hi;
        }
        return false;
    }

    @Override
    public boolean isNullConstant() {
        return hi == Numbers.LONG_NULL && lo == Numbers.LONG_NULL;
    }

    public void toPlan(PlanSink sink) {
        sink.valUuid(hi, lo);
    }
}
