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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.str.CharSink;

class Long256BindVariable extends Long256Function implements Mutable {
    final Long256Impl value = new Long256Impl();

    @Override
    public void clear() {
        value.copyFrom(Long256Impl.NULL_LONG256);
    }

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        Numbers.appendLong256(value, sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        return value;
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return value;
    }

    @Override
    public boolean isNonDeterministic() {
        return true;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    public void setValue(long l0, long l1, long l2, long l3) {
        this.value.setAll(l0, l1, l2, l3);
    }

    public void setValue(Long256 value) {
        this.value.copyFrom(value);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("?::long256");
    }
}
