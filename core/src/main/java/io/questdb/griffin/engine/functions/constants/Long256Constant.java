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
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.str.CharSink;

public class Long256Constant extends Long256Function implements Long256, ConstantFunction {
    protected Long256Impl value;

    Long256Constant() {
        // used by Long256NullConstant
    }

    public Long256Constant(Long256 that) {
        this(that.getLong0(), that.getLong1(), that.getLong2(), that.getLong3());
    }

    public Long256Constant(long l0, long l1, long l2, long l3) {
        value = new Long256Impl();
        value.setAll(l0, l1, l2, l3);
    }

    @Override
    public long getLong0() {
        return value.getLong0();
    }

    @Override
    public long getLong1() {
        return value.getLong1();
    }

    @Override
    public long getLong2() {
        return value.getLong2();
    }

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        value.toSink(sink);
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
    public long getLong3() {
        return value.getLong3();
    }

    @Override
    public boolean isEquivalentTo(Function obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Long256Constant that) {
            return this.value.equals(that.value);
        }
        return false;
    }

    @Override
    public boolean isNullConstant() {
        return Long256Impl.NULL_LONG256.equals(value);
    }

    @Override
    public void setAll(long l0, long l1, long l2, long l3) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(value);
    }
}
