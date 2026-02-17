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
import io.questdb.griffin.engine.functions.IPv4Function;
import io.questdb.std.Numbers;

public class IPv4Constant extends IPv4Function implements ConstantFunction {
    public static final IPv4Constant NULL = new IPv4Constant(Numbers.IPv4_NULL);
    private final int value;

    public IPv4Constant(int value) {
        super();
        this.value = value;
    }

    public static IPv4Constant newInstance(int value) {
        return value != Numbers.IPv4_NULL ? new IPv4Constant(value) : NULL;
    }

    @Override
    public int getIPv4(Record rec) {
        return value;
    }

    @Override
    public boolean isEquivalentTo(Function obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof IPv4Constant that) {
            return this.value == that.value;
        }
        return false;
    }

    @Override
    public boolean isNullConstant() {
        return value == Numbers.IPv4_NULL;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(value);
    }
}
