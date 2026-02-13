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
import io.questdb.griffin.engine.functions.UInt16Function;
import io.questdb.std.Numbers;

public class UInt16Constant extends UInt16Function implements ConstantFunction {
    public static final UInt16Constant NULL = new UInt16Constant(Numbers.UINT16_NULL);
    private final short value;

    public UInt16Constant(short value) {
        this.value = value;
    }

    public static UInt16Constant newInstance(short value) {
        return value != Numbers.UINT16_NULL ? new UInt16Constant(value) : NULL;
    }

    @Override
    public short getShort(Record rec) {
        return value;
    }

    @Override
    public boolean isEquivalentTo(Function obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof UInt16Constant that) {
            return this.value == that.value;
        }
        return false;
    }

    @Override
    public boolean isNullConstant() {
        return value == Numbers.UINT16_NULL;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(Short.toUnsignedInt(value));
    }
}
