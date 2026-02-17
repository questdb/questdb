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

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.str.Utf8Sequence;

public class BooleanConstant extends BooleanFunction implements ConstantFunction {

    public static final BooleanConstant FALSE = new BooleanConstant(false);
    public static final BooleanConstant TRUE = new BooleanConstant(true);
    private final boolean value;

    private BooleanConstant(boolean value) {
        this.value = value;
    }

    public static BooleanConstant of(boolean value) {
        return value ? TRUE : FALSE;
    }

    @Override
    public boolean getBool(Record rec) {
        return value;
    }

    @Override
    public byte getByte(Record rec) {
        return (byte) (value ? 1 : 0);
    }

    @Override
    public char getChar(Record rec) {
        return value ? 'T' : 'F';
    }

    @Override
    public long getDate(Record rec) {
        return value ? 1 : 0;
    }

    @Override
    public double getDouble(Record rec) {
        return value ? 1 : 0;
    }

    @Override
    public float getFloat(Record rec) {
        return value ? 1 : 0;
    }

    @Override
    public int getInt(Record rec) {
        return value ? 1 : 0;
    }

    @Override
    public long getLong(Record rec) {
        return value ? 1 : 0;
    }

    @Override
    public short getShort(Record rec) {
        return (short) (value ? 1 : 0);
    }

    @Override
    public long getTimestamp(Record rec) {
        return value ? 1 : 0;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(value);
    }

    @Override
    protected String getStr0(Record rec) {
        return value ? "true" : "false";
    }

    @Override
    protected Utf8Sequence getVarchar0(Record rec) {
        return value ? UTF_8_TRUE : UTF_8_FALSE;
    }
}
