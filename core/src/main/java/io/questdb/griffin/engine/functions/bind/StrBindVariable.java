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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

public class StrBindVariable extends StrFunction implements Mutable {
    private final StringSink utf16Sink = new StringSink();
    private final Utf8StringSink utf8Sink = new Utf8StringSink();
    private boolean isNull = true;

    @Override
    public void clear() {
        isNull = true;
        utf16Sink.clear();
        utf8Sink.clear();
    }

    @Override
    public CharSequence getStrA(Record rec) {
        return isNull ? null : utf16Sink;
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return isNull ? null : utf16Sink;
    }

    @Override
    public int getStrLen(Record rec) {
        if (isNull) {
            return -1;
        }
        return utf16Sink.length();
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        return isNull ? null : utf8Sink;
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        return isNull ? null : utf8Sink;
    }

    @Override
    public int getVarcharSize(Record rec) {
        if (isNull) {
            return -1;
        }
        return utf8Sink.size();
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

    public void setTimestamp(long value, int timestampType) {
        isNull = value == Numbers.LONG_NULL;
        if (!isNull) {
            utf16Sink.clear();
            TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);
            timestampDriver.append(utf16Sink, value);
            utf8Sink.clear();
            timestampDriver.append(utf8Sink, value);
        }
    }

    public void setUuidValue(long lo, long hi) {
        utf16Sink.clear();
        if (SqlUtil.implicitCastUuidAsStr(lo, hi, utf16Sink)) {
            utf8Sink.clear();
            Numbers.appendUuid(lo, hi, utf8Sink);
            isNull = false;
        }
    }

    public void setValue(boolean value) {
        isNull = false;
        utf16Sink.clear();
        utf16Sink.put(value);
        utf8Sink.clear();
        utf8Sink.put(value);
    }

    public void setValue(char value) {
        isNull = false;
        utf16Sink.clear();
        utf16Sink.put(value);
        utf8Sink.clear();
        utf8Sink.put(value);
    }

    public void setValue(long l0, long l1, long l2, long l3) {
        isNull = false;
        utf16Sink.clear();
        Numbers.appendLong256(l0, l1, l2, l3, utf16Sink);
        utf8Sink.clear();
        Numbers.appendLong256(l0, l1, l2, l3, utf8Sink);
    }

    public void setValue(short value) {
        isNull = false;
        utf16Sink.clear();
        utf16Sink.put(value);
        utf8Sink.clear();
        utf8Sink.put(value);
    }

    public void setValue(byte value) {
        isNull = false;
        utf16Sink.clear();
        utf16Sink.put(value);
        utf8Sink.clear();
        utf8Sink.put((int) value);
    }

    public void setValue(long value) {
        isNull = value == Numbers.LONG_NULL;
        if (!isNull) {
            utf16Sink.clear();
            utf16Sink.put(value);
            utf8Sink.clear();
            utf8Sink.put(value);
        }
    }

    public void setValue(int value) {
        isNull = value == Numbers.INT_NULL;
        if (!isNull) {
            utf16Sink.clear();
            utf16Sink.put(value);
            utf8Sink.clear();
            utf8Sink.put(value);
        }
    }

    public void setValue(double value) {
        isNull = Numbers.isNull(value);
        if (!isNull) {
            utf16Sink.clear();
            utf16Sink.put(value);
            utf8Sink.clear();
            utf8Sink.put(value);
        }
    }

    public void setValue(float value) {
        isNull = Numbers.isNull(value);
        if (!isNull) {
            utf16Sink.clear();
            utf16Sink.put(value);
            utf8Sink.clear();
            utf8Sink.put(value);
        }
    }

    public void setValue(CharSequence value) {
        if (value == null) {
            isNull = true;
        } else {
            isNull = false;
            utf16Sink.clear();
            utf16Sink.put(value);
            utf8Sink.clear();
            utf8Sink.put(value);
        }
    }

    public void setValue(Utf8Sequence value) {
        if (value == null) {
            isNull = true;
        } else {
            isNull = false;
            utf16Sink.clear();
            utf16Sink.put(value);
            utf8Sink.clear();
            utf8Sink.put(value);
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("?::string");
    }
}
