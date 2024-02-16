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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.ScalarFunction;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.*;

public class StrBindVariable extends StrFunction implements ScalarFunction, Mutable {
    private final int floatScale;
    private final StringSink sink = new StringSink();
    private final Utf8StringSink utf8Sink = new Utf8StringSink();
    private boolean isNull = true;

    public StrBindVariable(int floatScale) {
        this.floatScale = floatScale;
    }

    @Override
    public void clear() {
        isNull = true;
        sink.clear();
        utf8Sink.clear();
    }

    @Override
    public void getStr(Record rec, Utf16Sink utf16Sink) {
        if (isNull) {
            utf16Sink.put((CharSequence) null);
        } else {
            utf16Sink.put(sink);
        }
    }

    @Override
    public CharSequence getStr(Record rec) {
        return isNull ? null : sink;
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return isNull ? null : sink;
    }

    @Override
    public int getStrLen(Record rec) {
        if (isNull) {
            return -1;
        }
        return sink.length();
    }

    @Override
    public void getVarchar(Record rec, Utf8Sink utf8Sink) {
        if (isNull) {
            utf8Sink.put((CharSequence) null);
        } else {
            utf8Sink.put(this.utf8Sink);
        }
    }

    @Override
    public void getVarchar(Record rec, Utf16Sink utf16Sink) {
        if (isNull) {
            utf16Sink.put((CharSequence) null);
        } else {
            utf16Sink.put(sink);
        }
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
    public boolean isRuntimeConstant() {
        return true;
    }

    public void setTimestamp(long value) {
        isNull = value == Numbers.LONG_NaN;
        if (!isNull) {
            sink.clear();
            TimestampFormatUtils.appendDateTimeUSec(sink, value);
            utf8Sink.clear();
            TimestampFormatUtils.appendDateTimeUSec(utf8Sink, value);
        }
    }

    public void setUuidValue(long lo, long hi) {
        sink.clear();
        if (SqlUtil.implicitCastUuidAsStr(lo, hi, sink)) {
            utf8Sink.clear();
            Numbers.appendUuid(lo, hi, utf8Sink);
            isNull = false;
        }
    }

    public void setValue(char value) {
        isNull = false;
        sink.clear();
        sink.put(value);
        utf8Sink.clear();
        utf8Sink.put(value);
    }

    public void setValue(long l0, long l1, long l2, long l3) {
        isNull = false;
        sink.clear();
        Numbers.appendLong256(l0, l1, l2, l3, sink);
        utf8Sink.clear();
        Numbers.appendLong256(l0, l1, l2, l3, utf8Sink);
    }

    public void setValue(short value) {
        isNull = false;
        sink.clear();
        sink.put(value);
        utf8Sink.clear();
        utf8Sink.put(value);
    }

    public void setValue(byte value) {
        isNull = false;
        sink.clear();
        sink.put(value);
        utf8Sink.clear();
        utf8Sink.put((int) value);
    }

    public void setValue(long value) {
        isNull = value == Numbers.LONG_NaN;
        if (!isNull) {
            sink.clear();
            sink.put(value);
            utf8Sink.clear();
            utf8Sink.put(value);
        }
    }

    public void setValue(int value) {
        isNull = value == Numbers.INT_NaN;
        if (!isNull) {
            sink.clear();
            sink.put(value);
            utf8Sink.clear();
            utf8Sink.put(value);
        }
    }

    public void setValue(double value) {
        isNull = value == Numbers.LONG_NaN;
        if (!isNull) {
            sink.clear();
            sink.put(value);
            utf8Sink.clear();
            utf8Sink.put(value);
        }
    }

    public void setValue(float value) {
        isNull = value == Numbers.LONG_NaN;
        if (!isNull) {
            sink.clear();
            sink.put(value, floatScale);
            utf8Sink.clear();
            utf8Sink.put(value, floatScale);
        }
    }

    public void setValue(CharSequence value) {
        if (value == null) {
            isNull = true;
        } else {
            isNull = false;
            sink.clear();
            sink.put(value);
            utf8Sink.clear();
            utf8Sink.put(value);
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("?::string");
    }
}
