/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

class StrBindVariable extends StrFunction implements ScalarFunction, Mutable {
    private final StringSink sink = new StringSink();
    private boolean isNull = true;
    private final int floatScale;

    public StrBindVariable(int floatScale) {
        this.floatScale = floatScale;
    }

    @Override
    public void clear() {
        isNull = true;
        sink.clear();
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        if (isNull) {
            sink.put((CharSequence) null);
        } else {
            sink.put(this.sink);
        }
    }

    @Override
    public int getStrLen(Record rec) {
        if (isNull) {
            return -1;
        }
        return sink.length();
    }

    @Override
    public CharSequence getStr(Record rec) {
        return isNull ? null : sink;
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return isNull ? null : sink;
    }

    public void setValue(char value) {
        sink.clear();
        isNull = false;
        sink.put(value);
    }

    public void setValue(long l0, long l1, long l2, long l3) {
        sink.clear();
        isNull = false;
        Numbers.appendLong256(l0, l1, l2, l3, sink);
    }

    public void setValue(short value) {
        sink.clear();
        isNull = false;
        sink.put(value);
    }

    public void setValue(byte value) {
        sink.clear();
        isNull = false;
        sink.put(value);
    }

    public void setValue(long value) {
        isNull = value == Numbers.LONG_NaN;
        if (!isNull) {
            sink.clear();
            sink.put(value);
        }
    }

    public void setValue(int value) {
        isNull = value == Numbers.INT_NaN;
        if (!isNull) {
            sink.clear();
            sink.put(value);
        }
    }

    public void setTimestamp(long value) {
        isNull = value == Numbers.LONG_NaN;
        if (!isNull) {
            sink.clear();
            TimestampFormatUtils.appendDateTimeUSec(sink, value);
        }
    }

    public void setValue(double value) {
        isNull = value == Numbers.LONG_NaN;
        if (!isNull) {
            sink.clear();
            sink.put(value);
        }
    }
    public void setValue(float value) {
        isNull = value == Numbers.LONG_NaN;
        if (!isNull) {
            sink.clear();
            sink.put(value, floatScale);
        }
    }

    public void setValue(CharSequence value) {
        if (value == null) {
            isNull = true;
        } else {
            isNull = false;
            sink.clear();
            sink.put(value);
        }
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }
}
