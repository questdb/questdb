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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.Chars;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;

public class StrConstant extends StrFunction implements ConstantFunction {
    public static final StrConstant EMPTY = new StrConstant("");
    public static final StrConstant NULL = new StrConstant(null);
    private final int length;
    private final Utf8String utf8Value;
    private final String value;

    public StrConstant(CharSequence value) {
        if (value == null) {
            this.value = null;
            this.utf8Value = null;
            this.length = TableUtils.NULL_LEN;
        } else {
            if (Chars.startsWith(value, '\'')) {
                this.value = Chars.toString(value, 1, value.length() - 1, value.charAt(0));
            } else {
                this.value = Chars.toString(value);
            }
            this.utf8Value = new Utf8String(this.value);
            this.length = this.value.length();
        }
    }

    public static StrConstant newInstance(CharSequence value) {
        return value != null ? new StrConstant(value) : NULL;
    }

    @Override
    public CharSequence getStrA(Record rec) {
        return value;
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return value;
    }

    @Override
    public int getStrLen(Record rec) {
        return length;
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        return utf8Value;
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        return utf8Value;
    }

    @Override
    public int getVarcharSize(Record rec) {
        return utf8Value != null ? utf8Value.size() : TableUtils.NULL_LEN;
    }

    @Override
    public boolean isNullConstant() {
        return value == null;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (value == null) {
            sink.val("null");
        } else {
            sink.val('\'').val(value).val('\'');
        }
    }
}
