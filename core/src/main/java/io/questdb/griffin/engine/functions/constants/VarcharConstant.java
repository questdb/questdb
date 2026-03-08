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
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.Chars;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

public class VarcharConstant extends VarcharFunction implements ConstantFunction {
    public static final VarcharConstant EMPTY = new VarcharConstant("");
    public static final VarcharConstant NULL = new VarcharConstant((Utf8Sequence) null);
    private final int length;
    private final String utf16Value;
    private final Utf8String value;

    public VarcharConstant(Utf8Sequence value) {
        if (value == null) {
            this.value = null;
            this.utf16Value = null;
            this.length = TableUtils.NULL_LEN;
        } else {
            if (Utf8s.startsWithAscii(value, "'")) {
                this.utf16Value = Utf8s.toString(value, 1, value.size() - 1, value.byteAt(0));
                this.value = new Utf8String(utf16Value);
            } else {
                this.value = Utf8String.newInstance(value);
                this.utf16Value = Utf8s.toString(value);
            }
            this.length = this.utf16Value.length();
        }
    }

    public VarcharConstant(CharSequence utf16Value) {
        if (utf16Value == null) {
            this.utf16Value = null;
            this.value = null;
            this.length = TableUtils.NULL_LEN;
        } else {
            if (Chars.startsWith(utf16Value, '\'')) {
                this.utf16Value = Chars.toString(utf16Value, 1, utf16Value.length() - 1, utf16Value.charAt(0));
                this.value = new Utf8String(this.utf16Value);
            } else {
                this.utf16Value = Chars.toString(utf16Value);
                this.value = new Utf8String(utf16Value);
            }
            this.length = this.utf16Value.length();
        }
    }

    public static VarcharConstant newInstance(CharSequence value) {
        return value != null ? new VarcharConstant(value) : NULL;
    }

    public static VarcharConstant newInstance(Utf8Sequence value) {
        return value != null ? new VarcharConstant(value) : NULL;
    }

    @Override
    public CharSequence getStrA(Record rec) {
        return utf16Value;
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return utf16Value;
    }

    @Override
    public int getStrLen(Record rec) {
        return length;
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        return value;
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        return value;
    }

    @Override
    public int getVarcharSize(Record rec) {
        return value != null ? value.size() : TableUtils.NULL_LEN;
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
