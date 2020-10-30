/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

class StrBindVariable extends StrFunction implements ScalarFunction {
    private final StringSink sink = new StringSink();
    private boolean isNull = false;

    public StrBindVariable(CharSequence value) {
        super(0);
        if (value == null) {
            isNull = true;
        } else {
            sink.put(value);
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

    public void setValue(CharSequence value) {
        sink.clear();
        if (value == null) {
            isNull = true;
        } else {
            isNull = false;
            sink.put(value);
        }
    }
}
