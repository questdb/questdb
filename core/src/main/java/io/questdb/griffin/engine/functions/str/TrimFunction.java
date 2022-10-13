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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.str.StringSink;
import static io.questdb.std.Chars.trim;

public class TrimFunction extends StrFunction implements UnaryFunction {

    private final StringSink sink1 = new StringSink();
    private final StringSink sink2 = new StringSink();
    private final Function arg;
    private final TrimType type;

    public TrimFunction(Function arg, TrimType type) {
        this.arg = arg;
        this.type = type;
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public CharSequence getStr(final Record rec) {
        trim(type, getArg().getStr(rec), sink1);
        return sink1;
    }

    @Override
    public int getStrLen(Record rec) {
        int len = arg.getStrLen(rec);
        if (len == TableUtils.NULL_LEN) {
            return TableUtils.NULL_LEN;
        }
        trim(type, getArg().getStr(rec), sink1);
        return sink1.length();
    }

    @Override
    public CharSequence getStrB(final Record rec) {
        trim(type, getArg().getStr(rec), sink2);
        return sink2;
    }
}
