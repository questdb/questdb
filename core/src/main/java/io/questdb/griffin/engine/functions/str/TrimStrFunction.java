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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.str.StringSink;

import static io.questdb.std.Chars.trim;

public class TrimStrFunction extends StrFunction implements UnaryFunction {
    private final Function arg;
    private final StringSink sinkA = new StringSink();
    private final StringSink sinkB = new StringSink();
    private final TrimType type;

    public TrimStrFunction(Function arg, TrimType type) {
        this.arg = arg;
        this.type = type;
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public String getName() {
        switch (type) {
            case LTRIM:
                return "ltrim";
            case RTRIM:
                return "rtrim";
            default:
                return "trim";
        }
    }

    @Override
    public CharSequence getStrA(final Record rec) {
        final CharSequence charSequence = getArg().getStrA(rec);
        if (charSequence == null) {
            return null;
        }
        trim(type, charSequence, sinkA);
        return sinkA;
    }

    @Override
    public CharSequence getStrB(final Record rec) {
        final CharSequence charSequence = getArg().getStrA(rec);
        if (charSequence == null) {
            return null;
        }
        trim(type, charSequence, sinkB);
        return sinkB;
    }

    @Override
    public int getStrLen(Record rec) {
        final int len = arg.getStrLen(rec);
        if (len == TableUtils.NULL_LEN) {
            return TableUtils.NULL_LEN;
        }
        trim(type, getArg().getStrA(rec), sinkA);
        return sinkA.length();
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }
}
