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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;

class StrCaseFunction extends StrFunction implements MultiArgFunction {
    private final CaseFunctionPicker picker;
    private final ObjList<Function> args;

    public StrCaseFunction(CaseFunctionPicker picker, ObjList<Function> args) {
        this.picker = picker;
        this.args = args;
    }

    @Override
    public CharSequence getStr(Record rec) {
        return picker.pick(rec).getStr(rec);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return picker.pick(rec).getStrB(rec);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        picker.pick(rec).getStr(rec, sink);
    }

    @Override
    public int getStrLen(Record rec) {
        return picker.pick(rec).getStrLen(rec);
    }

    @Override
    public ObjList<Function> getArgs() {
        return args;
    }
}
