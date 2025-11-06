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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;

@SuppressWarnings("resource")
class Long256CaseFunction extends Long256Function implements CaseFunction {
    private final ObjList<Function> args;
    private final CaseFunctionPicker picker;

    public Long256CaseFunction(CaseFunctionPicker picker, ObjList<Function> args) {
        this.picker = picker;
        this.args = args;
    }

    @Override
    public ObjList<Function> args() {
        return args;
    }

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        picker.pick(rec).getLong256(rec, sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        return picker.pick(rec).getLong256A(rec);
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return picker.pick(rec).getLong256B(rec);
    }
}
