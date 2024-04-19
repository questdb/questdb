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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.str.Utf8s.trim;

public class TrimConstVarcharFunction extends VarcharFunction implements UnaryFunction {

    private final Function arg;
    private final DirectUtf8Sink sink1;
    private final DirectUtf8Sink sink2;

    public TrimConstVarcharFunction(Function arg, TrimType type) {
        this.arg = arg;
        Utf8Sequence value = getArg().getVarcharA(null);
        if (value == null) {
            this.sink1 = new DirectUtf8Sink(0);
            this.sink2 = new DirectUtf8Sink(0);
        } else {
            this.sink1 = new DirectUtf8Sink(value.size());
            trim(type, value, sink1);
            this.sink2 = new DirectUtf8Sink(sink1.size());
            sink2.put(sink1);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public void getVarchar(Record rec, Utf8Sink utf8Sink) {
        utf8Sink.put(sink1);
    }

    @Override
    public @Nullable Utf8Sequence getVarcharA(Record rec) {
        return sink1;
    }

    @Override
    public @Nullable Utf8Sequence getVarcharB(Record rec) {
        return sink2;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val('\'').val(sink1).val('\'');
    }
}
