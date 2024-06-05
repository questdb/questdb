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

package io.questdb.griffin.engine.functions.json;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;

public class JsonPathLongFunc extends LongFunction implements BinaryFunction {
    private final String functionName;
    private final Function json;
    private final Function path;
    private final DirectUtf8Sink pointer;
    private final SupportingState state;
    private final boolean strict;

    public JsonPathLongFunc(String functionName, Function json, Function path, DirectUtf8Sink pointer, boolean strict) {
        this.functionName = functionName;
        this.json = json;
        this.path = path;
        this.pointer = pointer;
        this.strict = strict;
        this.state = new SupportingState();
    }

    @Override
    public void close() {
        state.close();
        pointer.close();
    }

    @Override
    public Function getLeft() {
        return json;
    }

    @Override
    public long getLong(Record rec) {
        Utf8Sequence jsonSeq = json.getVarcharA(rec);
        if (jsonSeq == null) {
            return Long.MIN_VALUE;
        }
        final long res = state.parser.queryPointerLong(state.initPaddedJson(jsonSeq), pointer, state.jsonResult);
        if (strict && !state.jsonResult.isNull()) {
            state.jsonResult.throwIfError(functionName, path.getVarcharA(null));
        }
        return res;
    }

    @Override
    public Function getRight() {
        return path;
    }
}
