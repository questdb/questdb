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
import io.questdb.std.QuietCloseable;
import io.questdb.std.json.Json;
import io.questdb.std.json.JsonResult;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

class SupportingState implements QuietCloseable {
    public Json parser = new Json();
    public JsonResult jsonResult = new JsonResult();
    private DirectUtf8Sink jsonSink = null;
    public DirectUtf8Sequence jsonSeq = null;

    public static @NotNull DirectUtf8Sink varcharConstantToJsonPointer(Function fn) {
        assert fn.isConstant();
        final Utf8Sequence seq = fn.getVarcharA(null);
        assert seq != null;
        try (DirectUtf8Sink path = new DirectUtf8Sink(seq.size())) {
            path.put(seq);
            final DirectUtf8Sink pointer = new DirectUtf8Sink(seq.size());
            Json.convertJsonPathToPointer(path, pointer);
            return pointer;
        }
    }

    @Override
    public void close() {
        parser.close();

        if (jsonSink != null) {
            jsonSink.close();
        }

        jsonResult.close();
    }

    public DirectUtf8Sequence initPaddedJson(@NotNull Utf8Sequence json) {
        if ((json instanceof DirectUtf8Sequence) && ((DirectUtf8Sequence) json).tailPadding() >= Json.SIMDJSON_PADDING) {
            jsonSeq = (DirectUtf8Sequence) json;
        }
        else {
            if (jsonSink == null) {
                jsonSink = new DirectUtf8Sink(json.size() + Json.SIMDJSON_PADDING);
            } else {
                jsonSink.clear();
                jsonSink.reserve(json.size() + Json.SIMDJSON_PADDING);
            }
            jsonSink.put(json);
            jsonSeq = jsonSink;
        }
        return jsonSeq;
    }
}
