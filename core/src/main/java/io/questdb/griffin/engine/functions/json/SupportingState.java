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

import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.json.SimdJsonParser;
import io.questdb.std.json.SimdJsonResult;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

class SupportingState implements QuietCloseable {
    public DirectUtf8Sequence jsonSeq = null;
    public SimdJsonParser parser = new SimdJsonParser();
    public SimdJsonResult simdJsonResult = new SimdJsonResult();
    private DirectUtf8Sink jsonSink = null;

    @Override
    public void close() {
        parser = Misc.free(parser);
        jsonSink = Misc.free(jsonSink);
        simdJsonResult = Misc.free(simdJsonResult);
    }

    public DirectUtf8Sequence initPaddedJson(@NotNull Utf8Sequence json) {
        if ((json instanceof DirectUtf8Sequence) && ((DirectUtf8Sequence) json).tailPadding() >= SimdJsonParser.SIMDJSON_PADDING) {
            jsonSeq = (DirectUtf8Sequence) json;
        } else {
            if (jsonSink == null) {
                jsonSink = new DirectUtf8Sink(json.size() + SimdJsonParser.SIMDJSON_PADDING);
            } else {
                jsonSink.clear();
                jsonSink.reserve(json.size() + SimdJsonParser.SIMDJSON_PADDING);
            }
            jsonSink.put(json);
            jsonSeq = jsonSink;
        }
        return jsonSeq;
    }
}
