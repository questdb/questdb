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

import io.questdb.std.QuietCloseable;
import io.questdb.std.json.Json;
import io.questdb.std.json.JsonResult;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;

abstract class JsonPathFuncSupportingState implements QuietCloseable {
    public JsonResult jsonResult = new JsonResult();
    public DirectUtf8Sink jsonSink = null;

    @Override
    public void close() {
        if (jsonSink != null) {
            jsonSink.close();
        }

        jsonResult.close();
    }

    public void initJsonSink(Utf8Sequence json) {
        // TODO: This copy is possibly not necessary. Is there a way to avoid it?
        //       Can we detect when:
        //         * The data already exists in a malloc'ed memory buffer.
        //         * Such buffer has at least `Json.SIMDJSON_PADDING` bytes of allocated
        //           memory past the end of the data.
        //       If so, we can avoid the copy.
        if (jsonSink == null) {
            jsonSink = new DirectUtf8Sink(json.size() + Json.SIMDJSON_PADDING);
        } else {
            jsonSink.clear();
            jsonSink.reserve(json.size() + Json.SIMDJSON_PADDING);
        }
        jsonSink.put(json);
    }
}
