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

package io.questdb.griffin.engine.functions.json;

import io.questdb.cairo.sql.Function;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.json.SimdJsonParser;
import io.questdb.std.json.SimdJsonResult;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Buffer state for JSON extraction functions.
 * This is used by the `JsonExtractFunction` to:
 * * Create a UTF-8 input buffer with sufficient padding where needed.
 * * Cache a UTF-8 result - sometimes as intermediate for TIMESTAMP, DATE or IPV4 parsing.
 * * Cache a UTF-16 result.
 */
public class JsonExtractSupportingState implements QuietCloseable {
    public static final String EXTRACT_FUNCTION_NAME = "json_extract";
    // Only set for VARCHAR extraction to provide string backwards compatibility.
    public @Nullable StringSink destUtf16Sink;
    // Only set for types that require UTF-8 storage: VARCHAR, TIMESTAMP, DATE or IPV4 parsing.
    public @Nullable DirectUtf8Sink destUtf8Sink;
    public DirectUtf8Sequence jsonSeq = null;
    public SimdJsonParser parser = new SimdJsonParser();
    public SimdJsonResult simdJsonResult = new SimdJsonResult();
    private DirectUtf8Sink jsonSink = null;

    JsonExtractSupportingState(@Nullable DirectUtf8Sink destUtf8Sink, boolean useUtf6Sink) {
        this.destUtf8Sink = destUtf8Sink;
        this.destUtf16Sink = useUtf6Sink ? new StringSink() : null;
    }

    public static DirectUtf8Sink varcharConstantToJsonPointer(Function fn) {
        final Utf8Sequence seq = fn.getVarcharA(null);
        if (seq == null) {
            return null;
        }
        try (DirectUtf8Sink path = new DirectUtf8Sink(seq.size())) {
            path.put(seq);
            final DirectUtf8Sink pointer = new DirectUtf8Sink(seq.size());
            SimdJsonParser.convertJsonPathToPointer(path, pointer);
            return pointer;
        }
    }

    @Override
    public void close() {
        parser = Misc.free(parser);
        jsonSink = Misc.free(jsonSink);
        simdJsonResult = Misc.free(simdJsonResult);
        destUtf8Sink = Misc.free(destUtf8Sink);
    }

    public void deflate() {
        Misc.free(destUtf8Sink);
    }

    public DirectUtf8Sequence initPaddedJson(@NotNull Utf8Sequence json) {
        if (json.tailPadding() >= SimdJsonParser.SIMDJSON_PADDING) {
            jsonSeq = (DirectUtf8Sequence) json;
        } else {
            initPaddedJsonSlow(json);
        }
        return jsonSeq;
    }

    public void reopen() {
        if (destUtf8Sink != null) {
            destUtf8Sink.reopen();
        }
    }

    private void initPaddedJsonSlow(@NotNull Utf8Sequence json) {
        if (jsonSink == null) {
            jsonSink = new DirectUtf8Sink(json.size() + SimdJsonParser.SIMDJSON_PADDING);
        } else {
            jsonSink.clear();
            jsonSink.reserve(json.size() + SimdJsonParser.SIMDJSON_PADDING);
        }
        jsonSink.put(json);
        jsonSeq = jsonSink;
    }
}
