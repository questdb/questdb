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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.json.Json;
import io.questdb.std.json.JsonResult;
import io.questdb.std.str.*;
import org.jetbrains.annotations.Nullable;

public class JsonPathVarcharFunctionFactory implements FunctionFactory {
    private static final String SIGNATURE = "json_path(Øø)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position, ObjList<Function> args, IntList argPositions,
            CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext
    ) {
        final Function json = args.getQuick(0);
        final Function path = args.getQuick(1);

        // Note: path is always a constant. We copy it just once to malloc'ed memory.
        Utf8Sequence pathSeq = path.getVarcharA(null);
        DirectUtf8Sink pathSink = new DirectUtf8Sink(pathSeq.size());
        pathSink.put(pathSeq);

        final int maxSize = configuration.getStrFunctionMaxBufferLength();
        return new Func(json, path, pathSink, maxSize);
    }

    private static class SupportingState implements QuietCloseable {
        public DirectUtf8Sink destSink = null;
        public DirectUtf8Sink jsonSink = null;
        public JsonResult jsonResult = new JsonResult();

        private void initJsonSink(Utf8Sequence json) {
            if (jsonSink == null) {
                jsonSink = new DirectUtf8Sink(json.size() + Json.SIMDJSON_PADDING);
            }
            else {
                jsonSink.clear();
                jsonSink.reserve(json.size() + Json.SIMDJSON_PADDING);
            }

            // TODO: This copy is possibly not necessary. Is there a way to avoid it?
            jsonSink.put(json);
        }

        @Override
        public void close() {
            // TODO: Should this be done here, considering it might be assigned from the outside?
            //       When executing from `public void getVarchar(Record rec, Utf8Sink utf8Sink) {`
            if (destSink != null) {
                destSink.close();
            }
            if (jsonSink != null) {
                jsonSink.close();
            }
        }
    }

    private static class Func extends VarcharFunction implements BinaryFunction {
        private final Function json;
        private final Function path;
        private final DirectUtf8Sink pathSink;
        private final SupportingState copied = new SupportingState();
        private final SupportingState a = new SupportingState();
        private final SupportingState b = new SupportingState();
        private final int maxSize;

        public Func(Function json, Function path, DirectUtf8Sink pathSeq, int maxSize) {
            this.json = json;
            this.path = path;
            this.pathSink = pathSeq;
            this.maxSize = maxSize;
        }

        @Override
        public Function getLeft() {
            return json;
        }

        @Override
        public Function getRight() {
            return path;
        }

        @Override
        public void getVarchar(Record rec, Utf8Sink utf8Sink) {
            if (utf8Sink instanceof DirectUtf8Sink) {
                copied.destSink = (DirectUtf8Sink) utf8Sink;
                jsonPath(json.getVarcharA(rec), pathSink, copied, maxSize);
            } else {
                // Extra intermediate copy from malloc'd memory to java memory required.
                // TODO: Should the `utf8Sink` sink be cleared here?
                //       Should I validate a minimum residual capacity?
                //       At the moment, the `maxSize` passed in is the min of whatever is available and
                //       what is the actual max.
                // final int curtailedMaxSize = (int) Math.min(maxSize, utf8Sink.capacity());  /// utf8Sink `capacity` is missing!
                final int curtailedMaxSize = maxSize;
                if (copied.destSink == null) {
                    copied.destSink = new DirectUtf8Sink(curtailedMaxSize);
                }
                else {
                    copied.destSink.clear();
                    copied.destSink.reserve(curtailedMaxSize);
                }
                jsonPath(json.getVarcharA(rec), pathSink, copied, curtailedMaxSize);
                utf8Sink.put(copied.destSink);
            }
        }

        @Override
        public @Nullable DirectUtf8Sink getVarcharA(Record rec) {
            return jsonPath(json.getVarcharA(rec), pathSink, a, maxSize);
        }

        @Override
        public @Nullable DirectUtf8Sink getVarcharB(Record rec) {
            return jsonPath(json.getVarcharB(rec), pathSink, b, maxSize);
        }

        private static @Nullable DirectUtf8Sink jsonPath(
                @Nullable Utf8Sequence json,
                @Nullable DirectUtf8Sequence path,
                SupportingState state,
                int maxSize
        ) {
            if (json == null || path == null) {
                return null;
            }
            if (state.destSink == null) {
                state.destSink = new DirectUtf8Sink(maxSize);
            }
            else {
                state.destSink.clear();
            }
            state.initJsonSink(json);
            Json.queryPath(state.jsonSink, path, state.jsonResult, state.destSink, maxSize);
            if (state.jsonResult.hasValue()) {
                return state.destSink;
            }
            return null;
        }

        @Override
        public void close() {
            copied.close();
            a.close();
            b.close();
            json.close();
            path.close();
            pathSink.close();
        }
    }
}
