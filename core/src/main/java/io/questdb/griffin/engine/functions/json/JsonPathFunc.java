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
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

class JsonPathFunc extends VarcharFunction implements BinaryFunction {
    private final VarcharSupportingState a;
    private final VarcharSupportingState b;
    private final VarcharSupportingState copied;
    private final String functionName;
    private final Function json;
    private final int maxSize;
    private final Function path;
    private final DirectUtf8Sink pointer;
    private final boolean strict;

    public JsonPathFunc(String functionName, Function json, Function path, DirectUtf8Sink pointer, int maxSize, boolean strict) {
        this.a = new VarcharSupportingState(new DirectUtf8Sink(maxSize));
        this.b = new VarcharSupportingState(new DirectUtf8Sink(maxSize));
        this.copied = new VarcharSupportingState();
        this.functionName = functionName;
        this.json = json;
        this.path = path;
        this.pointer = pointer;
        this.maxSize = maxSize;
        this.strict = strict;
    }

    @Override
    public void close() {
        a.close();
        b.close();
        copied.close();
        pointer.close();
    }

    @Override
    public Function getLeft() {
        return json;
    }

    @Override
    public String getName() {
        return functionName;
    }

    @Override
    public Function getRight() {
        return path;
    }

    @Override
    public void getVarchar(Record rec, Utf8Sink utf8Sink) {
        if (utf8Sink instanceof DirectUtf8Sink) {
            if ((copied.destSink != null) && copied.closeDestSink) {
                copied.destSink.close();
            }
            copied.destSink = (DirectUtf8Sink) utf8Sink;
            copied.closeDestSink = false;
            jsonPointer(functionName, json.getVarcharA(rec), pointer, Objects.requireNonNull(path.getVarcharA(null)), copied);
        } else {
            if (copied.destSink == null) {
                copied.destSink = new DirectUtf8Sink(maxSize);
                copied.closeDestSink = true;
            } else {
                copied.destSink.clear();
                copied.destSink.reserve(maxSize);
            }
            jsonPointer(functionName, json.getVarcharA(rec), pointer, Objects.requireNonNull(path.getVarcharA(null)), copied);
            utf8Sink.put(copied.destSink);
        }
    }

    @Override
    public @Nullable DirectUtf8Sink getVarcharA(Record rec) {
        return jsonPointer(functionName, json.getVarcharA(rec), pointer, Objects.requireNonNull(path.getVarcharA(null)), a);
    }

    @Override
    public @Nullable DirectUtf8Sink getVarcharB(Record rec) {
        return jsonPointer(functionName, json.getVarcharB(rec), pointer, Objects.requireNonNull(path.getVarcharB(null)), b);
    }

    private @Nullable DirectUtf8Sink jsonPointer(
            String functionName,
            @Nullable Utf8Sequence json,
            @NotNull DirectUtf8Sequence pointer,
            @NotNull Utf8Sequence path,
            VarcharSupportingState state
    ) {
        if (json == null) {
            return null;
        }
        state.destSink.clear();
        state.parser.queryPointer(state.initPaddedJson(json), pointer, state.jsonResult, state.destSink, maxSize);
        if (state.jsonResult.hasValue()) {
            return state.destSink;
        } else if (strict && !state.jsonResult.isNull()) {
            state.jsonResult.throwIfError(functionName, path);
        }
        return null;
    }

    private static class VarcharSupportingState extends SupportingState {
        public @Nullable DirectUtf8Sink destSink;
        public boolean closeDestSink;

        public VarcharSupportingState() {
            this.destSink = null;
            this.closeDestSink = false;
        }

        public VarcharSupportingState(@NotNull DirectUtf8Sink destSink) {
            this.destSink = destSink;
            this.closeDestSink = true;
        }

        @Override
        public void close() {
            super.close();

            if (closeDestSink && destSink != null) {
                destSink.close();
            }
        }
    }
}
