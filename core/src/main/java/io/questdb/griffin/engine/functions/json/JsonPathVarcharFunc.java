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

class JsonPathVarcharFunc extends VarcharFunction implements BinaryFunction, JsonPathFunc {
    private final int position;
    private final VarcharSupportingState a;
    private final VarcharSupportingState b;
    private final VarcharSupportingState copied;
    private final Function json;
    private final int maxSize;
    private final Function path;
    private final DirectUtf8Sink pointer;
    private final boolean strict;
    private DirectUtf8Sink defaultVarchar = null;

    public JsonPathVarcharFunc(
            int position,
            Function json,
            Function path,
            DirectUtf8Sink pointer,
            int maxSize,
            boolean strict) {
        this.position = position;
        this.a = new VarcharSupportingState(new DirectUtf8Sink(maxSize));
        this.b = new VarcharSupportingState(new DirectUtf8Sink(maxSize));
        this.copied = new VarcharSupportingState();
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
        if (defaultVarchar != null) {
            defaultVarchar.close();
        }
    }

    @Override
    public Function getLeft() {
        return json;
    }

    @Override
    public String getName() {
        return "json_path";
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
            jsonPointer(json.getVarcharA(rec), pointer, Objects.requireNonNull(path.getVarcharA(null)), copied);
        } else {
            if (copied.destSink == null) {
                copied.destSink = new DirectUtf8Sink(maxSize);
                copied.closeDestSink = true;
            } else {
                copied.destSink.clear();
                copied.destSink.reserve(maxSize);
            }
            jsonPointer(json.getVarcharA(rec), pointer, Objects.requireNonNull(path.getVarcharA(null)), copied);
            utf8Sink.put(copied.destSink);
        }
    }

    @Override
    public @Nullable DirectUtf8Sink getVarcharA(Record rec) {
        return jsonPointer(json.getVarcharA(rec), pointer, Objects.requireNonNull(path.getVarcharA(null)), a);
    }

    @Override
    public @Nullable DirectUtf8Sink getVarcharB(Record rec) {
        return jsonPointer(json.getVarcharB(rec), pointer, Objects.requireNonNull(path.getVarcharB(null)), b);
    }

    @Override
    public void setDefaultBool(boolean bool) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDefaultDouble(double aDouble) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDefaultFloat(float aFloat) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDefaultInt(int anInt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDefaultLong(long aLong) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDefaultShort(short aShort) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDefaultSymbol(CharSequence symbol) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDefaultVarchar(Utf8Sequence varcharA) {
        defaultVarchar = new DirectUtf8Sink(varcharA.size());
        defaultVarchar.put(varcharA);
    }

    private @Nullable DirectUtf8Sink jsonPointer(
            @Nullable Utf8Sequence json,
            @NotNull DirectUtf8Sequence pointer,
            @NotNull Utf8Sequence path,
            VarcharSupportingState state
    ) {
        if (json == null) {
            return null;
        }
        assert state.destSink != null;
        state.destSink.clear();
        long defaultValuePtr = 0;
        long defaultValueSize = 0;
        if (defaultVarchar != null) {
            defaultValuePtr = defaultVarchar.ptr();
            defaultValueSize = defaultVarchar.size();
        }
        state.parser.queryPointerString(state.initPaddedJson(json), pointer, state.simdJsonResult, state.destSink, maxSize, defaultValuePtr, defaultValueSize);
        if (state.simdJsonResult.hasValue()) {
            return state.destSink;
        } else if (strict && !state.simdJsonResult.isNull()) {
            state.simdJsonResult.throwIfError(path);
        }
        return defaultVarchar;  // usually null
    }

    private static class VarcharSupportingState extends SupportingState {
        public boolean closeDestSink;
        public @Nullable DirectUtf8Sink destSink;

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
