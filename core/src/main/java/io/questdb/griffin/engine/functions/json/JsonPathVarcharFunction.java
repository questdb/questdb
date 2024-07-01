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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.Misc;
import io.questdb.std.json.SimdJsonError;
import io.questdb.std.json.SimdJsonResult;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class JsonPathVarcharFunction extends VarcharFunction implements JsonPathFunction {
    private final VarcharSupportingState a;
    private final VarcharSupportingState b;
    private final VarcharSupportingState copied;
    private final @NotNull String functionName;
    private final Function json;
    private final int maxSize;
    private final Function path;
    private final int position;
    private final boolean strict;
    private DirectUtf8Sink defaultVarchar = null;
    private DirectUtf8Sink pointer;

    public JsonPathVarcharFunction(
            @NotNull String functionName,
            int position,
            Function json,
            Function path,
            int maxSize,
            boolean strict
    ) {
        this.functionName = functionName;
        this.position = position;
        this.a = new VarcharSupportingState(new DirectUtf8Sink(maxSize));
        this.b = new VarcharSupportingState(new DirectUtf8Sink(maxSize));
        this.copied = new VarcharSupportingState();
        this.json = json;
        this.path = path;
        this.maxSize = maxSize;
        this.strict = strict;
    }

    @Override
    public void close() {
        a.close();
        b.close();
        copied.close();
        pointer = Misc.free(pointer);
        defaultVarchar = Misc.free(defaultVarchar);
    }

    @Override
    public String getName() {
        return functionName;
    }

    @Override
    public void getVarchar(Record rec, Utf8Sink utf8Sink) {
        if (utf8Sink instanceof DirectUtf8Sink) {
            if ((copied.destSink != null) && copied.closeDestSink) {
                copied.destSink.close();
            }
            copied.destSink = (DirectUtf8Sink) utf8Sink;
            copied.closeDestSink = false;
            jsonPointer(json.getVarcharA(rec), pointer, copied);
        } else {
            if (copied.destSink == null) {
                copied.destSink = new DirectUtf8Sink(maxSize);
                copied.closeDestSink = true;
            } else {
                copied.destSink.clear();
                copied.destSink.reserve(maxSize);
            }
            jsonPointer(json.getVarcharA(rec), pointer, copied);
            utf8Sink.put(copied.destSink);
        }
    }

    @Override
    public @Nullable DirectUtf8Sink getVarcharA(Record rec) {
        Utf8Sequence utf8Json = json.getVarcharA(rec);
        if (utf8Json != null && pointer != null) {
            return jsonPointer(utf8Json, pointer, a);
        }
        return null;
    }

    @Override
    public @Nullable DirectUtf8Sink getVarcharB(Record rec) {
        Utf8Sequence utf8Json = json.getVarcharB(rec);
        if (utf8Json != null && pointer != null) {
            return jsonPointer(utf8Json, pointer, b);
        }
        return null;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        json.init(symbolTableSource, executionContext);
        path.init(symbolTableSource, executionContext);
        pointer = Misc.free(pointer);
        pointer = JsonPathFunction.varcharConstantToJsonPointer(path);
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
        state.parser.queryPointerString(
                state.initPaddedJson(json),
                pointer,
                state.simdJsonResult,
                state.destSink,
                maxSize,
                defaultValuePtr,
                defaultValueSize
        );
        if (state.simdJsonResult.hasValue()) {
            return state.destSink;
        } else if (strict && !state.simdJsonResult.isNull()) {
            final int error = state.simdJsonResult.getError();
            if (error != SimdJsonError.SUCCESS) {
                // the path is constant or runtime constant, we're allowed to use null record
                throw SimdJsonResult.formatError(position, functionName, path.getVarcharA(null), error);
            }
        }
        return defaultVarchar;  // usually null
    }

    private static class VarcharSupportingState extends SupportingState {
        public boolean closeDestSink;
        public @Nullable DirectUtf8Sink destSink;

        public VarcharSupportingState() {
            this.closeDestSink = false;
        }

        public VarcharSupportingState(@NotNull DirectUtf8Sink destSink) {
            this.destSink = destSink;
            this.closeDestSink = true;
        }

        @Override
        public void close() {
            super.close();
            if (closeDestSink) {
                destSink = Misc.free(destSink);
            }
        }
    }
}
