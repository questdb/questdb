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
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class JsonExtractVarcharFunction extends VarcharFunction {
    private final VarcharJsonExtractSupportingState a;
    private final VarcharJsonExtractSupportingState b;
    private final VarcharJsonExtractSupportingState copied;
    private final Function json;
    private final int jsonPosition;
    private final int maxSize;
    private final Function path;
    private DirectUtf8Sink pointer;

    public JsonExtractVarcharFunction(
            int jsonPosition,
            Function json,
            Function path,
            int maxSize
    ) {
        this.jsonPosition = jsonPosition;
        this.a = new VarcharJsonExtractSupportingState(new DirectUtf8Sink(maxSize));
        this.b = new VarcharJsonExtractSupportingState(new DirectUtf8Sink(maxSize));
        this.copied = new VarcharJsonExtractSupportingState();
        this.json = json;
        this.path = path;
        this.maxSize = maxSize;
    }

    @Override
    public void close() {
        a.close();
        b.close();
        copied.close();
        pointer = Misc.free(pointer);
    }

    @Override
    public String getName() {
        return JsonExtractSupportingState.EXTRACT_FUNCTION_NAME;
    }

    @Override
    public @Nullable DirectUtf8Sink getVarcharA(Record rec) {
        return jsonPointer(json.getVarcharA(rec), a);
    }

    @Override
    public @Nullable DirectUtf8Sink getVarcharB(Record rec) {
        return jsonPointer(json.getVarcharB(rec), b);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        json.init(symbolTableSource, executionContext);
        path.init(symbolTableSource, executionContext);
        pointer = Misc.free(pointer);
        pointer = JsonExtractSupportingState.varcharConstantToJsonPointer(path);
    }

    private @Nullable DirectUtf8Sink jsonPointer(Utf8Sequence json, VarcharJsonExtractSupportingState state) {
        if (json != null && pointer != null) {
            assert state.destSink != null;
            state.destSink.clear();
            state.parser.queryPointerUtf8(
                    state.initPaddedJson(json),
                    pointer,
                    state.simdJsonResult,
                    state.destSink,
                    maxSize
            );

            if (state.simdJsonResult.hasValue()) {
                return state.destSink;
            }
            state.throwIfInError(jsonPosition);
        }
        return null;
    }

    private static class VarcharJsonExtractSupportingState extends JsonExtractSupportingState {
        public boolean closeDestSink;
        public @Nullable DirectUtf8Sink destSink;

        public VarcharJsonExtractSupportingState() {
            this.closeDestSink = false;
        }

        public VarcharJsonExtractSupportingState(@NotNull DirectUtf8Sink destSink) {
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
