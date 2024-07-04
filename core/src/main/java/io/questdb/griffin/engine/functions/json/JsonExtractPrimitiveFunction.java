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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

public class JsonExtractPrimitiveFunction implements ScalarFunction {
    private static final boolean defaultBool = false;
    private final int columnType;
    private final Function json;
    private final Function path;
    private final int jsonPosition;
    private final JsonExtractSupportingState state;
    private DirectUtf8Sink pointer;

    public JsonExtractPrimitiveFunction(
            int jsonPposition,
            int columnType,
            Function json,
            Function path
    ) {
        this.jsonPosition = jsonPposition;
        this.columnType = columnType;
        this.json = json;
        this.path = path;
        this.state = new JsonExtractSupportingState();
    }

    @Override
    public void close() {
        state.close();
        pointer = Misc.free(pointer);
        json.close();
        path.close();
    }

    @Override
    public BinarySequence getBin(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBinLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool(Record rec) {
        final Utf8Sequence jsonSeq = json.getVarcharA(rec);
        if (jsonSeq == null) {
            return defaultBool;
        }
        return state.parser.queryPointerBoolean(state.initPaddedJson(jsonSeq), pointer, state.simdJsonResult);
    }

    @Override
    public byte getByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public char getChar(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDate(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(Record rec) {
        final Utf8Sequence jsonSeq = json.getVarcharA(rec);
        if (jsonSeq == null) {
            return Double.NaN;
        }
        final double d = state.parser.queryPointerDouble(state.initPaddedJson(jsonSeq), pointer, state.simdJsonResult);
        state.throwIfInError(jsonPosition);
        return d;
    }

    @Override
    public float getFloat(Record rec) {
        return (float) getDouble(rec);
    }

    @Override
    public byte getGeoByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getGeoInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getGeoLong(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getGeoShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getIPv4(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(Record rec) {
        final Utf8Sequence jsonSeq = json.getVarcharA(rec);
        if (jsonSeq == null) {
            return Numbers.INT_NULL;
        }
        return state.parser.queryPointerInt(state.initPaddedJson(jsonSeq), pointer, state.simdJsonResult);
    }

    @Override
    public long getLong(Record rec) {
        final Utf8Sequence jsonSeq = json.getVarcharA(rec);
        if (jsonSeq == null) {
            return Numbers.LONG_NULL;
        }
        return state.parser.queryPointerLong(state.initPaddedJson(jsonSeq), pointer, state.simdJsonResult);
    }

    @Override
    public long getLong128Hi(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong128Lo(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long256 getLong256A(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long256 getLong256B(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(Record rec) {
        final Utf8Sequence jsonSeq = json.getVarcharA(rec);
        if (jsonSeq == null) {
            return 0;
        }
        return state.parser.queryPointerShort(state.initPaddedJson(jsonSeq), pointer, state.simdJsonResult);
    }

    @Override
    public void getStr(Record rec, Utf16Sink utf16Sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrA(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStrLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTimestamp(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getType() {
        return columnType;
    }

    @Override
    public @Nullable Utf8Sequence getVarcharA(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable Utf8Sequence getVarcharB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getVarcharSize(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return JsonExtractSupportingState.EXTRACT_FUNCTION_NAME;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        json.init(symbolTableSource, executionContext);
        path.init(symbolTableSource, executionContext);
        pointer = Misc.free(pointer);
        pointer = JsonExtractSupportingState.varcharConstantToJsonPointer(path);
    }
}
