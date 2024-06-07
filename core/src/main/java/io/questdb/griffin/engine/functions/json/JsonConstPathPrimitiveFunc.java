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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.ScalarFunction;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.*;
import org.jetbrains.annotations.Nullable;

public class JsonConstPathPrimitiveFunc implements ScalarFunction, BinaryFunction {
    private final int columnType;
    private final String functionName;
    private final Function json;
    private final Function path;
    private final DirectUtf8Sink pointer;
    private final SupportingState state;
    private final boolean strict;

    public JsonConstPathPrimitiveFunc(
            int columnType,
            String functionName,
            Function json,
            Function path,
            DirectUtf8Sink pointer,
            boolean strict) {
        this.columnType = columnType;
        this.functionName = functionName;
        this.json = json;
        this.path = path;
        this.pointer = pointer;
        this.strict = strict;
        this.state = new SupportingState();
    }

    @Override
    public void close() {
        state.close();
        pointer.close();
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
            return false;
        }
        final boolean res = state.parser.queryPointerBoolean(state.initPaddedJson(jsonSeq), pointer, state.jsonResult);
        if (strict && !state.jsonResult.isNull()) {
            state.jsonResult.throwIfError(functionName, path.getVarcharA(null));
        }
        return res;
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
        final double res = state.parser.queryPointerDouble(state.initPaddedJson(jsonSeq), pointer, state.jsonResult);
        if (strict && !state.jsonResult.isNull()) {
            state.jsonResult.throwIfError(functionName, path.getVarcharA(null));
        }
        return res;
    }

    @Override
    public float getFloat(Record rec) {
        final Utf8Sequence jsonSeq = json.getVarcharA(rec);
        if (jsonSeq == null) {
            return Float.NaN;
        }
        final float res = state.parser.queryPointerFloat(state.initPaddedJson(jsonSeq), pointer, state.jsonResult);
        if (strict && !state.jsonResult.isNull()) {
            state.jsonResult.throwIfError(functionName, path.getVarcharA(null));
        }
        return res;
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
            return Integer.MIN_VALUE;
        }
        final int res = state.parser.queryPointerInt(state.initPaddedJson(jsonSeq), pointer, state.jsonResult);
        if (strict && !state.jsonResult.isNull()) {
            state.jsonResult.throwIfError(functionName, path.getVarcharA(null));
        }
        return res;
    }

    @Override
    public Function getLeft() {
        return json;
    }

    @Override
    public long getLong(Record rec) {
        final Utf8Sequence jsonSeq = json.getVarcharA(rec);
        if (jsonSeq == null) {
            return Long.MIN_VALUE;
        }
        final long res = state.parser.queryPointerLong(state.initPaddedJson(jsonSeq), pointer, state.jsonResult);
        if (strict && !state.jsonResult.isNull()) {
            state.jsonResult.throwIfError(functionName, path.getVarcharA(null));
        }
        return res;
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
    public Function getRight() {
        return path;
    }

    @Override
    public short getShort(Record rec) {
        final Utf8Sequence jsonSeq = json.getVarcharA(rec);
        if (jsonSeq == null) {
            return Short.MIN_VALUE;
        }
        final short res = state.parser.queryPointerShort(state.initPaddedJson(jsonSeq), pointer, state.jsonResult);
        if (strict && !state.jsonResult.isNull()) {
            state.jsonResult.throwIfError(functionName, path.getVarcharA(null));
        }
        return res;
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
    public void getVarchar(Record rec, Utf8Sink utf8Sink) {
        throw new UnsupportedOperationException();
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
}
