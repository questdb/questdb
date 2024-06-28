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
import io.questdb.std.json.SimdJsonError;
import io.questdb.std.json.SimdJsonResult;
import io.questdb.std.str.*;
import org.jetbrains.annotations.Nullable;

public class JsonPathPrimitiveFunction implements ScalarFunction, BinaryFunction, JsonPathFunction {
    private final int columnType;
    private final String functionName;
    private final Function json;
    private final Function path;
    private final DirectUtf8Sink pointer;
    private final int position;
    private final SupportingState state;
    private final boolean strict;
    private boolean defaultBool = false;
    private double defaultDouble = Double.NaN;
    private float defaultFloat = Float.NaN;
    private int defaultInt = Integer.MIN_VALUE;
    private long defaultLong = Long.MIN_VALUE;
    private short defaultShort = Short.MIN_VALUE;

    public JsonPathPrimitiveFunction(
            String functionName,
            int position,
            int columnType,
            Function json,
            Function path,
            DirectUtf8Sink pointer,
            boolean strict) {
        this.functionName = functionName;
        this.position = position;
        this.columnType = columnType;
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
            if (strict) {
                throw SimdJsonResult.formatError(functionName, path.getVarcharA(rec), SimdJsonError.NO_SUCH_FIELD);
            } else {
                return defaultBool;
            }
        }
        final boolean res = state.parser.queryPointerBoolean(state.initPaddedJson(jsonSeq), pointer, state.simdJsonResult, defaultBool);
        if (strict) {
            final int error = state.simdJsonResult.getError();
            if (error != SimdJsonError.SUCCESS) {
                throw SimdJsonResult.formatError(functionName, path.getVarcharA(null), error);
            }
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
        final double res = state.parser.queryPointerDouble(state.initPaddedJson(jsonSeq), pointer, state.simdJsonResult, defaultDouble);
        if (strict && !state.simdJsonResult.isNull()) {
            final int error = state.simdJsonResult.getError();
            if (error != SimdJsonError.SUCCESS) {
                throw SimdJsonResult.formatError(functionName, path.getVarcharA(null), error);
            }
        }
        return res;
    }

    @Override
    public float getFloat(Record rec) {
        final Utf8Sequence jsonSeq = json.getVarcharA(rec);
        if (jsonSeq == null) {
            return Float.NaN;
        }
        final float res = state.parser.queryPointerFloat(state.initPaddedJson(jsonSeq), pointer, state.simdJsonResult, defaultFloat);
        if (strict && !state.simdJsonResult.isNull()) {
            final int error = state.simdJsonResult.getError();
            if (error != SimdJsonError.SUCCESS) {
                throw SimdJsonResult.formatError(functionName, path.getVarcharA(null), error);
            }
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
        final int res = state.parser.queryPointerInt(state.initPaddedJson(jsonSeq), pointer, state.simdJsonResult, defaultInt);
        if (strict && !state.simdJsonResult.isNull()) {
            final int error = state.simdJsonResult.getError();
            if (error != SimdJsonError.SUCCESS) {
                throw SimdJsonResult.formatError(functionName, path.getVarcharA(null), error);
            }
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
        final long res = state.parser.queryPointerLong(state.initPaddedJson(jsonSeq), pointer, state.simdJsonResult, defaultLong);
        if (strict && !state.simdJsonResult.isNull()) {
            final int error = state.simdJsonResult.getError();
            if (error != SimdJsonError.SUCCESS) {
                throw SimdJsonResult.formatError(functionName, path.getVarcharA(null), error);
            }
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
    public String getName() {
        return functionName;
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
        final short res = state.parser.queryPointerShort(state.initPaddedJson(jsonSeq), pointer, state.simdJsonResult, defaultShort);
        if (strict && !state.simdJsonResult.isNull()) {
            final int error = state.simdJsonResult.getError();
            if (error != SimdJsonError.SUCCESS) {
                throw SimdJsonResult.formatError(functionName, path.getVarcharA(null), error);
            }
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

    @Override
    public void setDefaultBool(boolean value) {
        defaultBool = value;
    }

    @Override
    public void setDefaultDouble(double value) {
        defaultDouble = value;
    }

    @Override
    public void setDefaultFloat(float value) {
        defaultFloat = value;
    }

    @Override
    public void setDefaultInt(int value) {
        defaultInt = value;
    }

    @Override
    public void setDefaultLong(long value) {
        defaultLong = value;
    }

    @Override
    public void setDefaultShort(short value) {
        defaultShort = value;
    }

    @Override
    public void setDefaultSymbol(CharSequence value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDefaultVarchar(Utf8Sequence varcharA) {
        throw new UnsupportedOperationException();
    }
}
