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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BinarySequence;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.json.SimdJsonNumberType;
import io.questdb.std.json.SimdJsonType;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class JsonExtractFunction implements Function {
    private static final boolean defaultBool = false;
    private final TimestampDriver driver;
    private final Function json;
    private final int maxSize;
    private final Function path;
    private final @NotNull JsonExtractSupportingState stateA;
    // Only set for VARCHAR
    private final @Nullable JsonExtractSupportingState stateB;
    private final int targetType;
    private DirectUtf8Sink pointer;

    public JsonExtractFunction(
            int targetType,
            Function json,
            Function path,
            int maxSize
    ) {
        this.targetType = targetType;
        this.json = json;
        this.path = path;
        this.maxSize = maxSize;
        switch (ColumnType.tagOf(targetType)) {
            case ColumnType.IPv4:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                // cxx json code will not resize the utf8 sink, so the initial size is its max size
                stateA = new JsonExtractSupportingState(new DirectUtf8Sink(maxSize, false), false);
                stateB = null;
                break;
            case ColumnType.VARCHAR:
                stateA = new JsonExtractSupportingState(new DirectUtf8Sink(maxSize, false), true);
                stateB = new JsonExtractSupportingState(new DirectUtf8Sink(maxSize, false), true);
                break;
            default:
                stateA = new JsonExtractSupportingState(null, false);
                stateB = null;
                break;
        }
        driver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(targetType));
    }

    @Override
    public void close() {
        Misc.free(stateA);
        Misc.free(stateB);
        pointer = Misc.free(pointer);
        json.close();
        path.close();
    }

    @Override
    public void cursorClosed() {
        this.stateA.deflate();
        if (this.stateB != null) {
            this.stateB.deflate();
        }
    }

    @Override
    public ArrayView getArray(Record rec) {
        throw new UnsupportedOperationException();
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
        if (jsonSeq == null || pointer == null) {
            return defaultBool;
        }
        return stateA.parser.queryPointerBoolean(stateA.initPaddedJson(jsonSeq), pointer, stateA.simdJsonResult);
    }

    @Override
    public final byte getByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public char getChar(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDate(Record rec) {
        final Utf8Sequence jsonInput = json.getVarcharA(rec);
        if ((jsonInput == null) || (pointer == null)) {
            return Numbers.LONG_NULL;
        }

        final long res = queryPointerValue(jsonInput);
        switch (stateA.simdJsonResult.getType()) {
            case SimdJsonType.STRING:
                assert stateA.destUtf8Sink != null;
                try {
                    return DateFormatUtils.parseDate(stateA.destUtf8Sink.asAsciiCharSequence());
                } catch (NumericException e) {
                    return Numbers.LONG_NULL;
                }
            case SimdJsonType.NUMBER: {
                return extractLongFromJsonNumber(res);
            }
            default:
                return Numbers.LONG_NULL;
        }
    }

    @Override
    public double getDouble(Record rec) {
        final Utf8Sequence jsonSeq = json.getVarcharA(rec);
        if (jsonSeq == null || pointer == null) {
            return Double.NaN;
        }
        final double d = stateA.parser.queryPointerDouble(
                stateA.initPaddedJson(jsonSeq),
                pointer,
                stateA.simdJsonResult
        );
        if (stateA.simdJsonResult.getError() == 0) {
            return d;
        }
        return Double.NaN;
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
        final Utf8Sequence jsonInput = json.getVarcharA(rec);
        if ((jsonInput == null) || (pointer == null)) {
            return Numbers.IPv4_NULL;
        }
        final long res = queryPointerValue(jsonInput);
        switch (stateA.simdJsonResult.getType()) {
            case SimdJsonType.STRING:
                assert stateA.destUtf8Sink != null;
                return Numbers.parseIPv4Quiet(stateA.destUtf8Sink.asAsciiCharSequence());
            case SimdJsonType.NUMBER: {
                if (stateA.simdJsonResult.getNumberType() == SimdJsonNumberType.SIGNED_INTEGER) {
                    final int asInt = (int) res;
                    if (asInt == res) {  // precision is intact
                        return asInt;
                    }
                }
                return Numbers.IPv4_NULL;
            }
            default:
                return Numbers.IPv4_NULL;
        }
    }

    @Override
    public int getInt(Record rec) {
        final Utf8Sequence jsonSeq = json.getVarcharA(rec);
        if (jsonSeq == null || pointer == null) {
            return Numbers.INT_NULL;
        }
        return stateA.parser.queryPointerInt(
                stateA.initPaddedJson(jsonSeq),
                pointer,
                stateA.simdJsonResult
        );
    }

    @Override
    public @NotNull Interval getInterval(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(Record rec) {
        final Utf8Sequence jsonSeq = json.getVarcharA(rec);
        if (jsonSeq == null || pointer == null) {
            return Numbers.LONG_NULL;
        }
        return stateA.parser.queryPointerLong(stateA.initPaddedJson(jsonSeq), pointer, stateA.simdJsonResult);
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
        return JsonExtractSupportingState.EXTRACT_FUNCTION_NAME;
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(Record rec) {
        final Utf8Sequence jsonSeq = json.getVarcharA(rec);
        if (jsonSeq == null || pointer == null) {
            return 0;
        }
        return stateA.parser.queryPointerShort(
                stateA.initPaddedJson(jsonSeq),
                pointer,
                stateA.simdJsonResult
        );
    }

    @Override
    public CharSequence getStrA(Record rec) {
        Utf8Sequence utf8seq = getVarcharA(rec);
        if (utf8seq == null) {
            return null;
        }
        if (utf8seq.isAscii()) {
            return utf8seq.asAsciiCharSequence();
        }
        assert stateA.destUtf16Sink != null;
        stateA.destUtf16Sink.clear();
        Utf8s.utf8ToUtf16(utf8seq, stateA.destUtf16Sink);
        return stateA.destUtf16Sink;
    }

    @Override
    public CharSequence getStrB(Record rec) {
        Utf8Sequence utf8seq = getVarcharB(rec);
        if (utf8seq == null) {
            return null;
        }
        if (utf8seq.isAscii()) {
            return utf8seq.asAsciiCharSequence();
        }
        assert stateB != null;
        assert stateB.destUtf16Sink != null;
        stateB.destUtf16Sink.clear();
        Utf8s.utf8ToUtf16(utf8seq, stateB.destUtf16Sink);
        return stateB.destUtf16Sink;
    }

    @Override
    public int getStrLen(Record rec) {
        return TableUtils.lengthOf(getStrA(rec));
    }

    @Override
    public final CharSequence getSymbol(Record rec) {
        return getStrA(rec);
    }

    public final CharSequence getSymbolB(Record rec) {
        return getStrB(rec);
    }

    @Override
    public long getTimestamp(Record rec) {
        final Utf8Sequence jsonInput = json.getVarcharA(rec);
        if ((jsonInput == null) || (pointer == null)) {
            return Numbers.LONG_NULL;
        }
        final long res = queryPointerValue(jsonInput);
        switch (stateA.simdJsonResult.getType()) {
            case SimdJsonType.STRING:
                assert stateA.destUtf8Sink != null;
                try {
                    return driver.parseFloorLiteral(stateA.destUtf8Sink);
                } catch (NumericException e) {
                    return Numbers.LONG_NULL;
                }
            case SimdJsonType.NUMBER: {
                return extractLongFromJsonNumber(res);
            }
            default:
                return Numbers.LONG_NULL;
        }
    }

    @Override
    public final int getType() {
        return targetType;
    }

    @Override
    public @Nullable DirectUtf8Sink getVarcharA(Record rec) {
        return extractVarchar(json.getVarcharA(rec), stateA);
    }

    @Override
    public @Nullable DirectUtf8Sink getVarcharB(Record rec) {
        assert stateB != null;
        return extractVarchar(json.getVarcharB(rec), stateB);
    }

    @Override
    public int getVarcharSize(Record rec) {
        final Utf8Sequence utf8seq = getVarcharA(rec);
        if (utf8seq == null) {
            return TableUtils.NULL_LEN;
        }
        return utf8seq.size();
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        stateA.reopen();
        if (stateB != null) {
            stateB.reopen();
        }
        json.init(symbolTableSource, executionContext);
        path.init(symbolTableSource, executionContext);
        pointer = Misc.free(pointer);
        pointer = JsonExtractSupportingState.varcharConstantToJsonPointer(path);
    }

    @Override
    public boolean isRuntimeConstant() {
        return pointer == null;
    }

    @Override
    public boolean shouldMemoize() {
        return true;
    }

    private long extractLongFromJsonNumber(long res) {
        switch (stateA.simdJsonResult.getNumberType()) {
            case SimdJsonNumberType.SIGNED_INTEGER:
                return res;
            case SimdJsonNumberType.FLOATING_POINT_NUMBER: {
                final double d = Double.longBitsToDouble(res);
                if (d >= Long.MIN_VALUE && d <= Long.MAX_VALUE) {
                    return (long) d;
                }
                break;
            }
        }
        return Numbers.LONG_NULL;
    }

    private @Nullable DirectUtf8Sink extractVarchar(Utf8Sequence json, JsonExtractSupportingState state) {
        if (json != null && pointer != null) {
            assert state.destUtf8Sink != null;
            state.destUtf8Sink.clear();
            state.parser.queryPointerUtf8(
                    state.initPaddedJson(json),
                    pointer,
                    state.simdJsonResult,
                    state.destUtf8Sink,
                    maxSize
            );

            if (state.simdJsonResult.hasValue()) {
                return state.destUtf8Sink;
            }
        }
        return null;
    }

    private long queryPointerValue(Utf8Sequence jsonInput) {
        assert stateA.destUtf8Sink != null;
        stateA.destUtf8Sink.clear();
        return stateA.parser.queryPointerValue(
                stateA.initPaddedJson(jsonInput),
                pointer,
                stateA.simdJsonResult,
                stateA.destUtf8Sink,
                maxSize
        );
    }
}
