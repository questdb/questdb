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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.json.SimdJsonError;
import io.questdb.std.json.SimdJsonParser;
import io.questdb.std.json.SimdJsonResult;
import io.questdb.std.json.SimdJsonType;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;

/**
 * UnnestSource implementation for JSON arrays. Wraps a VARCHAR-producing
 * function (typically json_extract() or a string literal) and exposes
 * declared columns from each JSON array element.
 * <p>
 * For object arrays, each element's fields are accessed by name via JSON
 * Pointer (e.g., /3/price). For scalar arrays, the element itself is
 * accessed directly (e.g., /3).
 */
public class JsonUnnestSource implements UnnestSource, QuietCloseable {
    // Max size for JSON field value extraction. JSON field values are
    // typically short, so 4KB is generous. This bounds the per-column
    // DirectUtf8Sink allocation (2 sinks * 4KB per VARCHAR/TIMESTAMP col).
    private static final int MAX_JSON_VALUE_SIZE = 4096;
    private final ObjList<CharSequence> columnNames;
    private final IntList columnTypes;
    private final Function function;
    private final SimdJsonParser parser;
    private final DirectUtf8Sink pointerSink;
    private final SimdJsonResult result;
    // Per-column varchar sinks: [col][0]=A copy, [col][1]=B copy
    private final DirectUtf8Sink[][] varcharSinks;
    private int arrayLength;
    private boolean isObjectArray;
    private DirectUtf8Sequence jsonSeq;
    private DirectUtf8Sink jsonSink;

    public JsonUnnestSource(
            Function function,
            ObjList<CharSequence> columnNames,
            IntList columnTypes
    ) {
        this.function = function;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.parser = new SimdJsonParser();
        this.result = new SimdJsonResult();
        this.pointerSink = new DirectUtf8Sink(64);
        this.jsonSink = null;

        // Allocate varchar sinks for VARCHAR and TIMESTAMP columns.
        // Each sink must have capacity >= MAX_JSON_VALUE_SIZE to satisfy
        // the SimdJsonParser.queryPointerUtf8 assertion.
        int colCount = columnTypes.size();
        this.varcharSinks = new DirectUtf8Sink[colCount][];
        for (int i = 0; i < colCount; i++) {
            int type = ColumnType.tagOf(columnTypes.getQuick(i));
            if (type == ColumnType.VARCHAR || type == ColumnType.TIMESTAMP) {
                varcharSinks[i] = new DirectUtf8Sink[]{
                        new DirectUtf8Sink(MAX_JSON_VALUE_SIZE),
                        new DirectUtf8Sink(MAX_JSON_VALUE_SIZE)
                };
            }
        }
    }

    @Override
    public void close() {
        Misc.free(parser);
        Misc.free(result);
        Misc.free(pointerSink);
        Misc.free(jsonSink);
        for (int i = 0, n = varcharSinks.length; i < n; i++) {
            if (varcharSinks[i] != null) {
                Misc.free(varcharSinks[i][0]);
                Misc.free(varcharSinks[i][1]);
            }
        }
    }

    @Override
    public ArrayView getArray(
            int sourceCol,
            int elementIndex,
            int columnType
    ) {
        return null;
    }

    @Override
    public boolean getBool(int sourceCol, int elementIndex) {
        if (jsonSeq == null) {
            return false;
        }
        buildPointer(elementIndex, sourceCol);
        boolean val = parser.queryPointerBoolean(jsonSeq, pointerSink, result);
        if (result.getError() != SimdJsonError.SUCCESS) {
            return false;
        }
        return val;
    }

    @Override
    public byte getByte(int sourceCol, int elementIndex) {
        return 0;
    }

    @Override
    public char getChar(int sourceCol, int elementIndex) {
        return 0;
    }

    @Override
    public int getColumnCount() {
        return columnTypes.size();
    }

    @Override
    public int getColumnType(int sourceCol) {
        return columnTypes.getQuick(sourceCol);
    }

    @Override
    public long getDate(int sourceCol, int elementIndex) {
        return Numbers.LONG_NULL;
    }

    @Override
    public double getDouble(int sourceCol, int elementIndex) {
        if (jsonSeq == null) {
            return Double.NaN;
        }
        buildPointer(elementIndex, sourceCol);
        double val = parser.queryPointerDouble(jsonSeq, pointerSink, result);
        if (result.getError() != SimdJsonError.SUCCESS) {
            return Double.NaN;
        }
        return val;
    }

    @Override
    public float getFloat(int sourceCol, int elementIndex) {
        return Float.NaN;
    }

    @Override
    public int getInt(int sourceCol, int elementIndex) {
        if (jsonSeq == null) {
            return Numbers.INT_NULL;
        }
        buildPointer(elementIndex, sourceCol);
        int val = parser.queryPointerInt(jsonSeq, pointerSink, result);
        if (result.getError() != SimdJsonError.SUCCESS) {
            return Numbers.INT_NULL;
        }
        return val;
    }

    @Override
    public long getLong(int sourceCol, int elementIndex) {
        if (jsonSeq == null) {
            return Numbers.LONG_NULL;
        }
        buildPointer(elementIndex, sourceCol);
        long val = parser.queryPointerLong(jsonSeq, pointerSink, result);
        if (result.getError() != SimdJsonError.SUCCESS) {
            return Numbers.LONG_NULL;
        }
        return val;
    }

    @Override
    public short getShort(int sourceCol, int elementIndex) {
        if (jsonSeq == null) {
            return 0;
        }
        buildPointer(elementIndex, sourceCol);
        short val = parser.queryPointerShort(jsonSeq, pointerSink, result);
        if (result.getError() != SimdJsonError.SUCCESS) {
            return 0;
        }
        return val;
    }

    @Override
    public CharSequence getStrA(int sourceCol, int elementIndex) {
        return null;
    }

    @Override
    public CharSequence getStrB(int sourceCol, int elementIndex) {
        return null;
    }

    @Override
    public int getStrLen(int sourceCol, int elementIndex) {
        return -1;
    }

    @Override
    public long getTimestamp(int sourceCol, int elementIndex) {
        if (jsonSeq == null) {
            return Numbers.LONG_NULL;
        }
        // Extract as varchar, then parse as timestamp
        DirectUtf8Sink sink = varcharSinks[sourceCol][0];
        sink.clear();
        buildPointer(elementIndex, sourceCol);
        parser.queryPointerUtf8(
                jsonSeq, pointerSink, result, sink, MAX_JSON_VALUE_SIZE
        );
        if (result.getError() != SimdJsonError.SUCCESS
                || result.getType() != SimdJsonType.STRING) {
            // For numeric timestamps, extract as long
            if (result.getError() == SimdJsonError.SUCCESS
                    && result.getType() == SimdJsonType.NUMBER) {
                buildPointer(elementIndex, sourceCol);
                long val = parser.queryPointerLong(
                        jsonSeq, pointerSink, result
                );
                if (result.getError() != SimdJsonError.SUCCESS) {
                    return Numbers.LONG_NULL;
                }
                return val;
            }
            return Numbers.LONG_NULL;
        }
        try {
            return MicrosTimestampDriver.INSTANCE.parseFloorLiteral(sink);
        } catch (NumericException e) {
            return Numbers.LONG_NULL;
        }
    }

    @Override
    public Utf8Sequence getVarcharA(int sourceCol, int elementIndex) {
        return getVarchar(sourceCol, elementIndex, 0);
    }

    @Override
    public Utf8Sequence getVarcharB(int sourceCol, int elementIndex) {
        return getVarchar(sourceCol, elementIndex, 1);
    }

    @Override
    public int getVarcharSize(int sourceCol, int elementIndex) {
        Utf8Sequence seq = getVarcharA(sourceCol, elementIndex);
        return seq != null ? seq.size() : -1;
    }

    @Override
    public int init(Record baseRecord) {
        Utf8Sequence json = function.getVarcharA(baseRecord);
        if (json == null || json.size() == 0) {
            this.jsonSeq = null;
            this.arrayLength = 0;
            return 0;
        }
        initPaddedJson(json);
        // Use empty pointer to get array length of top-level array
        pointerSink.clear();
        int len = parser.queryPointerArrayLength(
                jsonSeq, pointerSink, result
        );
        if (result.getError() != SimdJsonError.SUCCESS) {
            this.jsonSeq = null;
            this.arrayLength = 0;
            return 0;
        }
        this.arrayLength = len;
        if (len == 0) {
            return 0;
        }
        // Detect scalar vs object array by probing element 0
        if (columnNames.size() == 1) {
            // Single column: could be scalar or object array.
            // Probe /0 to check type.
            pointerSink.clear();
            pointerSink.putAscii("/0");
            parser.queryPointerBoolean(jsonSeq, pointerSink, result);
            isObjectArray = result.getError() == SimdJsonError.SUCCESS
                    && result.getType() == SimdJsonType.OBJECT;
        } else {
            // Multiple columns: must be object array
            isObjectArray = true;
        }
        return len;
    }

    private void buildPointer(int elementIndex, int sourceCol) {
        pointerSink.clear();
        pointerSink.putAscii('/');
        pointerSink.put(elementIndex);
        if (isObjectArray) {
            pointerSink.putAscii('/');
            pointerSink.put(columnNames.getQuick(sourceCol));
        }
    }

    private Utf8Sequence getVarchar(
            int sourceCol, int elementIndex, int copy
    ) {
        if (jsonSeq == null) {
            return null;
        }
        DirectUtf8Sink sink = varcharSinks[sourceCol][copy];
        sink.clear();
        buildPointer(elementIndex, sourceCol);
        parser.queryPointerUtf8(
                jsonSeq, pointerSink, result, sink, MAX_JSON_VALUE_SIZE
        );
        if (result.getError() != SimdJsonError.SUCCESS) {
            return null;
        }
        return sink;
    }

    private void initPaddedJson(Utf8Sequence json) {
        if (json instanceof DirectUtf8Sequence
                && ((DirectUtf8Sequence) json).tailPadding()
                >= SimdJsonParser.SIMDJSON_PADDING
        ) {
            jsonSeq = (DirectUtf8Sequence) json;
        } else {
            if (jsonSink == null) {
                jsonSink = new DirectUtf8Sink(
                        json.size() + SimdJsonParser.SIMDJSON_PADDING
                );
            } else {
                jsonSink.clear();
                jsonSink.reserve(
                        json.size() + SimdJsonParser.SIMDJSON_PADDING
                );
            }
            jsonSink.put(json);
            jsonSeq = jsonSink;
        }
    }
}
