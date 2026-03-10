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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
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
 * <p>
 * Uses batch native extraction: parses the JSON document once per element
 * and extracts all columns in a single JNI call, avoiding the O(n*m)
 * repeated parsing of the per-column approach.
 */
public class JsonUnnestSource implements UnnestSource, QuietCloseable {
    public static final int DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE = 4096;
    // column_desc_t layout (32 bytes, packed):
    //   [0..7]   field_name ptr (long)
    //   [8..15]  field_name_len (long)
    //   [16..19] column_type (int)
    //   [20..23] max_size (int)
    //   [24..31] sink ptr (long)
    static final int COLUMN_DESC_COLUMN_TYPE_OFFSET = 16;
    static final int COLUMN_DESC_FIELD_NAME_LEN_OFFSET = 8;
    static final int COLUMN_DESC_FIELD_NAME_OFFSET = 0;
    static final int COLUMN_DESC_MAX_SIZE_OFFSET = 20;
    static final int COLUMN_DESC_SINK_OFFSET = 24;
    static final int COLUMN_DESC_SIZE = 32;
    // column_result_t layout (24 bytes, packed):
    //   [0..7]   value (long)
    //   [8..11]  error (int)
    //   [12..15] type (int)
    //   [16..19] number_type (int)
    //   [20..23] truncated (int)
    static final int COLUMN_RESULT_ERROR_OFFSET = 8;
    static final int COLUMN_RESULT_SIZE = 24;
    static final int COLUMN_RESULT_TRUNCATED_OFFSET = 20;
    static final int COLUMN_RESULT_TYPE_OFFSET = 12;
    static final int COLUMN_RESULT_VALUE_OFFSET = 0;
    private final int columnCount;
    private final DirectUtf8Sink[] columnNameSinks;
    private final ObjList<CharSequence> columnNames;
    private final IntList columnTypes;
    private final Function function;
    private final int maxJsonValueSize;
    private final SimdJsonParser parser;
    private final DirectUtf8Sink pointerSink;
    private final SimdJsonResult result;
    // Per-column varchar sinks: [col][0]=A copy, [col][1]=B copy
    private final DirectUtf8Sink[][] varcharSinks;
    private long descsPtr;
    private int extractedElementIndex;
    private boolean isObjectArray;
    private DirectUtf8Sequence jsonSeq;
    private DirectUtf8Sink jsonSink;
    private long resultsPtr;

    public JsonUnnestSource(
            Function function,
            ObjList<CharSequence> columnNames,
            IntList columnTypes,
            int maxJsonValueSize
    ) {
        this.function = function;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.maxJsonValueSize = maxJsonValueSize;
        this.columnCount = columnTypes.size();
        this.parser = new SimdJsonParser();
        this.result = new SimdJsonResult();
        this.pointerSink = new DirectUtf8Sink(64);
        this.jsonSink = null;
        this.extractedElementIndex = -1;
        this.varcharSinks = new DirectUtf8Sink[columnCount][];
        this.columnNameSinks = new DirectUtf8Sink[columnCount];

        try {
            // Allocate varchar sinks for VARCHAR and TIMESTAMP columns.
            // Each sink must have capacity >= maxJsonValueSize to satisfy
            // the SimdJsonParser.queryPointerUtf8 assertion.
            for (int i = 0; i < columnCount; i++) {
                int type = ColumnType.tagOf(columnTypes.getQuick(i));
                if (type == ColumnType.VARCHAR || type == ColumnType.TIMESTAMP) {
                    varcharSinks[i] = new DirectUtf8Sink[]{
                            new DirectUtf8Sink(maxJsonValueSize),
                            new DirectUtf8Sink(maxJsonValueSize)
                    };
                }
            }

            // Allocate native memory for column names (stable pointers for C++ access).
            for (int i = 0; i < columnCount; i++) {
                CharSequence name = columnNames.getQuick(i);
                DirectUtf8Sink nameSink = new DirectUtf8Sink(name.length());
                nameSink.put(name);
                columnNameSinks[i] = nameSink;
            }

            // Allocate and populate native column descriptors.
            this.descsPtr = Unsafe.calloc(
                    (long) columnCount * COLUMN_DESC_SIZE, MemoryTag.NATIVE_DEFAULT
            );
            for (int i = 0; i < columnCount; i++) {
                long base = descsPtr + (long) i * COLUMN_DESC_SIZE;
                Unsafe.getUnsafe().putLong(base + COLUMN_DESC_FIELD_NAME_OFFSET, columnNameSinks[i].ptr());
                Unsafe.getUnsafe().putLong(base + COLUMN_DESC_FIELD_NAME_LEN_OFFSET, columnNameSinks[i].size());
                Unsafe.getUnsafe().putInt(base + COLUMN_DESC_COLUMN_TYPE_OFFSET, ColumnType.tagOf(columnTypes.getQuick(i)));
                Unsafe.getUnsafe().putInt(base + COLUMN_DESC_MAX_SIZE_OFFSET, maxJsonValueSize);
                long sinkPtr = 0;
                if (varcharSinks[i] != null) {
                    sinkPtr = varcharSinks[i][0].borrowDirectByteSink().ptr();
                }
                Unsafe.getUnsafe().putLong(base + COLUMN_DESC_SINK_OFFSET, sinkPtr);
            }

            // Allocate native column results.
            this.resultsPtr = Unsafe.calloc(
                    (long) columnCount * COLUMN_RESULT_SIZE, MemoryTag.NATIVE_DEFAULT
            );
        } catch (Throwable th) {
            close();
            throw th;
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
        for (int i = 0, n = columnNameSinks.length; i < n; i++) {
            Misc.free(columnNameSinks[i]);
        }
        if (descsPtr != 0) {
            descsPtr = Unsafe.free(descsPtr, (long) columnCount * COLUMN_DESC_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
        if (resultsPtr != 0) {
            resultsPtr = Unsafe.free(resultsPtr, (long) columnCount * COLUMN_RESULT_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Override
    public ArrayView getArray(
            int sourceCol,
            int elementIndex,
            int columnType
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool(int sourceCol, int elementIndex) {
        if (jsonSeq == null) {
            return false;
        }
        ensureExtracted(elementIndex);
        long resultBase = resultsPtr + (long) sourceCol * COLUMN_RESULT_SIZE;
        int error = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_ERROR_OFFSET);
        if (error != SimdJsonError.SUCCESS) {
            return false;
        }
        return Unsafe.getUnsafe().getLong(resultBase + COLUMN_RESULT_VALUE_OFFSET) != 0;
    }

    @Override
    public byte getByte(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public char getChar(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getColumnType(int sourceCol) {
        return columnTypes.getQuick(sourceCol);
    }

    @Override
    public long getDate(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int sourceCol, int elementIndex) {
        if (jsonSeq == null) {
            return Double.NaN;
        }
        ensureExtracted(elementIndex);
        long resultBase = resultsPtr + (long) sourceCol * COLUMN_RESULT_SIZE;
        int error = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_ERROR_OFFSET);
        if (error != SimdJsonError.SUCCESS) {
            return Double.NaN;
        }
        return Double.longBitsToDouble(
                Unsafe.getUnsafe().getLong(resultBase + COLUMN_RESULT_VALUE_OFFSET)
        );
    }

    @Override
    public float getFloat(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int sourceCol, int elementIndex) {
        if (jsonSeq == null) {
            return Numbers.INT_NULL;
        }
        ensureExtracted(elementIndex);
        long resultBase = resultsPtr + (long) sourceCol * COLUMN_RESULT_SIZE;
        int error = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_ERROR_OFFSET);
        if (error != SimdJsonError.SUCCESS) {
            return Numbers.INT_NULL;
        }
        return (int) Unsafe.getUnsafe().getLong(resultBase + COLUMN_RESULT_VALUE_OFFSET);
    }

    @Override
    public long getLong(int sourceCol, int elementIndex) {
        if (jsonSeq == null) {
            return Numbers.LONG_NULL;
        }
        ensureExtracted(elementIndex);
        long resultBase = resultsPtr + (long) sourceCol * COLUMN_RESULT_SIZE;
        int error = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_ERROR_OFFSET);
        if (error != SimdJsonError.SUCCESS) {
            return Numbers.LONG_NULL;
        }
        return Unsafe.getUnsafe().getLong(resultBase + COLUMN_RESULT_VALUE_OFFSET);
    }

    @Override
    public short getShort(int sourceCol, int elementIndex) {
        // 0 is the correct NULL sentinel for SHORT in QuestDB
        if (jsonSeq == null) {
            return 0;
        }
        ensureExtracted(elementIndex);
        long resultBase = resultsPtr + (long) sourceCol * COLUMN_RESULT_SIZE;
        int error = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_ERROR_OFFSET);
        if (error != SimdJsonError.SUCCESS) {
            return 0;
        }
        return (short) Unsafe.getUnsafe().getLong(resultBase + COLUMN_RESULT_VALUE_OFFSET);
    }

    @Override
    public CharSequence getStrA(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrB(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStrLen(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTimestamp(int sourceCol, int elementIndex) {
        if (jsonSeq == null) {
            return Numbers.LONG_NULL;
        }
        ensureExtracted(elementIndex);
        long resultBase = resultsPtr + (long) sourceCol * COLUMN_RESULT_SIZE;
        int error = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_ERROR_OFFSET);
        if (error != SimdJsonError.SUCCESS) {
            return Numbers.LONG_NULL;
        }
        int type = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_TYPE_OFFSET);
        if (type == SimdJsonType.STRING) {
            int truncated = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_TRUNCATED_OFFSET);
            if (truncated != 0) {
                throw overflowError(sourceCol, elementIndex);
            }
            DirectUtf8Sink sink = varcharSinks[sourceCol][0];
            try {
                return MicrosTimestampDriver.INSTANCE.parseFloorLiteral(sink);
            } catch (NumericException e) {
                return Numbers.LONG_NULL;
            }
        }
        // Numeric timestamp or null
        return Unsafe.getUnsafe().getLong(resultBase + COLUMN_RESULT_VALUE_OFFSET);
    }

    @Override
    public Utf8Sequence getVarcharA(int sourceCol, int elementIndex) {
        if (jsonSeq == null) {
            return null;
        }
        ensureExtracted(elementIndex);
        long resultBase = resultsPtr + (long) sourceCol * COLUMN_RESULT_SIZE;
        int error = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_ERROR_OFFSET);
        if (error != SimdJsonError.SUCCESS) {
            return null;
        }
        int truncated = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_TRUNCATED_OFFSET);
        if (truncated != 0) {
            throw overflowError(sourceCol, elementIndex);
        }
        return varcharSinks[sourceCol][0];
    }

    @Override
    public Utf8Sequence getVarcharB(int sourceCol, int elementIndex) {
        // Cold path: extract into B sink using per-column approach
        return getVarcharFresh(sourceCol, elementIndex, 1);
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
            return 0;
        }
        initPaddedJson(json);
        extractedElementIndex = -1;
        // Use empty pointer to get array length of top-level array
        pointerSink.clear();
        int len = parser.queryPointerArrayLength(
                jsonSeq, pointerSink, result
        );
        if (result.getError() != SimdJsonError.SUCCESS) {
            this.jsonSeq = null;
            return 0;
        }
        if (len == 0) {
            return 0;
        }
        // Detect scalar vs object array.
        // If all elements are null, we default to scalar (isObjectArray = false).
        // This is correct because both scalar (/N) and object (/N/col) paths
        // return NULL for null elements, producing identical results.
        if (columnNames.size() == 1) {
            // Single column: could be scalar or object array.
            // Scan forward to find the first non-null element and
            // check its type. Null elements are inconclusive.
            isObjectArray = false;
            for (int i = 0; i < len; i++) {
                pointerSink.clear();
                pointerSink.putAscii('/');
                pointerSink.put(i);
                parser.queryPointerBoolean(jsonSeq, pointerSink, result);
                if (result.getError() == SimdJsonError.SUCCESS
                        && result.getType() != SimdJsonType.NULL) {
                    isObjectArray =
                            result.getType() == SimdJsonType.OBJECT;
                    break;
                }
            }
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

    private void ensureExtracted(int elementIndex) {
        if (extractedElementIndex == elementIndex) {
            return;
        }
        // Clear all varchar/timestamp sinks (A copy) before batch extraction
        for (int i = 0; i < columnCount; i++) {
            if (varcharSinks[i] != null) {
                varcharSinks[i][0].clear();
            }
        }
        parser.extractArrayElement(
                jsonSeq, elementIndex, isObjectArray,
                descsPtr, resultsPtr, columnCount
        );
        extractedElementIndex = elementIndex;
    }

    private Utf8Sequence getVarcharFresh(int sourceCol, int elementIndex, int copy) {
        if (jsonSeq == null) {
            return null;
        }
        DirectUtf8Sink sink = varcharSinks[sourceCol][copy];
        sink.clear();
        buildPointer(elementIndex, sourceCol);
        parser.queryPointerUtf8(
                jsonSeq, pointerSink, result, sink, maxJsonValueSize
        );
        if (result.getError() != SimdJsonError.SUCCESS) {
            return null;
        }
        if (result.isTruncated()) {
            throw overflowError(sourceCol, elementIndex);
        }
        return sink;
    }

    private void initPaddedJson(Utf8Sequence json) {
        if (json instanceof DirectUtf8Sequence
                && json.tailPadding()
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

    private CairoException overflowError(int sourceCol, int elementIndex) {
        return CairoException.nonCritical()
                .put("JSON UNNEST: value exceeds maximum size of ")
                .put(maxJsonValueSize)
                .put(" bytes for column '")
                .put(columnNames.getQuick(sourceCol))
                .put("' at array index ")
                .put(elementIndex);
    }
}
