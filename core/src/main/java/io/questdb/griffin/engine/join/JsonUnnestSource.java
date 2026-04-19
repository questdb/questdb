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
import io.questdb.std.bytes.NativeByteSink;
import io.questdb.std.json.SimdJsonError;
import io.questdb.std.json.SimdJsonParser;
import io.questdb.std.json.SimdJsonResult;
import io.questdb.std.json.SimdJsonType;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import io.questdb.cairo.TableUtils;
import org.jetbrains.annotations.Nullable;

/**
 * UnnestSource implementation for JSON arrays. Wraps a VARCHAR-producing
 * function (typically json_extract() or a string literal) and exposes
 * declared columns from each JSON array element.
 * <p>
 * For object arrays, each element's fields are accessed by name via JSON
 * Pointer (e.g., /3/price). For scalar arrays, the element itself is
 * accessed directly (e.g., /3).
 * <p>
 * Uses bulk native extraction: parses the JSON document once for all
 * elements in a single JNI call, storing all results in a pre-sized
 * matrix. String data (VARCHAR/TIMESTAMP) is appended to a shared
 * buffer with packed (offset, length) references in each result.
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
    private final Function function;
    private final int maxJsonValueSize;
    private final SimdJsonParser parser;
    private final SimdJsonResult result;
    // Shared buffer for all VARCHAR/TIMESTAMP string data from bulk extraction.
    // Each column_result_t stores (offset << 32 | length) into this buffer.
    private final DirectUtf8Sink stringBuf;
    // Flyweight views for returning STRING values (UTF-16 conversion of VARCHAR data).
    private final StringSink strSinkA = new StringSink();
    private final StringSink strSinkB = new StringSink();
    // Flyweight views into stringBuf for returning VARCHAR values.
    private final DirectUtf8String varcharViewA = new DirectUtf8String();
    private final DirectUtf8String varcharViewB = new DirectUtf8String();
    private int bulkResultsCapacity;
    private long bulkResultsPtr;
    private int currentElementCount;
    private long descsPtr;
    private DirectUtf8Sequence jsonSeq;
    private DirectUtf8Sink jsonSink;

    public JsonUnnestSource(
            Function function,
            ObjList<CharSequence> columnNames,
            IntList columnTypes,
            int maxJsonValueSize
    ) {
        this.function = function;
        this.columnNames = columnNames;
        this.maxJsonValueSize = maxJsonValueSize;
        this.columnCount = columnTypes.size();
        this.jsonSink = null;
        this.columnNameSinks = new DirectUtf8Sink[columnCount];

        try {
            this.parser = new SimdJsonParser();
            this.result = new SimdJsonResult();
            this.stringBuf = new DirectUtf8Sink(maxJsonValueSize);
            // Allocate native memory for column names (stable pointers for C++ access).
            for (int i = 0; i < columnCount; i++) {
                CharSequence name = columnNames.getQuick(i);
                @SuppressWarnings("resource") DirectUtf8Sink nameSink = new DirectUtf8Sink(name.length());
                nameSink.put(name);
                columnNameSinks[i] = nameSink;
            }

            // Allocate and populate native column descriptors.
            // The sink pointer is set to 0 (null) because bulk extraction
            // uses a shared string buffer instead of per-column sinks.
            this.descsPtr = Unsafe.calloc(
                    (long) columnCount * COLUMN_DESC_SIZE, MemoryTag.NATIVE_DEFAULT
            );
            for (int i = 0; i < columnCount; i++) {
                long base = descsPtr + (long) i * COLUMN_DESC_SIZE;
                Unsafe.getUnsafe().putLong(base + COLUMN_DESC_FIELD_NAME_OFFSET, columnNameSinks[i].ptr());
                Unsafe.getUnsafe().putLong(base + COLUMN_DESC_FIELD_NAME_LEN_OFFSET, columnNameSinks[i].size());
                // Map STRING to VARCHAR (both extract UTF-8 strings) and DATE to LONG
                // (both extract epoch as a long) for C++ extraction.
                int colType = ColumnType.tagOf(columnTypes.getQuick(i));
                int nativeType = switch (colType) {
                    case ColumnType.STRING -> ColumnType.VARCHAR;
                    case ColumnType.DATE -> ColumnType.LONG;
                    default -> colType;
                };
                Unsafe.getUnsafe().putInt(base + COLUMN_DESC_COLUMN_TYPE_OFFSET, nativeType);
                Unsafe.getUnsafe().putInt(base + COLUMN_DESC_MAX_SIZE_OFFSET, maxJsonValueSize);
                Unsafe.getUnsafe().putLong(base + COLUMN_DESC_SINK_OFFSET, 0);
            }
            // Pre-allocate bulk results buffer for common small arrays.
            this.bulkResultsPtr = Unsafe.calloc(
                    16L * columnCount * COLUMN_RESULT_SIZE, MemoryTag.NATIVE_DEFAULT
            );
            this.bulkResultsCapacity = 16;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        Misc.free(parser);
        Misc.free(result);
        Misc.free(stringBuf);
        Misc.free(jsonSink);
        for (int i = 0, n = columnNameSinks.length; i < n; i++) {
            Misc.free(columnNameSinks[i]);
        }
        if (descsPtr != 0) {
            descsPtr = Unsafe.free(descsPtr, (long) columnCount * COLUMN_DESC_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
        if (bulkResultsPtr != 0) {
            bulkResultsPtr = Unsafe.free(
                    bulkResultsPtr,
                    (long) bulkResultsCapacity * columnCount * COLUMN_RESULT_SIZE,
                    MemoryTag.NATIVE_DEFAULT
            );
            bulkResultsCapacity = 0;
        }
    }

    @Override
    public boolean getBool(int sourceCol, int elementIndex) {
        if (elementIndex >= currentElementCount) {
            return false;
        }
        long resultBase = bulkResultBase(sourceCol, elementIndex);
        int error = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_ERROR_OFFSET);
        if (error != SimdJsonError.SUCCESS) {
            return false;
        }
        return Unsafe.getUnsafe().getLong(resultBase + COLUMN_RESULT_VALUE_OFFSET) != 0;
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public long getDate(int sourceCol, int elementIndex) {
        // DATE is extracted as LONG (epoch millis).
        return getLong0(sourceCol, elementIndex);
    }

    private long getLong0(int sourceCol, int elementIndex) {
        if (elementIndex >= currentElementCount) {
            return Numbers.LONG_NULL;
        }
        long resultBase = bulkResultBase(sourceCol, elementIndex);
        int error = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_ERROR_OFFSET);
        if (error != SimdJsonError.SUCCESS) {
            return Numbers.LONG_NULL;
        }
        return Unsafe.getUnsafe().getLong(resultBase + COLUMN_RESULT_VALUE_OFFSET);
    }

    @Override
    public double getDouble(int sourceCol, int elementIndex) {
        if (elementIndex >= currentElementCount) {
            return Double.NaN;
        }
        long resultBase = bulkResultBase(sourceCol, elementIndex);
        int error = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_ERROR_OFFSET);
        if (error != SimdJsonError.SUCCESS) {
            return Double.NaN;
        }
        return Double.longBitsToDouble(
                Unsafe.getUnsafe().getLong(resultBase + COLUMN_RESULT_VALUE_OFFSET)
        );
    }

    @Override
    public int getInt(int sourceCol, int elementIndex) {
        if (elementIndex >= currentElementCount) {
            return Numbers.INT_NULL;
        }
        long resultBase = bulkResultBase(sourceCol, elementIndex);
        int error = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_ERROR_OFFSET);
        if (error != SimdJsonError.SUCCESS) {
            return Numbers.INT_NULL;
        }
        return (int) Unsafe.getUnsafe().getLong(resultBase + COLUMN_RESULT_VALUE_OFFSET);
    }

    @Override
    public long getLong(int sourceCol, int elementIndex) {
        return getLong0(sourceCol, elementIndex);
    }

    @Override
    public short getShort(int sourceCol, int elementIndex) {
        // 0 is the correct NULL sentinel for SHORT in QuestDB
        if (elementIndex >= currentElementCount) {
            return 0;
        }
        long resultBase = bulkResultBase(sourceCol, elementIndex);
        int error = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_ERROR_OFFSET);
        if (error != SimdJsonError.SUCCESS) {
            return 0;
        }
        return (short) Unsafe.getUnsafe().getLong(resultBase + COLUMN_RESULT_VALUE_OFFSET);
    }

    @Override
    public CharSequence getStrA(int sourceCol, int elementIndex) {
        Utf8Sequence seq = getVarcharA(sourceCol, elementIndex);
        if (seq == null) {
            return null;
        }
        if (seq.isAscii()) {
            return seq.asAsciiCharSequence();
        }
        strSinkA.clear();
        Utf8s.utf8ToUtf16(seq, strSinkA);
        return strSinkA;
    }

    @Override
    public CharSequence getStrB(int sourceCol, int elementIndex) {
        Utf8Sequence seq = getVarcharB(sourceCol, elementIndex);
        if (seq == null) {
            return null;
        }
        if (seq.isAscii()) {
            return seq.asAsciiCharSequence();
        }
        strSinkB.clear();
        Utf8s.utf8ToUtf16(seq, strSinkB);
        return strSinkB;
    }

    @Override
    public int getStrLen(int sourceCol, int elementIndex) {
        return TableUtils.lengthOf(getStrA(sourceCol, elementIndex));
    }

    @Override
    public long getTimestamp(int sourceCol, int elementIndex) {
        if (elementIndex >= currentElementCount) {
            return Numbers.LONG_NULL;
        }
        long resultBase = bulkResultBase(sourceCol, elementIndex);
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
            long value = Unsafe.getUnsafe().getLong(resultBase + COLUMN_RESULT_VALUE_OFFSET);
            int offset = (int) (value >>> 32);
            int length = (int) (value & 0xFFFFFFFFL);
            long base = stringBuf.ptr();
            varcharViewA.of(base + offset, base + offset + length, true);
            try {
                return MicrosTimestampDriver.INSTANCE.parseFloorLiteral(varcharViewA);
            } catch (NumericException e) {
                return Numbers.LONG_NULL;
            }
        }
        // Numeric timestamp or null
        return Unsafe.getUnsafe().getLong(resultBase + COLUMN_RESULT_VALUE_OFFSET);
    }

    @Override
    public Utf8Sequence getVarcharA(int sourceCol, int elementIndex) {
        return getUtf8Sequence(sourceCol, elementIndex, varcharViewA);
    }

    @Nullable
    private Utf8Sequence getUtf8Sequence(int sourceCol, int elementIndex, DirectUtf8String varcharView) {
        if (elementIndex >= currentElementCount) {
            return null;
        }
        long resultBase = bulkResultBase(sourceCol, elementIndex);
        int error = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_ERROR_OFFSET);
        if (error != SimdJsonError.SUCCESS) {
            return null;
        }
        int type = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_TYPE_OFFSET);
        if (type == SimdJsonType.NULL) {
            return null;
        }
        int truncated = Unsafe.getUnsafe().getInt(resultBase + COLUMN_RESULT_TRUNCATED_OFFSET);
        if (truncated != 0) {
            throw overflowError(sourceCol, elementIndex);
        }
        long value = Unsafe.getUnsafe().getLong(resultBase + COLUMN_RESULT_VALUE_OFFSET);
        int offset = (int) (value >>> 32);
        int length = (int) (value & 0xFFFFFFFFL);
        long base = stringBuf.ptr();
        varcharView.of(base + offset, base + offset + length, jsonSeq.isAscii());
        return varcharView;
    }

    @Override
    public Utf8Sequence getVarcharB(int sourceCol, int elementIndex) {
        return getUtf8Sequence(sourceCol, elementIndex, varcharViewB);
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
            this.currentElementCount = 0;
            return 0;
        }
        initPaddedJson(json);

        // Combined call: counts elements, determines object vs scalar type,
        // and extracts all elements in a single parse when the buffer is
        // large enough. Returns negative count if buffer needs to grow.
        stringBuf.clear();
        NativeByteSink nativeSink = stringBuf.borrowDirectByteSink();
        try {
            int len = parser.queryAndExtractArray(
                    jsonSeq,
                    result,
                    descsPtr,
                    bulkResultsPtr,
                    columnCount,
                    bulkResultsCapacity,
                    nativeSink.ptr()
            );
            if (result.getError() != SimdJsonError.SUCCESS) {
                this.jsonSeq = null;
                this.currentElementCount = 0;
                return 0;
            }
            if (len == 0) {
                this.jsonSeq = null;
                this.currentElementCount = 0;
                return 0;
            }
            if (len < 0) {
                // Buffer too small — grow and fall back to extraction-only
                // call (second parse) since we can't reuse the borrow.
                len = -len;
                Unsafe.free(
                        bulkResultsPtr,
                        (long) bulkResultsCapacity * columnCount * COLUMN_RESULT_SIZE,
                        MemoryTag.NATIVE_DEFAULT
                );
                bulkResultsPtr = 0;
                bulkResultsCapacity = 0;
                bulkResultsPtr = Unsafe.calloc(
                        (long) len * columnCount * COLUMN_RESULT_SIZE,
                        MemoryTag.NATIVE_DEFAULT
                );
                bulkResultsCapacity = len;
                boolean isObjectArray = columnCount > 1
                        || result.getType() == SimdJsonType.OBJECT;
                nativeSink.close();
                stringBuf.clear();
                nativeSink = stringBuf.borrowDirectByteSink();
                parser.extractAllArrayElements(
                        jsonSeq, isObjectArray, descsPtr, bulkResultsPtr,
                        columnCount, len, nativeSink.ptr()
                );
            }
            this.currentElementCount = len;
            return len;
        } finally {
            nativeSink.close();
        }
    }

    private long bulkResultBase(int sourceCol, int elementIndex) {
        return bulkResultsPtr + ((long) elementIndex * columnCount + sourceCol) * COLUMN_RESULT_SIZE;
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
