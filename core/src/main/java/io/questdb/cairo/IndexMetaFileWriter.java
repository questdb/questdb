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

package io.questdb.cairo;

import io.questdb.std.Os;

/**
 * JNI wrapper for the Rust {@code _im} covering-index metadata file writer,
 * format version 2. Builds an {@code _im} file in memory using the Rust
 * writer implementation, so the bytes Java produces and the bytes Rust
 * produces are the same bytes by construction.
 * <p>
 * The result is a native memory buffer holding the complete {@code _im} file
 * bytes, with {@code IM_FILE_SIZE} already patched into the header at offset
 * 0. The caller accesses the data via {@link #resultDataPtr} and
 * {@link #resultDataLen}, and must call {@link #destroyResult} when done.
 * <p>
 * Usage: {@link #create} once, {@link #addColumn} per index column (the
 * synthetic {@code key_id} and {@code row_id} columns first, then the covered
 * columns), then per index row group {@link #addRowGroup} followed by an
 * {@link #addOutOfLineStat} for each statistic too wide to inline,
 * {@link #setDataRowGroupBoundaries}, and finally {@link #finish}.
 * <p>
 * {@link #addRowGroup} takes a buffer of {@link #CHUNK_SIZE}-byte column
 * chunks rather than one call per chunk field: the caller lays the chunks out
 * with the {@code CHUNK_*} offsets below and passes a single pointer. The
 * buffer must be zeroed first - the four reserved bytes at offset 4 of each
 * chunk must be zero.
 * <p>
 * The format specification is {@code docs/index-metadata.md}.
 */
public class IndexMetaFileWriter {

    public static final int CHUNK_BYTE_RANGE_START_OFF = 16;
    public static final int CHUNK_CODEC_OFF = 0;
    public static final int CHUNK_DISTINCT_COUNT_OFF = 40;
    public static final int CHUNK_ENCODINGS_OFF = 1;
    public static final int CHUNK_MAX_STAT_OFF = 56;
    public static final int CHUNK_MIN_STAT_OFF = 48;
    public static final int CHUNK_NULL_COUNT_OFF = 32;
    public static final int CHUNK_NUM_VALUES_OFF = 8;
    // Column chunk layout, identical to _pm's 64-byte structure.
    public static final int CHUNK_SIZE = 64;
    public static final int CHUNK_STAT_FLAGS_OFF = 2;
    public static final int CHUNK_STAT_SIZES_OFF = 3;
    public static final int CHUNK_TOTAL_COMPRESSED_OFF = 24;
    // One index row per key; there is no row_id column and ROW_ID_COLUMN is -1.
    public static final int PAYLOAD_ROW_PER_KEY = 1;
    // One index row per posting, carrying a row_id column.
    public static final int PAYLOAD_ROW_PER_POSTING = 0;

    /**
     * Appends an index column: the {@code _pm} 32-byte descriptor plus its
     * name. {@code id} carries the covered column's QuestDB writer index, or
     * {@code -1} for the synthetic {@code key_id} / {@code row_id} columns.
     * The descriptor's name offset and length are owned by the writer and
     * backpatched by {@link #finish}.
     */
    public static native void addColumn(long writerPtr, long namePtr, int nameLen, int id, int colType, int flags, int fixedByteLen, int physicalType, int maxRepLevel, int maxDefLevel) throws CairoException;

    /**
     * Patches a min or max statistic of the most recently added row group's
     * column chunk into that block's out-of-line region, for covered columns
     * whose statistics exceed the 8 inline bytes. Must be called after the
     * {@link #addRowGroup} that carries the chunk.
     */
    public static native void addOutOfLineStat(long writerPtr, int colIndex, boolean isMin, long dataPtr, int dataLen) throws CairoException;

    /**
     * Appends one index row group: its first (smallest) key id, its row count
     * and {@code chunkCount} column chunks read from {@code chunksPtr}, which
     * must address {@code chunkCount * } {@link #CHUNK_SIZE} zeroed bytes laid
     * out with the {@code CHUNK_*} offsets.
     */
    public static native void addRowGroup(long writerPtr, int firstKey, long numRows, long chunksPtr, int chunkCount) throws CairoException;

    /**
     * Creates a writer. {@code keyIdColumn} and {@code rowIdColumn} are
     * indices into the columns added with {@link #addColumn};
     * {@code rowIdColumn} is {@code -1} under {@link #PAYLOAD_ROW_PER_KEY}.
     * Callers that do not yet know the payload kind or key count pass any
     * value and correct it later with {@link #setPayload}.
     */
    public static native long create(int payloadKind, int keyCount, int keyIdColumn, int rowIdColumn);

    public static native void destroyResult(long resultPtr);

    public static native void destroyWriter(long writerPtr);

    public static native long finish(long writerPtr) throws CairoException;

    public static native long resultDataLen(long resultPtr);

    public static native long resultDataPtr(long resultPtr);

    /**
     * Sets {@code data.parquet}'s cumulative row group boundaries. The array
     * has {@code DATA_RG_COUNT + 1} entries, starts at {@code 0} and is
     * non-decreasing.
     */
    public static native void setDataRowGroupBoundaries(long writerPtr, long boundariesPtr, int count);

    /**
     * Overwrites the payload kind and key count passed to {@link #create}.
     */
    public static native void setPayload(long writerPtr, int payloadKind, int keyCount);

    static {
        Os.init();
    }
}
