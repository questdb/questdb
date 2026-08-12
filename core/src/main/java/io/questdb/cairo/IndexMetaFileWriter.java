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
 * format version 3. Builds an {@code _im} file in memory using the Rust
 * writer implementation, so the bytes Java produces and the bytes Rust
 * produces are the same bytes by construction.
 * <p>
 * The result is a native memory buffer holding the complete {@code _im} file
 * bytes, with {@code IM_FILE_SIZE} already patched into the header at offset
 * 0. The caller accesses the data via {@link #resultDataPtr} and
 * {@link #resultDataLen}, and must call {@link #destroyResult} when done.
 * <p>
 * Usage: {@link #create} once, {@link #setPidxFooter} once the index parquet
 * has been written, {@link #addColumn} per index column (the synthetic
 * {@code key_id} and {@code row_id} columns first, then the covered columns in
 * cover-slot order), then per index row group {@link #addRowGroup} followed by
 * an {@link #addOutOfLineStat} for each statistic too wide to inline,
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
     * Appends one index row group: its first (smallest) key id, the smallest
     * and largest row id it holds, its row count and {@code chunkCount} column
     * chunks read from {@code chunksPtr}, which must address
     * {@code chunkCount * } {@link #CHUNK_SIZE} zeroed bytes laid out with the
     * {@code CHUNK_*} offsets.
     * <p>
     * The row-id range is passed rather than derived from the {@code row_id}
     * chunk because {@code RG_ROW_ID_MIN} and {@code RG_ROW_ID_MAX} are
     * unconditional: under {@link #PAYLOAD_ROW_PER_KEY} there is no
     * {@code row_id} column to take it from. Under
     * {@link #PAYLOAD_ROW_PER_POSTING} the writer cross-checks it against that
     * chunk's statistics.
     * <p>
     * {@code chunksLen} is the buffer's own byte length and must equal
     * {@code chunkCount * } {@link #CHUNK_SIZE}. The count alone decides how
     * far the native side reads, so without the length a caller that miscounts
     * produces an out-of-bounds native read that neither side can detect; a
     * mismatch throws instead.
     */
    public static native void addRowGroup(long writerPtr, int firstKey, long rowIdMin, long rowIdMax, long numRows, long chunksPtr, long chunksLen, int chunkCount) throws CairoException;

    /**
     * Creates a writer. {@code keyIdColumn} and {@code rowIdColumn} are
     * indices into the columns added with {@link #addColumn};
     * {@code rowIdColumn} is {@code -1} under {@link #PAYLOAD_ROW_PER_KEY}.
     * Callers that do not yet know the payload kind or key space size pass any
     * non-negative value and correct it later with {@link #setPayload}.
     * <p>
     * {@code keySpaceSize} is the exclusive upper bound on key ids - the native
     * reader's {@code keyCountIncludingNulls} - and not a count of distinct
     * keys present: occupancy is sparse, and a distinct-key count would make
     * every key above the first report as absent.
     * <p>
     * {@code firstCoverColumn} is the descriptor index of cover slot 0, so
     * {@code coverSlot -> descriptorIndex = firstCoverColumn + coverSlot}. Both
     * it and {@code keySpaceSize} are rejected when negative.
     */
    public static native long create(int payloadKind, int keySpaceSize, int keyIdColumn, int rowIdColumn, int firstCoverColumn) throws CairoException;

    public static native void destroyResult(long resultPtr);

    public static native void destroyWriter(long writerPtr);

    public static native long finish(long writerPtr) throws CairoException;

    /**
     * Builds the complete {@code _im} for the covering index parquet the given
     * finished streaming parquet writer produced, and returns a result pointer
     * whose bytes are that file. The caller must release it with
     * {@link #destroyResult}.
     * <p>
     * This, not the create / {@link #addColumn} / {@link #addRowGroup} /
     * {@link #finish} surface above, is how production writes an {@code _im}:
     * the per-chunk codec, encodings, byte ranges, null counts and statistics
     * exist only inside the writer's own thrift metadata, and Java has none of
     * them. Java supplies only what it alone knows - the per-row-group first
     * key, the row-id zone maps, {@code data.parquet}'s row group boundaries
     * and the header scalars.
     * <p>
     * Valid only after {@code finishStreamingParquetWrite}: before that the
     * parquet footer has not been written, and the zero footer offset is
     * rejected rather than recorded.
     * <p>
     * {@code keySpaceSize} is the exclusive upper bound on key ids - the native
     * reader's {@code keyCountIncludingNulls} - and not a count of distinct
     * keys present. {@code count} is the number of index row groups, and each
     * of {@code firstKeysLen}, {@code rowIdMinLen} and {@code rowIdMaxLen} is
     * its buffer's own byte length. Pass the size the buffer was allocated
     * with, never {@code count} multiplied by the element width: the native
     * side can reject a length that disagrees with the count, but a consistent
     * pair of wrong values is indistinguishable from a correct one and reads
     * out of bounds. {@code dataBoundariesLen} carries the boundary count on
     * its own, so there is no second count for it to disagree with.
     * <p>
     * Every {@code _im} writer validation stays in force, including the
     * key-alignment invariant: an index whose row groups split a key across a
     * group it shares with another key is refused here rather than written and
     * discovered later. Nothing at read time can detect that violation.
     */
    public static native long generateIndexMetadata(
            long writerPtr,
            long firstKeysPtr,
            long firstKeysLen,
            long rowIdMinPtr,
            long rowIdMinLen,
            long rowIdMaxPtr,
            long rowIdMaxLen,
            long dataBoundariesPtr,
            long dataBoundariesLen,
            int count,
            int keySpaceSize,
            int keyIdColumn,
            int rowIdColumn,
            int firstCoverColumn,
            int payloadKind
    ) throws CairoException;

    public static native long resultDataLen(long resultPtr);

    public static native long resultDataPtr(long resultPtr);

    /**
     * Sets {@code data.parquet}'s cumulative row group boundaries. The array
     * has {@code DATA_RG_COUNT + 1} entries, starts at {@code 0} and is
     * non-decreasing.
     * <p>
     * {@code boundariesLen} is the buffer's own byte length and must equal
     * {@code count * } {@link Long#BYTES}. The count alone decides how far the
     * native side reads, so without the length a caller that miscounts
     * produces an out-of-bounds native read that neither side can detect; a
     * mismatch throws instead.
     */
    public static native void setDataRowGroupBoundaries(long writerPtr, long boundariesPtr, long boundariesLen, int count) throws CairoException;

    /**
     * Overwrites the payload kind and key space size passed to
     * {@link #create}. A negative {@code keySpaceSize} is rejected.
     */
    public static native void setPayload(long writerPtr, int payloadKind, int keySpaceSize) throws CairoException;

    /**
     * Records where {@code <col>.pidx.<indexTxn>.parquet}'s own parquet footer
     * starts and how long it is. The index parquet's committed size follows as
     * {@code footerOffset + footerLength + 8}, which is what lets cold-storage
     * upload and orphan validation work without an {@code ff.length()} call.
     * Both values must be non-negative, and {@link #finish} rejects a zero in
     * either.
     */
    public static native void setPidxFooter(long writerPtr, long footerOffset, int footerLength) throws CairoException;

    static {
        Os.init();
    }
}
