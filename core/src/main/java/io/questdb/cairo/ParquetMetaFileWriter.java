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
 * JNI wrapper for the Rust _pm metadata file writer.
 * Builds a _pm file in memory using the real Rust writer implementation.
 * <p>
 * The result is a native memory buffer holding the complete _pm file bytes.
 * The caller accesses the data via {@link #resultDataPtr}, {@link #resultDataLen},
 * and {@link #resultParquetMetaFileSize}, and must call {@link #destroyResult} when done.
 * {@link #resultParquetMetaFileSize} returns the total committed file size that the
 * Rust writer has already patched into the header at offset 0.
 */
public class ParquetMetaFileWriter {

    public static native void addBloomFilter(long writerPtr, int colIndex, long bitsetPtr, int bitsetLen);

    public static native void addColumn(long writerPtr, long namePtr, int nameLen, int id, int colType, int flags, int fixedByteLen, int physicalType, int maxRepLevel, int maxDefLevel);

    public static native void addCoveringIndex(long writerPtr, int columnId, long indexTxn, long imFileSize);

    public static native void addRowGroup(long writerPtr, long numRows) throws CairoException;

    public static native void addSortingColumn(long writerPtr, int index);

    /**
     * Builds an append-only {@code _pm} snapshot that restates the
     * covering-index section and changes nothing else: same row group offsets,
     * same parquet footer, same {@code unused_bytes}, and the prior footer's
     * {@code seqTxn} explicitly inherited. This is how a seal publishes its
     * index token without rewriting {@code data.parquet}.
     * <p>
     * {@code existingAddr} must address {@code appendBase} bytes of the
     * {@code _pm}. {@code parseAnchor} is the committed {@code _pm} size the
     * current {@code data.parquet} size resolves to, and {@code appendBase} is
     * the {@code _pm} header at offset 0; the two differ only inside the crash
     * window a rolled-back update leaves behind.
     * <p>
     * {@code entriesAddr} addresses {@code entryCount} entries of three longs
     * each: {@code column_id}, {@code index_txn} and {@code im_file_size}. That
     * is the complete set, not a delta; zero entries drops the section.
     * <p>
     * The result's {@link #resultDataPtr} bytes go at {@code appendBase}, not at
     * offset 0, and {@link #resultParquetMetaFileSize} is what the caller
     * patches into the header as the last write of the sequence.
     */
    public static native long buildCoveringIndexAppend(long existingAddr, long parseAnchor, long appendBase, long entriesAddr, int entryCount) throws CairoException;

    public static native long create();

    public static native void destroyResult(long resultPtr);

    public static native void destroyWriter(long writerPtr);

    public static native long finish(long writerPtr) throws CairoException;

    public static native long resultDataLen(long resultPtr);

    public static native long resultDataPtr(long resultPtr);

    public static native long resultParquetMetaFileSize(long resultPtr);

    public static native void setDesignatedTimestamp(long writerPtr, int index);

    public static native void setParquetFooter(long writerPtr, long offset, int length);

    static {
        Os.init();
    }
}
