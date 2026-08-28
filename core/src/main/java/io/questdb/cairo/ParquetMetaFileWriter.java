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

    public static native void addRowGroup(long writerPtr, long numRows) throws CairoException;

    public static native void addSortingColumn(long writerPtr, int index);

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
