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
 * JNI wrapper for the Rust _im index metadata file writer.
 * Builds an _im file in memory using the Rust writer implementation.
 * <p>
 * The result is a native memory buffer holding the complete _im file bytes,
 * with IM_FILE_SIZE already patched into the header at offset 0. The caller
 * accesses the data via {@link #resultDataPtr} and {@link #resultDataLen},
 * and must call {@link #destroyResult} when done.
 */
public class IndexMetaFileWriter {

    public static native void addRowGroup(long writerPtr, int firstKey, long rowIdMin, long rowIdMax, long colRangesPtr, int colCount) throws CairoException;

    public static native long create();

    public static native void destroyResult(long resultPtr);

    public static native void destroyWriter(long writerPtr);

    public static native long finish(long writerPtr) throws CairoException;

    public static native long resultDataLen(long resultPtr);

    public static native long resultDataPtr(long resultPtr);

    public static native void setDataRowGroupBoundaries(long writerPtr, long boundariesPtr, int count);

    public static native void setPayload(long writerPtr, int payloadKind, int keyCount);

    static {
        Os.init();
    }
}
