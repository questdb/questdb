/*+*****************************************************************************
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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.cairo.CairoException;
import io.questdb.std.Os;

/**
 * Writes a parquet partition and its {@code _pm} a frame at a time, in {@link PartitionEncoder} streaming layouts.
 * Column names, the {@code _pm} fd and every frame's memory must stay valid until {@link #destroy}.
 */
public class PartitionFrameEncoder {

    public static native long create(
            int columnCount,
            long columnNamesPtr,
            int columnNamesSize,
            long columnMetadataPtr,
            int timestampIndex,
            long destPathPtr,
            int destPathLength,
            long compressionCodec,
            boolean statisticsEnabled,
            boolean rawArrayEncoding,
            long rowGroupSize,
            long dataPageSize,
            int version,
            long bloomFilterColumnIndexesPtr,
            int bloomFilterColumnCount,
            double bloomFilterFpp,
            double minCompressionRatio,
            int parquetMetaFd,
            long squashTracker,
            long seqTxn
    ) throws CairoException;

    public static native void destroy(long writerPtr);

    /**
     * Writes the last row group, the footer and the {@code _pm} sidecar; returns the parquet file size.
     */
    public static native long finish(long writerPtr, boolean sync) throws CairoException;

    /**
     * Returns the rows written to row groups so far, counted from the first frame.
     */
    public static native long writeFrame(long writerPtr, long columnDataPtr, long rowCount) throws CairoException;

    static {
        Os.init();
    }
}
