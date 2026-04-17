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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.std.DirectLongList;
import io.questdb.std.str.DirectUtf8Sequence;

public interface ParquetColumnChunkResolver {

    /**
     * Release the column chunks previously returned by {@link #resolve}.
     * The list is owned by the decoder; the resolver must only release the
     * underlying native buffers it allocated.
     */
    void release(DirectLongList chunks, int columnsSize);

    /**
     * Fetches the requested column-chunk byte ranges into native buffers
     * and writes their addresses and sizes into {@code chunksOut}.
     * <p>
     * The decoder reads {@code _pm} to determine byte ranges and passes them
     * in via {@code byteRanges}. The resolver only needs to fetch the bytes
     * (typically from object storage) and report buffer addresses.
     * <p>
     * Each chunk buffer must contain exactly the column-chunk bytes
     * starting at {@code byteRanges[2*i]} of length {@code byteRanges[2*i+1]}
     * laid down at buffer offset 0. The output buffer's index in
     * {@code chunksOut} matches the byte range's index in {@code byteRanges}.
     *
     * @param partitionPath path relative to the database dir of the partition using {@link io.questdb.cairo.TableUtils#setPathForParquetPartition}
     * @param byteRanges    pre-filled list of {@code 2 * columnsSize} longs as {@code [byte_offset, byte_length]} pairs
     * @param columnsSize   number of byte-range / chunk-out pairs
     * @param chunksOut     pre-allocated list with {@code 2 * columnsSize} slots for {@code [address, size]} pairs
     */
    void resolve(DirectUtf8Sequence partitionPath, DirectLongList byteRanges, int columnsSize, DirectLongList chunksOut);

    /**
     * Holds the process-wide resolver reference. Null in open-source builds;
     * enterprise entry points set it at startup. Kept in a separate class
     * because interface fields are implicitly {@code static final}.
     */
    final class Holder {
        public static volatile ParquetColumnChunkResolver INSTANCE;

        private Holder() {
        }
    }
}
