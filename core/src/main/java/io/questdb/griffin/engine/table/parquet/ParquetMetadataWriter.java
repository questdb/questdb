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

/**
 * Generates a {@code _pm} metadata file from an existing parquet file.
 * <p>
 * The native implementation reads the parquet footer, extracts column and
 * row group metadata, and writes a compact binary {@code _pm} file that
 * replaces the need to read the parquet footer for metadata access.
 */
public class ParquetMetadataWriter {

    /**
     * Reads parquet metadata from the file descriptor {@code parquetFd},
     * generates the {@code _pm} metadata file, and writes it to
     * {@code parquetMetaFd}.
     * <p>
     * When a row group's designated timestamp column lacks inline min/max
     * statistics, the native side memory-maps the parquet file and decodes
     * the first and last timestamp values directly to backfill the stats.
     * The backfill uses {@code allocator} for short-lived native buffers.
     *
     * @param allocator       {@code QdbAllocator} pointer
     *                        (from {@link io.questdb.std.Unsafe#getNativeAllocator(int)})
     * @param parquetFd       file descriptor of the parquet file (read-only)
     * @param parquetFileSize size of the parquet file in bytes
     * @param parquetMetaFd   file descriptor of the parquet meta file (write)
     * @return the parquet meta file size (to store in {@code _txn})
     * @throws io.questdb.cairo.CairoException on error
     */
    public static native long generate(long allocator, int parquetFd, long parquetFileSize, int parquetMetaFd);
}
