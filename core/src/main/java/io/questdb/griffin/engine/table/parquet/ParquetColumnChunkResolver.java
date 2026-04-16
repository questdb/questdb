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
    ParquetColumnChunkResolver INSTANCE = null;

    /**
     * Release the column chunks.
     */
    void release(DirectLongList chunks, int columnsSize);

    /**
     * Resolves the required column chunks and updates columnChunks with the address and size of column chunks.
     *
     * @param partitionPath   path relative to the database dir of the partition using {@link io.questdb.cairo.TableUtils#setPathForParquetPartition}
     * @param parquetMetaAddr pointer to an mmaped _pm file
     * @param parquetMetaSize size of the _pm file
     * @param columns         [parquet_column_index, column_type] pairs
     * @param columnsSize     number of pairs in columns
     * @param rowGroupIndex   index of the row group to resolve
     * @param chunksOut       [address, size] pair list address returned by {@link DirectLongList#getAddress()} with a pre-allocated list of 2*columnsSize elements
     */
    void resolve(DirectUtf8Sequence partitionPath, long parquetMetaAddr, long parquetMetaSize, long columns, int columnsSize, int rowGroupIndex, DirectLongList chunksOut);
}
