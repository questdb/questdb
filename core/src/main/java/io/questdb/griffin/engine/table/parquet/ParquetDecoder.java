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

import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;

/**
 * Common decoding interface for parquet row group data.
 * <p>
 * {@link PageFrameMemoryPool} uses this interface to decode row groups without
 * knowing which metadata source backs the decoder. Two implementations exist:
 * <ul>
 *   <li>{@link ParquetMetaPartitionDecoder} reads metadata from the {@code _pm}
 *       sidecar file. The table reader path uses this for table partitions.</li>
 *   <li>{@link PartitionDecoder} parses the parquet footer directly. The
 *       {@code read_parquet()} SQL function uses this for external parquet
 *       files that have no {@code _pm} sidecar.</li>
 * </ul>
 * <p>
 * The interface covers only the operations needed by the memory pool: row group
 * decoding, file identity checks, and column metadata for column-ID mapping.
 *
 * @see PageFrameMemoryPool
 */
public interface ParquetDecoder {

    /**
     * Decodes the specified row range from a row group into native buffers.
     *
     * @param buffers   target buffers that receive decoded column data
     * @param columns   {@code [parquet_column_index, column_type]} pairs
     * @param rowGroup  zero-based row group index
     * @param rowLo     first row within the row group (inclusive)
     * @param rowHi     last row within the row group (exclusive)
     * @return number of rows decoded
     */
    int decodeRowGroup(
            RowGroupBuffers buffers,
            DirectIntList columns,
            int rowGroup,
            int rowLo,
            int rowHi
    );

    /**
     * Decodes a row group applying a row filter. Rows not in the filter are skipped.
     *
     * @param buffers      target buffers
     * @param columnOffset starting offset in the columns list
     * @param columns      {@code [parquet_column_index, column_type]} pairs
     * @param rowGroup     zero-based row group index
     * @param rowLo        first row within the row group (inclusive)
     * @param rowHi        last row within the row group (exclusive)
     * @param filteredRows sorted list of row indices to decode
     */
    void decodeRowGroupWithRowFilter(
            RowGroupBuffers buffers,
            int columnOffset,
            DirectIntList columns,
            int rowGroup,
            int rowLo,
            int rowHi,
            DirectLongList filteredRows
    );

    /**
     * Decodes a row group applying a row filter, filling unfiltered positions
     * with null values.
     *
     * @param buffers      target buffers
     * @param columnOffset starting offset in the columns list
     * @param columns      {@code [parquet_column_index, column_type]} pairs
     * @param rowGroup     zero-based row group index
     * @param rowLo        first row within the row group (inclusive)
     * @param rowHi        last row within the row group (exclusive)
     * @param filteredRows sorted list of row indices to decode
     */
    void decodeRowGroupWithRowFilterFillNulls(
            RowGroupBuffers buffers,
            int columnOffset,
            DirectIntList columns,
            int rowGroup,
            int rowLo,
            int rowHi,
            DirectLongList filteredRows
    );

    /**
     * Returns the base address of the parquet data file backing this decoder.
     * Used by the memory pool to detect when the underlying file changes and
     * the decoder needs to be re-initialized.
     */
    long getFileAddr();

    /**
     * Returns the size in bytes of the parquet data file backing this decoder.
     * Used together with {@link #getFileAddr()} for file identity checks.
     */
    long getFileSize();

    /**
     * Returns the number of columns described in the parquet metadata.
     * Used to build the column-ID-to-index map during {@code openParquet()}.
     */
    int getColumnCount();

    /**
     * Returns the column ID (writer index / field_id) for the given column.
     * Returns a negative value when the parquet file does not carry field IDs
     * (e.g. external files not written by QuestDB).
     *
     * @param columnIndex zero-based column index within the parquet file
     */
    int getColumnId(int columnIndex);

    /**
     * Returns {@code true} when the row group can be skipped entirely based on
     * min/max statistics and bloom filter checks against the supplied filter list.
     *
     * @param rowGroupIndex zero-based row group index
     * @param filters       encoded filter descriptors produced by
     *                      {@code ParquetRowGroupFilter.prepareFilterList()}
     * @param filterBufEnd  end pointer of the filter value buffer
     */
    boolean canSkipRowGroup(int rowGroupIndex, DirectLongList filters, long filterBufEnd);
}
