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

package io.questdb.cairo.sql;

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;

/**
 * Represents a contiguous fragment of a table partition.
 * <p>
 * When it comes to data access, page frames should not be used directly
 * as it's only valid for partitions in the native format. Instead,
 * a combination of {@link PageFrameAddressCache} and {@link PageFrameMemoryPool}
 * should be used.
 */
public interface PageFrame {

    /**
     * Auxiliary index page for variable-length column types, such as Varchar, String, and Binary.
     * <p>
     * Can be called only for frames in native format.
     *
     * @param columnIndex index of variable length column
     * @return contiguous memory address containing offsets for variable value entries
     */
    long getAuxPageAddress(int columnIndex);

    /**
     * Return the size of the page frame aux vector in bytes.
     * <p>
     * Can be called only for frames in native format.
     *
     * @param columnIndex index of column
     * @return size of column in bytes
     */
    long getAuxPageSize(int columnIndex);

    BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction);

    /**
     * The count of columns in the page frame. In some cases it is possible to have
     * page frame with no columns.
     *
     * @return column count
     */
    int getColumnCount();

    /**
     * Returns page frame format.
     * <p>
     * Possible values: {@link PartitionFormat#NATIVE} and {@link PartitionFormat#PARQUET}.
     */
    byte getFormat();

    /**
     * Return the address of the start of the page frame or if this page represents
     * a column top (a column that was added to the table when other columns already
     * had data) then return 0.
     * <p>
     * Can be called only for frames in native format.
     *
     * @param columnIndex index of column
     * @return address of column or 0 if column is empty
     */
    long getPageAddress(int columnIndex);

    /**
     * Return the size of the page frame data vector in bytes.
     * <p>
     * Can be called only for frames in native format.
     *
     * @param columnIndex index of column
     * @return size of column in bytes
     */
    long getPageSize(int columnIndex);

    PartitionDecoder getParquetPartitionDecoder();

    /**
     * Returns row group index corresponding to the Parquet page frame.
     * <p>
     * Possible values: {@link PartitionFormat#NATIVE} and {@link PartitionFormat#PARQUET}.
     */
    int getParquetRowGroup();

    /**
     * Returns high row index within the row group, exclusive.
     */
    int getParquetRowGroupHi();

    /**
     * Returns low row index within the row group, inclusive.
     */
    int getParquetRowGroupLo();

    /**
     * Return high row index within the frame's partition, exclusive.
     */
    long getPartitionHi();

    /**
     * Return index of the partition the frame belongs to.
     */
    int getPartitionIndex();

    /**
     * Return low row index within the frame's partition, inclusive.
     */
    long getPartitionLo();

}
