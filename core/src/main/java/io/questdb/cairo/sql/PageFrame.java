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

package io.questdb.cairo.sql;

import io.questdb.cairo.idx.IndexReader;
import io.questdb.griffin.engine.table.parquet.ParquetDecoder;

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

    IndexReader getIndexReader(int columnIndex, int direction);

    /**
     * Per-column runtime source tag.
     *
     * @param columnIndex index of the column
     * @return {@link DataSource#DIRECT} for a normally memory-mapped column, or
     * {@link DataSource#COVERED} for a column whose values are decoded
     * from the covering index sidecar by the worker-side decode arm.
     * Default {@link DataSource#DIRECT} keeps all existing frames unchanged.
     */
    default byte getColumnSource(int columnIndex) {
        return DataSource.DIRECT;
    }

    /**
     * The posting-index sidecar include index that covered query column
     * {@code columnIndex} decodes from, i.e. the argument passed to
     * {@code CoveringRowCursor.getCoveredXxx(includeIdx)}. Defined only for
     * columns that report {@link DataSource#COVERED}; {@code -1} otherwise
     * (the symbol key column and any non-covered column). Consumed by the
     * worker-side covered decode arm.
     */
    default int getCoveredIncludeIndex(int columnIndex) {
        return -1;
    }

    /**
     * The deduplicated set of sidecar include indices this frame's covered
     * columns decode from — passed as the required-cover-columns argument when
     * opening a covering cursor over the frame's posting reader. {@code null}
     * for non-covered frames.
     */
    default int[] getCoveredIncludeIndices() {
        return null;
    }

    /**
     * The resolved WHERE symbol key whose rows this frame's covered columns
     * belong to. Meaningful only for covered frames (frames that report at
     * least one {@link DataSource#COVERED} column); other frames return the
     * not-found sentinel. Consumed by the worker-side covered decode arm.
     */
    default int getCoveredKey() {
        return SymbolTable.VALUE_NOT_FOUND;
    }

    /**
     * Exclusive high row index, within the base partition, of the range this
     * covered frame's rows were produced from. {@code -1L} for non-covered frames.
     * <p>
     * Note: the non-covered sentinel here is {@code -1L} (a row index), which is
     * distinct from {@link SymbolTable#VALUE_NOT_FOUND} ({@code -2}) used by
     * {@link #getCoveredKey()}. Do not conflate the two.
     */
    default long getCoveredRowHi() {
        return -1;
    }

    /**
     * Inclusive low row index, within the base partition, of the range this
     * covered frame's rows were produced from. {@code -1L} for non-covered frames.
     * <p>
     * Note: the non-covered sentinel here is {@code -1L} (a row index), which is
     * distinct from {@link SymbolTable#VALUE_NOT_FOUND} ({@code -2}) used by
     * {@link #getCoveredKey()}. Do not conflate the two.
     */
    default long getCoveredRowLo() {
        return -1;
    }

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

    default ParquetDecoder getParquetDecoder() {
        return null;
    }

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
