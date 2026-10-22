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

import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.std.DirectLongList;
import org.jetbrains.annotations.TestOnly;

/**
 * Filter-pushdown contract for row group pruning. Implementations decide
 * whether a row group can be skipped based on min/max statistics and bloom
 * filter checks against an encoded filter list.
 * <p>
 * Two implementations exist:
 * <ul>
 *   <li>{@link io.questdb.cairo.ParquetMetaFileReader} reads stats and bloom
 *       filter bitsets from the {@code _pm} sidecar file. Used by the table
 *       reader path.</li>
 *   <li>{@link ParquetFileDecoder} parses the parquet footer directly and reads
 *       bloom filters from the parquet file. Used by the {@code read_parquet()}
 *       SQL function for external parquet files without a {@code _pm}
 *       sidecar.</li>
 * </ul>
 *
 * @see io.questdb.griffin.engine.table.ParquetRowGroupFilter#canSkipRowGroup
 */
public interface ParquetRowGroupSkipper {

    /**
     * Returns {@code true} when the row group can be skipped entirely based on
     * min/max statistics and bloom filter checks against the supplied filter
     * list.
     *
     * @param rowGroupIndex zero-based row group index
     * @param filters       encoded filter descriptors, {@link ParquetRowGroupFilter#LONGS_PER_FILTER} longs each
     * @param filterBufEnd  exclusive end address of the filter values buffer,
     *                      used for native bounds checking
     */
    @TestOnly
    default boolean canSkipRowGroup(int rowGroupIndex, DirectLongList filters, long filterBufEnd) {
        assert filters.size() % ParquetRowGroupFilter.LONGS_PER_FILTER == 0;
        return canSkipRowGroup(rowGroupIndex, filters.getAddress(), (int) (filters.size() / ParquetRowGroupFilter.LONGS_PER_FILTER), filterBufEnd);
    }

    /**
     * Same as {@link #canSkipRowGroup(int, DirectLongList, long)} over the {@code filterCount} descriptors at
     * {@code filtersAddr}.
     */
    boolean canSkipRowGroup(int rowGroupIndex, long filtersAddr, int filterCount, long filterBufEnd);

    /**
     * Returns {@code true} when the statistics prove the column chunk holds no SQL NULL: no Parquet null, and no
     * stored value that reads back as the NULL sentinel of the column type.
     */
    boolean hasNoNulls(int rowGroupIndex, int columnIndex);
}
