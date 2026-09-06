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

package io.questdb.cairo;

import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.std.IntList;
import io.questdb.cairo.view.ViewDefinition;
import org.jetbrains.annotations.NotNull;

public interface TableStructure {

    int getColumnCount();

    CharSequence getColumnName(int columnIndex);

    int getColumnType(int columnIndex);

    int getIndexBlockCapacity(int columnIndex);

    default int getParquetEncodingConfig(int columnIndex) {
        return 0;
    }

    default LiveViewDefinition getLiveViewDefinition() {
        return null;
    }

    default MatViewDefinition getMatViewDefinition() {
        return null;
    }

    int getMaxUncommittedRows();

    long getO3MaxLag();

    int getPartitionBy();

    boolean getSymbolCacheFlag(int columnIndex);

    int getSymbolCapacity(int columnIndex);

    /**
     * Returns the default storage format for new partitions.
     * {@link TableUtils#TABLE_FORMAT_NATIVE} (default) or
     * {@link TableUtils#TABLE_FORMAT_PARQUET}.
     */
    default int getTableFormat() {
        return TableUtils.TABLE_FORMAT_NATIVE;
    }

    CharSequence getTableName();

    int getTimestampIndex();

    /**
     * Returns the time-to-live (TTL) of the data in this table:
     * if positive, it's in hours;
     * if negative, it's in months (and the actual value is positive);
     * zero means "no TTL".
     */
    default int getTtlHoursOrMonths() {
        return 0; // TTL disabled by default
    }

    default ViewDefinition getViewDefinition() {
        return null;
    }

    default boolean hasParquetPartitions() {
        return false;
    }

    default void init(TableToken tableToken) {
    }

    /**
     * Returns the index type for the column.
     *
     * @param columnIndex the column index
     * @return the index type (see {@link IndexType})
     */
    byte getIndexType(int columnIndex);

    default IntList getCoveringColumnIndices(int columnIndex) {
        return null;
    }

    /**
     * Returns the resolved composite-partitioning spec for this structure. Never null: implementers
     * that are not composite (the vast majority) inherit this default, which returns the shared,
     * never-mutated {@link PartitionSpec#EMPTY} whose {@link PartitionSpec#isComposite()} is false.
     * {@link TableUtils#writeMetadata} persists the additive composite block only when the returned
     * spec is composite.
     * <p>
     * NOTE: the dimension {@code columnIndex} and cluster-column indices carried by the returned
     * spec are stable WRITER indices (create-time physical column index, persisted in {@code
     * _meta} and unaffected by later {@code DROP COLUMN}s), never dense positions -- a dense-keyed
     * consumer (e.g. rendering column names off {@link CairoTable}, which is dense-keyed) must
     * translate writer index to name/position explicitly rather than indexing directly.
     */
    default PartitionSpec getPartitionSpec() {
        return PartitionSpec.EMPTY;
    }

    default boolean isCovering(int columnIndex) {
        IntList indices = getCoveringColumnIndices(columnIndex);
        return indices != null && indices.size() > 0;
    }

    boolean isDedupKey(int columnIndex);

    default boolean isIndexed(int columnIndex) {
        return IndexType.isIndexed(getIndexType(columnIndex));
    }

    default boolean isLiveView() {
        return false;
    }

    default boolean isMatView() {
        return false;
    }

    default boolean isView() {
        return false;
    }

    boolean isWalEnabled();

    default void onCreated(@NotNull CairoEngine engine, @NotNull TableToken tableToken) {
    }
}
