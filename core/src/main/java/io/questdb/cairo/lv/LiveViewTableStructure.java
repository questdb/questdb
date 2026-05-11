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

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableStructure;
import io.questdb.std.Numbers;

/**
 * Adapts a live view's metadata to {@link TableStructure} so the engine can
 * materialize the on-disk WAL-backed table that backs the view's durable tier.
 * <p>
 * Phase 1 brings live views in line with materialized views: real
 * {@code partitionBy}, the standard {@code _meta}/{@code _txn}/partition layout,
 * and per-segment {@code wal<n>/} directories. The default partition scheme is
 * inherited from the base table at CREATE time; an explicit {@code PARTITION BY}
 * clause overrides.
 */
public class LiveViewTableStructure implements TableStructure {
    private final CairoConfiguration configuration;
    private final GenericRecordMetadata metadata;
    private final int partitionBy;
    private final String viewName;

    public LiveViewTableStructure(
            CairoConfiguration configuration,
            String viewName,
            int partitionBy,
            GenericRecordMetadata metadata
    ) {
        this.configuration = configuration;
        this.viewName = viewName;
        this.partitionBy = partitionBy;
        this.metadata = metadata;
    }

    @Override
    public int getColumnCount() {
        return metadata.getColumnCount();
    }

    @Override
    public CharSequence getColumnName(int columnIndex) {
        return metadata.getColumnName(columnIndex);
    }

    @Override
    public int getColumnType(int columnIndex) {
        return metadata.getColumnType(columnIndex);
    }

    @Override
    public int getIndexBlockCapacity(int columnIndex) {
        return 0;
    }

    @Override
    public byte getIndexType(int columnIndex) {
        return IndexType.NONE;
    }

    @Override
    public int getMaxUncommittedRows() {
        return 0;
    }

    @Override
    public long getO3MaxLag() {
        return 0;
    }

    @Override
    public int getPartitionBy() {
        return partitionBy;
    }

    @Override
    public boolean getSymbolCacheFlag(int columnIndex) {
        return configuration.getDefaultSymbolCacheFlag();
    }

    @Override
    public int getSymbolCapacity(int columnIndex) {
        return configuration.getDefaultSymbolCapacity();
    }

    @Override
    public CharSequence getTableName() {
        return viewName;
    }

    @Override
    public int getTimestampIndex() {
        return metadata.getTimestampIndex();
    }

    @Override
    public boolean isDedupKey(int columnIndex) {
        return false;
    }

    @Override
    public boolean isIndexed(int columnIndex) {
        return false;
    }

    @Override
    public boolean isLiveView() {
        return true;
    }

    @Override
    public boolean isWalEnabled() {
        return true;
    }

    /**
     * Resolves an LV's partition scheme at CREATE. {@code explicit} comes from
     * the parser via {@code CreateLiveViewOperation.getPartitionBy()} and is
     * {@code Numbers.INT_NULL} when the user omitted the {@code PARTITION BY}
     * clause; in that case we inherit {@code baseTablePartitionBy}. Any other
     * value (including {@code PartitionBy.NONE} for explicit "no partitioning")
     * is honoured as-is.
     */
    public static int resolvePartitionBy(int explicit, int baseTablePartitionBy) {
        if (explicit == Numbers.INT_NULL) {
            return baseTablePartitionBy;
        }
        return explicit;
    }
}
