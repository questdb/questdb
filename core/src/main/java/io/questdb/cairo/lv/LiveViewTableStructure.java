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
import io.questdb.std.BoolList;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.Nullable;

/**
 * Adapts a live view's metadata to {@link TableStructure} so the engine can
 * materialize the on-disk WAL-backed table that backs the view's durable tier.
 * <p>
 * Live views are in line with materialized views: real
 * {@code partitionBy}, the standard {@code _meta}/{@code _txn}/partition layout,
 * and per-segment {@code wal<n>/} directories. The default partition scheme is
 * inherited from the base table at CREATE time; an explicit {@code PARTITION BY}
 * clause overrides.
 * <p>
 * A SYMBOL column the view projects straight out of the base table inherits the
 * base column's cache flag, which the caller resolves and hands to the
 * constructor. Everything else - a computed SYMBOL, or a projection whose alias
 * no longer names a base column - falls back to the server default.
 * <p>
 * The table carries dedup keys only where the caller resolved a sparse-publication
 * key; see {@link #isDedupKey(int)}.
 */
public class LiveViewTableStructure implements TableStructure {
    private final CairoConfiguration configuration;
    // The projected partition key's output column index, so this table carries
    // (designated timestamp, that column) as its dedup keys, or
    // LiveViewCheckpointOutputUniqueness.NO_KEY_COLUMN for a table that carries none.
    private final int dedupKeyColumnIndex;
    private final LiveViewDefinition definition;
    private final GenericRecordMetadata metadata;
    private final int partitionBy;
    // Per output column, the base SYMBOL column's cache flag, or the server
    // default where the column does not come from one. Parallel to metadata's
    // columns; null when the caller resolved nothing.
    private final BoolList symbolCacheFlags;
    private final String viewName;

    public LiveViewTableStructure(
            CairoConfiguration configuration,
            String viewName,
            int partitionBy,
            GenericRecordMetadata metadata,
            LiveViewDefinition definition,
            @Nullable BoolList symbolCacheFlags,
            int dedupKeyColumnIndex
    ) {
        this.configuration = configuration;
        this.viewName = viewName;
        this.partitionBy = partitionBy;
        this.metadata = metadata;
        this.definition = definition;
        this.symbolCacheFlags = symbolCacheFlags;
        this.dedupKeyColumnIndex = dedupKeyColumnIndex;
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
    public LiveViewDefinition getLiveViewDefinition() {
        return definition;
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

    /**
     * The base column's cache flag when this column projects a base SYMBOL
     * column, else the server default.
     * <p>
     * CACHE enables a native writer value-to-key map and lets each reader lazily
     * retain the values it resolves on the heap. A view over a high-cardinality
     * base column that says NOCACHE must not turn
     * those caches back on through its own output column.
     */
    @Override
    public boolean getSymbolCacheFlag(int columnIndex) {
        if (symbolCacheFlags != null && columnIndex < symbolCacheFlags.size()) {
            return symbolCacheFlags.get(columnIndex);
        }
        return configuration.getDefaultSymbolCacheFlag();
    }

    /**
     * The server default, deliberately: the base column's capacity is NOT
     * inherited.
     * <p>
     * Inheriting it looks right - it would stop the view's dictionary doubling
     * its way up from 256 - and it does shrink the view's symbol files by about
     * 14%. But it makes refresh 5 to 7 times slower. The view resolves every
     * output value against its own committed dictionary
     * ({@code LiveViewSymbolCache.intern} -> {@code SymbolMapReader.keyOf}), once
     * per row, and that probe is markedly slower against an index that was
     * pre-sized than against one of the same final capacity that grew into it.
     * The end state is identical - both reach the same capacity - so this is
     * about the path, not the size.
     * <p>
     * Note this is specific to the read-back the live view does. Pre-sizing is a
     * large win for plain ingestion, where nothing probes the dictionary per row.
     * Revisit once the per-row committed probe is gone.
     */
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

    /**
     * The designated timestamp and the projected partition key, when the caller resolved
     * one - the pair a sparse repair publication upserts on - and nothing otherwise.
     * <p>
     * A view carries them only where
     * {@code CairoConfiguration.isLiveViewCheckpointRepairSparsePublicationEnabled()} held
     * at CREATE and the view's output carries a key the pair can be named through. It is a
     * schema property from that moment on: the flags live in the table's own {@code _meta},
     * so a view created with them keeps them however the configuration moves afterwards.
     * <p>
     * The timestamp goes in because {@code TableWriter.isDeduplicationEnabled()} keys on
     * it: a table whose designated timestamp is not a dedup key does not deduplicate at
     * all, whatever else is flagged.
     */
    @Override
    public boolean isDedupKey(int columnIndex) {
        if (dedupKeyColumnIndex == LiveViewCheckpointOutputUniqueness.NO_KEY_COLUMN) {
            return false;
        }
        return columnIndex == dedupKeyColumnIndex || columnIndex == metadata.getTimestampIndex();
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
