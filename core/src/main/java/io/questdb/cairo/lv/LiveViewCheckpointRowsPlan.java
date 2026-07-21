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

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.std.IntList;
import org.jetbrains.annotations.NotNull;

/**
 * Immutable compiler-owned union of the finite ROWS dependencies in one live view.
 * The ROWS counterpart of {@link LiveViewCheckpointRangePlan}, and present only when
 * every window function belongs to the same partition/order domain and has a finite
 * {@code ROWS N PRECEDING ... CURRENT ROW} dependency.
 * <p>
 * The union takes the widest look-behind of any function in the view, because the
 * dependency floor has to satisfy every one of them at once. Where the two plans part
 * company is what that width means. A RANGE width is a timestamp offset, so both
 * repair bounds follow from arithmetic and never touch the data. {@code Nmax} is a
 * per-key <b>row</b> count, so neither bound has a closed form: how far back
 * {@code Nmax} rows of one key sit, and how far forward the change reaches, depend on
 * where that key's rows actually are. {@link LiveViewCheckpointRowsBounds} discovers
 * them by counting rows per key over the bounded page-frame scans.
 * <p>
 * That is why this plan carries a key projector - the partition-by column list, its
 * types and a {@link RecordSink} over them - and the range plan does not. The
 * projector is built at compile time against the base factory's metadata, so the
 * discovery scan reads keys out of a page-frame record with no codegen of its own and
 * no dependency on the live window functions' own partition maps.
 * <p>
 * Every plan is keyed. A keyless ROWS frame compiles to a scalar window function that
 * carries no checkpoint state at all, so no live view can hold one - and a discovery
 * with nothing to count per key would have to count over the whole cursor, which is a
 * different contract rather than a degenerate case of this one.
 */
public final class LiveViewCheckpointRowsPlan {
    private final int functionCount;
    private final ColumnTypes keyColumnTypes;
    private final RecordSink keySink;
    private final long maxPrecedingRows;
    private final String orderSignature;
    private final IntList partitionByColumnIndexes;
    private final String partitionSignature;
    private final int timestampIndex;
    private final int timestampType;

    public LiveViewCheckpointRowsPlan(
            int functionCount,
            long maxPrecedingRows,
            @NotNull CharSequence partitionSignature,
            @NotNull CharSequence orderSignature,
            @NotNull IntList partitionByColumnIndexes,
            @NotNull ColumnTypes keyColumnTypes,
            @NotNull RecordSink keySink,
            int timestampIndex,
            int timestampType
    ) {
        // Nmax below 1 is a frame with no look-behind, and an empty key list is a
        // window with no PARTITION BY. Neither has a checkpoint-capable window function
        // behind it today, and both would put the discovery on a path it cannot count
        // over, so they are refused here rather than half-supported at the scan.
        if (functionCount < 1 || maxPrecedingRows < 1 || timestampIndex < 0 || partitionByColumnIndexes.size() < 1) {
            throw new IllegalArgumentException("invalid ROWS dependency plan");
        }
        this.functionCount = functionCount;
        this.maxPrecedingRows = maxPrecedingRows;
        this.partitionSignature = partitionSignature.toString();
        this.orderSignature = orderSignature.toString();
        this.partitionByColumnIndexes = new IntList(partitionByColumnIndexes.size());
        this.partitionByColumnIndexes.addAll(partitionByColumnIndexes);
        this.keyColumnTypes = keyColumnTypes;
        this.keySink = keySink;
        this.timestampIndex = timestampIndex;
        this.timestampType = timestampType;
    }

    public int getFunctionCount() {
        return functionCount;
    }

    /** Returns the map key shape the {@link #getKeySink() projector} writes. */
    public @NotNull ColumnTypes getKeyColumnTypes() {
        return keyColumnTypes;
    }

    /**
     * Returns the projector that writes one base record's partition key into a map key.
     * It reads the base factory's own column indexes, so it must only be handed records
     * from that factory's cursors.
     * <p>
     * A SYMBOL key column is written as its table-local integer, not as a string. Those
     * integers are stable for the lifetime of one reader, and a repair plans and replays
     * against one pinned reader, so the key identity holds for exactly as long as it is
     * used.
     */
    public @NotNull RecordSink getKeySink() {
        return keySink;
    }

    /**
     * Returns the widest finite look-behind {@code Nmax} across the view's ROWS
     * functions, in rows of one partition key. A dependency floor that satisfies this
     * count satisfies every function in the view.
     */
    public long getMaxPrecedingRows() {
        return maxPrecedingRows;
    }

    public String getOrderSignature() {
        return orderSignature;
    }

    public int getPartitionByColumnCount() {
        return partitionByColumnIndexes.size();
    }

    /** Returns the base-factory column index of the {@code n}-th PARTITION BY column. */
    public int getPartitionByColumnIndex(int n) {
        return partitionByColumnIndexes.getQuick(n);
    }

    public String getPartitionSignature() {
        return partitionSignature;
    }

    /** Returns the designated timestamp's column index in the base factory's metadata. */
    public int getTimestampIndex() {
        return timestampIndex;
    }

    public int getTimestampType() {
        return timestampType;
    }
}
