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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.map.MapValue;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Seals one partition's bounded-frame ring into persistent chunks, carrying the
 * previous boundary's chunk pages forward by reference so a cadence seal encodes
 * only the rows the batch added.
 * <p>
 * The function streams its whole live ring and does not say which rows are new.
 * This class splits the stream at the previous boundary's {@code maxTimestamp}:
 * every row at or below it was already encoded by that boundary's root, and
 * every row above it is new. The split holds because a normal seal only runs on
 * a batch strictly above the current head - the caller proves that and passes
 * {@code null} for {@code previous} when it cannot - so the survivors are
 * exactly the suffix of the previous ring that expiry left behind.
 * <p>
 * Sharing is worth having only when a chunk descriptor rides on enough rows to
 * pay for itself: two {@link LiveViewCheckpointStatePageRef}s cost 80 bytes of
 * metadata in every later root, against 16 raw bytes for the row they replace.
 * {@link #chunkCap(long)} therefore lets a partition hold one chunk per
 * {@link #MIN_SHARED_CHUNK_ROWS} live rows and rebuilds from empty rather than
 * exceed that, which leaves a small frame writing one complete image per root -
 * the cheapest thing it can do - and lets a large one share almost everything.
 */
public class LiveViewCheckpointRingSeal implements Closeable, LiveViewCheckpointRingStateSink {

    /**
     * Live chunks one partition may reference, whatever its row count. Caps the
     * partition entry's reference list, and with it the metadata a root spends
     * on one partition.
     */
    public static final int MAX_LIVE_CHUNKS = 256;
    /**
     * Rows a shared chunk must carry on average. A chunk costs 80 metadata bytes
     * in every root that references it; at this many rows that is under a tenth
     * of a byte per row against the raw 16 a row would otherwise cost again in
     * every root.
     */
    public static final int MIN_SHARED_CHUNK_ROWS = 64;

    private final LiveViewCheckpointAvgDoubleRangeStateBuilder builder;
    private long frameSize;
    private boolean hasAggregateState;
    private boolean isAppending;
    private boolean isRebuildRequired;
    private long lastSurvivorTimestamp;
    private long previousLastTimestamp;
    private long previousRowCount;
    private long rowsStreamed;
    private long splitTimestamp;
    private double sum;
    private long survivorCount;
    private LiveViewCheckpointDataSegmentWriter writer;

    public LiveViewCheckpointRingSeal(
            @NotNull CairoConfiguration configuration,
            @Nullable MemoryTracker memoryTracker
    ) {
        builder = new LiveViewCheckpointAvgDoubleRangeStateBuilder(configuration, memoryTracker);
    }

    /**
     * @return the chunks a partition holding {@code rowCount} live rows may
     * reference before the next seal rebuilds it from empty
     */
    public static int chunkCap(long rowCount) {
        final long cap = rowCount / MIN_SHARED_CHUNK_ROWS;
        return cap < 1 ? 1 : (int) Math.min(cap, MAX_LIVE_CHUNKS);
    }

    @Override
    public void close() {
        Misc.free(builder);
        writer = null;
    }

    @Override
    public void putAggregateState(double sum, long frameSize) {
        this.sum = sum;
        this.frameSize = frameSize;
        this.hasAggregateState = true;
    }

    @Override
    public void putRow(long timestamp, double value) {
        rowsStreamed++;
        if (!isAppending) {
            if (timestamp <= splitTimestamp) {
                survivorCount++;
                lastSurvivorTimestamp = timestamp;
                if (survivorCount > previousRowCount) {
                    // More rows below the previous boundary than that boundary
                    // held: the shared prefix is not a prefix. Give up on sharing
                    // rather than splice a ring that never existed.
                    isRebuildRequired = true;
                }
                return;
            }
            endSurvivors();
        }
        if (isRebuildRequired) {
            return;
        }
        builder.append(writer, timestamp, value);
    }

    /**
     * Freezes one partition's ring into {@code out}.
     *
     * @param dataWriter                  the open data segment new chunks land in
     * @param function                    the ring's owner, streamed through
     *                                    {@link WindowFunction#freezeCheckpointRingState}
     * @param value                       the partition's map value, or null for a
     *                                    scalar function
     * @param key                         the encoded partition key
     * @param previous                    the same partition's entry in the boundary
     *                                    before this one, or null when there is none
     *                                    or when the caller cannot prove this batch
     *                                    sits strictly above that boundary
     * @param previousBoundaryMaxTimestamp the boundary {@code previous} was frozen at
     * @return the logical state bytes this partition's ring accounts for, keys
     * excluded
     */
    public long seal(
            @NotNull LiveViewCheckpointDataSegmentWriter dataWriter,
            @NotNull WindowFunction function,
            @Nullable MapValue value,
            @NotNull byte[] key,
            @Nullable LiveViewCheckpointPartitionMapEntry previous,
            long previousBoundaryMaxTimestamp,
            @NotNull LiveViewCheckpointPartitionMapEntry out
    ) {
        writer = dataWriter;
        try {
            boolean isShared = false;
            if (previous != null) {
                builder.of(previous);
                isShared = builder.getChunkCount() < chunkCap(builder.getRowCount())
                        && builder.getLastTimestamp() <= previousBoundaryMaxTimestamp;
            }
            beginPartition(isShared, previousBoundaryMaxTimestamp);
            function.freezeCheckpointRingState(this, value);
            endSurvivors();
            if (isRebuildRequired) {
                // Nothing has been appended yet: every rejection above is decided
                // before the first row above the split, so the rebuild re-streams
                // the same ring into an empty builder and writes no orphan page.
                beginPartition(false, previousBoundaryMaxTimestamp);
                function.freezeCheckpointRingState(this, value);
                endSurvivors();
                if (isRebuildRequired) {
                    throw CairoException.critical(0)
                            .put("live view checkpoint ring state rebuild did not converge");
                }
            }
            if (!hasAggregateState) {
                // NaN is a legitimate stored sum, so a function that never
                // published its aggregate would otherwise seal a plausible-looking
                // partition whose continuation state is invented.
                throw CairoException.critical(0)
                        .put("live view checkpoint ring state published no aggregate state");
            }
            builder.freeze(writer, key, sum, frameSize, out);
            return LiveViewCheckpointAvgDoubleRangeStateReader.SCALAR_STATE_BYTES
                    + rowsStreamed * 2 * Long.BYTES;
        } finally {
            writer = null;
        }
    }

    private void beginPartition(boolean isShared, long previousBoundaryMaxTimestamp) {
        if (isShared) {
            previousRowCount = builder.getRowCount();
            previousLastTimestamp = builder.getLastTimestamp();
            splitTimestamp = previousBoundaryMaxTimestamp;
        } else {
            builder.ofEmpty();
            previousRowCount = 0;
            previousLastTimestamp = 0;
            splitTimestamp = Long.MIN_VALUE;
        }
        isAppending = !isShared;
        frameSize = 0;
        hasAggregateState = false;
        lastSurvivorTimestamp = 0;
        isRebuildRequired = false;
        rowsStreamed = 0;
        sum = Double.NaN;
        survivorCount = 0;
    }

    /**
     * Closes the survivor run and expires whatever the previous ring held below
     * it. The last survivor must be the previous ring's newest row, because
     * expiry only ever takes rows off the head; anything else means the stream
     * is not the previous ring plus new rows.
     */
    private void endSurvivors() {
        if (isAppending) {
            return;
        }
        isAppending = true;
        if (isRebuildRequired
                || survivorCount > previousRowCount
                || (survivorCount > 0 && lastSurvivorTimestamp != previousLastTimestamp)) {
            isRebuildRequired = true;
            return;
        }
        builder.dropHeadRows(previousRowCount - survivorCount);
    }
}
