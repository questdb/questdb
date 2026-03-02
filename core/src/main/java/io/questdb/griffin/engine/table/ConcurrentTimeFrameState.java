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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

import java.util.concurrent.atomic.AtomicIntegerArray;

import static io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor.populatePartitionTimestamps;

/**
 * Shared state across all concurrent time frame cursors for a query.
 * Manages partition metadata and synchronized lazy partition opening.
 * <p>
 * Each partition gets its own {@link PageFrameAddressCache} instance,
 * created lazily on first open and reused across queries. Metadata
 * is stored in flat arrays for zero-GC on the hot path.
 * <p>
 * Thread safety: {@link #ensurePartitionOpened(int)} uses double-checked
 * locking with {@link AtomicIntegerArray} for acquire/release fences.
 * The hot path (already-opened partition) is a volatile read + array
 * lookups with zero allocation.
 * <p>
 * This class is created once and reused via {@link #of} / {@link #clear}.
 */
public class ConcurrentTimeFrameState implements QuietCloseable, Mutable {
    private final Object openLock = new Object();
    // Shared cumulative row counts across all partitions.
    // Indexed as: partitionPageFrameStart[p] + localPfIndex.
    // LongList uses Java heap arrays, so resize is safe for concurrent
    // readers (old arrays survive until GC).
    private final LongList pageFrameCumulativeRows = new LongList();
    // Lazily created, reused across queries. Only opened partitions
    // have non-null entries.
    private final ObjList<PageFrameAddressCache> partitionCaches = new ObjList<>();
    private final LongList partitionCeilings = new LongList();
    private final LongList partitionTimestamps = new LongList();
    private IntList columnIndexes;
    private TablePageFrameCursor frameCursor;
    private boolean isExternal;
    private RecordMetadata metadata;
    private int pageFrameCount;
    private int partitionCount;
    private AtomicIntegerArray partitionOpened;
    private int[] partitionPageFrameCount;
    private int[] partitionPageFrameStart;
    private long[] partitionTotalRows;

    @Override
    public void clear() {
        // Clear opened caches but keep the ObjList and cache objects for reuse.
        for (int i = 0, n = partitionCaches.size(); i < n; i++) {
            PageFrameAddressCache cache = partitionCaches.getQuick(i);
            if (cache != null) {
                cache.clear();
            }
        }
        pageFrameCumulativeRows.clear();
        pageFrameCount = 0;
        frameCursor = null;
    }

    @Override
    public void close() {
        Misc.freeObjList(partitionCaches);
        partitionCaches.clear();
        frameCursor = null;
    }

    /**
     * Opens the given partition lazily if not already opened.
     * Uses double-checked locking via {@link AtomicIntegerArray}:
     * the volatile read ({@code get}) provides an acquire fence,
     * and the volatile write ({@code set}) after population provides
     * a release fence, ensuring all writes to the partition's
     * {@link PageFrameAddressCache} and flat arrays are visible
     * to concurrent readers.
     *
     * @param partitionIndex the partition to open
     * @return the partition's address cache (immutable after this call)
     */
    public PageFrameAddressCache ensurePartitionOpened(int partitionIndex) {
        if (partitionOpened.get(partitionIndex) != 0) { // acquire fence
            return partitionCaches.getQuick(partitionIndex);
        }
        synchronized (openLock) {
            if (partitionOpened.get(partitionIndex) != 0) {
                return partitionCaches.getQuick(partitionIndex);
            }

            PageFrameAddressCache cache = partitionCaches.getQuick(partitionIndex);
            if (cache == null) {
                cache = new PageFrameAddressCache();
                partitionCaches.setQuick(partitionIndex, cache);
            }
            cache.of(metadata, columnIndexes, isExternal);

            frameCursor.toPartition(partitionIndex);
            int pfStart = pageFrameCount;
            long totalRows = 0;
            int pfCount = 0;
            PageFrame frame;
            while ((frame = frameCursor.next()) != null) {
                cache.add(pfCount, frame);
                long pfRows = frame.getPartitionHi() - frame.getPartitionLo();
                totalRows += pfRows;
                pageFrameCumulativeRows.add(totalRows);
                pageFrameCount++;
                pfCount++;
            }

            partitionPageFrameStart[partitionIndex] = pfStart;
            partitionPageFrameCount[partitionIndex] = pfCount;
            partitionTotalRows[partitionIndex] = totalRows;
            partitionOpened.set(partitionIndex, 1); // release fence
            return cache;
        }
    }

    public LongList getPageFrameCumulativeRows() {
        return pageFrameCumulativeRows;
    }

    public long getPartitionCeiling(int index) {
        return partitionCeilings.getQuick(index);
    }

    public LongList getPartitionCeilings() {
        return partitionCeilings;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public int getPartitionPageFrameCount(int index) {
        return partitionPageFrameCount[index];
    }

    public int getPartitionPageFrameStart(int index) {
        return partitionPageFrameStart[index];
    }

    public long getPartitionTimestamp(int index) {
        return partitionTimestamps.getQuick(index);
    }

    public LongList getPartitionTimestamps() {
        return partitionTimestamps;
    }

    public long getPartitionTotalRows(int index) {
        return partitionTotalRows[index];
    }

    public ConcurrentTimeFrameState of(
            TablePageFrameCursor frameCursor,
            RecordMetadata metadata,
            IntList columnIndexes,
            boolean isExternal
    ) {
        this.frameCursor = frameCursor;
        this.metadata = metadata;
        this.columnIndexes = columnIndexes;
        this.isExternal = isExternal;

        populatePartitionTimestamps(frameCursor, partitionTimestamps, partitionCeilings);
        this.partitionCount = partitionTimestamps.size();

        // Resize flat arrays if partition count changed; reuse if large enough.
        if (partitionPageFrameStart == null || partitionPageFrameStart.length < partitionCount) {
            partitionPageFrameStart = new int[partitionCount];
            partitionPageFrameCount = new int[partitionCount];
            partitionTotalRows = new long[partitionCount];
        }

        // AtomicIntegerArray: new instance only if size changed, else reset.
        if (partitionOpened == null || partitionOpened.length() < partitionCount) {
            partitionOpened = new AtomicIntegerArray(partitionCount);
        } else {
            for (int i = 0; i < partitionCount; i++) {
                partitionOpened.set(i, 0);
            }
        }

        // Ensure ObjList is sized (null entries for not-yet-opened partitions).
        for (int i = partitionCaches.size(); i < partitionCount; i++) {
            partitionCaches.add(null);
        }
        // Clear caches from previous query.
        for (int i = 0, n = Math.min(partitionCaches.size(), partitionCount); i < n; i++) {
            PageFrameAddressCache cache = partitionCaches.getQuick(i);
            if (cache != null) {
                cache.clear();
            }
        }

        pageFrameCumulativeRows.clear();
        pageFrameCount = 0;
        return this;
    }
}
