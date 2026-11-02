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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.table.parquet.ParquetDecoder;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

/**
 * Owns the Java side of one prepared partition-frame window. The native cursor
 * borrows the base views, so this object closes the cursor before releasing the
 * Parquet base backing. Decode resources may point into the base views: a native
 * base stays mapped for the query, and a Parquet base's {@link ParquetBaseLease}
 * is retained by the decoded frame buffers with every resource built from it.
 */
final class PartitionFrameWindowContext implements QuietCloseable {
    private final BaseViewLease baseViewLease;
    private final PartitionFrameDecoder decoder;
    private final ProjectionPlan projectionPlan;
    private long preparedPlanRevision = -1;
    private long preparedTrackerAddress;
    private int preparedWindow = -1;
    private long state;

    PartitionFrameWindowContext(CairoConfiguration configuration) {
        BaseViewLease baseViewLease = null;
        PartitionFrameDecoder decoder = null;
        ProjectionPlan projectionPlan = null;
        try {
            decoder = configuration.newPartitionFrameDecoder();
            if (decoder == null) {
                throw new IllegalStateException("partition frame decoder factory returned null");
            }
            baseViewLease = new BaseViewLease(configuration);
            projectionPlan = new ProjectionPlan();
        } catch (Throwable th) {
            Misc.free(baseViewLease);
            Misc.free(decoder);
            Misc.free(projectionPlan);
            throw th;
        }
        this.baseViewLease = baseViewLease;
        this.decoder = decoder;
        this.projectionPlan = projectionPlan;
    }

    @Override
    public void close() {
        releaseWindow();
        Misc.free(baseViewLease);
        Misc.free(decoder);
        Misc.free(projectionPlan);
        state = 0;
    }

    private void invalidatePreparedKey() {
        preparedPlanRevision = -1;
        preparedTrackerAddress = 0;
        preparedWindow = -1;
    }

    /**
     * The Parquet base lease of the prepared window, or {@code null} for a native or
     * empty base. A decode resource built from the window points into it.
     */
    @Nullable
    ParquetBaseLease baseLease() {
        return baseViewLease.lease;
    }

    boolean bind(long state) {
        if (this.state == state) {
            return false;
        }
        releaseWindow();
        decoder.bind(state);
        this.state = state;
        return true;
    }

    PartitionFrameDecoder decoder() {
        return decoder;
    }

    boolean isActive() {
        return state != 0;
    }

    boolean isWindowPrepared() {
        return preparedWindow >= 0;
    }

    void prepareWindow(PageFrameAddressCache addressCache, int frameIndex, RowGroupBuffers targetBuffers) {
        final long frameState = addressCache.getPartitionFrameState(frameIndex);
        if (frameState == 0 || frameState != state) {
            throw new IllegalStateException("partition frame window context is not bound to the frame state");
        }
        final int window = addressCache.getParquetRowGroup(frameIndex);
        final long trackerAddress = targetBuffers.memoryTrackerAddr();
        if (preparedWindow == window
                && preparedPlanRevision == projectionPlan.revision()
                && preparedTrackerAddress == trackerAddress) {
            return;
        }

        releaseWindow();
        try {
            baseViewLease.prepare(
                    addressCache,
                    frameIndex,
                    projectionPlan.requiredColumns(),
                    targetBuffers,
                    frameState,
                    window
            );
            // Both bases outlive every decode resource built from the window: a native base
            // stays mapped for the query, and the decoded frame buffers retain a Parquet
            // base's lease with each resource, so slices may point into either.
            decoder.prepareWindow(
                    window,
                    projectionPlan.pairs(),
                    projectionPlan.primaryColumnCount(),
                    baseViewLease.viewAddress(),
                    baseViewLease.viewCount(),
                    true,
                    trackerAddress
            );
            preparedPlanRevision = projectionPlan.revision();
            preparedTrackerAddress = trackerAddress;
            preparedWindow = window;
        } catch (Throwable th) {
            releaseWindow();
            throw th;
        }
    }

    void releaseWindow() {
        decoder.releaseWindow();
        baseViewLease.clear();
        invalidatePreparedKey();
    }

    void unbind() {
        if (state != 0) {
            releaseWindow();
            state = 0;
        }
    }

    void updateProjection(
            DirectIntList primaryPairs,
            @Nullable IntHashSet primaryColumnIndexes,
            ColumnMapping columnMapping,
            IntList columnTypes
    ) {
        if (projectionPlan.prepareCandidate(primaryPairs, primaryColumnIndexes, columnMapping, columnTypes)) {
            releaseWindow();
            projectionPlan.commitCandidate(columnMapping);
        }
    }

    private static final class BaseViewLease implements QuietCloseable {
        private static final int BASE_VIEW_LONGS = 7;
        private final CairoConfiguration configuration;
        // Released leases, kept for reuse while the window context is open.
        private final ObjList<ParquetBaseLease> freeLeases = new ObjList<>();
        // Debug-only: released leases overwrite their decoded buffers.
        private final boolean isFreedBufferPoisonEnabled;
        private final DirectIntList parquetColumns;
        private final DirectLongList views;
        private boolean isClosed;
        // The prepared window's Parquet base, or null for a native or empty base.
        private ParquetBaseLease lease;
        private ParquetPartitionDecoder parquetDecoder;

        private BaseViewLease(CairoConfiguration configuration) {
            this.configuration = configuration;
            this.isFreedBufferPoisonEnabled = configuration.isDebugFreedBufferPoisonEnabled();
            DirectIntList parquetColumns = null;
            DirectLongList views = null;
            try {
                parquetColumns = new DirectIntList(32, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                views = new DirectLongList(64, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            } catch (Throwable th) {
                Misc.free(parquetColumns);
                Misc.free(views);
                throw th;
            }
            this.parquetColumns = parquetColumns;
            this.views = views;
        }

        // The pool releases every decoded frame buffer, and so every retained lease,
        // before it closes the window context, so the decoder freed here has released
        // every chunk resource it handed out.
        @Override
        public void close() {
            clear();
            isClosed = true;
            Misc.freeObjListAndClear(freeLeases);
            Misc.free(parquetColumns);
            parquetDecoder = Misc.free(parquetDecoder);
            Misc.free(views);
        }

        private ParquetBaseLease acquireLease() {
            final int freeCount = freeLeases.size();
            final ParquetBaseLease acquired;
            if (freeCount > 0) {
                acquired = freeLeases.getQuick(freeCount - 1);
                freeLeases.remove(freeCount - 1);
            } else {
                acquired = new ParquetBaseLease(this);
            }
            acquired.refCount = 1;
            return acquired;
        }

        private void addView(
                int columnId,
                int type,
                long dataAddress,
                long dataSize,
                long auxAddress,
                long auxSize,
                long top
        ) {
            views.add(columnId);
            views.add(type);
            views.add(dataAddress);
            views.add(dataSize);
            views.add(auxAddress);
            views.add(auxSize);
            views.add(top);
        }

        private void clear() {
            if (lease != null) {
                final ParquetBaseLease released = lease;
                lease = null;
                released.release();
            }
            parquetColumns.clear();
            views.clear();
        }

        private static int findParquetColumn(ParquetDecoder decoder, int columnId) {
            for (int column = 0, n = decoder.getColumnCount(); column < n; column++) {
                if (decoder.getColumnId(column) == columnId) {
                    return column;
                }
            }
            return -1;
        }

        private void prepare(
                PageFrameAddressCache addressCache,
                int frameIndex,
                DirectIntList requiredColumns,
                RowGroupBuffers targetBuffers,
                long state,
                int window
        ) {
            if (addressCache.getFrameFormat(frameIndex) == PartitionFormat.NATIVE) {
                prepareNative(addressCache, frameIndex, requiredColumns);
            } else {
                prepareParquet(addressCache, frameIndex, requiredColumns, targetBuffers, state, window);
            }
        }

        private void prepareNative(PageFrameAddressCache addressCache, int frameIndex, DirectIntList requiredColumns) {
            final ColumnMapping mapping = addressCache.getColumnMapping();
            final int columnOffset = addressCache.toColumnOffset(frameIndex);
            for (long i = 0, n = requiredColumns.size(); i < n; i += 2) {
                final int columnId = requiredColumns.get(i);
                final int type = requiredColumns.get(i + 1);
                if (columnId == mapping.getTimestampWriterIndex()) {
                    addView(
                            columnId,
                            type,
                            addressCache.getDesignatedTimestampPageAddress(frameIndex),
                            addressCache.getDesignatedTimestampPageSize(frameIndex),
                            0,
                            0,
                            addressCache.getDesignatedTimestampPageTop(frameIndex)
                    );
                    continue;
                }
                int queryColumn = -1;
                for (int q = 0, columnCount = mapping.getColumnCount(); q < columnCount; q++) {
                    if (mapping.getOriginalWriterIndex(q) == columnId) {
                        queryColumn = q;
                        break;
                    }
                }
                if (queryColumn >= 0) {
                    addView(
                            columnId,
                            type,
                            addressCache.getPageAddresses().get(columnOffset + queryColumn),
                            addressCache.getPageSizes().get(columnOffset + queryColumn),
                            addressCache.getAuxPageAddresses().get(columnOffset + queryColumn),
                            addressCache.getAuxPageSizes().get(columnOffset + queryColumn),
                            addressCache.getPageTops().get(columnOffset + queryColumn)
                    );
                }
            }
        }

        private void prepareParquet(
                PageFrameAddressCache addressCache,
                int frameIndex,
                DirectIntList requiredColumns,
                RowGroupBuffers targetBuffers,
                long state,
                int window
        ) {
            final ParquetDecoder frameDecoder = addressCache.getParquetDecoder(frameIndex);
            if (!(frameDecoder instanceof ParquetPartitionDecoder parquetFrame)) {
                throw new IllegalStateException("materialized parquet frame has no partition decoder");
            }
            if (parquetDecoder == null) {
                parquetDecoder = configuration.newParquetPartitionDecoder();
                if (parquetDecoder == null) {
                    throw new IllegalStateException("parquet partition decoder factory returned null");
                }
            }
            if (!parquetDecoder.isSamePageFrameBinding(parquetFrame)) {
                parquetDecoder.of(parquetFrame);
            }

            final long baseRows = PartitionFrameState.getBaseRowCount(state, window);
            if (baseRows == 0) {
                return;
            }
            for (long i = 0, n = requiredColumns.size(); i < n; i += 2) {
                final int columnId = requiredColumns.get(i);
                final int parquetColumn = findParquetColumn(parquetDecoder, columnId);
                if (parquetColumn >= 0) {
                    // Lend VARCHAR as slices, so base rows reach the output without a copy.
                    final int type = requiredColumns.get(i + 1);
                    parquetColumns.add(parquetColumn);
                    parquetColumns.add(ColumnType.tagOf(type) == ColumnType.VARCHAR ? ColumnType.VARCHAR_SLICE : type);
                }
            }

            final int count = (int) (parquetColumns.size() / 2);
            if (count == 0) {
                return;
            }
            lease = acquireLease();
            final RowGroupBuffers parquetBuffers = lease.buffers;
            parquetBuffers.copyMemoryTrackerFrom(targetBuffers);
            try {
                parquetBuffers.reopen();
                lease.releaser = parquetDecoder;
                parquetDecoder.decodeRowGroup(
                        parquetBuffers,
                        parquetColumns,
                        window,
                        0,
                        Math.toIntExact(baseRows)
                );
                lease.resource = parquetDecoder.takeDecodeResource();
            } catch (Throwable th) {
                lease.resource = parquetDecoder.takeDecodeResource();
                clear();
                throw th;
            }
            lease.bytes = parquetBuffers.sumChunkBytes(0, count);
            lease.slotCount = count;
            for (int slot = 0; slot < count; slot++) {
                final int columnId = parquetDecoder.getColumnId(parquetColumns.get(2L * slot));
                final int type = parquetColumns.get(2L * slot + 1);
                final long dataSize = parquetBuffers.getChunkDataSize(slot);
                final long auxSize = parquetBuffers.getChunkAuxSize(slot);
                if (dataSize != 0 || auxSize != 0 || columnId == addressCache.getColumnMapping().getTimestampWriterIndex()) {
                    addView(
                            columnId,
                            type,
                            parquetBuffers.getChunkDataPtr(slot),
                            dataSize,
                            parquetBuffers.getChunkAuxPtr(slot),
                            auxSize,
                            0
                    );
                }
            }
        }

        private void recycle(ParquetBaseLease released) {
            if (isClosed) {
                released.close();
            } else {
                freeLeases.add(released);
            }
        }

        private long viewAddress() {
            return views.size() == 0 ? 0 : views.getAddress();
        }

        private int viewCount() {
            return (int) (views.size() / BASE_VIEW_LONGS);
        }
    }

    /**
     * The decoded Parquet base of one window and the chunk resource its slices point
     * into. The window holds one reference; each decoded frame buffer that retains a
     * decode resource built from the window holds another. The last release frees the
     * buffers and the chunk resource, and returns the lease for reuse.
     */
    static final class ParquetBaseLease implements QuietCloseable {
        private static final byte FREED_BUFFER_POISON = (byte) 0xA5;
        private final RowGroupBuffers buffers;
        private final BaseViewLease owner;
        // Decoded bytes the lease keeps alive, counted by every buffer that retains it.
        private long bytes;
        private int refCount;
        private ParquetPartitionDecoder releaser;
        private long resource;
        // Decoded column slots in buffers, 0 until a decode completes.
        private int slotCount;

        private ParquetBaseLease(BaseViewLease owner) {
            this.owner = owner;
            this.buffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER, true);
        }

        @Override
        public void close() {
            Misc.free(buffers);
        }

        long bytes() {
            return bytes;
        }

        void release() {
            assert refCount > 0 : "parquet base lease released more often than retained";
            if (--refCount == 0) {
                freeContents();
                owner.recycle(this);
            }
        }

        void retain() {
            assert refCount > 0 : "parquet base lease retained after its last release";
            refCount++;
        }

        private void freeContents() {
            if (owner.isFreedBufferPoisonEnabled) {
                poisonBuffers();
            }
            buffers.close();
            if (resource != 0 && releaser != null) {
                releaser.releaseDecodeResource(resource);
            }
            resource = 0;
            releaser = null;
            bytes = 0;
            slotCount = 0;
        }

        private static void poison(long address, long size) {
            if (address != 0 && size > 0) {
                Unsafe.setMemory(address, size, FREED_BUFFER_POISON);
            }
        }

        // Overwrites the decoded data and aux bytes, which slices may point into, before they are
        // freed. Page buffers have no Java view; a debug native build poisons them on close.
        private void poisonBuffers() {
            for (int slot = 0; slot < slotCount; slot++) {
                poison(buffers.getChunkDataPtr(slot), buffers.getChunkDataSize(slot));
                poison(buffers.getChunkAuxPtr(slot), buffers.getChunkAuxSize(slot));
            }
        }
    }

    private static final class ProjectionPlan implements QuietCloseable {
        private final DirectIntList candidatePairs;
        private final DirectIntList pairs;
        private final DirectIntList requiredColumns;
        private int candidatePrimaryColumnCount;
        private int primaryColumnCount = -1;
        private long revision;

        private ProjectionPlan() {
            DirectIntList candidatePairs = null;
            DirectIntList pairs = null;
            DirectIntList requiredColumns = null;
            try {
                candidatePairs = new DirectIntList(32, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                pairs = new DirectIntList(32, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                requiredColumns = new DirectIntList(32, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            } catch (Throwable th) {
                Misc.free(candidatePairs);
                Misc.free(pairs);
                Misc.free(requiredColumns);
                throw th;
            }
            this.candidatePairs = candidatePairs;
            this.pairs = pairs;
            this.requiredColumns = requiredColumns;
        }

        @Override
        public void close() {
            Misc.free(candidatePairs);
            Misc.free(pairs);
            Misc.free(requiredColumns);
        }

        private void addRequiredColumn(int columnId, int type) {
            if (columnId < 0 || type <= 0) {
                return;
            }
            final int normalizedType = normalizeType(type);
            for (long i = 0, n = requiredColumns.size(); i < n; i += 2) {
                if (requiredColumns.get(i) == columnId) {
                    return;
                }
            }
            requiredColumns.add(columnId);
            requiredColumns.add(normalizedType);
        }

        private void buildRequiredColumns(ColumnMapping columnMapping) {
            requiredColumns.clear();
            for (long i = 0, n = pairs.size(); i < n; i += 2) {
                addRequiredColumn(pairs.get(i), pairs.get(i + 1));
            }
            addRequiredColumn(
                    columnMapping.getTimestampWriterIndex(),
                    columnMapping.getTimestampType()
            );
            for (long i = 2, n = requiredColumns.size(); i < n; i += 2) {
                final int columnId = requiredColumns.get(i);
                final int type = requiredColumns.get(i + 1);
                long j = i - 2;
                while (j >= 0 && requiredColumns.get(j) > columnId) {
                    requiredColumns.set(j + 2, requiredColumns.get(j));
                    requiredColumns.set(j + 3, requiredColumns.get(j + 1));
                    j -= 2;
                }
                requiredColumns.set(j + 2, columnId);
                requiredColumns.set(j + 3, type);
            }
        }

        private void commitCandidate(ColumnMapping columnMapping) {
            pairs.clear();
            pairs.addAll(candidatePairs);
            primaryColumnCount = candidatePrimaryColumnCount;
            buildRequiredColumns(columnMapping);
            revision++;
        }

        private boolean isCandidateCurrent() {
            if (candidatePrimaryColumnCount != primaryColumnCount || candidatePairs.size() != pairs.size()) {
                return false;
            }
            for (long i = 0, n = pairs.size(); i < n; i++) {
                if (candidatePairs.get(i) != pairs.get(i)) {
                    return false;
                }
            }
            return true;
        }

        private static int normalizeType(int type) {
            return ColumnType.tagOf(type) == ColumnType.VARCHAR_SLICE ? ColumnType.VARCHAR : type;
        }

        private DirectIntList pairs() {
            return pairs;
        }

        private boolean prepareCandidate(
                DirectIntList primaryPairs,
                @Nullable IntHashSet primaryColumnIndexes,
                ColumnMapping columnMapping,
                IntList columnTypes
        ) {
            candidatePairs.clear();
            for (long i = 0, n = primaryPairs.size(); i < n; i += 2) {
                candidatePairs.add(primaryPairs.get(i));
                candidatePairs.add(toDecodeType(primaryPairs.get(i + 1)));
            }
            candidatePrimaryColumnCount = (int) (candidatePairs.size() / 2);
            if (primaryColumnIndexes != null) {
                final long remainingStart = candidatePairs.size();
                for (int i = 0, n = columnMapping.getColumnCount(); i < n; i++) {
                    if (primaryColumnIndexes.contains(i)) {
                        continue;
                    }
                    final int columnId = columnMapping.getOriginalWriterIndex(i);
                    boolean isPresent = false;
                    for (long p = remainingStart, m = candidatePairs.size(); p < m; p += 2) {
                        if (candidatePairs.get(p) == columnId) {
                            isPresent = true;
                            break;
                        }
                    }
                    if (!isPresent) {
                        candidatePairs.add(columnId);
                        candidatePairs.add(toDecodeType(columnTypes.getQuick(i)));
                    }
                }
            }
            return !isCandidateCurrent();
        }

        private int primaryColumnCount() {
            return primaryColumnCount;
        }

        private DirectIntList requiredColumns() {
            return requiredColumns;
        }

        private long revision() {
            return revision;
        }

        // A custom frame reads every VARCHAR column as VARCHAR_SLICE entries.
        private static int toDecodeType(int type) {
            return ColumnType.tagOf(type) == ColumnType.VARCHAR ? ColumnType.VARCHAR_SLICE : type;
        }
    }
}
