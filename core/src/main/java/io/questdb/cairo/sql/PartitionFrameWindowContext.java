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
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Owns the Java side of one prepared partition-frame window. The native cursor
 * borrows the base views, so this object closes the cursor before releasing the
 * Parquet base backing.
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

    boolean bind(long state) {
        if (this.state == state) {
            return false;
        }
        releaseWindow();
        decoder.bind(state);
        this.state = state;
        return true;
    }

    @Override
    public void close() {
        releaseWindow();
        Misc.free(baseViewLease);
        Misc.free(decoder);
        Misc.free(projectionPlan);
        state = 0;
    }

    PartitionFrameDecoder decoder() {
        return decoder;
    }

    boolean isActive() {
        return state != 0;
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
            decoder.prepareWindow(
                    window,
                    projectionPlan.pairs(),
                    projectionPlan.primaryColumnCount(),
                    baseViewLease.viewAddress(),
                    baseViewLease.viewCount(),
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

    private void invalidatePreparedKey() {
        preparedPlanRevision = -1;
        preparedTrackerAddress = 0;
        preparedWindow = -1;
    }

    private static final class BaseViewLease implements QuietCloseable {
        private static final int BASE_VIEW_LONGS = 7;
        private final CairoConfiguration configuration;
        private final RowGroupBuffers parquetBuffers;
        private final DirectIntList parquetColumns;
        private final DirectLongList views;
        private ParquetPartitionDecoder parquetDecoder;
        private long parquetResource;

        private BaseViewLease(CairoConfiguration configuration) {
            this.configuration = configuration;
            RowGroupBuffers parquetBuffers = null;
            DirectIntList parquetColumns = null;
            DirectLongList views = null;
            try {
                parquetBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER, true);
                parquetColumns = new DirectIntList(32, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                views = new DirectLongList(64, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            } catch (Throwable th) {
                Misc.free(parquetBuffers);
                Misc.free(parquetColumns);
                Misc.free(views);
                throw th;
            }
            this.parquetBuffers = parquetBuffers;
            this.parquetColumns = parquetColumns;
            this.views = views;
        }

        @Override
        public void close() {
            clear();
            Misc.free(parquetBuffers);
            Misc.free(parquetColumns);
            parquetDecoder = Misc.free(parquetDecoder);
            Misc.free(views);
        }

        private void addView(
                int writer,
                int type,
                long dataAddress,
                long dataSize,
                long auxAddress,
                long auxSize,
                long top
        ) {
            views.add(writer);
            views.add(type);
            views.add(dataAddress);
            views.add(dataSize);
            views.add(auxAddress);
            views.add(auxSize);
            views.add(top);
        }

        private void clear() {
            parquetBuffers.close();
            if (parquetResource != 0 && parquetDecoder != null) {
                parquetDecoder.releaseDecodeResource(parquetResource);
            }
            parquetResource = 0;
            parquetColumns.clear();
            views.clear();
        }

        private static int findParquetColumn(ParquetDecoder decoder, int writer) {
            for (int column = 0, n = decoder.getColumnCount(); column < n; column++) {
                if (decoder.getColumnId(column) == writer) {
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
                final int writer = requiredColumns.get(i);
                final int type = requiredColumns.get(i + 1);
                if (writer == mapping.getTimestampWriterIndex()) {
                    addView(
                            writer,
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
                    if (mapping.getWriterIndex(q) == writer) {
                        queryColumn = q;
                        break;
                    }
                }
                if (queryColumn >= 0) {
                    addView(
                            writer,
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
                final int writer = requiredColumns.get(i);
                final int parquetColumn = findParquetColumn(parquetDecoder, writer);
                if (parquetColumn >= 0) {
                    parquetColumns.add(parquetColumn);
                    parquetColumns.add(requiredColumns.get(i + 1));
                }
            }

            final int count = (int) (parquetColumns.size() / 2);
            if (count == 0) {
                return;
            }
            parquetBuffers.copyMemoryTrackerFrom(targetBuffers);
            parquetBuffers.reopen();
            try {
                parquetDecoder.decodeRowGroup(
                        parquetBuffers,
                        parquetColumns,
                        window,
                        0,
                        Math.toIntExact(baseRows)
                );
                parquetResource = parquetDecoder.takeDecodeResource();
            } catch (Throwable th) {
                final long resource = parquetDecoder.takeDecodeResource();
                parquetBuffers.close();
                parquetDecoder.releaseDecodeResource(resource);
                throw th;
            }
            for (int slot = 0; slot < count; slot++) {
                final int parquetColumn = parquetColumns.get(2L * slot);
                final int writer = parquetDecoder.getColumnId(parquetColumn);
                final int type = parquetColumns.get(2L * slot + 1);
                final long dataSize = parquetBuffers.getChunkDataSize(slot);
                final long auxSize = parquetBuffers.getChunkAuxSize(slot);
                if (dataSize != 0 || auxSize != 0 || writer == addressCache.getColumnMapping().getTimestampWriterIndex()) {
                    addView(
                            writer,
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

        private long viewAddress() {
            return views.size() == 0 ? 0 : views.getAddress();
        }

        private int viewCount() {
            return (int) (views.size() / BASE_VIEW_LONGS);
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

        private void addRequiredColumn(int writer, int type) {
            if (writer < 0 || type <= 0) {
                return;
            }
            final int normalizedType = normalizeType(type);
            for (long i = 0, n = requiredColumns.size(); i < n; i += 2) {
                if (requiredColumns.get(i) == writer) {
                    return;
                }
            }
            requiredColumns.add(writer);
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
                final int writer = requiredColumns.get(i);
                final int type = requiredColumns.get(i + 1);
                long j = i - 2;
                while (j >= 0 && requiredColumns.get(j) > writer) {
                    requiredColumns.set(j + 2, requiredColumns.get(j));
                    requiredColumns.set(j + 3, requiredColumns.get(j + 1));
                    j -= 2;
                }
                requiredColumns.set(j + 2, writer);
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
                candidatePairs.add(normalizeType(primaryPairs.get(i + 1)));
            }
            candidatePrimaryColumnCount = (int) (candidatePairs.size() / 2);
            if (primaryColumnIndexes != null) {
                final long remainingStart = candidatePairs.size();
                for (int i = 0, n = columnMapping.getColumnCount(); i < n; i++) {
                    if (primaryColumnIndexes.contains(i)) {
                        continue;
                    }
                    final int writer = columnMapping.getWriterIndex(i);
                    boolean isPresent = false;
                    for (long p = remainingStart, m = candidatePairs.size(); p < m; p += 2) {
                        if (candidatePairs.get(p) == writer) {
                            isPresent = true;
                            break;
                        }
                    }
                    if (!isPresent) {
                        candidatePairs.add(writer);
                        candidatePairs.add(normalizeType(columnTypes.getQuick(i)));
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
    }
}
