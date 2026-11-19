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

package io.questdb.cairo.sql.async;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.std.DirectLongList;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.IntHashSet;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.NumericException;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Vect;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PageFrameReduceTask implements QuietCloseable, Mutable {
    public static final byte TYPE_FILTER = 0;
    public static final byte TYPE_HORIZON_JOIN = 3;
    public static final byte TYPE_TOP_K = 1;
    public static final byte TYPE_WINDOW_JOIN = 2;
    private static final String exceptionMessage = "unexpected filter error";

    // Concatenated per-sub-frame filteredRows for a multi-sub-frame run (subFrameCount > 1); a single
    // sub-frame keeps using filteredRows directly with no copy. See runReduce.
    private final DirectLongList accumulatedRows;
    private final DirectLongList auxAddresses;
    private final DirectLongList dataAddresses;
    private final StringSink errorMsg = new StringSink();
    private final DirectLongList filteredRows; // Used for TYPE_FILTER, TYPE_WINDOW_JOIN and TYPE_HORIZON_JOIN.
    private final PageFrameMemoryPool frameMemoryPool;
    private final long frameQueueCapacity;
    // Per-sub-frame logical matching-row count (what the reducer reports via setFilteredRowCount).
    private final LongList subFrameRowCounts = new LongList();
    // Per-sub-frame start offset into the run's rows list, with a trailing total (size subFrameCount + 1).
    private final LongList subFrameRowOffsets = new LongList();
    private int errno = CairoException.NON_CRITICAL;
    private byte errorKind = AsyncQueryErrorKind.KIND_NONE;
    private int errorMessagePosition;
    private long filteredRowCount;
    // Global frame index of this run's first sub-frame.
    private int firstFrameIndex;
    // Global index of the sub-frame currently visible to the reducer or collector.
    private int frameIndex = Integer.MAX_VALUE;
    private int frameIndexStep;
    private PageFrameMemory frameMemory;
    private PageFrameSequence<?> frameSequence;
    private long frameSequenceId = -1;
    private int interruptionReason = SqlExecutionCircuitBreaker.STATE_OK;
    // Valid for TYPE_FILTER and TYPE_HORIZON_JOIN only. When set, the reducer sets filteredRowCount
    // and the collecting cursor must not read filteredRows: a filtered TYPE_HORIZON_JOIN reducer
    // still fills it with the filtered row indexes.
    private boolean isCountOnly;
    private boolean isOutOfMemory;
    // Number of sub-frames in this run (>= 1). A native frame or an unsplit parquet row group is 1.
    private int subFrameCount;
    // The run index in the dispatch/collect protocol (one task == one row-group run).
    private int taskIndex = Integer.MAX_VALUE;
    private byte taskType;

    public PageFrameReduceTask(CairoConfiguration configuration, int memoryTag) {
        try {
            this.frameQueueCapacity = configuration.getPageFrameReduceQueueCapacity();
            this.filteredRows = new DirectLongList(configuration.getPageFrameReduceRowIdListCapacity(), memoryTag);
            // Only a run of several sub-frames fills it, so it allocates on first use.
            this.accumulatedRows = new DirectLongList(configuration.getPageFrameReduceRowIdListCapacity(), memoryTag, true);
            this.dataAddresses = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), memoryTag);
            this.auxAddresses = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), memoryTag);
            this.frameMemoryPool = new PageFrameMemoryPool(configuration, 0L);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public static void populateJitAddresses(
            @NotNull PageFrameMemory frameMemory,
            @NotNull PageFrameAddressCache pageAddressCache,
            @NotNull DirectLongList dataAddresses,
            @NotNull DirectLongList auxAddresses
    ) {
        final int columnCount = pageAddressCache.getColumnCount();

        dataAddresses.clear();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            dataAddresses.add(frameMemory.getPageAddress(columnIndex));
        }

        auxAddresses.clear();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            auxAddresses.add(
                    pageAddressCache.isVarSizeColumn(columnIndex)
                            ? frameMemory.getAuxPageAddress(columnIndex)
                            : 0
            );
        }
    }

    /**
     * Builds the typed exception to re-throw at the collector. The kind is the class
     * of the throwable captured by the worker via {@link #setErrorMsg(Throwable)};
     * {@link ImplicitCastException} and {@link NumericException} are preserved so
     * callers (and the fuzzer oracle) can recognise legitimate user-facing errors.
     * All other throwables fall back to a non-critical {@link CairoException}, which
     * preserves the pre-existing behaviour for truly unexpected errors (e.g. NPE).
     */
    public RuntimeException buildError() {
        return switch (errorKind) {
            case AsyncQueryErrorKind.KIND_IMPLICIT_CAST ->
                    ImplicitCastException.instance().position(errorMessagePosition).put(errorMsg);
            case AsyncQueryErrorKind.KIND_NUMERIC ->
                    NumericException.instance().position(errorMessagePosition).put(errorMsg);
            // critical(errno) preserves the worker's errno and, with it, isCritical();
            // errno == NON_CRITICAL reduces to the previous nonCritical() behaviour.
            default -> CairoException.critical(errno)
                    .position(errorMessagePosition)
                    .put(errorMsg)
                    .setInterruptionReason(interruptionReason)
                    .setOutOfMemory(isOutOfMemory);
        };
    }

    @Override
    public void clear() {
        filteredRowCount = 0;
        subFrameCount = 0;
        isCountOnly = false;
        filteredRows.resetCapacity();
        accumulatedRows.close();
        subFrameRowCounts.clear();
        subFrameRowOffsets.clear();
        dataAddresses.resetCapacity();
        auxAddresses.resetCapacity();
        frameMemoryPool.clear();
    }

    @Override
    public void close() {
        filteredRowCount = 0;
        subFrameCount = 0;
        isCountOnly = false;
        Misc.free(filteredRows);
        Misc.free(accumulatedRows);
        Misc.free(dataAddresses);
        Misc.free(auxAddresses);
        Misc.free(frameMemoryPool);
    }

    /**
     * Returns list of pointers to aux vectors (var-size columns only).
     */
    public DirectLongList getAuxAddresses() {
        return auxAddresses;
    }

    /**
     * Returns list of pointers to data vectors.
     */
    public DirectLongList getDataAddresses() {
        return dataAddresses;
    }

    public long getFilteredRowCount() {
        return filteredRowCount;
    }

    public DirectLongList getFilteredRows() {
        return filteredRows;
    }

    public int getFrameIndex() {
        return frameIndex;
    }

    public PageFrameMemory getFrameMemory() {
        if (frameMemory == null && subFrameCount > 1) {
            // Only the last sub-frame decoded by the reducer remains bound. Some collectors need just row ids.
            frameMemory = frameMemoryPool.navigateTo(frameIndex);
        }
        return frameMemory;
    }

    public long getFrameRowCount() {
        return frameSequence.getFrameRowCount(frameIndex);
    }

    public PageFrameSequence<?> getFrameSequence() {
        return frameSequence;
    }

    @SuppressWarnings({"unchecked", "unused"})
    public <T extends StatefulAtom> PageFrameSequence<T> getFrameSequence(Class<T> unused) {
        return (PageFrameSequence<T>) frameSequence;
    }

    public long getFrameSequenceId() {
        return frameSequenceId;
    }

    /**
     * Returns the per-query memory tracker captured by the owning frame sequence
     * at workload start, or {@code null} between workloads / when no per-query
     * limit is configured. Workers feed this to tracker-aware allocation paths.
     */
    public MemoryTracker getMemoryTracker() {
        return frameSequence != null ? frameSequence.getMemoryTracker() : null;
    }

    // Number of sub-frames in this run; the collector iterates them one at a time via the sequence so the
    // row-group run stays one unit of parallel work while every cursor still sees one frame per collect.
    public int getSubFrameCount() {
        return subFrameCount;
    }

    public int getTaskIndex() {
        return taskIndex;
    }

    public byte getTaskType() {
        return taskType;
    }

    public boolean hasError() {
        return !errorMsg.isEmpty();
    }

    public boolean isCancelled() {
        return interruptionReason == SqlExecutionCircuitBreaker.STATE_CANCELLED;
    }

    public boolean isCountOnly() {
        return isCountOnly;
    }

    public boolean isOutOfMemory() {
        return isOutOfMemory;
    }

    public boolean isParquetFrame() {
        return frameSequence.getPageFrameAddressCache().getFrameFormat(frameIndex) == PartitionFormat.PARQUET;
    }

    public void of(PageFrameSequence<?> frameSequence, int taskIndex, boolean countOnly) {
        this.frameSequence = frameSequence;
        final boolean sameQueryExecution = frameSequenceId == frameSequence.getId();
        this.frameSequenceId = frameSequence.getId();
        this.taskType = frameSequence.getTaskType();
        this.taskIndex = taskIndex;
        this.firstFrameIndex = frameSequence.getTaskFirstFrame(taskIndex);
        this.subFrameCount = frameSequence.getTaskFrameCount(taskIndex);
        this.frameIndexStep = frameSequence.getDispatchFrameStep();
        this.frameIndex = firstFrameIndex;
        this.isCountOnly = countOnly;
        // Rebind the per-query tracker on every task: clear() nulls it on the
        // pool between tasks, and the pool.of() below only re-runs on a fresh
        // query. Top K uses its own frame memory pool, so this is a no-op there.
        frameMemoryPool.setMemoryTracker(frameSequence.getMemoryTracker());
        // Bind the memory pool on the first task of each query. Top K owns its memory pool.
        if (!sameQueryExecution && taskType != TYPE_TOP_K) {
            frameMemoryPool.of(frameSequence.getPageFrameAddressCache());
        }
        frameMemory = null;
        filteredRows.clear();
        accumulatedRows.clear();
        subFrameRowCounts.clear();
        subFrameRowOffsets.clear();
        filteredRowCount = 0;
        errorMsg.clear();
        errorMessagePosition = 0;
        errno = CairoException.NON_CRITICAL;
        errorKind = AsyncQueryErrorKind.KIND_NONE;
        interruptionReason = SqlExecutionCircuitBreaker.STATE_OK;
        isOutOfMemory = false;
    }

    public PageFrameMemory populateFrameMemory() {
        assert taskType != TYPE_TOP_K;
        frameMemory = frameMemoryPool.navigateTo(frameIndex);
        return frameMemory;
    }

    public PageFrameMemory populateFrameMemory(IntHashSet columnIndexes) {
        assert taskType != TYPE_TOP_K;
        frameMemory = frameMemoryPool.navigateTo(frameIndex, columnIndexes);
        return frameMemory;
    }

    // Must be called after populateFrameMemory.
    public void populateJitData() {
        populateJitData(frameMemory);
    }

    // Useful when using external frame memory pool.
    public void populateJitData(@NotNull PageFrameMemory frameMemory) {
        assert frameMemory.getFrameIndex() == frameIndex;
        populateJitAddresses(frameMemory, frameSequence.getPageFrameAddressCache(), dataAddresses, auxAddresses);
        if (!isCountOnly) {
            final long rowCount = getFrameRowCount();
            if (filteredRows.getCapacity() < rowCount) {
                filteredRows.setCapacity(rowCount);
            }
        }
    }

    public boolean populateRemainingColumns(IntHashSet filterColumnIndexes, DirectLongList filteredRows, boolean fillWithNulls) {
        assert frameMemory != null;
        return frameMemory.populateRemainingColumns(filterColumnIndexes, filteredRows, fillWithNulls);
    }

    /**
     * Positions the task to present one sub-frame's reduce output to the collector.
     */
    public void positionAtSubFrame(int subFrame) {
        if (subFrameCount == 1) {
            return;
        }
        final int nextFrameIndex = firstFrameIndex + subFrame * frameIndexStep;
        if (frameIndex != nextFrameIndex) {
            frameMemory = null;
            frameIndex = nextFrameIndex;
        }
        // A cancelled or failed run can leave the per-sub-frame results incomplete.
        if (subFrame >= subFrameRowCounts.size() || subFrame + 1 >= subFrameRowOffsets.size()) {
            filteredRowCount = 0;
            filteredRows.clear();
            return;
        }
        filteredRowCount = subFrameRowCounts.getQuick(subFrame);
        final long offset = subFrameRowOffsets.getQuick(subFrame);
        final long len = subFrameRowOffsets.getQuick(subFrame + 1) - offset;
        filteredRows.clear();
        if (len > 0) {
            if (filteredRows.getCapacity() < len) {
                filteredRows.setCapacity(len);
            }
            Vect.memcpy(filteredRows.getAddress(), accumulatedRows.getAddress() + (offset << 3), len << 3);
            filteredRows.setPos(len);
        }
    }

    public void releaseFrameMemory() {
        frameMemoryPool.releaseDecodedFrameBuffers();
        frameMemoryPool.releasePartitionFrameWindow();
        frameMemory = null;
    }

    /**
     * Reduces the whole row-group run as a single unit of parallel work. The split into bounded
     * sub-frames is the producer's; this drives the per-frame reducer over the run's sub-frames in
     * order on one worker, presenting one sub-frame at a time (so the reducer kernels are unchanged) and
     * snapshotting each sub-frame's output. A single sub-frame (the common, unsplit case) runs the
     * reducer exactly once with no extra copy.
     */
    public void runReduce(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final PageFrameReducer reducer = frameSequence.getReducer();
        if (subFrameCount == 1) {
            reducer.reduce(workerId, record, this, circuitBreaker, stealingFrameSequence);
            return;
        }
        long accumulatedLen = 0;
        for (int j = 0; j < subFrameCount; j++) {
            subFrameRowOffsets.add(accumulatedLen);
            frameIndex = firstFrameIndex + j * frameIndexStep;
            frameMemory = null;
            filteredRows.clear();
            filteredRowCount = 0;
            reducer.reduce(workerId, record, this, circuitBreaker, stealingFrameSequence);
            subFrameRowCounts.add(filteredRowCount);
            accumulatedRows.addAll(filteredRows);
            accumulatedLen += filteredRows.size();
        }
        subFrameRowOffsets.add(accumulatedLen);
    }

    public void setErrorMsg(Throwable th) {
        if (th instanceof FlyweightMessageContainer fmc) {
            errorMsg.put(fmc.getFlyweightMessage());
            errorMessagePosition = fmc.getPosition();
        } else {
            final String msg = th.getMessage();
            errorMsg.put(msg != null ? msg : exceptionMessage);
        }

        if (th instanceof CairoException ce) {
            errno = ce.getErrno();
            interruptionReason = ce.getInterruptionReason();
            isOutOfMemory = ce.isOutOfMemory();
        }

        errorKind = AsyncQueryErrorKind.of(th);
    }

    public void setFilteredRowCount(long filteredRowCount) {
        this.filteredRowCount = filteredRowCount;
    }

    public void setTaskType(byte taskType) {
        this.taskType = taskType;
    }

    void collected() {
        collected(false);
    }

    void collected(boolean forceCollect) {
        final long taskCount = frameSequence.getTaskCount();

        // tasks are published in ascending order, so when we see the last task index
        // we free up the remaining resources
        if (taskIndex + 1 == taskCount) {
            frameSequence.markAsDone();
        }

        frameSequence = null;
        frameMemory = null;

        // We have to reset capacity only on max all queue items
        // What we are avoiding here is resetting capacity on 1000 tasks given our queue size
        // is 32 items. If our particular producer resizes queue items to 10x of the initial size
        // we let these sizes stick until produce starts to wind down.
        if (forceCollect || taskIndex >= taskCount - frameQueueCapacity) {
            clear();
        } else {
            // Never keep parquet buffers around to avoid OOM even if there is an ongoing query.
            releaseFrameMemory();
        }
    }

    long getTaskRowCount() {
        // Scheduling budgets cover the whole run; reducers still read one subframe at a time.
        long rowCount = 0;
        for (int i = 0; i < subFrameCount; i++) {
            rowCount += frameSequence.getFrameRowCount(firstFrameIndex + i * frameIndexStep);
        }
        return rowCount;
    }
}
