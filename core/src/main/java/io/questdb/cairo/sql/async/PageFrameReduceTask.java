/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.std.DirectLongList;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class PageFrameReduceTask implements QuietCloseable, Mutable {
    public static final byte TYPE_FILTER = 0;
    public static final byte TYPE_GROUP_BY = 1;
    public static final byte TYPE_GROUP_BY_NOT_KEYED = 2;
    public static final byte TYPE_TOP_K = 3;
    private static final String exceptionMessage = "unexpected filter error";

    private final DirectLongList auxAddresses;
    private final DirectLongList dataAddresses;
    private final StringSink errorMsg = new StringSink();
    private final DirectLongList filteredRows; // Used for TYPE_FILTER.
    private final PageFrameMemoryPool frameMemoryPool;
    private final long frameQueueCapacity;
    private int errorMessagePosition;
    private int frameIndex = Integer.MAX_VALUE;
    private PageFrameMemory frameMemory;
    private PageFrameSequence<?> frameSequence;
    private long frameSequenceId;
    private boolean isCancelled;
    private boolean isOutOfMemory;
    private byte taskType;

    public PageFrameReduceTask(CairoConfiguration configuration, int memoryTag) {
        try {
            this.frameQueueCapacity = configuration.getPageFrameReduceQueueCapacity();
            this.filteredRows = new DirectLongList(configuration.getPageFrameReduceRowIdListCapacity(), memoryTag);
            this.dataAddresses = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), memoryTag);
            this.auxAddresses = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), memoryTag);
            // We don't need to cache anything when reducing,
            // and use page frame memory only, hence cache size of 1.
            this.frameMemoryPool = new PageFrameMemoryPool(1);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        filteredRows.resetCapacity();
        dataAddresses.resetCapacity();
        auxAddresses.resetCapacity();
        frameMemoryPool.clear();
    }

    @Override
    public void close() {
        Misc.free(frameMemoryPool);
        Misc.free(filteredRows);
        Misc.free(dataAddresses);
        Misc.free(auxAddresses);
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

    public int getErrorMessagePosition() {
        return errorMessagePosition;
    }

    public CharSequence getErrorMsg() {
        return errorMsg;
    }

    public DirectLongList getFilteredRows() {
        return filteredRows;
    }

    public int getFrameIndex() {
        return frameIndex;
    }

    public PageFrameMemory getFrameMemory() {
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

    public byte getTaskType() {
        return taskType;
    }

    public boolean hasError() {
        return errorMsg.length() > 0;
    }

    public boolean isCancelled() {
        return isCancelled;
    }

    public boolean isOutOfMemory() {
        return isOutOfMemory;
    }

    public void of(PageFrameSequence<?> frameSequence, int frameIndex) {
        this.frameSequence = frameSequence;
        this.frameSequenceId = frameSequence.getId();
        this.taskType = frameSequence.getTaskType();
        this.frameIndex = frameIndex;
        // Top K uses its own frame memory pool.
        if (taskType != TYPE_TOP_K) {
            frameMemoryPool.of(frameSequence.getPageFrameAddressCache());
        }
        frameMemory = null;
        filteredRows.clear();
        errorMsg.clear();
        isCancelled = false;
        isOutOfMemory = false;
    }

    public PageFrameMemory populateFrameMemory() {
        assert taskType != TYPE_TOP_K;
        frameMemory = frameMemoryPool.navigateTo(frameIndex);
        return frameMemory;
    }

    // Must be called after populateFrameMemory.
    public void populateJitData() {
        populateJitData(frameMemory);
    }

    // Useful when using external frame memory pool.
    public void populateJitData(@NotNull PageFrameMemory frameMemory) {
        assert frameMemory.getFrameIndex() == frameIndex;

        final PageFrameAddressCache pageAddressCache = frameSequence.getPageFrameAddressCache();
        final long columnCount = pageAddressCache.getColumnCount();

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

        final long rowCount = getFrameRowCount();
        if (filteredRows.getCapacity() < rowCount) {
            filteredRows.setCapacity(rowCount);
        }
    }

    public void releaseFrameMemory() {
        Misc.free(frameMemoryPool);
        frameMemory = null;
    }

    // same as clear(), but also releases frame pool memory
    public void reset() {
        clear();
        releaseFrameMemory();
    }

    public void setErrorMsg(Throwable th) {
        if (th instanceof FlyweightMessageContainer) {
            errorMsg.put(((FlyweightMessageContainer) th).getFlyweightMessage());
        } else {
            final String msg = th.getMessage();
            errorMsg.put(msg != null ? msg : exceptionMessage);
        }

        if (th instanceof CairoException) {
            final CairoException ce = (CairoException) th;
            isCancelled = ce.isCancellation();
            isOutOfMemory = ce.isOutOfMemory();
            errorMessagePosition = ce.getPosition();
        }
    }

    public void setTaskType(byte taskType) {
        this.taskType = taskType;
    }

    void collected() {
        collected(false);
    }

    void collected(boolean forceCollect) {
        final long frameCount = frameSequence.getFrameCount();

        // we assume that frame indexes are published in ascending order
        // and when we see the last index, we would free up the remaining resources
        if (frameIndex + 1 == frameCount) {
            frameSequence.reset();
        }

        frameSequence = null;
        frameMemory = null;

        // We have to reset capacity only on max all queue items
        // What we are avoiding here is resetting capacity on 1000 frames given our queue size
        // is 32 items. If our particular producer resizes queue items to 10x of the initial size
        // we let these sizes stick until produce starts to wind down.
        if (forceCollect || frameIndex >= frameCount - frameQueueCapacity) {
            reset();
        }
    }
}
