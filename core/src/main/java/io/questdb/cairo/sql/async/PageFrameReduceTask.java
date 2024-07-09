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
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public class PageFrameReduceTask implements Closeable {

    public static final byte TYPE_FILTER = 0;
    public static final byte TYPE_GROUP_BY = 1;
    public static final byte TYPE_GROUP_BY_NOT_KEYED = 2;
    private static final String exceptionMessage = "unexpected filter error";
    // Used to pass the list of column page frame addresses to a JIT-compiled filter.
    private final DirectLongList columns;
    private final StringSink errorMsg = new StringSink();
    private final DirectLongList filteredRows; // Used for TYPE_FILTER.
    private final PageFrameMemoryPool frameMemoryPool;
    private final long frameQueueCapacity;
    private final DirectLongList varSizeAux;
    private int frameIndex = Integer.MAX_VALUE;
    private PageFrameMemory frameMemory;
    private PageFrameSequence<?> frameSequence;
    private long frameSequenceId;
    private boolean isCancelled;
    private byte type;

    public PageFrameReduceTask(CairoConfiguration configuration, int memoryTag) {
        try {
            this.filteredRows = new DirectLongList(configuration.getPageFrameReduceRowIdListCapacity(), memoryTag);
            this.columns = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), memoryTag);
            this.varSizeAux = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), memoryTag);
            this.frameQueueCapacity = configuration.getPageFrameReduceQueueCapacity();
            this.frameMemoryPool = new PageFrameMemoryPool();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        Misc.free(frameMemoryPool);
        Misc.free(filteredRows);
        Misc.free(columns);
        Misc.free(varSizeAux);
    }

    /**
     * Returns list of pointers to data vectors.
     */
    public DirectLongList getData() {
        return columns;
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

    public byte getType() {
        return type;
    }

    /**
     * Returns list of pointers to aux vectors (var-size columns only).
     */
    public DirectLongList getVarSizeAux() {
        return varSizeAux;
    }

    public boolean hasError() {
        return errorMsg.length() > 0;
    }

    public boolean isCancelled() {
        return isCancelled;
    }

    public void of(PageFrameSequence<?> frameSequence, int frameIndex) {
        this.frameSequence = frameSequence;
        this.frameMemoryPool.of(frameSequence.getAddressCache());
        this.frameSequenceId = frameSequence.getId();
        this.type = frameSequence.getTaskType();
        this.frameIndex = frameIndex;
        errorMsg.clear();
        isCancelled = false;
        frameMemory = null;
        if (type == TYPE_FILTER) {
            filteredRows.clear();
        }
    }

    public PageFrameMemory populateFrameMemory() {
        frameMemory = frameMemoryPool.navigateTo(frameIndex);
        return frameMemory;
    }

    // Must be called after populateFrameMemory.
    public void populateJitData() {
        assert frameMemory != null;
        assert frameMemory.getFrameIndex() == frameIndex;

        final PageFrameAddressCache pageAddressCache = frameSequence.getAddressCache();
        final long columnCount = pageAddressCache.getColumnCount();
        if (columns.getCapacity() < columnCount) {
            columns.setCapacity(columnCount);
        }
        columns.clear();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            columns.add(frameMemory.getPageAddress(columnIndex));
        }

        if (varSizeAux.getCapacity() < columnCount) {
            varSizeAux.setCapacity(columnCount);
        }
        varSizeAux.clear();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            varSizeAux.add(
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
        frameMemory = Misc.free(frameMemory);
        frameMemoryPool.close();
    }

    public void resetCapacities() {
        filteredRows.resetCapacity();
        columns.resetCapacity();
        varSizeAux.resetCapacity();
        frameMemoryPool.close();
    }

    public void setErrorMsg(Throwable th) {
        if (th instanceof FlyweightMessageContainer) {
            errorMsg.put(((FlyweightMessageContainer) th).getFlyweightMessage());
        } else {
            final String msg = th.getMessage();
            errorMsg.put(msg != null ? msg : exceptionMessage);
        }

        if (th instanceof CairoException) {
            isCancelled = ((CairoException) th).isCancellation();
        }
    }

    public void setType(byte type) {
        this.type = type;
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
        frameMemory = Misc.free(frameMemory);

        // We have to reset capacity only on max all queue items
        // What we are avoiding here is resetting capacity on 1000 frames given our queue size
        // is 32 items. If our particular producer resizes queue items to 10x of the initial size
        // we let these sizes stick until produce starts to wind down.
        if (forceCollect || frameIndex >= frameCount - frameQueueCapacity) {
            resetCapacities();
        }
    }
}
