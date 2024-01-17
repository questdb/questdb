/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.PageAddressCache;
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
    private final long pageFrameQueueCapacity;
    private int frameIndex = Integer.MAX_VALUE;
    private PageFrameSequence<?> frameSequence;
    private long frameSequenceId;
    private byte type;

    public PageFrameReduceTask(CairoConfiguration configuration, int memoryTag) {
        this.filteredRows = new DirectLongList(configuration.getPageFrameReduceRowIdListCapacity(), memoryTag);
        this.columns = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), memoryTag);
        this.pageFrameQueueCapacity = configuration.getPageFrameReduceQueueCapacity();
    }

    @Override
    public void close() {
        Misc.free(filteredRows);
        Misc.free(columns);
    }

    public DirectLongList getColumns() {
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

    public long getFrameRowCount() {
        return this.frameSequence.getFrameRowCount(frameIndex);
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

    public PageAddressCache getPageAddressCache() {
        return frameSequence.getPageAddressCache();
    }

    public byte getType() {
        return type;
    }

    public boolean hasError() {
        return errorMsg.length() > 0;
    }

    public void of(PageFrameSequence<?> frameSequence, int frameIndex) {
        this.frameSequence = frameSequence;
        this.frameSequenceId = frameSequence.getId();
        this.type = frameSequence.getTaskType();
        this.frameIndex = frameIndex;
        errorMsg.clear();
        if (type == TYPE_FILTER) {
            filteredRows.clear();
        }
    }

    public void resetCapacities() {
        filteredRows.resetCapacity();
        columns.resetCapacity();
    }

    public void setErrorMsg(Throwable th) {
        if (th instanceof FlyweightMessageContainer) {
            errorMsg.put(((FlyweightMessageContainer) th).getFlyweightMessage());
        } else {
            final String msg = th.getMessage();
            errorMsg.put(msg != null ? msg : exceptionMessage);
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
        // We have to reset capacity only on max all queue items
        // What we are avoiding here is resetting capacity on 1000 frames given our queue size
        // is 32 items. If our particular producer resizes queue items to 10x of the initial size
        // we let these sizes stick until produce starts to wind down.
        if (forceCollect || frameIndex >= frameCount - pageFrameQueueCapacity) {
            resetCapacities();
        }

        // we assume that frame indexes are published in ascending order
        // and when we see the last index, we would free up the remaining resources
        if (frameIndex + 1 == frameCount) {
            frameSequence.reset();
        }

        frameSequence = null;
    }
}
