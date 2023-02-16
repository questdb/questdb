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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;

import java.io.Closeable;

public class PageFrameReduceTask implements Closeable {

    // Used to pass the list of column page frame addresses to a JIT-compiled filter.
    private final DirectLongList columns;
    private final long pageFrameQueueCapacity;
    private final DirectLongList rows;
    private int frameIndex = Integer.MAX_VALUE;
    private PageFrameSequence<?> frameSequence;
    private long frameSequenceId;

    public PageFrameReduceTask(CairoConfiguration configuration) {
        this.rows = new DirectLongList(configuration.getPageFrameReduceRowIdListCapacity(), MemoryTag.NATIVE_OFFLOAD);
        this.columns = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD);
        this.pageFrameQueueCapacity = configuration.getPageFrameReduceQueueCapacity();
    }

    @Override
    public void close() {
        Misc.free(rows);
        Misc.free(columns);
    }

    public DirectLongList getColumns() {
        return columns;
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

    public DirectLongList getRows() {
        return rows;
    }

    public void of(PageFrameSequence<?> frameSequence, int frameIndex) {
        this.frameSequence = frameSequence;
        this.frameSequenceId = frameSequence.getId();
        this.frameIndex = frameIndex;
        rows.clear();
    }

    public void resetCapacities() {
        rows.resetCapacity();
        columns.resetCapacity();
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
