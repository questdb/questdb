/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;

import java.io.Closeable;

public class PageFrameReduceTask implements Closeable {
    private static final Log LOG = LogFactory.getLog(PageFrameReduceTask.class);
    private final DirectLongList rows;
    private final long pageFrameQueueCapacity;
    private int frameIndex = Integer.MAX_VALUE;
    private PageFrameSequence<?> frameSequence;

    public PageFrameReduceTask(CairoConfiguration configuration, int pageFrameQueueCapacity) {
        this.rows = new DirectLongList(configuration.getPageFrameRowsCapacity(), MemoryTag.NATIVE_LONG_LIST);
        this.pageFrameQueueCapacity = pageFrameQueueCapacity;
    }

    @Override
    public void close() {
        Misc.free(rows);
    }

    public void collected() {
        final long frameCount = frameSequence.getFrameCount();
        final int shard = frameSequence.getShard();
        // We have to reset capacity only on max all queue items
        // What we are avoiding here is resetting capacity on 1000 frames given our queue size
        // is 32 items. If our particular producer resizes queue items to 10x of the initial size
        // we let these sizes stick until produce starts to wind down.
        if (frameIndex >= frameCount - pageFrameQueueCapacity) {
            getRows().resetCapacity();
        }

        // we assume that frame indexes are published in ascending order
        // and when we see the last index, we would free up the remaining resources
        if (frameIndex + 1 == frameCount) {
            LOG.debug()
                    .$("cleanup [shard=").$(shard)
                    .$(", id=").$(frameSequence.getId())
                    .I$();
            frameSequence.reset();
        }
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

    public DirectLongList getRows() {
        return rows;
    }

    public void of(PageFrameSequence<?> frameSequence, int frameIndex) {
        this.frameSequence = frameSequence;
        this.frameIndex = frameIndex;
    }
}
