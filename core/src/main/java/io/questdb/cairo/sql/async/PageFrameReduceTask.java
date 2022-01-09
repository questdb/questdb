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
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;

import java.io.Closeable;

public class PageFrameReduceTask implements Closeable {
    private final DirectLongList rows;
    private int frameIndex;
    private PageFrameSequence<?> frameSequence;

    public PageFrameReduceTask(CairoConfiguration configuration) {
        this.rows = new DirectLongList(configuration.getPageFrameRowsCapacity(), MemoryTag.NATIVE_LONG_LIST);
    }

    @Override
    public void close() {
        Misc.free(rows);
    }

    public long getFrameRowCount() {
        return this.frameSequence.getFrameRowCount(frameIndex);
    }

    public PageFrameSequence<?> getFrameSequence() {
        return frameSequence;
    }

    @SuppressWarnings({"unchecked", "unused"})
    public <T> PageFrameSequence<T> getFrameSequence(Class<T> unused) {
        return (PageFrameSequence<T>) frameSequence;
    }

    public int getFrameIndex() {
        return frameIndex;
    }

    public DirectLongList getRows() {
        return rows;
    }

    public void of(PageFrameSequence<?> frameSequence, int frameIndex) {
        this.frameSequence = frameSequence;
        this.frameIndex = frameIndex;
    }
}
