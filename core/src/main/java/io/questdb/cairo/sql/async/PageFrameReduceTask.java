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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageAddressCache;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PageFrameReduceTask implements Closeable {
    private final DirectLongList rows;

    private long frameSequenceId;
    private int frameSequenceFrameCount;
    private AtomicBoolean frameSequenceValid;
    private PageFrameReducer frameSequenceReducer;
    private LongList frameSequenceFrameRowCounts;
    private AtomicInteger frameSequenceReduceCounter;
    private SOUnboundedCountDownLatch frameSequenceDoneLatch;

    private SCSequence collectSubSeq;
    private SymbolTableSource symbolTableSource;
    private PageAddressCache pageAddressCache;
    private Function filter;

    private int frameSequenceFrameIndex;

    public PageFrameReduceTask(CairoConfiguration configuration) {
        this.rows = new DirectLongList(configuration.getPageFrameRowsCapacity(), MemoryTag.NATIVE_LONG_LIST);
    }

    @Override
    public void close() {
        Misc.free(rows);
    }

    public SCSequence getCollectSubSeq() {
        return collectSubSeq;
    }

    public SOUnboundedCountDownLatch getFrameSequenceDoneLatch() {
        return frameSequenceDoneLatch;
    }

    public Function getFilter() {
        return filter;
    }

    public int getFrameSequenceFrameCount() {
        return frameSequenceFrameCount;
    }

    public int getFrameSequenceFrameIndex() {
        return frameSequenceFrameIndex;
    }

    public long getFrameRowCount() {
        return frameSequenceFrameRowCounts.getQuick(frameSequenceFrameIndex);
    }

    public LongList getFrameSequenceFrameRowCounts() {
        return frameSequenceFrameRowCounts;
    }

    public AtomicInteger getFrameSequenceReduceCounter() {
        return frameSequenceReduceCounter;
    }

    public PageAddressCache getPageAddressCache() {
        return pageAddressCache;
    }

    public long getFrameSequenceId() {
        return frameSequenceId;
    }

    public PageFrameReducer getReducer() {
        return frameSequenceReducer;
    }

    public DirectLongList getRows() {
        return rows;
    }

    public SymbolTableSource getSymbolTableSource() {
        return symbolTableSource;
    }

    public boolean isFrameSequenceValid() {
        return frameSequenceValid.get();
    }

    public void setFrameSequenceValid(boolean frameSequenceValid) {
        this.frameSequenceValid.compareAndSet(false, frameSequenceValid);
    }

    public void of(
            PageFrameReducer frameSequenceReducer,
            long frameSequenceId,
            PageAddressCache cache,
            int frameSequenceFrameIndex,
            int frameSequenceFrameCount,
            LongList frameRowCounts,
            SymbolTableSource symbolTableSource,
            Function filter,
            AtomicInteger frameSequenceReduceCounter,
            SCSequence collectSubSeq,
            SOUnboundedCountDownLatch frameSequenceDoneLatch,
            AtomicBoolean frameSequenceValid
    ) {
        this.frameSequenceReducer = frameSequenceReducer;
        this.frameSequenceId = frameSequenceId;
        this.pageAddressCache = cache;
        this.frameSequenceFrameIndex = frameSequenceFrameIndex;
        this.frameSequenceFrameCount = frameSequenceFrameCount;
        this.frameSequenceFrameRowCounts = frameRowCounts;
        this.symbolTableSource = symbolTableSource;
        this.filter = filter;
        this.frameSequenceReduceCounter = frameSequenceReduceCounter;
        this.collectSubSeq = collectSubSeq;
        this.frameSequenceDoneLatch = frameSequenceDoneLatch;
        this.frameSequenceValid = frameSequenceValid;
    }
}
