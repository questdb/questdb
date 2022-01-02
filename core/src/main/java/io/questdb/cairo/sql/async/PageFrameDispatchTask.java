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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageAddressCache;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.LongList;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PageFrameDispatchTask {
    private int shard;

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

    public SCSequence getCollectSubSeq() {
        return collectSubSeq;
    }

    public Function getFilter() {
        return filter;
    }

    public SOUnboundedCountDownLatch getFrameSequenceDoneLatch() {
        return frameSequenceDoneLatch;
    }

    public int getFrameSequenceFrameCount() {
        return frameSequenceFrameCount;
    }

    public LongList getFrameSequenceFrameRowCounts() {
        return frameSequenceFrameRowCounts;
    }

    public long getFrameSequenceId() {
        return frameSequenceId;
    }

    public AtomicInteger getFrameSequenceReduceCounter() {
        return frameSequenceReduceCounter;
    }

    public AtomicBoolean getFrameSequenceValid() {
        return frameSequenceValid;
    }

    public PageAddressCache getPageAddressCache() {
        return pageAddressCache;
    }

    public PageFrameReducer getReducer() {
        return frameSequenceReducer;
    }

    public int getShard() {
        return shard;
    }

    public SymbolTableSource getSymbolTableSource() {
        return symbolTableSource;
    }

    public void of(
            long frameSequenceId,
            int frameSequenceFrameCount,
            AtomicBoolean frameSequenceValid,
            PageFrameReducer frameSequenceReducer,
            LongList frameSequenceFrameRowCounts,
            AtomicInteger frameSequenceReduceCounter,
            SOUnboundedCountDownLatch frameSequenceDoneLatch,
            SCSequence collectSubSeq,
            SymbolTableSource symbolTableSource,
            PageAddressCache pageAddressCache,
            Function filter,
            int shard
    ) {
        this.frameSequenceId = frameSequenceId;
        this.frameSequenceFrameCount = frameSequenceFrameCount;
        this.frameSequenceValid = frameSequenceValid;
        this.frameSequenceReducer = frameSequenceReducer;
        this.frameSequenceFrameRowCounts = frameSequenceFrameRowCounts;
        this.frameSequenceReduceCounter = frameSequenceReduceCounter;
        this.frameSequenceDoneLatch = frameSequenceDoneLatch;

        this.collectSubSeq = collectSubSeq;
        this.symbolTableSource = symbolTableSource;
        this.pageAddressCache = pageAddressCache;
        this.filter = filter;

        this.shard = shard;
    }
}
