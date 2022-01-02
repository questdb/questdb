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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FrameSequence implements Mutable {
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final AtomicBoolean valid = new AtomicBoolean(true);
    private final AtomicInteger reduceCounter = new AtomicInteger(0);
    private final LongList frameRowCounts = new LongList();
    private final PageFrameReducer reducer;
    private final PageAddressCache pageAddressCache;
    private long id;
    private int shard;
    private int frameCount;
    private SCSequence consumerSubSeq;
    private SymbolTableSource symbolTableSource;

    public FrameSequence(CairoConfiguration configuration, PageFrameReducer reducer) {
        this.reducer = reducer;
        this.pageAddressCache = new PageAddressCache(configuration);
    }

    public void await() {
        doneLatch.await(1);
    }

    @Override
    public void clear() {
        this.id = -1;
        this.shard = -1;
    }

    public SCSequence getConsumerSubSeq() {
        return consumerSubSeq;
    }

    public SOUnboundedCountDownLatch getDoneLatch() {
        return doneLatch;
    }

    public int getFrameCount() {
        return frameCount;
    }

    public LongList getFrameRowCounts() {
        return frameRowCounts;
    }

    public long getId() {
        return id;
    }

    public PageAddressCache getPageAddressCache() {
        return pageAddressCache;
    }

    public AtomicInteger getReduceCounter() {
        return reduceCounter;
    }

    public PageFrameReducer getReducer() {
        return reducer;
    }

    public int getShard() {
        return shard;
    }

    public SymbolTableSource getSymbolTableSource() {
        return symbolTableSource;
    }

    public AtomicBoolean getValid() {
        return valid;
    }

    public void of(
            int shard,
            long frameSequenceId,
            int frameCount,
            SCSequence consumerSubSeq,
            SymbolTableSource symbolTableSource
    ) {
        this.id = frameSequenceId;
        this.doneLatch.reset();
        this.valid.set(true);
        this.reduceCounter.set(0);
        this.shard = shard;
        this.frameCount = frameCount;
        this.consumerSubSeq = consumerSubSeq;
        this.symbolTableSource = symbolTableSource;
    }
}
