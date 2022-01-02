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

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.MPSequence;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Rnd;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PageFrameSequence<T> implements Mutable {
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final AtomicBoolean valid = new AtomicBoolean(true);
    private final AtomicInteger reduceCounter = new AtomicInteger(0);
    private final LongList frameRowCounts = new LongList();
    private final PageFrameReducer reducer;
    private final PageAddressCache pageAddressCache;
    private long id;
    private int shard;
    private int frameCount;
    private SCSequence collectSubSeq;
    private SymbolTableSource symbolTableSource;
    private T atom;

    public PageFrameSequence(CairoConfiguration configuration, PageFrameReducer reducer) {
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
        this.frameCount = 0;
        pageAddressCache.clear();
        frameRowCounts.clear();
        symbolTableSource = Misc.free(symbolTableSource);
        doneLatch.countDown();
    }

    public T getAtom() {
        return atom;
    }

    public SCSequence getCollectSubSeq() {
        return collectSubSeq;
    }

    public int getFrameCount() {
        return frameCount;
    }

    public long getFrameRowCount(int frameIndex) {
        return frameRowCounts.getQuick(frameIndex);
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

    public boolean isValid() {
        return valid.get();
    }

    public void setValid(boolean valid) {
        this.valid.compareAndSet(true, valid);
    }

    public PageFrameSequence<T> dispatch(
            RecordCursorFactory base,
            SqlExecutionContext executionContext,
            SCSequence consumerSubSeq,
            T atom
    ) throws SqlException {
        final Rnd rnd = executionContext.getRandom();
        final MessageBus bus = executionContext.getMessageBus();
        // before thread begins we will need to pick a shard
        // of queues that we will interact with
        final int shard = rnd.nextInt(bus.getPageFrameReduceShardCount());
        final PageFrameCursor pageFrameCursor = base.getPageFrameCursor(executionContext);
        final MPSequence dispatchPubSeq = bus.getPageFrameDispatchPubSeq();
        long dispatchCursor = dispatchPubSeq.next();

        // pass one to cache page addresses
        // this has to be separate pass to ensure there no cache reads
        // while cache might be resizing
        this.pageAddressCache.of(base.getMetadata());

        PageFrame frame;
        int frameIndex = 0;
        while ((frame = pageFrameCursor.next()) != null) {
            this.pageAddressCache.add(frameIndex++, frame);
            frameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
        }

        of(shard, rnd.nextLong(), frameIndex, consumerSubSeq, pageFrameCursor, atom);

        if (dispatchCursor < 0) {
            dispatchCursor = dispatchPubSeq.nextBully();
        }

        // We need to subscribe publisher sequence before we return
        // control to the caller of this method. However, this sequence
        // will be unsubscribed asynchronously.
        bus.getPageFrameCollectFanOut(shard).and(consumerSubSeq);

        PageFrameDispatchTask dispatchTask = bus.getPageFrameDispatchQueue().get(dispatchCursor);
        dispatchTask.of(this);
        dispatchPubSeq.done(dispatchCursor);
        return this;
    }

    private void of(
            int shard,
            long frameSequenceId,
            int frameCount,
            SCSequence collectSubSeq,
            SymbolTableSource symbolTableSource,
            T atom
    ) {
        this.id = frameSequenceId;
        this.doneLatch.reset();
        this.valid.set(true);
        this.reduceCounter.set(0);
        this.shard = shard;
        this.frameCount = frameCount;
        this.collectSubSeq = collectSubSeq;
        this.symbolTableSource = symbolTableSource;
        this.atom = atom;
    }
}
