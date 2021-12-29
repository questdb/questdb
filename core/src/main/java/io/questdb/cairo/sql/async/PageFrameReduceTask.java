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
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

public class PageFrameReduceTask implements Closeable {
    private final DirectLongList rows;
    private Function filter;
    private long producerId;
    private PageAddressCache pageAddressCache;
    private int frameIndex;
    private int frameCount;
    private LongList frameRowCounts;
    private SymbolTableSource symbolTableSource;
    private boolean failed;
    private SCSequence collectSubSeq;
    private AtomicInteger framesCollectedCounter;
    private AtomicInteger framesReducedCounter;

    public PageFrameReduceTask(CairoConfiguration configuration) {
        this.rows = new DirectLongList(32, MemoryTag.NATIVE_LONG_LIST);
    }

    @Override
    public void close() {
        Misc.free(rows);
    }

    public SCSequence getCollectSubSeq() {
        return collectSubSeq;
    }

    public Function getFilter() {
        return filter;
    }

    public int getFrameCount() {
        return frameCount;
    }

    public int getFrameIndex() {
        return frameIndex;
    }

    public long getFrameRowCount() {
        return frameRowCounts.getQuick(frameIndex);
    }

    public LongList getFrameRowCounts() {
        return frameRowCounts;
    }

    public AtomicInteger getFramesCollectedCounter() {
        return framesCollectedCounter;
    }

    public AtomicInteger getFramesReducedCounter() {
        return framesReducedCounter;
    }

    public PageAddressCache getPageAddressCache() {
        return pageAddressCache;
    }

    public long getProducerId() {
        return producerId;
    }

    public DirectLongList getRows() {
        return rows;
    }

    public SymbolTableSource getSymbolTableSource() {
        return symbolTableSource;
    }

    public boolean isFailed() {
        return failed;
    }

    public void setFailed(boolean failed) {
        this.failed = failed;
    }

    public void of(
            long producerId,
            PageAddressCache cache,
            int frameIndex,
            int frameCount,
            LongList frameRowCounts,
            SymbolTableSource symbolTableSource,
            Function filter,
            AtomicInteger framesCollectedCounter,
            AtomicInteger framesReducedCounter,
            SCSequence collectSubSeq
    ) {
        this.producerId = producerId;
        this.pageAddressCache = cache;
        this.frameIndex = frameIndex;
        this.frameCount = frameCount;
        this.frameRowCounts = frameRowCounts;
        this.symbolTableSource = symbolTableSource;
        this.filter = filter;
        this.failed = false;
        this.framesCollectedCounter = framesCollectedCounter;
        this.framesReducedCounter = framesReducedCounter;
        this.collectSubSeq = collectSubSeq;
    }
}
