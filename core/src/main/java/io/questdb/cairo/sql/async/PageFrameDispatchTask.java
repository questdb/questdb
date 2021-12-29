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
import io.questdb.std.LongList;

public class PageFrameDispatchTask {
    private int shard;
    private SCSequence collectSubSeq;
    private SymbolTableSource symbolTableSource;
    private PageAddressCache pageAddressCache;
    private int frameCount;
    private long producerId;
    private LongList frameRowCounts;
    private Function filter;

    public SCSequence getCollectSubSeq() {
        return collectSubSeq;
    }

    public Function getFilter() {
        return filter;
    }

    public int getFrameCount() {
        return frameCount;
    }

    public LongList getFrameRowCounts() {
        return frameRowCounts;
    }

    public PageAddressCache getPageAddressCache() {
        return pageAddressCache;
    }

    public long getProducerId() {
        return producerId;
    }

    public int getShard() {
        return shard;
    }

    public SymbolTableSource getSymbolTableSource() {
        return symbolTableSource;
    }

    public void of(
            PageAddressCache pageAddressCache,
            int frameCount,
            int shard,
            long producerId,
            SCSequence consumerSubSeq,
            SymbolTableSource symbolTableSource,
            LongList frameRowCounts,
            Function filter
    ) {
        this.shard = shard;
        this.collectSubSeq = consumerSubSeq;
        this.symbolTableSource = symbolTableSource;
        this.pageAddressCache = pageAddressCache;
        this.frameCount = frameCount;
        this.producerId = producerId;
        this.frameRowCounts = frameRowCounts;
        this.filter = filter;
    }
}
