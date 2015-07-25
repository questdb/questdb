/*******************************************************************************
 * _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 * <p/>
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.nfsdb.ql.impl;

import com.nfsdb.collections.MinHeap;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.RowCursor;
import com.nfsdb.ql.RowSource;
import com.nfsdb.ql.SymFacade;
import com.nfsdb.utils.Unsafe;

public class HeapMergingRowSource implements RowSource, RowCursor {
    private final RowSource[] sources;
    private final RowCursor[] cursors;
    private final MinHeap heap;

    public HeapMergingRowSource(RowSource... sources) {
        this.sources = sources;
        this.cursors = new RowCursor[sources.length];
        this.heap = new MinHeap(sources.length);
    }

    @Override
    public void configure(JournalMetadata metadata) {
        for (RowSource src : sources) {
            src.configure(metadata);
        }
    }

    @Override
    public void prepare(SymFacade facade) {
        for (RowSource src : sources) {
            src.prepare(facade);
        }
    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        heap.clear();
        for (int i = 0, n = sources.length; i < n; i++) {
            RowCursor c = Unsafe.arrayGet(sources, i).prepareCursor(slice);
            Unsafe.arrayPut(cursors, i, c);
            if (c.hasNext()) {
                heap.add(i, c.next());
            }
        }

        return this;
    }

    @Override
    public void reset() {
        heap.clear();
        for (RowSource src : sources) {
            src.reset();
        }
    }

    @Override
    public boolean hasNext() {
        return heap.hasNext();
    }

    @Override
    public long next() {
        int idx = heap.popIndex();
        return Unsafe.arrayGet(cursors, idx).hasNext() ? heap.popAndReplace(idx, Unsafe.arrayGet(cursors, idx).next()) : heap.popValue();
    }
}
