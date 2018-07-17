/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.latest;

import com.questdb.common.RowCursor;
import com.questdb.common.StorageFacade;
import com.questdb.ql.CancellationHandler;
import com.questdb.ql.PartitionSlice;
import com.questdb.ql.RowSource;
import com.questdb.std.IntLongPriorityQueue;
import com.questdb.std.Unsafe;
import com.questdb.std.str.CharSink;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.JournalMetadata;

public class HeapMergingRowSource implements RowSource, RowCursor {
    private final RowSource[] sources;
    private final RowCursor[] cursors;
    private final IntLongPriorityQueue heap;

    public HeapMergingRowSource(RowSource... sources) {
        this.sources = sources;
        this.cursors = new RowCursor[sources.length];
        this.heap = new IntLongPriorityQueue();
    }

    @Override
    public void configure(JournalMetadata metadata) {
        for (int i = 0, n = sources.length; i < n; i++) {
            Unsafe.arrayGet(sources, i).configure(metadata);
        }
    }

    @Override
    public void prepare(ReaderFactory factory, StorageFacade facade, CancellationHandler cancellationHandler) {
        for (int i = 0, n = sources.length; i < n; i++) {
            Unsafe.arrayGet(sources, i).prepare(factory, facade, cancellationHandler);
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
    public void toTop() {
//        heap.clear();
        for (RowSource src : sources) {
            src.toTop();
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

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("HeapMergingRowSource").put(',');
        sink.putQuoted("src").put(':').put('[');
        for (int i = 0, n = sources.length; i < n; i++) {
            if (i > 0) {
                sink.put(',');
            }
            sink.put(sources[i]);
        }
        sink.put(']');
        sink.put('}');
    }

}
