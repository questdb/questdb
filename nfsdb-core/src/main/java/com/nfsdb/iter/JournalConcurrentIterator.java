/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.iter;

import com.nfsdb.Journal;
import com.nfsdb.collections.ObjList;
import com.nfsdb.collections.ObjectFactory;
import com.nfsdb.concurrent.RingQueue;
import com.nfsdb.concurrent.SCSequence;
import com.nfsdb.concurrent.SPSequence;
import com.nfsdb.concurrent.Sequence;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.misc.NamedDaemonThreadFactory;
import com.nfsdb.misc.Rows;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JournalConcurrentIterator<T> extends com.nfsdb.collections.AbstractImmutableIterator<T>
        implements ObjectFactory<JournalConcurrentIterator.Holder<T>>, ConcurrentIterator<T> {
    private final Journal<T> journal;
    private final ObjList<JournalIteratorRange> ranges;
    private final ExecutorService service;
    private RingQueue<Holder<T>> buffer;
    private Sequence pubSeq;
    private Sequence subSeq;
    private int bufferSize;
    private boolean started = false;
    private long cursor = -1;

    public JournalConcurrentIterator(Journal<T> journal, ObjList<JournalIteratorRange> ranges, int bufferSize) {
        this.bufferSize = bufferSize;
        this.service = Executors.newSingleThreadExecutor(new NamedDaemonThreadFactory("nfsdb-iterator", false));
        this.journal = journal;
        this.ranges = ranges;
    }

    @Override
    public ConcurrentIterator<T> buffer(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    @Override
    public void close() {
        service.shutdown();
    }

    @Override
    public Journal<T> getJournal() {
        return journal;
    }

    @Override
    public boolean hasNext() {
        if (!started) {
            start();
            started = true;
        }
        if (cursor >= 0) {
            subSeq.done(cursor);
        }
        this.cursor = subSeq.nextBully();
        return buffer.get(cursor).hasNext;
    }

    @Override
    public T next() {
        return buffer.get(cursor).object;
    }

    @Override
    public Holder<T> newInstance() {
        Holder<T> h = new Holder<>();
        h.object = getJournal().newObject();
        h.hasNext = true;
        return h;
    }

    private Runnable getRunnable() {
        return new Runnable() {

            boolean hasNext = true;
            private int currentIndex = 0;
            private long currentRowID;
            private long currentUpperBound;
            private int currentPartitionID;

            @Override
            public void run() {
                updateVariables();
                while (true) {
                    try {
                        long cursor = pubSeq.nextBully();
                        Holder<T> holder = buffer.get(cursor);
                        boolean hadNext = hasNext;
                        if (hadNext) {
                            journal.read(Rows.toRowID(currentPartitionID, currentRowID), holder.object);
                            if (currentRowID < currentUpperBound) {
                                currentRowID++;
                            } else {
                                currentIndex++;
                                updateVariables();
                            }
                        }
                        holder.hasNext = hadNext;
                        pubSeq.done(cursor);

                        if (!hadNext) {
                            break;
                        }
                    } catch (JournalException e) {
                        throw new JournalRuntimeException("Error in iterator [%s]", e, this);
                    }
                }
            }

            private void updateVariables() {
                if (currentIndex < ranges.size()) {
                    JournalIteratorRange w = ranges.getQuick(currentIndex);
                    currentRowID = w.lo;
                    currentUpperBound = w.hi;
                    currentPartitionID = w.partitionID;
                } else {
                    hasNext = false;
                }
            }

        };
    }

    private void start() {
        this.buffer = new RingQueue<>(this, bufferSize);
        this.pubSeq = new SPSequence(bufferSize);
        this.subSeq = new SCSequence();

        this.pubSeq.followedBy(subSeq);
        this.subSeq.followedBy(pubSeq);

        service.submit(getRunnable());
    }

    protected final static class Holder<T> {
        T object;
        boolean hasNext;
    }
}
