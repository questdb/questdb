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

package com.questdb.store.query.iter;

import com.questdb.mp.RingQueue;
import com.questdb.mp.SCSequence;
import com.questdb.mp.SPSequence;
import com.questdb.mp.Sequence;
import com.questdb.std.NamedDaemonThreadFactory;
import com.questdb.std.ObjList;
import com.questdb.std.ObjectFactory;
import com.questdb.std.Rows;
import com.questdb.std.ex.JournalException;
import com.questdb.store.Journal;
import com.questdb.store.JournalRuntimeException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JournalConcurrentIterator<T>
        implements ObjectFactory<JournalConcurrentIterator.Holder<T>>, ConcurrentIterator<T>, com.questdb.std.ImmutableIterator<T> {
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
        this.service = Executors.newSingleThreadExecutor(new NamedDaemonThreadFactory("questdb-iterator", false));
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
        this.pubSeq.then(subSeq).then(pubSeq);
        service.submit(getRunnable());
    }

    protected final static class Holder<T> {
        T object;
        boolean hasNext;
    }
}
