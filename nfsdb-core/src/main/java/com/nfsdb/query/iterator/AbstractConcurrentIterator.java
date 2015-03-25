/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.query.iterator;

import com.lmax.disruptor.*;
import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.utils.NamedDaemonThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class AbstractConcurrentIterator<T> extends AbstractImmutableIterator<T> implements EventFactory<AbstractConcurrentIterator.Holder<T>>, ConcurrentIterator<T> {
    private final ExecutorService service;
    RingBuffer<Holder<T>> buffer;
    SequenceBarrier barrier;
    private int bufferSize;
    private Sequence sequence;
    private long nextSequence;
    private long availableSequence;
    private boolean started = false;

    AbstractConcurrentIterator(int bufferSize) {
        this.bufferSize = bufferSize;
        this.service = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("nfsdb-iterator", false));
    }

    @Override
    public ConcurrentIterator<T> buffer(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    @Override
    public void close() {
        service.shutdown();
        barrier.alert();
    }

    @Override
    public boolean hasNext() {

        if (!started) {
            start();
            started = true;
        }

        if (availableSequence >= nextSequence) {
            return buffer.get(nextSequence).hasNext;
        }
        try {
            availableSequence = barrier.waitFor(nextSequence);
            return availableSequence >= nextSequence && buffer.get(nextSequence).hasNext;
        } catch (AlertException | TimeoutException | InterruptedException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public AbstractConcurrentIterator.Holder<T> newInstance() {
        Holder<T> h = new Holder<>();
        h.object = getJournal().newObject();
        h.hasNext = true;
        return h;
    }

    @Override
    public T next() {
        hasNext();
        T result = null;

        if (availableSequence >= nextSequence) {
            result = buffer.get(nextSequence).object;
            nextSequence++;
        }
        sequence.set(nextSequence - 2);

        return result;
    }

    protected abstract Runnable getRunnable();

    private void start() {
        this.buffer = RingBuffer.createSingleProducer(this, bufferSize, new BusySpinWaitStrategy());
        this.barrier = buffer.newBarrier();
        this.sequence = new Sequence(barrier.getCursor());
        this.nextSequence = sequence.get() + 1L;
        this.availableSequence = -1L;
        this.buffer.addGatingSequences(sequence);
        service.submit(getRunnable());
    }

    protected final static class Holder<T> {
        T object;
        boolean hasNext;
    }
}
