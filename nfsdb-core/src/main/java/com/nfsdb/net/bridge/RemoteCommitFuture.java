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

package com.nfsdb.net.bridge;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.tx.TxFuture;

import java.util.concurrent.TimeUnit;

public class RemoteCommitFuture implements TxFuture {

    private final SequenceBarrier barrier;
    private final Sequence sequence;
    private final RingBuffer<JournalEvent> ringBuffer;
    private final long timestamp;
    private final int journalIndex;
    private long nextSequence;

    public RemoteCommitFuture(RingBuffer<JournalEvent> ringBuffer, SequenceBarrier barrier, int journalIndex, long timestamp) {
        this.ringBuffer = ringBuffer;
        this.barrier = barrier;
        this.sequence = new Sequence(barrier.getCursor());
        this.nextSequence = sequence.get() + 1L;
        this.timestamp = timestamp;
        this.journalIndex = journalIndex;
        this.ringBuffer.addGatingSequences(sequence);
    }


    public boolean waitFor(long timeout, TimeUnit unit) {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        this.barrier.clearAlert();
        try {
            do {
                try {
                    long availableSequence = this.barrier.waitFor(nextSequence);
                    try {
                        while (nextSequence <= availableSequence) {
                            JournalEvent event = ringBuffer.get(nextSequence++);
                            if (timestamp == event.getTimestamp() && journalIndex == event.getIndex()) {
                                ringBuffer.removeGatingSequence(sequence);
                                return true;
                            }
                        }
                        sequence.set(availableSequence);
                    } catch (final Throwable e) {
                        sequence.set(nextSequence);
                        nextSequence++;
                    }
                } catch (com.lmax.disruptor.TimeoutException e) {
                    // do nothing
                } catch (AlertException | InterruptedException e) {
                    throw new JournalRuntimeException(e);
                }
            }
            while (System.nanoTime() < deadline);

        } finally {
            ringBuffer.removeGatingSequence(sequence);
        }
        return false;
    }
}
