/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.net.bridge;

import com.lmax.disruptor.*;
import com.nfsdb.journal.exceptions.JournalRuntimeException;

public class JournalEventProcessor {
    private final SequenceBarrier barrier;
    private final Sequence sequence;
    private final RingBuffer<JournalEvent> ringBuffer;
    private long nextSequence;

    public JournalEventProcessor(JournalEventBridge bridge) {
        this(bridge.getOutRingBuffer(), bridge.createAgentSequence(), bridge.getOutBarrier());
    }

    public boolean process(JournalEventHandler handler, boolean blocking) {
        this.barrier.clearAlert();
        try {
            long availableSequence = blocking ? this.barrier.waitFor(nextSequence) : this.barrier.getCursor();
            try {
                while (nextSequence <= availableSequence) {
                    JournalEvent event = ringBuffer.get(nextSequence);
                    handler.handle(event);
                    nextSequence++;
                }
                sequence.set(availableSequence);
            } catch (final Throwable e) {
                sequence.set(nextSequence);
                nextSequence++;
            }
            return true;
        } catch (InterruptedException | AlertException e) {
            throw new JournalRuntimeException(e);
        } catch (TimeoutException e) {
            return false;
        }
    }

    public Sequence getSequence() {
        return sequence;
    }

    private JournalEventProcessor(RingBuffer<JournalEvent> ringBuffer, Sequence sequence, SequenceBarrier barrier) {
        this.ringBuffer = ringBuffer;
        this.barrier = barrier;
        this.sequence = sequence;
        this.nextSequence = sequence.get() + 1L;
    }
}
