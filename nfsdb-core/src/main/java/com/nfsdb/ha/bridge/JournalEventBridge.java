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

package com.nfsdb.ha.bridge;

import com.lmax.disruptor.*;
import com.nfsdb.utils.NamedDaemonThreadFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *                                                             +---------+
 *                                                       +---> | AGENT 1 |----+          +--------------+
 * +----------+                                          |     +---------+    |     +--> | writer 1 ack |
 * | writer 1 +----+                                     |                    |     |    +--------------+
 * +----------+    |      +-------+        +--------+    |     +---------+    |     |
 *                 +----> | IN_RB +------> | OUT RB +----+---> | AGENT 2 |----+-----+
 *                 |      +-------+        +--------+    |     +---------+    |     |
 * +----------+    |                                     |                    |     |    +--------------+
 * | writer 2 +----+                                     |     +---------+    |     +--> | writer 2 ack |
 * +----------+                                          +---> | AGENT 3 |----+          +--------------+
 *                                                             +---------+
 * </pre>
 */
public class JournalEventBridge {

    private static final int BUFFER_SIZE = 1024;
    private final ExecutorService executor = Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory("nfsdb-evt-bridge", false));
    private final RingBuffer<JournalEvent> inRingBuffer;
    private final BatchEventProcessor<JournalEvent> batchEventProcessor;
    private final RingBuffer<JournalEvent> outRingBuffer;
    private final SequenceBarrier outBarrier;

    @SuppressWarnings("CanBeFinal")


    /**
     * Many-to-many bridge between multiple journals publishing their commits and multiple subscribers consuming them.
     * These subscribers are typically {@link com.nfsdb.ha.JournalServerAgent} instances.
     * Disruptor library doesn't provide many-to-many ring buffer out-of-box, so this implementation employs
     * two many-to-one ring buffers, <i>in</i> and <i>out</i>. Single thread is the sole consumer on the <b>in</b> buffer and
     * sole publisher on the <i>out</i> ring buffer.
     * <p>Out ring buffer has timeout blocking wait strategy to enable {@link com.nfsdb.ha.JournalServerAgent} to send
     * heartbeat message to client. Value of {@code timeout} is heartbeat frequency between server agent and client.
     *
     * @param timeout         for out buffer wait.
     * @param unit            time unit for timeout value
     */
    public JournalEventBridge(long timeout, TimeUnit unit) {
        this.inRingBuffer = RingBuffer.createMultiProducer(JournalEvent.EVENT_FACTORY, BUFFER_SIZE, new BlockingWaitStrategy());
        this.outRingBuffer = RingBuffer.createSingleProducer(JournalEvent.EVENT_FACTORY, BUFFER_SIZE, new com.nfsdb.ha.bridge.TimeoutBlockingWaitStrategy(timeout, unit));
        this.outBarrier = outRingBuffer.newBarrier();
        this.batchEventProcessor = new BatchEventProcessor<>(inRingBuffer, inRingBuffer.newBarrier(), new EventHandler<JournalEvent>() {
            @Override
            public void onEvent(JournalEvent event, long sequence, boolean endOfBatch) throws Exception {
                long outSeq = outRingBuffer.next();
                JournalEvent outEvent = outRingBuffer.get(outSeq);
                outEvent.setIndex(event.getIndex());
                outEvent.setTimestamp(event.getTimestamp());
                outRingBuffer.publish(outSeq);
            }
        });
        inRingBuffer.addGatingSequences(batchEventProcessor.getSequence());
    }

    public Sequence createAgentSequence() {
        Sequence sequence = new Sequence(outBarrier.getCursor());
        outRingBuffer.addGatingSequences(sequence);
        return sequence;
    }

    public SequenceBarrier getOutBarrier() {
        return outBarrier;
    }

    public RingBuffer<JournalEvent> getOutRingBuffer() {
        return outRingBuffer;
    }

    @SuppressFBWarnings({"MDM_THREAD_YIELD"})
    public void halt() {
        executor.shutdown();
        while (inRingBuffer.getCursor() < inRingBuffer.getMinimumGatingSequence()) {
            Thread.yield();
        }
        while (batchEventProcessor.isRunning()) {
            batchEventProcessor.halt();
        }
    }

    public void publish(final int journalIndex, final long timestamp) {
        long sequence = inRingBuffer.next();
        JournalEvent event = inRingBuffer.get(sequence);
        event.setIndex(journalIndex);
        event.setTimestamp(timestamp);
        inRingBuffer.publish(sequence);
    }

    public void removeAgentSequence(Sequence sequence) {
        outRingBuffer.removeGatingSequence(sequence);
    }

    public void start() {
        executor.submit(batchEventProcessor);
    }
}
