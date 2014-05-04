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

package com.nfsdb.journal.concurrent;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.logging.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PartitionCleaner {

    private static final Logger LOGGER = Logger.getLogger(PartitionCleaner.class);
    private final ExecutorService executor;
    private final RingBuffer<PartitionCleanerEvent> ringBuffer = RingBuffer.createSingleProducer(PartitionCleanerEvent.EVENT_FACTORY, 32, new BlockingWaitStrategy());
    private final BatchEventProcessor<PartitionCleanerEvent> batchEventProcessor;
    private boolean started = false;

    public PartitionCleaner(JournalWriter writer, String name) {
        this.executor = Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory("jj-cleaner-" + name, true));
        this.batchEventProcessor = new BatchEventProcessor<>(ringBuffer, ringBuffer.newBarrier(), new PartitionCleanerEventHandler(writer));
        ringBuffer.addGatingSequences(batchEventProcessor.getSequence());
    }

    public void start() {
        started = true;
        executor.submit(batchEventProcessor);
    }

    public void purge() {
        ringBuffer.publish(ringBuffer.next());
    }

    public void halt() {
        executor.shutdown();

        while (started && !batchEventProcessor.isRunning()) {
            Thread.yield();
        }

        started = false;

        do {
            batchEventProcessor.halt();
        } while (batchEventProcessor.isRunning());

        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.info("Partition cleaner shutdown, but thread is still running");
        }
    }
}
