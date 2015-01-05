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

package org.nfsdb.examples.messaging;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiMarketTickPersistenceMain {

    // ring buffer size 128k
    public static final int BUFFER_SIZE = 1024 * 128;

    // message queue
    private final RingBuffer<Tick> ringBuffer;
    // message persistence
    private final BatchEventProcessor<Tick> storeProcessor;
    private final BatchEventProcessor<Tick> vwapProcessor;
    // thread pool
    private final ExecutorService service;
    // persister would count down this latch when it's received end of marker messages.
    private final CountDownLatch latch;
    private final int instrumentCount;
    private final int marketCount;
    private final int messageCount;

    public MultiMarketTickPersistenceMain(JournalFactory factory, int instrumentCount, int marketCount, int messageCount) {
        this.instrumentCount = instrumentCount;
        this.marketCount = marketCount;
        this.messageCount = messageCount;
        this.latch = new CountDownLatch(marketCount);
        this.service = Executors.newCachedThreadPool();
        this.ringBuffer = RingBuffer.createMultiProducer(Tick.EVENT_FACTORY, BUFFER_SIZE, new BusySpinWaitStrategy());
        SequenceBarrier barrier = this.ringBuffer.newBarrier();
        this.storeProcessor = new BatchEventProcessor<>(this.ringBuffer, barrier, new TickStore(factory, latch));
        this.vwapProcessor = new BatchEventProcessor<>(this.ringBuffer, barrier, new TickAvgPrice(instrumentCount));
        // prevent buffer wrap without messages being handled.
        this.ringBuffer.addGatingSequences(storeProcessor.getSequence(), vwapProcessor.getSequence());
    }

    public static void main(String[] args) throws JournalException, InterruptedException {
        if (args.length != 1) {
            System.out.println("Usage: " + MultiMarketTickPersistenceMain.class.getName() + " <path>");
            System.exit(1);
        }

        MultiMarketTickPersistenceMain main = new MultiMarketTickPersistenceMain(
                new JournalFactory(
                        new JournalConfigurationBuilder() {{
                            $(Tick.class)
                                    .recordCountHint(10000000)
                                    .$ts()
                            ;
                        }}.build(args[0])
                )
                , 100, 2, 200000000
        );
        long t = System.currentTimeMillis();
        main.execute();
        System.out.println(System.currentTimeMillis() - t + "ms");
    }

    public void execute() throws InterruptedException {
        this.service.submit(storeProcessor);
        this.service.submit(vwapProcessor);
        for (int i = 0; i < marketCount; i++) {
            this.service.submit(new TickMarket(ringBuffer, instrumentCount, messageCount / marketCount));
        }
        latch.await();
        storeProcessor.halt();
        vwapProcessor.halt();
        service.shutdown();
    }
}
