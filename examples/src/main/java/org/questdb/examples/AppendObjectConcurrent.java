/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2017 Appsicle
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package org.questdb.examples;

import com.questdb.JournalWriter;
import com.questdb.ex.JournalException;
import com.questdb.factory.Factory;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Rnd;
import com.questdb.mp.MPSequence;
import com.questdb.mp.RingQueue;
import com.questdb.mp.SCSequence;
import com.questdb.std.ObjectFactory;
import org.questdb.examples.support.ModelConfiguration;
import org.questdb.examples.support.Quote;

import java.util.concurrent.locks.LockSupport;

public class AppendObjectConcurrent {

    private static final Log LOG = LogFactory.getLog(AppendObjectConcurrent.class);

    /**
     * Multiple threads append to same journal concurrently.
     * <p>
     * Single append thread receives data from multiple publishers via circular queue using QuestDB's thread messaging mechanism.
     * Please refer to my blog post: http://blog.questdb.org/2016/08/the-art-of-thread-messaging.html for more details.
     */
    public static void main(String[] args) throws JournalException, InterruptedException {

        if (args.length != 1) {
            System.out.println("Usage: " + AppendObjectConcurrent.class.getName() + " <path>");
            System.exit(1);
        }

        //
        RingQueue<Quote> queue = new RingQueue<>(new ObjectFactory<Quote>() {
            @Override
            public Quote newInstance() {
                return new Quote();
            }
        }, 4096);

        // publisher sequence
        final MPSequence pubSequence = new MPSequence(queue.getCapacity());
        final SCSequence subSequence = new SCSequence();

        // create circular dependency between sequences
        pubSequence.then(subSequence).then(pubSequence);

        // run configuration
        int nThreads = 2;
        int nMessages = 1000000;

        JournalConfiguration configuration = ModelConfiguration.CONFIG.build(args[0]);
        try (Factory factory = new Factory(configuration, 1000, 1, 0)) {

            // start publishing threads
            for (int i = 0; i < nThreads; i++) {
                new Thread(new Publisher(queue, pubSequence, nMessages)).start();
            }

            // consume messages in main thread
            int count = 0;
            int deadline = nMessages * nThreads;

            try (JournalWriter<Quote> writer = factory.writer(Quote.class)) {
                while (count < deadline) {
                    long cursor = subSequence.next();
                    if (cursor < 0) {
                        LockSupport.parkNanos(1);
                        continue;
                    }

                    long available = subSequence.available();
                    while (cursor < available) {
                        Quote q = queue.get(cursor++);
                        q.setTimestamp(System.currentTimeMillis());
                        try {
                            writer.append(q);
                        } catch (JournalException e) {
                            // append may fail, log and continue

                            // N.B. this logging uses builder pattern to construct message. Building finishes with $() call.
                            LOG.error().$("Sequence failed: ").$(cursor - 1).$(e).$();
                        }
                        count++;
                    }
                    subSequence.done(available - 1);
                    try {
                        writer.commit();
                    } catch (JournalException e) {
                        // something serious, attempt to rollback
                        LOG.error().$("Batch commit() failed [").$(available - 1).$(']').$(e).$();
                    }
                }
            }
            LOG.info().$("Writer done").$();
        }
    }

    private static class Publisher implements Runnable {
        private final RingQueue<Quote> queue;
        private final MPSequence sequence;
        private final int nMessages;

        public Publisher(RingQueue<Quote> queue, MPSequence pubSequence, int nMessages) {
            this.queue = queue;
            this.sequence = pubSequence;
            this.nMessages = nMessages;
        }

        @Override
        public void run() {
            final String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
            final Rnd r = new Rnd();
            int count = 0;
            while (count < nMessages) {
                long cursor = sequence.next();
                if (cursor < 0) {
                    LockSupport.parkNanos(1);
                    continue;
                }
                try {
                    Quote q = queue.get(cursor);
                    // generate some data
                    String sym = symbols[Math.abs(r.nextInt() % (symbols.length - 1))];
                    q.setSym(sym);
                    q.setAsk(Math.abs(r.nextDouble()));
                    q.setBid(Math.abs(r.nextDouble()));
                    q.setAskSize(Math.abs(r.nextInt() % 10000));
                    q.setBidSize(Math.abs(r.nextInt() % 10000));
                    q.setEx("LXE");
                    q.setMode("Fast trading");
                } finally {
                    sequence.done(cursor);
                    count++;
                }
            }
            LOG.info().$("Publisher done").$();
        }
    }
}
