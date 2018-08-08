/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2018 Appsicle
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

package org.questdb.examples.replication.cluster;

import com.questdb.net.ha.ClusterController;
import com.questdb.net.ha.ClusterStatusListener;
import com.questdb.net.ha.config.ClientConfig;
import com.questdb.net.ha.config.ServerConfig;
import com.questdb.net.ha.config.ServerNode;
import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.std.ex.JournalException;
import com.questdb.std.ex.JournalNetworkException;
import com.questdb.store.JournalKey;
import com.questdb.store.JournalWriter;
import com.questdb.store.PartitionBy;
import com.questdb.store.factory.Factory;
import com.questdb.store.factory.configuration.JournalConfiguration;
import com.questdb.store.factory.configuration.JournalConfigurationBuilder;
import org.questdb.examples.support.Price;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClusteredProducerMain {

    public static void main(String[] args) throws JournalException, JournalNetworkException, NumericException {

        final String pathToDatabase = args[0];
        final int instance = Numbers.parseInt(args[1]);
        final JournalConfiguration configuration = new JournalConfigurationBuilder() {{
            $(Price.class).$ts();
        }}.build(pathToDatabase);

        final Factory factory = new Factory(configuration, 1000, 1, 0);

        final JournalWriter<Price> writer = factory.writer(new JournalKey<>(Price.class, null, PartitionBy.DEFAULT, 1000000000));

        final WorkerController wc = new WorkerController(writer);

        final ClusterController cc = new ClusterController(
                new ServerConfig() {{
                    addNode(new ServerNode(1, "127.0.0.1:7080"));
                    addNode(new ServerNode(2, "127.0.0.1:7090"));
                }},
                new ClientConfig(),
                factory,
                instance,
                new ArrayList<JournalWriter>() {{
                    add(writer);
                }},
                wc
        );

        cc.start();

        Runtime.getRuntime().addShutdownHook(new Thread(cc::halt));
    }

    /**
     * Controller listens to cluster state and performs fail over of work.
     * In this case by starting worker thread when node is activated.
     */
    public static class WorkerController implements ClusterStatusListener {

        private final JournalWriter<Price> writer;
        private Worker worker;

        public WorkerController(JournalWriter<Price> writer) {
            this.writer = writer;
        }

        @Override
        public void goActive() {
            System.out.println("This node is active");
            (worker = new Worker(writer)).start();
        }

        @Override
        public void goPassive(ServerNode activeNode) {
            System.out.println("This node is standing by");
            stopWorker();
        }

        @Override
        public void onShutdown() {
            stopWorker();
            writer.close();
        }

        private void stopWorker() {
            if (worker != null) {
                worker.halt();
                worker = null;
            }
        }
    }

    public static class Worker {
        private final JournalWriter<Price> writer;
        private final Price p = new Price();
        private final CountDownLatch breakLatch = new CountDownLatch(1);
        private final CountDownLatch haltLatch = new CountDownLatch(1);

        public Worker(JournalWriter<Price> writer) {
            this.writer = writer;
        }

        public void halt() {
            try {
                breakLatch.countDown();
                haltLatch.await();
            } catch (InterruptedException ignore) {
            }
        }

        public void start() {
            new Thread(() -> {
                try {
                    long t = writer.getMaxTimestamp();
                    if (t == 0) {
                        System.currentTimeMillis();
                    }

                    while (true) {
                        for (int i = 0; i < 50000; i++) {
                            p.setTimestamp(t += i);
                            p.setNanos(System.currentTimeMillis());
                            p.setSym(String.valueOf(i % 20));
                            p.setPrice(i * 1.04598 + i);
                            writer.append(p);
                        }
                        writer.commit();

                        if (breakLatch.await(2, TimeUnit.SECONDS)) {
                            break;
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    haltLatch.countDown();
                }
            }).start();
        }
    }
}
