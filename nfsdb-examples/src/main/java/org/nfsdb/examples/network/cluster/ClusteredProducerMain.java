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

package org.nfsdb.examples.network.cluster;

import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.net.cluster.ClusterController;
import com.nfsdb.net.cluster.ClusterNode;
import com.nfsdb.net.cluster.ClusterStatusListener;
import com.nfsdb.utils.Numbers;
import org.nfsdb.examples.model.Price;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClusteredProducerMain {

    public static void main(String[] args) throws JournalException {

        final String pathToDatabase = args[0];
        final int instance = Numbers.parseInt(args[1]);
        final JournalFactory factory = new JournalFactory(new JournalConfigurationBuilder() {{
            $(Price.class).$ts();
        }}.build(pathToDatabase));

        final JournalWriter<Price> writer = factory.writer(Price.class);
        final WorkerController wc = new WorkerController(writer);

        final ClusterController cc = new ClusterController(
                new ArrayList<ClusterNode>() {{
                    add(new ClusterNode(1, "localhost:7080"));
                    add(new ClusterNode(2, "localhost:7090"));
                }}
                , instance
                , wc
                , factory
                ,
                new ArrayList<JournalWriter>() {{
                    add(writer);
                }}
        );

        cc.start();
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
        public void onNodeActive() {
            System.out.println("This node is active");
            (worker = new Worker(writer)).start();
        }

        private void stopWorker() {
            if (worker != null) {
                worker.halt();
                worker = null;
            }
        }

        @Override
        public void onNodeStandingBy(ClusterNode activeNode) {
            System.out.println("This node is standing by");
            stopWorker();
        }

        @Override
        public void onShutdown() {
            stopWorker();
            writer.close();
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

        public void start() {
            new Thread() {
                @Override
                public void run() {
                    try {
                        long t = writer.getMaxTimestamp();
                        if (t == 0) {
                            System.currentTimeMillis();
                        }

                        while (true) {
                            for (int i = 0; i < 50000; i++) {
                                p.setTimestamp(t += i);
                                p.setNanos(System.nanoTime());
                                p.setSym(String.valueOf(i % 20));
                                p.setPrice(i * 1.04598 + i);
                                writer.append(p);
                            }
                            writer.commit();

                            breakLatch.await(2, TimeUnit.SECONDS);
                            if (breakLatch.getCount() == 0) {
                                break;
                            }

                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        haltLatch.countDown();
                    }
                }
            }.start();
        }

        public void halt() {
            try {
                breakLatch.countDown();
                haltLatch.await();
            } catch (InterruptedException ignore) {
            }
        }
    }
}
