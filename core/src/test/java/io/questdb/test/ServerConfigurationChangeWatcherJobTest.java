/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test;

import io.questdb.BuildInformationHolder;
import io.questdb.PropertyKey;
import io.questdb.ReloadingPropServerConfiguration;
import io.questdb.ServerConfigurationChangeWatcherJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Os;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerConfigurationChangeWatcherJobTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    protected static final Log LOG = LogFactory.getLog(ReloadingPropServerConfigurationTest.class);
    @Test
    public void testConfigReload() throws Exception {

        temp.newFolder("conf");
        try (PrintWriter writer = new PrintWriter(temp.newFile("conf/mime.types"), Charset.defaultCharset())) {
            writer.println("");
        }

        try (PrintWriter writer = new PrintWriter(temp.newFile("conf/server.conf"), Charset.defaultCharset())) {
            writer.print(PropertyKey.HTTP_CONNECTION_POOL_INITIAL_CAPACITY);
            writer.print("=");
            writer.print(99);
        }

        ReloadingPropServerConfiguration config = new ReloadingPropServerConfiguration(
            temp.getRoot().getAbsolutePath(), new Properties(), null, ServerConfigurationChangeWatcherJobTest.LOG, new BuildInformationHolder()
        );

        try (ServerConfigurationChangeWatcherJob job = new ServerConfigurationChangeWatcherJob(config)) {
            int concurrencyLevel = 4;
            AtomicInteger values = new AtomicInteger(10);

            CyclicBarrier startBarrier = new CyclicBarrier(concurrencyLevel + 1);
            CyclicBarrier valueBarrier = new CyclicBarrier(concurrencyLevel + 1);
            SOCountDownLatch endLatch = new SOCountDownLatch(concurrencyLevel);

            new Thread(() -> {
                TestUtils.await(startBarrier);
                while (values.get() >= 0) {
                    try (PrintWriter writer = new PrintWriter(Paths.get(temp.getRoot().getAbsolutePath(), "conf/server.conf").toString())) {
                        writer.print(PropertyKey.HTTP_CONNECTION_POOL_INITIAL_CAPACITY);
                        writer.print("=");
                        writer.print(values.get());
                    } catch (IOException e) {
                        // If there is an error in writing the file, end the test
                        endLatch.setCount(0);
                       Assert.fail(e.getMessage());
                    }

                    Assert.assertTrue(job.run(1));
                    values.decrementAndGet();
                    TestUtils.await(valueBarrier);
                }
            }).start();

            for (int i = 0; i < concurrencyLevel; i++) {
                new Thread(() -> {
                    TestUtils.await(startBarrier);
                    try {
                        while (true) {
                            if (values.get() == -1) {
                                break;
                            }
                            if (config.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionPoolInitialCapacity() == values.get()) {
                                TestUtils.await(valueBarrier);
                            }
                        }
                    }
                    finally {
                        endLatch.countDown();
                    }
                }).start();
            }
            endLatch.await();

        }










    }

}