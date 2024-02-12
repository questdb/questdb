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

import io.questdb.*;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerConfigurationChangeWatcherJobTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    protected static final Log LOG = LogFactory.getLog(ServerConfigurationChangeWatcherJob.class);

    @Test
    public void testConfigReload() throws Exception {
        temp.newFolder("conf");
        try (PrintWriter writer = new PrintWriter(temp.newFile("conf/mime.types"), Charset.defaultCharset())) {
            writer.println("");
        }

        try (PrintWriter writer = new PrintWriter(temp.newFile("conf/server.conf"), Charset.defaultCharset())) {
            writer.println("");
        }

        Bootstrap bootstrap = new Bootstrap("-d",temp.getRoot().getAbsolutePath());
        ServerMain server = new ServerMain(bootstrap);
        ServerConfigurationChangeWatcherJob job = new ServerConfigurationChangeWatcherJob(
                bootstrap, server, false
        );

        job.run(1);

        Assert.assertTrue(server.getConfiguration().getHttpServerConfiguration().isEnabled());

        Path serverConfPath = Path.of(temp.getRoot().getAbsolutePath(), "conf", "server.conf");
        try (PrintWriter writer = new PrintWriter(serverConfPath.toString(), Charset.defaultCharset())) {
            writer.print(PropertyKey.HTTP_ENABLED);
            writer.print("=");
            writer.print(false);
        }

        job.run(1);

        Assert.assertFalse(server.getConfiguration().getHttpServerConfiguration().isEnabled());

    }
}
/*




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

        package io.questdb.test;

import io.questdb.*;
import io.questdb.cutlass.json.JsonException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.test.tools.TestUtils;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class ReloadingPropServerConfigurationTest {
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    protected static final Log LOG = LogFactory.getLog(ReloadingPropServerConfigurationTest.class);
    protected static String root;

    @AfterClass
    public static void afterClass() {
        TestUtils.removeTestPath(root);
    }

    @BeforeClass
    public static void setupMimeTypes() throws IOException {
        File root = new File(temp.getRoot(), "root");
        TestUtils.copyMimeTypes(root.getAbsolutePath());
        ReloadingPropServerConfigurationTest.root = root.getAbsolutePath();
    }


    @Test
    public void testSimpleReload() throws Exception {
        Properties properties = new Properties();
        ReloadingPropServerConfiguration configuration = newReloadingPropServerConfiguration(root, properties, null, new BuildInformationHolder());
        Assert.assertEquals(4, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionPoolInitialCapacity());

        properties = new Properties();
        properties.setProperty(String.valueOf(PropertyKey.HTTP_CONNECTION_POOL_INITIAL_CAPACITY), "99");
        Assert.assertEquals("99", properties.getProperty(String.valueOf(PropertyKey.HTTP_CONNECTION_POOL_INITIAL_CAPACITY)));
        configuration.reload(properties, null);
        Assert.assertEquals(99, configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionPoolInitialCapacity());
    }

    @Test
    public void testConcurrentReload() throws Exception {
        int concurrencyLevel = 4;
        int numValues = 10;
        AtomicInteger values = new AtomicInteger(numValues);

        Properties properties = new Properties();
        properties.setProperty(String.valueOf(PropertyKey.HTTP_CONNECTION_POOL_INITIAL_CAPACITY), "99");
        ReloadingPropServerConfiguration configuration = newReloadingPropServerConfiguration(root, properties, null, new BuildInformationHolder());

        CyclicBarrier startBarrier = new CyclicBarrier(concurrencyLevel);
        CyclicBarrier valueBarrier = new CyclicBarrier(concurrencyLevel + 1);
        CyclicBarrier decrementBarrier = new CyclicBarrier(concurrencyLevel + 1);
        SOCountDownLatch endLatch = new SOCountDownLatch(concurrencyLevel);

        new Thread(() -> {
            while (values.get() >= 0) {
                Properties newProperties = new Properties();
                newProperties.setProperty(String.valueOf(PropertyKey.HTTP_CONNECTION_POOL_INITIAL_CAPACITY), Integer.toString(values.get()));
                Assert.assertNotEquals(newProperties, properties);
                configuration.reload(newProperties, null);
                TestUtils.await(valueBarrier);
                values.decrementAndGet();
                TestUtils.await(decrementBarrier);
            }
        }).start();

        for (int i = 0; i < concurrencyLevel; i++) {
            new Thread(() -> {
                TestUtils.await(startBarrier);
                try {
                    while (true) {
                        if (configuration.getHttpServerConfiguration().getHttpContextConfiguration().getConnectionPoolInitialCapacity() == values.get()) {
                            TestUtils.await(valueBarrier);
                            TestUtils.await(decrementBarrier);
                        }
                        if (values.get() == -1) {
                            break;
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

    @NotNull
    protected ReloadingPropServerConfiguration newReloadingPropServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            BuildInformation buildInformation
    ) throws ServerConfigurationException, JsonException {
        return new ReloadingPropServerConfiguration(root, properties, env, ReloadingPropServerConfigurationTest.LOG, buildInformation);
    }

}

*/