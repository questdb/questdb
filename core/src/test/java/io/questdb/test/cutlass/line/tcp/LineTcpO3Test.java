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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.FreeOnExit;
import io.questdb.Metrics;
import io.questdb.PropServerConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cutlass.line.tcp.LineTcpReceiver;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.Net;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUnboundedByteSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.zip.GZIPInputStream;

@RunWith(Parameterized.class)
public class LineTcpO3Test extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(LineTcpO3Test.class);
    private final boolean walEnabled;
    private final FreeOnExit freeOnExit = new FreeOnExit();
    private LineTcpReceiverConfiguration lineConfiguration;
    private long resourceAddress;
    private int resourceSize;
    private WorkerPoolConfiguration sharedWorkerPoolConfiguration;

    public LineTcpO3Test(WalMode walMode) {
        this.walEnabled = (walMode == WalMode.WITH_WAL);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {WalMode.WITH_WAL}, {WalMode.NO_WAL}
        });
    }

    @BeforeClass
    public static void setUpStatic() {
        LOG.info().$("begin").$();
    }

    @AfterClass
    public static void tearDownStatic() {
    }

    @Override
    @Before
    public void setUp() {
        LOG.info().$("setup engine").$();
        try {
            root = temp.newFolder("dbRoot").getAbsolutePath();
        } catch (IOException e) {
            throw new ExceptionInInitializerError();
        }

        PropServerConfiguration serverConf;
        Properties properties = new Properties();
        String name;
        if (walEnabled) {
            name = LineTcpO3Test.class.getSimpleName() + ".wal.server.conf";
        } else {
            name = LineTcpO3Test.class.getSimpleName() + ".server.conf";
        }
        try (InputStream is = LineTcpO3Test.class.getResourceAsStream(name)) {
            File mimeTypesFile = new File(new File(root, PropServerConfiguration.CONFIG_DIRECTORY), "mime.types");
            if (!mimeTypesFile.exists()) {
                mimeTypesFile.getParentFile().mkdirs();
                FileOutputStream fos = new FileOutputStream(mimeTypesFile);
                fos.write('\n');
                fos.close();
            }
            properties.load(is);
            serverConf = new PropServerConfiguration(root, properties, null, LOG, null);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        configuration = serverConf.getCairoConfiguration();
        lineConfiguration = serverConf.getLineTcpReceiverConfiguration();
        sharedWorkerPoolConfiguration = serverConf.getWorkerPoolConfiguration();
        metrics = Metrics.enabled();
        engine = new CairoEngine(configuration, metrics);
        serverConf.init(
                engine,
                new FunctionFactoryCache(
                        configuration,
                        ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())
                ),
                freeOnExit
        );
        messageBus = engine.getMessageBus();
        LOG.info().$("setup engine completed").$();
    }

    @Override
    @After
    public void tearDown() {
        freeOnExit.close();
        engine = Misc.free(engine);
        TestUtils.removeTestPath(root);
        freeOnExit.close();
    }

    @Test
    public void testInOrder() throws Exception {
        test("ilp.inOrder1");
    }

    @Test
    public void testO3() throws Exception {
        test("ilp.outOfOrder1");
    }

    private void mayDrainWalQueue() {
        if (walEnabled) {
            drainWalQueue();
        }
    }

    private void readGzResource(String rname) {
        int resourceNLines;
        try (InputStream is = new GZIPInputStream(getClass().getResourceAsStream(getClass().getSimpleName() + "." + rname + ".gz"))) {
            final int bufSz = 10_000_000;
            byte[] bytes = new byte[bufSz];
            resourceSize = 0;
            while (true) {
                int off = resourceSize;
                int len = bytes.length - off;
                int rc = is.read(bytes, off, len);
                if (rc > 0) {
                    resourceSize += rc;
                    if (resourceSize >= bytes.length) {
                        byte[] newBytes = new byte[bytes.length + bufSz];
                        System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
                        bytes = newBytes;
                    }
                    continue;
                }
                break;
            }
            resourceAddress = Unsafe.malloc(resourceSize, MemoryTag.NATIVE_DEFAULT);
            resourceNLines = 0;
            for (int i = 0; i < resourceSize; i++) {
                byte b = bytes[i];
                Unsafe.getUnsafe().putByte(resourceAddress + i, b);
                if (b == '\n') {
                    resourceNLines++;
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        LOG.info().$("read ").$(rname).$(", found ").$(resourceNLines).$(" lines in ").$(resourceSize).$(" bytes").$();
    }

    private void test(String ilpResourceName) throws Exception {
        assertMemoryLeak(() -> {
            int clientFd = Net.socketTcp(true);
            Assert.assertTrue(clientFd >= 0);

            long ilpSockAddr = Net.sockaddr(Net.parseIPv4("127.0.0.1"), lineConfiguration.getDispatcherConfiguration().getBindPort());
            WorkerPool sharedWorkerPool = new WorkerPool(sharedWorkerPoolConfiguration, metrics.health());
            try (
                    LineTcpReceiver ignored = new LineTcpReceiver(lineConfiguration, engine, sharedWorkerPool, sharedWorkerPool);
                    SqlCompiler compiler = new SqlCompiler(engine);
                    SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)
            ) {
                SOCountDownLatch haltLatch = new SOCountDownLatch(1);
                engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                    if (PoolListener.isWalOrWriter(factoryType)) {
                        if (event == PoolListener.EV_RETURN && Chars.equalsNc(name.getTableName(), "cpu")) {
                            haltLatch.countDown();
                        }
                    }
                });

                sharedWorkerPool.start(LOG);
                TestUtils.assertConnect(clientFd, ilpSockAddr);
                readGzResource(ilpResourceName);
                Net.send(clientFd, resourceAddress, resourceSize);
                Unsafe.free(resourceAddress, resourceSize, MemoryTag.NATIVE_DEFAULT);

                haltLatch.await();
                // stop pool twice and this is ok
                sharedWorkerPool.halt();
                mayDrainWalQueue();

                Assert.assertEquals(walEnabled, isWalTable("cpu"));
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from cpu", sink);
                readGzResource("selectAll1");
                DirectUnboundedByteSink expectedSink = new DirectUnboundedByteSink(resourceAddress);
                expectedSink.clear(resourceSize);
                TestUtils.assertEquals(expectedSink.toString(), sink);
                Unsafe.free(resourceAddress, resourceSize, MemoryTag.NATIVE_DEFAULT);
            } finally {
                engine.clear();
                Net.close(clientFd);
                Net.freeSockAddr(ilpSockAddr);
                sharedWorkerPool.halt();
            }
        });
    }
}
