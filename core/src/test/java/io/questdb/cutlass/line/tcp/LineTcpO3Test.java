package io.questdb.cutlass.line.tcp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.questdb.PropServerConfiguration;
import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.O3PurgeCleaner;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.Net;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUnboundedByteSink;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;

public class LineTcpO3Test extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(LineTcpO3Test.class);
    private LineTcpReceiverConfiguration lineConfiguration;
    private WorkerPoolConfiguration sharedWorkerPoolConfiguration;
    private long resourceAddress;
    private int resourceSize;

    @BeforeClass
    public static void setUpStatic() {
        LOG.info().$("begin").$();
    }

    @AfterClass
    public static void tearDownStatic() {
    }

    @Test
    public void testInOrder() throws Exception {
        test("ilp.inOrder1");
    }

    @Test
    public void testO3() throws Exception {
        test("ilp.outOfOrder1");
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
        try (InputStream is = LineTcpO3Test.class.getResourceAsStream(LineTcpO3Test.class.getSimpleName() + ".server.conf")) {
            File mimeTypesFile = new File(new File(root.toString(), PropServerConfiguration.CONFIG_DIRECTORY), "mime.types");
            if (!mimeTypesFile.exists()) {
                mimeTypesFile.getParentFile().mkdirs();
                FileOutputStream fos = new FileOutputStream(mimeTypesFile);
                fos.write('\n');
                fos.close();
            }
            properties.load(is);
            serverConf = new PropServerConfiguration(root.toString(), properties, null, LOG, null);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        configuration = serverConf.getCairoConfiguration();
        lineConfiguration = serverConf.getLineTcpReceiverConfiguration();
        sharedWorkerPoolConfiguration = serverConf.getWorkerPoolConfiguration();
        engine = new CairoEngine(configuration);
        messageBus = engine.getMessageBus();
        LOG.info().$("setup engine completed").$();
    }

    @Override
    @After
    public void tearDown() {
        engine.close();
        engine = null;
        TestUtils.removeTestPath(root);
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
            resourceAddress = Unsafe.malloc(resourceSize);
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
            long clientFd = Net.socketTcp(true);
            Assert.assertTrue(clientFd >= 0);

            long ilpSockAddr = Net.sockaddr(Net.parseIPv4("127.0.0.1"), lineConfiguration.getNetDispatcherConfiguration().getBindPort());
            WorkerPool sharedWorkerPool = new WorkerPool(sharedWorkerPoolConfiguration);
            try (
                    LineTcpServer ignored = LineTcpServer.create(lineConfiguration, sharedWorkerPool, LOG, engine);
                    SqlCompiler compiler = new SqlCompiler(engine);
                    SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
                    O3PurgeCleaner ignored2 = new O3PurgeCleaner(engine.getMessageBus())) {
                sharedWorkerPool.assignCleaner(Path.CLEANER);
                sharedWorkerPool.start(LOG);
                long rc = Net.connect(clientFd, ilpSockAddr);
                Assert.assertEquals(0, rc);

                readGzResource(ilpResourceName);
                Net.send(clientFd, resourceAddress, resourceSize);
                Unsafe.free(resourceAddress, resourceSize);

                int maxIter = 50;
                while (true) {
                    try {
                        TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "cpu");
                        writer.close();
                        break;
                    } catch (CairoException ex) {
                        maxIter--;
                        Assert.assertTrue(maxIter > 0);
                        Thread.sleep(200);
                    }
                    LOG.info().$("Failed to get writer after ").$(maxIter).$(" iterations").$();
                }

                TestUtils.printSql(compiler, sqlExecutionContext, "select * from " + "cpu", sink);
                // try (OutputStreamWriter oow = new OutputStreamWriter(new FileOutputStream(new File("/tmp/1")))) {
                // oow.append(sink);
                // }
                readGzResource("selectAll1");
                DirectUnboundedByteSink expectedSink = new DirectUnboundedByteSink(resourceAddress);
                expectedSink.clear(resourceSize);
                TestUtils.assertEquals(expectedSink.toString(), sink);
                Unsafe.free(resourceAddress, resourceSize);

                sharedWorkerPool.halt();
            } finally {
                Net.close(clientFd);
                Net.freeSockAddr(ilpSockAddr);
            }
        });

    }
}
