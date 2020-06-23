package io.questdb.cutlass.line.tcp;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;

public class LineTcpConnectionContextTest extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(LineTcpConnectionContextTest.class);
    private static final int FD = 1_000_000;
    private LineTcpConnectionContext context;
    private LineTcpReceiverConfiguration lineTcpConfiguration;
    private LineTcpMeasurementScheduler scheduler;
    private boolean disconnected;
    private String recvBuffer;

    @Test
    public void testSingleMeasurement() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            context.handleIO();
            Assert.assertFalse(disconnected);
            recvBuffer = null;
            context.handleIO();
            Assert.assertTrue(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testMultipleMeasurements() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            context.handleIO();
            Assert.assertFalse(disconnected);
            recvBuffer = null;
            context.handleIO();
            Assert.assertTrue(disconnected);
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testEmptyLine() throws Exception {
        runInContext(() -> {
            recvBuffer = "\n";
            context.handleIO();
            Assert.assertFalse(disconnected);
        });
    }

    @Test
    public void testOverflow() throws Exception {
        runInContext(() -> {
            int msgBufferSize = lineTcpConfiguration.getMsgBufferSize();
            recvBuffer = "A";
            while (recvBuffer.length() <= msgBufferSize) {
                recvBuffer += recvBuffer;
            }
            int nUnread = recvBuffer.length() - msgBufferSize;
            context.handleIO();
            Assert.assertTrue(disconnected);
            Assert.assertEquals(nUnread, recvBuffer.length());
        });
    }

    private void runInContext(Runnable r) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration, null)) {
                setupContext(engine);
                try {
                    r.run();
                } finally {
                    closeContext();
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                }
            }
        });
    }

    private void assertTable(CharSequence expected, CharSequence tableName) {
        try (TableReader reader = new TableReader(configuration, tableName)) {
            assertThat(expected, reader.getCursor(), reader.getMetadata(), true);
        }
    }

    @Before
    public void before() {
        NetworkFacade nf = new NetworkFacadeImpl() {
            @Override
            public int recv(long fd, long buffer, int bufferLen) {
                Assert.assertEquals(FD, fd);
                if (null == recvBuffer) {
                    return -1;
                }

                byte[] bytes = recvBuffer.getBytes(StandardCharsets.UTF_8);
                int n = 0;
                while (n < bufferLen && n < bytes.length) {
                    Unsafe.getUnsafe().putByte(buffer++, bytes[n++]);
                }
                recvBuffer = new String(bytes, n, bytes.length - n);
                return n;
            }
        };
        lineTcpConfiguration = new DefaultLineTcpReceiverConfiguration() {
            @Override
            public NetworkFacade getNetworkFacade() {
                return nf;
            }

            @Override
            public int getMsgBufferSize() {
                return 512;
            }

            @Override
            public int getMaxMeasurementSize() {
                return 128;
            }
        };
    }

    private void setupContext(CairoEngine engine) {
        scheduler = new LineTcpMeasurementScheduler(configuration, lineTcpConfiguration, engine);
        context = new LineTcpConnectionContext(lineTcpConfiguration, scheduler);
        disconnected = false;
        recvBuffer = null;
        IODispatcher<LineTcpConnectionContext> dispatcher = new IODispatcher<LineTcpConnectionContext>() {
            @Override
            public void close() throws IOException {
            }

            @Override
            public boolean run(int workerId) {
                return false;
            }

            @Override
            public int getConnectionCount() {
                return 0;
            }

            @Override
            public void registerChannel(LineTcpConnectionContext context, int operation) {
            }

            @Override
            public boolean processIOQueue(IORequestProcessor<LineTcpConnectionContext> processor) {
                return false;
            }

            @Override
            public void disconnect(LineTcpConnectionContext context) {
                disconnected = true;
            }
        };
        context.of(FD, dispatcher);
    }

    private void closeContext() {
        if (null != scheduler) {
            scheduler.waitUntilClosed();
            scheduler = null;
            context.close();
            context = null;
        }
    }
}
