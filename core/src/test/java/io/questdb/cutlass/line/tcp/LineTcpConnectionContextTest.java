/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.*;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.FloatingDirectCharSink;
import io.questdb.std.str.LPSZ;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

public class LineTcpConnectionContextTest extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(LineTcpConnectionContextTest.class);
    private static final int FD = 1_000_000;
    private static Function<LPSZ, Void> FF_OPENRW_TASK;
    private LineTcpConnectionContext context;
    private LineTcpReceiverConfiguration lineTcpConfiguration;
    private LineTcpMeasurementScheduler scheduler;
    private boolean disconnected;
    private String recvBuffer;
    private int nWriterThreads;
    private WorkerPool workerPool;
    private int[] rebalanceLoadByThread;
    private int rebalanceNLoadCheckCycles = 0;
    private int rebalanceNRebalances = 0;
    private long microSecondTicks;

    @BeforeClass
    public static void setUp() throws IOException {
        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops
        // which causes memory leak detector to fail should logger be
        // created mid-test
        LOG.info().$("begin").$();
        root = temp.newFolder("dbRoot").getAbsolutePath();
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                FF_OPENRW_TASK.apply(name);
                return super.openRW(name);
            }
        };
        configuration = new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        };
    }

    @Before
    public void before() {
        FF_OPENRW_TASK = (name) -> null;
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
        nWriterThreads = 2;
        microSecondTicks = -1;
        lineTcpConfiguration = new DefaultLineTcpReceiverConfiguration() {
            @Override
            public int getNetMsgBufferSize() {
                return 512;
            }

            @Override
            public int getMaxMeasurementSize() {
                return 128;
            }

            @Override
            public NetworkFacade getNetworkFacade() {
                return nf;
            }

            @Override
            public MicrosecondClock getMicrosecondClock() {
                return new MicrosecondClockImpl() {
                    @Override
                    public long getTicks() {
                        if (microSecondTicks >= 0) {
                            return microSecondTicks;
                        }
                        return super.getTicks();
                    }
                };
            }
        };
    }

    @Test
    public void testAddFieldColumn() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81,humidity=23 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\thumidity\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\tNaN\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\tNaN\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\t23.0\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\tNaN\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\tNaN\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\tNaN\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\tNaN\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testAddTagColumn() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast,city=york temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\tcity\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\t\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\t\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\tyork\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\t\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\t\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\t\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\t\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testAddToExistingTable() throws Exception {
        addTable();
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            waitForIOCompletion();
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
    public void testBadCast() throws Exception {
        addTable();
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=0x85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testBadLineSyntax1() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 a=146583983102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testBadLineSyntax2() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast,broken temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testBadLineSyntax3() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast broken=23 temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testBadLineSyntax4() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast broken.col=aString,temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testBadLineSyntax5() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast,broken.col=aString temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testBadLineSyntax6() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast broken.col=aString,temperature=80 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast,broken.col=aString temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testBadTimestamp() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 146583983x102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testCairoExceptionOnAddColumn() throws Exception {
        FF_OPENRW_TASK = (fnm) -> {
            if (fnm.toString().endsWith("broken.d")) {
                throw CairoException.instance(2).put("Cannot open ").put(fnm);
            }
            return null;
        };
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81,broken=23 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testCairoExceptionOnCreateTable() throws Exception {
        FF_OPENRW_TASK = (fnm) -> {
            if (fnm.toString().endsWith("broken.d")) {
                throw CairoException.instance(2).put("Cannot open ").put(fnm);
            }
            return null;
        };
        runInContext(() -> {
            recvBuffer = "weather,location=us-eastcoast temperature=81,broken=23 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\tbroken\ttimestamp\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testColumnConversion1() throws Exception {
        runInContext(() -> {
            try (
                    TableModel model = new TableModel(configuration, "t_ilp21",
                            PartitionBy.NONE).col("event", ColumnType.SHORT).col("id", ColumnType.LONG256).col("ts", ColumnType.TIMESTAMP).timestamp()) {
                CairoTestUtils.create(model);
            }
            microSecondTicks = 1465839830102800L;
            recvBuffer = "t_ilp21 event=12i,id=0x05a9796963abad00001e5f6bbdb38i,ts=1465839830102400i\n" +
                    "t_ilp21 event=12i,id=0x5a9796963abad00001e5f6bbdb38i,ts=1465839830102400i\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "event\tid\tts\ttimestamp\n" +
                    "12\t0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t2016-06-13T17:43:50.102800Z\n" +
                    "12\t0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t2016-06-13T17:43:50.102800Z\n";
            assertTable(expected, "t_ilp21");
        });
    }

    @Test
    public void testColumnNameWithSlash1() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81,no/way/humidity=23 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testColumnNameWithSlash2() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-eastcoast temperature=81,no/way/humidity=23 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testColumnTypeChange() throws Exception {
        addTable();
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85i 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
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
            handleContextIO();
            Assert.assertFalse(disconnected);
        });
    }

    @Test
    public void testExtremeFragmentation() throws Exception {
        runInContext(() -> {
            String allMsgs = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            int n = 0;
            while (n < allMsgs.length()) {
                recvBuffer = allMsgs.substring(n, n + 1);
                n++;
                handleContextIO();
                Assert.assertFalse(disconnected);
            }
            waitForIOCompletion();
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
    public void testFailure() throws Exception {
        final AtomicInteger nCommitedLines = new AtomicInteger(4);
        Runnable onCommitNewEvent = () -> {
            if (nCommitedLines.decrementAndGet() <= 0) {
                throw new RuntimeException("Failed");
            }
        };
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO();
            Assert.assertTrue(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n";
            assertTable(expected, "weather");
        }, onCommitNewEvent);
    }

    @Test
    public void testFragmentation1() throws Exception {
        testFragmentation("weat".length());
    }

    @Test
    public void testFragmentation10() throws Exception {
        testFragmentation("weather,location=us-midwest tempera".length());
    }

    @Test
    public void testFragmentation11() throws Exception {
        testFragmentation("weather,location=us-midwest temperature".length());
    }

    @Test
    public void testFragmentation12() throws Exception {
        testFragmentation("weather,location=us-midwest temperature=".length());
    }

    @Test
    public void testFragmentation13() throws Exception {
        testFragmentation("weather,location=us-midwest temperature=8".length());
    }

    @Test
    public void testFragmentation14() throws Exception {
        testFragmentation("weather,location=us-midwest temperature=82".length());
    }

    @Test
    public void testFragmentation15() throws Exception {
        testFragmentation("weather,location=us-midwest temperature=82 ".length());
    }

    @Test
    public void testFragmentation16() throws Exception {
        testFragmentation("weather,location=us-midwest temperature=82 1465839830100400".length());
    }

    @Test
    public void testFragmentation17() throws Exception {
        testFragmentation("weather,location=us-midwest temperature=82 1465839830100400200".length());
    }

    @Test
    public void testFragmentation2() throws Exception {
        testFragmentation("weather".length());
    }

    @Test
    public void testFragmentation3() throws Exception {
        testFragmentation("weather,".length());
    }

    @Test
    public void testFragmentation4() throws Exception {
        testFragmentation("weather,locat".length());
    }

    @Test
    public void testFragmentation5() throws Exception {
        testFragmentation("weather,location".length());
    }

    @Test
    public void testFragmentation6() throws Exception {
        testFragmentation("weather,location=".length());
    }

    @Test
    public void testFragmentation7() throws Exception {
        testFragmentation("weather,location=us-midw".length());
    }

    @Test
    public void testFragmentation8() throws Exception {
        testFragmentation("weather,location=us-midwest".length());
    }

    @Test
    public void testFragmentation9() throws Exception {
        testFragmentation("weather,location=us-midwest ".length());
    }

    @Test
    public void testMaxSizes() throws Exception {
        runInContext(() -> {
            String longMeasurement = "weather,location=us-eastcoastxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx temperature=81 1465839830101400200\n";
            Assert.assertFalse(longMeasurement.length() < lineTcpConfiguration.getMaxMeasurementSize());
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    longMeasurement +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            Assert.assertFalse(recvBuffer.length() < lineTcpConfiguration.getNetMsgBufferSize());
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoastxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testMultiplTablesWithMultipleWriterThreads() throws Exception {
        nWriterThreads = 5;
        int nTables = 12;
        int nIterations = 20_000;
        testThreading(nTables, nIterations, null);
    }

    @Test
    public void testMultiplTablesWithSingleWriterThread() throws Exception {
        nWriterThreads = 1;
        int nTables = 3;
        int nIterations = 20_000;
        testThreading(nTables, nIterations, null);
        Assert.assertEquals(0, rebalanceNRebalances);
    }

    @Test
    public void testMultipleMeasurements1() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
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
    public void testMultipleMeasurements2() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            recvBuffer = "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
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
    public void testMultipleMeasurements3() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather temperature=82,pressure=100i 1465839830100400200\n" +
                    "weather temperature=83,pressure=100i 1465839830100500200\n" +
                    "weather temperature=81,pressure=102i 1465839830101400200\n" +
                    "weather temperature=85,pressure=103i 1465839830102300200\n" +
                    "weather temperature=89,pressure=101i 1465839830102400200\n" +
                    "weather temperature=80,pressure=100i 1465839830102400200\n" +
                    "weather temperature=82,pressure=100i 1465839830102500200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "temperature\tpressure\ttimestamp\n" +
                    "82.0\t100\t2016-06-13T17:43:50.100400Z\n" +
                    "83.0\t100\t2016-06-13T17:43:50.100500Z\n" +
                    "81.0\t102\t2016-06-13T17:43:50.101400Z\n" +
                    "85.0\t103\t2016-06-13T17:43:50.102300Z\n" +
                    "89.0\t101\t2016-06-13T17:43:50.102400Z\n" +
                    "80.0\t100\t2016-06-13T17:43:50.102400Z\n" +
                    "82.0\t100\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testMultipleMeasurements4() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather temperature=82,pressure=100i 1465839830100400200\n" +
                    "weather temperature=83,pressure=100i 1465839830100500200\n" +
                    "weather temperature=81,pressure=102i 1465839830101400200\n" +
                    "weather temperature=85,pressure=103i 1465839830102300200\n" +
                    "weather temperature=89,pressure=101i 1465839830102400200\n" +
                    "weather temperature=80,pressure=100i 1465839830102400200\n" +
                    "weather temperature=82,pressure=100i 1465839830102500200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "temperature\tpressure\ttimestamp\n" +
                    "82.0\t100\t2016-06-13T17:43:50.100400Z\n" +
                    "83.0\t100\t2016-06-13T17:43:50.100500Z\n" +
                    "81.0\t102\t2016-06-13T17:43:50.101400Z\n" +
                    "85.0\t103\t2016-06-13T17:43:50.102300Z\n" +
                    "89.0\t101\t2016-06-13T17:43:50.102400Z\n" +
                    "80.0\t100\t2016-06-13T17:43:50.102400Z\n" +
                    "82.0\t100\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testOverflow() throws Exception {
        runInContext(() -> {
            int msgBufferSize = lineTcpConfiguration.getNetMsgBufferSize();
            recvBuffer = "A";
            while (recvBuffer.length() <= msgBufferSize) {
                recvBuffer += recvBuffer;
            }
            int nUnread = recvBuffer.length() - msgBufferSize;
            handleContextIO();
            Assert.assertTrue(disconnected);
            Assert.assertEquals(nUnread, recvBuffer.length());
        });
    }

    @Test
    public void testSingleMeasurement() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest temperature=82 1465839830100400200\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testThreadsWithUnbalancedLoad() throws Exception {
        nWriterThreads = 3;
        int nTables = 12;
        int nIterations = 20_000;
        double[] loadFactors = { 10, 10, 10, 20, 20, 20, 20, 20, 20, 30, 30, 60 };
        testThreading(nTables, nIterations, loadFactors);

        int maxLoad = Integer.MIN_VALUE;
        int minLoad = Integer.MAX_VALUE;
        for (int load : rebalanceLoadByThread) {
            if (maxLoad < load) {
                maxLoad = load;
            }
            if (minLoad > load) {
                minLoad = load;
            }
        }
        double loadRatio = (double) maxLoad / (double) minLoad;
        LOG.info().$("testThreadsWithUnbalancedLoad final load ratio is ").$(loadRatio).$();
        Assert.assertTrue(loadRatio < 1.05);
    }

    @Test
    public void testUseReceivedTimestamp1() throws Exception {
        runInContext(() -> {
            microSecondTicks = 0;
            recvBuffer = "weather,location=us-midwest temperature=82\n" +
                    "weather,location=us-midwest temperature=83\n" +
                    "weather,location=us-eastcoast temperature=81\n" +
                    "weather,location=us-midwest temperature=85\n" +
                    "weather,location=us-eastcoast temperature=89\n" +
                    "weather,location=us-eastcoast temperature=80\n" +
                    "weather,location=us-westcost temperature=82\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t1970-01-01T00:00:00.000000Z\n" +
                    "us-midwest\t83.0\t1970-01-01T00:00:00.000000Z\n" +
                    "us-eastcoast\t81.0\t1970-01-01T00:00:00.000000Z\n" +
                    "us-midwest\t85.0\t1970-01-01T00:00:00.000000Z\n" +
                    "us-eastcoast\t89.0\t1970-01-01T00:00:00.000000Z\n" +
                    "us-eastcoast\t80.0\t1970-01-01T00:00:00.000000Z\n" +
                    "us-westcost\t82.0\t1970-01-01T00:00:00.000000Z\n";
            assertTable(expected, "weather");
        });
    }

    private void addTable() {
        try (
                TableModel model = new TableModel(configuration, "weather",
                        PartitionBy.NONE).col("location", ColumnType.SYMBOL).col("temperature", ColumnType.DOUBLE).timestamp()) {
            CairoTestUtils.create(model);
        }
    }

    private void assertTable(CharSequence expected, CharSequence tableName) {
        try (TableReader reader = new TableReader(configuration, tableName)) {
            assertThat(expected, reader.getCursor(), reader.getMetadata(), true);
        }
    }

    private void assertTableCount(CharSequence tableName, int nExpectedRows, long maxExpectedTimestampNanos) {
        try (TableReader reader = new TableReader(configuration, tableName)) {
            Assert.assertEquals(maxExpectedTimestampNanos / 1000, reader.getMaxTimestamp());
            int timestampColIndex = reader.getMetadata().getTimestampIndex();
            TableReaderRecordCursor recordCursor = reader.getCursor();
            int nRows = 0;
            long timestampinNanos = 1465839830100400200L;
            while (recordCursor.hasNext()) {
                long actualTimestampInMicros = recordCursor.getRecord().getTimestamp(timestampColIndex);
                Assert.assertEquals(timestampinNanos / 1000, actualTimestampInMicros);
                timestampinNanos += 1000;
                nRows++;
            }
            Assert.assertEquals(nExpectedRows, nRows);
        }
    }

    private void closeContext() {
        if (null != scheduler) {
            workerPool.halt();
            Assert.assertFalse(context.invalid());
            Assert.assertEquals(FD, context.getFd());
            context.close();
            Assert.assertTrue(context.invalid());
            Assert.assertEquals(-1, context.getFd());
            context = null;
            scheduler.close();
            scheduler = null;
        }
    }

    private void runInContext(Runnable r) throws Exception {
        runInContext(r, null);
    }

    private void runInContext(Runnable r, Runnable onCommitNewEvent) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                setupContext(engine, onCommitNewEvent);
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

    private void setupContext(CairoEngine engine, Runnable onCommitNewEvent) {
        workerPool = new WorkerPool(new WorkerPoolConfiguration() {
            private final int workerCount;
            private final int[] affinityByThread;

            @Override
            public int[] getWorkerAffinity() {
                return affinityByThread;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public boolean haltOnError() {
                return false;
            }

            {
                workerCount = nWriterThreads;
                affinityByThread = new int[workerCount];
                Arrays.fill(affinityByThread, -1);
            }
        });
        scheduler = new LineTcpMeasurementScheduler(lineTcpConfiguration, engine, workerPool, null) {
            @Override
            boolean tryCommitNewEvent(NewLineProtoParser protoParser, FloatingDirectCharSink charSink) {
                if (null != onCommitNewEvent) {
                    onCommitNewEvent.run();
                }
                return super.tryCommitNewEvent(protoParser, charSink);
            }
        };
        context = new LineTcpConnectionContext(lineTcpConfiguration, scheduler);
        disconnected = false;
        recvBuffer = null;
        IODispatcher<LineTcpConnectionContext> dispatcher = new IODispatcher<>() {
            @Override
            public void close() {
            }

            @Override
            public int getConnectionCount() {
                return disconnected ? 0 : 1;
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

            @Override
            public boolean run(int workerId) {
                return false;
            }
        };
        Assert.assertNull(context.getDispatcher());
        context.of(FD, dispatcher);
        Assert.assertFalse(context.invalid());
        Assert.assertEquals(FD, context.getFd());
        Assert.assertEquals(dispatcher, context.getDispatcher());
        workerPool.start(LOG);
    }

    private void testFragmentation(int breakPos) throws Exception {
        runInContext(() -> {
            String allMsgs = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast temperature=80 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            recvBuffer = allMsgs.substring(0, breakPos);
            handleContextIO();
            Assert.assertFalse(disconnected);
            recvBuffer = allMsgs.substring(breakPos);
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
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

    private void testThreading(int nTables, int nIterations, double[] lf) throws Exception {
        if (null == lf) {
            lf = new double[nTables];
            Arrays.fill(lf, 1d);
        } else {
            assert lf.length == nTables;
        }
        for (int n = 1; n < nTables; n++) {
            lf[n] += lf[n - 1];
        }
        final double[] loadFactors = lf;
        final double accLoadFactors = loadFactors[nTables - 1];
        Random random = new Random(0);
        int[] countByTable = new int[nTables];
        long[] maxTimestampByTable = new long[nTables];
        final long initialTimestampNanos = 1465839830100400200L;
        final long timestampIncrementInNanos = 1000;
        Arrays.fill(maxTimestampByTable, initialTimestampNanos);
        runInContext(() -> {
            int nTablesSelected = 0;
            int nTotalUpdates = 0;
            for (int nIter = 0; nIter < nIterations; nIter++) {
                int nLines = random.nextInt(50) + 1;
                recvBuffer = "";
                for (int nLine = 0; nLine < nLines; nLine++) {
                    int nTable;
                    if (nTablesSelected < nTables) {
                        nTable = nTablesSelected++;
                    } else {
                        double tableSelector = random.nextDouble() * accLoadFactors;
                        nTable = nTables;
                        while (--nTable > 0) {
                            if (tableSelector > loadFactors[nTable - 1]) {
                                break;
                            }
                        }
                    }
                    long timestamp = maxTimestampByTable[nTable];
                    maxTimestampByTable[nTable] += timestampIncrementInNanos;
                    double temperature = 50.0 + (random.nextInt(500) / 10.0);
                    recvBuffer += "weather" + nTable + ",location=us-midwest temperature=" + temperature + " " + timestamp + "\n";
                    countByTable[nTable]++;
                    nTotalUpdates++;
                }
                do {
                    handleContextIO();
                    // Assert.assertFalse(disconnected);
                } while (recvBuffer.length() > 0);
            }
            waitForIOCompletion();
            rebalanceNLoadCheckCycles = scheduler.getNLoadCheckCycles();
            rebalanceNRebalances = scheduler.getNRebalances();
            rebalanceLoadByThread = scheduler.getLoadByThread();
            closeContext();
            LOG.info().$("Completed ").$(nTotalUpdates).$(" measurements with ").$(nTables).$(" measurement types processed by ").$(nWriterThreads).$(" threads. ")
                    .$(rebalanceNLoadCheckCycles).$(" load checks lead to ").$(rebalanceNRebalances).$(" load rebalancing operations").$();
            for (int nTable = 0; nTable < nTables; nTable++) {
                assertTableCount("weather" + nTable, countByTable[nTable], maxTimestampByTable[nTable] - timestampIncrementInNanos);
            }
        });
    }

    private void handleContextIO() {
        switch (context.handleIO()) {
            case NEEDS_READ:
                context.getDispatcher().registerChannel(context, IOOperation.READ);
                break;
            case NEEDS_WRITE:
                context.getDispatcher().registerChannel(context, IOOperation.WRITE);
                break;
            case NEEDS_CPU:
                break;
            case NEEDS_DISCONNECT:
                context.getDispatcher().disconnect(context);
                break;
            default:
                break;
        }
    }

    private void waitForIOCompletion() {
        int maxIterations = 256;
        // Guard against slow writers on disconnect
        while (maxIterations-- > 0 && context.getDispatcher().getConnectionCount() > 0) {
            if (null != recvBuffer && recvBuffer.length() == 0) {
                recvBuffer = null;
            }
            handleContextIO();
            LockSupport.parkNanos(1_000_000);
        }
        Assert.assertTrue(maxIterations > 0);
        Assert.assertTrue(disconnected);
    }
}
