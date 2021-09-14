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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.CairoTestUtils;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableModel;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderRecordCursor;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.tcp.LineTcpMeasurementScheduler.NetworkIOJob;
import io.questdb.cutlass.line.tcp.LineTcpMeasurementScheduler.TableUpdateDetails;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.IODispatcher;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.FloatingDirectCharSink;
import io.questdb.std.str.LPSZ;

public class LineTcpConnectionContextTest extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(LineTcpConnectionContextTest.class);
    private static final int FD = 1_000_000;
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
            public long getWriterIdleTimeout() {
                return 150;
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
    public void testTableParameterRetentionOnAddColumn() throws Exception {
        runInContext(() -> {
            try (
                    SqlCompiler compiler = new SqlCompiler(engine);
                    SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                compiler.compile(
                        "create table weather (location SYMBOL, temperature DOUBLE, timestamp TIMESTAMP) timestamp(timestamp) partition by DAY WITH maxUncommittedRows=3, commitLag=250ms;",
                        sqlExecutionContext);
            } catch (SqlException ex) {
                throw new RuntimeException(ex);
            }
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "weather")) {
                Assert.assertEquals(3, reader.getMetadata().getMaxUncommittedRows());
                Assert.assertEquals(250_000, reader.getMetadata().getCommitLag());
            }
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
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "weather")) {
                Assert.assertEquals(3, reader.getMetadata().getMaxUncommittedRows());
                Assert.assertEquals(250_000, reader.getMetadata().getCommitLag());
            }
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
        runInContext(
                new FilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name) {
                        if (Chars.endsWith(name, "broken.d")) {
                            return -1;
                        }
                        return super.openRW(name);
                    }
                },
                () -> {
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
                }, null);
    }

    @Test
    public void testCairoExceptionOnCreateTable() throws Exception {
        runInContext(
                new FilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name) {
                        if (Chars.endsWith(name, "broken.d")) {
                            return -1;
                        }
                        return super.openRW(name);
                    }
                },
                () -> {
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
                }, null);
    }

    @Test
    public void testColumnConversion1() throws Exception {
        runInContext(() -> {
            try (
                    @SuppressWarnings("resource")
            TableModel model = new TableModel(configuration, "t_ilp21",
                    PartitionBy.NONE).col("event", ColumnType.SHORT).col("id", ColumnType.LONG256).col("ts", ColumnType.TIMESTAMP).col("float1", ColumnType.FLOAT).col("int1", ColumnType.INT)
                            .col("date1", ColumnType.DATE).col("byte1", ColumnType.BYTE).timestamp()) {
                CairoTestUtils.create(model);
            }
            microSecondTicks = 1465839830102800L;
            recvBuffer = "t_ilp21 event=12i,id=0x05a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1.2,int1=23i,date1=1465839830102i,byte1=-7i\n" +
                    "t_ilp21 event=12i,id=0x5a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1e3,int1=-500000i,date1=1465839830102i,byte1=3i\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "event\tid\tts\tfloat1\tint1\tdate1\tbyte1\ttimestamp\n" +
                    "12\t0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1.2000\t23\t2016-06-13T17:43:50.102Z\t-7\t2016-06-13T17:43:50.102800Z\n" +
                    "12\t0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1000.0000\t-500000\t2016-06-13T17:43:50.102Z\t3\t2016-06-13T17:43:50.102800Z\n";
            assertTable(expected, "t_ilp21");
        });
    }

    @Test
    public void testColumnConversion2() throws Exception {
        runInContext(() -> {
            try (
                    @SuppressWarnings("resource")
            TableModel model = new TableModel(configuration, "t_ilp21",
                    PartitionBy.NONE).col("l", ColumnType.LONG)) {
                CairoTestUtils.create(model);
            }
            microSecondTicks = 1465839830102800L;
            recvBuffer = "t_ilp21 l=843530699759026177i\n" +
                    "t_ilp21 l=\"843530699759026178\"\n" +
                    "t_ilp21 l=843530699759026179i\n";
            handleContextIO();
            Assert.assertFalse(disconnected);
            waitForIOCompletion();
            closeContext();
            String expected = "l\n" +
                    "843530699759026177\n" +
                    "843530699759026179\n";
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

    @Test
    public void testBooleans() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-eastcoast raining=true 1465839830100400200\n" +
                    "weather,location=us-midwest raining=false 1465839830100400200\n" +
                    "weather,location=us-midwest raining=f 1465839830100500200\n" +
                    "weather,location=us-midwest raining=t 1465839830102300200\n" +
                    "weather,location=us-eastcoast raining=T 1465839830102400200\n" +
                    "weather,location=us-eastcoast raining=F 1465839830102400200\n" +
                    "weather,location=us-westcost raining=False 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            waitForIOCompletion();
            closeContext();
            String expected = "location\training\ttimestamp\n" +
                    "us-eastcoast\ttrue\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\tfalse\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\tfalse\t2016-06-13T17:43:50.100500Z\n" +
                    "us-midwest\ttrue\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\ttrue\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\tfalse\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\tfalse\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testStrings() throws Exception {
        runInContext(() -> {
            recvBuffer = "weather,location=us-eastcoast raining=\"true\" 1465839830100400200\n" +
                    "weather,location=us-midwest raining=\"false\" 1465839830100400200\n" +
                    "weather,location=us-midwest raining=\"f\" 1465839830100500200\n" +
                    "weather,location=us-midwest raining=\"t\" 1465839830102300200\n" +
                    "weather,location=us-eastcoast raining=\"T\" 1465839830102400200\n" +
                    "weather,location=us-eastcoast raining=\"F\" 1465839830102400200\n" +
                    "weather,location=us-westcost raining=\"False\" 1465839830102500200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            waitForIOCompletion();
            closeContext();
            String expected = "location\training\ttimestamp\n" +
                    "us-eastcoast\ttrue\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\tfalse\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\tf\t2016-06-13T17:43:50.100500Z\n" +
                    "us-midwest\tt\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\tT\t2016-06-13T17:43:50.102400Z\n" +
                    "us-eastcoast\tF\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\tFalse\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testSymbolOrder1() throws Exception {
        addTable();
        runInContext(() -> {
            recvBuffer = "weather,location=us-midwest,sensor=type3 temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest,sensor=type1 temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast,sensor=type6 temperature=81 1465839830101400200\n" +
                    "weather,location=us-midwest,sensor=type1 temperature=85 1465839830102400200\n" +
                    "weather,location=us-eastcoast,sensor=type1 temperature=89 1465839830102400200\n" +
                    "weather,location=us-eastcoast,sensor=type3 temperature=80 1465839830102400200\n" +
                    "weather,sensor=type1,location=us-midwest temperature=85 1465839830102401200\n" +
                    "weather,location=us-eastcoast,sensor=type1 temperature=89 1465839830102402200\n" +
                    "weather,sensor=type3,location=us-eastcoast temperature=80 1465839830102403200\n" +
                    "weather,location=us-westcost,sensor=type1 temperature=82 1465839830102504200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            waitForIOCompletion();
            closeContext();
            String expected = "location\ttemperature\ttimestamp\tsensor\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\ttype3\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\ttype1\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\ttype6\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102400Z\ttype1\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\ttype1\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102400Z\ttype3\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102401Z\ttype1\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102402Z\ttype1\n" +
                    "us-eastcoast\t80.0\t2016-06-13T17:43:50.102403Z\ttype3\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102504Z\ttype1\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testQuotes() throws Exception {
        runInContext(() -> {
            recvBuffer = "tbl,t1=tv1,t2=tv2 f1=\"fv1\",f2=\"Zen Internet Ltd\" 1465839830100400200\n" +
                    "tbl,t1=tv1,t2=tv2 f1=\"Zen Internet Ltd\" 1465839830100400200\n" +
                    "tbl,t1=tv1,t2=tv2 f1=\"Zen=Internet,Ltd\" 1465839830100400200\n" +
                    "tbl,t1=t\\\"v1,t2=t\"v2 f2=\"1\" 1465839830100400200\n" +
                    "tbl,t1=\"tv1\",t2=tv2 f2=\"1\" 1465839830100400200\n" +
                    "tbl,t1=tv1,t2=tv2 f1=\"Zen Internet Ltd\",f2=\"fv2\" 1465839830100400200\n";
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            waitForIOCompletion();
            closeContext();
            String expected = "t1\tt2\tf1\tf2\ttimestamp\n" +
                    "tv1\ttv2\tfv1\tZen Internet Ltd\t2016-06-13T17:43:50.100400Z\n" +
                    "tv1\ttv2\tZen Internet Ltd\t\t2016-06-13T17:43:50.100400Z\n" +
                    "tv1\ttv2\tZen=Internet,Ltd\t\t2016-06-13T17:43:50.100400Z\n" +
                    "t\"v1\tt\"v2\t\t1\t2016-06-13T17:43:50.100400Z\n" +
                    "\"tv1\"\ttv2\t\t1\t2016-06-13T17:43:50.100400Z\n" +
                    "tv1\ttv2\tZen Internet Ltd\tfv2\t2016-06-13T17:43:50.100400Z\n";
            assertTable(expected, "tbl");
        });
    }

    private void addTable() {
        try (
                @SuppressWarnings("resource")
                TableModel model = new TableModel(configuration, "weather",
                        PartitionBy.NONE).col("location", ColumnType.SYMBOL).col("temperature", ColumnType.DOUBLE).timestamp()) {
            CairoTestUtils.create(model);
        }
    }

    private void assertTable(CharSequence expected, CharSequence tableName) {
        try (TableReader reader = new TableReader(configuration, tableName)) {
            assertCursorTwoPass(expected, reader.getCursor(), reader.getMetadata());
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
        runInContext(null, r, onCommitNewEvent);
    }

    private void runInContext(FilesFacade ff, Runnable r, Runnable onCommitNewEvent) throws Exception {
        assertMemoryLeak(ff, () -> {
            setupContext(onCommitNewEvent);
            try {
                r.run();
            } finally {
                closeContext();
            }
        });
    }

    private void setupContext(Runnable onCommitNewEvent) {
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

        WorkerPool netIoWorkerPool = new WorkerPool(new WorkerPoolConfiguration() {
            private final int[] affinityByThread = { -1 };

            @Override
            public boolean haltOnError() {
                return true;
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public int[] getWorkerAffinity() {
                return affinityByThread;
            }
        });

        scheduler = new LineTcpMeasurementScheduler(lineTcpConfiguration, engine, netIoWorkerPool, null, workerPool) {
            @Override
            boolean tryButCouldNotCommit(NetworkIOJob netIoJob, NewLineProtoParser protoParser, FloatingDirectCharSink charSink) {
                if (null != onCommitNewEvent) {
                    onCommitNewEvent.run();
                }
                return super.tryButCouldNotCommit(netIoJob, protoParser, charSink);
            }

            @Override
            protected NetworkIOJob createNetworkIOJob(IODispatcher<LineTcpConnectionContext> dispatcher, int workerId) {
                Assert.assertEquals(0, workerId);
                return netIoJob;
            }
        };
        context = new LineTcpConnectionContext(lineTcpConfiguration, scheduler);
        disconnected = false;
        recvBuffer = null;
        IODispatcher<LineTcpConnectionContext> dispatcher = new IODispatcher<LineTcpConnectionContext>() {
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
            public void disconnect(LineTcpConnectionContext context, int reason) {
                disconnected = true;
            }

            @Override
            public boolean run(int workerId) {
                return false;
            }

            @Override
            public boolean isListening() {
                return true;
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
        switch (context.handleIO(netIoJob)) {
            case NEEDS_READ:
                context.getDispatcher().registerChannel(context, IOOperation.READ);
                break;
            case NEEDS_WRITE:
                context.getDispatcher().registerChannel(context, IOOperation.WRITE);
                break;
            case NEEDS_DISCONNECT:
                context.getDispatcher().disconnect(context, IODispatcher.DISCONNECT_REASON_PROTOCOL_VIOLATION);
                break;
            default:
                break;
        }
    }

    private void waitForIOCompletion() {
        waitForIOCompletion(true);
    }

    private void waitForIOCompletion(boolean closeConnection) {
        int maxIterations = 256;
        // Guard against slow writers on disconnect
        while (maxIterations-- > 0 && context.getDispatcher().getConnectionCount() > 0) {
            if (null != recvBuffer && recvBuffer.length() == 0) {
                if (!closeConnection) {
                    break;
                }
                recvBuffer = null;
            }
            handleContextIO();
            LockSupport.parkNanos(1_000_000);
        }
        Assert.assertTrue(maxIterations > 0);
        Assert.assertTrue(disconnected || !closeConnection);
        // Wait for last commit
        try {
            Thread.sleep(lineTcpConfiguration.getMaintenanceInterval() + 50);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    private final NetworkIOJob netIoJob = new NetworkIOJob() {
        private final CharSequenceObjHashMap<TableUpdateDetails> localTableUpdateDetailsByTableName = new CharSequenceObjHashMap<>();
        private final ObjList<SymbolCache> unusedSymbolCaches = new ObjList<>();

        @Override
        public int getWorkerId() {
            return 0;
        }

        @Override
        public TableUpdateDetails getTableUpdateDetails(CharSequence tableName) {
            return localTableUpdateDetailsByTableName.get(tableName);
        }

        @Override
        public void addTableUpdateDetails(TableUpdateDetails tableUpdateDetails) {
            localTableUpdateDetailsByTableName.put(tableUpdateDetails.tableName, tableUpdateDetails);
        }

        @Override
        public boolean run(int workerId) {
            Assert.fail("This is a mock job, not designed to run in a wroker pool");
            return false;
        }

        @Override
        public ObjList<SymbolCache> getUnusedSymbolCaches() {
            return unusedSymbolCaches;
        }

        @Override
        public void close() {
        }
    };
}
