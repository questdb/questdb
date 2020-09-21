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

import java.net.URL;
import java.security.PrivateKey;
import java.util.Random;
import java.util.function.Supplier;

import org.junit.Test;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderRecordCursor;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.udp.AuthenticatedLineTCPProtoSender;
import io.questdb.cutlass.line.udp.LineProtoSender;
import io.questdb.cutlass.line.udp.LineTCPProtoSender;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

public class LineTcpServerTest extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(LineTcpServerTest.class);
    private final static String AUTH_KEY_ID1 = "testUser1";
    private final static PrivateKey AUTH_PRIVATE_KEY1 = AuthDb.importPrivateKey("5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48");
    private final static String AUTH_KEY_ID2 = "testUser2";
    private final static PrivateKey AUTH_PRIVATE_KEY2 = AuthDb.importPrivateKey("lwJi3TSb4G6UcHxFJmPhOTWa4BLwJOOiK76wT6Uk7pI");

    @Test(timeout = 120000)
    public void testUnauthenticated() {
        test(null, null, 200);
    }

    @Test(timeout = 120000)
    public void testGoodAuthenticated() {
        test(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, 768);
    }

    @Test(timeout = 120000, expected = NetworkError.class)
    public void testInvalidUser() {
        test(AUTH_KEY_ID2, AUTH_PRIVATE_KEY2, 768);
    }

    @Test(timeout = 120000, expected = NetworkError.class)
    public void testInvalidSignature() {
        test(AUTH_KEY_ID1, AUTH_PRIVATE_KEY2, 768);
    }

    private void test(String authKeyId, PrivateKey authPrivateKey, int msgBufferSize) {
        WorkerPool sharedWorkerPool = new WorkerPool(new WorkerPoolConfiguration() {
            private final int[] affinity = { -1, -1 };

            @Override
            public boolean haltOnError() {
                return true;
            }

            @Override
            public int getWorkerCount() {
                return 2;
            }

            @Override
            public int[] getWorkerAffinity() {
                return affinity;
            }
        });

        final int bindIp = 0;
        final int bindPort = 9002; // Dont clash with other tests since they may run in parallel
        IODispatcherConfiguration ioDispatcherConfiguration = new DefaultIODispatcherConfiguration() {
            @Override
            public int getBindIPv4Address() {
                return bindIp;
            }

            @Override
            public int getBindPort() {
                return bindPort;
            }
        };
        LineTcpReceiverConfiguration lineConfiguration = new DefaultLineTcpReceiverConfiguration() {
            @Override
            public IODispatcherConfiguration getNetDispatcherConfiguration() {
                return ioDispatcherConfiguration;
            }

            @Override
            public int getWriterQueueSize() {
                return 4;
            }

            @Override
            public int getNetMsgBufferSize() {
                return msgBufferSize;
            }

            @Override
            public int getMaxMeasurementSize() {
                return 50;
            }

            @Override
            public int getNUpdatesPerLoadRebalance() {
                return 100;
            }

            @Override
            public double getMaxLoadRatio() {
                // Always rebalance as long as there are more tables than threads;
                return 1;
            }

            @Override
            public String getAuthDbPath() {
                if (null == authKeyId) {
                    return null;
                }
                URL u = getClass().getResource("authDb.txt");
                return u.getFile();
            }
        };

        final int nRows = 1000;
        final String[] tables = { "weather1", "weather2", "weather3" };
        final String[] locations = { "london", "paris", "rome" };

        final Random rand = new Random(0);
        final StringBuilder[] expectedSbs = new StringBuilder[tables.length];

        try (CairoEngine engine = new CairoEngine(configuration)) {
            LineTcpServer tcpServer = LineTcpServer.create(lineConfiguration, sharedWorkerPool, LOG, engine);

            SOCountDownLatch tablesCreated = new SOCountDownLatch();
            tablesCreated.setCount(tables.length);
            Supplier<Path> pathSupplier = Path::new;
            sharedWorkerPool.assign(new SynchronizedJob() {
                private final ThreadLocal<Path> tlPath = ThreadLocal.withInitial(pathSupplier);

                @Override
                public boolean runSerially() {
                    int nTable = tables.length - tablesCreated.getCount();
                    if (nTable < tables.length) {
                        String tableName = tables[nTable];
                        int status = engine.getStatus(AllowAllCairoSecurityContext.INSTANCE, tlPath.get(), tableName);
                        if (status == TableUtils.TABLE_EXISTS) {
                            tablesCreated.countDown();
                        }
                        return true;
                    }

                    return false;
                }
            });
            sharedWorkerPool.start(LOG);

            try {
                final LineProtoSender[] senders = new LineProtoSender[tables.length];
                for (int n = 0; n < senders.length; n++) {
                    if (null != authKeyId) {
                        AuthenticatedLineTCPProtoSender sender = new AuthenticatedLineTCPProtoSender(authKeyId, authPrivateKey, Net.parseIPv4("127.0.0.1"), bindPort, 4096);
                        sender.authenticate();
                        senders[n] = sender;
                    } else {
                        senders[n] = new LineTCPProtoSender(Net.parseIPv4("127.0.0.1"), bindPort, 4096);
                    }
                    StringBuilder sb = new StringBuilder((nRows + 1) * lineConfiguration.getMaxMeasurementSize());
                    sb.append("location\ttemp\ttimestamp\n");
                    expectedSbs[n] = sb;
                }

                long ts = Os.currentTimeMicros();
                StringSink tsSink = new StringSink();
                for (int nRow = 0; nRow < nRows; nRow++) {
                    int nTable = nRow < tables.length ? nRow : rand.nextInt(tables.length);
                    LineProtoSender sender = senders[nTable];
                    StringBuilder sb = expectedSbs[nTable];
                    String tableName = tables[nTable];
                    sender.metric(tableName);
                    String location = locations[rand.nextInt(locations.length)];
                    sb.append(location);
                    sb.append('\t');
                    sender.tag("location", location);
                    int temp = rand.nextInt(100);
                    sb.append(temp);
                    sb.append('\t');
                    sender.field("temp", temp);
                    tsSink.clear();
                    TimestampFormatUtils.appendDateTimeUSec(tsSink, ts);
                    sb.append(tsSink.toString());
                    sb.append('\n');
                    sender.$(ts * 1000);
                    sender.flush();
                    ts += rand.nextInt(1000);
                }

                for (int n = 0; n < senders.length; n++) {
                    LineProtoSender sender = senders[n];
                    sender.close();
                }

                tablesCreated.await();
                int nRowsWritten;
                do {
                    Thread.yield();
                    nRowsWritten = 0;
                    for (int n = 0; n < tables.length; n++) {
                        String tableName = tables[n];
                        try (TableReader reader = new TableReader(configuration, tableName)) {
                            TableReaderRecordCursor cursor = reader.getCursor();
                            while (cursor.hasNext()) {
                                nRowsWritten++;
                            }
                        }
                    }
                } while (nRowsWritten < nRows);
                LOG.info().$(nRowsWritten).$(" rows written").$();
            } finally {
                Misc.free(tcpServer);
                sharedWorkerPool.halt();
            }
        }

        for (int n = 0; n < tables.length; n++) {
            String tableName = tables[n];
            LOG.info().$("checking table ").$(tableName).$();
            assertTable(expectedSbs[n].toString(), tableName);
        }
    }

    private void assertTable(CharSequence expected, CharSequence tableName) {
        try (TableReader reader = new TableReader(configuration, tableName)) {
            assertThat(expected, reader.getCursor(), reader.getMetadata(), true);
        }
    }
}
