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
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.udp.LineProtoSender;
import io.questdb.cutlass.line.udp.LineTCPProtoSender;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.Net;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.junit.Test;

import java.util.Random;
import java.util.function.Supplier;

public class LineTcpServerTest extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(LineTcpConnectionContextTest.class);

    @Test(timeout = 120000)
    public void test() {
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
                return 200;
            }

            @Override
            public int getMaxMeasurementSize() {
                return 50;
            }

            @Override
            public int getnUpdatesPerLoadRebalance() {
                return 100;
            }

            @Override
            public double getMaxLoadRatio() {
                // Always rebalance as long as there are more tables than threads;
                return 1;
            }
        };

        final int nRows = 1000;
        final String[] tables = { "weather1", "weather2", "weather3" };
        final String[] locations = { "london", "paris", "rome" };

        final Random rand = new Random(0);
        final StringBuilder[] expectedSbs = new StringBuilder[tables.length];

        try (CairoEngine engine = new CairoEngine(configuration, null)) {
            LineTcpServer tcpServer = LineTcpServer.create(configuration, lineConfiguration, sharedWorkerPool, LOG, engine, messageBus);

            SOCountDownLatch tablesCreated = new SOCountDownLatch();
            tablesCreated.setCount(tables.length);
            Supplier<Path> pathSupplier = Path::new;
            sharedWorkerPool.assign(new Job() {
                private final ThreadLocal<Path> tlPath = ThreadLocal.withInitial(pathSupplier);

                @Override
                public boolean run(int workerId) {
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

            final LineProtoSender[] senders = new LineProtoSender[tables.length];
            for (int n = 0; n < senders.length; n++) {
                senders[n] = new LineTCPProtoSender(Net.parseIPv4("127.0.0.1"), bindPort, 4096);
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

                if (nRow == tables.length) {
                    tablesCreated.await();
                }
            }

            for (int n = 0; n < senders.length; n++) {
                LineProtoSender sender = senders[n];
                sender.close();
            }

            int nRowsWritten;
            do {
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

            Misc.free(tcpServer);
        }
        sharedWorkerPool.halt();

        for (int n = 0; n < tables.length; n++) {
            String tableName = tables[n];
            assertTable(expectedSbs[n].toString(), tableName);
        }
    }

    private void assertTable(CharSequence expected, CharSequence tableName) {
        try (TableReader reader = new TableReader(configuration, tableName)) {
            assertThat(expected, reader.getCursor(), reader.getMetadata(), true);
        }
    }
}
