/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.O3Utils;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.LineTcpSender;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.network.IODispatcher;
import io.questdb.network.Net;
import org.junit.Assert;
import org.junit.Test;

public class LineTcpReceiverShutdownTest extends AbstractLineTcpReceiverTest {
    private final static Log LOG = LogFactory.getLog(LineTcpReceiverShutdownTest.class);
    private final SOCountDownLatch finished = new SOCountDownLatch(1);

    @Test
    public void testIlpExceptionInCreateMeasurementEvent() throws Exception {
        testSenderIsSendingWhileReceiverIsShuttingDown(false);
    }

    @Test
    public void testIlpExceptionInScheduleEvent() throws Exception {
        testSenderIsSendingWhileReceiverIsShuttingDown(true);
    }

    private void testSenderIsSendingWhileReceiverIsShuttingDown(boolean partialClose) throws Exception {
        String tableName = "tab";
        this.minIdleMsBeforeWriterRelease = 100;
        this.disconnectOnError = true;

        assertMemoryLeak(() -> {
            try {
                LineTcpReceiver receiver = new LineTcpReceiver(lineConfiguration, engine, sharedWorkerPool, sharedWorkerPool, (configuration, engine, ioWorkerPool, dispatcher, writerWorkerPool) -> {
                    LineTcpMeasurementSchedulerWrapper wrapper = new LineTcpMeasurementSchedulerWrapper(configuration, engine, ioWorkerPool, dispatcher, writerWorkerPool);
                    if (partialClose) {
                        wrapper.setWriterThreadId(0);
                    } else {
                        wrapper.setWriterThreadId(Integer.MIN_VALUE);
                    }
                    return wrapper;
                }
                );
                O3Utils.setupWorkerPool(sharedWorkerPool, engine, null, null);
                sharedWorkerPool.start(LOG);

                try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), bindPort, msgBufferSize)) {
                    for (int i = 0; i < 100; i++) {
                        sender.metric(tableName)
                                .field("id", 1)
                                .$(10_000_000L);
                        sender.flush();
                    }

                    new Thread(() -> {
                        try {
                            sharedWorkerPool.halt();
                            receiver.close();
                        } catch (Throwable e) {
                            LOG.error().$("Can't close pool or ilp receiver!").$(e).$();
                            e.printStackTrace();
                        } finally {
                            finished.countDown();
                        }
                    }, "shutdown thread").start();

                    sender.metric(tableName)
                            .field("id", 2)
                            .$(20_000_000L);
                    sender.flush();
                    finished.await();

                    for (int i = 3; i < 100; i++) {
                        sender.metric(tableName)
                                .field("id", i)
                                .$(i * 10_000_000L);
                        System.out.println(i);
                        sender.flush();
                    }
                }
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException lse) {
                LOG.info().$(lse).$();
            }
        });
    }

    static class LineTcpMeasurementSchedulerWrapper extends LineTcpMeasurementScheduler {
        int writerThreadId = 0;

        LineTcpMeasurementSchedulerWrapper(LineTcpReceiverConfiguration lineConfiguration,
                                           CairoEngine engine,
                                           WorkerPool ioWorkerPool,
                                           IODispatcher<LineTcpConnectionContext> dispatcher,
                                           WorkerPool writerWorkerPool) {
            super(lineConfiguration, engine, ioWorkerPool, dispatcher, writerWorkerPool);
        }

        public void setWriterThreadId(int writerThreadId) {
            this.writerThreadId = writerThreadId;
        }

        @Override
        boolean scheduleEvent(NetworkIOJob netIoJob, LineTcpParser parser) {
            TableUpdateDetails details = netIoJob.getLocalTableDetails("tab");
            if (details != null) {
                details.close();
                details.setWriterThreadId(writerThreadId);
                return super.scheduleEvent(netIoJob, parser);
            } else {
                return super.scheduleEvent(netIoJob, parser);
            }
        }
    }
}
