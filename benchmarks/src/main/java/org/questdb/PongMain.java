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

package org.questdb;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.*;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;

import static io.questdb.network.IODispatcher.*;

public class PongMain {

    private static final Log LOG = LogFactory.getLog(PongMain.class);

    public static void main(String[] args) {
        // configuration defines bind address and port
        final IODispatcherConfiguration dispatcherConf = new DefaultIODispatcherConfiguration();
        // worker pool, which would handle jobs
        final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public String getPoolName() {
                return "pool";
            }
        });
        // event loop that accepts connections and publishes network events to event queue
        final IODispatcher<PongConnectionContext> dispatcher = IODispatchers.create(dispatcherConf, new MutableIOContextFactory<>(PongConnectionContext::new, 8));
        // event queue processor
        final PongRequestProcessor processor = new PongRequestProcessor();
        // event loop job
        workerPool.assign(dispatcher);
        // queue processor job
        workerPool.assign(workerId -> dispatcher.processIOQueue(processor));
        // lets go!
        workerPool.start();
    }

    private static class PongRequestProcessor implements IORequestProcessor<PongConnectionContext> {
        @Override
        public void onRequest(int operation, PongConnectionContext context) {
            switch (operation) {
                case IOOperation.READ:
                    context.receivePing();
                    break;
                case IOOperation.WRITE:
                    context.sendPong();
                    break;
                default:
                    context.getDispatcher().disconnect(context, DISCONNECT_REASON_UNKNOWN_OPERATION);
                    break;
            }
        }
    }

    private static class PongConnectionContext extends AbstractMutableIOContext<PongConnectionContext> {
        private final static String PING = "PING";
        private final static String PONG = "PONG";
        private final DirectByteCharSequence flyweight = new DirectByteCharSequence();
        private final int bufSize = 1024;
        private final long bufStart = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        private long buf = bufStart;
        private int writtenLen;

        @Override
        public void clear() {
            buf = bufStart;
            writtenLen = 0;
            LOG.info().$("cleared").$();
        }

        @Override
        public void close() {
            Unsafe.free(bufStart, bufSize, MemoryTag.NATIVE_DEFAULT);
            LOG.info().$("closed").$();
        }

        public void receivePing() {
            // expect "PING"
            int n = Net.recv(getFd(), buf, (int) (bufSize - (buf - bufStart)));
            if (n > 0) {
                flyweight.of(bufStart, buf + n);
                if (Chars.startsWith(PING, flyweight)) {
                    if (flyweight.length() < PING.length()) {
                        // accrue protocol artefacts while they still make sense
                        buf += n;
                        // fair resource use
                        getDispatcher().registerChannel(this, IOOperation.READ);
                    } else {
                        // reset buffer
                        this.buf = bufStart;
                        // send PONG by preparing the buffer and asking client to receive
                        LOG.info().$(flyweight).$();
                        Chars.asciiStrCpy(PONG, bufStart);
                        writtenLen = PONG.length();
                        getDispatcher().registerChannel(this, IOOperation.WRITE);
                    }
                } else {
                    getDispatcher().disconnect(this, DISCONNECT_REASON_PROTOCOL_VIOLATION);
                }
            } else {
                // handle peer disconnect
                getDispatcher().disconnect(this, DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV);
            }
        }

        public void sendPong() {
            int n = Net.send(getFd(), buf, (int) (writtenLen - (buf - bufStart)));
            if (n > -1) {
                if (n > 0) {
                    buf += n;
                    if (buf - bufStart < writtenLen) {
                        getDispatcher().registerChannel(this, IOOperation.WRITE);
                    } else {
                        flyweight.of(bufStart, bufStart + writtenLen);
                        LOG.info().$(flyweight).$();
                        buf = bufStart;
                        writtenLen = 0;
                        getDispatcher().registerChannel(this, IOOperation.READ);
                    }
                } else {
                    getDispatcher().registerChannel(this, IOOperation.WRITE);
                }
            } else {
                // handle peer disconnect
                getDispatcher().disconnect(this, DISCONNECT_REASON_PEER_DISCONNECT_AT_SEND);
            }
        }
    }
}
