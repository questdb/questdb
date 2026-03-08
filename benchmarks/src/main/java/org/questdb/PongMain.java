/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.Metrics;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IOContext;
import io.questdb.network.IOContextFactoryImpl;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.IODispatchers;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.ServerDisconnectException;
import io.questdb.network.SocketFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

import static io.questdb.network.IODispatcher.*;

public class PongMain {

    private static final Log LOG = LogFactory.getLog(PongMain.class);

    public static void main(String[] args) {
        // configuration defines bind address and port
        final IODispatcherConfiguration dispatcherConf = new DefaultIODispatcherConfiguration();
        // worker pool, which would handle jobs
        final WorkerPool workerPool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }
        });
        // event loop that accepts connections and publishes network events to event queue
        final IODispatcher<PongConnectionContext> dispatcher = IODispatchers.create(
                dispatcherConf,
                new IOContextFactoryImpl<>(() -> new PongConnectionContext(PlainSocketFactory.INSTANCE, dispatcherConf.getNetworkFacade(), LOG), 8)
        );
        // event queue processor
        final PongRequestProcessor processor = new PongRequestProcessor();
        // event loop job
        workerPool.assign(dispatcher);
        // queue processor job
        workerPool.assign((workerId, runStatus) -> dispatcher.processIOQueue(processor));
        // lets go!
        workerPool.start();
    }

    private static class PongConnectionContext extends IOContext<PongConnectionContext> {
        private final static Utf8String PING = new Utf8String("PING");
        private final static Utf8String PONG = new Utf8String("PONG");
        private final int bufSize = 1024;
        private final long bufStart = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        private long buf = bufStart;
        private final DirectUtf8String flyweight = new DirectUtf8String();
        private int writtenLen;

        protected PongConnectionContext(SocketFactory socketFactory, NetworkFacade nf, Log log) {
            super(socketFactory, nf, log);
        }

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

        public void receivePing() throws PeerIsSlowToWriteException, PeerIsSlowToReadException, ServerDisconnectException {
            // expect "PING"
            int n = Net.recv(getFd(), buf, (int) (bufSize - (buf - bufStart)));
            if (n > 0) {
                flyweight.of(bufStart, buf + n);
                if (Utf8s.startsWith(PING, flyweight)) {
                    if (flyweight.size() < PING.size()) {
                        // accrue protocol artefacts while they still make sense
                        buf += n;
                        // fair resource use
                        throw registerDispatcherRead();
                    } else {
                        // reset buffer
                        this.buf = bufStart;
                        // send PONG by preparing the buffer and asking client to receive
                        LOG.info().$(flyweight).$();
                        Utf8s.strCpy(PONG, PONG.size(), bufStart);
                        writtenLen = PONG.size();
                        throw registerDispatcherWrite();
                    }
                } else {
                    throw registerDispatcherDisconnect(DISCONNECT_REASON_PROTOCOL_VIOLATION);
                }
            } else {
                // handle peer disconnect
                throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV);
            }
        }

        public void sendPong() throws PeerIsSlowToReadException, PeerIsSlowToWriteException, ServerDisconnectException {
            int n = Net.send(getFd(), buf, (int) (writtenLen - (buf - bufStart)));
            if (n > -1) {
                if (n > 0) {
                    buf += n;
                    if (buf - bufStart < writtenLen) {
                        throw registerDispatcherWrite();
                    } else {
                        flyweight.of(bufStart, bufStart + writtenLen);
                        LOG.info().$(flyweight).$();
                        buf = bufStart;
                        writtenLen = 0;
                        throw registerDispatcherRead();
                    }
                } else {
                    throw registerDispatcherWrite();
                }
            } else {
                // handle peer disconnect
                throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_SEND);
            }
        }
    }

    private static class PongRequestProcessor implements IORequestProcessor<PongConnectionContext> {
        @Override
        public boolean onRequest(int operation, PongConnectionContext context, IODispatcher<PongConnectionContext> dispatcher) {
            try {
                switch (operation) {
                    case IOOperation.READ:
                        context.receivePing();
                        break;
                    case IOOperation.WRITE:
                        context.sendPong();
                        break;
                    case IOOperation.HEARTBEAT:
                        dispatcher.registerChannel(context, IOOperation.HEARTBEAT);
                        return false;
                    default:
                        dispatcher.disconnect(context, DISCONNECT_REASON_UNKNOWN_OPERATION);
                        break;
                }
            } catch (PeerIsSlowToWriteException e) {
                dispatcher.registerChannel(context, IOOperation.READ);
            } catch (PeerIsSlowToReadException e) {
                dispatcher.registerChannel(context, IOOperation.WRITE);
            } catch (ServerDisconnectException e) {
                dispatcher.disconnect(context, context.getDisconnectReason());
            }
            return true;
        }
    }
}
