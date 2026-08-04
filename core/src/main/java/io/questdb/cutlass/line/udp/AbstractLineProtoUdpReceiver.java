/*+*****************************************************************************
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

package io.questdb.cutlass.line.udp;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.str.Path;

import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractLineProtoUdpReceiver extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(AbstractLineProtoUdpReceiver.class);
    protected final AtomicBoolean acceptOpen;
    protected final LineUdpLexer lexer;
    protected final NetworkFacade nf;
    protected final LineUdpParserImpl parser;
    private final LineUdpReceiverConfiguration configuration;
    private final SOCountDownLatch halted = new SOCountDownLatch(1);
    private boolean isCommitAttempted;
    private boolean isLexerFreed;
    private boolean isParserFreed;
    private boolean isSocketClosed;
    private boolean isStartAttempted;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final SOCountDownLatch started = new SOCountDownLatch(1);
    private volatile boolean closed;
    private volatile boolean closedAcknowledged;
    protected int commitRate;
    protected long fd;
    protected long totalCount = 0;

    public AbstractLineProtoUdpReceiver(
            LineUdpReceiverConfiguration configuration,
            CairoEngine engine,
            WorkerPool workerPool
    ) {
        this(configuration, engine, workerPool, new AtomicBoolean(true));
    }

    public AbstractLineProtoUdpReceiver(
            LineUdpReceiverConfiguration configuration,
            CairoEngine engine,
            WorkerPool workerPool,
            AtomicBoolean acceptOpen
    ) {
        this.acceptOpen = acceptOpen;
        this.configuration = configuration;
        nf = configuration.getNetworkFacade();
        fd = nf.socketUdp();
        if (fd < 0) {
            int errno = nf.errno();
            LOG.error().$("cannot open UDP socket [errno=").$(errno).$(']').$();
            throw NetworkError.instance(errno, "Cannot open UDP socket");
        }

        try {
            // when listening for multicast packets bind address must be 0
            bind(configuration);
            this.commitRate = configuration.getCommitRate();

            if (configuration.getReceiveBufferSize() != -1 && nf.setRcvBuf(fd, configuration.getReceiveBufferSize()) != 0) {
                LOG.error()
                        .$("could not set receive buffer size [fd=").$(fd)
                        .$(", size=").$(configuration.getReceiveBufferSize())
                        .$(", errno=").$(configuration.getNetworkFacade().errno())
                        .I$();
            }

            lexer = new LineUdpLexer(configuration.getMsgBufferSize());
            parser = new LineUdpParserImpl(engine, configuration);
            lexer.withParser(parser);

            if (!configuration.ownThread()) {
                workerPool.assign(this);
                logStarted(configuration);
            }
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @Override
    public boolean run(@NotNull WorkerContext workerContext) {
        if (closed) {
            return super.run(workerContext);
        }
        if (!acceptOpen.get()) {
            return false;
        }
        return super.run(workerContext);
    }

    protected final boolean checkClosed() {
        if (closed) {
            closedAcknowledged = true;
            return true;
        }
        return false;
    }

    @Override
    public synchronized void close() {
        if (!isCloseComplete(0, false)) {
            throw new IllegalStateException("line UDP receiver did not halt");
        }
    }

    public synchronized boolean closeBy(long deadlineNanos) {
        return isCloseComplete(deadlineNanos, true);
    }

    public synchronized void start() {
        if (configuration.ownThread() && fd > -1 && !isSocketClosed && !isStartAttempted) {
            isStartAttempted = true;
            running.set(true);
            final Thread thread;
            try {
                thread = createThread(() -> {
                    started.countDown();
                    try {
                        if (configuration.ownThreadAffinity() != -1) {
                            Os.setCurrentThreadAffinity(configuration.ownThreadAffinity());
                        }
                        logStarted(configuration);
                        while (running.get()) {
                            runSerially();
                        }
                        LOG.info().$("shutdown").$();
                    } finally {
                        Path.clearThreadLocals();
                        halted.countDown();
                    }
                });
                thread.setName("line-udp-receiver");
            } catch (Throwable th) {
                resetFailedStart();
                throw th;
            }
            try {
                thread.start();
            } catch (Throwable th) {
                if (thread.getState() == Thread.State.NEW) {
                    resetFailedStart();
                }
                throw th;
            }
        }
    }

    protected Thread createThread(Runnable runnable) {
        return new Thread(runnable);
    }

    private void bind(LineUdpReceiverConfiguration configuration) {
        if (nf.bindUdp(fd, configuration.isUnicast() ? configuration.getBindIPv4Address() : 0, configuration.getPort())) {
            if (!configuration.isUnicast() && !nf.join(fd, configuration.getBindIPv4Address(), configuration.getGroupIPv4Address())) {
                throw NetworkError.instance(nf.errno())
                        .put("cannot join group ")
                        .put("[fd=").put(fd)
                        .put(", bind=").ip(configuration.getBindIPv4Address())
                        .put(", group=").ip(configuration.getGroupIPv4Address())
                        .put(']');
            }
        } else {
            throw NetworkError.instance(nf.errno()).couldNotBindSocket("udp-line-server", configuration.getBindIPv4Address(), configuration.getPort());
        }
    }

    private boolean isCloseComplete(long deadlineNanos, boolean isBounded) {
        if (fd > -1) {
            running.set(false);
            closed = true;
            if (isStartAttempted) {
                if (isBounded) {
                    if (!started.await(Math.max(0, deadlineNanos - System.nanoTime()))
                            || !halted.await(Math.max(0, deadlineNanos - System.nanoTime()))) {
                        return false;
                    }
                } else {
                    started.await();
                    halted.await();
                }
            }
            if (!isStartAttempted) {
                while (!closedAcknowledged) {
                    run();
                    if (closedAcknowledged) {
                        break;
                    }
                    if (isBounded && deadlineNanos - System.nanoTime() <= 0) {
                        return false;
                    }
                    Os.pause();
                }
            }
            if (!isSocketClosed) {
                if (nf.close(fd) != 0) {
                    try {
                        LOG.error().$("could not close [fd=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
                    } catch (Throwable ignore) {
                    }
                } else {
                    isSocketClosed = true;
                }
            }
            if (isBounded && deadlineNanos - System.nanoTime() <= 0) {
                return false;
            }
            Throwable cleanupFailure = null;
            if (!isCommitAttempted && parser != null) {
                try {
                    if (isBounded && !parser.isCommitAllComplete(deadlineNanos)) {
                        return false;
                    }
                    if (!isBounded) {
                        parser.commitAll();
                    }
                    isCommitAttempted = true;
                } catch (Throwable th) {
                    isCommitAttempted = true;
                    cleanupFailure = th;
                }
            }
            if (!isParserFreed) {
                try {
                    Misc.free(parser);
                    isParserFreed = true;
                } catch (Throwable th) {
                    if (cleanupFailure == null) {
                        cleanupFailure = th;
                    } else if (cleanupFailure != th) {
                        cleanupFailure.addSuppressed(th);
                    }
                }
            }
            if (!isLexerFreed) {
                try {
                    Misc.free(lexer);
                    isLexerFreed = true;
                } catch (Throwable th) {
                    if (cleanupFailure == null) {
                        cleanupFailure = th;
                    } else if (cleanupFailure != th) {
                        cleanupFailure.addSuppressed(th);
                    }
                }
            }
            if (isSocketClosed && isParserFreed && isLexerFreed) {
                final long closedFd = fd;
                fd = -1;
                try {
                    LOG.info().$("closed [fd=").$(closedFd).$(']').$();
                } catch (Throwable ignore) {
                }
            }
            CairoException.rethrowCleanupFailure(cleanupFailure);
            if (!isSocketClosed) {
                return false;
            }
        }
        return true;
    }

    private void logStarted(LineUdpReceiverConfiguration configuration) {
        if (configuration.isUnicast()) {
            LOG.info()
                    .$("receiving unicast on ")
                    .$ip(configuration.getBindIPv4Address())
                    .$(':')
                    .$(configuration.getPort())
                    .$(" [fd=").$(fd)
                    .$(", commitRate=").$(commitRate)
                    .I$();
        } else {
            LOG.info()
                    .$("receiving multicast from ")
                    .$ip(configuration.getGroupIPv4Address())
                    .$(':')
                    .$(configuration.getPort())
                    .$(" via ")
                    .$ip(configuration.getBindIPv4Address())
                    .$(" [fd=").$(fd)
                    .$(", commitRate=").$(commitRate)
                    .I$();
        }
    }

    private void resetFailedStart() {
        running.set(false);
        started.countDown();
        halted.countDown();
    }
}
