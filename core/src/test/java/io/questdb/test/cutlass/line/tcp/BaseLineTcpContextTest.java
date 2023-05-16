/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableReader;
import io.questdb.cutlass.auth.AuthenticatorFactory;
import io.questdb.cutlass.auth.EllipticCurveAuthenticatorFactory;
import io.questdb.cutlass.line.tcp.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.ByteCharSequence;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;


abstract class BaseLineTcpContextTest extends AbstractCairoTest {
    static final int FD = 1_000_000;
    static final Log LOG = LogFactory.getLog(BaseLineTcpContextTest.class);
    protected final AtomicInteger netMsgBufferSize = new AtomicInteger();
    protected boolean autoCreateNewColumns = true;
    protected boolean autoCreateNewTables = true;
    protected LineTcpConnectionContext context;
    protected boolean disconnectOnError;
    protected boolean disconnected;
    protected short floatDefaultColumnType;
    protected short integerDefaultColumnType;
    protected LineTcpReceiverConfiguration lineTcpConfiguration;
    protected long microSecondTicks;
    protected int nWriterThreads;
    protected NoNetworkIOJob noNetworkIOJob = new NoNetworkIOJob();
    protected String recvBuffer;
    protected LineTcpMeasurementScheduler scheduler;
    protected boolean stringAsTagSupported;
    protected boolean stringToCharCastAllowed;
    protected boolean symbolAsFieldSupported;
    protected WorkerPool workerPool;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        nWriterThreads = 2;
        microSecondTicks = -1;
        recvBuffer = null;
        disconnected = true;
        netMsgBufferSize.set(512);
        disconnectOnError = false;
        floatDefaultColumnType = ColumnType.DOUBLE;
        integerDefaultColumnType = ColumnType.LONG;
        autoCreateNewColumns = true;
        autoCreateNewTables = true;
        lineTcpConfiguration = createNoAuthReceiverConfiguration(provideLineTcpNetworkFacade());
    }

    private static WorkerPool createWorkerPool(final int workerCount, final boolean haltOnError) {
        return new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public long getSleepTimeout() {
                return 1;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public boolean haltOnError() {
                return haltOnError;
            }
        }, metrics.health());
    }

    protected void assertTable(CharSequence expected, String tableName) {
        try (TableReader reader = newTableReader(configuration, tableName)) {
            assertCursorTwoPass(expected, reader.getCursor(), reader.getMetadata());
        }
    }

    protected void closeContext() {
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

    protected LineTcpReceiverConfiguration createNoAuthReceiverConfiguration(NetworkFacade nf) {
        return createReceiverConfiguration(false, nf);
    }

    protected LineTcpReceiverConfiguration createReceiverConfiguration(final boolean withAuth, final NetworkFacade nf) {
        final FactoryProvider factoryProvider = new DefaultFactoryProvider() {
            @Override
            public AuthenticatorFactory getAuthenticatorFactory() {
                if (withAuth) {
                    URL u = getClass().getResource("authDb.txt");
                    assert u != null;
                    return new EllipticCurveAuthenticatorFactory(nf, u.getFile());
                }
                return super.getAuthenticatorFactory();
            }
        };
        return new DefaultLineTcpReceiverConfiguration() {
            @Override
            public boolean getAutoCreateNewColumns() {
                return autoCreateNewColumns;
            }

            @Override
            public boolean getAutoCreateNewTables() {
                return autoCreateNewTables;
            }

            @Override
            public short getDefaultColumnTypeForFloat() {
                return floatDefaultColumnType;
            }

            @Override
            public short getDefaultColumnTypeForInteger() {
                return integerDefaultColumnType;
            }

            @Override
            public boolean getDisconnectOnError() {
                return disconnectOnError;
            }

            @Override
            public FactoryProvider getFactoryProvider() {
                return factoryProvider;
            }

            @Override
            public int getMaxMeasurementSize() {
                return 128;
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

            @Override
            public int getNetMsgBufferSize() {
                return netMsgBufferSize.get();
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
            public boolean isStringAsTagSupported() {
                return stringAsTagSupported;
            }

            @Override
            public boolean isStringToCharCastAllowed() {
                return stringToCharCastAllowed;
            }

            @Override
            public boolean isSymbolAsFieldSupported() {
                return symbolAsFieldSupported;
            }

            @Override
            public boolean readOnlySecurityContext() {
                return false;
            }
        };
    }

    protected boolean handleContextIO() {
        switch (context.handleIO(noNetworkIOJob)) {
            case NEEDS_READ:
                context.getDispatcher().registerChannel(context, IOOperation.READ);
                break;
            case NEEDS_WRITE:
                context.getDispatcher().registerChannel(context, IOOperation.WRITE);
                break;
            case QUEUE_FULL:
                return true;
            case NEEDS_DISCONNECT:
                context.getDispatcher().disconnect(context, IODispatcher.DISCONNECT_REASON_PROTOCOL_VIOLATION);
                break;
        }
        context.commitWalTables(Long.MAX_VALUE);
        scheduler.doMaintenance(noNetworkIOJob.localTableUpdateDetailsByTableName, noNetworkIOJob.getWorkerId(), Long.MAX_VALUE);
        return false;
    }

    NetworkFacade provideLineTcpNetworkFacade() {
        return new LineTcpNetworkFacade();
    }

    protected void runInAuthContext(Runnable r) throws Exception {
        assertMemoryLeak(() -> {
            setupContext(null);
            try {
                r.run();
            } finally {
                closeContext();
            }
        });
    }

    protected void runInContext(Runnable r) throws Exception {
        runInContext(r, null);
    }

    protected void runInContext(Runnable r, Runnable onCommitNewEvent) throws Exception {
        runInContext(null, r, onCommitNewEvent);
    }

    protected void runInContext(FilesFacade ff, Runnable r, Runnable onCommitNewEvent) throws Exception {
        assertMemoryLeak(ff, () -> {
            setupContext(onCommitNewEvent);
            try {
                r.run();
            } finally {
                closeContext();
            }
        });
    }

    protected void setupContext(Runnable onCommitNewEvent) {
        disconnected = false;
        recvBuffer = null;
        scheduler = new LineTcpMeasurementScheduler(
                lineTcpConfiguration,
                engine,
                createWorkerPool(1, true),
                null,
                workerPool = createWorkerPool(nWriterThreads, false)
        ) {

            @Override
            public boolean scheduleEvent(
                    SecurityContext securityContext,
                    NetworkIOJob netIoJob,
                    LineTcpConnectionContext context,
                    LineTcpParser parser
            ) {
                if (null != onCommitNewEvent) {
                    onCommitNewEvent.run();
                }
                return super.scheduleEvent(securityContext, netIoJob, context, parser);
            }

            @Override
            protected NetworkIOJob createNetworkIOJob(IODispatcher<LineTcpConnectionContext> dispatcher, int workerId) {
                Assert.assertEquals(0, workerId);
                return noNetworkIOJob;
            }
        };
        noNetworkIOJob.setScheduler(scheduler);
        context = new LineTcpConnectionContext(lineTcpConfiguration, scheduler, metrics);
        Assert.assertNull(context.getDispatcher());
        context.of(FD, new IODispatcher<LineTcpConnectionContext>() {
            @Override
            public void close() {
            }

            @Override
            public void disconnect(LineTcpConnectionContext context, int reason) {
                disconnected = true;
            }

            @Override
            public int getConnectionCount() {
                return disconnected ? 0 : 1;
            }

            @Override
            public int getPort() {
                return 9009;
            }

            @Override
            public boolean isListening() {
                return true;
            }

            @Override
            public boolean processIOQueue(IORequestProcessor<LineTcpConnectionContext> processor) {
                return false;
            }

            @Override
            public void registerChannel(LineTcpConnectionContext context, int operation) {
            }

            @Override
            public boolean run(int workerId, @NotNull RunStatus runStatus) {
                return false;
            }
        });
        Assert.assertFalse(context.invalid());
        Assert.assertEquals(FD, context.getFd());
        workerPool.start(LOG);
    }

    protected void waitForIOCompletion() {
        recvBuffer = null;
        // Guard against slow writers on disconnect
        int maxIterations = 2000;
        while (maxIterations-- > 0) {
            if (!handleContextIO()) {
                break;
            }
            LockSupport.parkNanos(1_000_000);
        }
        Assert.assertTrue(maxIterations > 0);
        Assert.assertTrue(disconnected);
        // Wait for last commit
        Os.sleep(lineTcpConfiguration.getMaintenanceInterval() + 50);
    }

    static class NoNetworkIOJob implements NetworkIOJob {
        private final ByteCharSequenceObjHashMap<TableUpdateDetails> localTableUpdateDetailsByTableName = new ByteCharSequenceObjHashMap<>();
        private final ObjList<SymbolCache> unusedSymbolCaches = new ObjList<>();
        private LineTcpMeasurementScheduler scheduler;

        @Override
        public void addTableUpdateDetails(ByteCharSequence tableNameUtf8, TableUpdateDetails tableUpdateDetails) {
            localTableUpdateDetailsByTableName.put(tableNameUtf8, tableUpdateDetails);
        }

        @Override
        public void close() {
        }

        @Override
        public TableUpdateDetails getLocalTableDetails(DirectByteCharSequence tableName) {
            return localTableUpdateDetailsByTableName.get(tableName);
        }

        @Override
        public ObjList<SymbolCache> getUnusedSymbolCaches() {
            return unusedSymbolCaches;
        }

        @Override
        public int getWorkerId() {
            return 0;
        }

        @Override
        public void releaseWalTableDetails() {
            scheduler.releaseWalTableDetails(localTableUpdateDetailsByTableName);
        }

        @Override
        public TableUpdateDetails removeTableUpdateDetails(DirectByteCharSequence tableNameUtf8) {
            final int keyIndex = localTableUpdateDetailsByTableName.keyIndex(tableNameUtf8);
            if (keyIndex < 0) {
                TableUpdateDetails tud = localTableUpdateDetailsByTableName.valueAtQuick(keyIndex);
                localTableUpdateDetailsByTableName.removeAt(keyIndex);
                return tud;
            }
            return null;
        }

        @Override
        public boolean run(int workerId, @NotNull RunStatus runStatus) {
            Assert.fail("This is a mock job, not designed to run in a worker pool");
            return false;
        }

        public void setScheduler(LineTcpMeasurementScheduler scheduler) {
            this.scheduler = scheduler;
        }
    }

    class LineTcpNetworkFacade extends NetworkFacadeImpl {
        @Override
        public int recv(int fd, long buffer, int bufferLen) {
            Assert.assertEquals(FD, fd);
            if (recvBuffer == null) {
                return -1;
            }

            byte[] bytes = getBytes(recvBuffer);
            int n = 0;
            while (n < bufferLen && n < bytes.length) {
                Unsafe.getUnsafe().putByte(buffer++, bytes[n++]);
            }
            recvBuffer = new String(bytes, n, bytes.length - n);
            return n;
        }

        byte[] getBytes(String recvBuffer) {
            return recvBuffer.getBytes(StandardCharsets.UTF_8);
        }
    }
}
