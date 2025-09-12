/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.Metrics;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableReader;
import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.cutlass.auth.EllipticCurveAuthenticatorFactory;
import io.questdb.cutlass.auth.LineAuthenticatorFactory;
import io.questdb.cutlass.line.tcp.DefaultLineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpConnectionContext;
import io.questdb.cutlass.line.tcp.LineTcpMeasurementScheduler;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.NetworkIOJob;
import io.questdb.cutlass.line.tcp.StaticChallengeResponseMatcher;
import io.questdb.cutlass.line.tcp.SymbolCache;
import io.questdb.cutlass.line.tcp.TableUpdateDetails;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.IODispatcher;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.Pool;
import io.questdb.std.Unsafe;
import io.questdb.std.Utf8StringObjHashMap;
import io.questdb.std.WeakClosableObjectPool;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.nanotime.NanosecondClockImpl;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;


abstract class BaseLineTcpContextTest extends AbstractCairoTest {
    static final int FD = 1_000_000;
    static final Log LOG = LogFactory.getLog(BaseLineTcpContextTest.class);
    protected final AtomicInteger maxRecvBufferSize = new AtomicInteger();
    protected boolean autoCreateNewColumns = true;
    protected boolean autoCreateNewTables = true;
    protected LineTcpConnectionContext context;
    protected boolean disconnectOnError;
    protected boolean disconnected;
    protected short floatDefaultColumnType;
    protected short integerDefaultColumnType;
    protected LineTcpReceiverConfiguration lineTcpConfiguration;
    protected int nWriterThreads;
    protected NoNetworkIOJob noNetworkIOJob;
    protected String recvBuffer;
    protected LineTcpMeasurementScheduler scheduler;
    protected boolean stringToCharCastAllowed;
    protected boolean symbolAsFieldSupported;
    protected long timestampTicks;
    protected TestTimestampType timestampType = TestTimestampType.MICRO;
    protected boolean useLegacyString;
    protected WorkerPool workerPool;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        nWriterThreads = 2;
        timestampTicks = -1;
        recvBuffer = null;
        disconnected = true;
        maxRecvBufferSize.set(512 * 1024);
        disconnectOnError = false;
        floatDefaultColumnType = ColumnType.DOUBLE;
        integerDefaultColumnType = ColumnType.LONG;
        useLegacyString = true;
        autoCreateNewColumns = true;
        autoCreateNewTables = true;
        lineTcpConfiguration = createNoAuthReceiverConfiguration(provideLineTcpNetworkFacade());
        noNetworkIOJob = new NoNetworkIOJob(lineTcpConfiguration);
    }

    private static WorkerPool createWorkerPool(final int workerCount, final boolean haltOnError, Metrics metrics) {
        return new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public Metrics getMetrics() {
                return metrics;
            }

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
        });
    }

    protected void assertTable(CharSequence expected, String tableName) {
        try (
                TableReader reader = newOffPoolReader(configuration, tableName);
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
            assertCursorTwoPass(expected, cursor, reader.getMetadata());
        }
    }

    protected void closeContext() {
        if (scheduler != null) {
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

    @SuppressWarnings("resource")
    protected LineTcpReceiverConfiguration createReceiverConfiguration(final boolean withAuth, final NetworkFacade nf) {
        final FactoryProvider factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull LineAuthenticatorFactory getLineAuthenticatorFactory() {
                if (withAuth) {
                    URL u = getClass().getResource("authDb.txt");
                    assert u != null;
                    CharSequenceObjHashMap<PublicKey> authDb = AuthUtils.loadAuthDb(u.getFile());
                    return new EllipticCurveAuthenticatorFactory(() -> new StaticChallengeResponseMatcher(authDb));
                }
                return super.getLineAuthenticatorFactory();
            }
        };
        return new DefaultLineTcpReceiverConfiguration(configuration) {
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
            public int getDefaultColumnTypeForTimestamp() {
                return timestampType.getTimestampType();
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
            public long getMaxRecvBufferSize() {
                return maxRecvBufferSize.get();
            }

            @Override
            public Clock getMicrosecondClock() {
                return new MicrosecondClockImplInner();
            }

            @Override
            public NetworkFacade getNetworkFacade() {
                return nf;
            }

            @Override
            public int getRecvBufferSize() {
                return Math.min(super.getRecvBufferSize(), (int) getMaxRecvBufferSize());
            }

            @Override
            public long getWriterIdleTimeout() {
                return 150;
            }

            @Override
            public boolean isStringToCharCastAllowed() {
                return stringToCharCastAllowed;
            }

            @Override
            public boolean isUseLegacyStringDefault() {
                return useLegacyString;
            }
        };
    }

    protected boolean handleContextIO0() {
        switch (context.handleIO(noNetworkIOJob)) {
            case QUEUE_FULL:
                return true;
            case NEEDS_DISCONNECT:
                disconnected = true;
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

    protected void runInContext(UnstableRunnable r) throws Exception {
        runInContext(r, null);
    }

    protected void runInContext(UnstableRunnable r, UnstableRunnable onCommitNewEvent) throws Exception {
        runInContext(null, r, onCommitNewEvent);
    }

    protected void runInContext(FilesFacade ff, UnstableRunnable r, UnstableRunnable onCommitNewEvent) throws Exception {
        assertMemoryLeak(ff, () -> {
            setupContext(onCommitNewEvent);
            try {
                r.run();
            } finally {
                closeContext();
            }
        });
    }

    protected void setupContext(UnstableRunnable onCommitNewEvent) throws TlsSessionInitFailedException {
        disconnected = false;
        recvBuffer = null;
        ((MicrosTimestampDriver) MicrosTimestampDriver.INSTANCE).setTicker(new MicrosecondClockImplInner());
        ((NanosTimestampDriver) NanosTimestampDriver.INSTANCE).setTicker(new NanosecondClockImplInner());

        scheduler = new LineTcpMeasurementScheduler(
                lineTcpConfiguration,
                engine,
                createWorkerPool(1, true, lineTcpConfiguration.getMetrics()),
                null,
                workerPool = createWorkerPool(nWriterThreads, false, lineTcpConfiguration.getMetrics())
        ) {

            @Override
            public boolean scheduleEvent(
                    SecurityContext securityContext,
                    NetworkIOJob netIoJob,
                    LineTcpConnectionContext context,
                    LineTcpParser parser
            ) throws Exception {
                if (onCommitNewEvent != null) {
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
        context = new LineTcpConnectionContext(lineTcpConfiguration, scheduler);
        context.of(FD);
        context.init();
        Assert.assertFalse(context.invalid());
        Assert.assertEquals(FD, context.getFd());
        workerPool.start(LOG);
    }

    protected void waitForIOCompletion() {
        recvBuffer = null;
        // Guard against slow writers on disconnect
        int maxIterations = 2000;
        while (maxIterations-- > 0) {
            if (!handleContextIO0()) {
                break;
            }
            LockSupport.parkNanos(1_000_000);
        }
        Assert.assertTrue(maxIterations > 0);
        Assert.assertTrue(disconnected);
        // Wait for last commit
        Os.sleep(lineTcpConfiguration.getMaintenanceInterval() + 50);
    }

    @FunctionalInterface
    public interface UnstableRunnable {
        void run() throws Exception;
    }

    static class NoNetworkIOJob implements NetworkIOJob {
        private final Utf8StringObjHashMap<TableUpdateDetails> localTableUpdateDetailsByTableName = new Utf8StringObjHashMap<>();
        private final WeakClosableObjectPool<SymbolCache> unusedSymbolCaches;
        private LineTcpMeasurementScheduler scheduler;

        NoNetworkIOJob(LineTcpReceiverConfiguration config) {
            unusedSymbolCaches = new WeakClosableObjectPool<>(() -> new SymbolCache(config), 10, true);
        }

        @Override
        public void addTableUpdateDetails(Utf8String tableNameUtf8, TableUpdateDetails tableUpdateDetails) {
            localTableUpdateDetailsByTableName.put(tableNameUtf8, tableUpdateDetails);
        }

        @Override
        public void close() {
        }

        @Override
        public TableUpdateDetails getLocalTableDetails(DirectUtf8Sequence tableName) {
            return localTableUpdateDetailsByTableName.get(tableName);
        }

        @Override
        public Pool<SymbolCache> getSymbolCachePool() {
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
        public void close(long fd, Log log) {
            Assert.assertEquals(FD, fd);
        }

        @Override
        public int recvRaw(long fd, long buffer, int bufferLen) {
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

    private class MicrosecondClockImplInner extends MicrosecondClockImpl {
        @Override
        public long getTicks() {
            if (timestampTicks >= 0) {
                return timestampTicks;
            }
            return super.getTicks();
        }
    }

    private class NanosecondClockImplInner extends NanosecondClockImpl {
        @Override
        public long getTicks() {
            if (timestampTicks >= 0) {
                return timestampTicks;
            }
            return super.getTicks();
        }
    }
}
