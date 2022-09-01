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

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;


abstract class BaseLineTcpContextTest extends AbstractCairoTest {
    static final Log LOG = LogFactory.getLog(BaseLineTcpContextTest.class);
    static final int FD = 1_000_000;
    protected final AtomicInteger netMsgBufferSize = new AtomicInteger();
    protected final NetworkIOJob NO_NETWORK_IO_JOB = new NetworkIOJob() {
        private final CharSequenceObjHashMap<TableUpdateDetails> localTableUpdateDetailsByTableName = new CharSequenceObjHashMap<>();
        private final ObjList<SymbolCache> unusedSymbolCaches = new ObjList<>();

        @Override
        public void addTableUpdateDetails(String tableNameUtf8, TableUpdateDetails tableUpdateDetails) {
            localTableUpdateDetailsByTableName.put(tableNameUtf8, tableUpdateDetails);
        }

        @Override
        public void close() {
        }

        @Override
        public TableUpdateDetails getLocalTableDetails(CharSequence tableName) {
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
        public boolean run(int workerId) {
            Assert.fail("This is a mock job, not designed to run in a worker pool");
            return false;
        }
    };
    protected LineTcpConnectionContext context;
    protected LineTcpReceiverConfiguration lineTcpConfiguration;
    protected LineTcpMeasurementScheduler scheduler;
    protected boolean disconnected;
    protected String recvBuffer;
    protected WorkerPool workerPool;
    protected int nWriterThreads;
    protected long microSecondTicks;
    protected boolean disconnectOnError;
    protected boolean stringToCharCastAllowed;
    protected boolean symbolAsFieldSupported;
    protected short floatDefaultColumnType;
    protected short integerDefaultColumnType;
    protected boolean autoCreateNewColumns = true;
    protected boolean autoCreateNewTables = true;

    @Before
    public void before() {
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

    NetworkFacade provideLineTcpNetworkFacade() {
        return new LineTcpNetworkFacade();
    }

    private static WorkerPool createWorkerPool(final int workerCount, final boolean haltOnError) {
        return new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public int[] getWorkerAffinity() {
                return TestUtils.getWorkerAffinity(workerCount);
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public boolean haltOnError() {
                return haltOnError;
            }
        }, metrics);
    }

    protected void assertTable(CharSequence expected, CharSequence tableName) {
        try (TableReader reader = new TableReader(configuration, tableName)) {
            assertCursorTwoPass(expected, reader.getCursor(), reader.getMetadata());
        }
    }

    protected void closeContext() {
        if (null != scheduler) {
            workerPool.close();
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
            public int getNetMsgBufferSize() {
                return netMsgBufferSize.get();
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
            public boolean getDisconnectOnError() {
                return disconnectOnError;
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
            public short getDefaultColumnTypeForFloat() {
                return floatDefaultColumnType;
            }

            @Override
            public short getDefaultColumnTypeForInteger() {
                return integerDefaultColumnType;
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
            public String getAuthDbPath() {
                if (withAuth) {
                    URL u = getClass().getResource("authDb.txt");
                    assert u != null;
                    return u.getFile();
                }
                return super.getAuthDbPath();
            }

            @Override
            public long getWriterIdleTimeout() {
                return 150;
            }
        };
    }

    protected boolean handleContextIO() {
        switch (context.handleIO(NO_NETWORK_IO_JOB)) {
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
        return false;
    }

    protected void runInAuthContext(Runnable r) throws Exception {
        assertMemoryLeak(() -> {
            setupContext(new AuthDb(lineTcpConfiguration), null);
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
        runInContext(null, r, null, onCommitNewEvent);
    }

    protected void runInContext(FilesFacade ff, Runnable r, AuthDb authDb, Runnable onCommitNewEvent) throws Exception {
        assertMemoryLeak(ff, () -> {
            setupContext(authDb, onCommitNewEvent);
            try {
                r.run();
            } finally {
                closeContext();
            }
        });
    }

    protected void setupContext(AuthDb authDb, Runnable onCommitNewEvent) {
        disconnected = false;
        recvBuffer = null;
        scheduler = new LineTcpMeasurementScheduler(
                lineTcpConfiguration,
                engine,
                createWorkerPool(1, true),
                null,
                workerPool = createWorkerPool(nWriterThreads, false)) {

            @Override
            protected NetworkIOJob createNetworkIOJob(IODispatcher<LineTcpConnectionContext> dispatcher, int workerId) {
                Assert.assertEquals(0, workerId);
                return NO_NETWORK_IO_JOB;
            }

            @Override
            boolean scheduleEvent(NetworkIOJob netIoJob, LineTcpParser parser) {
                if (null != onCommitNewEvent) {
                    onCommitNewEvent.run();
                }
                return super.scheduleEvent(netIoJob, parser);
            }
        };
        if (authDb == null) {
            context = new LineTcpConnectionContext(lineTcpConfiguration, scheduler, metrics);
        } else {
            context = new LineTcpAuthConnectionContext(lineTcpConfiguration, authDb, scheduler, metrics);
        }
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
            public boolean processIOQueue(IORequestProcessor<LineTcpConnectionContext> processor) {
                return false;
            }

            @Override
            public boolean isListening() {
                return true;
            }

            @Override
            public void registerChannel(LineTcpConnectionContext context, int operation) {
            }

            @Override
            public boolean run(int workerId) {
                return false;
            }
        });
        Assert.assertFalse(context.invalid());
        Assert.assertEquals(FD, context.getFd());
        workerPool.assignCleaner(Path.CLEANER);
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

    class LineTcpNetworkFacade extends NetworkFacadeImpl {
        @Override
        public int recv(long fd, long buffer, int bufferLen) {
            Assert.assertEquals(FD, fd);
            if (null == recvBuffer) {
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
