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

package io.questdb.cairo;

import io.questdb.DefaultTelemetryConfiguration;
import io.questdb.MessageBus;
import io.questdb.Metrics;
import io.questdb.TelemetryConfiguration;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.catalogue.DumpThreadStacksFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.rules.Timeout;
import org.junit.runner.Description;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AbstractCairoTest {

    protected static final PlanSink planSink = new PlanSink();
    protected static final StringSink sink = new StringSink();
    protected static final RecordCursorPrinter printer = new RecordCursorPrinter();
    protected static final Log LOG = LogFactory.getLog(AbstractCairoTest.class);
    private static final MicrosecondClock defaultMicrosecondClock = new X();
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    public static long writerAsyncCommandBusyWaitTimeout = -1;
    public static long writerAsyncCommandMaxTimeout = -1;
    protected static CharSequence root;
    protected static CairoConfiguration configuration;
    protected static MessageBus messageBus;
    protected static long currentMicros = -1;
    protected static MicrosecondClock testMicrosClock = defaultMicrosecondClock;
    protected static CairoEngine engine;
    protected static DatabaseSnapshotAgent snapshotAgent;
    protected static String inputRoot = null;
    protected static String inputWorkRoot = null;
    protected static int sqlCopyBufferSize = 1024 * 1024;
    protected static IOURingFacade ioURingFacade = IOURingFacadeImpl.INSTANCE;
    protected static FilesFacade ff;
    protected static CharSequence backupDir;
    protected static DateFormat backupDirTimestampFormat;
    protected static long configOverrideCommitLagMicros = -1;
    protected static int configOverrideMaxUncommittedRows = -1;
    protected static Metrics metrics = Metrics.enabled();
    protected static int capacity = -1;
    protected static int sampleByIndexSearchPageSize;
    protected static int binaryEncodingMaxLength = -1;
    protected static CharSequence defaultMapType;
    protected static int pageFrameMaxRows = -1;
    protected static int jitMode = SqlJitMode.JIT_MODE_ENABLED;
    protected static int rndFunctionMemoryPageSize = -1;
    protected static int rndFunctionMemoryMaxPages = -1;
    protected static String snapshotInstanceId = null;
    protected static Boolean snapshotRecoveryEnabled = null;
    protected static Boolean enableParallelFilter = null;
    protected static int queryCacheEventQueueCapacity = -1;
    protected static int pageFrameReduceShardCount = -1;
    protected static int pageFrameReduceQueueCapacity = -1;
    protected static int columnVersionTaskPoolCapacity = -1;
    protected static RostiAllocFacade rostiAllocFacade = null;
    protected static int parallelImportStatusLogKeepNDays = -1;
    protected static Boolean ioURingEnabled = null;

    @Rule
    public TestName testName = new TestName();
    public static long spinLockTimeout = -1;
    protected static boolean hideTelemetryTable = false;
    protected static int writerCommandQueueCapacity = 4;
    protected static long writerCommandQueueSlotSize = 2048L;
    protected static double columnPurgeRetryDelayMultiplier = -1;
    protected static long columnPurgeRetryDelay = -1;
    protected static int columnVersionPurgeQueueCapacity = -1;
    protected static int defaultTableWriteMode = -1;

    private static TelemetryConfiguration telemetryConfiguration;
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(20 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Rule
    public TestWatcher failedWatcher = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            // Flush all logs
            LogFactory.INSTANCE.flushJobs();
        }
    };

    @BeforeClass
    public static void setUpStatic() {
        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops
        // which causes memory leak detector to fail should logger be
        // created mid-test
        LOG.info().$("begin").$();
        try {
            root = temp.newFolder("dbRoot").getAbsolutePath();
        } catch (IOException e) {
            throw new ExceptionInInitializerError();
        }

        telemetryConfiguration = new DefaultTelemetryConfiguration() {
            @Override
            public boolean hideTables() {
                return hideTelemetryTable;
            }
        };

        configuration = new DefaultCairoConfiguration(root) {
            @Override
            public DateFormat getBackupDirTimestampFormat() {
                if (backupDirTimestampFormat != null) {
                    return backupDirTimestampFormat;
                }
                return super.getBackupDirTimestampFormat();
            }

            @Override
            public CharSequence getBackupRoot() {
                if (backupDir != null) {
                    return backupDir;
                }
                return super.getBackupRoot();
            }

            @Override
            public int getBinaryEncodingMaxLength() {
                return binaryEncodingMaxLength > 0 ? binaryEncodingMaxLength : super.getBinaryEncodingMaxLength();
            }

            @Override
            public double getColumnPurgeRetryDelayMultiplier() {
                return columnPurgeRetryDelayMultiplier > 0 ? columnPurgeRetryDelayMultiplier : 2.0;
            }

            @Override
            public long getColumnPurgeRetryDelay() {
                return columnPurgeRetryDelay > 0 ? columnPurgeRetryDelay : 10;
            }

            @Override
            public CharSequence getSnapshotInstanceId() {
                if (snapshotInstanceId == null) {
                    return super.getSnapshotInstanceId();
                }
                return snapshotInstanceId;
            }

            @Override
            public boolean isSnapshotRecoveryEnabled() {
                return snapshotRecoveryEnabled == null ? super.isSnapshotRecoveryEnabled() : snapshotRecoveryEnabled;
            }

            @Override
            public int getCopyPoolCapacity() {
                return capacity == -1 ? super.getCopyPoolCapacity() : capacity;
            }

            @Override
            public CharSequence getDefaultMapType() {
                if (defaultMapType == null) {
                    return super.getDefaultMapType();
                }
                return defaultMapType;
            }

            @Override
            public FilesFacade getFilesFacade() {
                if (ff != null) {
                    return ff;
                }
                return super.getFilesFacade();
            }

            @Override
            public CharSequence getSqlCopyInputRoot() {
                return inputRoot;
            }

            @Override
            public CharSequence getSqlCopyInputWorkRoot() {
                return inputWorkRoot;
            }

            @Override
            public long getCommitLag() {
                return configOverrideCommitLagMicros >= 0 ? configOverrideCommitLagMicros : super.getCommitLag();
            }

            @Override
            public long getSpinLockTimeout() {
                if (spinLockTimeout > -1) {
                    return spinLockTimeout;
                }
                return 5_000;
            }

            @Override
            public int getMaxUncommittedRows() {
                if (configOverrideMaxUncommittedRows >= 0) return configOverrideMaxUncommittedRows;
                return super.getMaxUncommittedRows();
            }

            @Override
            public MicrosecondClock getMicrosecondClock() {
                return testMicrosClock;
            }

            @Override
            public MillisecondClock getMillisecondClock() {
                return () -> testMicrosClock.getTicks() / 1000L;
            }

            @Override
            public int getPartitionPurgeListCapacity() {
                // Bump it to high number so that test don't fail with memory leak if LongList
                // re-allocates
                return 512;
            }

            @Override
            public int getSampleByIndexSearchPageSize() {
                return sampleByIndexSearchPageSize > 0 ? sampleByIndexSearchPageSize : super.getSampleByIndexSearchPageSize();
            }

            @Override
            public int getRndFunctionMemoryPageSize() {
                return rndFunctionMemoryPageSize < 0 ? super.getRndFunctionMemoryPageSize() : rndFunctionMemoryPageSize;
            }

            @Override
            public int getRndFunctionMemoryMaxPages() {
                return rndFunctionMemoryMaxPages < 0 ? super.getRndFunctionMemoryMaxPages() : rndFunctionMemoryMaxPages;
            }

            @Override
            public int getSqlJitMode() {
                return jitMode;
            }

            @Override
            public int getSqlPageFrameMaxRows() {
                return pageFrameMaxRows < 0 ? super.getSqlPageFrameMaxRows() : pageFrameMaxRows;
            }

            @Override
            public TelemetryConfiguration getTelemetryConfiguration() {
                return telemetryConfiguration;
            }

            @Override
            public long getWriterAsyncCommandBusyWaitTimeout() {
                return writerAsyncCommandBusyWaitTimeout < 0 ? super.getWriterAsyncCommandBusyWaitTimeout() : writerAsyncCommandBusyWaitTimeout;
            }

            @Override
            public long getWriterAsyncCommandMaxTimeout() {
                return writerAsyncCommandMaxTimeout < 0 ? super.getWriterAsyncCommandMaxTimeout() : writerAsyncCommandMaxTimeout;
            }

            @Override
            public int getWriterCommandQueueCapacity() {
                return writerCommandQueueCapacity;
            }

            @Override
            public long getWriterCommandQueueSlotSize() {
                return writerCommandQueueSlotSize;
            }

            @Override
            public int getQueryCacheEventQueueCapacity() {
                return queryCacheEventQueueCapacity < 0 ? super.getQueryCacheEventQueueCapacity() : queryCacheEventQueueCapacity;
            }

            @Override
            public int getPageFrameReduceShardCount() {
                return pageFrameReduceShardCount < 0 ? super.getPageFrameReduceShardCount() : pageFrameReduceShardCount;
            }

            @Override
            public int getPageFrameReduceQueueCapacity() {
                return pageFrameReduceQueueCapacity < 0 ? super.getPageFrameReduceQueueCapacity() : pageFrameReduceQueueCapacity;
            }

            @Override
            public int getColumnPurgeQueueCapacity() {
                return columnVersionPurgeQueueCapacity < 0 ? super.getColumnPurgeQueueCapacity() : columnVersionPurgeQueueCapacity;
            }

            @Override
            public boolean isSqlParallelFilterEnabled() {
                return enableParallelFilter != null ? enableParallelFilter : super.isSqlParallelFilterEnabled();
            }

            @Override
            public int getColumnPurgeTaskPoolCapacity() {
                return columnVersionTaskPoolCapacity >= 0 ? columnVersionTaskPoolCapacity : super.getColumnPurgeTaskPoolCapacity();
            }

            @Override
            public int getSqlCopyLogRetentionDays() {
                return parallelImportStatusLogKeepNDays >= 0 ? parallelImportStatusLogKeepNDays : super.getSqlCopyLogRetentionDays();
            }

            @Override
            public RostiAllocFacade getRostiAllocFacade() {
                return rostiAllocFacade != null ? rostiAllocFacade : super.getRostiAllocFacade();
            }

            @Override
            public boolean getWallEnabledDefault() {
                return defaultTableWriteMode < 0 ? super.getWallEnabledDefault() : defaultTableWriteMode == 1;
            }

            @Override
            public boolean isIOURingEnabled() {
                return ioURingEnabled != null ? ioURingEnabled : super.isIOURingEnabled();
            }

            @Override
            public int getSqlCopyBufferSize() {
                return sqlCopyBufferSize;
            }

            @Override
            public int getMetadataPoolCapacity() {
                return 1;
            }
        };
        engine = new CairoEngine(configuration, metrics);
        snapshotAgent = new DatabaseSnapshotAgent(engine);
        messageBus = engine.getMessageBus();
    }

    @AfterClass
    public static void tearDownStatic() {
        snapshotAgent = Misc.free(snapshotAgent);
        engine = Misc.free(engine);
        backupDir = null;
        backupDirTimestampFormat = null;
        DumpThreadStacksFunctionFactory.dumpThreadStacks();
    }

    @Before
    public void setUp() {
        SharedRandom.RANDOM.set(new Rnd());
        LOG.info().$("Starting test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        TestUtils.createTestPath(root);
        engine.getTableIdGenerator().open();
        engine.getTableIdGenerator().reset();
        SharedRandom.RANDOM.set(new Rnd());
    }

    @After
    public void tearDown() {
        LOG.info().$("Tearing down test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        snapshotAgent.clear();
        engine.getTableIdGenerator().close();
        engine.clear();
        TestUtils.removeTestPath(root);
        configOverrideMaxUncommittedRows = -1;
        configOverrideCommitLagMicros = -1;
        currentMicros = -1;
        testMicrosClock = defaultMicrosecondClock;
        sampleByIndexSearchPageSize = -1;
        defaultMapType = null;
        writerAsyncCommandBusyWaitTimeout = -1;
        writerAsyncCommandMaxTimeout = -1;
        pageFrameMaxRows = -1;
        jitMode = SqlJitMode.JIT_MODE_ENABLED;
        rndFunctionMemoryPageSize = -1;
        rndFunctionMemoryMaxPages = -1;
        spinLockTimeout = -1;
        snapshotInstanceId = null;
        snapshotRecoveryEnabled = null;
        enableParallelFilter = null;
        hideTelemetryTable = false;
        writerCommandQueueCapacity = 4;
        queryCacheEventQueueCapacity = -1;
        pageFrameReduceShardCount = -1;
        pageFrameReduceQueueCapacity = -1;
        columnPurgeRetryDelayMultiplier = -1;
        columnVersionPurgeQueueCapacity = -1;
        columnVersionTaskPoolCapacity = -1;
        rostiAllocFacade = null;
        sqlCopyBufferSize = 1024 * 1024;
        ioURingFacade = IOURingFacadeImpl.INSTANCE;
        ioURingEnabled = null;
        parallelImportStatusLogKeepNDays = -1;
        defaultTableWriteMode = -1;
        sink.clear();
    }

    protected static void configureForBackups() throws IOException {
        backupDir = temp.newFolder().getAbsolutePath();
        backupDirTimestampFormat = new TimestampFormatCompiler().compile("ddMMMyyyy");
    }

    protected static void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        assertMemoryLeak(null, code);
    }

    protected static void assertMemoryLeak(@Nullable FilesFacade ff, TestUtils.LeakProneCode code) throws Exception {
        final FilesFacade ff2 = ff;
        TestUtils.assertMemoryLeak(() -> {
            AbstractCairoTest.ff = ff2;
            try {
                code.run();
                engine.releaseInactive();
                Assert.assertEquals(0, engine.getBusyWriterCount());
                Assert.assertEquals(0, engine.getBusyReaderCount());
            } finally {
                engine.clear();
                AbstractCairoTest.ff = null;
            }
        });
    }

    protected void assertCursor(CharSequence expected, RecordCursor cursor, RecordMetadata metadata, boolean header) {
        TestUtils.assertCursor(expected, cursor, metadata, header, sink);
    }

    protected void assertCursorTwoPass(CharSequence expected, RecordCursor cursor, RecordMetadata metadata) {
        assertCursor(expected, cursor, metadata, true);
        cursor.toTop();
        assertCursor(expected, cursor, metadata, true);
    }

    private static class X implements MicrosecondClock {
        @Override
        public long getTicks() {
            return currentMicros >= 0 ? currentMicros : MicrosecondClockImpl.INSTANCE.getTicks();
        }
    }

    protected static void drainWalQueue() {
        ApplyWal2TableJob job = new ApplyWal2TableJob(engine);
        while (job.run(0)) {
            // run until empty
        }
    }

    protected static void clearWalQueue() {
        MessageBus bus = engine.getMessageBus();
        long cursor;
        while((cursor = bus.getWalTxnNotificationSubSequence().next()) > -1L) {
            bus.getWalTxnNotificationSubSequence().done(cursor);
        }
    }
}
