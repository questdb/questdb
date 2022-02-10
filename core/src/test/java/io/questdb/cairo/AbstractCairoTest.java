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

import io.questdb.MessageBus;
import io.questdb.Metrics;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.IOException;

public class AbstractCairoTest {

    protected static final StringSink sink = new StringSink();
    protected static final RecordCursorPrinter printer = new RecordCursorPrinter();
    protected final static Log LOG = LogFactory.getLog(AbstractCairoTest.class);
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    protected static CharSequence root;
    protected static CairoConfiguration configuration;
    protected static MessageBus messageBus;
    protected static long currentMicros = -1;
    protected final static MicrosecondClock testMicrosClock =
            () -> currentMicros >= 0 ? currentMicros : MicrosecondClockImpl.INSTANCE.getTicks();
    protected static CairoEngine engine;
    protected static String inputRoot = null;
    protected static FilesFacade ff;
    protected static long configOverrideCommitLagMicros = -1;
    protected static int configOverrideMaxUncommittedRows = -1;
    protected static Metrics metrics = Metrics.enabled();
    protected static int capacity = -1;
    protected static int sampleByIndexSearchPageSize;
    protected static int binaryEncodingMaxLength = -1;
    protected static CharSequence defaultMapType;
    protected static int pageFrameMaxSize = -1;

    @Rule
    public TestName testName = new TestName();
    public static long writerAsyncCommandBusyWaitTimeout = -1;
    public static long writerAsyncCommandMaxTimeout = -1;
    public static long spinLockTimeoutUs = -1;

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
        configuration = new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                if (ff != null) {
                    return ff;
                }
                return super.getFilesFacade();
            }

            @Override
            public int getCopyPoolCapacity() {
                return capacity == -1 ? super.getCopyPoolCapacity() : capacity;
            }

            @Override
            public MicrosecondClock getMicrosecondClock() {
                return testMicrosClock;
            }

            @Override
            public CharSequence getInputRoot() {
                return inputRoot;
            }

            @Override
            public long getCommitLag() {
                return configOverrideCommitLagMicros >= 0 ? configOverrideCommitLagMicros : super.getCommitLag();
            }

            @Override
            public long getSpinLockTimeoutUs() {
                if (spinLockTimeoutUs > -1) {
                    return spinLockTimeoutUs;
                }
                return 5_000_000;
            }

            @Override
            public int getMaxUncommittedRows() {
                if (configOverrideMaxUncommittedRows >= 0) return configOverrideMaxUncommittedRows;
                return super.getMaxUncommittedRows();
            }

            @Override
            public int getSampleByIndexSearchPageSize() {
                return sampleByIndexSearchPageSize > 0 ? sampleByIndexSearchPageSize : super.getSampleByIndexSearchPageSize();
            }

            @Override
            public int getBinaryEncodingMaxLength() {
                return binaryEncodingMaxLength > 0 ? binaryEncodingMaxLength : super.getBinaryEncodingMaxLength();
            }

            @Override
            public CharSequence getDefaultMapType() {
                if (defaultMapType == null) {
                    return super.getDefaultMapType();
                }
                return defaultMapType;
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
            public boolean enableDevelopmentUpdates() {
                return true;
            }

            @Override
            public int getSqlJitMode() {
                // JIT compiler is a beta feature and thus is disabled by default,
                // but we want to have it enabled in tests.
                return SqlJitMode.JIT_MODE_ENABLED;
            }

            @Override
            public int getSqlPageFrameMaxSize() {
                return pageFrameMaxSize < 0 ? super.getSqlPageFrameMaxSize() : pageFrameMaxSize;
            }

            @Override
            public int getPartitionPurgeListCapacity() {
                // Bump it to high number so that test don't fail with memory leak if LongList
                // re-allocates
                return 512;
            }
        };
        engine = new CairoEngine(configuration, metrics);
        messageBus = engine.getMessageBus();
    }

    @AfterClass
    public static void tearDownStatic() {
        engine = Misc.free(engine);
    }

    @Before
    public void setUp() {
        SharedRandom.RANDOM.set(new Rnd());
        LOG.info().$("Starting test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        TestUtils.createTestPath(root);
        engine.openTableId();
        engine.resetTableId();
        SharedRandom.RANDOM.set(new Rnd());
    }

    @After
    public void tearDown() {
        LOG.info().$("Tearing down test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        engine.freeTableId();
        engine.clear();
        TestUtils.removeTestPath(root);
        configOverrideMaxUncommittedRows = -1;
        configOverrideCommitLagMicros = -1;
        currentMicros = -1;
        sampleByIndexSearchPageSize = -1;
        defaultMapType = null;
        writerAsyncCommandBusyWaitTimeout = -1;
        writerAsyncCommandMaxTimeout = -1;
        pageFrameMaxSize = -1;
        spinLockTimeoutUs = -1;
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
}
