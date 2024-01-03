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

package io.questdb.test;

import io.questdb.FactoryProvider;
import io.questdb.MessageBus;
import io.questdb.Metrics;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.*;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.ExplainPlanFactory;
import io.questdb.griffin.engine.functions.catalogue.DumpThreadStacksFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.model.ExplainModel;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.cairo.*;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.rules.Timeout;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public abstract class AbstractCairoTest extends AbstractTest {
    protected static final Log LOG = LogFactory.getLog(AbstractCairoTest.class);
    protected static final PlanSink planSink = new TextPlanSink();
    protected static final RecordCursorPrinter printer = new RecordCursorPrinter();
    protected static final StringSink sink = new StringSink();
    private final static double EPSILON = 0.000001;
    private static final long[] SNAPSHOT = new long[MemoryTag.SIZE];
    private static final LongList rows = new LongList();
    public static long dataAppendPageSize = -1;
    public static int recreateDistressedSequencerAttempts = 3;
    public static long spinLockTimeout = -1;
    public static int walTxnNotificationQueueCapacity = -1;
    public static long writerAsyncCommandBusyWaitTimeout = -1;
    public static long writerAsyncCommandMaxTimeout = -1;
    protected static String attachableDirSuffix = null;
    protected static CharSequence backupDir;
    protected static DateFormat backupDirTimestampFormat;
    protected static int binaryEncodingMaxLength = -1;
    protected static BindVariableService bindVariableService;
    protected static NetworkSqlExecutionCircuitBreaker circuitBreaker;
    protected static SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration;
    protected static long columnPurgeRetryDelay = -1;
    protected static int columnVersionPurgeQueueCapacity = -1;
    protected static CairoConfiguration configuration;
    protected static TestCairoConfigurationFactory configurationFactory;
    protected static long currentMicros = -1;
    protected static final MicrosecondClock defaultMicrosecondClock = () -> currentMicros >= 0 ? currentMicros : MicrosecondClockImpl.INSTANCE.getTicks();
    protected static MicrosecondClock testMicrosClock = defaultMicrosecondClock;
    protected static CairoEngine engine;
    protected static TestCairoEngineFactory engineFactory;
    protected static FactoryProvider factoryProvider;
    protected static FilesFacade ff;
    protected static int groupByShardingThreshold = -1;
    protected static String inputRoot = null;
    protected static String inputWorkRoot = null;
    protected static IOURingFacade ioURingFacade = IOURingFacadeImpl.INSTANCE;
    protected static int maxOpenPartitions = -1;
    protected static MessageBus messageBus;
    protected static Metrics metrics;
    protected static QuestDBTestNode node1;
    protected static ObjList<QuestDBTestNode> nodes = new ObjList<>();
    protected static int pageFrameMaxRows = -1;
    protected static int pageFrameReduceQueueCapacity = -1;
    protected static int pageFrameReduceShardCount = -1;
    protected static SecurityContext securityContext;
    protected static String snapshotInstanceId = null;
    protected static Boolean snapshotRecoveryEnabled = null;
    protected static int sqlCopyBufferSize = 1024 * 1024;
    protected static SqlExecutionContext sqlExecutionContext;
    protected static int writerCommandQueueCapacity = 4;
    protected static long writerCommandQueueSlotSize = 2048L;
    static boolean[] FACTORY_TAGS = new boolean[MemoryTag.SIZE];
    private static long memoryUsage = -1;
    @Rule
    public final TestWatcher flushLogsOnFailure = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            LogFactory.getInstance().flushJobs();
        }
    };
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(20 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    public static boolean assertCursor(
            CharSequence expected,
            boolean supportsRandomAccess,
            boolean sizeExpected,
            boolean sizeCanBeVariable,
            RecordCursor cursor,
            RecordMetadata metadata,
            boolean fragmentedSymbolTables
    ) {
        return assertCursor(
                expected,
                supportsRandomAccess,
                sizeExpected,
                sizeCanBeVariable,
                cursor,
                metadata,
                sink,
                printer,
                rows,
                fragmentedSymbolTables
        );
    }

    // Thread-safe cursor assertion method.
    public static boolean assertCursor(
            CharSequence expected,
            boolean supportsRandomAccess,
            boolean sizeExpected,
            boolean sizeCanBeVariable,
            RecordCursor cursor,
            RecordMetadata metadata,
            StringSink sink,
            RecordCursorPrinter printer,
            LongList rows,
            boolean fragmentedSymbolTables
    ) {
        if (expected == null) {
            Assert.assertFalse(cursor.hasNext());
            cursor.toTop();
            Assert.assertFalse(cursor.hasNext());
            return true;
        }

        TestUtils.assertCursor(expected, cursor, metadata, true, sink);

        testSymbolAPI(metadata, cursor, fragmentedSymbolTables);
        cursor.toTop();
        testStringsLong256AndBinary(metadata, cursor);

        // test API where same record is being updated by cursor
        cursor.toTop();
        Record record = cursor.getRecord();
        Assert.assertNotNull(record);
        sink.clear();
        printer.printHeader(metadata, sink);
        long count = 0;
        long cursorSize = cursor.size();
        while (cursor.hasNext()) {
            printer.print(record, metadata, sink);
            count++;
        }

        if (!sizeCanBeVariable) {
            if (sizeExpected) {
                Assert.assertTrue("Concrete cursor size expected but was -1", cursorSize != -1);
            } else {
                Assert.assertTrue("Invalid/undetermined cursor size expected but was " + cursorSize, cursorSize <= 0);
            }
        }
        if (cursorSize != -1) {
            Assert.assertEquals("Actual cursor records vs cursor.size()", count, cursorSize);
        }

        TestUtils.assertEquals(expected, sink);

        if (supportsRandomAccess) {
            cursor.toTop();
            sink.clear();
            rows.clear();
            while (cursor.hasNext()) {
                rows.add(record.getRowId());
            }

            final Record rec = cursor.getRecordB();
            printer.printHeader(metadata, sink);
            for (int i = 0, n = rows.size(); i < n; i++) {
                cursor.recordAt(rec, rows.getQuick(i));
                printer.print(rec, metadata, sink);
            }

            TestUtils.assertEquals(expected, sink);

            sink.clear();

            final Record factRec = cursor.getRecordB();
            printer.printHeader(metadata, sink);
            for (int i = 0, n = rows.size(); i < n; i++) {
                cursor.recordAt(factRec, rows.getQuick(i));
                printer.print(factRec, metadata, sink);
            }

            TestUtils.assertEquals(expected, sink);

            // test that absolute positioning of record does not affect state of record cursor
            if (rows.size() > 0) {
                sink.clear();

                cursor.toTop();
                int target = rows.size() / 2;
                printer.printHeader(metadata, sink);
                while (target-- > 0 && cursor.hasNext()) {
                    printer.print(record, metadata, sink);
                }

                // no obliterate record with absolute positioning
                for (int i = 0, n = rows.size(); i < n; i++) {
                    cursor.recordAt(factRec, rows.getQuick(i));
                }

                // not continue normal fetch
                while (cursor.hasNext()) {
                    printer.print(record, metadata, sink);
                }

                TestUtils.assertEquals(expected, sink);
            }
        } else {
            try {
                cursor.getRecordB();
                Assert.fail();
            } catch (UnsupportedOperationException ignore) {
            }

            try {
                cursor.recordAt(record, 0);
                Assert.fail();
            } catch (UnsupportedOperationException ignore) {
            }
        }
        return false;
    }

    public static void assertReader(String expected, CharSequence tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            TestUtils.assertReader(expected, reader, sink);
        }
    }

    public static void assertVariableColumns(RecordCursorFactory factory, SqlExecutionContext executionContext) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            RecordMetadata metadata = factory.getMetadata();
            final int columnCount = metadata.getColumnCount();
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                for (int i = 0; i < columnCount; i++) {
                    switch (ColumnType.tagOf(metadata.getColumnType(i))) {
                        case ColumnType.STRING:
                            CharSequence a = record.getStr(i);
                            CharSequence b = record.getStrB(i);
                            if (a == null) {
                                Assert.assertNull(b);
                                Assert.assertEquals(TableUtils.NULL_LEN, record.getStrLen(i));
                            } else {
                                if (a instanceof AbstractCharSequence) {
                                    // AbstractCharSequence are usually mutable. We cannot have same mutable instance for A and B
                                    Assert.assertNotSame(a, b);
                                }
                                TestUtils.assertEquals(a, b);
                                Assert.assertEquals(a.length(), record.getStrLen(i));
                            }
                            break;
                        case ColumnType.BINARY:
                            BinarySequence s = record.getBin(i);
                            if (s == null) {
                                Assert.assertEquals(TableUtils.NULL_LEN, record.getBinLen(i));
                            } else {
                                Assert.assertEquals(s.length(), record.getBinLen(i));
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        assertFactoryMemoryUsage();
    }

    public static void configOverrideMangleTableDirNames(boolean mangle) {
        node1.getConfigurationOverrides().setMangleTableDirNames(mangle);
    }

    public static boolean doubleEquals(double a, double b, double epsilon) {
        return a == b || Math.abs(a - b) < epsilon;
    }

    public static boolean doubleEquals(double a, double b) {
        return doubleEquals(a, b, EPSILON);
    }

    //ignores:
    // o3, mmap - because they're usually linked with table readers that are kept in pool
    // join map memory - because it's usually a small and can't really be released until factory is closed
    // native sample by long list - because it doesn't seem to grow beyond initial size (10kb)
    public static long getMemUsedByFactories() {
        long memUsed = 0;
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            if (FACTORY_TAGS[i]) {
                memUsed += Unsafe.getMemUsedByTag(i);
            }
        }
        return memUsed;
    }

    public static void insert(CharSequence insertSql) throws SqlException {
        insert(insertSql, sqlExecutionContext);
    }

    public static void insert(CharSequence insertSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        engine.insert(insertSql, sqlExecutionContext);
    }

    public static void printFactoryMemoryUsageDiff() {
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            if (!FACTORY_TAGS[i]) {
                continue;
            }

            long value = Unsafe.getMemUsedByTag(i) - SNAPSHOT[i];

            if (value != 0L) {
                System.out.println(MemoryTag.nameOf(i) + ":" + value);
            }
        }
    }

    @SuppressWarnings("unused")
    public static void printMemoryUsage() {
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            System.err.print(MemoryTag.nameOf(i));
            System.err.print(":");
            System.err.println(Unsafe.getMemUsedByTag(i));
        }
    }

    @SuppressWarnings("unused")
    public static void printMemoryUsageDiff() {
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            long value = Unsafe.getMemUsedByTag(i) - SNAPSHOT[i];

            if (value != 0L) {
                System.err.print(MemoryTag.nameOf(i));
                System.err.print(":");
                System.err.println(value);
            }
        }
    }

    public static void refreshTablesInBaseEngine() {
        engine.reloadTableNames();
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops
        // which causes memory leak detector to fail should logger be
        // created mid-test
        AbstractTest.setUpStatic();
        nodes.clear();
        node1 = newNode(Chars.toString(root), false, 1, new StaticOverrides(), getEngineFactory(), getConfigurationFactory());
        configuration = node1.getConfiguration();
        securityContext = configuration.getFactoryProvider().getSecurityContextFactory().getRootContext();
        metrics = node1.getMetrics();
        engine = node1.getEngine();
        messageBus = node1.getMessageBus();

        node1.initGriffin(circuitBreaker);
        bindVariableService = node1.getBindVariableService();
        sqlExecutionContext = node1.getSqlExecutionContext();
    }

    public static void snapshotMemoryUsage() {
        memoryUsage = getMemUsedByFactories();

        for (int i = 0; i < MemoryTag.SIZE; i++) {
            SNAPSHOT[i] = Unsafe.getMemUsedByTag(i);
        }
    }

    @AfterClass
    public static void tearDownStatic() {
        forEachNode(QuestDBTestNode::closeCairo);
        circuitBreaker = Misc.free(circuitBreaker);
        nodes.clear();
        backupDir = null;
        backupDirTimestampFormat = null;
        factoryProvider = null;
        engineFactory = null;
        configurationFactory = null;
        AbstractTest.tearDownStatic();
        DumpThreadStacksFunctionFactory.dumpThreadStacks();
    }

    @Before
    public void setUp() {
        super.setUp();
        SharedRandom.RANDOM.set(new Rnd());
        forEachNode(QuestDBTestNode::setUpCairo);
        engine.resetNameRegistryMemory();
        refreshTablesInBaseEngine();
        SharedRandom.RANDOM.set(new Rnd());
        TestFilesFacadeImpl.resetTracking();
        memoryUsage = -1;
        forEachNode(QuestDBTestNode::setUpGriffin);
        sqlExecutionContext.setParallelFilterEnabled(configuration.isSqlParallelFilterEnabled());
    }

    @After
    public void tearDown() throws Exception {
        tearDown(true);
        super.tearDown();
    }

    public void tearDown(boolean removeDir) {
        LOG.info().$("Tearing down test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        forEachNode(node -> node.tearDownCairo(removeDir));
        ioURingFacade = IOURingFacadeImpl.INSTANCE;
        sink.clear();
        memoryUsage = -1;
        if (inputWorkRoot != null) {
            try (Path path = new Path().of(inputWorkRoot).$()) {
                if (Files.exists(path)) {
                    Files.rmdir(path, true);
                }
            }
        }
    }

    private static void assertCalculateSize(RecordCursorFactory factory) throws SqlException {
        long size;
        SqlExecutionCircuitBreaker circuitBreaker = sqlExecutionContext.getCircuitBreaker();
        RecordCursor.Counter counter = new RecordCursor.Counter();

        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            cursor.calculateSize(circuitBreaker, counter);
            size = counter.get();
            cursor.toTop();
            counter.clear();
            cursor.calculateSize(circuitBreaker, counter);
            long sizeAfterToTop = counter.get();

            Assert.assertEquals(size, sizeAfterToTop);
        }

        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            counter.clear();
            cursor.calculateSize(circuitBreaker, counter);
            long sizeAfterReopen = counter.get();

            Assert.assertEquals(size, sizeAfterReopen);
        }
    }

    private static void assertException0(CharSequence sql, SqlExecutionContext sqlExecutionContext, boolean fullFatJoins) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(fullFatJoins);
            CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
            if (cq.getRecordCursorFactory() != null) {
                try (
                        RecordCursorFactory factory = cq.getRecordCursorFactory();
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    cursor.hasNext();
                }
            } else {
                try (OperationFuture future = cq.execute(null)) {
                    future.await();
                }
            }
        }
        Assert.fail();
    }

    private static void assertSymbolColumnThreadSafety(int numberOfIterations, int symbolColumnCount, ObjList<SymbolTable> symbolTables, int[] symbolTableKeySnapshot, String[][] symbolTableValueSnapshot) {
        final Rnd rnd = TestUtils.generateRandom(null);
        for (int i = 0; i < numberOfIterations; i++) {
            int symbolColIndex = rnd.nextInt(symbolColumnCount);
            SymbolTable symbolTable = symbolTables.getQuick(symbolColIndex);
            int max = symbolTableKeySnapshot[symbolColIndex] + 1;
            // max could be -1 meaning we have nulls; max can also be 0, meaning only one symbol value
            // basing boundary on 2 we convert -1 tp 1 and 0 to 2
            int key = rnd.nextInt(max + 1) - 1;
            String expected = symbolTableValueSnapshot[symbolColIndex][key + 1];
            TestUtils.assertEquals(expected, symbolTable.valueOf(key));
            // now test static symbol table
            if (expected != null && symbolTable instanceof StaticSymbolTable) {
                StaticSymbolTable staticSymbolTable = (StaticSymbolTable) symbolTable;
                Assert.assertEquals(key, staticSymbolTable.keyOf(expected));
            }
        }
    }

    private static TestCairoConfigurationFactory getConfigurationFactory() {
        return configurationFactory != null ? configurationFactory : CairoTestConfiguration::new;
    }

    private static TestCairoEngineFactory getEngineFactory() {
        return engineFactory != null ? engineFactory : CairoEngine::new;
    }

    private static void testStringsLong256AndBinary(RecordMetadata metadata, RecordCursor cursor) {
        Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                switch (ColumnType.tagOf(metadata.getColumnType(i))) {
                    case ColumnType.STRING:
                        CharSequence s = record.getStr(i);
                        if (s != null) {
                            CharSequence b = record.getStrB(i);
                            if (b instanceof AbstractCharSequence) {
                                // AbstractCharSequence are usually mutable. We cannot have same mutable instance for A and B
                                Assert.assertNotSame("Expected string instances be different for getStr and getStrB", s, b);
                            }
                        } else {
                            Assert.assertNull(record.getStrB(i));
                            Assert.assertEquals(TableUtils.NULL_LEN, record.getStrLen(i));
                        }
                        break;
                    case ColumnType.BINARY:
                        BinarySequence bs = record.getBin(i);
                        if (bs != null) {
                            Assert.assertEquals(record.getBin(i).length(), record.getBinLen(i));
                        } else {
                            Assert.assertEquals(TableUtils.NULL_LEN, record.getBinLen(i));
                        }
                        break;
                    case ColumnType.LONG256:
                        Long256 l1 = record.getLong256A(i);
                        Long256 l2 = record.getLong256B(i);
                        if (l1 == Long256Impl.NULL_LONG256) {
                            Assert.assertSame(l1, l2);
                        } else {
                            Assert.assertNotSame(l1, l2);
                        }
                        Assert.assertEquals(l1.getLong0(), l2.getLong0());
                        Assert.assertEquals(l1.getLong1(), l2.getLong1());
                        Assert.assertEquals(l1.getLong2(), l2.getLong2());
                        Assert.assertEquals(l1.getLong3(), l2.getLong3());
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private static void testSymbolAPI(RecordMetadata metadata, RecordCursor cursor, boolean fragmentedSymbolTables) {
        IntList symbolIndexes = null;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                if (symbolIndexes == null) {
                    symbolIndexes = new IntList();
                }
                symbolIndexes.add(i);
            }
        }

        if (symbolIndexes != null) {

            // create new symbol tables and make sure they are not the same
            // as the default ones

            ObjList<SymbolTable> clonedSymbolTables = new ObjList<>();
            ObjList<SymbolTable> originalSymbolTables = new ObjList<>();
            int[] symbolTableKeySnapshot = new int[symbolIndexes.size()];
            String[][] symbolTableValueSnapshot = new String[symbolIndexes.size()][];
            try {
                cursor.toTop();
                if (!fragmentedSymbolTables && cursor.hasNext()) {
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        final int columnIndex = symbolIndexes.getQuick(i);
                        originalSymbolTables.add(cursor.getSymbolTable(columnIndex));
                    }

                    // take snapshot of symbol tables
                    // multiple passes over the same cursor, if not very efficient, we
                    // can swap loops around
                    int sumOfMax = 0;
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        cursor.toTop();
                        final Record rec = cursor.getRecord();
                        final int column = symbolIndexes.getQuick(i);
                        int max = -1;
                        while (cursor.hasNext()) {
                            max = Math.max(max, rec.getInt(column));
                        }
                        String[] values = new String[max + 2];
                        final SymbolTable symbolTable = cursor.getSymbolTable(column);
                        for (int k = -1; k <= max; k++) {
                            values[k + 1] = Chars.toString(symbolTable.valueOf(k));
                        }
                        symbolTableKeySnapshot[i] = max;
                        symbolTableValueSnapshot[i] = values;
                        sumOfMax += max;
                    }

                    // We grab clones after iterating through the symbol values due to
                    // the cache warm up required by Cast*ToSymbolFunctionFactory functions.
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        final int columnIndex = symbolIndexes.getQuick(i);
                        SymbolTable tab = cursor.newSymbolTable(columnIndex);
                        Assert.assertNotNull(tab);
                        clonedSymbolTables.add(tab);
                    }

                    // Now start two threads, one will be using normal symbol table
                    // another will be using a clone. Threads will randomly check that
                    // symbol table is able to convert keys to values without problems

                    int numberOfIterations = sumOfMax * 2;
                    int symbolColumnCount = symbolIndexes.size();
                    int workerCount = 2;
                    CyclicBarrier barrier = new CyclicBarrier(workerCount);
                    SOCountDownLatch doneLatch = new SOCountDownLatch(workerCount);
                    AtomicInteger errorCount = new AtomicInteger(0);

                    // thread that is hitting clones
                    new Thread(() -> {
                        try {
                            TestUtils.await(barrier);
                            assertSymbolColumnThreadSafety(numberOfIterations, symbolColumnCount, clonedSymbolTables, symbolTableKeySnapshot, symbolTableValueSnapshot);
                        } catch (Throwable e) {
                            errorCount.incrementAndGet();
                            //noinspection CallToPrintStackTrace
                            e.printStackTrace();
                        } finally {
                            doneLatch.countDown();
                        }
                    }).start();

                    // thread that is hitting the original symbol tables
                    new Thread(() -> {
                        try {
                            TestUtils.await(barrier);
                            assertSymbolColumnThreadSafety(numberOfIterations, symbolColumnCount, originalSymbolTables, symbolTableKeySnapshot, symbolTableValueSnapshot);
                        } catch (Throwable e) {
                            errorCount.incrementAndGet();
                            //noinspection CallToPrintStackTrace
                            e.printStackTrace();
                        } finally {
                            doneLatch.countDown();
                        }
                    }).start();

                    doneLatch.await();

                    Assert.assertEquals(0, errorCount.get());
                }

                cursor.toTop();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    for (int i = 0, n = symbolIndexes.size(); i < n; i++) {
                        int column = symbolIndexes.getQuick(i);
                        SymbolTable symbolTable = cursor.getSymbolTable(column);
                        if (symbolTable instanceof StaticSymbolTable) {
                            CharSequence sym = Chars.toString(record.getSym(column));
                            int value = record.getInt(column);
                            if (((StaticSymbolTable) symbolTable).containsNullValue() && value == ((StaticSymbolTable) symbolTable).getSymbolCount()) {
                                Assert.assertEquals(Integer.MIN_VALUE, ((StaticSymbolTable) symbolTable).keyOf(sym));
                            } else {
                                Assert.assertEquals(value, ((StaticSymbolTable) symbolTable).keyOf(sym));
                            }
                            TestUtils.assertEquals(sym, symbolTable.valueOf(value));
                        } else {
                            final int value = record.getInt(column);
                            TestUtils.assertEquals(record.getSym(column), symbolTable.valueOf(value));
                        }
                    }
                }
            } finally {
                Misc.freeObjListIfCloseable(clonedSymbolTables);
            }
        }
    }

    private void assertQuery(
            SqlCompiler compiler,
            String expected,
            String query,
            String expectedTimestamp,
            SqlExecutionContext sqlExecutionContext,
            boolean supportsRandomAccess,
            boolean expectSize
    ) throws SqlException {
        snapshotMemoryUsage();
        try (final RecordCursorFactory factory = CairoEngine.select(compiler, query, sqlExecutionContext)) {
            assertFactoryCursor(
                    expected,
                    expectedTimestamp,
                    factory,
                    supportsRandomAccess,
                    sqlExecutionContext,
                    expectSize,
                    false
            );
        }
    }

    protected static void addColumn(TableWriterAPI writer, String columnName, int columnType) {
        AlterOperationBuilder addColumnC = new AlterOperationBuilder().ofAddColumn(0, writer.getTableToken(), 0);
        addColumnC.ofAddColumn(columnName, 1, columnType, 0, false, false, 0);
        writer.apply(addColumnC.build(), true);
    }

    protected static void assertCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            boolean sizeExpected,
            boolean sizeCanBeVariable, // this means size() can either be -1 in some cases or known in others
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        boolean cursorAsserted;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertEquals("supports random access", supportsRandomAccess, factory.recordCursorSupportsRandomAccess());
            cursorAsserted = assertCursor(expected, supportsRandomAccess, sizeExpected, sizeCanBeVariable, cursor, factory.getMetadata(), factory.fragmentedSymbolTables());
        }

        assertFactoryMemoryUsage();

        if (cursorAsserted) {
            return;
        }

        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            testSymbolAPI(factory.getMetadata(), cursor, factory.fragmentedSymbolTables());
        }

        assertFactoryMemoryUsage();
    }

    protected static void assertCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            boolean expectSize
    ) throws SqlException {
        assertCursor(expected, factory, supportsRandomAccess, expectSize, false);
    }

    protected static void assertCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            boolean expectSize,
            boolean sizeCanBeVariable
    ) throws SqlException {
        assertCursor(expected, factory, supportsRandomAccess, expectSize, sizeCanBeVariable, sqlExecutionContext);
    }

    protected static void assertCursorRawRecords(Record[] expected, RecordCursorFactory factory, boolean expectSize) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            if (expected == null) {
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
                Assert.assertFalse(cursor.hasNext());
                return;
            }

            final long rowsCount = cursor.size();
            if (expectSize) {
                Assert.assertEquals(rowsCount, expected.length);
            }

            RecordMetadata metadata = factory.getMetadata();

            testSymbolAPI(metadata, cursor, factory.fragmentedSymbolTables());
            cursor.toTop();
            testStringsLong256AndBinary(metadata, cursor);

            cursor.toTop();
            final Record record = cursor.getRecord();
            Assert.assertNotNull(record);
            int expectedRow = 0;
            while (cursor.hasNext()) {
                for (int col = 0, n = metadata.getColumnCount(); col < n; col++) {
                    switch (ColumnType.tagOf(metadata.getColumnType(col))) {
                        case ColumnType.BOOLEAN:
                            Assert.assertEquals(expected[expectedRow].getBool(col), record.getBool(col));
                            break;
                        case ColumnType.BYTE:
                            Assert.assertEquals(expected[expectedRow].getByte(col), record.getByte(col));
                            break;
                        case ColumnType.SHORT:
                            Assert.assertEquals(expected[expectedRow].getShort(col), record.getShort(col));
                            break;
                        case ColumnType.CHAR:
                            Assert.assertEquals(expected[expectedRow].getChar(col), record.getChar(col));
                            break;
                        case ColumnType.INT:
                            Assert.assertEquals(expected[expectedRow].getInt(col), record.getInt(col));
                            break;
                        case ColumnType.LONG:
                            Assert.assertEquals(expected[expectedRow].getLong(col), record.getLong(col));
                            break;
                        case ColumnType.DATE:
                            Assert.assertEquals(expected[expectedRow].getDate(col), record.getDate(col));
                            break;
                        case ColumnType.TIMESTAMP:
                            Assert.assertEquals(expected[expectedRow].getTimestamp(col), record.getTimestamp(col));
                            break;
                        case ColumnType.FLOAT:
                            Assert.assertTrue(doubleEquals(expected[expectedRow].getFloat(col), record.getFloat(col)));
                            break;
                        case ColumnType.DOUBLE:
                            Assert.assertTrue(doubleEquals(expected[expectedRow].getDouble(col), record.getDouble(col)));
                            break;
                        case ColumnType.STRING:
                            TestUtils.assertEquals(expected[expectedRow].getStr(col), record.getStr(col));
                            break;
                        case ColumnType.SYMBOL:
                            TestUtils.assertEquals(expected[expectedRow].getSym(col), record.getSym(col));
                            break;
                        case ColumnType.IPv4:
                            Assert.assertEquals(expected[expectedRow].getIPv4(col), record.getIPv4(col));
                            break;
                        case ColumnType.LONG256:
                            Long256 l1 = expected[expectedRow].getLong256A(col);
                            Long256 l2 = record.getLong256A(col);
                            Assert.assertEquals(l1.getLong0(), l2.getLong0());
                            Assert.assertEquals(l1.getLong1(), l2.getLong1());
                            Assert.assertEquals(l1.getLong2(), l2.getLong2());
                            Assert.assertEquals(l1.getLong3(), l2.getLong3());
                            break;
                        case ColumnType.BINARY:
                            TestUtils.assertEquals(expected[expectedRow].getBin(col), record.getBin(col), record.getBin(col).length());
                        default:
                            Assert.fail("Unknown column type");
                            break;
                    }
                }
                expectedRow++;
            }
            Assert.assertTrue((expectSize && rowsCount != -1) || (!expectSize && rowsCount == -1));
            Assert.assertTrue(rowsCount == -1 || expectedRow == rowsCount);
        }
        assertFactoryMemoryUsage();
    }

    protected static void assertException(CharSequence sql) throws Exception {
        assertException0(sql, sqlExecutionContext, false);
    }

    protected static void assertException(CharSequence sql, SqlExecutionContext executionContext) throws Exception {
        assertException(sql, executionContext, false);
    }

    protected static void assertException(CharSequence sql, SqlExecutionContext executionContext, boolean fullFatJoins) throws Exception {
        assertException0(sql, executionContext, fullFatJoins);
    }

    protected static void assertException(CharSequence sql, int errorPos, CharSequence contains) throws Exception {
        assertMemoryLeak(() -> assertException(sql, errorPos, contains, false));
    }

    protected static void assertException(CharSequence sql, int errorPos, CharSequence contains, boolean fullFatJoins) throws Exception {
        try {
            assertException(sql, sqlExecutionContext, fullFatJoins);
        } catch (Throwable e) {
            if (e instanceof FlyweightMessageContainer) {
                TestUtils.assertContains(((FlyweightMessageContainer) e).getFlyweightMessage(), contains);
                if (errorPos > -1) {
                    Assert.assertEquals(errorPos, ((FlyweightMessageContainer) e).getPosition());
                }
            } else {
                throw e;
            }
        }
    }

    protected static void assertExceptionNoLeakCheck(CharSequence sql, int errorPos) throws Exception {
        assertException(sql, errorPos, "inconvertible types: TIMESTAMP -> INT", false);
    }

    protected static void assertFactoryMemoryUsage() {
        if (memoryUsage > -1) {
            long memAfterCursorClose = getMemUsedByFactories();
            long limit = memoryUsage + 64 * 1024;
            if (memAfterCursorClose > limit) {
                dumpMemoryUsage();
                printFactoryMemoryUsageDiff();
                Assert.fail("cursor memory usage should be less or equal " + limit + " but was " + memAfterCursorClose + ". Diff " + (memAfterCursorClose - memoryUsage));
            }
        }
    }

    protected static void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        engine.clear();
        assertMemoryLeak(AbstractCairoTest.ff, code);
    }

    protected static void assertMemoryLeak(@Nullable FilesFacade ff, TestUtils.LeakProneCode code) throws Exception {
        final FilesFacade ff2 = ff;
        final FilesFacade ffBefore = AbstractCairoTest.ff;
        TestUtils.assertMemoryLeak(() -> {
            AbstractCairoTest.ff = ff2;
            try {
                code.run();
                forEachNode(node -> releaseInactive(node.getEngine()));
            } finally {
                forEachNode(node -> node.getEngine().clear());
                AbstractCairoTest.ff = ffBefore;
            }
        });
    }

    protected static void assertQuery(Record[] expected, CharSequence query, CharSequence ddl, @Nullable CharSequence expectedTimestamp, boolean expectSize) throws Exception {
        assertQuery(expected, query, ddl, expectedTimestamp, null, null, expectSize);
    }

    protected static void assertQuery(CharSequence expected, CharSequence query, CharSequence ddl, @Nullable CharSequence expectedTimestamp) throws Exception {
        assertQuery(expected, query, ddl, expectedTimestamp, null, null, true, false, false);
    }

    protected static void assertQuery(CharSequence expected, CharSequence query, CharSequence ddl, @Nullable CharSequence expectedTimestamp, boolean supportsRandomAccess) throws Exception {
        assertQuery(expected, query, ddl, expectedTimestamp, null, null, supportsRandomAccess, false, false);
    }

    /**
     * expectedTimestamp can either be exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2,
            boolean supportsRandomAccess
    ) throws Exception {
        assertQuery(expected, query, ddl, expectedTimestamp, ddl2, expected2, supportsRandomAccess, false, false);
    }

    protected static void assertQuery(
            Record[] expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable Record[] expected2,
            boolean expectSize
    ) throws Exception {
        assertMemoryLeak(() -> {
            if (ddl != null) {
                compile(ddl);
            }

            snapshotMemoryUsage();

            RecordCursorFactory factory = select(query);
            try {
                assertTimestamp(expectedTimestamp, factory);
                assertCursorRawRecords(expected, factory, expectSize);
                // make sure we get the same outcome when we get factory to create new cursor
                assertCursorRawRecords(expected, factory, expectSize);
                // make sure strings, binary fields and symbols are compliant with expected record behaviour
                assertVariableColumns(factory, sqlExecutionContext);

                if (ddl2 != null) {
                    ddl(ddl2, sqlExecutionContext);

                    int count = 3;
                    while (count > 0) {
                        try {
                            assertCursorRawRecords(expected2, factory, expectSize);
                            // and again
                            assertCursorRawRecords(expected2, factory, expectSize);
                            return;
                        } catch (TableReferenceOutOfDateException e) {
                            Misc.free(factory);
                            factory = select(query);
                            count--;
                        }
                    }
                }
            } finally {
                Misc.free(factory);
            }
        });
    }

    protected static void assertQuery(
            CharSequence expected,
            CharSequence query,
            CharSequence ddl,
            @Nullable CharSequence expectedTimestamp,
            @Nullable CharSequence ddl2,
            @Nullable CharSequence expected2,
            boolean supportsRandomAccess,
            boolean expectSize,
            boolean sizeCanBeVariable
    ) throws Exception {
        assertMemoryLeak(() -> {
            if (ddl != null) {
                compile(ddl);
                if (configuration.getWalEnabledDefault()) {
                    drainWalQueue();
                }
            }
            printSqlResult(() -> expected, query, expectedTimestamp, ddl2, expected2, supportsRandomAccess, expectSize, sizeCanBeVariable, null);
        });
    }

    /**
     * expectedTimestamp can either be exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertQuery(CharSequence expected, CharSequence query, CharSequence ddl, @Nullable CharSequence expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws Exception {
        assertQuery(expected, query, ddl, expectedTimestamp, null, null, supportsRandomAccess, expectSize, false);
    }

    protected static void assertQueryExpectSize(CharSequence expected, CharSequence query, CharSequence ddl) throws Exception {
        assertQuery(expected, query, ddl, null, null, null, true, true, false);
    }

    protected static void assertSqlCursors(CharSequence expectedSql, CharSequence actualSql) throws SqlException {
        try (SqlCompiler sqlCompiler = engine.getSqlCompiler()) {
            TestUtils.assertSqlCursors(
                    sqlCompiler,
                    sqlExecutionContext,
                    expectedSql,
                    actualSql,
                    LOG
            );
        }
    }

    /**
     * expectedTimestamp can either be exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertTimestamp(CharSequence expectedTimestamp, RecordCursorFactory factory) throws SqlException {
        assertTimestamp(expectedTimestamp, factory, sqlExecutionContext);
    }

    /**
     * expectedTimestamp can either be exact column name or in columnName###ord format, where ord is either ASC or DESC and specifies expected order.
     */
    protected static void assertTimestamp(CharSequence expectedTimestamp, RecordCursorFactory factory, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (expectedTimestamp == null || expectedTimestamp.length() == 0) {
            int timestampIdx = factory.getMetadata().getTimestampIndex();
            if (timestampIdx != -1) {
                Assert.fail("Expected no timestamp but found " + factory.getMetadata().getColumnName(timestampIdx) + ", idx=" + timestampIdx);
            }
        } else {
            boolean expectAscendingOrder = true;
            String tsDesc = expectedTimestamp.toString();
            int position = tsDesc.indexOf("###");
            if (position > 0) {
                expectedTimestamp = tsDesc.substring(0, position);
                expectAscendingOrder = tsDesc.substring(position + 3).equalsIgnoreCase("asc");
            }

            int index = factory.getMetadata().getColumnIndexQuiet(expectedTimestamp);
            Assert.assertTrue("Column '" + expectedTimestamp + "' can't be found in metadata", index > -1);
            Assert.assertNotEquals("Expected non-negative value as timestamp index", -1, index);
            Assert.assertEquals("Timestamp column index", index, factory.getMetadata().getTimestampIndex());
            assertTimestampColumnValues(factory, sqlExecutionContext, expectAscendingOrder);
        }
    }

    protected static void assertTimestampColumnValues(RecordCursorFactory factory, SqlExecutionContext sqlExecutionContext, boolean isAscending) throws SqlException {
        int index = factory.getMetadata().getTimestampIndex();
        long timestamp = isAscending ? Long.MIN_VALUE : Long.MAX_VALUE;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            long c = 0;
            while (cursor.hasNext()) {
                long ts = record.getTimestamp(index);
                if ((isAscending && timestamp > ts) || (!isAscending && timestamp < ts)) {

                    StringSink error = new StringSink();
                    error.put("record # ").put(c).put(" should have ").put(isAscending ? "bigger" : "smaller").put(" (or equal) timestamp than the row before. Values prior=");
                    TimestampFormatUtils.appendDateTimeUSec(error, timestamp);
                    error.put(" current=");
                    TimestampFormatUtils.appendDateTimeUSec(error, ts);

                    Assert.fail(error.toString());
                }
                timestamp = ts;
                c++;
            }
        }
        assertFactoryMemoryUsage();
    }

    protected static void compile(CharSequence sql) throws SqlException {
        engine.compile(sql, sqlExecutionContext);
    }

    protected static void compile(CharSequence sql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        engine.compile(sql, sqlExecutionContext);
    }

    protected static void compile(SqlCompiler compiler, CharSequence sql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        CairoEngine.compile(compiler, sql, sqlExecutionContext);
    }

    protected static void configOverrideColumnPreTouchEnabled(Boolean columnPreTouchEnabled) {
        node1.getConfigurationOverrides().setColumnPreTouchEnabled(columnPreTouchEnabled);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideColumnVersionTaskPoolCapacity(int columnVersionTaskPoolCapacity) {
        node1.getConfigurationOverrides().setColumnVersionTaskPoolCapacity(columnVersionTaskPoolCapacity);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideCopyPartitionOnAttach(Boolean copyPartitionOnAttach) {
        node1.getConfigurationOverrides().setCopyPartitionOnAttach(copyPartitionOnAttach);
    }

    protected static void configOverrideDefaultMapType(CharSequence defaultMapType) {
        node1.getConfigurationOverrides().setDefaultMapType(defaultMapType);
    }

    protected static void configOverrideDefaultTableWriteMode(int defaultTableWriteMode) {
        node1.getConfigurationOverrides().setDefaultTableWriteMode(defaultTableWriteMode);
    }

    protected static void configOverrideEnv(Map<String, String> env) {
        node1.getConfigurationOverrides().setEnv(env);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideHideTelemetryTable(boolean hideTelemetryTable) {
        node1.getConfigurationOverrides().setHideTelemetryTable(hideTelemetryTable);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideIoURingEnabled(Boolean ioURingEnabled) {
        node1.getConfigurationOverrides().setIoURingEnabled(ioURingEnabled);
    }

    protected static void configOverrideJitMode(int jitMode) {
        node1.getConfigurationOverrides().setJitMode(jitMode);
    }

    protected static void configOverrideMaxUncommittedRows(int maxUncommittedRows) {
        node1.getConfigurationOverrides().setMaxUncommittedRows(maxUncommittedRows);
    }

    protected static void configOverrideO3ColumnMemorySize(int size) {
        node1.getConfigurationOverrides().setO3ColumnMemorySize(size);
    }

    protected static void configOverrideO3MaxLag() {
        node1.getConfigurationOverrides().setO3MaxLag(28291);
    }

    protected static void configOverrideO3MinLag(long minLag) {
        node1.getConfigurationOverrides().setO3MinLag(minLag);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideO3QuickSortEnabled(boolean o3QuickSortEnabled) {
        node1.getConfigurationOverrides().setO3QuickSortEnabled(o3QuickSortEnabled);
    }

    protected static void configOverrideParallelGroupByEnabled(boolean parallelGroupByEnabled) {
        node1.getConfigurationOverrides().setParallelGroupByEnabled(parallelGroupByEnabled);
    }

    protected static void configOverrideParallelImportStatusLogKeepNDays(int parallelImportStatusLogKeepNDays) {
        node1.getConfigurationOverrides().setParallelImportStatusLogKeepNDays(parallelImportStatusLogKeepNDays);
    }

    protected static void configOverrideRndFunctionMemoryMaxPages(int rndFunctionMemoryMaxPages) {
        node1.getConfigurationOverrides().setRndFunctionMemoryMaxPages(rndFunctionMemoryMaxPages);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideRndFunctionMemoryPageSize(int rndFunctionMemoryPageSize) {
        node1.getConfigurationOverrides().setRndFunctionMemoryPageSize(rndFunctionMemoryPageSize);
    }

    protected static void configOverrideRostiAllocFacade(RostiAllocFacade rostiAllocFacade) {
        node1.getConfigurationOverrides().setRostiAllocFacade(rostiAllocFacade);
    }

    protected static void configOverrideSampleByIndexSearchPageSize(int sampleByIndexSearchPageSize) {
        node1.getConfigurationOverrides().setSampleByIndexSearchPageSize(sampleByIndexSearchPageSize);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideSqlJoinMetadataMaxResizes(int sqlJoinMetadataMaxResizes) {
        node1.getConfigurationOverrides().setSqlJoinMetadataMaxResizes(sqlJoinMetadataMaxResizes);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideSqlJoinMetadataPageSize(int sqlJoinMetadataPageSize) {
        node1.getConfigurationOverrides().setSqlJoinMetadataPageSize(sqlJoinMetadataPageSize);
    }

    protected static void configOverrideSqlWindowMaxRecursion(int maxRecursion) {
        node1.getConfigurationOverrides().setSqlWindowMaxRecursion(maxRecursion);
    }

    protected static void configOverrideSqlWindowStoreMaxPages(int windowStoreMaxPages) {
        node1.getConfigurationOverrides().setSqlWindowStoreMaxPages(windowStoreMaxPages);
    }

    protected static void configOverrideSqlWindowStorePageSize(int windowStorePageSize) {
        node1.getConfigurationOverrides().setSqlWindowStorePageSize(windowStorePageSize);
    }

    protected static void configOverrideWalApplyTableTimeQuota(long walApplyTableTimeQuota) {
        node1.getConfigurationOverrides().setWalApplyTableTimeQuota(walApplyTableTimeQuota);
    }

    protected static void configOverrideWalMaxLagTxnCount() {
        node1.getConfigurationOverrides().setWalMaxLagTxnCount(1);
    }

    @SuppressWarnings("SameParameterValue")
    protected static void configOverrideWalSegmentRolloverRowCount(long walSegmentRolloverRowCount) {
        node1.getConfigurationOverrides().setWalSegmentRolloverRowCount(walSegmentRolloverRowCount);
    }

    protected static void configOverrideWalSegmentRolloverSize(long walSegmentRolloverSize) {
        node1.getConfigurationOverrides().setWalSegmentRolloverSize(walSegmentRolloverSize);
    }

    protected static void configOverrideWriterMixedIOEnabled(boolean enableMixedIO) {
        node1.getConfigurationOverrides().setWriterMixedIOEnabled(enableMixedIO);
    }

    protected static void configureForBackups() throws IOException {
        backupDir = temp.newFolder().getAbsolutePath();
        backupDirTimestampFormat = new TimestampFormatCompiler().compile("ddMMMyyyy");
    }

    protected static boolean couldObtainLock(Path path) {
        final int lockFd = TableUtils.lock(TestFilesFacadeImpl.INSTANCE, path, false);
        if (lockFd != -1L) {
            TestFilesFacadeImpl.INSTANCE.close(lockFd);
            return true;  // Could lock/unlock.
        }
        return false;  // Could not obtain lock.
    }

    protected static TableToken createTable(TableModel model) {
        return engine.createTable(securityContext, model.getMem(), model.getPath(), false, model, false);
    }

    protected static ApplyWal2TableJob createWalApplyJob(QuestDBTestNode node) {
        return new ApplyWal2TableJob(node.getEngine(), 1, 1);
    }

    protected static ApplyWal2TableJob createWalApplyJob() {
        return new ApplyWal2TableJob(engine, 1, 1);
    }

    protected static void ddl(CharSequence ddl, SqlExecutionContext executionContext) throws SqlException {
        engine.ddl(ddl, executionContext);
    }

    protected static void ddl(CharSequence ddl) throws SqlException {
        ddl(ddl, sqlExecutionContext);
    }

    protected static void ddl(SqlCompiler compiler, CharSequence ddl) throws SqlException {
        ddl(compiler, ddl, sqlExecutionContext);
    }

    protected static void ddl(SqlCompiler compiler, CharSequence ddl, SqlExecutionContext sqlExecutionContext) throws SqlException {
        CairoEngine.ddl(compiler, ddl, sqlExecutionContext, null);
    }

    protected static void ddl(CharSequence ddlSql, SqlExecutionContext sqlExecutionContext, boolean fullFatJoins) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(fullFatJoins);
            ddl(compiler, ddlSql, sqlExecutionContext);
        }
    }

    protected static void drainWalQueue(QuestDBTestNode node) {
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob(node)) {
            drainWalQueue(walApplyJob, node.getEngine());
        }
    }

    protected static void drainWalQueue(ApplyWal2TableJob walApplyJob) {
        drainWalQueue(walApplyJob, engine);
    }

    protected static void drainWalQueue(ApplyWal2TableJob walApplyJob, CairoEngine engine) {
        CheckWalTransactionsJob checkWalTransactionsJob = new CheckWalTransactionsJob(engine);
        //noinspection StatementWithEmptyBody
        while (walApplyJob.run(0)) ;
        if (checkWalTransactionsJob.run(0)) {
            while (walApplyJob.run(0)) ;
        }
    }

    protected static void drainWalQueue() {
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
            drainWalQueue(walApplyJob);
        }
    }

    protected static void drop(CharSequence dropSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        engine.drop(dropSql, sqlExecutionContext, null);
    }

    protected static void drop(CharSequence dropSql, SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        engine.drop(dropSql, sqlExecutionContext, eventSubSeq);
    }

    protected static void drop(CharSequence dropSql) throws SqlException {
        drop(dropSql, sqlExecutionContext, null);
    }

    protected static void dumpMemoryUsage() {
        for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
            LOG.info().$(MemoryTag.nameOf(i)).$(": ").$(Unsafe.getMemUsedByTag(i)).$();
        }
    }

    protected static void forEachNode(QuestDBNodeTask task) {
        for (int i = 0; i < nodes.size(); i++) {
            task.run(nodes.get(i));
        }
    }

    protected static TableReader getReader(CharSequence tableName) {
        return engine.getReader(tableName);
    }

    protected static TableReader getReader(TableToken tt) {
        return engine.getReader(tt);
    }

    protected static TableWriterAPI getTableWriterAPI(CharSequence tableName) {
        return engine.getTableWriterAPI(tableName, "test");
    }

    @NotNull
    protected static WalWriter getWalWriter(CharSequence tableName) {
        return engine.getWalWriter(engine.verifyTableName(tableName));
    }

    protected static TableWriter getWriter(CharSequence tableName) {
        return TestUtils.getWriter(engine, tableName);
    }

    protected static TableWriter getWriter(CairoEngine engine, CharSequence tableName) {
        return TestUtils.getWriter(engine, tableName);
    }

    protected static TableWriter getWriter(TableToken tt) {
        return engine.getWriter(tt, "testing");
    }

    protected static void insert(SqlCompiler compiler, CharSequence insertSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        CairoEngine.insert(compiler, insertSql, sqlExecutionContext);
    }

    protected static QuestDBTestNode newNode(int nodeId) {
        String root = TestUtils.unchecked(() -> temp.newFolder("dbRoot" + nodeId).getAbsolutePath());
        return newNode(root, true, nodeId, new Overrides(), getEngineFactory(), getConfigurationFactory());
    }

    protected static QuestDBTestNode newNode(
            String root,
            boolean ownRoot,
            int nodeId,
            ConfigurationOverrides overrides,
            TestCairoEngineFactory engineFactory,
            TestCairoConfigurationFactory configurationFactory
    ) {
        final QuestDBTestNode node = new QuestDBTestNode(nodeId);
        node.initCairo(root, ownRoot, overrides, engineFactory, configurationFactory);
        nodes.add(node);
        return node;
    }

    protected static TableReader newTableReader(CairoConfiguration configuration, CharSequence tableName) {
        return new TableReader(configuration, engine.verifyTableName(tableName));
    }

    protected static TableWriter newTableWriter(CairoConfiguration configuration, CharSequence tableName, Metrics metrics) {
        return new TableWriter(configuration, engine.verifyTableName(tableName), metrics);
    }

    protected static void printSql(CharSequence sql) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.printSql(compiler, sqlExecutionContext, sql, sink);
        }
    }

    protected static void printSql(CharSequence sql, boolean fullFatJoins) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(fullFatJoins);
            TestUtils.printSql(compiler, sqlExecutionContext, sql, sink);
        }
    }

    protected static void printSqlResult(CharSequence expected, CharSequence query, CharSequence expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws SqlException {
        printSqlResult(() -> expected, query, expectedTimestamp, null, null, supportsRandomAccess, expectSize, false, null);
    }

    protected static void printSqlResult(
            Supplier<? extends CharSequence> expectedSupplier,
            CharSequence query,
            CharSequence expectedTimestamp,
            CharSequence ddl2,
            CharSequence expected2,
            boolean supportsRandomAccess,
            boolean expectSize,
            boolean sizeCanBeVariable,
            CharSequence expectedPlan
    ) throws SqlException {
        snapshotMemoryUsage();
        RecordCursorFactory factory = select(query);
        if (expectedPlan != null) {
            planSink.clear();
            factory.toPlan(planSink);
            assertCursor(expectedPlan, new ExplainPlanFactory(factory, ExplainModel.FORMAT_TEXT), false, expectSize, sizeCanBeVariable);
        }
        try {
            assertTimestamp(expectedTimestamp, factory);
            CharSequence expected = expectedSupplier.get();
            assertCursor(expected, factory, supportsRandomAccess, expectSize, sizeCanBeVariable);
            // make sure we get the same outcome when we get factory to create new cursor
            assertCursor(expected, factory, supportsRandomAccess, expectSize, sizeCanBeVariable);
            // make sure strings, binary fields and symbols are compliant with expected record behaviour
            assertVariableColumns(factory, sqlExecutionContext);

            if (ddl2 != null) {
                compile(ddl2);
                if (configuration.getWalEnabledDefault()) {
                    drainWalQueue();
                }

                int count = 3;
                while (count > 0) {
                    try {
                        assertCursor(expected2, factory, supportsRandomAccess, expectSize, sizeCanBeVariable);
                        // and again
                        assertCursor(expected2, factory, supportsRandomAccess, expectSize, sizeCanBeVariable);
                        return;
                    } catch (TableReferenceOutOfDateException e) {
                        Misc.free(factory);
                        factory = select(query);
                        count--;
                    }
                }
            }

            // make sure calculateSize() produces consistent result
            assertCalculateSize(factory);
        } finally {
            Misc.free(factory);
        }
    }

    protected static void releaseInactive(CairoEngine engine) {
        engine.releaseInactive();
        engine.releaseInactiveTableSequencers();
        engine.resetNameRegistryMemory();
        Assert.assertEquals("busy writer count", 0, engine.getBusyWriterCount());
        Assert.assertEquals("busy reader count", 0, engine.getBusyReaderCount());
    }

    protected static void replicate(String tableName, String wal, QuestDBTestNode srcNode, QuestDBTestNode dstNode) {
        TableToken srcTableToken = srcNode.getEngine().verifyTableName(tableName);
        TableToken dstTableToken = dstNode.getEngine().verifyTableName(tableName);

        dstNode.getEngine().getTableSequencerAPI().closeSequencer(dstTableToken);
        dstNode.getEngine().getTableSequencerAPI().releaseInactive();

        final FilesFacade ff = configuration.getFilesFacade();
        final int mkdirMode = configuration.getMkDirMode();

        final Path srcWal = Path.PATH.get().of(srcNode.getRoot()).concat(srcTableToken).concat(wal).$();
        final Path dstWal = Path.PATH2.get().of(dstNode.getRoot()).concat(dstTableToken).concat(wal).$();
        if (ff.exists(dstWal)) {
            Assert.assertTrue(ff.rmdir(dstWal));
        }
        Assert.assertEquals(0, ff.mkdir(dstWal, mkdirMode));
        Assert.assertEquals(0, ff.copyRecursive(srcWal, dstWal, mkdirMode));

        final Path srcTxnLog = Path.PATH.get().of(srcNode.getRoot()).concat(srcTableToken).concat(WalUtils.SEQ_DIR).$();
        final Path dstTxnLog = Path.PATH2.get().of(dstNode.getRoot()).concat(dstTableToken).concat(WalUtils.SEQ_DIR).$();
        Assert.assertTrue(ff.rmdir(dstTxnLog));
        Assert.assertEquals(0, ff.copyRecursive(srcTxnLog, dstTxnLog, mkdirMode));

        dstNode.getEngine().getTableSequencerAPI().openSequencer(srcTableToken);
    }

    protected static void replicateAndApplyToAllNodes(String tableName, String walName) {
        for (int i = 1, n = nodes.size(); i < n; i++) {
            final QuestDBTestNode node = nodes.get(i);
            replicate(tableName, walName, node1, node);
            drainWalQueue(node);
        }
    }

    protected static void runWalPurgeJob(FilesFacade ff) {
        try (WalPurgeJob job = new WalPurgeJob(engine, ff, engine.getConfiguration().getMicrosecondClock())) {
            engine.setWalPurgeJobRunLock(job.getRunLock());
            job.drain(0);
        }
    }

    protected static void runWalPurgeJob() {
        runWalPurgeJob(engine.getConfiguration().getFilesFacade());
    }

    protected static RecordCursorFactory select(CharSequence selectSql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return engine.select(selectSql, sqlExecutionContext);
    }

    protected static RecordCursorFactory select(CharSequence selectSql) throws SqlException {
        return select(selectSql, sqlExecutionContext);
    }

    protected static void tickWalQueue(int ticks) {
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
            for (int i = 0; i < ticks; i++) {
                walApplyJob.run(0);
            }
        }
    }

    protected void assertCursor(CharSequence expected, RecordCursor cursor, RecordMetadata metadata, boolean header) {
        TestUtils.assertCursor(expected, cursor, metadata, header, sink);
    }

    protected void assertCursorTwoPass(CharSequence expected, RecordCursor cursor, RecordMetadata metadata) {
        assertCursor(expected, cursor, metadata, true);
        cursor.toTop();
        assertCursor(expected, cursor, metadata, true);
    }

    protected void assertException(CharSequence sql, @NotNull CharSequence ddl, int errorPos, @NotNull CharSequence contains) throws Exception {
        assertMemoryLeak(() -> {
            try {
                ddl(ddl, sqlExecutionContext);
                assertException(sql, errorPos, contains);
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.clear();
            }
        });
    }

    void assertFactoryCursor(
            String expected,
            String expectedTimestamp,
            RecordCursorFactory factory,
            boolean supportsRandomAccess,
            SqlExecutionContext executionContext,
            boolean expectSize,
            boolean sizeCanBeVariable
    ) throws SqlException {
        assertTimestamp(expectedTimestamp, factory, executionContext);
        assertCursor(expected, factory, supportsRandomAccess, expectSize, sizeCanBeVariable, executionContext);
        // make sure we get the same outcome when we get factory to create new cursor
        assertCursor(expected, factory, supportsRandomAccess, expectSize, sizeCanBeVariable, executionContext);
        // make sure strings, binary fields and symbols are compliant with expected record behaviour
        assertVariableColumns(factory, executionContext);
    }

    //asserts plan without having to prefix query with 'explain ', specify the fixed output header, etc.
    protected void assertPlan(CharSequence query, CharSequence expectedPlan) throws SqlException {
        StringSink sink = new StringSink();
        sink.put("EXPLAIN ").put(query);

        try (ExplainPlanFactory planFactory = getPlanFactory(sink); RecordCursor cursor = planFactory.getCursor(sqlExecutionContext)) {
            if (!JitUtil.isJitSupported()) {
                expectedPlan = Chars.toString(expectedPlan).replace("Async JIT", "Async");
            }

            TestUtils.assertCursor(expectedPlan, cursor, planFactory.getMetadata(), false, sink);
        }
    }

    protected void assertPlan(SqlCompiler compiler, CharSequence query, CharSequence expectedPlan, SqlExecutionContext sqlExecutionContext) throws SqlException {
        StringSink sink = new StringSink();
        sink.put("EXPLAIN ").put(query);

        try (ExplainPlanFactory planFactory = getPlanFactory(compiler, sink, sqlExecutionContext); RecordCursor cursor = planFactory.getCursor(sqlExecutionContext)) {

            if (!JitUtil.isJitSupported()) {
                expectedPlan = Chars.toString(expectedPlan).replace("Async JIT", "Async");
            }

            TestUtils.assertCursor(expectedPlan, cursor, planFactory.getMetadata(), false, sink);
        }
    }

    protected void assertQuery(String expected, String query, boolean expectSize) throws Exception {
        assertQuery(expected, query, null, null, true, expectSize);
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp) throws SqlException {
        assertQuery(expected, query, expectedTimestamp, false);
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            assertQuery(compiler, expected, query, expectedTimestamp, supportsRandomAccess, sqlExecutionContext);
        }
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws SqlException {
        assertQueryFullFat(expected, query, expectedTimestamp, supportsRandomAccess, expectSize, false);
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize, boolean sizeCanBeVariable) throws SqlException {
        snapshotMemoryUsage();
        try (final RecordCursorFactory factory = select(query)) {
            assertFactoryCursor(expected, expectedTimestamp, factory, supportsRandomAccess, sqlExecutionContext, expectSize, sizeCanBeVariable);
        }
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, SqlExecutionContext sqlExecutionContext) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            assertQuery(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, false);
        }
    }

    protected void assertQuery(SqlCompiler compiler, String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, SqlExecutionContext sqlExecutionContext) throws SqlException {
        assertQuery(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, false);
    }

    protected void assertQuery(SqlCompiler compiler, String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, SqlExecutionContext sqlExecutionContext, boolean expectSize) throws SqlException {
        assertQuery(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, expectSize);
    }

    protected void assertQueryAndCache(String expected, String query, String expectedTimestamp, boolean expectSize) throws SqlException {
        assertQueryAndCache(expected, query, expectedTimestamp, false, expectSize);
    }

    protected void assertQueryAndCache(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws SqlException {
        assertQueryAndCacheFullFat(expected, query, expectedTimestamp, supportsRandomAccess, expectSize);
    }

    protected void assertQueryAndCacheFullFat(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws SqlException {
        snapshotMemoryUsage();
        try (final RecordCursorFactory factory = select(query)) {
            assertFactoryCursor(
                    expected,
                    expectedTimestamp,
                    factory,
                    supportsRandomAccess,
                    sqlExecutionContext,
                    expectSize,
                    false
            );
        }
    }

    protected void assertQueryFullFat(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize, boolean fullFatJoin) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(fullFatJoin);
            assertQuery(compiler, expected, query, expectedTimestamp, sqlExecutionContext, supportsRandomAccess, expectSize);
        }
    }

    protected void assertQueryPlain(String expected, String query) throws SqlException {
        snapshotMemoryUsage();
        try (RecordCursorFactory factory = select(query)) {
            assertFactoryCursor(
                    expected,
                    null,
                    factory,
                    true,
                    sqlExecutionContext,
                    true,
                    false
            );
        }
    }

    protected File assertSegmentExistence(boolean expectExists, String tableName, int walId, int segmentId) {
        TableToken tableToken = engine.verifyTableName(tableName);
        return assertSegmentExistence(expectExists, tableToken, walId, segmentId);
    }

    protected File assertSegmentExistence(boolean expectExists, @NotNull TableToken tableToken, int walId, int segmentId) {
        final CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableToken).concat("wal").put(walId).slash().put(segmentId).$();
            Assert.assertEquals(Utf8s.toString(path), expectExists, TestFilesFacadeImpl.INSTANCE.exists(path));
            return new File(Utf8s.toString(path));
        }
    }

    protected void assertSegmentLockEngagement(boolean expectLocked, String tableName, int walId, int segmentId) {
        TableToken tableToken = engine.verifyTableName(tableName);
        assertSegmentLockEngagement(expectLocked, tableToken, walId, segmentId);
    }

    protected void assertSegmentLockEngagement(boolean expectLocked, TableToken tableToken, int walId, int segmentId) {
        final CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableToken).concat("wal").put(walId).slash().put(segmentId).put(".lock").$();
            final boolean could = couldObtainLock(path);
            Assert.assertEquals(Utf8s.toString(path), expectLocked, !could);
        }
    }

    protected void assertSegmentLockExistence(boolean expectExists, String tableName, @SuppressWarnings("SameParameterValue") int walId, int segmentId) {
        final CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            path.of(root).concat(engine.verifyTableName(tableName)).concat("wal").put(walId).slash().put(segmentId).put(".lock").$();
            Assert.assertEquals(Utf8s.toString(path), expectExists, TestFilesFacadeImpl.INSTANCE.exists(path));
        }
    }

    protected void assertSql(CharSequence expected, CharSequence sql) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.assertSql(compiler, sqlExecutionContext, sql, sink, expected);
        }
    }

    protected void assertSql(CharSequence sql, CharSequence expected, boolean fullFatJoins) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(fullFatJoins);
            TestUtils.assertSql(compiler, sqlExecutionContext, sql, sink, expected);
        }
    }

    protected void assertSqlRunWithJit(CharSequence selectSql) throws Exception {
        try (RecordCursorFactory factory = select(selectSql)) {
            Assert.assertTrue("JIT was not enabled for selectSql: " + selectSql, factory.usesCompiledFilter());
        }
    }

    protected void assertSqlWithTypes(CharSequence sql, CharSequence expected) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.assertSqlWithTypes(compiler, sqlExecutionContext, sql, sink, expected);
        }
    }

    protected void assertTableExistence(boolean expectExists, @NotNull TableToken tableToken) {
        final CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableToken).$();
            Assert.assertEquals(Utf8s.toString(path), expectExists, TestFilesFacadeImpl.INSTANCE.exists(path));
        }
    }

    protected void assertWalExistence(boolean expectExists, String tableName, int walId) {
        TableToken tableToken = engine.verifyTableName(tableName);
        assertWalExistence(expectExists, tableToken, walId);
    }

    protected void assertWalExistence(boolean expectExists, @NotNull TableToken tableToken, int walId) {
        final CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableToken).concat("wal").put(walId).$();
            Assert.assertEquals(Utf8s.toString(path), expectExists, TestFilesFacadeImpl.INSTANCE.exists(path));
        }
    }

    protected void assertWalLockEngagement(boolean expectLocked, String tableName, @SuppressWarnings("SameParameterValue") int walId) {
        TableToken tableToken = engine.verifyTableName(tableName);
        assertWalLockEngagement(expectLocked, tableToken, walId);
    }

    protected void assertWalLockEngagement(boolean expectLocked, TableToken tableToken, int walId) {
        final CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableToken).concat("wal").put(walId).put(".lock").$();
            final boolean could = couldObtainLock(path);
            Assert.assertEquals(Utf8s.toString(path), expectLocked, !could);
        }
    }

    protected void assertWalLockExistence(boolean expectExists, String tableName, @SuppressWarnings("SameParameterValue") int walId) {
        final CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            TableToken tableToken = engine.verifyTableName(tableName);
            path.of(root).concat(tableToken).concat("wal").put(walId).put(".lock").$();
            Assert.assertEquals(Utf8s.toString(path), expectExists, TestFilesFacadeImpl.INSTANCE.exists(path));
        }
    }

    protected void createPopulateTable(TableModel tableModel, int totalRows, String startDate, int partitionCount) throws NumericException, SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.createPopulateTable(compiler, sqlExecutionContext, tableModel, totalRows, startDate, partitionCount);
        }
    }

    protected TableToken createPopulateTable(int tableId, TableModel tableModel, int totalRows, String startDate, int partitionCount) throws NumericException, SqlException {
        return createPopulateTable(tableId, tableModel, 1, totalRows, startDate, partitionCount);
    }

    protected TableToken createPopulateTable(int tableId, TableModel tableModel, int insertIterations, int totalRowsPerIteration, String startDate, int partitionCount) throws NumericException, SqlException {
        TableToken tableToken = registerTableName(tableModel.getTableName());
        try (
                MemoryMARW mem = Vm.getMARWInstance();
                Path path = new Path().of(configuration.getRoot()).concat(tableToken)
        ) {
            TableUtils.createTable(configuration, mem, path, tableModel, tableId, tableToken.getDirName());
            for (int i = 0; i < insertIterations; i++) {
                insert(TestUtils.insertFromSelectPopulateTableStmt(tableModel, totalRowsPerIteration, startDate, partitionCount));
            }
        }
        return tableToken;
    }

    protected ExplainPlanFactory getPlanFactory(CharSequence query) throws SqlException {
        return (ExplainPlanFactory) select(query);
    }

    protected ExplainPlanFactory getPlanFactory(SqlCompiler compiler, CharSequence query, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return (ExplainPlanFactory) compiler.compile(query, sqlExecutionContext).getRecordCursorFactory();
    }

    protected PlanSink getPlanSink(CharSequence query) throws SqlException {
        try (RecordCursorFactory factory = select(query)) {
            planSink.of(factory, sqlExecutionContext);
            return planSink;
        }
    }

    protected boolean isWalTable(CharSequence tableName) {
        return engine.isWalTable(engine.verifyTableName(tableName));
    }

    protected TableWriter newTableWriter(CairoConfiguration configuration, CharSequence tableName, MessageBus messageBus, Metrics metrics) {
        return new TableWriter(configuration, engine.verifyTableName(tableName), messageBus, metrics);
    }

    protected TableToken registerTableName(CharSequence tableName) {
        TableToken token = engine.lockTableName(tableName, false);
        if (token != null) {
            engine.registerTableToken(token);
        }
        return token;
    }

    protected long update(CharSequence updateSql) throws SqlException {
        return update(updateSql, sqlExecutionContext, null);
    }

    protected long update(CharSequence updateSql, SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            while (true) {
                try {
                    CompiledQuery cc = compiler.compile(updateSql, sqlExecutionContext);
                    try (OperationFuture future = cc.execute(eventSubSeq)) {
                        future.await();
                        return future.getAffectedRowsCount();
                    }
                } catch (TableReferenceOutOfDateException ex) {
                    // retry, e.g. continue
                } catch (SqlException ex) {
                    if (Chars.contains(ex.getFlyweightMessage(), "cached query plan cannot be used because table schema has changed")) {
                        continue;
                    }
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    protected enum StringAsTagMode {
        WITH_STRINGS_AS_TAG, NO_STRINGS_AS_TAG
    }

    protected enum SymbolAsFieldMode {
        WITH_SYMBOLS_AS_FIELD, NO_SYMBOLS_AS_FIELD
    }

    public enum WalMode {
        WITH_WAL, NO_WAL
    }

    @FunctionalInterface
    protected interface QuestDBNodeTask {
        void run(QuestDBTestNode node);
    }

    static {
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            FACTORY_TAGS[i] = !Chars.startsWith(MemoryTag.nameOf(i), "MMAP");
        }

        FACTORY_TAGS[MemoryTag.NATIVE_O3] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_JOIN_MAP] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_OFFLOAD] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_SAMPLE_BY_LONG_LIST] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_TABLE_READER] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_TABLE_WRITER] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_IMPORT] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_PARALLEL_IMPORT] = false;
        FACTORY_TAGS[MemoryTag.NATIVE_REPL] = false;
    }
}
