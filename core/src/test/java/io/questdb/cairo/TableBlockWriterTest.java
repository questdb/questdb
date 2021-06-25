/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.TableBlockWriter.TableBlockWriterJob;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.test.tools.TestUtils;
import io.questdb.test.tools.TestUtils.LeakProneCode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class TableBlockWriterTest extends AbstractGriffinTest {
    private static final Log LOG = LogFactory.getLog(TableBlockWriterTest.class);
    private static final int ONE_MEG_IN_PAGES = 1024 * 1024 / (int) Files.PAGE_SIZE;
    private static final AtomicInteger N_MAPPED_PAGES = new AtomicInteger(ONE_MEG_IN_PAGES);
    private static FilesFacade ff;

    @BeforeClass
    public static void setUpStatic() {
        AbstractGriffinTest.setUpStatic();
        sqlExecutionContext.getRandom().reset(0, 1);
        ff = new FilesFacadeImpl() {
            @Override
            public long getMapPageSize() {
                return N_MAPPED_PAGES.get() * getPageSize();
            }
        };
    }

    @Test
    public void testAddColumn1() throws Exception {
        runTest("testAddColumn1", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                            "SELECT timestamp_sequence(0, 1000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(5)" +
                            ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            compiler.compile("ALTER TABLE source ADD COLUMN str STRING",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM source");
            runReplicationTests(expected, "(ts TIMESTAMP, l LONG, str STRING) TIMESTAMP(ts)", 2);
        });
    }

    @Test
    public void testAddColumn2() throws Exception {
        runTest("testAddColumn2", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                            "SELECT timestamp_sequence(0, 1000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(5)" +
                            ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            compiler.compile("ALTER TABLE source ADD COLUMN str STRING",
                    sqlExecutionContext);
            compiler.compile("INSERT INTO source(ts, l, str) " +
                            "SELECT" +
                            " timestamp_sequence(5000000000, 500000000) ts," +
                            " rnd_long(-55, 9009, 2) l," +
                            " rnd_str(3,3,2) str" +
                            " from long_sequence(5)" +
                            ";",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM source");
            runReplicationTests(expected, "(ts TIMESTAMP, l LONG, str STRING) TIMESTAMP(ts)", 2);
        });
    }

    @Test
    public void testAddColumnPartitioned() throws Exception {
        runTest("testAddColumnPartitioned", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                            "SELECT timestamp_sequence(0, 1000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(200)" +
                            ") TIMESTAMP (ts) PARTITION BY DAY;",
                    sqlExecutionContext);
            compiler.compile("ALTER TABLE source ADD COLUMN str STRING",
                    sqlExecutionContext);
            compiler.compile("INSERT INTO source(ts, l, str) " +
                            "SELECT" +
                            " timestamp_sequence(400000000000, 500000000) ts," +
                            " rnd_long(-55, 9009, 2) l," +
                            " rnd_str(3,3,2) str" +
                            " from long_sequence(250)" +
                            ";",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM source");
            runReplicationTests(expected, "(ts TIMESTAMP, l LONG, str STRING) TIMESTAMP(ts) PARTITION BY DAY", 2);
        });
    }

    @Test
    public void testAllTypes() throws Exception {
        runTest("testAllTypes", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                            "SELECT" +
                            " rnd_char() ch," +
                            " rnd_long256() ll," +
                            " rnd_int() a1," +
                            " rnd_int(0, 30, 2) a," +
                            " rnd_boolean() b," +
                            " rnd_str(3,3,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_short() f1," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long(100,200,2) j," +
                            " rnd_long() j1," +
                            " timestamp_sequence(0, 1000000000) ts," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m" +
                            " from long_sequence(1000)" +
                            ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM source");
            runReplicationTests(expected,
                    "(ch CHAR, ll LONG256, a1 INT, a INT, b BOOLEAN, c STRING, d DOUBLE, e FLOAT, f SHORT, f1 SHORT, g DATE, h TIMESTAMP, i SYMBOL, j LONG, j1 LONG, ts TIMESTAMP, l BYTE, m BINARY) TIMESTAMP(ts)",
                    2);
        });
    }

    @Test
    public void testAllTypesPartitioned() throws Exception {
        runTest("testAllTypesPartitioned", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                            "SELECT" +
                            " rnd_char() ch," +
                            " rnd_long256() ll," +
                            " rnd_int() a1," +
                            " rnd_int(0, 30, 2) a," +
                            " rnd_boolean() b," +
                            " rnd_str(3,3,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_short() f1," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long(100,200,2) j," +
                            " rnd_long() j1," +
                            " timestamp_sequence(0, 1000000000) ts," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m" +
                            " from long_sequence(1000)" +
                            ") TIMESTAMP (ts) PARTITION BY DAY;",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM source");
            runReplicationTests(expected,
                    "(ch CHAR, ll LONG256, a1 INT, a INT, b BOOLEAN, c STRING, d DOUBLE, e FLOAT, f SHORT, f1 SHORT, g DATE, h TIMESTAMP, i SYMBOL, j LONG, j1 LONG, ts TIMESTAMP, l BYTE, m BINARY) TIMESTAMP(ts) PARTITION BY DAY",
                    2);
        });
    }

    @Test
    public void testAllTypesResumeBlock() throws Exception {
        int nTest = 0;
        boolean[] bools = {true, false};
        long[] maxRowsPerFrameList = {Long.MAX_VALUE, 3};
        int[] nMappedPagesList = {ONE_MEG_IN_PAGES, 2};
        for (int nThreads = 0; nThreads <= 1; nThreads++) {
            for (int nMappedPages : nMappedPagesList) {
                FilesFacadeImpl ff = new FilesFacadeImpl() {
                    @Override
                    public long getMapPageSize() {
                        return nMappedPages * getPageSize();
                    }
                };
                for (boolean retry : bools) {
                    for (boolean cancel : bools) {
                        for (boolean partitioned : bools) {
                            for (long maxRowsPerFrame : maxRowsPerFrameList) {
                                testAllTypesResumeBlock(ff, nTest++, maxRowsPerFrame, true, nThreads, partitioned, retry, cancel);
                            }
                        }
                    }
                }
            }
        }
    }

    public void testAllTypesResumeBlock(
            FilesFacade ff,
            int nTest,
            long maxRowsPerFrame,
            boolean commitAllAtOnce,
            int nThreads,
            boolean partitioned,
            boolean retry,
            boolean cancel
    ) throws Exception {
        if (!retry && cancel) {
            // This scenario does not make sense;
            return;
        }
        runTest(ff, "testAllTypesResumeBlock(" + maxRowsPerFrame + ")", () -> {
            String sourceTableName = "source" + nTest;
            String destTableName = "dest" + nTest;
            String partitionSrt = partitioned ? " PARTITION BY DAY" : "";
            int nConsecutiveRows = 50;
            long tsStart = 0;
            long tsInc = 1000000000;
            compiler.compile("CREATE TABLE " + sourceTableName + " AS (" +
                            "SELECT" +
                            " rnd_char() ch," +
                            " rnd_long256() ll," +
                            " rnd_int() a1," +
                            " rnd_int(0, 30, 2) a," +
                            " rnd_boolean() b," +
                            " rnd_str(3,3,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_short() f1," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long(100,200,2) j," +
                            " rnd_long() j1," +
                            " timestamp_sequence(" + tsStart + ", " + tsInc + ") ts," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m" +
                            " from long_sequence(" + nConsecutiveRows + ")" +
                            ") TIMESTAMP (ts)" + partitionSrt + ";",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM " + sourceTableName);

            compiler.compile(
                    "CREATE TABLE " + destTableName
                            + " (ch CHAR, ll LONG256, a1 INT, a INT, b BOOLEAN, c STRING, d DOUBLE, e FLOAT, f SHORT, f1 SHORT, g DATE, h TIMESTAMP, i SYMBOL, j LONG, j1 LONG, ts TIMESTAMP, l BYTE, m BINARY) TIMESTAMP(ts)"
                            + partitionSrt + ";",
                    sqlExecutionContext);
            replicateTable(sourceTableName, destTableName, 0, true, maxRowsPerFrame, false, commitAllAtOnce, nThreads);

            CharSequence actual = select("SELECT * FROM " + destTableName);
            Assert.assertEquals(expected, actual);

            tsStart += nConsecutiveRows * tsInc;
            compiler.compile("INSERT INTO " + sourceTableName + "(ch, ll, a1, a, b, c, d, e, f, f1, g, h, i, j, j1, ts, l, m) " +
                            "SELECT" +
                            " rnd_char() ch," +
                            " rnd_long256() ll," +
                            " rnd_int() a1," +
                            " rnd_int(0, 30, 2) a," +
                            " rnd_boolean() b," +
                            " rnd_str(3,3,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_short() f1," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long(100,200,2) j," +
                            " rnd_long() j1," +
                            " timestamp_sequence(" + tsStart + ", " + tsInc + ") ts," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m" +
                            " from long_sequence(" + nConsecutiveRows + ")" +
                            ";",
                    sqlExecutionContext);

            if (retry) {
                replicateTable(sourceTableName, destTableName, nConsecutiveRows, false, maxRowsPerFrame, cancel, commitAllAtOnce, nThreads);
                actual = select("SELECT * FROM " + destTableName);
                Assert.assertEquals(expected, actual);
            }
            expected = select("SELECT * FROM " + sourceTableName);
            replicateTable(sourceTableName, destTableName, nConsecutiveRows, true, maxRowsPerFrame, false, commitAllAtOnce, nThreads);
            actual = select("SELECT * FROM " + destTableName);
            Assert.assertEquals(expected, actual);

            compiler.compile("DROP TABLE " + sourceTableName, sqlExecutionContext);
            compiler.compile("DROP TABLE " + destTableName, sqlExecutionContext);
        });
    }

    @Test
    public void testBinary1() throws Exception {
        testBinary(false);
    }

    @Test
    public void testBinary2() throws Exception {
        testBinary(true);
    }

    @Test
    public void testLargePartition() throws Exception {
        // Ensure partition data is more than the amount of data mapped into memory by the writer (see FilesFacade#FilesFacade)
        runTest("testPartitioned", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                            "SELECT timestamp_sequence(0, 25000000000) ts, rnd_long(-55, 9009, 2) l, rnd_bin(10000, 20000, 1) bin FROM long_sequence(200)" +
                            ") TIMESTAMP (ts) PARTITION BY MONTH;",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM source");
            runReplicationTests(expected, "(ts TIMESTAMP, l LONG, bin BINARY) TIMESTAMP(ts) PARTITION BY MONTH", 2);
        });
    }

    @Test
    public void testMixedWrites() throws Exception {
        runTest("testMixedWrites", () -> {
            int nTest = 0;
            boolean[] bools = {true, false};
            for (boolean commitAllAtOnce : bools) {
                testMixedWrites(nTest++, 0, commitAllAtOnce, 20);
                testMixedWrites(nTest++, 2, commitAllAtOnce, 10);
            }
        });
    }

    @Test
    public void testNoTimestamp() throws Exception {
        runTest("testNoTimestamp", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                            "SELECT rnd_long(-55, 9009, 2) l FROM long_sequence(500)" +
                            ");",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM source");
            runReplicationTests(expected, "(l LONG)", 1);
        });
    }

    @Test
    public void testOpenFailure() throws Exception {
        String failedFn = "dest" + Files.SEPARATOR + "1970-03" + Files.SEPARATOR + "l.d";
        runTest(
                new FilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name) {
                        if (Chars.endsWith(name, failedFn)) {
                            return -1L;
                        }
                        return super.openRW(name);
                    }
                },
                "testPartitioned", () -> {
                    compiler.compile("CREATE TABLE source AS (" +
                                    "SELECT timestamp_sequence(5000000000000, 5000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(500)" +
                                    ") TIMESTAMP (ts) PARTITION BY MONTH;",
                            sqlExecutionContext);
                    compiler.compile("CREATE TABLE dest (ts TIMESTAMP, l LONG) TIMESTAMP(ts) PARTITION BY MONTH",
                            sqlExecutionContext);
                    CharSequence expected = select("SELECT * FROM source WHERE ts < '1970-03-01T00:00:00.000000Z'");
                    try {
                        replicateTable("source", "dest", 0, true, Long.MAX_VALUE, false, false, 0);
                        Assert.fail();
                    } catch (CairoException ex) {
                        Assert.assertTrue(ex.getFlyweightMessage().toString().contains("could not open"));
                    }
                    CharSequence actual = select("SELECT * FROM dest");
                    TestUtils.assertEquals(expected, actual);
                }
        );
    }

    @Test
    public void testPartitioned1() throws Exception {
        runTest("testPartitioned", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                            "SELECT timestamp_sequence(0, 1000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(500)" +
                            ") TIMESTAMP (ts) PARTITION BY DAY;",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM source");
            runReplicationTests(expected, "(ts TIMESTAMP, l LONG) TIMESTAMP(ts) PARTITION BY DAY", 2);
        });
    }

    @Test
    public void testPartitioned2() throws Exception {
        runTest("testPartitioned", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                            "SELECT timestamp_sequence(500000000000000, 1000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(500)" +
                            ") TIMESTAMP (ts) PARTITION BY DAY;",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM source");
            runReplicationTests(expected, "(ts TIMESTAMP, l LONG) TIMESTAMP(ts) PARTITION BY DAY", 2);
        });
    }

    @Test
    public void testSimple1() throws Exception {
        runTest("testSimple", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                            "SELECT timestamp_sequence(0, 1000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(500)" +
                            ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM source");
            runReplicationTests(expected, "(ts TIMESTAMP, l LONG) TIMESTAMP(ts)", 2);
        });
    }

    @Test
    public void testSimple2() throws Exception {
        runTest("testSimple", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                            "SELECT timestamp_sequence(500000000000000, 1000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(500)" +
                            ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM source");
            runReplicationTests(expected, "(ts TIMESTAMP, l LONG) TIMESTAMP(ts)", 2);
        });
    }

    @Test
    public void testSimpleResumeBlock() throws Exception {
        int nTest = 1;
        long[] maxRowsPerFrameList = {Long.MAX_VALUE, 1, 2};
        int[] nConsecutiveRowList = {50, 10, 71};
        for (int nThreads = 0; nThreads < 2; nThreads++) {
            for (long maxRowsPerFrame : maxRowsPerFrameList) {
                for (int nConsecutiveRows : nConsecutiveRowList) {
                    testSimpleResumeBlock(nTest, maxRowsPerFrame, true, nThreads, nConsecutiveRows);
                    nTest++;
                    testSimpleResumeBlock(nTest, maxRowsPerFrame, false, nThreads, nConsecutiveRows);
                    nTest++;
                }
            }
        }
    }

    @Test
    public void testSimpleResumeBlockWithRetry() throws Exception {
        int nTest = 0;
        boolean[] bools = {true, false};
        for (int nThreads = 0; nThreads <= 2; nThreads++) {
            for (boolean commitAllAtOnce : bools) {
                for (boolean cancel : bools) {
                    testSimpleResumeBlockWithRetry(nTest++, cancel, commitAllAtOnce, nThreads);
                }
            }
        }
    }

    @Test
    public void testString1() throws Exception {
        testString(false);
    }

    @Test
    public void testString2() throws Exception {
        testString(true);
    }

    @Test
    public void testSymbol() throws Exception {
        runTest("testSymbol", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                            "SELECT timestamp_sequence(0, 1000000000) ts, rnd_symbol(60,2,16,2) sym FROM long_sequence(500)" +
                            ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM source");
            runReplicationTests(expected, "(ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts)", 2);
        });
    }

    private TableReplicationRecordCursorFactory createReplicatingRecordCursorFactory(String tableName, long maxRowsPerFrame) {
        return new TableReplicationRecordCursorFactory(engine, tableName, maxRowsPerFrame);
    }

    private void replicateTable(
            String sourceTableName,
            String destTableName,
            long nFirstRow,
            boolean commit,
            long maxRowsPerFrame,
            boolean cancel,
            boolean commitAllAtOnce,
            int nThreads
    ) {
        LOG.info().$("Replicating [sourceTableName=").$(sourceTableName).$(", destTableName=").$(destTableName).$(", nFirstRow=").$(nFirstRow).$(", commit=").$(commit)
                .$(", maxRowsPerFrame=").$(maxRowsPerFrame).$(", cancel=").$(cancel).$(", commitAllAtOnce=").$(commitAllAtOnce).$(", nThreads=").$(nThreads).$(']').$();

        final SOCountDownLatch threadsStarted = new SOCountDownLatch(nThreads);
        final AtomicBoolean threadsRunning = new AtomicBoolean(true);
        final SOCountDownLatch threadsFinished = new SOCountDownLatch(nThreads);
        final AtomicInteger nBusyCycles = new AtomicInteger(0);
        final AtomicInteger nIdleCycles = new AtomicInteger(0);
        final TableBlockWriterJob job = new TableBlockWriterJob(engine.getMessageBus());
        for (int n = 0; n < nThreads; n++) {
            final int workerId = n;
            Thread t = new Thread("replication-" + destTableName + "-" + n) {
                @Override
                public void run() {
                    try {
                        long timeInMs = System.currentTimeMillis();
                        LOG.info().$("Starting worker ").$(workerId).$();
                        threadsStarted.countDown();
                        int nSuccessiveIdle = 0;
                        do {
                            if (job.run(workerId)) {
                                nBusyCycles.incrementAndGet();
                                nSuccessiveIdle = 0;
                            } else {
                                nIdleCycles.incrementAndGet();
                                long t = System.currentTimeMillis();
                                if ((t - timeInMs) > 1_000) {
                                    timeInMs = t;
                                    LOG.info().$("Running worker ").$(workerId).$(", ").$(nBusyCycles).$(" total busy cycles, ").$(nIdleCycles).$(" total idle cycles").$();
                                }
                                LockSupport.parkNanos(1_000);
                                nSuccessiveIdle++;
                            }
                        } while (nSuccessiveIdle < 8 || threadsRunning.get());
                        LOG.info().$("Stopping worker ").$(workerId).$();
                        threadsFinished.countDown();
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            };
            t.start();
        }
        threadsStarted.await();
        LOG.info().$(nThreads).$(" worker threads started").$();

        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, sourceTableName);
                TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), destTableName, "testing")) {
            final int columnCount = writer.getMetadata().getColumnCount();

            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                int columnType = writer.getMetadata().getColumnType(columnIndex);
                if (columnType == ColumnType.SYMBOL) {
                    SymbolMapReader symReader = reader.getSymbolMapReader(columnIndex);
                    writer.updateSymbols(columnIndex, symReader);
                }
            }
        }

        try (TableReplicationRecordCursorFactory factory = createReplicatingRecordCursorFactory(sourceTableName, maxRowsPerFrame);
                TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), destTableName, "testing")) {
            final int columnCount = factory.getMetadata().getColumnCount();
            int nFrames = 0;
            int timestampColumnIndex = factory.getMetadata().getTimestampIndex();
            try (TablePageFrameCursor cursor = factory.getPageFrameCursorFrom(sqlExecutionContext, timestampColumnIndex, nFirstRow)) {
                PageFrame frame;
                TableBlockWriter blockWriter = null;
                if (commitAllAtOnce) {
                    blockWriter = writer.newBlock();
                }

                while ((frame = cursor.next()) != null) {
                    if (!commitAllAtOnce) {
                        blockWriter = writer.newBlock();
                    }
                    long firstTimestamp = frame.getFirstTimestamp();
                    LOG.info().$("Replicating frame from ").$ts(firstTimestamp).$();
                    blockWriter.startPageFrame(firstTimestamp);

                    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                        blockWriter.appendPageFrameColumn(
                                columnIndex,
                                frame.getPageSize(columnIndex),
                                frame.getPageAddress(columnIndex)
                        );
                    }
                    nFrames++;
                    if (!commitAllAtOnce) {
                        if (commit) {
                            // Wait until at least one worker thread has run
                            int triggerNIdleCycles = nIdleCycles.get() + nThreads;
                            while (nIdleCycles.get() < triggerNIdleCycles) {
                                Thread.yield();
                            }
                            assert !cancel;
                            blockWriter.commit();
                        } else {
                            if (cancel) {
                                blockWriter.cancel();
                            }
                            break;
                        }
                    }
                }
                if (commitAllAtOnce) {
                    if (commit) {
                        // Wait until at least one worker thread has run
                        int triggerNIdleCycles = nIdleCycles.get() + nThreads;
                        while (nIdleCycles.get() < triggerNIdleCycles) {
                            Thread.yield();
                        }
                        assert !cancel;
                        blockWriter.commit();
                    } else {
                        if (cancel) {
                            blockWriter.cancel();
                        }
                    }
                }

                LOG.info().$("Waiting for ").$(nThreads).$(" to stop").$();
                threadsRunning.set(false);
                threadsFinished.await();
                LOG.info().$("Replication finished in ").$(nFrames).$(" frames, ").$(nBusyCycles.get()).$(" busy cycles, ").$(nIdleCycles.get()).$(" idle cycles, ").$();
            }
        }
    }

    private void runReplicationTests(CharSequence expected, String tableCreateFields, int nMaxThreads) throws SqlException {
        int nTest = 1;
        int[] nMappedPagesList = {ONE_MEG_IN_PAGES, 1};
        for (int nThreads = 0; nThreads <= nMaxThreads; nThreads++) {
            for (int nMappedPages : nMappedPagesList) {
                N_MAPPED_PAGES.set(nMappedPages);

                String destTableName = "dest" + nTest;
                compiler.compile("CREATE TABLE " + destTableName + " " + tableCreateFields + ";", sqlExecutionContext);
                replicateTable("source", destTableName, 0, true, Long.MAX_VALUE, false, false, nThreads);
                CharSequence actual = select("SELECT * FROM " + destTableName);
                TestUtils.assertEquals(expected, actual);
                nTest++;

                destTableName = "dest" + nTest;
                compiler.compile("CREATE TABLE " + destTableName + " " + tableCreateFields + ";", sqlExecutionContext);
                replicateTable("source", destTableName, 0, true, Long.MAX_VALUE, false, true, nThreads);
                actual = select("SELECT * FROM " + destTableName);
                TestUtils.assertEquals(expected, actual);
                nTest++;
                compiler.compile("drop table " + destTableName, sqlExecutionContext);
            }
        }
    }

    private void runTest(String name, LeakProneCode runnable) throws Exception {
        runTest(ff, name, runnable);
    }

    private void runTest(FilesFacade ff, String name, LeakProneCode runnable) throws Exception {
        LOG.info().$("Starting test ").$(name).$();
        assertMemoryLeak(ff, runnable);
        LOG.info().$("Finished test ").$(name).$();
    }

    private CharSequence select(CharSequence selectSql) throws SqlException {
        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                selectSql,
                sink
        );
        return sink;
    }

    private void testBinary(boolean endsWithNull) throws Exception {
        runTest("testBinary", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                            "SELECT timestamp_sequence(0, 1000000000) ts, rnd_bin(10, 20, 2) bin FROM long_sequence(500)" +
                            ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            if (endsWithNull) {
                compiler.compile("INSERT INTO source (ts) SELECT ts+1 FROM (" +
                                "SELECT ts, bin FROM source ORDER BY ts DESC LIMIT 1" +
                                ")",
                        sqlExecutionContext);
            } else {
                compiler.compile("INSERT INTO source (ts, bin) SELECT ts+1, rnd_bin() FROM (" +
                                "SELECT ts, bin FROM source ORDER BY ts DESC LIMIT 1" +
                                ")",
                        sqlExecutionContext);
            }
            CharSequence expected = select("SELECT * FROM source");
            runReplicationTests(expected, "(ts TIMESTAMP, bin BINARY) TIMESTAMP(ts)", 2);
        });
    }

    private void testMixedWrites(int nTest, int nThreads, boolean commitAllAtOnce, int nBatches) throws Exception {
        CharSequence expected;
        CharSequence actual;
        String sourceTableName = "source" + nTest;
        String destTableName = "dest" + nTest;
        compiler.compile("CREATE TABLE " + sourceTableName
                        + " (batch INT, ch CHAR, ll LONG256, a1 INT, a INT, b BOOLEAN, c STRING, d DOUBLE, e FLOAT, f SHORT, f1 SHORT, g DATE, h TIMESTAMP, i SYMBOL, j LONG, j1 LONG, ts TIMESTAMP, l BYTE, m BINARY) TIMESTAMP(ts) PARTITION BY DAY;",
                sqlExecutionContext);
        compiler.compile("CREATE TABLE " + destTableName
                        + " (batch INT, ch CHAR, ll LONG256, a1 INT, a INT, b BOOLEAN, c STRING, d DOUBLE, e FLOAT, f SHORT, f1 SHORT, g DATE, h TIMESTAMP, i SYMBOL, j LONG, j1 LONG, ts TIMESTAMP, l BYTE, m BINARY) TIMESTAMP(ts) PARTITION BY DAY;",
                sqlExecutionContext);
        int nRowsWritten = 0;
        long tsStart = 0;
        long tsIncrement = 200000000;
        for (int batch = 1; batch < nBatches; batch++) {
            int nBatchRows = batch * 100;
            compiler.compile("INSERT INTO " + sourceTableName + " (batch, ch, ll, a1, a, b, c, d, e, f, f1, g, h, i, j, j1, ts, l, m) " +
                            "SELECT" +
                            " " + batch + " batch," +
                            " rnd_char() ch," +
                            " rnd_long256() ll," +
                            " rnd_int() a1," +
                            " rnd_int(0, 30, 2) a," +
                            " rnd_boolean() b," +
                            " rnd_str(3,3,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_short() f1," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long(100,200,2) j," +
                            " rnd_long() j1," +
                            " timestamp_sequence(" + tsStart + ", " + tsIncrement + ") ts," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m" +
                            " from long_sequence(" + nBatchRows + ")" +
                            ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            expected = select("SELECT * FROM " + sourceTableName);
            if (batch % 2 == 1) {
                compiler.compile("INSERT INTO " + destTableName + " (batch, ch, ll, a1, a, b, c, d, e, f, f1, g, h, i, j, j1, ts, l, m) " +
                        "SELECT batch, ch, ll, a1, a, b, c, d, e, f, f1, g, h, i, j, j1, ts, l, m FROM " + sourceTableName + " WHERE batch=" + batch + ";", sqlExecutionContext);
            } else {
                replicateTable(sourceTableName, destTableName, nRowsWritten, true, 133, false, commitAllAtOnce, nThreads);
            }
            actual = select("SELECT * FROM " + destTableName);
            TestUtils.assertEquals(expected, actual);
            nRowsWritten += nBatchRows;
            tsStart += nBatchRows * tsIncrement;
        }

        compiler.compile("DROP TABLE " + sourceTableName, sqlExecutionContext);
        compiler.compile("DROP TABLE " + destTableName, sqlExecutionContext);
    }

    private void testSimpleResumeBlock(int nTest, long maxRowsPerFrame, boolean commitAllAtOnce, int nThreads, int nConsecutiveRows) throws Exception {
        runTest("testSimpleResumeBlock", () -> {
            String destTableName = "dest" + nTest;
            String sourceTableName = "source" + nTest;
            long tsStart = 0;
            long tsInc = 1000000000;
            compiler.compile("CREATE TABLE " + sourceTableName + " AS (" +
                            "SELECT" +
                            " rnd_long(100,200,2) j," +
                            " timestamp_sequence(" + tsStart + ", " + tsInc + ") ts" +
                            " from long_sequence(" + nConsecutiveRows + ")" +
                            ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM " + sourceTableName);

            compiler.compile(
                    "CREATE TABLE " + destTableName + " (j LONG, ts TIMESTAMP) TIMESTAMP(ts);",
                    sqlExecutionContext);
            replicateTable(sourceTableName, destTableName, 0, true, maxRowsPerFrame, false, commitAllAtOnce, nThreads);
            CharSequence actual = select("SELECT * FROM " + destTableName);
            TestUtils.assertEquals(expected, actual);

            tsStart += nConsecutiveRows * tsInc;
            compiler.compile("INSERT INTO " + sourceTableName + "(j, ts) " +
                            "SELECT" +
                            " rnd_long(100,200,2) j," +
                            " timestamp_sequence(" + tsStart + ", " + tsInc + ") ts" +
                            " from long_sequence(" + nConsecutiveRows + ")" +
                            ";",
                    sqlExecutionContext);
            expected = select("SELECT * FROM " + sourceTableName);
            replicateTable(sourceTableName, destTableName, nConsecutiveRows, true, maxRowsPerFrame, false, commitAllAtOnce, nThreads);
            actual = select("SELECT * FROM " + destTableName);
            TestUtils.assertEquals(expected, actual);
        });
    }

    private void testSimpleResumeBlockWithRetry(int nTest, boolean cancel, boolean commitAllAtOnce, int nThreads) throws Exception {
        runTest("testSimpleResumeBlockWithRetry(" + cancel + ")", () -> {
            String sourceTableName = "source" + nTest;
            String destTableName = "dest" + nTest;
            int nConsecutiveRows = 10;
            long tsStart = 0;
            long tsInc = 1000000000;
            compiler.compile("CREATE TABLE " + sourceTableName + " AS (" +
                            "SELECT" +
                            " rnd_long(100,200,2) j," +
                            " timestamp_sequence(" + tsStart + ", " + tsInc + ") ts" +
                            " from long_sequence(" + nConsecutiveRows + ")" +
                            ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            CharSequence expected = select("SELECT * FROM " + sourceTableName);

            compiler.compile(
                    "CREATE TABLE " + destTableName + " (j LONG, ts TIMESTAMP) TIMESTAMP(ts);",
                    sqlExecutionContext);
            replicateTable(sourceTableName, destTableName, 0, true, Long.MAX_VALUE, false, commitAllAtOnce, nThreads);
            CharSequence actual = select("SELECT * FROM " + destTableName);
            TestUtils.assertEquals(expected, actual);

            tsStart += nConsecutiveRows * tsInc;
            compiler.compile("INSERT INTO " + sourceTableName + "(j, ts) " +
                            "SELECT" +
                            " rnd_long(100,200,2) j," +
                            " timestamp_sequence(" + tsStart + ", " + tsInc + ") ts" +
                            " from long_sequence(" + nConsecutiveRows + ")" +
                            ";",
                    sqlExecutionContext);
            replicateTable(sourceTableName, destTableName, nConsecutiveRows, false, Long.MAX_VALUE, cancel, commitAllAtOnce, nThreads);
            actual = select("SELECT * FROM " + destTableName);
            TestUtils.assertEquals(expected, actual);

            replicateTable(sourceTableName, destTableName, nConsecutiveRows, true, Long.MAX_VALUE, false, commitAllAtOnce, nThreads);
            actual = select("SELECT * FROM " + destTableName);
            expected = select("SELECT * FROM " + sourceTableName);
            TestUtils.assertEquals(expected, actual);
        });
    }

    private void testString(boolean endsWithNull) throws Exception {
        runTest("testString", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                            "SELECT timestamp_sequence(0, 1000000000) ts, rnd_str(5,10,2) s FROM long_sequence(300)" +
                            ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            if (endsWithNull) {
                compiler.compile("INSERT INTO source (ts, s) SELECT ts+1, null FROM (" +
                                "SELECT ts, s FROM source ORDER BY ts DESC LIMIT 1" +
                                ")",
                        sqlExecutionContext);
            } else {
                compiler.compile("INSERT INTO source (ts, s) SELECT ts+1, 'ABC' FROM (" +
                                "SELECT ts, s FROM source ORDER BY ts DESC LIMIT 1" +
                                ")",
                        sqlExecutionContext);
            }
            CharSequence expected = select("SELECT * FROM source");
            runReplicationTests(expected, "(ts TIMESTAMP, s STRING) TIMESTAMP(ts)", 2);
        });
    }
}
