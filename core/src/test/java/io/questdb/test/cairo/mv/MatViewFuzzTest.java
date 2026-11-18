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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.mv.MatViewBareNameGuard;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateStore;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlParser;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import io.questdb.test.QueryAssertion;
import io.questdb.test.cairo.fuzz.AbstractFuzzTest;
import io.questdb.test.fuzz.FuzzDropCreateTableOperation;
import io.questdb.test.fuzz.FuzzDropPartitionOperation;
import io.questdb.test.fuzz.FuzzInsertOperation;
import io.questdb.test.fuzz.FuzzSetTtlOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.fuzz.FuzzTruncateTableOperation;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MatViewFuzzTest extends AbstractFuzzTest {
    static final String FUZZ_INVALIDATION_REASON = "fuzz invalidation";
    // A raced base column change of the bare-name round pauses its WAL apply in the removal of the swap file of the
    // base's metadata, see raceBareNameColumnChange(): the bare-name guard has run, and a new reader does not see the
    // change yet.
    private static final AtomicReference<CountDownLatch> bareNameApplyPaused = new AtomicReference<>();
    private static final AtomicReference<CountDownLatch> bareNameApplyRelease = new AtomicReference<>();
    private static final AtomicReference<String> bareNamePauseSuffix = new AtomicReference<>();
    private static final FilesFacade BARE_NAME_FF = new TestFilesFacadeImpl() {
        @Override
        public boolean removeQuiet(LPSZ name) {
            final String suffix = bareNamePauseSuffix.get();
            if (suffix != null && Utf8s.endsWithAscii(name, suffix) && bareNamePauseSuffix.compareAndSet(suffix, null)) {
                bareNameApplyPaused.get().countDown();
                try {
                    Assert.assertTrue(bareNameApplyRelease.get().await(60, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.removeQuiet(name);
        }
    };
    // Zones whose spring-forward gap holds the floor of a day bucket under an offset inside the gap, each [zone, the
    // transition, the first and the last minute of the day inside the gap]. Europe/Berlin jumps from 02:00 to 03:00,
    // Africa/Cairo from 00:00 to 01:00, and Australia/Lord_Howe from 02:00 to 02:30 on the 1st of October, where month
    // buckets start too.
    private static final String[][] DST_GAP_ZONES = {
            {"Europe/Berlin", "2023-03-26T01:00:00.000000Z", "121", "179"},
            {"Africa/Cairo", "2023-04-27T22:00:00.000000Z", "1", "59"},
            {"Australia/Lord_Howe", "2023-09-30T15:30:00.000000Z", "121", "149"}
    };
    // Two hours of fuzz data that NOT BETWEEN reads and view queries exclude.
    private static final String NOT_BETWEEN_HI = "'2022-02-24T20:00:00.000000Z'";
    private static final String NOT_BETWEEN_LO = "'2022-02-24T18:00:00.000000Z'";
    // The days of testViewAndBaseOperations: 2024-01-01 and the five after it.
    private static final int OPS_DAY_COUNT = 6;
    private static final long OPS_DAY_LO = 1_704_067_200_000_000L;
    // The share of the generic fuzzers' data commits that replace a range of the base, see setFuzzParams().
    private static final double REPLACE_INSERT_PROB = 0.1;
    private static final long SPIN_LOCK_TIMEOUT = 100_000_000;
    // Month starts that follow the end of a year, of February in a common and a leap year, and of a 30-day month.
    private static final ObjList<String> monthStarts = new ObjList<>(
            "2022-01-01T00:00:00.000000Z",
            "2022-03-01T00:00:00.000000Z",
            "2024-03-01T00:00:00.000000Z",
            "2022-05-01T00:00:00.000000Z"
    );
    // Daylight saving time changes, in UTC, of the time zones in periodTimeZones: the clock moves forward at the first
    // of each pair and back at the second. Australia/Lord_Howe moves its clock by half an hour.
    private static final ObjList<String> periodDstChanges = new ObjList<>(
            "2022-03-13T07:00:00.000000Z", "2022-11-06T06:00:00.000000Z",
            "2022-03-27T01:00:00.000000Z", "2022-10-30T01:00:00.000000Z",
            "2022-10-01T15:30:00.000000Z", "2022-04-02T15:00:00.000000Z"
    );
    private static final ObjList<String> periodTimeZones = new ObjList<>("America/New_York", "Europe/Berlin", "Australia/Lord_Howe");
    private static final String[] timestampTypes = new String[]{"timestamp", "timestamp_ns"};
    // None of these time zones changes its UTC offset within two weeks of the monthStarts.
    private static final ObjList<String> timeZones = new ObjList<>("Europe/Berlin", "Asia/Kolkata", "America/Sao_Paulo");

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // The real state store, with the hook that testViewAndBaseOperations' reload of the base arms, see
        // applyOperationsFuzzWalWithEarlyRangeTask().
        engineFactory = conf -> new CairoEngine(conf) {
            @Override
            protected MatViewStateStore createMatViewStateStore() {
                return getConfiguration().isMatViewEnabled() ? new MatViewRangeTaskSeamStateStore(this) : super.createMatViewStateStore();
            }
        };
        AbstractFuzzTest.setUpStatic();
    }

    @Override
    public void setUp() {
        super.setUp();
        // Timer refresh tests mess with the clock, so set the spin timeout
        // to a large value to avoid false positive errors.
        node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, SPIN_LOCK_TIMEOUT);
        spinLockTimeout = SPIN_LOCK_TIMEOUT;
        final Rnd rnd = TestUtils.generateRandom(LOG);
        node1.setProperty(PropertyKey.CAIRO_INACTIVE_READER_MAX_OPEN_PARTITIONS, rnd.nextInt(10000));
    }

    @Test
    public void test2LevelDependencyView() throws Exception {
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final String mv2Name = testName.getMethodName() + "_mv2";
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by 1h";
        final String view2Sql = "select min(min) as min, max(max) as max, ts from " + mvName + " sample by 2h";
        final Rnd rnd = generateRandom(LOG);
        testMvFuzz(rnd, tableName, mvName, viewSql, mv2Name, view2Sql);
    }

    @Test
    public void testAggregatingSqlGeneratorEmitsAllShapes() {
        // testAggregatingViewAlterAs must keep drawing SQL that ALTER ... AS takes and SQL that it refuses, through
        // each source of the aggregating SELECT, each bucketing and each wrapper, see generateAggregatingSql(). The
        // error position a refused SQL predicts is that of its outermost timestamp column, or the SQL's start under a
        // wildcard. The seed is fixed, so a failure reproduces.
        final Rnd rnd = new Rnd(42, 42);
        final AggregatingSql sql = new AggregatingSql();
        final int[][] takenCounts = new int[3][3];
        final int[][] refusedCounts = new int[3][3];
        final int[] wrapperCounts = new int[3];
        final int[] refusedKindCounts = new int[3];
        int timestampClauses = 0;
        for (int i = 0; i < 600; i++) {
            generateAggregatingSql(rnd, sql);
            final String text = sql.sql.toString();
            final int kind = sql.inner.kind;
            if (kind == PassthroughSql.PASSED_THROUGH) {
                Assert.assertFalse(text, text.contains("dateadd") || text.contains("TIMESTAMP(ts2)") || text.contains("ts2 AS"));
                takenCounts[sql.source][sql.bucketing]++;
            } else {
                Assert.assertTrue(text, text.contains("dateadd") || text.contains("TIMESTAMP(ts2)"));
                refusedCounts[sql.source][sql.bucketing]++;
                refusedKindCounts[kind]++;
            }
            if (kind == PassthroughSql.SHIFTED) {
                // a shift loses the designated timestamp, which the clause restores
                Assert.assertTrue(text, text.contains(") TIMESTAMP(ts)") || text.contains("FROM a TIMESTAMP(ts)"));
            }
            if (text.contains(") TIMESTAMP(ts)") || text.contains("FROM a TIMESTAMP(ts)")) {
                timestampClauses++;
            }
            switch (sql.bucketing) {
                case AggregatingSql.SAMPLE_BY ->
                        Assert.assertTrue(text, text.contains(" SAMPLE BY 1h") && !text.contains("timestamp_floor"));
                case AggregatingSql.FLOOR ->
                        Assert.assertTrue(text, text.contains("timestamp_floor('1h', ") && !text.contains("SAMPLE BY"));
                default ->
                        Assert.assertTrue(text, text.contains("ts, '2024-01-01T00:00:00.000000Z') AS ts") && !text.contains("SAMPLE BY"));
            }
            switch (sql.source) {
                case AggregatingSql.CTE ->
                        Assert.assertTrue(text, text.startsWith("WITH a AS (") || text.contains("(WITH a AS ("));
                case AggregatingSql.JOIN -> Assert.assertTrue(text, text.contains(" b JOIN other o ON (sym)"));
                default -> Assert.assertFalse(text, text.contains("WITH a AS (") || text.contains(" JOIN "));
            }
            wrapperCounts[sql.wrapper]++;
            if (sql.wrapper == AggregatingSql.WILDCARD) {
                Assert.assertTrue(text, text.startsWith("SELECT * FROM ("));
                Assert.assertEquals(text, 0, sql.timestampPosition);
            } else {
                Assert.assertTrue(text, text.startsWith(sql.topTimestampColumn, sql.timestampPosition));
                // the column names ts, as a bare or prefixed reference or under an alias
                Assert.assertTrue(text, sql.topTimestampColumn.equals("ts") || sql.topTimestampColumn.endsWith(".ts")
                        || sql.topTimestampColumn.endsWith(" AS ts"));
            }
        }
        for (int source = 0; source < 3; source++) {
            for (int bucketing = 0; bucketing < 3; bucketing++) {
                Assert.assertTrue("too few taken SQL [source=" + source + ", bucketing=" + bucketing + ']', takenCounts[source][bucketing] > 10);
                Assert.assertTrue("too few refused SQL [source=" + source + ", bucketing=" + bucketing + ']', refusedCounts[source][bucketing] > 10);
            }
        }
        for (int wrapper = 0; wrapper < 3; wrapper++) {
            Assert.assertTrue("too few SQL with wrapper " + wrapper, wrapperCounts[wrapper] > 100);
        }
        Assert.assertTrue("too few SQL shifting the timestamp", refusedKindCounts[PassthroughSql.SHIFTED] > 75);
        Assert.assertTrue("too few SQL taking ts2 as the timestamp", refusedKindCounts[PassthroughSql.OTHER_COLUMN] > 75);
        Assert.assertTrue("too few TIMESTAMP clauses", timestampClauses > 150);
    }

    @Test
    public void testAggregatingViewAlterAs() throws Exception {
        // ALTER MATERIALIZED VIEW ... AS on an aggregating view, mixed with ingestion in and out of order, WAL apply and
        // refresh job runs at random points, FULL refreshes and restarts of the view states. Half of the new SQLs bucket
        // the base table's designated timestamp, taken as it is through a random chain of aliases, subqueries, a CTE
        // or the master of a join, which the statement takes; the others bucket ts2 or a shifted timestamp, which the
        // statement refuses with the aggregating timestamp rule, at the outermost timestamp column, and no view
        // transaction, see generateAggregatingSql(). The oracle is the view's SQL run on the base: at each check, the
        // view holds exactly its rows.
        // The budget: questdb.fuzz.matview.rounds independent rounds of questdb.fuzz.matview.steps operations.
        final int rounds = Integer.getInteger("questdb.fuzz.matview.rounds", 1);
        final int steps = Integer.getInteger("questdb.fuzz.matview.steps", -1);
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            for (int round = 0; round < rounds; round++) {
                LOG.info().$("fuzz: aggregating round ").$(round).$();
                runAlterAsOperations(rnd, steps > 0 ? steps : 40 + rnd.nextInt(40), true);
            }
        });
    }

    @Test
    public void testBareNameSwitchResume() throws Exception {
        // A bare-name round, see MatViewFuzzOpGenerator.forBareNameRound(): a view whose SQL calls count without
        // parentheses, as the versions before the explicit-name rule store it, over a base that gains and drops a
        // column named count, with ALTER ... AS writing count() or the qualified column, forced invalidations, RESUME,
        // FULL, restarts of the view states and job runs. The generator predicts the outcome of RESUME and ALTER ... AS,
        // and the test asserts that the engine agrees. Oracle: the readings of the view's rows. A bucket that calls
        // count() holds the row count, one that reads the column holds NULL, since no row writes the column; a FULL
        // rebuilds every row under one reading, and only an ALTER ... AS that changes the reading of later rows may
        // mix them, never an accepted RESUME.
        // The budget: questdb.fuzz.matview.rounds independent rounds of questdb.fuzz.matview.steps operations.
        // A raced base column change runs other operations while its apply pauses, see raceBareNameColumnChange(). Half
        // of the rounds create the view REFRESH IMMEDIATE DEFERRED, and its first build races the round's first switch,
        // see MatViewFuzzOpGenerator.isBareNameViewBuilt().
        final int rounds = Integer.getInteger("questdb.fuzz.matview.rounds", 1);
        final int steps = Integer.getInteger("questdb.fuzz.matview.steps", -1);
        assertMemoryLeak(BARE_NAME_FF, () -> {
            final Rnd rnd = generateRandom(LOG);
            for (int round = 0; round < rounds; round++) {
                LOG.info().$("fuzz: bare-name round ").$(round).$();
                runBareNameOperations(rnd, steps > 0 ? steps : 40 + rnd.nextInt(40));
            }
        });
    }

    @Test
    public void testBaseTableCanHaveColumnsAdded() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 0.2, 0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1, true);
        });
    }

    @Test
    public void testDstGapDayBucketGeneratorCoversBucketAfterGap() throws Exception {
        // testMultipleQueryExecutionsPerRefreshDstGapDayBuckets must keep drawing every zone with offsets inside its
        // DST gap and outside it, day buckets of each stride and month buckets, and later batches with transactions
        // whose first row lies in the bucket after the gap: past the first timestamp after the transition, which the
        // refresh still merges with the bucket before the gap, and before the next bucket floor, from java.time. The
        // seed is fixed, so a failure reproduces.
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(42, 42);
            final DstGapDayBucketCase fuzzCase = new DstGapDayBucketCase();
            final int[] inGapCounts = new int[DST_GAP_ZONES.length];
            final int[] outOfGapCounts = new int[DST_GAP_ZONES.length];
            final int[] dayStrideCounts = new int[4];
            int monthCount = 0;
            for (int i = 0; i < 500; i++) {
                generateDstGapDayBucketCase(rnd, "base", fuzzCase);
                final String text = fuzzCase.sql.toString();
                Assert.assertTrue(text, text.matches("select min\\(c3\\) as min, max\\(c3\\) as max, ts from base sample by [1-3][dM] "
                        + "align to calendar time zone '[A-Za-z_/]+' with offset '[0-2][0-9]:[0-5][0-9]'"));
                final int zoneIndex = zoneIndex(fuzzCase.zone);
                final int gapLo = Integer.parseInt(DST_GAP_ZONES[zoneIndex][2]);
                final int gapHi = Integer.parseInt(DST_GAP_ZONES[zoneIndex][3]);
                final boolean isInGap = fuzzCase.offsetMinutes >= gapLo && fuzzCase.offsetMinutes <= gapHi;
                Assert.assertEquals(text, fuzzCase.isOffsetInGap, isInGap);
                if (isInGap) {
                    inGapCounts[zoneIndex]++;
                } else {
                    outOfGapCounts[zoneIndex]++;
                }
                if (fuzzCase.unit == 'M') {
                    Assert.assertEquals(text, 1, fuzzCase.stride);
                    monthCount++;
                } else {
                    dayStrideCounts[fuzzCase.stride]++;
                }
                Assert.assertEquals(text, fuzzCase.transitionTs - 3 * Micros.DAY_MICROS, fuzzCase.start);
                Assert.assertEquals(text, fuzzCase.transitionTs + 6 * Micros.HOUR_MICROS, fuzzCase.hotTs);
            }
            for (int i = 0; i < DST_GAP_ZONES.length; i++) {
                Assert.assertTrue("too few offsets inside the gap of " + DST_GAP_ZONES[i][0], inGapCounts[i] > 50);
                Assert.assertTrue("too few offsets outside the gap of " + DST_GAP_ZONES[i][0], outOfGapCounts[i] > 50);
            }
            for (int stride = 1; stride < 4; stride++) {
                Assert.assertTrue("too few day buckets of stride " + stride, dayStrideCounts[stride] > 50);
            }
            Assert.assertTrue("too few month buckets", monthCount > 50);

            setFuzzParams(rnd, 0, 0);
            fuzzer.createInitialTableWal("base", "timestamp");
            final int[] transactionsAfterGap = new int[DST_GAP_ZONES.length];
            for (int i = 0; i < 30; i++) {
                do {
                    generateDstGapDayBucketCase(rnd, "base", fuzzCase);
                } while (!fuzzCase.isOffsetInGap || fuzzCase.unit != 'd' || fuzzCase.stride != 1);
                final ZoneId zone = ZoneId.of(fuzzCase.zone);
                final Instant transition = Instant.EPOCH.plus(fuzzCase.transitionTs, ChronoUnit.MICROS);
                final LocalDateTime gapFloor = transition.atZone(zone).toLocalDate().atStartOfDay().plusMinutes(fuzzCase.offsetMinutes);
                final long nextFloorTs = ChronoUnit.MICROS.between(Instant.EPOCH, gapFloor.plusDays(1).atZone(zone).toInstant());
                final ObjList<FuzzTransaction> transactions = generateLaterBatch(rnd, "base", fuzzCase.hotTs);
                try {
                    for (int j = 0, n = transactions.size(); j < n; j++) {
                        final FuzzTransaction transaction = transactions.getQuick(j);
                        if (transaction.rollback || transaction.hasReplaceRange()) {
                            continue;
                        }
                        long firstRowTs = Long.MAX_VALUE;
                        for (int k = 0, m = transaction.operationList.size(); k < m; k++) {
                            if (transaction.operationList.getQuick(k) instanceof FuzzInsertOperation insert) {
                                firstRowTs = Math.min(firstRowTs, insert.getTimestamp());
                            }
                        }
                        if (firstRowTs > fuzzCase.transitionTs + 1 && firstRowTs < nextFloorTs) {
                            transactionsAfterGap[zoneIndex(fuzzCase.zone)]++;
                        }
                    }
                } finally {
                    Misc.freeObjListAndClear(transactions);
                }
            }
            for (int i = 0; i < DST_GAP_ZONES.length; i++) {
                Assert.assertTrue("no transaction starts in the bucket after the gap of " + DST_GAP_ZONES[i][0], transactionsAfterGap[i] > 0);
            }
        });
    }

    @Test
    public void testFullRefreshAfterUnsupportedOperations() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);

            fuzzer.setFuzzCounts(
                    rnd.nextBoolean(),
                    rnd.nextInt(2_000_000),
                    rnd.nextInt(1000),
                    rnd.nextInt(3),
                    rnd.nextInt(5),
                    rnd.nextInt(1000),
                    rnd.nextInt(1_000_000),
                    5 + rnd.nextInt(10)
            );

            // Don't rename/drop/change type for existing columns to keep the mat view query valid
            fuzzer.setFuzzProbabilities(
                    0.0,
                    0.0,
                    0.0,
                    0.3,
                    0.3,
                    0.0,
                    0.0,
                    0.0,
                    1,
                    0.0,
                    0.3,
                    0.3,
                    0.0,
                    0.3,
                    0.0,
                    0.0
            );

            setFuzzProperties(rnd);

            final String testTableName = getTestName();
            final int tableCount = 1 + rnd.nextInt(3);

            final AtomicBoolean stop = new AtomicBoolean();
            final ObjList<Thread> refreshJobs = new ObjList<>();

            final ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
            final ObjList<String> viewSqls = new ObjList<>();

            try {
                refreshJobs.add(startRefreshJob(0, stop, rnd));

                for (int i = 0; i < tableCount; i++) {
                    final String tableNameBase = testTableName + "_" + i;
                    final String tableNameMv = tableNameBase + "_mv";
                    final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableNameBase + " sample by 1h";
                    final ObjList<FuzzTransaction> transactions = createTransactionsAndMv(rnd, tableNameBase, tableNameMv, viewSql);
                    fuzzTransactions.add(transactions);
                    viewSqls.add(viewSql);
                }

                // Can help to reduce memory consumption.
                engine.releaseInactive();
                fuzzer.applyManyWalParallel(fuzzTransactions, rnd, testTableName, true, true);
            } finally {
                stopAndJoinJobs(stop, refreshJobs);
            }

            drainWalQueue();
            fuzzer.checkNoSuspendedTables();

            drainWalAndMatViewQueues();
            fuzzer.checkNoSuspendedTables();

            for (int i = 0; i < tableCount; i++) {
                final String viewSql = viewSqls.getQuick(i);
                final String mvName = testTableName + "_" + i + "_mv";

                execute("refresh materialized view '" + mvName + "' full;");
                drainWalAndMatViewQueues();

                LOG.info().$("asserting view ").$(mvName).$(" against ").$(viewSql).$();
                assertQuery("select count() from materialized_views where view_name = '" + mvName + "' and view_status <> 'invalid';")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                count
                                1
                                """);
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    TestUtils.assertSqlCursors(
                            compiler,
                            sqlExecutionContext,
                            viewSql,
                            mvName,
                            LOG
                    );
                }
            }
        });
    }

    @Test
    public void testFuzzParamsGenerateReplaceCommits() throws Exception {
        // The generic fuzzers write replace commits into their views' bases, which can empty base partitions under the
        // views' rows, and assertViewMatchesBase() accepts the rows the views keep there. The seed is fixed, so a
        // failure reproduces.
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(42, 42);
            setFuzzParams(rnd, 1_000, 1_000, 0, 0);
            fuzzer.createInitialTableWal("base", "timestamp");
            final ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions("base", rnd);
            try {
                final LongList removedRanges = collectRemovedRanges(transactions, new LongList());
                int replaceCommits = 0;
                for (int i = 0, n = transactions.size(); i < n; i++) {
                    if (transactions.getQuick(i).hasReplaceRange()) {
                        replaceCommits++;
                    }
                }
                Assert.assertTrue("no replace commit in " + transactions.size() + " transactions", replaceCommits > 0);
                Assert.assertEquals(2 * replaceCommits, removedRanges.size());
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        });
    }

    @Test
    public void testInvalidate() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            // TRUNCATE no longer invalidates the view, which keeps the rows the base lost
            setFuzzParams(rnd, 0, 0.5);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1, true);
        });
    }

    @Test
    public void testManyTablesPeriodView() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runPeriodMvFuzz(rnd, getTestName(), 1 + rnd.nextInt(4));
        });
    }

    @Test
    public void testManyTablesRefreshJobRace() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 1_000, 10_000, 0, 0.0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1 + rnd.nextInt(4), false);
        });
    }

    @Test
    public void testManyTablesTimerView() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runTimerMvFuzz(rnd, getTestName(), 1 + rnd.nextInt(4));
        });
    }

    @Test
    public void testManyTablesView() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1 + rnd.nextInt(4), true);
        });
    }

    @Test
    public void testMultipleQueryExecutionsPerRefresh() throws Exception {
        final Rnd rnd = generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by " + mins + "m";
        testMvFuzz(rnd, tableName, mvName, viewSql);
    }

    @Test
    public void testMultipleQueryExecutionsPerRefreshDSTShiftBack() throws Exception {
        final Rnd rnd = generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by " + mins + "m " +
                "align to calendar time zone 'Europe/Berlin'";
        long start = MicrosFormatUtils.parseUTCTimestamp("2020-10-23T20:30:00.000000Z");
        testMvFuzz(rnd, tableName, start, mvName, viewSql);
    }

    @Test
    public void testMultipleQueryExecutionsPerRefreshDSTShiftForward() throws Exception {
        final Rnd rnd = generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by " + mins + "m " +
                "align to calendar time zone 'Europe/Berlin'";
        long start = MicrosFormatUtils.parseUTCTimestamp("2021-03-28T00:59:00.000000Z");
        testMvFuzz(rnd, tableName, start, mvName, viewSql);
    }

    @Test
    public void testMultipleQueryExecutionsPerRefreshDSTWithOffset() throws Exception {
        final Rnd rnd = generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by " + mins + "m " +
                "align to calendar time zone 'Europe/Berlin' with offset '00:15'";
        long start = MicrosFormatUtils.parseUTCTimestamp("2021-03-28T00:59:00.000000Z");
        testMvFuzz(rnd, tableName, start, mvName, viewSql);
    }

    @Test
    public void testMultipleQueryExecutionsPerRefreshDstGapDayBuckets() throws Exception {
        // SAMPLE BY 1d to 3d or 1M ALIGN TO CALENDAR TIME ZONE with, half of the time, a WITH OFFSET inside the zone's
        // DST gap, so that the floor of the bucket after the gap lies inside the gap. The first batch starts three days
        // before the transition, and the later ones write around a point six hours after it, in the bucket after the
        // gap, so that their incremental refreshes start there. See generateDstGapDayBucketCase().
        // The budget: questdb.fuzz.matview.rounds independent rounds, each with its own base table and view.
        final int rounds = Integer.getInteger("questdb.fuzz.matview.rounds", 1);
        final Rnd rnd = generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final DstGapDayBucketCase fuzzCase = new DstGapDayBucketCase();
        for (int round = 0; round < rounds; round++) {
            final String tableName = testName.getMethodName() + "_" + round;
            generateDstGapDayBucketCase(rnd, tableName, fuzzCase);
            LOG.info().$("fuzz: DST gap round ").$(round).$(" [sql=").$(fuzzCase.sql).I$();
            testMvFuzz(rnd, tableName, fuzzCase.start, fuzzCase.hotTs, tableName + "_mv", fuzzCase.sql.toString());
        }
    }

    @Test
    public void testMultipleQueryExecutionsPerRefreshSmallSamplingInterval() throws Exception {
        final Rnd rnd = generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int secs = 1 + rnd.nextInt(30);
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by " + secs + "s";
        testMvFuzz(rnd, tableName, mvName, viewSql);
    }

    @Test
    public void testOneView() throws Exception {
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final Rnd rnd = generateRandom(LOG);
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by " + mins + "m";
        testMvFuzz(rnd, tableName, mvName, viewSql);
    }

    @Test
    public void testOneView_randomSampleByUnit() throws Exception {
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final Rnd rnd = generateRandom(LOG);
        final char[] units = {'y', 'M', 'w', 'd', 'h', 'm'};
        final char unit = units[rnd.nextInt(units.length)];
        final int interval = 1 + rnd.nextInt(3);
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by " + interval + unit + randomAlignment(rnd);
        // The first batch of rows crosses monthStart, and later batches start near it, where the offset or
        // the time zone moves the start of a month or year bucket away from midnight on the 1st.
        final long monthStart = MicrosFormatUtils.parseUTCTimestamp(monthStarts.getQuick(rnd.nextInt(monthStarts.size())));
        final long start = monthStart - 4 * Micros.DAY_MICROS - 7 * Micros.HOUR_MICROS;
        testMvFuzz(rnd, tableName, start, monthStart, mvName, viewSql);
    }

    @Test
    public void testOracleAcceptsStaleBucketOnlyWhereItStraddlesRemovedPartition() throws Exception {
        // The oracle, assertViewMatchesBase(), accepts a view bucket that differs from its SQL only where the bucket
        // straddles a base partition that a removed range emptied and the base removed: the partition clip keeps such
        // a bucket's old value ("Known limitations"). Day buckets that start at 12:00 straddle two day partitions.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (c3 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES (1, '2024-01-01T13:00'), (2, '2024-01-02T01:00')");
            final String viewSql = "SELECT min(c3) AS min, max(c3) AS max, ts FROM base SAMPLE BY 1d ALIGN TO CALENDAR WITH OFFSET '12:00'";
            execute("CREATE MATERIALIZED VIEW mv AS (" + viewSql + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            // The replace commit removes the row of 2024-01-02, and with it the partition, and writes a row on
            // 2024-01-03. The refresh skips the removed day, so the bucket that straddles it keeps its old value.
            final TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
            MatViewPeriodRefreshTest.replaceRange(engine, driver, "2024-01-02T00:00", "2024-01-03T06:00", 3, "2024-01-03T05:00");
            drainWalAndMatViewQueues();
            assertQuery("mv").timestamp("ts").expectSize().noLeakCheck().returns("""
                    min\tmax\tts
                    1\t2\t2024-01-01T12:00:00.000000Z
                    3\t3\t2024-01-02T12:00:00.000000Z
                    """);

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final LongList removedRanges = new LongList();
                removedRanges.add(driver.parseFloorLiteral("2024-01-02T00:00"), driver.parseFloorLiteral("2024-01-03T06:00"));
                assertViewMatchesBase(compiler, viewSql, "mv", removedRanges);

                // a removed range only inside a partition the base still holds explains no stale bucket, and neither
                // does no removed range
                final LongList keptPartitionRanges = new LongList();
                keptPartitionRanges.add(driver.parseFloorLiteral("2024-01-01T20:00"), driver.parseFloorLiteral("2024-01-01T21:00"));
                assertViewDiffersFromBase(compiler, viewSql, keptPartitionRanges);
                assertViewDiffersFromBase(compiler, viewSql, new LongList());
            }
        });
    }

    @Test
    public void testPassthroughSqlGeneratorEmitsBothKinds() {
        // testPassthroughViewAlterAs must keep drawing both kinds of SQL: a designated timestamp that passes the base's
        // through, which ALTER ... AS takes, and one that a level of the SQL shifts with dateadd() or takes from ts2,
        // which it refuses, at each nesting depth, with a shift inside and at the top, and with a CTE and a wildcard at
        // the top. The error position a refused SQL predicts is that of its outermost timestamp column, or the SQL's
        // start under a wildcard. The seed is fixed, so a failure reproduces.
        final Rnd rnd = new Rnd(42, 42);
        final PassthroughSql sql = new PassthroughSql();
        final int[] kindCounts = new int[3];
        final int[] passedThroughDepthCounts = new int[3];
        final int[] refusedDepthCounts = new int[3];
        int innerShift = 0;
        int topShift = 0;
        int cteTop = 0;
        int wildcardTop = 0;
        for (int i = 0; i < 500; i++) {
            generatePassthroughSql(rnd, sql);
            final String text = sql.sql.toString();
            kindCounts[sql.kind]++;
            switch (sql.kind) {
                case PassthroughSql.PASSED_THROUGH -> {
                    Assert.assertFalse(text, text.contains("dateadd") || text.contains("TIMESTAMP(ts2)") || text.contains("ts2 AS"));
                    Assert.assertEquals(text, -1, sql.transformLevel);
                    passedThroughDepthCounts[sql.depth]++;
                }
                case PassthroughSql.SHIFTED -> {
                    // a shift of the timestamp under the base's name, see generatePassthroughSql()
                    Assert.assertTrue(text, text.matches(".*dateadd\\('[mh]', -?[0-9]+, (ts|TS|\"ts\"|base\\.ts|b\\.ts|t\\.ts)\\).*")
                            && !text.contains("TIMESTAMP(ts2)"));
                    if (sql.transformLevel < sql.depth) {
                        innerShift++;
                    } else {
                        topShift++;
                    }
                    refusedDepthCounts[sql.depth]++;
                }
                default -> {
                    Assert.assertTrue(text, text.contains("ts2 AS ") && text.contains("TIMESTAMP(ts2)") && !text.contains("dateadd"));
                    Assert.assertEquals(text, 0, sql.transformLevel);
                    refusedDepthCounts[sql.depth]++;
                }
            }
            if (sql.isCteTop) {
                Assert.assertTrue(text, text.startsWith("WITH c AS ("));
                cteTop++;
            }
            if (sql.isWildcardTop) {
                Assert.assertEquals(text, 0, sql.timestampPosition);
                wildcardTop++;
            } else {
                Assert.assertTrue(text, text.startsWith(sql.topTimestampColumn, sql.timestampPosition));
                Assert.assertTrue(text, sql.topTimestampColumn.equals("ts") || sql.topTimestampColumn.endsWith(" AS ts")
                        || sql.topTimestampColumn.equals("TS") || sql.topTimestampColumn.equals("\"ts\"")
                        || sql.topTimestampColumn.endsWith(".ts"));
            }
        }
        for (int depth = 0; depth < 3; depth++) {
            Assert.assertTrue("no SQL passing the timestamp through at depth " + depth, passedThroughDepthCounts[depth] > 20);
            Assert.assertTrue("no refused SQL at depth " + depth, refusedDepthCounts[depth] > 20);
        }
        Assert.assertTrue("too few SQL passing the timestamp through", kindCounts[PassthroughSql.PASSED_THROUGH] > 150);
        Assert.assertTrue("too few SQL shifting the timestamp", kindCounts[PassthroughSql.SHIFTED] > 75);
        Assert.assertTrue("too few SQL taking ts2 as the timestamp", kindCounts[PassthroughSql.OTHER_COLUMN] > 75);
        Assert.assertTrue("too few shifts inside the SQL", innerShift > 20);
        Assert.assertTrue("too few shifts at the top", topShift > 20);
        Assert.assertTrue("too few CTEs at the top", cteTop > 20);
        Assert.assertTrue("too few wildcards at the top", wildcardTop > 20);
    }

    @Test
    public void testPassthroughViewAlterAs() throws Exception {
        // ALTER MATERIALIZED VIEW ... AS on a non-aggregating passthrough view, mixed with ingestion in and out of
        // order, WAL apply and refresh job runs at random points, FULL refreshes and restarts of the view states. Half
        // of the new SQLs take the base table's designated timestamp as it is through a random chain of aliases,
        // subqueries and a CTE, which the statement takes; the others shift it with dateadd() at a random level of such
        // a chain, or take ts2 as the designated timestamp, which the statement refuses with the passthrough timestamp
        // rule, at the outermost timestamp column, and no view transaction, see generatePassthroughSql(). Each SQL the
        // statement takes produces the base's rows as they are, so the oracle is the view's SQL run on the base: at
        // each check, the view holds exactly its rows.
        // The budget: questdb.fuzz.matview.rounds independent rounds of questdb.fuzz.matview.steps operations.
        final int rounds = Integer.getInteger("questdb.fuzz.matview.rounds", 1);
        final int steps = Integer.getInteger("questdb.fuzz.matview.steps", -1);
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            for (int round = 0; round < rounds; round++) {
                LOG.info().$("fuzz: passthrough round ").$(round).$();
                runAlterAsOperations(rnd, steps > 0 ? steps : 40 + rnd.nextInt(40), false);
            }
        });
    }

    @Test
    public void testPeriodRefreshConcurrent() throws Exception {
        final TestMicroClock testClock = new TestMicroClock(MicrosTimestampDriver.floor("2000-01-01T00:00:00.000000Z"));
        testMicrosClock = testClock;

        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);

            execute(
                    "create table base_price (" +
                            "  sym varchar, price long, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            final String viewQuery = "select sym, sum(price) as sum_price, ts from base_price sample by 1m";
            execute("create materialized view price_1h refresh period(length 5m) as " + viewQuery);

            final int iterations = 100;
            final AtomicInteger errorCounter = new AtomicInteger();
            final AtomicBoolean writesFinished = new AtomicBoolean();
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);

            final Thread writer = new Thread(() -> {
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    for (int i = 0; i < iterations; i++) {
                        executionContext.setNowAndFixClock(testClock.micros.get(), ColumnType.TIMESTAMP_MICRO);
                        execute(
                                "insert into base_price values ('gbpusd', 1317, dateadd('m', -3, now()))," +
                                        "('gbpusd', 1318, dateadd('m', -2, now()))," +
                                        "('gbpusd', 1319, dateadd('m', -1, now()))," +
                                        "('gbpusd', 1320, now())," +
                                        "('gbpusd', 1321, dateadd('m', 1, now()))," +
                                        "('gbpusd', 1322, dateadd('m', 2, now()))," +
                                        "('gbpusd', 1323, dateadd('m', 3, now()))",
                                executionContext
                        );
                        drainWalQueue();
                        testClock.micros.addAndGet(rnd.nextInt(10) * Micros.MINUTE_MICROS);
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errorCounter.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                    writesFinished.set(true);
                }
            });
            writer.start();

            while (!writesFinished.get()) {
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
            }

            writer.join();

            // do a big jump forward in time to make sure that all rows are in complete periods
            testClock.micros.addAndGet(Micros.HOUR_MICROS);
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();

            Assert.assertEquals(0, errorCounter.get());

            final StringSink sinkB = new StringSink();
            printSql(viewQuery, sink);
            printSql("price_1h", sinkB);
            TestUtils.assertEquals(sink, sinkB);
        });
    }

    @Test
    public void testResumeFullAndBaseReplacement() throws Exception {
        // Random base inserts, base replacements (DROP and CREATE under the same name), forced invalidations and
        // restarts of the view states, with RESUME and FULL recovering the view. Oracle: a twin view with the same
        // SQL, rebuilt with FULL at each check, which equals the SQL over the current base. While the view stays
        // valid with no RESUME in between, its incremental refreshes alone must match the twin; after a RESUME,
        // which jumps over the rows the base received while the view was invalid, a RANGE over the base's days
        // must. Every base, the replacements included, has rows in each of the same days, so the partition clip
        // keeps no view row the twin lacks.
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            final String viewSql = "SELECT ts, sym, last(price) AS price, sum(qty) AS qty FROM base SAMPLE BY 1h";
            final long dayLo = MicrosFormatUtils.parseTimestamp("2024-01-01T00:00:00.000000Z");
            final int dayCount = 4;
            createResumeFuzzBase(rnd, dayLo, dayCount);
            execute("CREATE MATERIALIZED VIEW mv AS (" + viewSql + ") PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW twin REFRESH MANUAL DEFERRED AS (" + viewSql + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();

            boolean isGapAccepted = false;
            final int steps = 20 + rnd.nextInt(20);
            for (int step = 0; step < steps; step++) {
                final int op = rnd.nextInt(10);
                if (op < 5) {
                    insertResumeFuzzRows(rnd, dayLo, dayCount, 1 + rnd.nextInt(5));
                } else if (op == 5) {
                    LOG.info().$("fuzz: replacing base table").$();
                    execute("DROP TABLE base");
                    createResumeFuzzBase(rnd, dayLo, dayCount);
                } else if (op == 6) {
                    LOG.info().$("fuzz: restarting view states").$();
                    engine.getMatViewStateStore().clear();
                    engine.hydrateMatViewStateStore();
                } else if (op == 7) {
                    engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("mv"), "fuzz invalidation");
                } else if (op == 8) {
                    drainWalQueue();
                }
                if (rnd.nextBoolean()) {
                    drainWalAndMatViewQueues();
                }
                if (step % 5 == 4 || step == steps - 1) {
                    isGapAccepted = checkResumeFuzzViews(rnd, dayLo, dayCount, isGapAccepted);
                }
            }
        });
    }

    @Test
    public void testSelfJoinQuery() throws Exception {
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final Rnd rnd = generateRandom(LOG);
        final int mins = 1 + rnd.nextInt(60);
        final String viewSql = "select first(t2.c2) as first, last(t2.c2) as last, t1.ts from  " + tableName + " t1 asof join " + tableName + " t2 sample by " + mins + "m";
        testMvFuzz(rnd, tableName, mvName, viewSql);
    }

    @Test
    public void testSingleTablePeriodView() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runPeriodMvFuzz(rnd, getTestName(), 1);
        });
    }

    @Test
    public void testSingleTableTimerView() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runTimerMvFuzz(rnd, getTestName(), 1);
        });
    }

    @Test
    public void testStressSqlRecompilation() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_MAX_REFRESH_RETRIES, 1);
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1, true);
        });
    }

    @Test
    public void testStressWalPurgeJob() throws Exception {
        // Here we generate many WAL segments and run WalPurgeJob frequently.
        // The goal is to make sure WalPurgeJob doesn't delete WAL-E files used by MatViewRefreshJob.
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10);
        setProperty(PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 10);
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            // Smaller workload on slow CI runners (Mac, Windows): fewer fuzz rows, initial rows, and tables.
            // Segment rollover stays at 10 rows and the purge interval at 10, so the test still
            // generates many WAL segments and runs WalPurgeJob frequently to exercise the race.
            final boolean isLinux = Os.isLinux();
            setFuzzParams(rnd, isLinux ? 10_000 : 3_000, isLinux ? 100_000 : 30_000, 0, 0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), isLinux ? 4 : 2, true);
        });
    }

    @Test
    public void testUpdateRefreshIntervalsConcurrent() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD, "10ms");

        final TestMicroClock testClock = new TestMicroClock(parseFloorPartialTimestamp("2000-01-01T00:00:00.000000Z"));
        testMicrosClock = testClock;

        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);

            execute(
                    "create table base_price (" +
                            "  sym varchar, price long, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            final String viewQuery = "select sym, sum(price) as sum_price, ts from base_price sample by 1m";
            execute("create materialized view price_1h refresh manual as " + viewQuery);
            execute("insert into base_price values ('gbpusd', 42, '2000-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            final int iterations = 100;
            final CyclicBarrier startBarrier = new CyclicBarrier(2);
            final AtomicBoolean writesFinished = new AtomicBoolean();
            final AtomicInteger errorCounter = new AtomicInteger();
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);

            final Thread writer = new Thread(() -> {
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    startBarrier.await();
                    for (int i = 0; i < iterations; i++) {
                        executionContext.setNowAndFixClock(testClock.micros.get(), ColumnType.TIMESTAMP_MICRO);
                        execute(
                                "insert into base_price values ('gbpusd', 41, dateadd('m', -1, now()))," +
                                        "('gbpusd', 42, now())," +
                                        "('gbpusd', 43, dateadd('m', 1, now()))",
                                executionContext
                        );
                        drainWalQueue();
                        drainMatViewTimerQueue(timerJob);
                        testClock.micros.addAndGet(rnd.nextInt(10) * Micros.MINUTE_MICROS);
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errorCounter.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                    writesFinished.set(true);
                }
            });
            writer.start();

            final Thread refresher = new Thread(() -> {
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    startBarrier.await();
                    for (int i = 0; i < iterations; i++) {
                        execute("refresh materialized view price_1h incremental;", executionContext);
                        drainMatViewQueue(engine);
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errorCounter.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                }
            });
            refresher.start();

            while (!writesFinished.get()) {
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
            }

            refresher.join();
            writer.join();

            execute("refresh materialized view price_1h incremental;");
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();

            Assert.assertEquals(0, errorCounter.get());

            final StringSink sinkB = new StringSink();
            printSql(viewQuery, sink);
            printSql("price_1h", sinkB);
            TestUtils.assertEquals(sink, sinkB);
        });
    }

    @Test
    public void testViewAndBaseOperations() throws Exception {
        // Ingestion mixed with base partition operations (DROP, DETACH and ATTACH PARTITION), TRUNCATE, UPDATE, TTL,
        // base replacements, view ADD and DROP COLUMN, view DROP PARTITION, ALTER MATERIALIZED VIEW ... AS, with SQL
        // that maps by name or copies by position, SET and
        // DROP EXPIRE of policies that hide no row, RANGE, FULL and RESUME, forced invalidations, backlog updates, WAL
        // purges, which delete the txn-log parts below the applied ones in a round that keeps the transaction logs in
        // parts, and restarts of the view states, among
        // them restarts right after a base replacement followed by FULL or RESUME, and reloads of a base day with
        // TRUNCATE, ATTACH PARTITION and UPDATE in one WAL apply run, whose range task the refresh job takes before the
        // apply job moves the base's SeqTxnTracker. The WAL apply and the refresh job run at random points in between.
        // The view is IMMEDIATE, so its own refreshes must keep up, or a period view
        // of one day, IMMEDIATE or EVERY, whose period timer firings complete the days one at a time while the other
        // operations run; the refresh job runs of an EVERY view stand for its timer's firings. Half of a period view's
        // days after the first take no base row until their period ends, as a weekend does, so a firing can find its
        // day without a base partition while the range of an UPDATE or ATTACH waits in the view's cache. Oracle: a
        // twin view with every column the view's SQL can produce, and the period of a period view, created and fully
        // refreshed at each check, which the view must match outside the base days the fuzzer removed and the view
        // days it dropped, see MatViewFuzzOracle; and at each restart, the view state that the writers committed to the
        // view's WAL, see restartOperationsFuzzViewStates().
        // The budget: questdb.fuzz.matview.rounds independent rounds of questdb.fuzz.matview.steps operations.
        final int rounds = Integer.getInteger("questdb.fuzz.matview.rounds", 1);
        final int steps = Integer.getInteger("questdb.fuzz.matview.steps", -1);
        // short WAL segments, so that the WAL purges between the operations find closed segments to purge
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 4);
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            for (int round = 0; round < rounds; round++) {
                LOG.info().$("fuzz: round ").$(round).$();
                runViewAndBaseOperations(rnd, steps > 0 ? steps : 40 + rnd.nextInt(40));
            }
        });
    }

    @Test
    public void testViewSqlGeneratorEmitsBothShapes() {
        // runMvFuzz must keep generating views with and without their own NOT BETWEEN on the
        // designated timestamp. The seed is fixed, so a failure reproduces.
        final Rnd rnd = new Rnd(42, 42);
        final String plainSql = "select min(c3) as min, max(c3) as max, ts from t sample by 1h";
        final String notBetweenSql = "select min(c3) as min, max(c3) as max, ts from t " +
                "where ts not between '2022-02-24T18:00:00.000000Z' and '2022-02-24T20:00:00.000000Z' sample by 1h";
        int plainCount = 0;
        int notBetweenCount = 0;
        for (int i = 0; i < 100; i++) {
            final String viewSql = generateViewSql(rnd, "t");
            if (plainSql.equals(viewSql)) {
                plainCount++;
            } else if (notBetweenSql.equals(viewSql)) {
                notBetweenCount++;
            } else {
                Assert.fail("unexpected view SQL: " + viewSql);
            }
        }
        Assert.assertTrue("no view without NOT BETWEEN in 100 draws", plainCount > 0);
        Assert.assertTrue("no view with NOT BETWEEN in 100 draws", notBetweenCount > 0);
    }

    // Asserts the view against its twin, rebuilt from the current base with FULL, and returns whether a gap that only
    // a RANGE fills may remain in the view: a RESUME jumps over the base changes made while the view was invalid.
    private static boolean checkResumeFuzzViews(Rnd rnd, long dayLo, int dayCount, boolean isGapAccepted) throws Exception {
        drainResumeFuzzQueues();
        final io.questdb.cairo.mv.MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName("mv"));
        if (state.isInvalid()) {
            if (rnd.nextBoolean()) {
                LOG.info().$("fuzz: resuming view").$();
                execute("REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG");
                isGapAccepted = true;
            } else {
                LOG.info().$("fuzz: rebuilding view with FULL").$();
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                isGapAccepted = false;
            }
            drainResumeFuzzQueues();
        }
        Assert.assertFalse(state.isInvalid());
        if (isGapAccepted) {
            final StringSink sink = new StringSink();
            sink.put("REFRESH MATERIALIZED VIEW mv RANGE FROM '");
            MicrosFormatUtils.appendDateTimeUSec(sink, dayLo);
            sink.put("' TO '");
            MicrosFormatUtils.appendDateTimeUSec(sink, dayLo + dayCount * Micros.DAY_MICROS);
            sink.put('\'');
            execute(sink);
            drainResumeFuzzQueues();
        }
        execute("REFRESH MATERIALIZED VIEW twin FULL");
        drainResumeFuzzQueues();
        Assert.assertFalse(state.isInvalid());
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, "twin ORDER BY ts, sym", "mv ORDER BY ts, sym", LOG);
        }
        return false;
    }

    // A base with rows in each of its days, and at the first and the last microsecond of its days, so a replacement
    // covers the same days as the base it replaces, and a RANGE over the days, which reads from the base's minimum to
    // its maximum timestamp, recomputes all of them.
    private static void createResumeFuzzBase(Rnd rnd, long dayLo, int dayCount) throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        insertResumeFuzzRow(rnd, dayLo);
        for (int d = 0; d < dayCount; d++) {
            insertResumeFuzzRow(rnd, dayLo + d * Micros.DAY_MICROS + rnd.nextLong(Micros.DAY_MICROS));
        }
        insertResumeFuzzRow(rnd, dayLo + dayCount * Micros.DAY_MICROS - 1);
    }

    // FULL on an invalid view waits for the view's own WAL, and the WAL apply job redelivers it, so the queues drain
    // in rounds.
    private static void drainResumeFuzzQueues() {
        for (int i = 0; i < 3; i++) {
            drainWalAndMatViewQueues();
        }
    }

    private static void insertResumeFuzzRow(Rnd rnd, long ts) throws SqlException {
        final StringSink sink = new StringSink();
        sink.put("INSERT INTO base VALUES ('s").put(rnd.nextInt(3)).put("', ").put(rnd.nextInt(1000)).put(".5, ")
                .put(rnd.nextInt(100)).put(", '");
        MicrosFormatUtils.appendDateTimeUSec(sink, ts);
        sink.put("')");
        execute(sink);
    }

    private static void insertResumeFuzzRows(Rnd rnd, long dayLo, int dayCount, int rowCount) throws SqlException {
        for (int i = 0; i < rowCount; i++) {
            insertResumeFuzzRow(rnd, dayLo + rnd.nextLong(dayCount * Micros.DAY_MICROS));
        }
    }

    // A view check of testBareNameSwitchResume(): drains the queues, recovers an invalid view with RESUME or FULL, as
    // the generator decides, and asserts the readings of the view's rows.
    private static void checkBareNameView(MatViewFuzzOpGenerator generator, MatViewFuzzOpGenerator.Op op, int step) throws Exception {
        drainOperationsFuzzQueues();
        final String context = "[step=" + step + ", sql=" + MatViewFuzzOpGenerator.bareNameViewSql(generator.getBareNameSql())
                + ", baseCount=" + generator.hasBaseCountColumn() + ']';
        final MatViewState state = opsViewState("mv");
        Assert.assertEquals("view validity " + context, generator.isBareNameViewInvalid(), state.isInvalid());
        if (state.isInvalid()) {
            generator.bareNameRecovery(op);
            runBareNameOp(generator, op);
        }
        Assert.assertFalse("view is invalid: " + state.getInvalidationReasonForReport() + ' ' + context, state.isInvalid());
        drainOperationsFuzzQueues();
        Assert.assertFalse("view is invalid: " + state.getInvalidationReasonForReport() + ' ' + context, state.isInvalid());

        int callRowCount = 0;
        int columnRowCount = 0;
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile("SELECT count FROM mv", sqlExecutionContext).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final long count = record.getLong(0);
                if (count == Numbers.LONG_NULL) {
                    columnRowCount++;
                } else {
                    Assert.assertTrue("a bucket counts its rows " + context, count > 0);
                    callRowCount++;
                }
            }
        }
        LOG.info().$("fuzz: view rows [callRows=").$(callRowCount).$(", columnRows=").$(columnRowCount)
                .$(", mixedByAlter=").$(generator.isReadingMixedByAlter())
                .I$();
        if (!generator.isReadingMixedByAlter()) {
            if (generator.isRowsReadingColumn()) {
                Assert.assertEquals("buckets that call count() in a view whose rows read the column " + context, 0, callRowCount);
            } else {
                Assert.assertEquals("buckets that read the column in a view whose rows call count() " + context, 0, columnRowCount);
            }
        }
    }

    private static void insertBareNameRows(MatViewFuzzOpGenerator.Op op) throws SqlException {
        if (op.rowTs.size() == 0) {
            return;
        }
        final StringSink sink = new StringSink();
        sink.put("INSERT INTO base (sym, price, ts) VALUES ");
        for (int i = 0, n = op.rowTs.size(); i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            sink.put("('s").put(op.rowSyms.getQuick(i)).put("', ").put(op.rowPrices.getQuick(i)).put(".5, '");
            MicrosFormatUtils.appendDateTimeUSec(sink, OPS_DAY_LO + op.rowTs.getQuick(i));
            sink.put("')");
        }
        execute(sink);
    }

    private static void runBareNameOp(MatViewFuzzOpGenerator generator, MatViewFuzzOpGenerator.Op op) throws Exception {
        switch (op.kind) {
            case INSERT -> insertBareNameRows(op);
            case BASE_ADD_BARE_NAME_COLUMN -> execute("ALTER TABLE base ADD COLUMN count LONG");
            case BASE_DROP_BARE_NAME_COLUMN -> execute("ALTER TABLE base DROP COLUMN count");
            case BASE_RACED_BARE_NAME_COLUMN_CHANGE -> raceBareNameColumnChange(op);
            case VIEW_ALTER_AS -> {
                // Steers clear of a known bug until it is fixed (D8): ALTER ... AS validates against the base columns
                // that the base's WAL applied, and the bare-name guard reads the SQL that the view's WAL applied, so a
                // base column change and an ALTER ... AS that apply out of their order can leave a switch unrecorded.
                // The WALs apply before and after the statement. Remove the steering with the fix.
                drainWalQueue();
                final String viewSql = MatViewFuzzOpGenerator.bareNameViewSql(op.bareNameSql);
                LOG.info().$("fuzz: ALTER AS ").$(viewSql).$();
                final long viewSeqTxn = opsViewSeqTxn();
                try {
                    execute("ALTER MATERIALIZED VIEW mv AS (" + viewSql + ')');
                    Assert.assertFalse("ALTER ... AS must refuse a column the base lacks: " + viewSql, op.isRefused);
                } catch (SqlException e) {
                    if (!op.isRefused) {
                        throw e;
                    }
                    TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column: base.count");
                    Assert.assertEquals("a refused ALTER ... AS wrote to the view's WAL", viewSeqTxn, opsViewSeqTxn());
                }
                drainWalQueue();
            }
            case VIEW_INVALIDATE ->
                    engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("mv"), FUZZ_INVALIDATION_REASON);
            case VIEW_RESUME -> {
                // RESUME waits for the view's WAL, and the jobs deliver the invalidations published so far
                drainOperationsFuzzQueues();
                LOG.info().$("fuzz: resuming view [skipBacklog=").$(op.isSkipBacklog).$(", outcome=").$(op.resumeOutcome).I$();
                try {
                    execute(op.isSkipBacklog ? "REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG" : "REFRESH MATERIALIZED VIEW mv RESUME");
                    Assert.assertEquals("RESUME accepted the view", MatViewFuzzOpGenerator.RESUME_ACCEPTED, op.resumeOutcome);
                } catch (SqlException e) {
                    switch (op.resumeOutcome) {
                        case MatViewFuzzOpGenerator.RESUME_REFUSED_COMPILE ->
                                TestUtils.assertContains(e.getFlyweightMessage(), "count is not a column; write count() to call the function");
                        case MatViewFuzzOpGenerator.RESUME_REFUSED_SWITCH ->
                                TestUtils.assertContains(e.getFlyweightMessage(), "materialized view SQL reads a bare name whose meaning a base column change switched");
                        default -> throw e;
                    }
                }
                Assert.assertEquals(generator.isBareNameViewInvalid(), opsViewState("mv").isInvalid());
            }
            case VIEW_FULL -> {
                LOG.info().$("fuzz: rebuilding view with FULL").$();
                drainWalQueue();
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                drainOperationsFuzzQueues();
                final MatViewState state = opsViewState("mv");
                Assert.assertFalse("view is invalid after FULL: " + state.getInvalidationReasonForReport(), state.isInvalid());
            }
            case RESTART -> {
                // The restart loses no task here, so the view's state stays the generator's.
                drainOperationsFuzzQueues();
                // Steers clear of the documented loss of the bare-name guard's notification: the switches the guard
                // records live in memory, so a restart loses a switch that reached a view already invalid for another
                // reason, and RESUME then accepts the view. A switch that invalidated a valid view leaves the guard's
                // reason in the view's state, on which RESUME refuses after a restart.
                if (generator.getBareNameSql() == MatViewFuzzOpGenerator.BARE_NAME_SQL_BARE
                        && generator.isBareNameSwitchStanding()
                        && !MatViewBareNameGuard.invalidationReason("count").equals(opsViewState("mv").getInvalidationReasonForReport())) {
                    LOG.info().$("fuzz: skipping the restart, it would lose a bare-name switch of a view invalid for another reason").$();
                } else {
                    LOG.info().$("fuzz: restarting view states").$();
                    engine.getMatViewStateStore().clear();
                    engine.hydrateMatViewStateStore();
                }
            }
            case APPLY_WAL -> drainWalQueue();
            case RUN_REFRESH_JOB -> drainMatViewQueue(engine);
            default -> throw new AssertionError("unexpected operation: " + op.kind);
        }
    }

    // Applies the WAL of the raced base column change on another thread, whose apply pauses after the bare-name guard
    // ran and before the change is visible, see BARE_NAME_FF, and runs the op's racers meanwhile on this thread.
    private static void raceBareNameColumnChange(MatViewFuzzOpGenerator.Op op) throws Exception {
        // The WAL applies first, and the view's WAL once more before the apply resumes, so that the thread of the paused
        // apply applies the change only: a data transaction that a short-lived thread applies leaves a native path of
        // the thread behind.
        drainWalQueue();
        final CountDownLatch paused = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        bareNameApplyPaused.set(paused);
        bareNameApplyRelease.set(release);
        bareNamePauseSuffix.set(engine.verifyTableName("base").getDirName() + Files.SEPARATOR + TableUtils.META_SWAP_FILE_NAME);
        execute(op.isColumnAdded ? "ALTER TABLE base ADD COLUMN count LONG" : "ALTER TABLE base DROP COLUMN count");
        final AtomicReference<Throwable> applierError = new AtomicReference<>();
        final Thread applier = new Thread(() -> {
            try {
                drainWalQueue();
            } catch (Throwable th) {
                applierError.set(th);
            } finally {
                Path.clearThreadLocals();
            }
        });
        applier.start();
        try {
            Assert.assertTrue(paused.await(30, TimeUnit.SECONDS));
            for (int i = 0, n = op.racers.size(); i < n; i++) {
                final int racer = op.racers.getQuick(i);
                LOG.info().$("fuzz: racing the base column change [racer=").$(racer).I$();
                switch (racer) {
                    case MatViewFuzzOpGenerator.RACER_APPLY_WAL -> drainWalQueue();
                    case MatViewFuzzOpGenerator.RACER_FULL -> {
                        // The FULL runs before the change is visible: the jobs deliver what is queued, and the view's WAL
                        // applies, so that the FULL does not wait for it.
                        drainMatViewQueue(engine);
                        drainWalQueue();
                        execute("REFRESH MATERIALIZED VIEW mv FULL");
                        drainMatViewQueue(engine);
                        Assert.assertFalse(opsViewState("mv").hasPendingFullRefreshOwnerForTesting());
                    }
                    case MatViewFuzzOpGenerator.RACER_INCREMENTAL -> {
                        try {
                            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                        } catch (SqlException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "materialized view is invalid, run REFRESH MATERIALIZED VIEW mv RESUME first");
                        }
                    }
                    case MatViewFuzzOpGenerator.RACER_INVALIDATE ->
                            engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("mv"), FUZZ_INVALIDATION_REASON);
                    case MatViewFuzzOpGenerator.RACER_RUN_REFRESH_JOB -> drainMatViewQueue(engine);
                    default -> throw new AssertionError("unexpected racer: " + racer);
                }
            }
            drainWalQueue();
        } finally {
            bareNamePauseSuffix.set(null);
            release.countDown();
            applier.join();
        }
        Assert.assertNull(applierError.get());
    }

    private static void runBareNameOperations(Rnd rnd, int steps) throws Exception {
        final boolean isViewDeferred = rnd.nextBoolean();
        LOG.info().$("fuzz: bare-name round [viewDeferred=").$(isViewDeferred).I$();
        final MatViewFuzzOpGenerator generator = MatViewFuzzOpGenerator.forBareNameRound(OPS_DAY_COUNT, isViewDeferred);
        final MatViewFuzzOpGenerator.Op op = new MatViewFuzzOpGenerator.Op();
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        for (int day = 0; day < OPS_DAY_COUNT; day++) {
            insertBareNameRows(generator.initRows(rnd, day, op));
        }
        // The refresh job takes the commit notifications of the initial rows before the view exists: the store notifies
        // the commits of a base that an earlier round's view read, and the notification would build a deferred view.
        drainWalAndMatViewQueues();
        SqlParser.setMatViewExplicitNamesCheckSkipped(true);
        try {
            execute("CREATE MATERIALIZED VIEW mv " + (isViewDeferred ? "REFRESH IMMEDIATE DEFERRED " : "")
                    + "AS (" + MatViewFuzzOpGenerator.bareNameViewSql(MatViewFuzzOpGenerator.BARE_NAME_SQL_BARE) + ") PARTITION BY DAY");
        } finally {
            SqlParser.setMatViewExplicitNamesCheckSkipped(false);
        }
        drainOperationsFuzzQueues();
        // a deferred view has no refresh position until its first build
        Assert.assertEquals("view built at CREATE", !isViewDeferred, opsViewState("mv").getLastRefreshBaseTxn() > -1);
        for (int step = 0; step < steps; step++) {
            final boolean wasViewBuilt = generator.isBareNameViewBuilt();
            generator.next(rnd, op);
            LOG.info().$("fuzz: step ").$(step).$(" ").$(op.kind.name()).I$();
            runBareNameOp(generator, op);
            if (!wasViewBuilt) {
                // The racers built the deferred view from the base without the change, after the INVALIDATE handler
                // declined the guard's invalidation of the view without a refresh position. The build committed a
                // refresh position below the switch that the guard recorded, so it published the guard's invalidation
                // again.
                drainOperationsFuzzQueues();
                final MatViewState state = opsViewState("mv");
                Assert.assertTrue(generator.isBareNameViewInvalid());
                Assert.assertTrue("the deferred view is valid after its first build raced the switch", state.isInvalid());
                Assert.assertEquals(MatViewBareNameGuard.invalidationReason("count"), state.getInvalidationReasonForReport());
            }
            if (rnd.nextBoolean()) {
                drainWalAndMatViewQueues();
            }
            if (step % 6 == 5 || step == steps - 1) {
                checkBareNameView(generator, op, step);
            }
        }
        execute("DROP MATERIALIZED VIEW mv");
        execute("DROP TABLE base");
        drainWalAndMatViewQueues();
    }

    private static void appendDay(StringSink sink, int day) {
        sink.put('\'');
        MicrosFormatUtils.appendDateTimeUSec(sink, OPS_DAY_LO + day * Micros.DAY_MICROS);
        sink.put('\'');
    }

    // Applies the base's WAL, as APPLY_WAL does, but the refresh job takes the first range task that the WAL apply job
    // queues as soon as it is queued: before the apply job moves the base's SeqTxnTracker past the batch, as a refresh
    // worker can, see MatViewRangeTaskSeamStateStore. When the task is the one of the reload's UPDATE of the day, the
    // base's last transaction, the oracle learns that the day's range task ran.
    private static void applyOperationsFuzzWalWithEarlyRangeTask(int day, MatViewFuzzOracle oracle) {
        final long updateTxn = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("base"));
        // the operation txn of the range task that the refresh job took early
        final LongList rangeTaskTxns = new LongList();
        final MatViewRangeTaskSeamStateStore stateStore = (MatViewRangeTaskSeamStateStore) engine.getMatViewStateStore();
        stateStore.armEarlyRangeTaskRun((baseTableToken, baseTableTxn) -> rangeTaskTxns.add(baseTableTxn));
        try {
            drainWalQueue();
        } finally {
            stateStore.disarmEarlyRangeTaskRun();
        }
        if (rangeTaskTxns.size() > 0) {
            LOG.info().$("fuzz: refresh job ran before the base's tracker moved [rangeTaskTxn=").$(rangeTaskTxns.getQuick(0))
                    .$(", updateTxn=").$(updateTxn)
                    .I$();
            oracle.onRefreshJobRun();
            if (rangeTaskTxns.getQuick(0) == updateTxn) {
                oracle.onRangeTaskRun(day);
            }
        }
    }

    private static void attachOperationsFuzzDay(int day, MatViewFuzzOracle oracle) throws SqlException {
        final StringSink sql = new StringSink();
        sql.put("ALTER TABLE base ATTACH PARTITION LIST ");
        appendDay(sql, day);
        execute(sql);
        oracle.onAttach(day);
    }

    // A view check of testViewAndBaseOperations: drains the queues, catches a period view up on the periods that ended,
    // recovers an invalid view with RESUME or FULL, backfills with a RANGE when a RESUME SKIP BACKLOG left a gap or a
    // view column became mapped, rebuilds the twin and asserts the view against it.
    private static void checkOperationsFuzzViews(Rnd rnd, MatViewFuzzOpGenerator generator, MatViewFuzzOracle oracle, int step) throws Exception {
        final StringSink context = new StringSink();
        context.put("[step=").put(step).put(", sql=").put(MatViewFuzzOpGenerator.viewSql(generator.getSqlColumns(), generator.isSqlPositional()))
                .put(", completeDays=").put(oracle.getCompleteDays()).put(']');
        drainOperationsFuzzQueues(oracle);
        assertNotSuspended("base");
        assertNotSuspended("mv");
        final MatViewState state = opsViewState("mv");
        if (generator.isPeriodView()) {
            catchUpOperationsFuzzPeriods(oracle);
        }
        if (state.isInvalid()) {
            final String reason = state.getInvalidationReasonForReport();
            // the fuzzer fails no refresh: a view goes invalid only when it is told to or its base is replaced
            final boolean isReplacementReason = "table drop operation".equals(reason)
                    || MatViewRefreshJob.BASE_TABLE_CHANGED_REASON.equals(reason)
                    || "base table is dropped or renamed".equals(reason)
                    // a restart finds the view's refresh position past the replacement base's transactions
                    || "materialized view is ahead of base table and cannot be synchronized".equals(reason);
            if (!FUZZ_INVALIDATION_REASON.equals(reason) && !(isReplacementReason && oracle.isBaseReplacedSinceValid())) {
                Assert.fail("unexpected invalidation reason: " + reason + ' ' + context);
            }
            if (rnd.nextInt(3) == 0) {
                runOperationsFuzzFull(oracle);
            } else {
                if (rnd.nextBoolean()) {
                    // a WAL purge must leave the base WAL that a tracked backlog needs
                    drainPurgeJob();
                }
                runOperationsFuzzResume(oracle, reason, false);
                if (state.isInvalid()) {
                    runOperationsFuzzResume(oracle, reason, true);
                }
                drainOperationsFuzzQueues(oracle);
            }
            if (generator.isPeriodView()) {
                catchUpOperationsFuzzPeriods(oracle);
            }
        }
        Assert.assertFalse("view is invalid: " + state.getInvalidationReasonForReport() + ' ' + context, state.isInvalid());
        if (rnd.nextInt(8) == 0) {
            // a FULL refresh rebuilds the view, which the oracle checks bucket for bucket from then on
            runOperationsFuzzFull(oracle);
        }

        // the view's columns and SQL are the fuzzer's
        final ObjList<String> viewColumns = generator.getViewColumns();
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
            Assert.assertEquals(context.toString(), viewColumns.size(), metadata.getColumnCount());
            for (int i = 0, n = viewColumns.size(); i < n; i++) {
                Assert.assertEquals(context.toString(), viewColumns.getQuick(i), metadata.getColumnName(i));
            }
        }
        Assert.assertEquals(context.toString(), MatViewFuzzOpGenerator.viewSql(generator.getSqlColumns(), generator.isSqlPositional()), state.getViewDefinition().getMatViewSql());
        // the view's EXPIRE ROWS policy is the fuzzer's, and every read of the view below goes through it
        generator.onViewWalApplied();
        try {
            // a static context: the builder of AbstractCairoTest.assertQuery() without its no-op prepare hook
            new QueryAssertion(engine, sqlExecutionContext, null, "SELECT expire_clause FROM materialized_views() WHERE view_name = 'mv'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("expire_clause\n" + (generator.getExpireClause() != null ? generator.getExpireClause() : "") + '\n');
        } catch (AssertionError e) {
            throw new AssertionError(context.toString(), e);
        }

        // The twin lives for the check only, so that it holds back no base WAL purge between the checks.
        final ObjList<String> allColumns = new ObjList<>(MatViewFuzzOpGenerator.TS, MatViewFuzzOpGenerator.SYM);
        for (String name : MatViewFuzzOpGenerator.CATALOGUE_NAMES) {
            allColumns.add(name);
        }
        execute("CREATE MATERIALIZED VIEW twin REFRESH MANUAL DEFERRED " + (generator.isPeriodView() ? "PERIOD (LENGTH 1d) " : "")
                + "AS (" + MatViewFuzzOpGenerator.viewSql(allColumns) + ") PARTITION BY DAY");
        execute("REFRESH MATERIALIZED VIEW twin FULL");
        drainOperationsFuzzQueues(oracle);
        Assert.assertFalse(context.toString(), opsViewState("twin").isInvalid());
        final TreeMap<String, MatViewFuzzOracle.Row> twinRows = readOperationsFuzzRows("twin");
        execute("DROP MATERIALIZED VIEW twin");
        final boolean[] baseDays = new boolean[OPS_DAY_COUNT];
        for (MatViewFuzzOracle.Row row : twinRows.values()) {
            baseDays[row.day] = true;
        }

        if (oracle.isGapAccepted() || oracle.isNeedsBackfill() || rnd.nextInt(8) == 0) {
            final StringSink sql = new StringSink();
            sql.put("REFRESH MATERIALIZED VIEW mv RANGE FROM ");
            appendDay(sql, 0);
            sql.put(" TO ");
            appendDay(sql, OPS_DAY_COUNT);
            drainWalQueue();
            execute(sql);
            drainOperationsFuzzQueues(oracle);
            Assert.assertNull(context.toString(), state.getLastRefreshError());
            Assert.assertFalse("view is invalid: " + state.getInvalidationReasonForReport() + ' ' + context, state.isInvalid());
            oracle.onFullRange(baseDays);
        }

        final TreeMap<String, MatViewFuzzOracle.Row> viewRows = readOperationsFuzzRows("mv");
        oracle.assertView(viewRows, twinRows, baseDays, generator.getSqlColumns(), context);
        oracle.onChecked(viewRows, baseDays);
    }

    // Asserts a view against its SQL run on the current base, which a fully refreshed twin view holds: every bucket of
    // the SQL is in the view with the same values, and the view holds no other bucket outside the ranges the base
    // lost. A replace commit that empties a partition, and TRUNCATE, remove base rows while the view keeps its own
    // there ("Known limitations"), so the view may keep a bucket that intersects one of those ranges. A bucket that
    // straddles a base partition such a range emptied and the base removed keeps its old value as well, see
    // isStraddlingRemovedPartition().
    private static void assertViewMatchesBase(SqlCompiler compiler, String viewSql, String viewName, LongList removedRanges) throws SqlException {
        final TreeMap<Long, String> expected = readRowsByTimestamp(compiler, viewSql);
        final TreeMap<Long, String> actual = readRowsByTimestamp(compiler, viewName);
        final TableToken viewToken = engine.verifyTableName(viewName);
        final int timestampType;
        try (TableMetadata metadata = engine.getTableMetadata(viewToken)) {
            timestampType = metadata.getColumnType(metadata.getTimestampIndex());
        }
        final long bucketWidth = ColumnType.getTimestampDriver(timestampType).fromMicros(maxBucketWidthMicros(viewToken));
        for (Map.Entry<Long, String> e : expected.entrySet()) {
            final long ts = e.getKey();
            final String actualRow = actual.get(ts);
            if (!e.getValue().equals(actualRow) && !isStraddlingRemovedPartition(viewToken, removedRanges, ts, ts + bucketWidth)) {
                Assert.fail("view " + viewName + " differs from its SQL at a bucket the base holds [ts=" + ts
                        + ", expected=" + e.getValue() + ", actual=" + actualRow + ']');
            }
        }
        for (Map.Entry<Long, String> e : actual.entrySet()) {
            final long ts = e.getKey();
            if (!expected.containsKey(ts) && !intersects(removedRanges, ts, ts + bucketWidth)) {
                Assert.fail("view " + viewName + " holds a bucket that neither the base nor a removed range explains [ts="
                        + ts + ", row=" + e.getValue() + ']');
            }
        }
    }

    // Asserts that assertViewMatchesBase() rejects the 2024-01-01T12:00 bucket of the view "mv" of
    // testOracleAcceptsStaleBucketOnlyWhereItStraddlesRemovedPartition() under the removed ranges.
    private static void assertViewDiffersFromBase(SqlCompiler compiler, String viewSql, LongList removedRanges) throws SqlException {
        AssertionError error = null;
        try {
            assertViewMatchesBase(compiler, viewSql, "mv", removedRanges);
        } catch (AssertionError e) {
            error = e;
        }
        Assert.assertNotNull(error);
        TestUtils.assertContains(error.getMessage(), "view mv differs from its SQL at a bucket the base holds [ts=1704110400000000, expected=1\t1\t");
        TestUtils.assertContains(error.getMessage(), "actual=1\t2\t");
    }

    // The ranges the transactions may remove from the base: a replace commit removes the rows in its range, and a
    // TRUNCATE all rows.
    private static LongList collectRemovedRanges(ObjList<FuzzTransaction> transactions, LongList sink) {
        for (int i = 0, n = transactions.size(); i < n; i++) {
            final FuzzTransaction transaction = transactions.getQuick(i);
            if (transaction.hasReplaceRange()) {
                sink.add(transaction.getReplaceLoTs(), transaction.getReplaceHiTs());
            }
            for (int j = 0, m = transaction.operationList.size(); j < m; j++) {
                final FuzzTransactionOperation operation = transaction.operationList.getQuick(j);
                if (operation instanceof FuzzTruncateTableOperation
                        || operation instanceof FuzzDropPartitionOperation
                        || operation instanceof FuzzSetTtlOperation
                        || operation instanceof FuzzDropCreateTableOperation) {
                    sink.add(Long.MIN_VALUE, Long.MAX_VALUE);
                }
            }
        }
        return sink;
    }

    private static boolean hasPartition(TableReader reader, TimestampDriver.TimestampFloorMethod floor, long partitionLo) {
        for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
            // a split partition starts after its partition's floor
            if (floor.floor(reader.getPartitionTimestampByIndex(i)) == partitionLo) {
                return true;
            }
        }
        return false;
    }

    private static boolean intersects(LongList ranges, long lo, long hi) {
        for (int i = 0, n = ranges.size(); i < n; i += 2) {
            if (ranges.getQuick(i) < hi && ranges.getQuick(i + 1) >= lo) {
                return true;
            }
        }
        return false;
    }

    // True when the bucket [lo, hi) of the view intersects a partition of the view's base that a removed range
    // intersects and that the base no longer holds: a replace commit emptied it and the base removed it. The refresh
    // clips its interval to the base's partitions, so the view keeps such a bucket's old value, which the spec
    // documents ("Buckets that straddle a removed partition").
    private static boolean isStraddlingRemovedPartition(TableToken viewToken, LongList removedRanges, long lo, long hi) {
        final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
        Assert.assertNotNull(state);
        try (TableReader reader = engine.getReader(state.getViewDefinition().getBaseTableName())) {
            final int partitionBy = reader.getPartitionedBy();
            if (!PartitionBy.isPartitioned(partitionBy)) {
                return false;
            }
            final int timestampType = reader.getMetadata().getTimestampType();
            final TimestampDriver.TimestampFloorMethod floor = PartitionBy.getPartitionFloorMethod(timestampType, partitionBy);
            final TimestampDriver.TimestampCeilMethod ceil = PartitionBy.getPartitionCeilMethod(timestampType, partitionBy);
            for (long partitionLo = floor.floor(lo); partitionLo < hi; ) {
                final long partitionHi = ceil.ceil(partitionLo);
                if (intersects(removedRanges, Math.max(lo, partitionLo), Math.min(hi, partitionHi))
                        && !hasPartition(reader, floor, partitionLo)) {
                    return true;
                }
                partitionLo = partitionHi;
            }
        }
        return false;
    }

    // An upper bound of the width of a view's buckets, a DST shift included, in microseconds. A bucket of a view on
    // another view spans the parent's buckets it aggregates.
    private static long maxBucketWidthMicros(TableToken viewToken) {
        final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
        Assert.assertNotNull(state);
        final long interval = state.getViewDefinition().getSamplingInterval();
        final long unitMicros = switch (state.getViewDefinition().getSamplingIntervalUnit()) {
            case 'y' -> 366 * Micros.DAY_MICROS;
            case 'M' -> 31 * Micros.DAY_MICROS;
            case 'w' -> Micros.WEEK_MICROS;
            case 'd' -> Micros.DAY_MICROS;
            case 'h' -> Micros.HOUR_MICROS;
            case 'm' -> Micros.MINUTE_MICROS;
            case 's' -> Micros.SECOND_MICROS;
            default ->
                    throw new AssertionError("unexpected sampling interval unit: " + state.getViewDefinition().getSamplingIntervalUnit());
        };
        long width = interval * unitMicros + Micros.HOUR_MICROS;
        final TableToken baseToken = engine.getTableTokenIfExists(state.getViewDefinition().getBaseTableName());
        if (baseToken != null && baseToken.isMatView()) {
            width += maxBucketWidthMicros(baseToken);
        }
        return width;
    }

    // Reads the rows of a query that has one row per designated timestamp value, each row printed whole.
    private static TreeMap<Long, String> readRowsByTimestamp(SqlCompiler compiler, String sql) throws SqlException {
        final TreeMap<Long, String> rows = new TreeMap<>();
        final StringSink sink = new StringSink();
        try (
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final RecordMetadata metadata = factory.getMetadata();
            final int timestampIndex = metadata.getTimestampIndex();
            Assert.assertTrue(timestampIndex > -1);
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                sink.clear();
                CursorPrinter.println(record, metadata, sink);
                Assert.assertNull("two rows at one timestamp in " + sql, rows.put(record.getTimestamp(timestampIndex), sink.toString()));
            }
        }
        return rows;
    }

    private static void assertNotSuspended(String tableName) {
        Assert.assertFalse(tableName + " WAL is suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }

    // The incremental refresh of a period view's next EVERY firing or base commit, which a check runs before it
    // compares the view: it catches the view up on the periods that ended while the view was invalid or whose period
    // refresh a restart lost, and it finds a base replaced after a restart lost the base drop's invalidation, as the
    // refresh of an IMMEDIATE view's base commit does.
    private static void catchUpOperationsFuzzPeriods(MatViewFuzzOracle oracle) {
        engine.getMatViewStateStore().enqueueIncrementalRefresh(engine.verifyTableName("mv"));
        drainOperationsFuzzQueues(oracle);
    }

    private static void createOperationsFuzzBase(String timestampType) throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, qty LONG, ts " + timestampType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private static void drainOperationsFuzzQueues() {
        for (int i = 0; i < 4; i++) {
            drainWalAndMatViewQueues();
        }
    }

    // Drains the WAL apply and the refresh job until neither has work: FULL on an invalid view waits for the view's
    // own WAL, which the WAL apply job redelivers.
    private static void drainOperationsFuzzQueues(MatViewFuzzOracle oracle) {
        for (int i = 0; i < 4; i++) {
            drainWalAndMatViewQueues();
            oracle.onDrained(isOperationsFuzzFullPending());
        }
    }

    // The EVERY timer of the view fires: it enqueues an incremental refresh unless the view is invalid or about to go
    // invalid, as MatViewTimerJob does.
    private static void fireOperationsFuzzTimer() {
        final MatViewState state = opsViewState("mv");
        if (!state.isInvalid() && !state.hasPendingInvalidationReason()) {
            engine.getMatViewStateStore().enqueueIncrementalRefresh(engine.verifyTableName("mv"));
        }
    }

    private static void insertOperationsFuzzRows(MatViewFuzzOpGenerator.Op op, MatViewFuzzOracle oracle) throws SqlException {
        if (op.rowTs.size() == 0) {
            return;
        }
        final StringSink sink = new StringSink();
        sink.put("INSERT INTO base VALUES ");
        for (int i = 0, n = op.rowTs.size(); i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            sink.put("('s").put(op.rowSyms.getQuick(i)).put("', ").put(op.rowPrices.getQuick(i)).put(".5, ").put(op.rowQtys.getQuick(i)).put(", '");
            MicrosFormatUtils.appendDateTimeUSec(sink, OPS_DAY_LO + op.rowTs.getQuick(i));
            sink.put("')");
            oracle.onInsert((int) (op.rowTs.getQuick(i) / Micros.DAY_MICROS));
        }
        execute(sink);
    }

    // Whether a view DROP PARTITION would remove the view's newest partition. The view's WAL applies first, as an
    // APPLY_WAL operation would, so the drop finds the partitions read here when it applies.
    private static boolean isDroppingNewestViewPartition(MatViewFuzzOpGenerator.Op op) {
        if (op.kind != MatViewFuzzOpGenerator.Kind.VIEW_DROP_PARTITION_LIST && op.kind != MatViewFuzzOpGenerator.Kind.VIEW_DROP_PARTITION_WHERE) {
            return false;
        }
        drainWalQueue();
        try (TableReader reader = engine.getReader("mv")) {
            final int partitionCount = reader.getPartitionCount();
            if (partitionCount == 0) {
                return false;
            }
            final TimestampDriver driver = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType());
            final long newestPartitionMicros = driver.toMicros(reader.getPartitionTimestampByIndex(partitionCount - 1));
            final int newestDay = (int) ((newestPartitionMicros - OPS_DAY_LO) / Micros.DAY_MICROS);
            // LIST drops one day; WHERE ts < day drops every day before it
            return op.kind == MatViewFuzzOpGenerator.Kind.VIEW_DROP_PARTITION_LIST ? newestDay == op.day : newestDay < op.day;
        }
    }

    // Whether the view still holds a FULL refresh: a FULL on an invalid view parks until the view's own WAL applies.
    private static boolean isOperationsFuzzFullPending() {
        return opsViewState("mv").hasPendingFullRefreshOwnerForTesting();
    }

    // Whether the WAL purge deleted the oldest parts of the view's transaction log, as it deletes the parts below the
    // one of the view's applied seqTxn.
    private static boolean isOperationsFuzzTxnLogPartPurged(TableToken viewToken) {
        return engine.getConfiguration().getDefaultSeqPartTxnCount() > 0 && !MatViewTxnLogParts.exists(engine, viewToken, 0);
    }

    // Turns a detached partition of the base into one that ATTACH PARTITION takes, and returns whether it detached.
    private static boolean makeOperationsFuzzAttachable(int day) {
        final TableToken baseToken = engine.verifyTableName("base");
        final StringSink partition = new StringSink();
        MicrosFormatUtils.appendDateTimeUSec(partition, OPS_DAY_LO + day * Micros.DAY_MICROS);
        partition.clear(10);
        try (Path path = new Path(); Path other = new Path()) {
            path.of(configuration.getDbRoot()).concat(baseToken).concat(partition).put(TableUtils.DETACHED_DIR_MARKER).$();
            if (!Files.exists(path.$())) {
                return false;
            }
            other.of(configuration.getDbRoot()).concat(baseToken).concat(partition).put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
            return true;
        }
    }

    private static long opsViewSeqTxn() {
        return engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("mv"));
    }

    private static MatViewState opsViewState(String viewName) {
        return engine.getMatViewStateStore().getViewState(engine.verifyTableName(viewName));
    }

    // Reads a view's rows keyed by their bucket and symbol, with every column printed, NULL as "null".
    private static TreeMap<String, MatViewFuzzOracle.Row> readOperationsFuzzRows(String viewName) throws SqlException {
        final TreeMap<String, MatViewFuzzOracle.Row> rows = new TreeMap<>();
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(viewName, sqlExecutionContext).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final RecordMetadata metadata = factory.getMetadata();
            final int tsIndex = metadata.getColumnIndex(MatViewFuzzOpGenerator.TS);
            final int symIndex = metadata.getColumnIndex(MatViewFuzzOpGenerator.SYM);
            final TimestampDriver driver = ColumnType.getTimestampDriver(metadata.getColumnType(tsIndex));
            final long dayLo = driver.fromMicros(OPS_DAY_LO);
            final long dayLength = driver.fromMicros(Micros.DAY_MICROS);
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final long ts = record.getTimestamp(tsIndex);
                final CharSequence sym = record.getSymA(symIndex);
                final MatViewFuzzOracle.Row row = new MatViewFuzzOracle.Row((int) Math.floorDiv(ts - dayLo, dayLength));
                Assert.assertTrue("bucket outside the fuzz days: " + ts, row.day > -1 && row.day < OPS_DAY_COUNT);
                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    row.values.put(metadata.getColumnName(i), printOperationsFuzzValue(record, metadata.getColumnType(i), i));
                }
                final String key = String.format("%020d|%s", ts, sym);
                Assert.assertNull("two rows for one bucket and symbol: " + key, rows.put(key, row));
            }
        }
        return rows;
    }

    private static String printOperationsFuzzValue(Record record, int columnType, int columnIndex) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DOUBLE -> {
                final double v = record.getDouble(columnIndex);
                return Numbers.isNull(v) ? "null" : Double.toString(v);
            }
            case ColumnType.LONG -> {
                final long v = record.getLong(columnIndex);
                return v == Numbers.LONG_NULL ? "null" : Long.toString(v);
            }
            case ColumnType.INT -> {
                final int v = record.getInt(columnIndex);
                return v == Numbers.INT_NULL ? "null" : Integer.toString(v);
            }
            case ColumnType.VARCHAR -> {
                final Utf8Sequence v = record.getVarcharA(columnIndex);
                return v == null ? "null" : v.toString();
            }
            case ColumnType.SYMBOL -> {
                final CharSequence v = record.getSymA(columnIndex);
                return v == null ? "null" : v.toString();
            }
            case ColumnType.TIMESTAMP -> {
                return Long.toString(record.getTimestamp(columnIndex));
            }
            default -> throw new AssertionError("unexpected column type: " + ColumnType.nameOf(columnType));
        }
    }

    private static void replaceOperationsFuzzBase(MatViewFuzzOpGenerator.Op op, MatViewFuzzOracle oracle, String timestampType) throws SqlException {
        LOG.info().$("fuzz: replacing base table").$();
        execute("DROP TABLE base");
        // the drop invalidates the views on the base
        oracle.onBaseReplaced();
        oracle.onInvalidationPublished();
        createOperationsFuzzBase(timestampType);
        insertOperationsFuzzRows(op, oracle);
    }

    // A restart of the view states: the store reloads them from disk, and loses the tasks it has not run. Unless a
    // task the restart loses would have changed it, the view keeps the state its writers committed to the view's WAL,
    // which the restart oracle compares: the validity and reason, the refresh position, the base table id and, when a
    // view state event persisted it, the backlog-lost marker.
    private static void restartOperationsFuzzViewStates(MatViewFuzzOracle oracle) {
        final TableToken viewToken = engine.verifyTableName("mv");
        final MatViewState before = opsViewState("mv");
        final boolean isStateCompared = !before.isLocked()
                && !before.isPendingInvalidation()
                && !before.hasPendingInvalidationReason()
                && !before.hasPendingFullRefreshOwnerForTesting();
        final boolean wasInvalid = before.isInvalid();
        final String reason = before.getInvalidationReasonForReport();
        final long lastRefreshBaseTxn = before.getLastRefreshBaseTxn();
        final int baseTableId = before.getBaseTableId();
        final String backlogLostReason = before.getBacklogLostReason();
        if (!wasInvalid) {
            oracle.onValidAtRestart();
        }
        if (isOperationsFuzzTxnLogPartPurged(viewToken)) {
            LOG.info().$("fuzz: restarting view states above purged txn-log parts").$();
        } else {
            LOG.info().$("fuzz: restarting view states").$();
        }
        engine.getMatViewStateStore().clear();
        engine.hydrateMatViewStateStore();
        oracle.onRestart();
        if (isStateCompared) {
            final MatViewState after = opsViewState("mv");
            final String context = " [before: invalid=" + wasInvalid + ", reason=" + reason + ", lastRefreshBaseTxn=" + lastRefreshBaseTxn
                    + ", baseTableId=" + baseTableId + ", backlogLost=" + backlogLostReason + "; after: invalid=" + after.isInvalid()
                    + ", reason=" + after.getInvalidationReasonForReport() + ", lastRefreshBaseTxn=" + after.getLastRefreshBaseTxn()
                    + ", baseTableId=" + after.getBaseTableId() + ", backlogLost=" + after.getBacklogLostReason() + ']';
            Assert.assertEquals("the restart changed the view's validity" + context, wasInvalid, after.isInvalid());
            if (wasInvalid) {
                Assert.assertEquals("the restart changed the invalidation reason" + context, reason, after.getInvalidationReasonForReport());
            }
            Assert.assertEquals("the restart changed the refresh position" + context, lastRefreshBaseTxn, after.getLastRefreshBaseTxn());
            Assert.assertEquals("the restart changed the base table id" + context, baseTableId, after.getBaseTableId());
            if (!oracle.isBacklogLostMarkerUnpersisted()) {
                Assert.assertEquals("the restart changed the backlog-lost marker" + context, backlogLostReason, after.getBacklogLostReason());
            }
        }
    }

    // An UPDATE that changes every row of the day.
    private static void updateOperationsFuzzDay(int day, MatViewFuzzOracle oracle) throws SqlException {
        final StringSink sql = new StringSink();
        sql.put("UPDATE base SET price = price + 1.5, qty = qty + 1 WHERE ts >= ");
        appendDay(sql, day);
        sql.put(" AND ts < ");
        appendDay(sql, day + 1);
        execute(sql);
        oracle.onUpdate(day);
    }

    private static void runOperationsFuzzFull(MatViewFuzzOracle oracle) throws SqlException {
        LOG.info().$("fuzz: rebuilding view with FULL").$();
        drainWalQueue();
        execute("REFRESH MATERIALIZED VIEW mv FULL");
        oracle.onFullIssued();
        drainOperationsFuzzQueues(oracle);
        final MatViewState state = opsViewState("mv");
        Assert.assertFalse("view is invalid after FULL: " + state.getInvalidationReasonForReport(), state.isInvalid());
        oracle.onFullCompleted();
        oracle.onResumed();
    }

    private static void runOperationsFuzzOp(
            MatViewFuzzOpGenerator generator,
            MatViewFuzzOpGenerator.Op op,
            ObjList<String> sqlColumnsBefore,
            boolean wasSqlPositional,
            MatViewFuzzOracle oracle,
            String timestampType,
            boolean isTimerView
    ) throws Exception {
        final StringSink sql = new StringSink();
        switch (op.kind) {
            case INSERT -> insertOperationsFuzzRows(op, oracle);
            case BASE_DROP_PARTITION_LIST -> {
                sql.put("ALTER TABLE base DROP PARTITION LIST ");
                appendDay(sql, op.day);
                execute(sql);
                oracle.onBaseRemoved(op.day);
            }
            case BASE_DROP_PARTITION_WHERE -> {
                sql.put("ALTER TABLE base DROP PARTITION WHERE ts < ");
                appendDay(sql, op.day);
                execute(sql);
                oracle.onBaseRemovedBefore(op.day);
            }
            case BASE_DETACH_PARTITION -> {
                sql.put("ALTER TABLE base DETACH PARTITION LIST ");
                appendDay(sql, op.day);
                execute(sql);
                // the partition detaches when the WAL applies
                drainWalQueue();
                final boolean isDetached = makeOperationsFuzzAttachable(op.day);
                generator.onDetached(op.day, isDetached);
                if (isDetached) {
                    oracle.onBaseRemoved(op.day);
                }
            }
            case BASE_ATTACH_PARTITION -> attachOperationsFuzzDay(op.day, oracle);
            case BASE_TRUNCATE -> {
                execute("TRUNCATE TABLE base");
                oracle.onBaseRemovedBefore(OPS_DAY_COUNT);
            }
            case BASE_TRUNCATE_ATTACH_UPDATE -> {
                LOG.info().$("fuzz: reloading base day ").$(op.day).$(" with TRUNCATE, ATTACH and UPDATE").$();
                execute("TRUNCATE TABLE base");
                oracle.onBaseRemovedBefore(OPS_DAY_COUNT);
                attachOperationsFuzzDay(op.day, oracle);
                updateOperationsFuzzDay(op.day, oracle);
                applyOperationsFuzzWalWithEarlyRangeTask(op.day, oracle);
            }
            case BASE_UPDATE -> updateOperationsFuzzDay(op.day, oracle);
            case BASE_SET_TTL -> {
                execute("ALTER TABLE base SET TTL " + op.day + " DAYS");
                oracle.onTtl(op.day);
            }
            case BASE_REPLACE -> replaceOperationsFuzzBase(op, oracle, timestampType);
            case BASE_REPLACE_FULL_RESTART -> {
                // the view goes invalid, FULL rebuilds it from the new base, and the restart comes before the view's
                // WAL applies the rebuild
                replaceOperationsFuzzBase(op, oracle, timestampType);
                drainOperationsFuzzQueues(oracle);
                LOG.info().$("fuzz: FULL after base replacement, then restart").$();
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                oracle.onFullIssued();
                drainMatViewQueue(engine);
                oracle.onRefreshJobRun();
                restartOperationsFuzzViewStates(oracle);
            }
            case BASE_REPLACE_RESUME_RESTART -> {
                // the view goes invalid, RESUME SKIP BACKLOG positions it on the new base, and the restart comes before
                // the view's WAL applies its state. A FULL issued before the replacement can rebuild the view from the
                // new base before the drop's invalidation reaches it, and then RESUME catches up on the new base.
                final boolean isFullPending = oracle.isFullPending();
                replaceOperationsFuzzBase(op, oracle, timestampType);
                drainOperationsFuzzQueues(oracle);
                final MatViewState state = opsViewState("mv");
                if (state.isInvalid()) {
                    LOG.info().$("fuzz: RESUME after base replacement, then restart").$();
                    try {
                        execute("REFRESH MATERIALIZED VIEW mv RESUME");
                        Assert.assertTrue("RESUME must refuse the backlog of a replaced base", isFullPending);
                        oracle.onResumed();
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "materialized view backlog is lost");
                        oracle.onBacklogLostMarkerUnpersisted();
                        execute("REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG");
                        oracle.onResumed();
                        oracle.onGapAccepted();
                    }
                    Assert.assertFalse(state.isInvalid());
                }
                restartOperationsFuzzViewStates(oracle);
            }
            case VIEW_ADD_COLUMN -> {
                final long viewSeqTxn = opsViewSeqTxn();
                try {
                    execute("ALTER MATERIALIZED VIEW mv ADD COLUMN " + op.column + ' ' + op.columnType);
                    Assert.assertFalse("ADD COLUMN must refuse a view whose SQL copies by position [column=" + op.column + ']', op.isRefused);
                    generator.onColumnChangeIssued();
                    oracle.onViewColumnAdded(op.column, generator.getSqlColumns());
                } catch (SqlException e) {
                    if (!op.isRefused) {
                        throw e;
                    }
                    TestUtils.assertContains(e.getFlyweightMessage(), "materialized view SQL relies on generated column names");
                    Assert.assertEquals("a refused ADD COLUMN wrote to the view's WAL", viewSeqTxn, opsViewSeqTxn());
                }
            }
            case VIEW_DROP_COLUMN -> {
                if (generator.isPolicyChangePending()) {
                    // DROP COLUMN checks the EXPIRE ROWS policy the view has applied, see
                    // MatViewFuzzOpGenerator.isPolicyChangePending()
                    drainWalQueue();
                    generator.onViewWalApplied();
                }
                final long viewSeqTxn = opsViewSeqTxn();
                try {
                    execute("ALTER MATERIALIZED VIEW mv DROP COLUMN " + op.column);
                    Assert.assertFalse("DROP COLUMN must refuse a column the EXPIRE ROWS policy reads [column=" + op.column
                            + ", policy=" + generator.getExpireClause() + ']', op.isRefused);
                    generator.onColumnChangeIssued();
                    oracle.onViewColumnDropped(op.column);
                } catch (SqlException e) {
                    if (!op.isRefused) {
                        throw e;
                    }
                    // the statement checks the policy first
                    if (generator.getPolicyColumns().indexOf(op.column) > -1) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "the EXPIRE ROWS policy of the materialized view reads the column [column=" + op.column + ']');
                    } else {
                        TestUtils.assertContains(e.getFlyweightMessage(), "materialized view SQL relies on generated column names");
                    }
                    Assert.assertEquals("a refused DROP COLUMN wrote to the view's WAL", viewSeqTxn, opsViewSeqTxn());
                }
            }
            case VIEW_SET_EXPIRE -> {
                if (op.isProbed && generator.isColumnChangePending()) {
                    // the read probe of a KEEP or window policy compiles against the view's applied columns
                    drainWalQueue();
                    generator.onViewWalApplied();
                }
                final long viewSeqTxn = opsViewSeqTxn();
                try {
                    execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS " + op.policy);
                    Assert.assertFalse("SET EXPIRE must refuse a policy over a column the view lacks: " + op.policy, op.isRefused);
                    generator.onPolicyChangeIssued();
                } catch (SqlException e) {
                    if (!op.isRefused) {
                        throw e;
                    }
                    TestUtils.assertContains(e.getFlyweightMessage(), "EXPIRE ROWS");
                    TestUtils.assertContains(e.getFlyweightMessage(), op.column);
                    Assert.assertEquals("a refused SET EXPIRE wrote to the view's WAL", viewSeqTxn, opsViewSeqTxn());
                }
            }
            case VIEW_DROP_EXPIRE -> {
                execute("ALTER MATERIALIZED VIEW mv DROP EXPIRE");
                generator.onPolicyChangeIssued();
            }
            case VIEW_DROP_PARTITION_LIST -> {
                sql.put("ALTER MATERIALIZED VIEW mv DROP PARTITION LIST ");
                appendDay(sql, op.day);
                execute(sql);
                oracle.onViewDropped(op.day);
            }
            case VIEW_DROP_PARTITION_WHERE -> {
                sql.put("ALTER MATERIALIZED VIEW mv DROP PARTITION WHERE ts < ");
                appendDay(sql, op.day);
                execute(sql);
                oracle.onViewDroppedBefore(op.day);
            }
            case VIEW_ALTER_AS -> {
                final String viewSql = MatViewFuzzOpGenerator.viewSql(op.sqlColumns, op.isPositional);
                LOG.info().$("fuzz: ALTER AS ").$(viewSql).$();
                final long viewSeqTxn = opsViewSeqTxn();
                try {
                    execute("ALTER MATERIALIZED VIEW mv AS (" + viewSql + ')');
                    Assert.assertFalse("ALTER ... AS must refuse a column out of its position: " + viewSql, op.isRefused);
                    oracle.onSqlChanged(sqlColumnsBefore, op.sqlColumns);
                    if (op.isPositional || wasSqlPositional) {
                        // View ADD and DROP COLUMN check the SQL the view has applied, so its WAL applies the SQL
                        // before the generator's next prediction. That also steers clear of a known bug until it is
                        // fixed: view ADD and DROP COLUMN pass while a SQL that copies by position waits in the view's
                        // WAL, and can then move a column under it. Remove that part of the steering with the fix.
                        drainWalQueue();
                        generator.onViewWalApplied();
                    }
                } catch (SqlException e) {
                    if (!op.isRefused) {
                        throw e;
                    }
                    final ObjList<String> viewColumns = generator.getViewColumns();
                    if (op.isPositional && op.sqlColumns.size() != viewColumns.size()) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "materialized view query does not match view schema [view=mv, queryColumnCount="
                                + op.sqlColumns.size() + ", viewColumnCount=" + viewColumns.size() + ']');
                    } else {
                        TestUtils.assertContains(e.getFlyweightMessage(), "materialized view column does not match the query column at the same position");
                    }
                    Assert.assertEquals("a refused ALTER ... AS wrote to the view's WAL", viewSeqTxn, opsViewSeqTxn());
                }
            }
            case VIEW_RANGE -> {
                sql.put("REFRESH MATERIALIZED VIEW mv RANGE FROM ");
                appendDay(sql, op.day);
                sql.put(" TO ");
                appendDay(sql, op.dayHi);
                // a REFRESH waits for an ALTER ... AS until the view's WAL applies it
                drainWalQueue();
                try {
                    execute(sql);
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "materialized view is invalid, run REFRESH MATERIALIZED VIEW mv RESUME first");
                    Assert.assertTrue(opsViewState("mv").isInvalid());
                }
            }
            case VIEW_FULL -> {
                drainWalQueue();
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                oracle.onFullIssued();
            }
            case VIEW_RESUME -> {
                final MatViewState state = opsViewState("mv");
                runOperationsFuzzResume(oracle, state.isInvalid() ? state.getInvalidationReasonForReport() : null, op.isSkipBacklog);
            }
            case VIEW_PERIOD_REFRESH -> {
                // The period timer fires at the end of the day: it enqueues the period's range refresh unless the view
                // is invalid or about to go invalid, as MatViewTimerJob does.
                currentMicros = OPS_DAY_LO + (op.day + 1) * Micros.DAY_MICROS;
                oracle.onPeriodCompleted();
                final MatViewState state = opsViewState("mv");
                if (!state.isInvalid() && !state.hasPendingInvalidationReason()) {
                    final long periodHi = state.getViewDefinition().getBaseTableTimestampDriver().fromMicros(currentMicros) - 1;
                    engine.getMatViewStateStore().enqueueRangeRefresh(engine.verifyTableName("mv"), Numbers.LONG_NULL, periodHi);
                }
            }
            case VIEW_INVALIDATE -> {
                engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("mv"), FUZZ_INVALIDATION_REASON);
                oracle.onInvalidationPublished();
            }
            case BACKLOG_UPDATE ->
                    engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(engine.verifyTableName("mv"));
            case RESTART -> restartOperationsFuzzViewStates(oracle);
            case APPLY_WAL -> drainWalQueue();
            case RUN_REFRESH_JOB -> {
                if (isTimerView) {
                    fireOperationsFuzzTimer();
                }
                drainMatViewQueue(engine);
                oracle.onRefreshJobRun();
            }
            // under the ADAPTIVE commit mode, a save point of the view's applied transactions comes first, as the WAL
            // apply job takes one now and then, so that the purge deletes the view's txn-log parts below them
            case PURGE_WAL -> MatViewTxnLogParts.purge(engine, engine.verifyTableName("mv"));
            default -> throw new AssertionError("unexpected operation: " + op.kind);
        }
    }

    // RESUME, or RESUME SKIP BACKLOG, of the view. Plain RESUME catches up on a tracked backlog and refuses a lost one;
    // either refuses while a FULL refresh is pending.
    private static void runOperationsFuzzResume(MatViewFuzzOracle oracle, String invalidationReason, boolean isSkipBacklog) throws SqlException {
        LOG.info().$("fuzz: resuming view [skipBacklog=").$(isSkipBacklog).I$();
        // RESUME waits for the view's WAL
        drainWalQueue();
        final MatViewState state = opsViewState("mv");
        final boolean wasInvalid = state.isInvalid();
        try {
            execute(isSkipBacklog ? "REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG" : "REFRESH MATERIALIZED VIEW mv RESUME");
        } catch (SqlException e) {
            final CharSequence message = e.getFlyweightMessage();
            if (Chars.contains(message, "materialized view backlog is lost")) {
                Assert.assertFalse("RESUME SKIP BACKLOG refused a lost backlog", isSkipBacklog);
                if (oracle.isBacklogExpectedTracked(invalidationReason)) {
                    Assert.fail("RESUME found the backlog of a forced invalidation lost: " + message);
                }
                oracle.onBacklogLostMarkerUnpersisted();
                return;
            }
            TestUtils.assertContains(message, "materialized view full refresh is pending");
            return;
        }
        if (wasInvalid) {
            Assert.assertFalse(state.isInvalid() && !state.isPendingInvalidation());
            oracle.onResumed();
            if (isSkipBacklog) {
                oracle.onGapAccepted();
            }
        }
    }

    private static void runViewAndBaseOperations(Rnd rnd, int steps) throws Exception {
        // The transaction logs of the base and the view in one file, or in parts, which the WAL purge deletes below the
        // one of the table's applied seqTxn, so that a restart loads the view's state above the deleted parts.
        final int seqPartTxnCount = MatViewFuzzOpGenerator.pickSeqPartTxnCount(rnd);
        LOG.info().$("fuzz: txn log part size ").$(seqPartTxnCount).$();
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, seqPartTxnCount);
        final String timestampType = rnd.nextBoolean() ? "TIMESTAMP" : "TIMESTAMP_NS";
        // The record copier the view's refresh writes rows with: picked by the schema's size (0), or forced to the
        // single-method, chunked or looping flavour. Production uses the looping copier for wide views when chunked
        // copiers are disabled or fail.
        final int copierType = rnd.nextInt(4);
        LOG.info().$("fuzz: copier type ").$(copierType).$();
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
        // The view: IMMEDIATE, or a period view of one day, IMMEDIATE or EVERY. A period view's clock starts at the
        // start of the first day, when no period has ended yet.
        final String viewRefresh = switch (rnd.nextInt(3)) {
            case 0 -> null;
            case 1 -> "IMMEDIATE";
            default -> "EVERY 1h";
        };
        final boolean isPeriodView = viewRefresh != null;
        final boolean isTimerView = "EVERY 1h".equals(viewRefresh);
        currentMicros = isPeriodView ? OPS_DAY_LO : -1;
        final MatViewFuzzOpGenerator generator = new MatViewFuzzOpGenerator(OPS_DAY_COUNT, isPeriodView);
        final MatViewFuzzOpGenerator.Op op = new MatViewFuzzOpGenerator.Op();
        createOperationsFuzzBase(timestampType);
        final ObjList<String> sqlColumns = generator.initView(rnd);
        final MatViewFuzzOracle oracle = new MatViewFuzzOracle(OPS_DAY_COUNT, generator.getViewColumns(), isPeriodView);
        // rows in every day, but the gap days of a period view
        for (int day = 0; day < OPS_DAY_COUNT; day++) {
            insertOperationsFuzzRows(generator.initRows(rnd, day, op), oracle);
            if (generator.isGapDay(day)) {
                LOG.info().$("fuzz: gap day ").$(day).$();
            }
        }
        LOG.info().$("fuzz: view SQL ").$(MatViewFuzzOpGenerator.viewSql(sqlColumns)).$(" [refresh=").$(viewRefresh).I$();
        execute("CREATE MATERIALIZED VIEW mv " + (isPeriodView ? "REFRESH " + viewRefresh + " PERIOD (LENGTH 1d) " : "")
                + "AS (" + MatViewFuzzOpGenerator.viewSql(sqlColumns) + ") PARTITION BY DAY");
        if (isTimerView) {
            // the first EVERY firing builds the view
            fireOperationsFuzzTimer();
        }
        drainOperationsFuzzQueues(oracle);

        final ObjList<String> sqlColumnsBefore = new ObjList<>();
        boolean isViewColumnAdded = false;
        for (int step = 0; step < steps; step++) {
            sqlColumnsBefore.clear();
            sqlColumnsBefore.addAll(generator.getSqlColumns());
            final boolean wasSqlPositional = generator.isSqlPositional();
            generator.next(rnd, op);
            LOG.info().$("fuzz: step ").$(step).$(" ").$(op.kind.name()).$(" [day=").$(op.day).$(", column=").$(op.column).I$();
            // Skips the sequence that questdb/questdb#8330 breaks until master fixes it: once a column was added to the
            // view, a DROP PARTITION that removes the view's newest partition can leave the column's "added at"
            // partition stale in the view's _cv file, and the column then reads NULL in the partitions a later refresh
            // creates before it. A drop that empties the view is one such drop. Remove the skip with the fix.
            if (isViewColumnAdded && isDroppingNewestViewPartition(op)) {
                LOG.info().$("fuzz: skipping view DROP PARTITION of the newest view partition, see questdb/questdb#8330").$();
            } else {
                runOperationsFuzzOp(generator, op, sqlColumnsBefore, wasSqlPositional, oracle, timestampType, isTimerView);
            }
            isViewColumnAdded |= op.kind == MatViewFuzzOpGenerator.Kind.VIEW_ADD_COLUMN && !op.isRefused;
            if (rnd.nextBoolean()) {
                drainWalAndMatViewQueues();
                oracle.onDrained(isOperationsFuzzFullPending());
            }
            if (step % 6 == 5 || step == steps - 1) {
                checkOperationsFuzzViews(rnd, generator, oracle, step);
            }
        }
        execute("DROP MATERIALIZED VIEW mv");
        execute("DROP TABLE base");
        drainWalAndMatViewQueues();
    }

    // A check of testPassthroughViewAlterAs() and testAggregatingViewAlterAs(): catches the view up, as the next base
    // commit would after a restart lost its refresh, and asserts that the view is valid, has the SQL the fuzzer last
    // saw taken, and holds exactly the rows the SQL gives on the base.
    private static void checkAlterAsFuzzView(String viewSql, int step) throws Exception {
        final TableToken viewToken = engine.verifyTableName("mv");
        engine.getMatViewStateStore().enqueueIncrementalRefresh(viewToken);
        for (int i = 0; i < 3; i++) {
            drainWalAndMatViewQueues();
        }
        final String context = "[step=" + step + ", sql=" + viewSql + ']';
        assertNotSuspended("base");
        assertNotSuspended("mv");
        final MatViewState state = opsViewState("mv");
        Assert.assertFalse("view is invalid: " + state.getInvalidationReasonForReport() + ' ' + context, state.isInvalid());
        Assert.assertEquals(context, viewSql, state.getViewDefinition().getMatViewSql());
        assertSqlCursors(
                "SELECT ts, sym, price, qty FROM (" + viewSql + ") ORDER BY ts, sym, price, qty",
                "SELECT ts, sym, price, qty FROM mv ORDER BY ts, sym, price, qty"
        );
    }

    private static void createMatView(String viewSql, String mvName, boolean deferred) throws SqlException {
        execute("create materialized view " + mvName + " refresh immediate " + (deferred ? "deferred" : "") + " as (" + viewSql + ") partition by DAY");
    }

    private static void createPeriodMatView(
            String viewSql,
            String mvName,
            int interval,
            char intervalUnit,
            @Nullable String timeZone,
            boolean deferred
    ) throws SqlException {
        execute(
                "create materialized view " + mvName + " refresh immediate " + (deferred ? "deferred" : "") +
                        " period (length " + interval + intervalUnit + (timeZone != null ? " time zone '" + timeZone + "'" : "") +
                        ") as (" + viewSql + ") partition by DAY"
        );
    }

    private static void createTimerMatView(String viewSql, String mvName, long start, int interval, char intervalUnit, boolean deferred) throws SqlException {
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, start);
        execute("create materialized view " + mvName + " refresh every " + interval + intervalUnit + " " + (deferred ? "deferred" : "") + " start '" + sink + "' as (" + viewSql + ") partition by DAY");
    }

    // Draws a case of testMultipleQueryExecutionsPerRefreshDstGapDayBuckets(): a zone of DST_GAP_ZONES, SAMPLE BY 1d to
    // 3d, or 1M a quarter of the time, and a WITH OFFSET inside the zone's gap half of the time, anywhere in the day
    // otherwise. The first batch starts three days before the transition, the later ones write around a point six
    // hours after it.
    private static void generateDstGapDayBucketCase(Rnd rnd, String tableName, DstGapDayBucketCase fuzzCase) throws NumericException {
        final String[] zone = DST_GAP_ZONES[rnd.nextInt(DST_GAP_ZONES.length)];
        fuzzCase.zone = zone[0];
        fuzzCase.transitionTs = MicrosFormatUtils.parseUTCTimestamp(zone[1]);
        fuzzCase.isOffsetInGap = rnd.nextBoolean();
        if (fuzzCase.isOffsetInGap) {
            final int gapLo = Integer.parseInt(zone[2]);
            fuzzCase.offsetMinutes = gapLo + rnd.nextInt(Integer.parseInt(zone[3]) - gapLo + 1);
        } else {
            do {
                fuzzCase.offsetMinutes = rnd.nextInt(24 * 60);
            } while (fuzzCase.offsetMinutes >= Integer.parseInt(zone[2]) && fuzzCase.offsetMinutes <= Integer.parseInt(zone[3]));
        }
        fuzzCase.unit = rnd.nextInt(4) == 0 ? 'M' : 'd';
        fuzzCase.stride = fuzzCase.unit == 'M' ? 1 : 1 + rnd.nextInt(3);
        fuzzCase.start = fuzzCase.transitionTs - 3 * Micros.DAY_MICROS;
        fuzzCase.hotTs = fuzzCase.transitionTs + 6 * Micros.HOUR_MICROS;
        final int hours = fuzzCase.offsetMinutes / 60;
        final int minutes = fuzzCase.offsetMinutes % 60;
        fuzzCase.sql.clear();
        fuzzCase.sql.put("select min(c3) as min, max(c3) as max, ts from ").put(tableName)
                .put(" sample by ").put(fuzzCase.stride).put(fuzzCase.unit)
                .put(" align to calendar time zone '").put(fuzzCase.zone).put("' with offset '")
                .put(hours < 10 ? "0" : "").put(hours).put(':').put(minutes < 10 ? "0" : "").put(minutes).put('\'');
    }

    // The SQL of an ALTER ... AS of testAggregatingViewAlterAs(): the sums of price and qty of each symbol by the hour
    // over the rows of a SQL of generatePassthroughSql(), the inner SQL, whose designated timestamp passes the base's
    // through, or is shifted or taken from ts2. The aggregating SELECT buckets its timestamp ts by SAMPLE BY, or by
    // timestamp_floor() with or without its origin argument, and reads the inner SQL as a subquery, a CTE or the master
    // of a join with the table other. A TIMESTAMP(ts) clause on the inner SQL restores the designated timestamp that a
    // shift loses, and appears at random elsewhere. One more SELECT may wrap the aggregating one, naming its columns
    // or selecting all. ALTER ... AS takes the SQL when the inner SQL passes the base's timestamp through, and
    // otherwise refuses it with the aggregating timestamp rule, at the outermost timestamp column, or at the SQL's
    // start under a wildcard.
    private static void generateAggregatingSql(Rnd rnd, AggregatingSql out) {
        generatePassthroughSql(rnd, out.inner);
        final String inner = out.inner.sql.toString();
        out.source = rnd.nextInt(3);
        out.bucketing = rnd.nextInt(3);
        out.wrapper = rnd.nextInt(3);
        final boolean hasTimestampClause = out.inner.kind == PassthroughSql.SHIFTED || rnd.nextInt(3) == 0;
        final StringBuilder sb = new StringBuilder();
        final String from;
        final String prefix;
        switch (out.source) {
            case AggregatingSql.CTE -> {
                sb.append("WITH a AS (").append(inner).append(") ");
                from = hasTimestampClause ? "a TIMESTAMP(ts)" : "a";
                prefix = !hasTimestampClause && rnd.nextBoolean() ? "a." : "";
            }
            case AggregatingSql.JOIN -> {
                from = (hasTimestampClause ? "((" + inner + ") TIMESTAMP(ts))" : "(" + inner + ")") + " b JOIN other o ON (sym)";
                // other has the columns ts and sym too
                prefix = "b.";
            }
            default -> {
                // SUBQUERY
                final boolean isAliased = !hasTimestampClause && rnd.nextBoolean();
                from = "(" + inner + ")" + (hasTimestampClause ? " TIMESTAMP(ts)" : (isAliased ? " t" : ""));
                prefix = isAliased && rnd.nextBoolean() ? "t." : "";
            }
        }
        final String timestampColumn = switch (out.bucketing) {
            case AggregatingSql.SAMPLE_BY -> prefix + "ts";
            case AggregatingSql.FLOOR -> "timestamp_floor('1h', " + prefix + "ts) AS ts";
            // FLOOR_FROM
            default -> "timestamp_floor('1h', " + prefix + "ts, '2024-01-01T00:00:00.000000Z') AS ts";
        };
        final String[] columns = {timestampColumn, prefix + "sym", "sum(" + prefix + "price) AS price", "sum(" + prefix + "qty) AS qty"};
        final int rotation = rnd.nextInt(4);
        sb.append("SELECT ");
        int timestampOffset = -1;
        for (int i = 0; i < 4; i++) {
            final int index = (i + rotation) % 4;
            if (i > 0) {
                sb.append(", ");
            }
            if (index == 0) {
                timestampOffset = sb.length();
            }
            sb.append(columns[index]);
        }
        sb.append(" FROM ").append(from).append(out.bucketing == AggregatingSql.SAMPLE_BY ? " SAMPLE BY 1h" : " ORDER BY ts");
        out.sql.clear();
        switch (out.wrapper) {
            case AggregatingSql.WILDCARD -> {
                out.sql.put("SELECT * FROM (").put(sb).put(')');
                out.timestampPosition = 0;
                out.topTimestampColumn = null;
            }
            case AggregatingSql.NAMED -> {
                out.sql.put("SELECT sym, ts, price, qty FROM (").put(sb).put(')');
                out.timestampPosition = "SELECT sym, ".length();
                out.topTimestampColumn = "ts";
            }
            default -> {
                // NO_WRAPPER
                out.sql.put(sb);
                out.timestampPosition = timestampOffset;
                out.topTimestampColumn = timestampColumn;
            }
        }
    }

    // The SQL of an ALTER ... AS of testPassthroughViewAlterAs(): the columns ts, sym, price and qty of the base's rows
    // through zero to two SELECTs over the innermost one, the outermost of them over a subquery or a CTE. Each level
    // names its timestamp column ts or another name, takes it as the level below names it, in any spelling, under a
    // table alias at times, and filters no row; the outermost one can order its columns otherwise or select them all.
    // With half the chance the designated timestamp passes the base's through. Otherwise one level shifts it with
    // dateadd(), or the innermost SELECT takes the base column ts2 under TIMESTAMP(ts2), and ALTER ... AS refuses the
    // SQL with the passthrough timestamp rule. The level below a shift names its timestamp ts: a dateadd() of a
    // timestamp under another name loses the designated timestamp, which the statement refuses with another error.
    private static void generatePassthroughSql(Rnd rnd, PassthroughSql out) {
        final int depth = rnd.nextInt(3);
        final int draw = rnd.nextInt(4);
        out.depth = depth;
        out.kind = draw < 2 ? PassthroughSql.PASSED_THROUGH : (draw == 2 ? PassthroughSql.SHIFTED : PassthroughSql.OTHER_COLUMN);
        out.transformLevel = switch (out.kind) {
            case PassthroughSql.SHIFTED -> rnd.nextInt(depth + 1);
            case PassthroughSql.OTHER_COLUMN -> 0;
            default -> -1;
        };
        out.isCteTop = depth > 0 && rnd.nextInt(3) == 0;
        out.isWildcardTop = false;
        out.topTimestampColumn = null;
        out.timestampPosition = 0;
        String levelSql = null;
        String levelName = null;
        final StringBuilder sb = new StringBuilder();
        final ObjList<String> columns = new ObjList<>();
        for (int level = 0; level <= depth; level++) {
            final boolean isTop = level == depth;
            final boolean isTransform = level == out.transformLevel;
            // the view's timestamp name at the top and below a shift, elsewhere ts or another name
            final String name = isTop || level + 1 == out.transformLevel || rnd.nextBoolean() ? "ts" : "x" + level;
            final String timestampColumn;
            final String from;
            boolean hasTs2 = false;
            if (level == 0) {
                // no alias before a TIMESTAMP clause
                final boolean isOtherColumn = out.kind == PassthroughSql.OTHER_COLUMN;
                final boolean isAliased = !isOtherColumn && rnd.nextInt(4) == 0;
                final String ref;
                if (isOtherColumn) {
                    ref = "ts2";
                } else if (isAliased) {
                    ref = rnd.nextBoolean() ? "b.ts" : "ts";
                } else {
                    ref = switch (rnd.nextInt(5)) {
                        case 0 -> "TS";
                        case 1 -> "\"ts\"";
                        case 2 -> "base.ts";
                        default -> "ts";
                    };
                }
                if (isTransform && !isOtherColumn) {
                    timestampColumn = shiftPassthroughTimestamp(rnd, ref) + " AS " + name;
                } else {
                    // each spelling of ts names the column ts, in one letter case or another
                    timestampColumn = !isOtherColumn && name.equals("ts") && rnd.nextBoolean() ? ref : ref + " AS " + name;
                }
                if (isOtherColumn) {
                    from = "base TIMESTAMP(ts2)";
                } else if (isAliased) {
                    from = "base b";
                } else {
                    from = rnd.nextInt(4) == 0 ? "base TIMESTAMP(ts)" : "base";
                }
                // a column the view does not have, which an outer SELECT * carries up
                hasTs2 = !isOtherColumn && rnd.nextInt(4) == 0;
            } else {
                final boolean isCte = isTop && out.isCteTop;
                final boolean isAliased = !isCte && rnd.nextInt(4) == 0;
                final String ref = isAliased ? "t." + levelName : levelName;
                if (!isTransform && name.equals(levelName) && rnd.nextInt(3) == 0) {
                    // SELECT *
                    timestampColumn = null;
                } else if (isTransform) {
                    timestampColumn = shiftPassthroughTimestamp(rnd, ref) + " AS " + name;
                } else {
                    timestampColumn = ref.equals(name) ? ref : ref + " AS " + name;
                }
                from = isCte ? "c" : "(" + levelSql + ")" + (isAliased ? " t" : "");
            }

            sb.setLength(0);
            if (isTop && out.isCteTop) {
                sb.append("WITH c AS (").append(levelSql).append(") ");
            }
            sb.append("SELECT ");
            if (timestampColumn == null) {
                sb.append('*');
                out.isWildcardTop = isTop;
            } else {
                columns.clear();
                columns.add(timestampColumn);
                columns.add("sym");
                columns.add("price");
                columns.add("qty");
                // the outermost SELECT can put the timestamp column after the others
                final int rotation = isTop ? rnd.nextInt(4) : 0;
                for (int i = 0; i < 4; i++) {
                    final int index = (i + rotation) % 4;
                    if (i > 0) {
                        sb.append(", ");
                    }
                    if (isTop && index == 0) {
                        out.timestampPosition = sb.length();
                        out.topTimestampColumn = timestampColumn;
                    }
                    sb.append(columns.getQuick(index));
                }
                if (hasTs2) {
                    sb.append(", ts2");
                }
            }
            sb.append(" FROM ").append(from);
            if (rnd.nextInt(4) == 0) {
                // no base row has a negative price
                sb.append(" WHERE price > -1");
            }
            levelSql = sb.toString();
            levelName = name;
        }
        out.sql.clear();
        out.sql.put(levelSql);
    }

    // Half of the views exclude two hours of base data with their own NOT BETWEEN on the designated
    // timestamp. Each refresh adds its refresh range to the same where-clause model.
    private static String generateViewSql(Rnd rnd, String tableName) {
        return "select min(c3) as min, max(c3) as max, ts from " + tableName +
                (rnd.nextBoolean() ? " where ts not between " + NOT_BETWEEN_LO + " and " + NOT_BETWEEN_HI : "") +
                " sample by 1h";
    }

    // Returns an empty string, i.e. the default calendar alignment, or ALIGN TO CALENDAR with an optional
    // time zone and an optional offset. With a month or year stride, a positive offset moves the bucket
    // start past midnight on the 1st, and a negative offset moves it to the last day of the previous month,
    // which is shorter than 31 days at the end of February or of a 30-day month.
    // Rows at random times of the days of testPassthroughViewAlterAs(), in and out of order, whose ts2 lies within an
    // hour of their ts.
    private static void insertPassthroughFuzzRows(Rnd rnd, int rowCount) throws SqlException {
        final StringSink sink = new StringSink();
        sink.put("INSERT INTO base VALUES ");
        for (int i = 0; i < rowCount; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            final long ts = OPS_DAY_LO + rnd.nextLong(OPS_DAY_COUNT * Micros.DAY_MICROS);
            sink.put("('");
            MicrosFormatUtils.appendDateTimeUSec(sink, ts);
            sink.put("', '");
            MicrosFormatUtils.appendDateTimeUSec(sink, ts - Micros.HOUR_MICROS + rnd.nextLong(2 * Micros.HOUR_MICROS));
            sink.put("', 's").put(rnd.nextInt(3)).put("', ").put(rnd.nextInt(1000)).put(".5, ").put(rnd.nextInt(100)).put(')');
        }
        execute(sink);
    }

    private static String randomAlignment(Rnd rnd) {
        if (rnd.nextInt(4) == 0) {
            return "";
        }
        final StringBuilder sb = new StringBuilder(" align to calendar");
        if (rnd.nextInt(3) == 0) {
            sb.append(" time zone '").append(timeZones.getQuick(rnd.nextInt(timeZones.size()))).append('\'');
        }
        if (rnd.nextInt(4) > 0) {
            final int hours = rnd.nextInt(24);
            final int minutes = rnd.nextInt(60);
            sb.append(" with offset '").append(rnd.nextBoolean() ? "-" : "")
                    .append(hours < 10 ? "0" : "").append(hours).append(':')
                    .append(minutes < 10 ? "0" : "").append(minutes).append('\'');
        }
        return sb.toString();
    }

    // NOT BETWEEN reads on the designated timestamp, each on a compiler drawn from the pool that the
    // refresh jobs share, so the refresh compiles land on compilers that have analyzed a negated
    // BETWEEN. An equivalent OR filter checks the count of each read.
    private static void runNotBetweenReads(String tableName) throws SqlException {
        for (int i = 0; i < 30; i++) {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertSqlCursors(
                        compiler,
                        sqlExecutionContext,
                        "select count() from " + tableName + " where ts < " + NOT_BETWEEN_LO + " or ts > " + NOT_BETWEEN_HI,
                        "select count() from " + tableName + " where ts not between " + NOT_BETWEEN_LO + " and " + NOT_BETWEEN_HI,
                        LOG
                );
            }
        }
    }

    // A round of testPassthroughViewAlterAs() or testAggregatingViewAlterAs(): a base with the designated timestamp ts
    // and another TIMESTAMP column ts2, of either precision, and a view of its ts, sym, price and qty, which passes the
    // base's rows through, or sums the price and qty of each symbol by the hour. The aggregating round reads the table
    // other, which holds a row of each symbol, in its joins.
    private static void runAlterAsOperations(Rnd rnd, int steps, boolean isAggregating) throws Exception {
        final String timestampType = rnd.nextBoolean() ? "TIMESTAMP" : "TIMESTAMP_NS";
        execute("CREATE TABLE base (ts " + timestampType + ", ts2 " + timestampType + ", sym SYMBOL, price DOUBLE, qty LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        insertPassthroughFuzzRows(rnd, 1 + rnd.nextInt(20));
        String viewSql;
        if (isAggregating) {
            execute("CREATE TABLE other (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO other VALUES ('2024-01-01T00:00:00.000000Z', 's0', 1.0), ('2024-01-01T00:00:00.000000Z', 's1', 2.0), ('2024-01-01T00:00:00.000000Z', 's2', 3.0)");
            viewSql = "SELECT ts, sym, sum(price) AS price, sum(qty) AS qty FROM base SAMPLE BY 1h";
        } else {
            viewSql = "SELECT ts, sym, price, qty FROM base";
        }
        LOG.info().$("fuzz: ").$(isAggregating ? "aggregating" : "passthrough").$(" view [timestampType=").$(timestampType).I$();
        execute("CREATE MATERIALIZED VIEW mv AS (" + viewSql + ") PARTITION BY DAY");
        drainWalAndMatViewQueues();
        final TableToken viewToken = engine.verifyTableName("mv");
        final String alterHead = "ALTER MATERIALIZED VIEW mv AS (";
        final PassthroughSql passthroughAlter = new PassthroughSql();
        final AggregatingSql aggregatingAlter = new AggregatingSql();
        for (int step = 0; step < steps; step++) {
            switch (rnd.nextInt(10)) {
                case 0, 1, 2 ->
                        insertPassthroughFuzzRows(rnd, rnd.nextInt(8) == 0 ? 20 + rnd.nextInt(40) : 1 + rnd.nextInt(5));
                case 3, 4, 5 -> {
                    final String sql;
                    final int kind;
                    final int timestampPosition;
                    if (isAggregating) {
                        generateAggregatingSql(rnd, aggregatingAlter);
                        sql = aggregatingAlter.sql.toString();
                        kind = aggregatingAlter.inner.kind;
                        timestampPosition = aggregatingAlter.timestampPosition;
                    } else {
                        generatePassthroughSql(rnd, passthroughAlter);
                        sql = passthroughAlter.sql.toString();
                        kind = passthroughAlter.kind;
                        timestampPosition = passthroughAlter.timestampPosition;
                    }
                    LOG.info().$("fuzz: step ").$(step).$(" ALTER AS ").$(sql).$(" [kind=").$(kind).I$();
                    final long viewSeqTxn = engine.getTableSequencerAPI().lastTxn(viewToken);
                    try {
                        execute(alterHead + sql + ')');
                        Assert.assertEquals("ALTER ... AS must refuse a designated timestamp that is not the base's as it is: " + sql,
                                PassthroughSql.PASSED_THROUGH, kind);
                        // one definition event
                        Assert.assertEquals(viewSeqTxn + 1, engine.getTableSequencerAPI().lastTxn(viewToken));
                        viewSql = sql;
                    } catch (SqlException e) {
                        if (kind == PassthroughSql.PASSED_THROUGH) {
                            throw e;
                        }
                        TestUtils.assertEquals(
                                sql,
                                isAggregating
                                        ? "aggregating materialized view timestamp must bucket the base table designated timestamp without transformation"
                                        : "passthrough materialized view timestamp must reference the base table designated timestamp without transformation",
                                e.getFlyweightMessage()
                        );
                        Assert.assertEquals(sql, alterHead.length() + timestampPosition, e.getPosition());
                        Assert.assertEquals("a refused ALTER ... AS wrote to the view's WAL: " + sql, viewSeqTxn, engine.getTableSequencerAPI().lastTxn(viewToken));
                    }
                }
                case 6 -> drainWalQueue();
                case 7 -> drainMatViewQueue(engine);
                case 8 -> {
                    LOG.info().$("fuzz: step ").$(step).$(" FULL").$();
                    // a REFRESH waits for an ALTER ... AS until the view's WAL applies it
                    drainWalQueue();
                    execute("REFRESH MATERIALIZED VIEW mv FULL");
                }
                default -> {
                    LOG.info().$("fuzz: step ").$(step).$(" restarting view states").$();
                    engine.getMatViewStateStore().clear();
                    engine.hydrateMatViewStateStore();
                }
            }
            if (rnd.nextBoolean()) {
                drainWalAndMatViewQueues();
            }
            if (step % 6 == 5 || step == steps - 1) {
                checkAlterAsFuzzView(viewSql, step);
            }
        }
        execute("DROP MATERIALIZED VIEW mv");
        execute("DROP TABLE base");
        if (isAggregating) {
            execute("DROP TABLE other");
        }
        drainWalAndMatViewQueues();
    }

    // dateadd() of the timestamp column by a nonzero number of minutes or hours, forward or back.
    private static String shiftPassthroughTimestamp(Rnd rnd, String ref) {
        final int stride = (1 + rnd.nextInt(30)) * (rnd.nextBoolean() ? 1 : -1);
        return "dateadd('" + (rnd.nextBoolean() ? 'm' : 'h') + "', " + stride + ", " + ref + ")";
    }

    private static int zoneIndex(String zone) {
        for (int i = 0; i < DST_GAP_ZONES.length; i++) {
            if (DST_GAP_ZONES[i][0].equals(zone)) {
                return i;
            }
        }
        throw new AssertionError("unexpected zone: " + zone);
    }

    private ObjList<FuzzTransaction> createTransactionsAndMv(
            Rnd rnd,
            String tableNameBase,
            String matViewName,
            String viewSql
    ) throws SqlException, NumericException {
        fuzzer.createInitialTableWal(tableNameBase, timestampTypes[rnd.nextInt(10) % 2]);
        runNotBetweenReads(tableNameBase);
        final boolean deferred = rnd.nextBoolean();
        createMatView(viewSql, matViewName, deferred);

        ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameBase, rnd);

        // Release table writers to reduce memory pressure
        engine.releaseInactive();
        return transactions;
    }

    private ObjList<FuzzTransaction> createTransactionsAndPeriodMv(
            Rnd rnd,
            String tableNameBase,
            String matViewName,
            String viewSql,
            long start,
            long end,
            int interval,
            char intervalUnit,
            @Nullable String timeZone
    ) throws SqlException {
        // create empty initial table as we want the timestamps to be in the [start, end] interval
        fuzzer.createInitialTableWal(tableNameBase, 0);
        final boolean deferred = rnd.nextBoolean();
        createPeriodMatView(viewSql, matViewName, interval, intervalUnit, timeZone, deferred);

        ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameBase, rnd, start, end);

        // Release table writers to reduce memory pressure
        engine.releaseInactive();
        return transactions;
    }

    private ObjList<FuzzTransaction> createTransactionsAndTimerMv(
            Rnd rnd,
            String tableNameBase,
            String matViewName,
            String viewSql,
            long start,
            int interval,
            char intervalUnit
    ) throws SqlException {
        fuzzer.createInitialTableWal(tableNameBase, timestampTypes[rnd.nextInt(10) % 2]);
        final boolean deferred = rnd.nextBoolean();
        createTimerMatView(viewSql, matViewName, start, interval, intervalUnit, deferred);

        ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameBase, rnd, start);

        // Release table writers to reduce memory pressure
        engine.releaseInactive();
        return transactions;
    }

    private void runMvFuzz(Rnd rnd, String testTableName, int tableCount, boolean expectValidMatViews) throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();
        final ObjList<Thread> refreshJobs = new ObjList<>();
        final int refreshJobCount = 1 + rnd.nextInt(4);

        final ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
        final ObjList<String> viewSqls = new ObjList<>();
        final ObjList<LongList> removedRanges = new ObjList<>();

        try {
            for (int i = 0; i < refreshJobCount; i++) {
                refreshJobs.add(startRefreshJob(i, stop, rnd));
            }

            for (int i = 0; i < tableCount; i++) {
                String tableNameBase = testTableName + "_" + i;
                String tableNameMv = tableNameBase + "_mv";
                String viewSql = generateViewSql(rnd, tableNameBase);
                ObjList<FuzzTransaction> transactions = createTransactionsAndMv(rnd, tableNameBase, tableNameMv, viewSql);
                fuzzTransactions.add(transactions);
                removedRanges.add(collectRemovedRanges(transactions, new LongList()));
                viewSqls.add(viewSql);
            }

            // Can help to reduce memory consumption.
            engine.releaseInactive();
            fuzzer.applyManyWalParallel(fuzzTransactions, rnd, testTableName, true, true);
        } finally {
            stopAndJoinJobs(stop, refreshJobs);
        }

        drainWalQueue();
        fuzzer.checkNoSuspendedTables();

        drainWalAndMatViewQueues();
        fuzzer.checkNoSuspendedTables();

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (int i = 0; i < tableCount; i++) {
                final String viewSql = viewSqls.getQuick(i);
                final String mvName = testTableName + "_" + i + "_mv";
                LOG.info().$("asserting view ").$(mvName).$(" against ").$(viewSql).$();
                // Check that the view exists.
                assertQuery("select count() " +
                        "from materialized_views " +
                        "where view_name = '" + mvName + "';")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                count
                                1
                                """);
                if (expectValidMatViews) {
                    assertQuery("select count() " +
                            "from materialized_views " +
                            "where view_name = '" + mvName + "' and view_status <> 'invalid';")
                            .noLeakCheck()
                            .expectSize()
                            .noRandomAccess()
                            .returns("""
                                    count
                                    1
                                    """);
                    assertViewMatchesBase(compiler, viewSql, mvName, removedRanges.getQuick(i));
                }
            }
        }
    }

    private void runPeriodMvFuzz(Rnd rnd, String testTableName, int tableCount) throws Exception {
        final ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
        final ObjList<String> viewSqls = new ObjList<>();
        final ObjList<LongList> removedRanges = new ObjList<>();

        final int length = 1 + rnd.nextInt(10);
        final char[] units = new char[]{'s', 'm', 'h'};
        final char lengthUnit = units[rnd.nextInt(units.length)];
        final long periodLengthMicros = switch (lengthUnit) {
            case 's' -> length * Micros.SECOND_MICROS;
            case 'm' -> length * Micros.MINUTE_MICROS;
            case 'h' -> length * Micros.HOUR_MICROS;
            default -> throw new IllegalStateException("unexpected unit: " + lengthUnit);
        };

        final long dataLength = (1 + rnd.nextInt(10)) * periodLengthMicros;
        // Half of the runs put the periods in a time zone, and the base rows around one of its daylight saving time
        // changes.
        final String timeZone;
        final long start;
        if (rnd.nextBoolean()) {
            final int change = rnd.nextInt(periodDstChanges.size());
            timeZone = periodTimeZones.getQuick(change / 2);
            start = MicrosTimestampDriver.floor(periodDstChanges.getQuick(change)) - rnd.nextLong(dataLength);
        } else {
            timeZone = null;
            start = MicrosTimestampDriver.floor("2022-01-02T03");
        }
        LOG.info().$("period view fuzz [length=").$(length).$(lengthUnit)
                .$(", timeZone=").$(timeZone)
                .$(", start=").$ts(MicrosTimestampDriver.INSTANCE, start)
                .I$();
        final long end = start + dataLength;
        final long clockJumpLimit = Math.min(end, start + SPIN_LOCK_TIMEOUT);
        currentMicros = start;

        final AtomicBoolean stop = new AtomicBoolean();
        // Timer refresh job must be created after currentMicros is set.
        // We need it here since period mat views with immediate refresh create implicit timers.
        final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
        final ObjList<Thread> refreshJobs = new ObjList<>();
        final int refreshJobCount = 1 + rnd.nextInt(4);

        try {
            for (int i = 0; i < refreshJobCount; i++) {
                refreshJobs.add(startTimerJob(i, stop, rnd, timerJob, periodLengthMicros, clockJumpLimit));
            }

            final int sampleByInterval = 1 + rnd.nextInt(length);
            for (int i = 0; i < tableCount; i++) {
                String tableNameBase = testTableName + "_" + i;
                String tableNameMv = tableNameBase + "_mv";
                String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableNameBase + " sample by " + sampleByInterval + lengthUnit;
                ObjList<FuzzTransaction> transactions = createTransactionsAndPeriodMv(rnd, tableNameBase, tableNameMv, viewSql, start, end, length, lengthUnit, timeZone);
                fuzzTransactions.add(transactions);
                removedRanges.add(collectRemovedRanges(transactions, new LongList()));
                viewSqls.add(viewSql);
            }

            // Can help to reduce memory consumption.
            engine.releaseInactive();
            fuzzer.applyManyWalParallel(fuzzTransactions, rnd, testTableName, true, true);
        } finally {
            stopAndJoinJobs(stop, refreshJobs);
        }

        drainWalQueue();
        fuzzer.checkNoSuspendedTables();

        // Where the clock moves back, a period in a time zone lasts up to an hour longer.
        currentMicros = Math.max(end, currentMicros) + periodLengthMicros + (timeZone != null ? Micros.HOUR_MICROS : 0);
        drainMatViewTimerQueue(timerJob);
        drainWalAndMatViewQueues();
        fuzzer.checkNoSuspendedTables();

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (int i = 0; i < tableCount; i++) {
                final String viewSql = viewSqls.getQuick(i);
                final String mvName = testTableName + "_" + i + "_mv";
                LOG.info().$("asserting view ").$(mvName).$(" against ").$(viewSql).$();
                assertQuery("select count() " +
                        "from materialized_views " +
                        "where view_name = '" + mvName + "' and view_status <> 'invalid';")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                count
                                1
                                """);
                assertViewMatchesBase(compiler, viewSql, mvName, removedRanges.getQuick(i));
            }
        }
    }

    private void runTimerMvFuzz(Rnd rnd, String testTableName, int tableCount) throws Exception {
        final ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
        final ObjList<String> viewSqls = new ObjList<>();
        final ObjList<LongList> removedRanges = new ObjList<>();

        final int interval = 1 + rnd.nextInt(10);
        final char[] units = new char[]{'m', 'h'};
        final char intervalUnit = units[rnd.nextInt(units.length)];
        final long clockJump = switch (intervalUnit) {
            case 'm' -> interval * Micros.MINUTE_MICROS;
            case 'h' -> interval * Micros.HOUR_MICROS;
            default -> throw new IllegalStateException("unexpected unit: " + intervalUnit);
        };

        final long start = MicrosTimestampDriver.floor("2022-02-24T17");
        currentMicros = start;
        final long clockJumpLimit = start + SPIN_LOCK_TIMEOUT;

        final AtomicBoolean stop = new AtomicBoolean();
        // Timer refresh job must be created after currentMicros is set.
        final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
        final ObjList<Thread> refreshJobs = new ObjList<>();
        final int refreshJobCount = 1 + rnd.nextInt(4);

        try {
            for (int i = 0; i < refreshJobCount; i++) {
                refreshJobs.add(startTimerJob(i, stop, rnd, timerJob, clockJump, clockJumpLimit));
            }

            for (int i = 0; i < tableCount; i++) {
                String tableNameBase = testTableName + "_" + i;
                String tableNameMv = tableNameBase + "_mv";
                String viewSql = "select min(c3) as min, max(c3) as max, ts from  " + tableNameBase + " sample by 1h";
                ObjList<FuzzTransaction> transactions = createTransactionsAndTimerMv(rnd, tableNameBase, tableNameMv, viewSql, start, interval, intervalUnit);
                fuzzTransactions.add(transactions);
                removedRanges.add(collectRemovedRanges(transactions, new LongList()));
                viewSqls.add(viewSql);
            }

            // Can help to reduce memory consumption.
            engine.releaseInactive();
            fuzzer.applyManyWalParallel(fuzzTransactions, rnd, testTableName, true, true);
        } finally {
            stopAndJoinJobs(stop, refreshJobs);
        }

        drainWalQueue();
        fuzzer.checkNoSuspendedTables();

        currentMicros += clockJump;
        drainMatViewTimerQueue(timerJob);
        drainWalAndMatViewQueues();
        fuzzer.checkNoSuspendedTables();

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (int i = 0; i < tableCount; i++) {
                final String viewSql = viewSqls.getQuick(i);
                final String mvName = testTableName + "_" + i + "_mv";
                LOG.info().$("asserting view ").$(mvName).$(" against ").$(viewSql).$();
                assertQuery("select count() " +
                        "from materialized_views " +
                        "where view_name = '" + mvName + "' and view_status <> 'invalid';")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                count
                                1
                                """);
                assertViewMatchesBase(compiler, viewSql, mvName, removedRanges.getQuick(i));
            }
        }
    }

    // A later batch of testMvFuzz(): rows over up to 2 days starting within a day of hotTs.
    private ObjList<FuzzTransaction> generateLaterBatch(Rnd rnd, String baseTableName, long hotTs) {
        final long lo = hotTs - Micros.DAY_MICROS + rnd.nextLong(2 * Micros.DAY_MICROS);
        final long hi = lo + Micros.HOUR_MICROS + rnd.nextLong(2 * Micros.DAY_MICROS);
        return fuzzer.generateTransactions(baseTableName, rnd, lo, hi);
    }

    private void setFuzzParams(Rnd rnd, double colAddProb, double truncateProb) {
        setFuzzParams(rnd, 10_000, 100_000, colAddProb, truncateProb);
    }

    private void setFuzzParams(Rnd rnd, int transactionCount, int initialRowCount, double colAddProb, double truncateProb) {
        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(transactionCount),
                rnd.nextInt(1000),
                rnd.nextInt(3),
                rnd.nextInt(5),
                rnd.nextInt(1000),
                rnd.nextInt(initialRowCount),
                5 + rnd.nextInt(10)
        );

        // Easy, no column manipulations. Replace commits remove every partition they empty, and TRUNCATE
        // removes them all, while the views keep their rows there, so the oracle, assertViewMatchesBase(),
        // accepts view buckets in the ranges they removed. The min..max recompute after missing WAL files
        // runs at random, and so crosses the gaps they leave.
        fuzzer.setFuzzProbabilities(
                0.0,
                0.0,
                0.0,
                0.0,
                colAddProb,
                0.0,
                0.0,
                0.0,
                1,
                0.0,
                0.0,
                truncateProb,
                0.0,
                0.0,
                REPLACE_INSERT_PROB,
                0.0
        );
        setProperty(PropertyKey.DEBUG_MAT_VIEW_REFRESH_MISSING_WAL_FILES_FATAL, String.valueOf(rnd.nextBoolean()));
    }

    private Thread startRefreshJob(int workerId, AtomicBoolean stop, Rnd outsideRnd) {
        final Rnd rnd = new Rnd(outsideRnd.nextLong(), outsideRnd.nextLong());
        final Thread th = new Thread(
                () -> {
                    try {
                        try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(workerId, engine, 0)) {
                            while (!stop.get()) {
                                refreshJob.run();
                                Os.sleep(rnd.nextInt(50));
                            }

                            // Run one final time before stopping
                            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                                do {
                                    drainWalQueue(walApplyJob, engine);
                                } while (refreshJob.run());
                            }
                        }
                    } catch (Throwable throwable) {
                        LOG.error().$("Refresh job failed: ").$(throwable).$();
                    } finally {
                        Path.clearThreadLocals();
                        Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                        LOG.info().$("Refresh job stopped").$();
                    }
                }, "refresh-job" + workerId
        );
        th.start();
        return th;
    }

    private Thread startTimerJob(
            int workerId,
            AtomicBoolean stop,
            Rnd outsideRnd,
            MatViewTimerJob timerJob,
            long clockJump,
            long clockJumpLimit
    ) {
        final Rnd rnd = new Rnd(outsideRnd.nextLong(), outsideRnd.nextLong());
        final Thread th = new Thread(
                () -> {
                    try {
                        try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(workerId, engine, 0)) {
                            while (!stop.get()) {
                                drainMatViewTimerQueue(timerJob);
                                refreshJob.run();
                                Os.sleep(rnd.nextInt(10));
                                if (rnd.nextBoolean()) {
                                    // Try to move the clock one jump forward.
                                    long timeBefore = currentMicros;
                                    synchronized (timerJob) {
                                        long timeNow = currentMicros;
                                        if (timeBefore == timeNow && timeNow < clockJumpLimit) {
                                            currentMicros += clockJump;
                                        }
                                    }
                                }
                            }

                            // Run one final time before stopping
                            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                                do {
                                    drainWalQueue(walApplyJob, engine);
                                } while (refreshJob.run());
                            }
                        }
                    } catch (Throwable throwable) {
                        LOG.error().$("Refresh job failed: ").$(throwable).$();
                    } finally {
                        Path.clearThreadLocals();
                        Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                        LOG.info().$("Refresh job stopped").$();
                    }
                }, "refresh-interval-job" + workerId
        );
        th.start();
        return th;
    }

    private void testMvFuzz(Rnd rnd, String baseTableName, String... mvNamesAndSqls) throws Exception {
        long start = MicrosTimestampDriver.floor("2022-02-24T17");
        testMvFuzz(rnd, baseTableName, start, mvNamesAndSqls);
    }

    private void testMvFuzz(Rnd rnd, String baseTableName, long start, String... mvNamesAndSqls) throws Exception {
        testMvFuzz(rnd, baseTableName, start, start, mvNamesAndSqls);
    }

    // The first batch of transactions writes rows over 5 to 14 days from start. Later batches write rows
    // over up to 2 days starting within a day of hotTs, so their incremental refreshes start at various
    // points around hotTs and revisit buckets that an earlier refresh has already written.
    private void testMvFuzz(Rnd rnd, String baseTableName, long start, long hotTs, String... mvNamesAndSqls) throws Exception {
        assertMemoryLeak(() -> {
            fuzzer.createInitialTableWal(baseTableName, timestampTypes[rnd.nextInt(10) % 2]);

            for (int i = 0, n = mvNamesAndSqls.length; i < n; i += 2) {
                final String mvName = mvNamesAndSqls[i];
                final String mvSql = mvNamesAndSqls[i + 1];
                final boolean deferred = rnd.nextBoolean();
                createMatView(mvSql, mvName, deferred);
            }

            AtomicBoolean stop = new AtomicBoolean();
            ObjList<Thread> refreshJobs = new ObjList<>();
            final LongList removedRanges = new LongList();

            try {
                refreshJobs.add(startRefreshJob(0, stop, rnd));

                setFuzzParams(rnd, 0, 0);

                final int batchCount = 2 + rnd.nextInt(4);
                for (int b = 0; b < batchCount; b++) {
                    final ObjList<FuzzTransaction> transactions;
                    if (b == 0) {
                        transactions = fuzzer.generateTransactions(baseTableName, rnd, start);
                    } else {
                        // Let the refresh job catch up, so that the next batch gets refreshes of its own.
                        drainWalAndMatViewQueues();
                        transactions = generateLaterBatch(rnd, baseTableName, hotTs);
                    }
                    collectRemovedRanges(transactions, removedRanges);
                    ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
                    fuzzTransactions.add(transactions);
                    fuzzer.applyManyWalParallel(
                            fuzzTransactions,
                            rnd,
                            baseTableName,
                            false,
                            true
                    );
                }
            } finally {
                stopAndJoinJobs(stop, refreshJobs);
            }

            drainWalQueue();

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (int i = 0, n = mvNamesAndSqls.length; i < n; i += 2) {
                    final String mvName = mvNamesAndSqls[i];
                    final String mvSql = mvNamesAndSqls[i + 1];
                    LOG.info().$("asserting view ").$(mvName).$(" against ").$(mvSql).$();
                    // A failed incremental refresh invalidates the view, e.g. on a row outside the replace range.
                    assertQuery("select view_name, invalidation_reason from materialized_views where view_name = '" + mvName + "' and view_status = 'invalid';")
                            .noLeakCheck()
                            .noRandomAccess()
                            .returns("view_name\tinvalidation_reason\n");
                    assertViewMatchesBase(compiler, mvSql, mvName, removedRanges);
                }
            }
        });
    }

    // A case of testMultipleQueryExecutionsPerRefreshDstGapDayBuckets(), see generateDstGapDayBucketCase().
    private static final class DstGapDayBucketCase {
        final StringSink sql = new StringSink();
        // the point the later batches write around, in micros
        long hotTs;
        boolean isOffsetInGap;
        int offsetMinutes;
        // the first batch's start, in micros
        long start;
        int stride;
        // the zone's spring-forward transition, in micros
        long transitionTs;
        char unit;
        String zone;
    }

    // An ALTER ... AS SQL of testAggregatingViewAlterAs(), see generateAggregatingSql().
    private static final class AggregatingSql {
        // the aggregating SELECT reads the inner SQL as a CTE
        static final int CTE = 1;
        // the aggregating SELECT buckets with timestamp_floor()
        static final int FLOOR = 1;
        // the aggregating SELECT buckets with timestamp_floor() and its origin argument
        static final int FLOOR_FROM = 2;
        // the aggregating SELECT reads the inner SQL as the master of a join
        static final int JOIN = 2;
        // the wrapper SELECT names the columns
        static final int NAMED = 2;
        // no wrapper SELECT
        static final int NO_WRAPPER = 0;
        // the aggregating SELECT buckets with SAMPLE BY
        static final int SAMPLE_BY = 0;
        // the aggregating SELECT reads the inner SQL as a subquery
        static final int SUBQUERY = 0;
        // the wrapper SELECT selects all columns
        static final int WILDCARD = 1;
        // the inner SQL, whose kind tells whether ALTER ... AS takes the SQL
        final PassthroughSql inner = new PassthroughSql();
        final StringSink sql = new StringSink();
        int bucketing;
        int source;
        // the offset in the SQL of the outermost SELECT's timestamp column, where a refusal points, or 0 under a
        // wildcard, the SQL's start
        int timestampPosition;
        // the outermost SELECT's timestamp column, null under a wildcard
        String topTimestampColumn;
        // NO_WRAPPER, WILDCARD or NAMED
        int wrapper;
    }

    // An ALTER ... AS SQL of testPassthroughViewAlterAs(), see generatePassthroughSql().
    private static final class PassthroughSql {
        // the innermost SELECT takes the base column ts2 as the designated timestamp, which ALTER ... AS refuses
        static final int OTHER_COLUMN = 2;
        // the designated timestamp passes the base's through, so ALTER ... AS takes the SQL
        static final int PASSED_THROUGH = 0;
        // a level of the SQL shifts the designated timestamp with dateadd(), which ALTER ... AS refuses
        static final int SHIFTED = 1;
        final StringSink sql = new StringSink();
        // the number of SELECTs over the innermost one
        int depth;
        boolean isCteTop;
        boolean isWildcardTop;
        int kind;
        // the offset in the SQL of the outermost SELECT's timestamp column, where a refusal points, or 0 under a
        // wildcard, the SQL's start
        int timestampPosition;
        // the outermost SELECT's timestamp column, null under a wildcard
        String topTimestampColumn;
        // the level that shifts the timestamp or takes ts2, from 0 for the innermost SELECT, or -1
        int transformLevel;
    }
}
