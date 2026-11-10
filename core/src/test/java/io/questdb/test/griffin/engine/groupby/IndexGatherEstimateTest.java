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


package io.questdb.test.griffin.engine.groupby;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.IndexGatherRecordCursorFactory;
import io.questdb.griffin.engine.table.IndexScanChoiceRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntGroupSort;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The selectivity estimate of the Index Or Scan choice
 * ({@link IndexGatherRecordCursorFactory#estimateSelection}): at most 64 distinct partitions,
 * evenly placed over the partitions the walk yields frames for, and at most 4096 per-key counts
 * per open, a NULL key counted unscaled and the other keys sampled and scaled up. Expected values
 * come from that rule in plain Java (the targets are floor(j * G / min(G, 64))), or from a twin
 * table with the same rows and a plain SYMBOL. Every query's rows are compared with the rows of
 * the same query with both switches off.
 */
public class IndexGatherEstimateTest extends AbstractCairoTest {
    private static final int MAX_COUNTS = 4096;
    private static final int SAMPLE_PARTITIONS = 64;

    @Override
    @After
    public void tearDown() throws Exception {
        IndexGatherRecordCursorFactory.setEstimateBudgetForTesting(SAMPLE_PARTITIONS, MAX_COUNTS);
        super.tearDown();
    }

    @Test
    public void testBindVariablesOnOneCachedFactory() throws Exception {
        // one cached factory, the list's values change between opens: each open estimates its own
        // keys, and the 5% rule picks the plan
        assertMemoryLeak(() -> {
            createTable("t", true, 257, 100, "case when r % 5 = 0 then 'f' else 'k' || (r % 100) end");
            setFeatures(true, true);
            final String sql = "select count() n, sum(a) s from t where s in ($1, $2)";
            bindVariableService.clear();
            bindVariableService.setStr(0, "k7");
            bindVariableService.setStr(1, "k7");
            try (RecordCursorFactory factory = select(sql)) {
                final String[][] lists = {{"k7", "k7"}, {"nope1", "nope2"}, {"f", "k7"}, {"k7", "k7"}};
                final int[] keyCounts = {1, 0, 2, 1};
                // f is 20 rows of every 100, k7 one
                final long[] selected = {257, 0, 21 * 257, 257};
                final boolean[] index = {true, true, false, true};
                for (int i = 0; i < lists.length; i++) {
                    bindVariableService.setStr(0, lists[i][0]);
                    bindVariableService.setStr(1, lists[i][1]);
                    final Open open = open(factory, sql);
                    final String what = lists[i][0] + ',' + lists[i][1];
                    Assert.assertEquals(what, keyCounts[i] == 0 ? "[]" : targets(257).toString(), open.partitions);
                    Assert.assertEquals(what, (long) Math.min(257, SAMPLE_PARTITIONS) * keyCounts[i], open.probes);
                    Assert.assertEquals(what, selected[i], open.selected);
                    Assert.assertEquals(what, 25_700, open.scanned);
                    Assert.assertEquals(what, index[i], open.index);
                }
            }
        });
    }

    @Test
    public void testClusteredKeysAndPlanWins() throws Exception {
        // plan-choice misestimates of clustered keys, which the sample accepts (speed only), and
        // the PR's wins: frequent keys scan, rare keys take the index
        assertMemoryLeak(() -> {
            final IntList targets = targets(257);
            final StringSink inTargets = new StringSink();
            for (int j = 0; j < targets.size(); j++) {
                inTargets.put(j == 0 ? "" : ", ").put(targets.getQuick(j));
            }
            final String symbol = "case when h = 1 then 'c1' " +
                    "when r >= 90 and h in (" + inTargets + ") then 'tg' " +
                    "when r % 5 = 0 then 'f' when r % 5 = 1 then 'g' else 'k' || (r % 100) end";
            createTable("t", true, 257, 100, symbol);
            createTable("tw", false, 257, 100, symbol);
            // only in partition 1, never a target: the estimate sees none of it
            Open open = open("select count() n, sum(a) s from t where s = 'c1'");
            Assert.assertEquals(0, open.selected);
            Assert.assertTrue(open.index);
            Assert.assertEquals(100, twinCount("s = 'c1'"));
            // 10 rows in each target partition only: 640 rows (2.5%), estimated at 10%
            open = open("select count() n, sum(a) s from t where s = 'tg'");
            Assert.assertEquals(640, twinCount("s = 'tg'"));
            Assert.assertEquals((long) ((double) (64 * 10) * 25_700 / (64 * 100)), open.selected);
            Assert.assertFalse(open.index);
            // the PR's wins: 20% keys scan, 1% keys take the index
            open = open("select count() n, sum(a) s from t where s = 'f'");
            Assert.assertFalse(open.index);
            open = open("select count() n, sum(a) s from t where s in ('f', 'g')");
            Assert.assertFalse(open.index);
            open = open("select count() n, sum(a) s from t where s = 'k7'");
            Assert.assertTrue(open.index);
            Assert.assertEquals(257, open.selected);
        });
    }

    @Test
    public void testColumnAddedAfterData() throws Exception {
        // the key column is added after 65 of 130 partitions: sampled older partitions have no
        // index and every row NULL; 32 targets fall on each side, so the estimate is exact
        assertMemoryLeak(() -> {
            for (String name : new String[]{"t", "tw"}) {
                execute("create table " + name + " (ts timestamp, a long) timestamp(ts) partition by hour bypass wal");
                execute("insert into " + name + " select ((x - 1) * 36_000_000L)::timestamp, x from long_sequence(6500)");
                execute("alter table " + name + " add column s symbol" + (name.equals("t") ? " index type posting" : ""));
                execute("insert into " + name + " (ts, a, s) select ((x + 6499) * 36_000_000L)::timestamp, x, " +
                        "case when (x - 1) % 10 = 0 then null else 'k' || ((x - 1) % 7) end from long_sequence(6500)");
            }
            for (String where : new String[]{"s = null", "s in ('k1', null)", "s = 'k1'"}) {
                final Open open = open("select count() n, sum(a) s from t where " + where);
                Assert.assertEquals(where, targets(130).toString(), open.partitions);
                Assert.assertEquals(where, twinCount(where), open.selected);
                Assert.assertEquals(where, 13_000, open.scanned);
            }
        });
    }

    @Test
    public void testColumnAddedToActivePartition() throws Exception {
        // the key column is added while hour 0 holds 50 rows: their key is NULL below the column
        // top; 50 more rows of hour 0 and 100 of hour 1 carry values
        assertMemoryLeak(() -> {
            for (String name : new String[]{"t", "tw"}) {
                execute("create table " + name + " (ts timestamp, a long) timestamp(ts) partition by hour bypass wal");
                execute("insert into " + name + " select ((x - 1) * 36_000_000L)::timestamp, x from long_sequence(50)");
                execute("alter table " + name + " add column s symbol" + (name.equals("t") ? " index type posting" : ""));
            }
            // only NULL so far: hour 0 is all column top
            Open open = open("select count() n, sum(a) s from t where s = null");
            Assert.assertEquals("[0]", open.partitions);
            Assert.assertEquals(50, open.selected);
            Assert.assertEquals(50, open.scanned);
            for (String name : new String[]{"t", "tw"}) {
                execute("insert into " + name + " (ts, a, s) select ((x + 49) * 36_000_000L)::timestamp, x, "
                        + "case when x % 2 = 0 then 'k1' else 'k2' end from long_sequence(150)");
            }
            for (String where : new String[]{"s = null", "s in (null, 'k1')", "s = 'k1'"}) {
                open = open("select count() n, sum(a) s from t where " + where);
                Assert.assertEquals(where, "[0,1]", open.partitions);
                Assert.assertEquals(where, twinCount(where), open.selected);
                Assert.assertEquals(where, 200, open.scanned);
            }
        });
    }

    @Test
    public void testLegacyEfBlocksCountEveryRow() throws Exception {
        // an EF block written without its rank trailer cannot count a clipped range: the frame
        // counts every row as selected, which leans towards the scan
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, s symbol index type posting ef, a long) timestamp(ts) partition by hour bypass wal");
            final String insert = "insert into t select ((x - 1) * 36_000_000L)::timestamp, 'k' || ((x - 1) % 2), x from long_sequence(400)";
            final String sql = "select count() n, sum(a) s from t where s = 'k1' and ts in '1970-01-01T00:10;20m;1h;4'";
            // rows 17-49 of each hour fall in the window (it ends before 00:30), 17 of them k1
            io.questdb.cairo.idx.PostingIndexUtils.isEfRankTrailerEnabled = false;
            try {
                execute(insert);
            } finally {
                io.questdb.cairo.idx.PostingIndexUtils.isEfRankTrailerEnabled = true;
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            Open open = open(sql);
            Assert.assertEquals("[0,1,2,3]", open.partitions);
            Assert.assertEquals(4 * 33, open.scanned);
            Assert.assertEquals(4 * 33, open.selected);
            Assert.assertFalse(open.index);
            TestUtils.assertContains(open.rows, "68\t");
            // the same rows with the trailer: exact
            execute("drop table t");
            execute("create table t (ts timestamp, s symbol index type posting ef, a long) timestamp(ts) partition by hour bypass wal");
            execute(insert);
            open = open(sql);
            Assert.assertEquals(4 * 17, open.selected);
        });
    }

    @Test
    public void testConcurrentAppendsDuringEstimates() throws Exception {
        // two threads, each with its own factory, open the choice while a third appends committed
        // rows; a barrier between iterations makes every estimate overlap a commit
        assertMemoryLeak(() -> {
            createTable("t", true, 100, 100, "'k' || (r % 100)", "1");
            setFeatures(true, true);
            final int iterations = 40;
            final AtomicLong committed = new AtomicLong(100);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final CyclicBarrier barrier = new CyclicBarrier(3);
            final String sql = "select count() n, sum(a) s from t where s = 'k7'";
            final Thread[] threads = new Thread[3];
            for (int t = 0; t < 2; t++) {
                threads[t] = new Thread(() -> {
                    try (
                            SqlExecutionContextImpl ctx = TestUtils.createSqlExecutionCtx(engine, 1);
                            SqlCompiler compiler = engine.getSqlCompiler();
                            RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()
                    ) {
                        for (int i = 0; i < iterations; i++) {
                            barrier.await();
                            final long before = committed.get();
                            final long n;
                            final long s;
                            try (RecordCursor cursor = factory.getCursor(ctx)) {
                                Assert.assertTrue(cursor.hasNext());
                                n = cursor.getRecord().getLong(0);
                                s = cursor.getRecord().getLong(1);
                            }
                            final long after = committed.get();
                            Assert.assertEquals(n, s);
                            Assert.assertTrue(before + " <= " + n + " <= " + after, before <= n && n <= after);
                        }
                    } catch (Throwable th) {
                        error.compareAndSet(null, th);
                        barrier.reset();
                    }
                });
            }
            threads[2] = new Thread(() -> {
                try (SqlExecutionContextImpl ctx = TestUtils.createSqlExecutionCtx(engine, 1)) {
                    for (int i = 0; i < iterations; i++) {
                        barrier.await();
                        // a new hour partition: 10 of its 100 rows are k7
                        engine.execute("insert into t select ((" + (100 + i) + " * 100 + x - 1) * 36_000_000L)::timestamp, "
                                + "case when x % 10 = 0 then 'k7' else 'k' || (x % 100) end, 1 from long_sequence(100)", ctx);
                        committed.addAndGet(10 + 1);
                    }
                } catch (Throwable th) {
                    error.compareAndSet(null, th);
                    barrier.reset();
                }
            });
            for (Thread t : threads) {
                t.start();
            }
            for (Thread t : threads) {
                t.join();
            }
            if (error.get() != null) {
                throw new AssertionError(error.get());
            }
            Assert.assertEquals(100 + iterations * 11L, twinCountOf("select count() from t where s = 'k7'"));
        });
    }

    @Test
    public void testDroppedAndAppendedPartitionsBetweenOpens() throws Exception {
        // a cached factory: the targets follow the partitions of each open
        assertMemoryLeak(() -> {
            createTable("t", true, 257, 100, "'k' || (r % 100)");
            setFeatures(true, true);
            final String sql = "select count() n, sum(a) s from t where s = 'k7'";
            try (RecordCursorFactory factory = select(sql)) {
                Open open = open(factory, sql);
                Assert.assertEquals(targets(257).toString(), open.partitions);
                // partition 4 is a target
                execute("alter table t drop partition list '1970-01-01T04'");
                open = open(factory, sql);
                Assert.assertEquals(targets(256).toString(), open.partitions);
                Assert.assertEquals(256, open.selected);
                Assert.assertEquals(25_600, open.scanned);
                // 44 more partitions
                execute("insert into t select ((25_700 + x - 1) * 36_000_000L)::timestamp, 'k' || ((x - 1) % 100), x from long_sequence(4400)");
                open = open(factory, sql);
                Assert.assertEquals(targets(300).toString(), open.partitions);
                Assert.assertEquals(300, open.selected);
                Assert.assertEquals(30_000, open.scanned);
                // TRUNCATE: no partition, no probe, the index plan over nothing
                execute("truncate table t");
                open = open(factory, sql);
                Assert.assertEquals("[]", open.partitions);
                Assert.assertEquals(0, open.probes);
                Assert.assertEquals(0, open.selected);
                Assert.assertEquals(0, open.scanned);
                Assert.assertTrue(open.index);
                Assert.assertEquals("n\ts\n0\tnull\n", open.rows);
            }
        });
    }

    @Test
    public void testDroppedOrAlteredTable() throws Exception {
        // a cached factory over a table altered or dropped since it compiled fails its open
        assertMemoryLeak(() -> {
            createTable("t", true, 257, 100, "'k' || (r % 100)");
            setFeatures(true, true);
            final String sql = "select count() n, sum(a) s from t where s = 'k7'";
            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertEquals(targets(257).toString(), open(factory, sql).partitions);
                execute("alter table t add column z int");
                try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("altered");
                } catch (TableReferenceOutOfDateException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cached query plan cannot be used because table schema has changed");
                }
            }
            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertEquals(targets(257).toString(), open(factory, sql).partitions);
                execute("drop table t");
                try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("dropped");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist [table=t]");
                }
            }
        });
    }

    @Test
    public void testDuplicateAndAbsentKeys() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", true, 257, 100, "'k' || (r % 100)");
            // K counts distinct keys
            Open open = open("select count() n, sum(a) s from t where s in ('k7', 'k7', 'k7')");
            Assert.assertEquals(targets(257).toString(), open.partitions);
            Assert.assertEquals(64, open.probes);
            Assert.assertEquals(257, open.selected);
            // K = 0: nothing to count, the index plan over no key
            open = open("select count() n, sum(a) s from t where s in ('nope1', 'nope2')");
            Assert.assertEquals("[]", open.partitions);
            Assert.assertEquals(0, open.probes);
            Assert.assertEquals(0, open.selected);
            Assert.assertEquals(25_700, open.scanned);
            Assert.assertTrue(open.index);
            Assert.assertEquals("n\ts\n0\tnull\n", open.rows);
        });
    }

    @Test
    public void testEstimateOpensOnlySampledPartitions() throws Exception {
        // the estimate opens the 64 target partitions and no other, parquet ones included
        assertMemoryLeak(() -> {
            createTable("t", true, 257, 100, "'k' || (r % 100)");
            final String sql = "select count() n, sum(a) s from t where s = 'k7'";
            for (boolean parquet : new boolean[]{false, true}) {
                if (parquet) {
                    final StringSink list = new StringSink();
                    for (int h = 0; h < 256; h += 2) {
                        list.put(h == 0 ? "'" : ", '").put(hour(h)).put('\'');
                    }
                    execute("alter table t convert partition to parquet list " + list);
                }
                setFeatures(true, true);
                try (RecordCursorFactory factory = select(sql)) {
                    final IndexGatherRecordCursorFactory gather = findGather(factory);
                    Assert.assertNotNull(gather);
                    engine.releaseAllReaders();
                    final long[] estimate = new long[2];
                    gather.estimateSelection(sqlExecutionContext, estimate);
                    Assert.assertEquals("parquet " + parquet, 64, gather.getLastEstimateOpenedPartitions());
                    Assert.assertEquals("parquet " + parquet, targets(257).toString(), gather.getLastEstimatePartitions().toString());
                    Assert.assertEquals(25_700, estimate[1]);
                }
            }
        });
    }

    @Test
    public void testExecutionModes() throws Exception {
        // WAL and non-WAL, BITMAP and plain symbols (no choice), PARTITION BY NONE
        assertMemoryLeak(() -> {
            execute("create table w (ts timestamp, s symbol index type posting, a long) timestamp(ts) partition by hour wal");
            execute("insert into w select ((x - 1) * 36_000_000L)::timestamp, 'k' || ((x - 1) % 100), x from long_sequence(25_700)");
            drainWalQueue();
            Open open = open("select count() n, sum(a) s from w where s = 'k7'");
            Assert.assertEquals(targets(257).toString(), open.partitions);
            Assert.assertEquals(64, open.probes);
            Assert.assertEquals(257, open.selected);
            Assert.assertTrue(open.index);

            for (String index : new String[]{" index type bitmap", ""}) {
                execute("create table b (ts timestamp, s symbol" + index + ", a long) timestamp(ts) partition by hour bypass wal");
                execute("insert into b select ((x - 1) * 36_000_000L)::timestamp, 'k' || ((x - 1) % 100), x from long_sequence(25_700)");
                setFeatures(true, true);
                final StringSink plan = new StringSink();
                printSql("explain select count() n, sum(a) s from b where s = 'k7'", plan);
                Assert.assertFalse(plan.toString(), plan.toString().contains("Index Or Scan"));
                execute("drop table b");
            }

            // one partition: kS = min(K, 4096)
            execute("create table n (ts timestamp, s symbol index type posting, a long) timestamp(ts) partition by none");
            execute("insert into n select (x * 1_000_000L)::timestamp, 'k' || ((x - 1) % 5000), x from long_sequence(5000)");
            final StringSink in = new StringSink();
            for (int i = 0; i < 5000; i++) {
                in.put(i == 0 ? "'k" : ", 'k").put(i).put('\'');
            }
            open = open("select count() n, sum(a) s from n where s in (" + in + ")");
            Assert.assertEquals("[0]", open.partitions);
            Assert.assertEquals(4096, open.probes);
            // 4096 keys of one row each, scaled by 5000 / 4096
            Assert.assertEquals((long) (4096.0 * 5000 / 4096), open.selected);
            Assert.assertEquals(5000, open.scanned);
        });
    }

    @Test
    public void testFilesFailingInSampledPartition() throws Exception {
        // a target partition's posting key file, then a column file of a target partition, fail
        // to open while the estimate reads it: the open fails, nothing leaks, and the next open
        // returns the rows
        // [partition directory prefix, file name prefix] of the next open to fail
        final AtomicReference<String[]> fail = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            private boolean injected;

            @Override
            public int errno() {
                return injected ? 13 : super.errno();
            }

            @Override
            public long openRO(LPSZ name) {
                final String[] target = fail.get();
                if (target != null) {
                    final String path = Misc.getThreadLocalUtf8Sink().put(name).toString();
                    final int slash = path.lastIndexOf(Files.SEPARATOR);
                    final String dir = path.substring(path.lastIndexOf(Files.SEPARATOR, slash - 1) + 1, slash);
                    if (dir.startsWith(target[0]) && path.substring(slash + 1).startsWith(target[1])) {
                        fail.set(null);
                        injected = true;
                        return -1;
                    }
                }
                injected = false;
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createTable("t", true, 257, 100, "'k' || (r % 100)");
            final String sql = "select count() n, sum(a) s from t where s = 'k7'";
            setFeatures(false, false);
            final String expected = runToString(engine, sqlExecutionContext, sql);
            setFeatures(true, true);
            // 1970-01-01T08 is the third target (j = 2: floor(2 * 257 / 64) = 8)
            for (String file : new String[]{"s.pk", "a.d"}) {
                try (RecordCursorFactory factory = select(sql)) {
                    final IndexGatherRecordCursorFactory gather = findGather(factory);
                    engine.releaseAllReaders();
                    fail.set(new String[]{"1970-01-01T08", file});
                    try {
                        gather.estimateSelection(sqlExecutionContext, new long[2]);
                        Assert.fail("estimate did not fail on " + file);
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "1970-01-01T08");
                    }
                    Assert.assertNull(fail.get());
                    TestUtils.assertEquals(file, expected, print(factory, sqlExecutionContext));
                }
            }
        });
    }

    @Test
    public void testFivePercentBoundary() throws Exception {
        // a key with exactly 5% of the rows in every partition takes the index (the rule is <=),
        // one with 6% the scan: the truncation of the extrapolated ratio does not flip it
        assertMemoryLeak(() -> {
            final String symbol = "case when r < 5 then 'hot5' when r < 11 then 'hot6' else 'k' || (r % 50) end";
            createTable("t", true, 257, 100, symbol);
            createTable("tw", false, 257, 100, symbol);
            Open open = open("select count() n, sum(a) s from t where s = 'hot5'");
            Assert.assertEquals(targets(257).toString(), open.partitions);
            Assert.assertEquals(64, open.probes);
            Assert.assertEquals((long) ((double) (64 * 5) * 25_700 / (64 * 100)), open.selected);
            Assert.assertEquals(1285, open.selected);
            Assert.assertEquals(twinCount("s = 'hot5'"), open.selected);
            Assert.assertTrue(open.index);
            open = open("select count() n, sum(a) s from t where s = 'hot6'");
            Assert.assertEquals(1542, open.selected);
            Assert.assertEquals(twinCount("s = 'hot6'"), open.selected);
            Assert.assertFalse(open.index);
        });
    }

    @Test
    public void testFractionalKeySample() throws Exception {
        // 500 of 5000 keys, 2 rows of each per partition: 64 sampled keys count 128 rows per
        // probed partition, scaled by 500 / 64 to 1000, 10% of the rows: the scan. Unscaled, the
        // 1.28% would take the index
        assertMemoryLeak(() -> {
            createTable("t", true, 257, 10_000, "'k' || (r % 5000)");
            final StringSink in = new StringSink();
            for (int i = 0; i < 500; i++) {
                in.put(i == 0 ? "'k" : ", 'k").put(i).put('\'');
            }
            final Open open = open("select count() n, sum(a) s from t where s in (" + in + ")");
            Assert.assertEquals(targets(257).toString(), open.partitions);
            Assert.assertEquals(4096, open.probes);
            Assert.assertEquals(257L * (long) (128.0 * 500 / 64), open.selected);
            Assert.assertEquals(257_000, open.selected);
            Assert.assertEquals(2_570_000, open.scanned);
            Assert.assertFalse(open.index);
            TestUtils.assertContains(open.rows, "257000\t");
        });
    }

    @Test
    public void testIntervalWithEmptyIntersections() throws Exception {
        // 400 hours, rows in minutes 0-9 of every hour and 40-49 of even hours; one 20-minute
        // window per hour, at 30-50: only the 200 even hours yield frames, and the targets are
        // placed over those
        assertMemoryLeak(() -> {
            for (String name : new String[]{"t", "tw"}) {
                execute("create table " + name + " (ts timestamp, s symbol" + (name.equals("t") ? " index type posting" : "") + ", a long)"
                        + " timestamp(ts) partition by hour bypass wal");
                execute("insert into " + name + " select ts, s, a from (select (((x - 1) / 20) * 3_600_000_000L "
                        + "+ (case when (x - 1) % 20 < 10 then (x - 1) % 20 else 30 + (x - 1) % 20 end) * 60_000_000L)::timestamp ts, "
                        + "'k' || ((x - 1) % 10) s, x a, (x - 1) % 20 < 10 or ((x - 1) / 20) % 2 = 0 keep from long_sequence(8000)) "
                        + "where keep");
            }
            final String interval = "ts in '1970-01-01T00:30;20m;1h;400'";
            Assert.assertEquals(2000, twinCount(interval));
            final Open open = open("select count() n, sum(a) s from t where s = 'k7' and " + interval);
            final IntList expected = new IntList();
            final IntList groups = targets(200);
            for (int j = 0; j < groups.size(); j++) {
                expected.add(2 * groups.getQuick(j));
            }
            Assert.assertEquals(expected.toString(), open.partitions);
            Assert.assertEquals(64, open.probes);
            Assert.assertEquals(2000, open.scanned);
            Assert.assertEquals(twinCount("s = 'k7' and " + interval), open.selected);
        });
    }

    @Test
    public void testKeyCountBoundaries() throws Exception {
        // with 64 sampled partitions, 64 keys are all counted and 65 sampled to 64; with one
        // partition, 4096 keys are all counted and 4097 sampled to 4096
        assertMemoryLeak(() -> {
            createTable("t", true, 65, 100, "'k' || (r % 100)");
            createTable("tw", false, 65, 100, "'k' || (r % 100)");
            for (int keys : new int[]{64, 65}) {
                final String where = "s in (" + keyList("k", 0, keys) + ")";
                final Open open = open("select count() n, sum(a) s from t where " + where);
                Assert.assertEquals(targets(65).toString(), open.partitions);
                Assert.assertEquals(64 * 64, open.probes);
                Assert.assertEquals(65L * keys, twinCount(where));
                Assert.assertEquals(65L * keys, open.selected);
            }
            execute("drop table t");
            execute("drop table tw");
            createTable("t", true, 1, 5000, "'k' || (r % 5000)");
            createTable("tw", false, 1, 5000, "'k' || (r % 5000)");
            for (int keys : new int[]{4096, 4097}) {
                final String where = "s in (" + keyList("k", 0, keys) + ")";
                final Open open = open("select count() n, sum(a) s from t where " + where);
                Assert.assertEquals("[0]", open.partitions);
                Assert.assertEquals(4096, open.probes);
                Assert.assertEquals(keys, twinCount(where));
                Assert.assertEquals(keys, open.selected);
            }
        });
    }

    @Test
    public void testKeyCountsCappedAtFrameRows() throws Exception {
        // legacy EF blocks (no rank trailer) cannot count a clipped range, so each of the two
        // keys counts all 33 rows of its frame: the frame's 66 is capped at its 33 rows, and the
        // four frames select 132 of 132 rather than 264
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, s symbol index type posting ef, a long) timestamp(ts) partition by hour bypass wal");
            io.questdb.cairo.idx.PostingIndexUtils.isEfRankTrailerEnabled = false;
            try {
                execute("insert into t select ((x - 1) * 36_000_000L)::timestamp, 'k' || ((x - 1) % 2), x from long_sequence(400)");
            } finally {
                io.questdb.cairo.idx.PostingIndexUtils.isEfRankTrailerEnabled = true;
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            final Open open = open("select count() n, sum(a) s from t where s in ('k0', 'k1') and ts in '1970-01-01T00:10;20m;1h;4'");
            Assert.assertEquals("[0,1,2,3]", open.partitions);
            Assert.assertEquals(4 * 33, open.scanned);
            Assert.assertEquals(4 * 33, open.selected);
            Assert.assertFalse(open.index);
            TestUtils.assertContains(open.rows, "132\t");
        });
    }

    @Test
    public void testKeyPickDoesNotOverflow() throws Exception {
        // one partition and 50,001 keys sampled to 50,000 (the budget raised through the test
        // hook): the i-th sampled key sits at floor(i * N / nS), where i * N passes 2^31
        assertMemoryLeak(() -> {
            createTable("t", true, 1, 50_001, "'k' || r");
            IndexGatherRecordCursorFactory.setEstimateBudgetForTesting(SAMPLE_PARTITIONS, 50_000);
            final Open open = open("select count() n, sum(a) s from t where s in (" + keyList("k", 0, 50_001) + ")");
            Assert.assertEquals("[0]", open.partitions);
            Assert.assertEquals(50_000, open.probes);
            Assert.assertEquals((long) (50_000.0 * 50_001 / 50_000), open.selected);
            Assert.assertEquals(50_001, open.selected);
            Assert.assertEquals(50_001, open.scanned);
        });
    }

    @Test
    public void testNullKeySampling() throws Exception {
        // 100 partitions of 25,000 rows: 250 NULL rows (1%) and 640 keys u0..u639 of 1 row each
        // (2.56%) per partition. 641 keys sample to 64: NULL counted as it is, 63 of the 640
        // others scaled by 640 / 63. Scaling NULL with the others would estimate 12.5% and scan
        assertMemoryLeak(() -> {
            final String symbol = "case when r < 250 then null when r < 890 then 'u' || (r - 250) else 'other' end";
            createTable("t", true, 100, 25_000, symbol);
            createTable("tw", false, 100, 25_000, symbol);
            final String keys = keyList("u", 0, 640);
            Open open = open("select count() n, sum(a) s from t where s in (null, " + keys + ")");
            Assert.assertEquals(targets(100).toString(), open.partitions);
            Assert.assertEquals(64 * 64, open.probes);
            Assert.assertEquals(100L * (250 + (long) (63.0 * 640 / 63)), open.selected);
            Assert.assertEquals(twinCount("s in (null, " + keys + ")"), open.selected);
            Assert.assertEquals(2_500_000, open.scanned);
            Assert.assertTrue(open.index);
            // the same keys without NULL: 640 keys sampled to 64, scaled by 640 / 64
            open = open("select count() n, sum(a) s from t where s in (" + keys + ")");
            Assert.assertEquals(64 * 64, open.probes);
            Assert.assertEquals(100L * (long) (64.0 * 640 / 64), open.selected);
            Assert.assertEquals(twinCount("s in (" + keys + ")"), open.selected);
            Assert.assertTrue(open.index);
            // NULL alone
            open = open("select count() n, sum(a) s from t where s = null");
            Assert.assertEquals(64, open.probes);
            Assert.assertEquals(25_000, open.selected);
        });
    }

    @Test
    public void testNullKeyWithSmallBudget() throws Exception {
        // the test hook's smallest budgets: one count per probed frame still keeps NULL and one
        // other key
        assertMemoryLeak(() -> {
            final String symbol = "case when r < 10 then null else 'k' || (r % 10) end";
            createTable("t", true, 4, 100, symbol);
            IndexGatherRecordCursorFactory.setEstimateBudgetForTesting(1, 1);
            // m' = 1 (partition 0), kS = 2: NULL and k1 of k1, k2, scaled by 2 / 1
            final Open open = open("select count() n, sum(a) s from t where s in (null, 'k1', 'k2')");
            Assert.assertEquals("[0]", open.partitions);
            Assert.assertEquals(2, open.probes);
            // 10 NULL + 9 k1 * 2 per 100 rows, over 400 rows
            Assert.assertEquals((long) ((double) (10 + 9 * 2) * 400 / 100), open.selected);
        });
    }

    @Test
    public void testO3SplitPartitions() throws Exception {
        // an O3 insert splits the last partition: each piece is a group of its own
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, s symbol index type posting, a long) timestamp(ts) partition by hour wal");
            execute("insert into t select ((x - 1) * 36_000_000L)::timestamp, 'k' || ((x - 1) % 100), x from long_sequence(25_700)");
            drainWalQueue();
            // into hour 256 after its first 96 rows: the prefix is large enough to split off
            execute("insert into t select (" + 256 * 3_600_000_000L + "L + 95 * 36_000_000L + x * 1_000_000L)::timestamp, 'k7', x from long_sequence(5)");
            drainWalQueue();
            final long partitions = twinCountOf("select count() from table_partitions('t')");
            Assert.assertEquals(258, partitions);
            final Open open = open("select count() n, sum(a) s from t where s = 'k7'");
            Assert.assertEquals(targets(258).toString(), open.partitions);
            Assert.assertEquals(25_705, open.scanned);
        });
    }

    @Test
    public void testParquetPartitionsCountAllRows() throws Exception {
        // every other partition of 130 is parquet: a sampled parquet partition counts all its rows
        assertMemoryLeak(() -> {
            createTable("t", true, 130, 100, "'k' || (r % 100)");
            final StringSink list = new StringSink();
            for (int h = 0; h < 130; h += 2) {
                list.put(h == 0 ? "'" : ", '").put(hour(h)).put('\'');
            }
            execute("alter table t convert partition to parquet list " + list);
            final IntList targets = targets(130);
            long sampledSelected = 0;
            int nativeTargets = 0;
            for (int j = 0; j < targets.size(); j++) {
                if (targets.getQuick(j) % 2 == 0) {
                    sampledSelected += 100;
                } else {
                    sampledSelected += 1;
                    nativeTargets++;
                }
            }
            Open open = open("select count() n, sum(a) s from t where s = 'k7'");
            Assert.assertEquals(targets.toString(), open.partitions);
            Assert.assertEquals(nativeTargets, open.probes);
            Assert.assertEquals((long) ((double) sampledSelected * 13_000 / (64 * 100)), open.selected);
            Assert.assertFalse(open.index);
        });
    }

    @Test
    public void testPartitionCounts() throws Exception {
        // probed partitions == min(P, 64), evenly placed, one count each; the uniform key's
        // estimate is exact
        assertMemoryLeak(() -> {
            for (int partitions : new int[]{1, 17, 64, 65, 257, 4000}) {
                createTable("t", true, partitions, 100, "'k' || (r % 100)");
                createTable("tw", false, partitions, 100, "'k' || (r % 100)");
                final Open open = open("select count() n, sum(a) s from t where s = 'k7'");
                Assert.assertEquals("P=" + partitions, targets(partitions).toString(), open.partitions);
                Assert.assertEquals("P=" + partitions, Math.min(partitions, SAMPLE_PARTITIONS), open.probes);
                Assert.assertEquals("P=" + partitions, twinCount("s = 'k7'"), open.selected);
                Assert.assertEquals("P=" + partitions, 100L * partitions, open.scanned);
                Assert.assertTrue(open.index);
                execute("drop table t");
                execute("drop table tw");
            }
            // empty table, then a single row
            execute("create table t (ts timestamp, s symbol index type posting, a long) timestamp(ts) partition by hour bypass wal");
            Open open = open("select count() n, sum(a) s from t where s = 'k7'");
            Assert.assertEquals("[]", open.partitions);
            Assert.assertEquals(0, open.scanned);
            Assert.assertTrue(open.index);
            execute("insert into t values (0, 'k7', 3)");
            open = open("select count() n, sum(a) s from t where s = 'k7'");
            Assert.assertEquals("[0]", open.partitions);
            Assert.assertEquals(1, open.probes);
            Assert.assertEquals(1, open.selected);
            Assert.assertEquals(1, open.scanned);
            Assert.assertEquals("n\ts\n1\t3\n", open.rows);
            // a key, but no frame: an interval outside the table
            open = open("select count() n, sum(a) s from t where s = 'k7' and ts in '2000-01-01'");
            Assert.assertEquals("[]", open.partitions);
            Assert.assertEquals(0, open.probes);
            Assert.assertEquals(0, open.selected);
            Assert.assertEquals(0, open.scanned);
            Assert.assertTrue(open.index);
            Assert.assertEquals("n\ts\n0\tnull\n", open.rows);
        });
    }

    @Test
    public void testProbesStayWithinBudget() throws Exception {
        // the counts per open stay within 4096 for long lists: m' * min(K, 4096 / m')
        assertMemoryLeak(() -> {
            final int[][] shapes = {{17, 300}, {257, 100}, {4000, 100}};
            for (int[] shape : shapes) {
                final int partitions = shape[0];
                createTable("t", true, partitions, shape[1], "'k' || (r % 5000)");
                for (int keys : new int[]{1, 50, 300, 5000}) {
                    final Open open = open("select count() n, sum(a) s from t where s in (" + keyList("k", 0, keys) + ")");
                    final int m = Math.min(partitions, SAMPLE_PARTITIONS);
                    final long expected = (long) m * Math.min(keys, MAX_COUNTS / m);
                    Assert.assertEquals("P=" + partitions + " K=" + keys, expected, open.probes);
                    Assert.assertTrue(open.probes <= MAX_COUNTS);
                    Assert.assertEquals("P=" + partitions + " K=" + keys, targets(partitions).toString(), open.partitions);
                }
                execute("drop table t");
            }
        });
    }

    @Test
    public void testResolveKeysSortsSortedListsInLinearTime() throws Exception {
        // an IN list of 20,000 rare keys in symbol-id order: each sort of the keys (one in the
        // estimate, one when the index plan opens) takes K - 1 comparisons
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, s symbol index type posting, a long) timestamp(ts) partition by hour bypass wal");
            // every 21st row is a rare key, r0 first: symbol ids in the list's order
            execute("insert into t select ((x - 1) * 9_000_000L)::timestamp, "
                    + "case when x % 21 = 0 then 'r' || (x / 21 - 1) else 'other' end, x from long_sequence(420_000)");
            final String sql = "select count() n, sum(a) s from t where s in (" + keyList("r", 0, 20_000) + ")";
            // the sampled keys alias with the sampled partitions here (both evenly spread), and
            // the estimate would scan: take the index plan whatever the estimate
            setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_CHOICE_MAX_SELECTIVITY, "1.0");
            setFeatures(true, true);
            try (RecordCursorFactory factory = select(sql)) {
                final IndexScanChoiceRecordCursorFactory choice = findChoice(factory);
                Assert.assertNotNull(choice);
                for (int run = 0; run < 2; run++) {
                    final long indexOpens = choice.getIndexPlanOpens();
                    final long comparisons = IntGroupSort.getComparisonCountForTesting();
                    final String rows = print(factory, sqlExecutionContext);
                    final long sorts = 1 + choice.getIndexPlanOpens() - indexOpens;
                    Assert.assertEquals(2, sorts);
                    Assert.assertEquals(19_999 * sorts, IntGroupSort.getComparisonCountForTesting() - comparisons);
                    TestUtils.assertContains(rows, "20000\t");
                }
            }
        });
    }

    @Test
    public void testSqlContexts() throws Exception {
        // the choice under a sub-query, a CTE, each side of a join, UNION ALL, ORDER BY and
        // LIMIT, EXPLAIN, and a materialized view refresh
        assertMemoryLeak(() -> {
            createTable("t", true, 257, 100, "case when r % 5 = 0 then 'f' else 'k' || (r % 100) end");
            final String agg = "select count() n, sum(a) s from t where s = 'k7'";
            final String keyed = "select s, count() n, sum(a) sa from t where s in ('k7', 'k8', 'k9') order by s";
            final String[] queries = {
                    "select * from (" + agg + ")",
                    "with c as (" + agg + ") select * from c",
                    "select * from (" + keyed + ") x join (select s, count() m from t where s in ('k7', 'k9') order by s) y on s",
                    "select * from (select s, count() m from t where s in ('k7', 'k9') order by s) y join (" + keyed + ") x on s",
                    agg + " union all select count() n, sum(a) s from t where s = 'f'",
                    keyed + " limit 2",
                    keyed + " limit -2",
                    "select s, count() n, sum(a) sa from t where s in ('k7', 'k8', 'k9') order by n desc, s limit 1",
            };
            for (String sql : queries) {
                setFeatures(false, false);
                final String expected = runToString(engine, sqlExecutionContext, sql);
                setFeatures(true, true);
                final StringSink plan = new StringSink();
                printSql("explain " + sql, plan);
                Assert.assertTrue(sql + '\n' + plan, plan.toString().contains("Index Or Scan"));
                TestUtils.assertEquals(sql, expected, runToString(engine, sqlExecutionContext, sql));
            }
            // EXPLAIN shows the sampled estimate of its open
            final StringSink plan = new StringSink();
            printSql("explain " + agg, plan);
            TestUtils.assertContains(plan, "  selected: 257\n  scanned: 25700\n  opened: index\n");

            execute("create table w (ts timestamp, s symbol index type posting, a long) timestamp(ts) partition by hour wal");
            execute("insert into w select ((x - 1) * 36_000_000L)::timestamp, case when (x - 1) % 5 = 0 then 'f' else 'k' || ((x - 1) % 100) end, x from long_sequence(25_700)");
            drainWalQueue();
            execute("create materialized view mv as (select ts, count() n, sum(a) sa from w where s = 'k7' sample by 1d)");
            drainWalAndMatViewQueues();
            final String view = "select ts, count() n, sum(a) sa from w where s = 'k7' sample by 1d";
            setFeatures(false, false);
            final String expected = runToString(engine, sqlExecutionContext, view);
            setFeatures(true, true);
            TestUtils.assertEquals(expected, runToString(engine, sqlExecutionContext, "mv"));
        });
    }

    @Test
    public void testTwoFramesPerPartition() throws Exception {
        // two windows per hour: frames outnumber partitions. 65 partitions and 64 keys allow one
        // probed frame per target; 4 partitions and 2 keys count every frame
        assertMemoryLeak(() -> {
            createTable("t", true, 65, 100, "'k' || (r % 100)");
            createTable("tw", false, 65, 100, "'k' || (r % 100)");
            final String interval = "ts in '1970-01-01T00:00;10m;30m;130'";
            // r 0-16 and 50-66 of every hour
            Assert.assertEquals(65 * 34, twinCount(interval));
            Open open = open("select count() n, sum(a) s from t where s in (" + keyList("k", 0, 64) + ") and " + interval);
            Assert.assertEquals(targets(65).toString(), open.partitions);
            Assert.assertEquals(64 * 64, open.probes);
            Assert.assertEquals(65 * 34, open.scanned);
            // the first frame of each target: 17 rows, all of them among k0..k63
            Assert.assertEquals((long) ((double) (64 * 17) * (65 * 34) / (64 * 17)), open.selected);
            execute("drop table t");
            execute("drop table tw");
            createTable("t", true, 4, 100, "'k' || (r % 100)");
            createTable("tw", false, 4, 100, "'k' || (r % 100)");
            open = open("select count() n, sum(a) s from t where s in ('k0', 'k50') and " + interval);
            Assert.assertEquals("[0,1,2,3]", open.partitions);
            Assert.assertEquals(8 * 2, open.probes);
            Assert.assertEquals(twinCount("s in ('k0', 'k50') and " + interval), open.selected);
            Assert.assertEquals(8, open.selected);
        });
    }

    @Test
    public void testWorkerPools() throws Exception {
        // the estimate runs on the query's thread whatever the pool
        assertMemoryLeak(() -> {
            for (int workers : new int[]{1, 4}) {
                final WorkerPool pool = new TestWorkerPool(workers, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
                TestUtils.execute(pool, (engine, compiler, ctx) -> {
                    engine.execute("create table t (ts timestamp, s symbol index type posting, a long) timestamp(ts) partition by hour bypass wal", ctx);
                    engine.execute("insert into t select ((x - 1) * 36_000_000L)::timestamp, 'k' || ((x - 1) % 100), x from long_sequence(25_700)", ctx);
                    final String sql = "select s, count() n, sum(a) sa from t where s in ('k7', 'k8') order by s";
                    setFeatures(false, false);
                    final String expected = runToString(engine, ctx, sql);
                    setFeatures(true, true);
                    try (RecordCursorFactory factory = engine.select(sql, ctx)) {
                        final IndexScanChoiceRecordCursorFactory choice = findChoice(factory);
                        Assert.assertNotNull(choice);
                        TestUtils.assertEquals(expected, print(factory, ctx));
                        final IndexGatherRecordCursorFactory gather = findGather(factory);
                        Assert.assertEquals(targets(257).toString(), gather.getLastEstimatePartitions().toString());
                        Assert.assertEquals(128, gather.getLastEstimateProbes());
                        Assert.assertEquals(514, choice.getLastSelected());
                        Assert.assertTrue(choice.getIndexPlanOpens() > 0);
                    }
                    engine.execute("drop table t", ctx);
                }, configuration, LOG);
            }
        });
    }

    private static IndexScanChoiceRecordCursorFactory findChoice(RecordCursorFactory factory) {
        RecordCursorFactory f = factory;
        for (int i = 0; f != null && i < 6; i++) {
            if (f instanceof IndexScanChoiceRecordCursorFactory choice) {
                return choice;
            }
            f = f.getBaseFactory();
        }
        return null;
    }

    private static IndexGatherRecordCursorFactory findGather(RecordCursorFactory factory) {
        final IndexScanChoiceRecordCursorFactory choice = findChoice(factory);
        RecordCursorFactory f = choice != null ? choice.getIndexPlan() : factory;
        for (int i = 0; f != null && i < 6; i++) {
            if (f instanceof IndexGatherRecordCursorFactory gather) {
                return gather;
            }
            f = f.getBaseFactory();
        }
        return null;
    }

    private static String hour(int h) {
        return String.format("1970-01-%02dT%02d", 1 + h / 24, h % 24);
    }

    private static String keyList(String prefix, int lo, int hi) {
        final StringSink sink = new StringSink();
        for (int i = lo; i < hi; i++) {
            sink.put(i == lo ? "'" : ", '").put(prefix).put(i).put('\'');
        }
        return sink.toString();
    }

    private static String print(RecordCursorFactory factory, SqlExecutionContext ctx) throws SqlException {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            io.questdb.cairo.CursorPrinter.println(cursor, factory.getMetadata(), sink);
        }
        return sink.toString();
    }

    private static String runToString(CairoEngine engine, SqlExecutionContext ctx, String sql) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
            return print(factory, ctx);
        }
    }

    private static void setFeatures(boolean gather, boolean choice) {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_INDEX_GROUPBY_ENABLED, String.valueOf(gather));
        setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_CHOICE_ENABLED, String.valueOf(choice));
    }

    // the partitions the estimate probes when the walk yields frames in `groups` partitions
    private static IntList targets(int groups) {
        final IntList targets = new IntList();
        final int m = Math.min(groups, SAMPLE_PARTITIONS);
        for (int j = 0; j < m; j++) {
            targets.add((int) ((long) j * groups / m));
        }
        return targets;
    }

    private static long twinCount(String where) throws SqlException {
        return twinCountOf("select count() from tw where " + where);
    }

    private static long twinCountOf(String sql) throws SqlException {
        try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    private void createTable(String name, boolean posting, int partitions, int rowsPerPartition, String symbol) throws SqlException {
        createTable(name, posting, partitions, rowsPerPartition, symbol, "x");
    }

    // `partitions` hours of `rowsPerPartition` rows; in `symbol`, r is the row's index in its
    // partition and h the partition's
    private void createTable(String name, boolean posting, int partitions, int rowsPerPartition, String symbol, String a) throws SqlException {
        execute("create table " + name + " (ts timestamp, s symbol" + (posting ? " index type posting" : "") + ", a long) timestamp(ts) partition by hour bypass wal");
        final String r = "((x - 1) % " + rowsPerPartition + ")";
        final String h = "((x - 1) / " + rowsPerPartition + ")";
        execute("insert into " + name + " select ((x - 1) * " + (3_600_000_000L / rowsPerPartition) + "L)::timestamp, "
                + symbol.replaceAll("\\br\\b", r).replaceAll("\\bh\\b", h) + ", " + a
                + " from long_sequence(" + (long) partitions * rowsPerPartition + ")");
    }

    private Open open(String sql) throws SqlException {
        setFeatures(true, true);
        try (RecordCursorFactory factory = select(sql)) {
            return open(factory, sql);
        }
    }

    // opens the choice, and checks its rows against the same query with both switches off
    private Open open(RecordCursorFactory factory, String sql) throws SqlException {
        final IndexScanChoiceRecordCursorFactory choice = findChoice(factory);
        Assert.assertNotNull(sql, choice);
        final IndexGatherRecordCursorFactory gather = findGather(factory);
        Assert.assertNotNull(sql, gather);
        final long indexOpens = choice.getIndexPlanOpens();
        final long scanOpens = choice.getScanPlanOpens();
        final Open open = new Open();
        open.rows = print(factory, sqlExecutionContext);
        open.index = choice.getIndexPlanOpens() > indexOpens;
        Assert.assertEquals(open.index, choice.getScanPlanOpens() == scanOpens);
        open.selected = choice.getLastSelected();
        open.scanned = choice.getLastScanned();
        open.probes = gather.getLastEstimateProbes();
        open.partitions = gather.getLastEstimatePartitions().toString();
        setFeatures(false, false);
        TestUtils.assertEquals(sql, runToString(engine, sqlExecutionContext, sql), open.rows);
        setFeatures(true, true);
        return open;
    }

    private static final class Open {
        boolean index;
        String partitions;
        long probes;
        String rows;
        long scanned;
        long selected;
    }
}
