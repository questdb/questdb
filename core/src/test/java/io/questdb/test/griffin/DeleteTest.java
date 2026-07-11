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

package io.questdb.test.griffin;

import io.questdb.cairo.TableToken;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.DeleteOperation;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DeleteTest extends AbstractCairoTest {

    @Test
    public void testDeleteCompilesToDeleteType() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cc = compiler.compile("DELETE FROM t WHERE x = 1", sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.DELETE, cc.getType());
                Assert.assertNotNull(cc.getDeleteOperation());
            }
        });
    }

    @Test
    public void testDeleteRequiresWhere() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
            try {
                execute("DELETE FROM t");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "WHERE");
            }
        });
    }

    @Test
    public void testDeleteRejectsNonWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY BYPASS WAL");
            try {
                execute("DELETE FROM t WHERE x = 1");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "WAL");
            }
        });
    }

    @Test
    public void testDeleteRejectsPlainView() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
            execute("create view t_view as (select ts, max(x) as x from t sample by 1h)");
            try {
                execute("DELETE FROM t_view WHERE x = 1");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "view");
            }
        });
    }

    @Test
    public void testDeleteRejectsUnknownColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
            try {
                execute("DELETE FROM t WHERE nope = 1");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "nope");
            }
        });
    }

    // ---- end-to-end execution tests (Task 1.10) ----

    @Test
    public void testDeleteByArbitraryCondition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*60*1000000L)::timestamp ts, x, rnd_symbol('a','b') s " +
                    "from long_sequence(10)) timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            execute("DELETE FROM t WHERE x % 2 = 0");
            drainWalQueue();
            assertQuery("select count(*) from t").noRandomAccess().expectSize().returns("count\n5\n");
            assertQuery("select * from t where x % 2 = 0").timestamp("ts").returns("ts\tx\ts\n");
        });
    }

    @Test
    public void testDeleteByTimeRangeAcrossPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x " +
                    "from long_sequence(96)) timestamp(ts) partition by DAY WAL"); // 4 days
            drainWalQueue();
            execute("DELETE FROM t WHERE ts < '1970-01-03T00:00:00.000000Z'");
            drainWalQueue();
            assertQuery("select min(ts) from t").timestamp("min").expectSize().returns("min\n1970-01-03T00:00:00.000000Z\n");
            // Exact survivor set: every row from x=48 (the first 1970-01-03 row) through x=96 must
            // remain, and nothing else, so a wrong-rows-survive bug can't hide behind a correct min().
            assertQuery("select x from t").expectSize().returns("""
                    x
                    48
                    49
                    50
                    51
                    52
                    53
                    54
                    55
                    56
                    57
                    58
                    59
                    60
                    61
                    62
                    63
                    64
                    65
                    66
                    67
                    68
                    69
                    70
                    71
                    72
                    73
                    74
                    75
                    76
                    77
                    78
                    79
                    80
                    81
                    82
                    83
                    84
                    85
                    86
                    87
                    88
                    89
                    90
                    91
                    92
                    93
                    94
                    95
                    96
                    """);
        });
    }

    @Test
    public void testDeleteEverythingEmptiesTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x from long_sequence(48)) " +
                    "timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            execute("DELETE FROM t WHERE ts >= '1970-01-01T00:00:00.000000Z'");
            drainWalQueue();
            assertQuery("select count(*) from t").noRandomAccess().expectSize().returns("count\n0\n");
            // Exact survivor set: the table must be truly empty, not just report a zero count.
            assertQuery("select * from t").timestamp("ts").expectSize().returns("ts\tx\n");
        });
    }

    @Test
    public void testDeleteNoMatchIsNoOp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*60*1000000L)::timestamp ts, x from long_sequence(10)) " +
                    "timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            execute("DELETE FROM t WHERE x > 1000");
            drainWalQueue();
            assertQuery("select count(*) from t").noRandomAccess().expectSize().returns("count\n10\n");
            // Exact survivor set: a no-op DELETE must leave every original row untouched.
            assertQuery("select ts, x from t").timestamp("ts").expectSize().returns("""
                    ts\tx
                    1970-01-01T00:01:00.000000Z\t1
                    1970-01-01T00:02:00.000000Z\t2
                    1970-01-01T00:03:00.000000Z\t3
                    1970-01-01T00:04:00.000000Z\t4
                    1970-01-01T00:05:00.000000Z\t5
                    1970-01-01T00:06:00.000000Z\t6
                    1970-01-01T00:07:00.000000Z\t7
                    1970-01-01T00:08:00.000000Z\t8
                    1970-01-01T00:09:00.000000Z\t9
                    1970-01-01T00:10:00.000000Z\t10
                    """);
        });
    }

    /**
     * Guards the survivor-set property against future optimiser changes: {@code executeDelete}
     * recompiles the DELETE predicate as {@code NOT(pred)} to build the survivor factory (see
     * {@link io.questdb.cairo.wal.OperationExecutor#executeDelete}). Under naive 3-valued SQL logic,
     * {@code NOT(NULL)} is itself {@code NULL} (not {@code TRUE}), which would risk a NULL row matching
     * neither {@code pred} nor {@code NOT(pred)} - silently dropping it from the table even though the
     * predicate never matched it.
     * <p>
     * Empirically, QuestDB's int equality functions do not implement that 3-valued propagation: {@code
     * EqIntFunctionFactory} (shared by {@code =} and {@code !=}, see {@code AbstractEqBinaryFunction}'s
     * {@code negated} flag) compares the {@code Numbers.INT_NULL} sentinel as a plain {@code int} with no
     * null short-circuit - {@code negated != (left.getInt(rec) == right.getInt(rec))}. So for a NULL n:
     * {@code n = 5} is a deterministic {@code false} (the row survives a {@code = 5} DELETE), while
     * {@code n != 5} is a deterministic {@code true} (the row is REMOVED by a {@code != 5} DELETE, since
     * {@code NOT(true)} excludes it from the survivor set). This pins that actual, asymmetric, verified
     * behavior so a future change to either the comparison functions or the DELETE negation path can't
     * silently flip it.
     */
    @Test
    public void testDeleteNullPredicateEqualityVsInequality() throws Exception {
        assertMemoryLeak(() -> {
            // n is NULL at x = 3, 6, 9 (x % 3 = 0); otherwise n = x.
            final String ddl = "create table %s as (select (x*60*1000000L)::timestamp ts, x, " +
                    "(case when x %% 3 = 0 then null else x end)::int n from long_sequence(9)) " +
                    "timestamp(ts) partition by DAY WAL";

            execute(String.format(ddl, "t1"));
            drainWalQueue();
            execute("DELETE FROM t1 WHERE n = 5");
            drainWalQueue();
            // n = 5 is false (not true) for a NULL n, so only the single n=5 row (x=5) is removed and
            // every NULL row (x=3,6,9) survives.
            assertQuery("select x, n from t1").expectSize().returns("""
                    x\tn
                    1\t1
                    2\t2
                    3\tnull
                    4\t4
                    6\tnull
                    7\t7
                    8\t8
                    9\tnull
                    """);

            execute(String.format(ddl, "t2"));
            drainWalQueue();
            execute("DELETE FROM t2 WHERE n != 5");
            drainWalQueue();
            // n != 5 is true (not false/unknown) for a NULL n, so every NULL row (x=3,6,9) is REMOVED
            // along with every other non-5 row; only the n=5 row (x=5) survives.
            assertQuery("select x, n from t2").expectSize().returns("""
                    x\tn
                    5\t5
                    """);
        });
    }

    // ---- correctness matrix: dedup, O3, symbol/index, concurrency, mat-view (Task 1.11) ----

    @Test
    public void testDeleteOnDedupTableKeepsKeysUnique() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, s symbol, v int) timestamp(ts) partition by DAY WAL " +
                    "dedup upsert keys(ts, s)");
            execute("insert into t(ts, s, v) values " +
                    "('1970-01-01T00:01:00.000000Z','a',1)," +
                    "('1970-01-01T00:02:00.000000Z','b',2)," +
                    "('1970-01-01T00:03:00.000000Z','a',3)," +
                    "('1970-01-01T00:04:00.000000Z','b',4)," +
                    "('1970-01-01T00:05:00.000000Z','a',5)," +
                    "('1970-01-01T00:06:00.000000Z','b',6)");
            drainWalQueue();

            execute("DELETE FROM t WHERE s = 'b'");
            drainWalQueue();

            // Survivors: the three 'a' rows, untouched.
            assertQuery("select ts, s, v from t").timestamp("ts").expectSize().returns("""
                    ts\ts\tv
                    1970-01-01T00:01:00.000000Z\ta\t1
                    1970-01-01T00:03:00.000000Z\ta\t3
                    1970-01-01T00:05:00.000000Z\ta\t5
                    """);

            // The replace-range write that lands the survivors bypasses UPSERT-key dedup WITHIN the
            // replaced range (the survivors are already authoritative, key-unique-by-construction
            // rows straight from the pre-delete table) - but the table's dedup config itself must
            // still be intact and enforced for FUTURE commits: upserting the same (ts, s) key as an
            // existing survivor must update it in place, not create a duplicate row.
            execute("insert into t(ts, s, v) values ('1970-01-01T00:01:00.000000Z','a',999)");
            drainWalQueue();

            assertQuery("select count(*) from t").noRandomAccess().expectSize().returns("count\n3\n");
            assertQuery("select ts, s, v from t").timestamp("ts").expectSize().returns("""
                    ts\ts\tv
                    1970-01-01T00:01:00.000000Z\ta\t999
                    1970-01-01T00:03:00.000000Z\ta\t3
                    1970-01-01T00:05:00.000000Z\ta\t5
                    """);
        });
    }

    @Test
    public void testDeleteMiddleBandWithOutOfOrderSurvivors() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");

            // Seed genuinely out of order: statement order is day3, day1, day2 - crossing all three
            // DAY partitions out of chronological order - and the day2 statement itself lists its six
            // rows in scrambled order. Every row lands via real O3 commit machinery at drain time,
            // unlike every prior DeleteTest case (each seeded via an ascending generator, or a single
            // statement whose rows were already in ts order): the day2 partition this DELETE targets
            // is genuinely built from out-of-order writes, not a single ordered one. (Note: by the
            // time DELETE recompiles its survivor SELECT, the table has already settled into physical
            // ts order - that's a storage invariant - so replaceRange's forced O3 restage-and-sort of
            // the survivor batch is exercised as an already-sorted no-op here too, same as every other
            // DELETE. What's new in this test is the O3-built partition shape and the middle-of-partition
            // survivor gap below, plus an independent order/content check on the result.)
            execute("insert into t(ts, x) values " +
                    "('1970-01-03T00:00:00.000000Z',11)," +
                    "('1970-01-03T06:00:00.000000Z',12)," +
                    "('1970-01-03T12:00:00.000000Z',13)," +
                    "('1970-01-03T18:00:00.000000Z',14)");
            execute("insert into t(ts, x) values " +
                    "('1970-01-01T00:00:00.000000Z',1)," +
                    "('1970-01-01T06:00:00.000000Z',2)," +
                    "('1970-01-01T12:00:00.000000Z',3)," +
                    "('1970-01-01T18:00:00.000000Z',4)");
            execute("insert into t(ts, x) values " +
                    "('1970-01-02T12:00:00.000000Z',8)," +
                    "('1970-01-02T00:00:00.000000Z',5)," +
                    "('1970-01-02T20:00:00.000000Z',10)," +
                    "('1970-01-02T04:00:00.000000Z',6)," +
                    "('1970-01-02T16:00:00.000000Z',9)," +
                    "('1970-01-02T08:00:00.000000Z',7)");
            drainWalQueue();

            // Snapshot every seeded row (now settled into physical ts order) as an independent oracle
            // for the NOT-matching reference below - this table is never touched by the DELETE.
            execute("create table t_ref as (select * from t)");

            // Delete a MIDDLE band inside the day2 partition, leaving survivors on BOTH sides of the
            // removed band within that same partition (x=5,6 before; x=9,10 after) - a genuine
            // middle-of-partition hole, not just a one-sided trim.
            execute("DELETE FROM t WHERE ts BETWEEN '1970-01-02T08:00:00.000000Z' AND '1970-01-02T12:00:00.000000Z'");
            drainWalQueue();

            assertQuery("select ts, x from t").timestamp("ts").expectSize().returns("""
                    ts\tx
                    1970-01-01T00:00:00.000000Z\t1
                    1970-01-01T06:00:00.000000Z\t2
                    1970-01-01T12:00:00.000000Z\t3
                    1970-01-01T18:00:00.000000Z\t4
                    1970-01-02T00:00:00.000000Z\t5
                    1970-01-02T04:00:00.000000Z\t6
                    1970-01-02T16:00:00.000000Z\t9
                    1970-01-02T20:00:00.000000Z\t10
                    1970-01-03T00:00:00.000000Z\t11
                    1970-01-03T06:00:00.000000Z\t12
                    1970-01-03T12:00:00.000000Z\t13
                    1970-01-03T18:00:00.000000Z\t14
                    """);

            // Independent dynamic oracle: every row of the untouched snapshot that does NOT fall in
            // the deleted band, in cursor (i.e. physical/timestamp) order, must equal the post-delete
            // table exactly - both content and order. assertSqlCursors does a lockstep cursor
            // comparison (see TestUtils#assertEquals), so an order mismatch fails it too.
            assertSqlCursors(
                    "select * from t_ref where not (ts between '1970-01-02T08:00:00.000000Z' and '1970-01-02T12:00:00.000000Z')",
                    "select * from t"
            );
        });
    }

    @Test
    public void testDeleteBySymbolKeepsIndexConsistent() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, s symbol index, x int) timestamp(ts) partition by DAY WAL");
            execute("insert into t(ts, s, x) values " +
                    "('1970-01-01T00:01:00.000000Z','a',1)," +
                    "('1970-01-01T00:02:00.000000Z','b',2)," +
                    "('1970-01-01T00:03:00.000000Z','a',3)," +
                    "('1970-01-01T00:04:00.000000Z','b',4)," +
                    "('1970-01-01T00:05:00.000000Z','a',5)," +
                    "('1970-01-01T00:06:00.000000Z','b',6)");
            drainWalQueue();

            execute("DELETE FROM t WHERE s = 'b'");
            drainWalQueue();

            // The indexed-symbol survivor read must reflect the delete: 'a' rows remain exactly as
            // they were (proves the symbol index isn't stale after the replace-range rewrite), and
            // 'b' returns nothing (not a stale posting list, and not silently falling back to a full
            // scan that happens to hide a stale index).
            assertQuery("select x from t where s = 'a'").returns("""
                    x
                    1
                    3
                    5
                    """);
            assertQuery("select count(*) from t where s = 'b'").noRandomAccess().expectSize().returns("count\n0\n");
            assertQuery("select x from t").expectSize().returns("""
                    x
                    1
                    3
                    5
                    """);
        });
    }

    @Test
    public void testDeleteThenInsertBeforeDrainAppliesInSeqTxnOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x from long_sequence(10)) " +
                    "timestamp(ts) partition by DAY WAL");
            drainWalQueue();

            // Store the DELETE's WAL SQL txn WITHOUT draining: it is now queued but not yet applied -
            // the table is untouched until the drainWalQueue() call below.
            execute("DELETE FROM t WHERE ts BETWEEN '1970-01-01T03:00:00.000000Z' AND '1970-01-01T07:00:00.000000Z'");

            // A later statement (higher seqTxn) inserts a row whose timestamp falls INSIDE the
            // not-yet-applied delete band, still before any drain.
            execute("INSERT INTO t(ts, x) VALUES ('1970-01-01T05:00:00.000000Z', 999)");

            // Apply both queued WAL txns now, strictly in seqTxn order.
            drainWalQueue();

            // THE key deferred-apply guarantee: the DELETE recomputes its survivor set as of ITS OWN
            // position in the WAL, seeing only rows already committed before it (x=1..10) - so
            // x=3..7 (03:00..07:00) are removed. The INSERT, sequenced strictly after, is applied
            // against the already-mutated table and is untouched by the (already-finished) DELETE:
            // the new row (x=999 at 05:00, inside the deleted band) must survive.
            assertQuery("select ts, x from t").timestamp("ts").expectSize().returns("""
                    ts\tx
                    1970-01-01T01:00:00.000000Z\t1
                    1970-01-01T02:00:00.000000Z\t2
                    1970-01-01T05:00:00.000000Z\t999
                    1970-01-01T08:00:00.000000Z\t8
                    1970-01-01T09:00:00.000000Z\t9
                    1970-01-01T10:00:00.000000Z\t10
                    """);
        });
    }

    @Test
    public void testDeleteInvalidatesMaterializedView() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
            execute("create materialized view t_1h as (select ts, sum(x) as x from t sample by 1h) partition by DAY");
            execute("insert into t(ts, x) values " +
                    "('1970-01-01T00:01:00.000000Z',1)," +
                    "('1970-01-01T00:31:00.000000Z',2)," +
                    "('1970-01-01T01:05:00.000000Z',3)");
            drainWalAndMatViewQueues();
            drainPurgeJob();

            assertQuery("select view_name, base_table_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            t_1h\tt\tvalid
                            """);

            execute("DELETE FROM t WHERE x = 2");
            drainWalAndMatViewQueues();
            drainPurgeJob();

            // DELETE invalidates a dependent mat view when it actually removes at least one row (see
            // ApplyWal2TableJob#processWalSql's CMD_DELETE_TABLE case: invalidation is gated on
            // deleted > 0) - it does NOT attempt an incremental refresh over the replaced range. Pin
            // the actual, real invalidation reason string (DeleteOperation.MAT_VIEW_INVALIDATION_REASON).
            assertQuery("select view_name, base_table_name, view_status, invalidation_reason from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            t_1h\tt\tinvalid\tdelete operation
                            """);
        });
    }

    // ---- Task 2.1: time-range fast path (single empty-replace over the deleted interval) ----

    /**
     * White-box guard on the classifier that routes the fast path: only a predicate that reduces to a
     * SINGLE designated-timestamp interval with NO residual non-timestamp filter is classified as a pure
     * time range (and later applied as one empty {@code replaceRange} over the deleted interval, instead of
     * staging survivors). This asserts the DECISION directly on the compiled {@link DeleteOperation} - a
     * functional survivor-set check cannot distinguish the fast path from the fallback because both produce
     * identical rows. The bounds are the DELETED interval {@code [lo, hiExcl)} in the table's timestamp
     * units. {@code isWalApplication()} is not required: {@code SqlCompilerImpl} classifies on every DELETE
     * compile from the ORIGINAL (un-negated) predicate, so a plain query-thread compile observes the same
     * decision the WAL-apply pass makes.
     */
    @Test
    public void testTimeRangeDeleteClassifiedAsPureInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int, s symbol) timestamp(ts) partition by DAY WAL");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // micros since epoch (the table's designated-timestamp unit): 1970-01-02 and 1970-01-03.
                final long d2 = 86400 * 1_000_000L;
                final long d3 = 172800 * 1_000_000L;
                // open upper bound: ts < d3 -> delete [MIN, d3); clamped to the table at apply time.
                assertPureTimeRange(compiler, "DELETE FROM t WHERE ts < '1970-01-03T00:00:00.000000Z'",
                        Long.MIN_VALUE, d3);
                // open lower bound: ts >= d3 -> delete [d3, MAX] (hiExcl saturates at Long.MAX_VALUE).
                assertPureTimeRange(compiler, "DELETE FROM t WHERE ts >= '1970-01-03T00:00:00.000000Z'",
                        d3, Long.MAX_VALUE);
                // half-open band: [d2, d3).
                assertPureTimeRange(compiler,
                        "DELETE FROM t WHERE ts >= '1970-01-02T00:00:00.000000Z' AND ts < '1970-01-03T00:00:00.000000Z'",
                        d2, d3);
                // BETWEEN is inclusive on both ends: [d2, d3] -> hiExcl = d3 + 1.
                assertPureTimeRange(compiler,
                        "DELETE FROM t WHERE ts BETWEEN '1970-01-02T00:00:00.000000Z' AND '1970-01-03T00:00:00.000000Z'",
                        d2, d3 + 1);
            }
        });
    }

    /**
     * The soundness half of the classifier: anything that is NOT a single designated-timestamp interval with
     * an empty residual filter must fall through to the whole-range survivor-replace. A false positive here
     * is a CORRECTNESS bug - e.g. classifying {@code ts < X AND s='a'} as the interval {@code [MIN, X)} would
     * empty-replace that whole range and wrongly delete {@code s='b'} rows too (see
     * {@link #testDeleteMixedTimestampAndFilterDeletesOnlyMatchingRows}). Mirrors the code generator's own
     * pure-interval-scan gate: {@code filter == null && keyColumn == null && hasIntervalFilters() && a single
     * static interval}.
     */
    @Test
    public void testMixedOrArbitraryDeleteNotClassifiedAsPureInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int, s symbol) timestamp(ts) partition by DAY WAL");
            execute("create table ti (ts timestamp, x int, s symbol index) timestamp(ts) partition by DAY WAL");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // timestamp bound AND a residual non-timestamp filter -> intrinsicModel.filter != null.
                assertNotPureTimeRange(compiler, "DELETE FROM t WHERE ts < '1970-01-03T00:00:00.000000Z' AND s = 'a'");
                // timestamp bound AND an indexed-symbol key -> intrinsicModel.keyColumn != null.
                assertNotPureTimeRange(compiler, "DELETE FROM ti WHERE ts < '1970-01-03T00:00:00.000000Z' AND s = 'a'");
                // no timestamp component at all.
                assertNotPureTimeRange(compiler, "DELETE FROM t WHERE x % 2 = 0");
                assertNotPureTimeRange(compiler, "DELETE FROM t WHERE s = 'b'");
                // two disjoint timestamp intervals (OR) -> not a SINGLE contiguous interval.
                assertNotPureTimeRange(compiler,
                        "DELETE FROM t WHERE ts < '1970-01-02T00:00:00.000000Z' OR ts > '1970-01-05T00:00:00.000000Z'");
            }
        });
    }

    /**
     * Defense-in-depth: pins the fallback for the DYNAMIC-bound cases {@code classifyDeleteTimeRange}'s own
     * comment calls out by name but that were not previously asserted by a test - {@code SqlCompilerImpl}:
     * "Compile-time-constant intervals only ... Runtime intervals (now(), bind vars) ... fall back - correct,
     * just unoptimized" - plus a subquery-typed timestamp bound, which hits the same
     * {@code RuntimeIntervalModel.isStatic()} guard via a different {@code WhereClauseParser} path
     * ({@code compareWithNode.type == ExpressionNode.QUERY} in {@code analyzeTimestampLess}). A future edit
     * to that gate that mistakenly treated a dynamic bound as static would silently route these onto the
     * pure-interval empty-replace path and delete the wrong rows depending on when/what the bound evaluates
     * to - the same false-positive hazard as {@link #testMixedOrArbitraryDeleteNotClassifiedAsPureInterval},
     * just for a DYNAMIC rather than a MIXED-filter predicate. Same white-box mechanism
     * ({@link #assertNotPureTimeRange}).
     * <p>
     * {@code ts IN (SELECT ts FROM t WHERE x = 1)} is deliberately NOT included: IN-with-subquery is only
     * wired for SYMBOL columns ({@code InSymbolCursorFunctionFactory}, signature {@code in(KC)}) - there is
     * no {@code in(TIMESTAMP, CURSOR)} overload, so that form does not compile as a DELETE at all. The
     * scalar comparison below ({@code ts < (subquery)}, signature {@code <(NC)} via
     * {@code LtTimestampCursorFunctionFactory}) is the closest ts-subquery predicate that both compiles and
     * demonstrably falls back.
     */
    @Test
    public void testRuntimeOrSubqueryBoundDeleteNotClassifiedAsPureInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int, s symbol) timestamp(ts) partition by DAY WAL");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // now() is a runtime constant (evaluated once per execution), not a compile-time constant:
                // WhereClauseParser.analyzeTimestampLess routes it through intersectIntervals(lo, Function,
                // adj), which makes RuntimeIntervalModel.isStatic() false.
                assertNotPureTimeRange(compiler, "DELETE FROM t WHERE ts < now()");

                // Bind variable: WhereClauseParser.isFunc() treats BIND_VARIABLE like FUNCTION, and
                // IndexedParameterLinkFunction.isRuntimeConstant() is unconditionally true, so $1 takes the
                // exact same dynamic-bound path as now() above.
                sqlExecutionContext.getBindVariableService().setTimestamp(0, 172800000000L);
                assertNotPureTimeRange(compiler, "DELETE FROM t WHERE ts < $1");

                // Subquery bound: ts compared against a scalar (single-row, single-TIMESTAMP-column)
                // subquery hits WhereClauseParser's dedicated ExpressionNode.QUERY case, which is also
                // folded into the interval as a dynamic Function bound.
                assertNotPureTimeRange(compiler, "DELETE FROM t WHERE ts < (SELECT max(ts) FROM t WHERE x = 1)");
            }
        });
    }

    @Test
    public void testDeleteOpenEndedTimeRangeDropsOldPartitions() throws Exception {
        assertMemoryLeak(() -> {
            // 4+ DAY partitions, one row per hour.
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x " +
                    "from long_sequence(96)) timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            execute("DELETE FROM t WHERE ts < '1970-01-03T00:00:00.000000Z'");
            drainWalQueue();
            // The two oldest whole partitions (days 1-2) are gone; day 3 onward survive intact.
            assertQuery("select count(*) from t where ts < '1970-01-03T00:00:00.000000Z'")
                    .noRandomAccess().expectSize().returns("count\n0\n");
            assertQuery("select min(ts), max(ts), count() from t").noRandomAccess().expectSize().returns("""
                    min\tmax\tcount
                    1970-01-03T00:00:00.000000Z\t1970-01-05T00:00:00.000000Z\t49
                    """);
        });
    }

    @Test
    public void testDeleteBoundedTimeRangeTrimsAndDrops() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x " +
                    "from long_sequence(96)) timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            // The interval [day2, day3) covers exactly the whole 1970-01-02 partition (x = 24..47).
            execute("DELETE FROM t WHERE ts >= '1970-01-02T00:00:00.000000Z' AND ts < '1970-01-03T00:00:00.000000Z'");
            drainWalQueue();
            // Day 2 fully gone.
            assertQuery("select count(*) from t where ts >= '1970-01-02T00:00:00.000000Z' and ts < '1970-01-03T00:00:00.000000Z'")
                    .noRandomAccess().expectSize().returns("count\n0\n");
            // Neighbours untouched: day 1 (x = 1..23) and day 3+ (x = 48..96) both intact.
            assertQuery("select count(*) from t").noRandomAccess().expectSize().returns("count\n72\n");
            assertQuery("select min(x), max(x) from t where ts < '1970-01-02T00:00:00.000000Z'")
                    .noRandomAccess().expectSize().returns("min\tmax\n1\t23\n");
            assertQuery("select min(x), max(x) from t where ts >= '1970-01-03T00:00:00.000000Z'")
                    .noRandomAccess().expectSize().returns("min\tmax\n48\t96\n");
        });
    }

    @Test
    public void testDeleteTimeRangeBoundarySplitsPartition() throws Exception {
        assertMemoryLeak(() -> {
            // A single DAY partition (x = 1..20 -> 01:00..20:00 on 1970-01-01).
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x " +
                    "from long_sequence(20)) timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            // Independent oracle snapshot, never touched by the DELETE.
            execute("create table t_ref as (select * from t)");

            // Delete a sub-partition band in the MIDDLE (05:00..10:00 inclusive) - a boundary trim with
            // survivors on both sides, no whole-partition drop.
            execute("DELETE FROM t WHERE ts BETWEEN '1970-01-01T05:00:00.000000Z' AND '1970-01-01T10:00:00.000000Z'");
            drainWalQueue();

            // Exact survivors via a NOT BETWEEN reference: a boundary-conversion or off-by-one bug in the
            // interval -> replaceRange mapping shows up here as a row difference.
            assertSqlCursors(
                    "select * from t_ref where ts not between '1970-01-01T05:00:00.000000Z' and '1970-01-01T10:00:00.000000Z'",
                    "select * from t"
            );
            assertQuery("select count(*) from t").noRandomAccess().expectSize().returns("count\n14\n");
        });
    }

    /**
     * The critical soundness guard: a predicate mixing a timestamp bound with a non-timestamp filter must
     * delete ONLY the rows matching the WHOLE predicate. If {@code ts < X AND s='a'} were (wrongly) routed
     * to an empty-replace over {@code [MIN, X)}, every {@code s='b'} row before X would be destroyed too.
     * Pins that mixed predicates take the always-correct survivor-replace path, not the fast path.
     */
    @Test
    public void testDeleteMixedTimestampAndFilterDeletesOnlyMatchingRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int, s symbol) timestamp(ts) partition by DAY WAL");
            execute("insert into t(ts, x, s) values " +
                    "('1970-01-01T00:00:00.000000Z',1,'a')," +
                    "('1970-01-01T12:00:00.000000Z',2,'b')," +
                    "('1970-01-02T00:00:00.000000Z',3,'a')," +
                    "('1970-01-02T12:00:00.000000Z',4,'b')," +
                    "('1970-01-03T00:00:00.000000Z',5,'a')," +
                    "('1970-01-03T12:00:00.000000Z',6,'b')," +
                    "('1970-01-04T00:00:00.000000Z',7,'a')," +
                    "('1970-01-04T12:00:00.000000Z',8,'b')");
            drainWalQueue();
            execute("create table t_ref as (select * from t)");

            execute("DELETE FROM t WHERE ts < '1970-01-03T00:00:00.000000Z' AND s = 'a'");
            drainWalQueue();

            // Only x=1 and x=3 (s='a' AND before day 3) may be removed. The s='b' rows before day 3 (x=2,4)
            // MUST survive - proving the mixed predicate did NOT empty-replace the whole [MIN, day3) range.
            assertQuery("select ts, x, s from t").timestamp("ts").expectSize().returns("""
                    ts\tx\ts
                    1970-01-01T12:00:00.000000Z\t2\tb
                    1970-01-02T12:00:00.000000Z\t4\tb
                    1970-01-03T00:00:00.000000Z\t5\ta
                    1970-01-03T12:00:00.000000Z\t6\tb
                    1970-01-04T00:00:00.000000Z\t7\ta
                    1970-01-04T12:00:00.000000Z\t8\tb
                    """);
            // Independent oracle: survivors == the reference minus rows matching the WHOLE predicate.
            assertSqlCursors(
                    "select * from t_ref where not (ts < '1970-01-03T00:00:00.000000Z' and s = 'a')",
                    "select * from t"
            );
        });
    }

    /**
     * Functional companion to {@link #testRuntimeOrSubqueryBoundDeleteNotClassifiedAsPureInterval}: proves
     * the fallback (whole-range survivor-replace) triggered by a runtime-bound predicate still produces the
     * CORRECT result, not just the correct routing decision. One row is far in the past and one is far in
     * the future relative to the real wall-clock {@code now()}, so this is a non-trivial split (not a
     * degenerate "delete everything") - a wrong-rows-survive bug can't hide behind an all-or-nothing result.
     */
    @Test
    public void testDeleteRuntimeBoundTimeRangeDeletesOnlyPastRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
            execute("insert into t(ts, x) values " +
                    "('2000-01-01T00:00:00.000000Z',1)," +
                    "('2999-01-01T00:00:00.000000Z',2)");
            drainWalQueue();
            execute("DELETE FROM t WHERE ts < now()");
            drainWalQueue();
            assertQuery("select x from t").expectSize().returns("""
                    x
                    2
                    """);
        });
    }

    // ---- Parquet partitions (Task 2.2) ----

    /**
     * A whole-partition time-range delete over a FULLY-COVERED Parquet partition is applied as an inline
     * O(1) drop during the replace-commit apply - no data rewrite, so it is Parquet-safe. Task 2.1 routes
     * this delete to a single empty-replace over [MIN, 1970-01-02); the replace-path guard (TableWriter
     * processO3Block) detects that the range covers the whole 1970-01-01 partition and drops it inline
     * instead of throwing "commit replace mode is not supported for Parquet partitions".
     */
    @Test
    public void testDeleteWholeParquetPartitionByTimeRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x from long_sequence(72)) " +
                    "timestamp(ts) partition by DAY WAL"); // 3 days
            drainWalQueue();
            execute("alter table t convert partition to parquet list '1970-01-01'");
            drainWalQueue();
            execute("DELETE FROM t WHERE ts < '1970-01-02T00:00:00.000000Z'");
            drainWalQueue();
            assertQuery("select min(ts) from t").timestamp("min").expectSize().returns("min\n1970-01-02T00:00:00.000000Z\n");
        });
    }

    /**
     * Task 3.1 (route a): a pure time-range delete that only PARTIALLY covers a Parquet partition (a boundary
     * trim inside the partition's day) needs a data rewrite the replace path cannot do on Parquet. The
     * convert-to-native pre-pass un-tiers the boundary partition first, then the replace trims it. Here
     * {@code ts < '1970-01-01T12:00:00Z'} deletes the first half of the Parquet day 1970-01-01 (x=1..11);
     * x=12..23 (and every later day) survive, and the partition ends up NATIVE. Pre-Task-3.1 this boundary
     * trim was rejected and suspended the table (see git history: testDeleteParquetBoundaryTrimStillRejected).
     */
    @Test
    public void testDeleteTimeRangeBoundaryTrimOnParquetConverts() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x from long_sequence(72)) " +
                    "timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            execute("alter table t convert partition to parquet list '1970-01-01'");
            drainWalQueue();
            // Deletes only the first half of the parquet partition 1970-01-01 (x=1..11); x=12..23 must survive.
            execute("DELETE FROM t WHERE ts < '1970-01-01T12:00:00.000000Z'");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t");
            Assert.assertFalse(
                    "a boundary trim of a Parquet partition must convert-to-native and succeed, not suspend",
                    engine.getTableSequencerAPI().isSuspended(tt));
            // 11 rows deleted from the boundary day; 61 survive.
            assertQuery("select count(*) from t").noRandomAccess().expectSize().returns("count\n61\n");
            // nothing below the boundary survives.
            assertQuery("select count(*) from t where ts < '1970-01-01T12:00:00.000000Z'")
                    .noRandomAccess().expectSize().returns("count\n0\n");
            // the surviving min is exactly the trim boundary.
            assertQuery("select min(ts) from t").timestamp("min").expectSize()
                    .returns("min\n1970-01-01T12:00:00.000000Z\n");
            // the surviving half of day 1 is intact (x=12..23 = 12 rows).
            assertQuery("select count(*) from t where ts < '1970-01-02T00:00:00.000000Z'")
                    .noRandomAccess().expectSize().returns("count\n12\n");
            // the boundary partition was un-tiered to native by the fallback.
            assertQuery("select isParquet from table_partitions('t') where name = '1970-01-01'")
                    .noRandomAccess().returns("isParquet\nfalse\n");
        });
    }

    /**
     * Drops TWO interior (non-first, non-last) Parquet partitions in a single replace-commit, mixed with
     * surviving native partitions on both sides. This exercises the coverage check's floor/ceiling bounds
     * (used for a partition that is neither the first nor the last, so neither getMinTimestamp() nor
     * getMaxTimestamp() applies) and confirms multiple inline Parquet drops settle the O3 partition
     * counters/latch without a hang. The delete range is partition-aligned, so both parquet partitions are
     * fully covered and dropped inline (no rewrite, no suspension).
     */
    @Test
    public void testDeleteInteriorParquetPartitionsByTimeRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x from long_sequence(96)) " +
                    "timestamp(ts) partition by DAY WAL"); // day1(x1-23) day2(x24-47) day3(x48-71) day4(x72-95) day5(x96)
            drainWalQueue();
            execute("alter table t convert partition to parquet list '1970-01-02'");
            drainWalQueue();
            execute("alter table t convert partition to parquet list '1970-01-03'");
            drainWalQueue();
            // Delete the two parquet days 2 and 3; day1 (native, before) and day4/day5 (native, after) survive.
            execute("DELETE FROM t WHERE ts >= '1970-01-02T00:00:00.000000Z' AND ts < '1970-01-04T00:00:00.000000Z'");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t");
            Assert.assertFalse(
                    "fully-covered whole-partition Parquet drops must NOT suspend the table",
                    engine.getTableSequencerAPI().isSuspended(tt));
            // 48 rows removed (x=24..71); 48 survive (x=1..23, 72..96).
            assertQuery("select count(*) from t").noRandomAccess().expectSize().returns("count\n48\n");
            // min unchanged (day1 native survives), max unchanged (day5 native survives), and the deleted band is gone.
            assertQuery("select min(ts), max(ts) from t").noRandomAccess().expectSize()
                    .returns("min\tmax\n1970-01-01T01:00:00.000000Z\t1970-01-05T00:00:00.000000Z\n");
            assertQuery("select count(*) from t where ts >= '1970-01-02T00:00:00.000000Z' and ts < '1970-01-04T00:00:00.000000Z'")
                    .noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    /**
     * Task 3.1 (mixed drop + convert): a single time-range delete over two Parquet partitions where one is
     * FULLY covered and the other only PARTIALLY covered. The fully-covered Parquet day 1970-01-01 is dropped
     * INLINE by the replace (Task 2.2, no convert - its data extent is entirely inside the range), while the
     * partially-covered Parquet day 1970-01-02 is CONVERTED to native and trimmed (Task 3.1). This exercises
     * the interaction between the inline-drop path (fully-covered partitions are NOT converted) and the
     * convert-fallback (only the boundary partition is). {@code ts < '1970-01-02T12:00:00Z'} removes all of
     * day 1 (x=1..23) and the first half of day 2 (x=24..35) = 35 rows; 37 survive, min becomes the day-2 trim
     * boundary, and the trimmed partition ends up NATIVE. Pre-Task-3.1 this was rejected and suspended the
     * table (see git history: testDeleteParquetDropThenPartialTrimRejectedAtomically).
     */
    @Test
    public void testDeleteParquetDropAndBoundaryTrimConverts() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x from long_sequence(72)) " +
                    "timestamp(ts) partition by DAY WAL"); // day1(x1-23) day2(x24-47) day3(x48-71) day4(x72)
            drainWalQueue();
            execute("alter table t convert partition to parquet list '1970-01-01'");
            drainWalQueue();
            execute("alter table t convert partition to parquet list '1970-01-02'");
            drainWalQueue();
            // Range fully covers parquet day1 (inline-dropped, NOT converted) AND partially covers parquet day2
            // (converted to native + trimmed).
            execute("DELETE FROM t WHERE ts < '1970-01-02T12:00:00.000000Z'");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t");
            Assert.assertFalse(
                    "drop-fully-covered + convert-and-trim-boundary in one commit must succeed, not suspend",
                    engine.getTableSequencerAPI().isSuspended(tt));
            // 23 (all day1) + 12 (day2 x24..35) = 35 deleted; 37 survive.
            assertQuery("select count(*) from t").noRandomAccess().expectSize().returns("count\n37\n");
            assertQuery("select count(*) from t where ts < '1970-01-02T12:00:00.000000Z'")
                    .noRandomAccess().expectSize().returns("count\n0\n");
            // day1 fully removed, so the surviving min is the day-2 trim boundary.
            assertQuery("select min(ts) from t").timestamp("min").expectSize()
                    .returns("min\n1970-01-02T12:00:00.000000Z\n");
            // day1 is gone (dropped inline); day2 was un-tiered to native by the fallback.
            assertQuery("select count(*) from table_partitions('t') where name = '1970-01-01'")
                    .noRandomAccess().expectSize().returns("count\n0\n");
            assertQuery("select isParquet from table_partitions('t') where name = '1970-01-02'")
                    .noRandomAccess().returns("isParquet\nfalse\n");
        });
    }

    /**
     * Task 3.1: an ARBITRARY-condition delete (a non-pure-time-range predicate, routed to the whole-range
     * survivor-replace) that must rewrite a Parquet partition converts EVERY Parquet partition to native first,
     * then rewrites. Here {@code x % 2 = 0 AND ts < '1970-01-02'} matches inside the Parquet day 1970-01-01, so
     * the survivor-replace has to rewrite it - impossible on Parquet in place, so the convert-to-native pre-pass
     * un-tiers it. day1 = x 1..23 (23 rows); the even x below day 2 are x=2,4,..,22 (11 rows) -> deleted; 48-11=37
     * survive. The converted partition ends up NATIVE.
     */
    @Test
    public void testDeleteArbitraryOnParquetPartitionConverts() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x from long_sequence(48)) " +
                    "timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            execute("alter table t convert partition to parquet list '1970-01-01'");
            drainWalQueue();
            // Deletes some rows WITHIN the parquet partition (arbitrary predicate) -> requires a rewrite.
            execute("DELETE FROM t WHERE x % 2 = 0 AND ts < '1970-01-02T00:00:00.000000Z'");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t");
            Assert.assertFalse(
                    "an arbitrary delete over a Parquet partition must convert-to-native and succeed, not suspend",
                    engine.getTableSequencerAPI().isSuspended(tt));
            // 11 even rows in day 1 deleted; 37 survive.
            assertQuery("select count(*) from t").noRandomAccess().expectSize().returns("count\n37\n");
            // no matching row survives.
            assertQuery("select count(*) from t where x % 2 = 0 and ts < '1970-01-02T00:00:00.000000Z'")
                    .noRandomAccess().expectSize().returns("count\n0\n");
            // the survivors in day 1 are exactly the odd x (1,3,..,23) = 12 rows, intact.
            assertQuery("select count(*) from t where ts < '1970-01-02T00:00:00.000000Z'")
                    .noRandomAccess().expectSize().returns("count\n12\n");
            // the Parquet partition was un-tiered to native by the fallback.
            assertQuery("select isParquet from table_partitions('t') where name = '1970-01-01'")
                    .noRandomAccess().returns("isParquet\nfalse\n");
        });
    }

    /**
     * Task 3.1 safety under a CALENDAR PARTITION GAP. Data lives on day 1970-01-01 (Parquet) and 1970-01-03
     * (native), with 1970-01-02 EMPTY, so the two physical partitions are calendar-non-adjacent. The convert
     * pre-pass bounds a partition's max data ts by the next PHYSICAL partition floor minus one, which under a
     * gap is looser than the replace path's calendar-aware ceiling - so {@code ts >= '1970-01-02T12:00'} (which
     * touches only day 3) makes the pre-pass eagerly convert the untouched Parquet day 1 to native as well
     * (a sound SUPERSET - see convertParquetPartitionsForDelete). This is wasteful but must stay CORRECT: day 1
     * is not in the delete range, so all 23 of its rows survive intact and only day 3 is removed. (This test
     * asserts the RESULT, not day 1's tier, so it survives a future exact-bound refinement.)
     */
    @Test
    public void testDeleteTimeRangeParquetWithCalendarGapStaysCorrect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
            // day 1 = x 1..23 (01:00..23:00 on 1970-01-01); day 2 left EMPTY; day 3 = x 49..71 (01:00..23:00 on 1970-01-03).
            execute("insert into t select (x*3600*1000000L)::timestamp, x from long_sequence(23)");
            execute("insert into t select ((x+48)*3600*1000000L)::timestamp, x+48 from long_sequence(23)");
            drainWalQueue();
            execute("alter table t convert partition to parquet list '1970-01-01'");
            drainWalQueue();
            // Deletes only day 3 (all ts >= 1970-01-03T01:00). day 1 is entirely below 1970-01-02T12:00.
            execute("DELETE FROM t WHERE ts >= '1970-01-02T12:00:00.000000Z'");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t");
            Assert.assertFalse(
                    "a time-range delete over a table with a partition gap must succeed, not suspend",
                    engine.getTableSequencerAPI().isSuspended(tt));
            // day 1 (23 rows) survives intact; day 3 (23 rows) removed.
            assertQuery("select count(*) from t").noRandomAccess().expectSize().returns("count\n23\n");
            assertQuery("select count(*) from t where ts >= '1970-01-02T12:00:00.000000Z'")
                    .noRandomAccess().expectSize().returns("count\n0\n");
            assertQuery("select min(ts), max(ts) from t").noRandomAccess().expectSize()
                    .returns("min\tmax\n1970-01-01T01:00:00.000000Z\t1970-01-01T23:00:00.000000Z\n");
        });
    }

    private void assertNotPureTimeRange(SqlCompiler compiler, String sql) throws SqlException {
        final CompiledQuery cc = compiler.compile(sql, sqlExecutionContext);
        Assert.assertEquals(CompiledQuery.DELETE, cc.getType());
        final DeleteOperation op = cc.getDeleteOperation();
        Assert.assertNotNull(op);
        Assert.assertFalse("must NOT be classified as a pure time range: " + sql, op.isPureTimeRange());
    }

    private void assertPureTimeRange(SqlCompiler compiler, String sql, long expectedLo, long expectedHiExcl) throws SqlException {
        final CompiledQuery cc = compiler.compile(sql, sqlExecutionContext);
        Assert.assertEquals(CompiledQuery.DELETE, cc.getType());
        final DeleteOperation op = cc.getDeleteOperation();
        Assert.assertNotNull(op);
        Assert.assertTrue("must be classified as a pure time range: " + sql, op.isPureTimeRange());
        Assert.assertEquals("deleted-interval lo for: " + sql, expectedLo, op.getTimeRangeLo());
        Assert.assertEquals("deleted-interval hiExcl for: " + sql, expectedHiExcl, op.getTimeRangeHiExcl());
    }
}
