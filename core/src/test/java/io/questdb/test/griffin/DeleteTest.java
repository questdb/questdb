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

import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
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
}
