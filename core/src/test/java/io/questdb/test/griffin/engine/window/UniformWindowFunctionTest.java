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

package io.questdb.test.griffin.engine.window;

import io.questdb.cairo.sql.BindVariableService;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class UniformWindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testBindVariableTarget() throws Exception {
        // uniform(target) accepts a runtime-constant (bind-variable) target, read PER-EXECUTION:
        // the SAME compiled factory produces target-3 vs keep-all sets as $1 is re-bound between
        // executions, and a runtime out-of-range target throws at cursor-open (not compile).
        final ObjList<BindVarTuple> cases = new ObjList<>();
        // $1 = 3 over 5 rows: byte-identical to the constant uniform(3) case (testEvenlySpacedSelection).
        cases.add(BindVarTuple.ok(
                "target 3",
                """
                        ts\tv\tkeep
                        1970-01-01T00:00:00.000001Z\t1.0\ttrue
                        1970-01-01T00:00:00.000002Z\t2.0\tfalse
                        1970-01-01T00:00:00.000003Z\t3.0\ttrue
                        1970-01-01T00:00:00.000004Z\t4.0\tfalse
                        1970-01-01T00:00:00.000005Z\t5.0\ttrue
                        """,
                bindVariableService -> bindVariableService.setLong(0, 3)
        ));
        // Re-bind $1 = 5 on the same compiled factory: target >= rows -> keep all. A different result
        // from the target-3 case above proves the target is read at execution, not frozen at compile.
        cases.add(BindVarTuple.ok(
                "target 5 (re-bind, keep all)",
                """
                        ts\tv\tkeep
                        1970-01-01T00:00:00.000001Z\t1.0\ttrue
                        1970-01-01T00:00:00.000002Z\t2.0\ttrue
                        1970-01-01T00:00:00.000003Z\t3.0\ttrue
                        1970-01-01T00:00:00.000004Z\t4.0\ttrue
                        1970-01-01T00:00:00.000005Z\t5.0\ttrue
                        """,
                bindVariableService -> bindVariableService.setLong(0, 5)
        ));
        // Runtime validation mirrors SUBSAMPLE's legacy cursor (including distinct NULL/range errors).
        cases.add(BindVarTuple.fails(
                "target 1 (runtime below minimum)",
                22,
                "target points must be at least 2",
                bindVariableService -> bindVariableService.setLong(0, 1)
        ));
        cases.add(BindVarTuple.fails(
                "target above int maximum",
                22,
                "target points exceeds maximum of 2147483647",
                bindVariableService -> bindVariableService.setLong(0, (long) Integer.MAX_VALUE + 1)
        ));
        cases.add(BindVarTuple.fails(
                "target unset",
                22,
                "target point count must be set",
                bindVariableService -> bindVariableService.setLong(0, Numbers.LONG_NULL)
        ));

        assertQuery("select ts, v, uniform($1) over (order by ts) keep from t")
                .ddl("create table t (ts timestamp, v double) timestamp(ts)",
                        "insert into t select x::timestamp, x from long_sequence(5)")
                .timestamp("ts")
                .expectSize()
                .assertBinds(cases);
    }

    @Test
    public void testKeepsAllWhenTargetGteRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(3)");
            // target 5 >= 3 rows -> keep all
            assertQuery("select ts, v, uniform(5) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t1.0\ttrue
                            1970-01-01T00:00:00.000002Z\t2.0\ttrue
                            1970-01-01T00:00:00.000003Z\t3.0\ttrue
                            """);
        });
    }

    @Test
    public void testEvenlySpacedSelection() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(5)");
            // n=5, N=3: divisor=2, range=4, half=1. pos(i)=(i*4+1)/2 -> 0, 2, 4 (0-based) => rows 1,3,5
            assertQuery("select ts, v, uniform(3) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t1.0\ttrue
                            1970-01-01T00:00:00.000002Z\t2.0\tfalse
                            1970-01-01T00:00:00.000003Z\t3.0\ttrue
                            1970-01-01T00:00:00.000004Z\t4.0\tfalse
                            1970-01-01T00:00:00.000005Z\t5.0\ttrue
                            """);
        });
    }

    @Test
    public void testFilterYieldsReducedSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(5)");
            assertQuery("select ts, v from (select ts, v, uniform(3) over (order by ts) keep from t) where keep")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize(false)
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000001Z\t1.0
                            1970-01-01T00:00:00.000003Z\t3.0
                            1970-01-01T00:00:00.000005Z\t5.0
                            """);
        });
    }

    @Test
    public void testTargetOneRejectedLikeRuntimeBind() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, uniform(1) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(19, "target points must be at least 2");
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, uniform(3) over (order by ts) from t")
                    .noLeakCheck()
                    .assertsPlan("CachedWindowLight\n" +
                            """
                                      unorderedFunctions: [uniform(3) over (order by [ts])]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    """);
        });
    }

    @Test
    public void testConstantTargetOutOfRangeFailsAtCompileTime() throws Exception {
        // Fix 2: a constant target's range is validated at newInstance (compile time), matching the
        // pre-bind-var-support factory and the legacy SUBSAMPLE cursor's own constant handling - not
        // deferred to cursor-open. select(...) below only compiles the query (it never calls
        // factory.getCursor(...)), so a thrown SqlException here proves the failure happened during
        // compilation, not execution.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            try {
                select("select ts, uniform(0) over (order by ts) from t");
                Assert.fail("expected compilation to fail for an out-of-range constant target");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "target points must be at least 2");
                Assert.assertEquals(19, e.getPosition());
            }
            assertQuery("select ts, uniform(2147483648) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(19, "target points exceeds maximum of 2147483647");
            assertQuery("select ts, uniform(null::long) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(23, "target point count must be set");
        });
    }

    @Test
    public void testBindVariableWrongTypeRejectedWithFriendlyMessage() throws Exception {
        // A bind variable already bound to a non-numeric type before this query compiles (e.g. reused
        // across differently-shaped statements) must be rejected with SUBSAMPLE's friendly "integer
        // expected for target point count" message, not a generic "no matching function" overload
        // error or an uncaught cast failure surfacing later inside init()/pass1.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
            bindVariableService.clear();
            bindVariableService.setStr(0, "abc");
            try {
                select("select ts, uniform($1) over (order by ts) from t");
                Assert.fail("expected compilation to fail for a non-numeric bind-variable target");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "integer expected for target point count");
                Assert.assertEquals(19, e.getPosition());
            }
        });
    }

    @Test
    public void testRejectsNonConstantTarget() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, uniform(v::long) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(20, "target must be a constant");
        });
    }
}
