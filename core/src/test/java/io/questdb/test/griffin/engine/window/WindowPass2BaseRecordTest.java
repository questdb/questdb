/*******************************************************************************
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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins {@link WindowFunction#pass2NeedsBaseRecord()} for the selector window functions whose
 * pass2 never reads the base {@code Record}: uniform(), cadence(), and sdt() (both
 * unpartitioned and partitioned).
 * <p>
 * The cached executor ({@code CachedWindowLightRecordCursorFactory}) precomputes a per-group
 * need flag from this method and skips the per-row random-access base re-read
 * ({@code positionRecordABaseOnly} -&gt; {@code baseCursor.recordAt}) in its pass2 loop when
 * every two-pass function in the group opts out. All three selector pass2 bodies drive
 * entirely off pass1-cached state (uniform/cadence read their {@code selected} ordinal list;
 * both sdt variants read their own keep-byte buffer in the retained traversal order pass1
 * appended it) and write output through the
 * {@link io.questdb.cairo.sql.WindowSPI}, so inheriting the {@code true} default buys nothing
 * and costs one base positioning per input row per query. BucketSelectWindowFunction
 * (minmax/m4/lttb) already opts out for exactly this reason.
 * <p>
 * The mixed-query tests preserve the caller's per-group OR semantics: a two-pass function
 * that DOES read the record in pass2 (partitioned avg re-reads the partition key columns)
 * must keep {@code pass2NeedsBaseRecord() == true} and force positioning for its group, and
 * mixed-query outputs must stay correct.
 */
public class WindowPass2BaseRecordTest extends AbstractCairoTest {

    private static final String DDL = "create table t (ts timestamp, sym symbol, v double) timestamp(ts)";
    private static final String INSERT = """
            insert into t values
            (1::timestamp,'a',1.0),
            (2::timestamp,'b',2.0),
            (3::timestamp,'a',3.0),
            (4::timestamp,'b',4.0),
            (5::timestamp,'a',5.0),
            (6::timestamp,'b',6.0)""";

    @Test
    public void testCadenceMixedWithRecordReadingPass2FunctionStaysCorrect() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            String query = "select ts, sym, v, cadence(2) over (order by ts) keep, avg(v) over (partition by sym) a from t";
            // cadence(2) over 6 rows keeps ordinals {0, 2, 4} plus the pinned last row 5.
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tv\tkeep\ta
                            1970-01-01T00:00:00.000001Z\ta\t1.0\ttrue\t3.0
                            1970-01-01T00:00:00.000002Z\tb\t2.0\tfalse\t4.0
                            1970-01-01T00:00:00.000003Z\ta\t3.0\ttrue\t3.0
                            1970-01-01T00:00:00.000004Z\tb\t4.0\tfalse\t4.0
                            1970-01-01T00:00:00.000005Z\ta\t5.0\ttrue\t3.0
                            1970-01-01T00:00:00.000006Z\tb\t6.0\ttrue\t4.0
                            """);
            assertNamedFunctionNeedsBaseRecord(query, "avg", true);
        });
    }

    @Test
    public void testCadencePass2DoesNotNeedBaseRecord() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            assertNamedFunctionNeedsBaseRecord(
                    "select ts, v, cadence(2) over (order by ts) keep from t",
                    "cadence",
                    false
            );
        });
    }

    @Test
    public void testSdtMixedWithRecordReadingPass2FunctionStaysCorrect() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            String query = "select ts, sym, v, sdt(ts, v, 0.5) over (order by ts) keep, avg(v) over (partition by sym) a from t";
            // v is a clean ramp: sdt keeps only the endpoints.
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tv\tkeep\ta
                            1970-01-01T00:00:00.000001Z\ta\t1.0\ttrue\t3.0
                            1970-01-01T00:00:00.000002Z\tb\t2.0\tfalse\t4.0
                            1970-01-01T00:00:00.000003Z\ta\t3.0\tfalse\t3.0
                            1970-01-01T00:00:00.000004Z\tb\t4.0\tfalse\t4.0
                            1970-01-01T00:00:00.000005Z\ta\t5.0\tfalse\t3.0
                            1970-01-01T00:00:00.000006Z\tb\t6.0\ttrue\t4.0
                            """);
            assertNamedFunctionNeedsBaseRecord(query, "avg", true);
        });
    }

    @Test
    public void testSdtPartitionedPass2DoesNotNeedBaseRecord() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            assertNamedFunctionNeedsBaseRecord(
                    "select ts, sym, v, sdt(ts, v, 0.5) over (partition by sym order by ts) keep from t",
                    "sdt",
                    false
            );
        });
    }

    @Test
    public void testSdtUnpartitionedPass2DoesNotNeedBaseRecord() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            assertNamedFunctionNeedsBaseRecord(
                    "select ts, v, sdt(ts, v, 0.5) over (order by ts) keep from t",
                    "sdt",
                    false
            );
        });
    }

    @Test
    public void testUniformMixedWithRecordReadingPass2FunctionStaysCorrect() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            String query = "select ts, sym, v, uniform(3) over (order by ts) keep, avg(v) over (partition by sym) a from t";
            // uniform(3) over 6 rows keeps ordinals {0, 3, 5}: (i*5 + 1) / 2 for i in 0..2.
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tv\tkeep\ta
                            1970-01-01T00:00:00.000001Z\ta\t1.0\ttrue\t3.0
                            1970-01-01T00:00:00.000002Z\tb\t2.0\tfalse\t4.0
                            1970-01-01T00:00:00.000003Z\ta\t3.0\tfalse\t3.0
                            1970-01-01T00:00:00.000004Z\tb\t4.0\ttrue\t4.0
                            1970-01-01T00:00:00.000005Z\ta\t5.0\tfalse\t3.0
                            1970-01-01T00:00:00.000006Z\tb\t6.0\ttrue\t4.0
                            """);
            assertNamedFunctionNeedsBaseRecord(query, "avg", true);
        });
    }

    @Test
    public void testUniformPass2DoesNotNeedBaseRecord() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            assertNamedFunctionNeedsBaseRecord(
                    "select ts, v, uniform(3) over (order by ts) keep from t",
                    "uniform",
                    false
            );
        });
    }

    // Asserts pass2NeedsBaseRecord() of the window function named `functionName` inside the
    // cached window factory that the query compiles to. This is the deterministic work
    // observation for the pass2 base-record skip: the cached executor's pass2 loop consults
    // exactly this flag (via its precomputed per-group need flag) to decide whether to issue a
    // baseCursor.recordAt() per input row.
    private static void assertNamedFunctionNeedsBaseRecord(
            String query,
            String functionName,
            boolean expectedNeedsBaseRecord
    ) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            ObjList<WindowFunction> functions = findCachedWindowFunctions(factory);
            Assert.assertNotNull(
                    "expected a cached window factory in the plan tree for: " + query,
                    functions
            );
            WindowFunction match = null;
            for (int i = 0, n = functions.size(); i < n; i++) {
                if (functionName.equals(functions.getQuick(i).getName())) {
                    Assert.assertNull("more than one '" + functionName + "' window function", match);
                    match = functions.getQuick(i);
                }
            }
            Assert.assertNotNull("no window function named '" + functionName + "' in: " + query, match);
            Assert.assertEquals(
                    functionName + "().pass2NeedsBaseRecord()",
                    expectedNeedsBaseRecord,
                    match.pass2NeedsBaseRecord()
            );
        }
    }

    private static ObjList<WindowFunction> findCachedWindowFunctions(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof CachedWindowLightRecordCursorFactory lightFactory) {
                return lightFactory.getAllWindowFunctions();
            }
            if (f instanceof CachedWindowRecordCursorFactory cachedFactory) {
                return cachedFactory.getAllWindowFunctions();
            }
        }
        return null;
    }
}
