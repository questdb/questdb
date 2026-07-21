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

package io.questdb.test.griffin;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.table.RuntimeConstGateRecordCursorFactory;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ANY;

/**
 * A WHERE predicate whose whole compiled filter is a runtime constant - a scalar boolean
 * sub-query used directly ({@code where (select b from x limit 1)}), its negation, or a bind
 * variable / {@code now()}-driven expression - is gated behind a single per-execution
 * evaluation. A false predicate returns an empty result without opening the outer base (no outer
 * I/O), and a true predicate delegates straight to the base. Compound predicates with a per-row
 * conjunct ({@code where a and (select b)}) keep the ordinary per-row async/serial filter.
 */
public class BooleanSubQueryRuntimeGateTest extends AbstractCairoTest {

    @Test
    public void testBindVarBooleanReEvaluatedPerExecution() throws Exception {
        // A single compiled gate factory must re-read the runtime constant on every open: a false
        // bind value yields no rows, a true bind value yields all rows, proving the value is never
        // baked at compile time.
        assertMemoryLeak(() -> {
            createTables();
            bindVariableService.setBoolean(0, true);
            try (RecordCursorFactory factory = select("select * from t where $1::boolean")) {
                bindVariableService.setBoolean(0, false);
                assertRowCount(0, factory);
                bindVariableService.setBoolean(0, true);
                assertRowCount(1, factory);
                bindVariableService.setBoolean(0, false);
                assertRowCount(0, factory);
            }
        });
    }

    @Test
    public void testBindVarEqualityReEvaluatedPerExecution() throws Exception {
        // A non-boolean-sub-query whole runtime constant ($1 = $2) also gates and must re-read the
        // bind values on every open: equal values match every row, unequal values match none.
        assertMemoryLeak(() -> {
            createTables();
            bindVariableService.setLong(0, 1);
            bindVariableService.setLong(1, 1);
            printSql("explain select * from t where $1 = $2");
            TestUtils.assertNotContains(sink, "Async Filter");
            TestUtils.assertContains(sink, "Filter");
            try (RecordCursorFactory factory = select("select * from t where $1 = $2")) {
                assertRowCount(1, factory);
                bindVariableService.setLong(1, 2);
                assertRowCount(0, factory);
                bindVariableService.setLong(1, 1);
                assertRowCount(1, factory);
            }
        });
    }

    @Test
    public void testCompoundNowPredicateStaysPerRow() throws Exception {
        // now() > const is a runtime constant, but ANDing it with a genuine per-row conjunct makes
        // the whole predicate non-constant, so the gate must NOT apply (guards against over-gating).
        assertMemoryLeak(() -> {
            createTables();
            printSql("explain select * from t where now() > '2000-01-01T00:00:00.000000Z' and v = 1");
            TestUtils.assertContains(sink, "Async Filter");
        });
    }

    @Test
    public void testCompoundPredicateStaysPerRow() throws Exception {
        // With a genuine per-row conjunct the whole filter is not runtime-constant, so the gate
        // must NOT apply and the async per-row filter is retained.
        assertMemoryLeak(() -> {
            createTables();
            printSql("explain select * from t where v = 1 and (select b from x_false limit 1)");
            TestUtils.assertContains(sink, "Async Filter");
            printSql("explain select * from t where v = 5 or (select b from x_true limit 1)");
            TestUtils.assertContains(sink, "Async Filter");
        });
    }

    @Test
    public void testFalseGateOverParquetClaimsOnlyPageFrameCursor() throws Exception {
        // read_parquet()'s page-frame cursor is a plain PageFrameCursor, not a TablePageFrameCursor.
        // The FALSE gate's empty wrapper must claim only what the base provides: over a parquet base
        // it must NOT advertise TablePageFrameCursor (its getTableReader()/hasIntervalFilter()/
        // toPartition() could only throw ClassCastException on the inner cast), while the plain
        // PageFrameCursor surface stays fully usable and empty.
        assertMemoryLeak(() -> {
            createTables();
            createParquetFile();
            try (RecordCursorFactory factory =
                         select("select * from read_parquet('p.parquet') where (select b from x_false limit 1)")) {
                RecordCursorFactory gate = findFactory(factory, RuntimeConstGateRecordCursorFactory.class);
                Assert.assertNotNull("runtime-const WHERE over read_parquet must gate", gate);
                Assert.assertTrue("gate must keep the parquet base page-frame capability", gate.supportsPageFrameCursor());
                try (PageFrameCursor cursor = gate.getPageFrameCursor(sqlExecutionContext, ORDER_ANY)) {
                    Assert.assertFalse(
                            "the empty wrapper must not claim TablePageFrameCursor over a non-table base",
                            cursor instanceof TablePageFrameCursor
                    );
                    Assert.assertTrue("the parquet base is external", cursor.isExternal());
                    Assert.assertNotNull("column mapping must delegate to the parquet cursor", cursor.getColumnMapping());
                    Assert.assertNull("false gate must yield no page frames", cursor.next());
                    Assert.assertEquals("false gate size must be zero", 0, cursor.size());
                    cursor.toTop();
                    Assert.assertNull("still empty after toTop", cursor.next());
                }
            }
        });
    }

    @Test
    public void testFalseGatePageFrameSurvivesTablePageFrameCursorCast() throws Exception {
        // A parent that unconditionally casts the base page-frame cursor to TablePageFrameCursor
        // (a SelectedRecordCursorFactory with a genuinely crossed projection casts at
        // SelectedRecordCursorFactory.getPageFrameCursor) must not hit a ClassCastException over
        // the FALSE gate. The duplicated column defeats projection pushdown, so the compiled
        // chain is Selected -> gate -> table scan - the production composition of this seam. The
        // wrapper over a TABLE base IS a TablePageFrameCursor (a non-table base gets a plain
        // wrapper instead), so the projected scan simply yields zero frames; the TRUE path
        // delegates the real table cursor through the same cast. assertMemoryLeak guards the
        // acquired reader.
        assertMemoryLeak(() -> {
            createBigTable();
            createTables();
            try (RecordCursorFactory factory =
                         select("select v, ts, v as v2 from big where (select b from x_false limit 1)")) {
                Assert.assertNotNull(
                        "the crossed projection must compile above the gate",
                        findFactory(factory, SelectedRecordCursorFactory.class));
                Assert.assertNotNull(
                        "the runtime-const WHERE must gate under the projection",
                        findFactory(factory, RuntimeConstGateRecordCursorFactory.class));
                Assert.assertTrue("projection must keep the base page-frame capability", factory.supportsPageFrameCursor());
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ANY)) {
                    Assert.assertNull("false gate must yield no page frames through the projection", cursor.next());
                    Assert.assertEquals("false gate size must be zero through the projection", 0, cursor.size());
                    cursor.toTop();
                    Assert.assertNull("still empty after toTop", cursor.next());
                }
            }
            // TRUE path: the same cast seam passes the real table cursor through and every row flows.
            try (
                    RecordCursorFactory factory =
                            select("select v, ts, v as v2 from big where (select b from x_true limit 1)");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ANY)
            ) {
                long rows = 0;
                PageFrame frame;
                while ((frame = cursor.next()) != null) {
                    rows += frame.getPartitionHi() - frame.getPartitionLo();
                }
                Assert.assertEquals("true gate must expose every base row through the projection", 1_000, rows);
            }
        });
    }

    @Test
    public void testFalseGatePageFrameYieldsEmptyNoScan() throws Exception {
        // The false page-frame path opens a real base cursor so metadata accessors honor their
        // contract, but yields ZERO frames so no column data is lifted; the acquired reader is
        // released on close (assertMemoryLeak guards the leak).
        assertMemoryLeak(() -> {
            createBigTable();
            RecordCursorFactory base = select("select * from big");
            RuntimeConstGateRecordCursorFactory gate =
                    new RuntimeConstGateRecordCursorFactory(base, new ConstBoolFilter(false));
            try {
                Assert.assertTrue("base must support page frames", base.supportsPageFrameCursor());
                Assert.assertTrue("gate must keep the base page-frame capability", gate.supportsPageFrameCursor());
                try (PageFrameCursor cursor = gate.getPageFrameCursor(sqlExecutionContext, ORDER_ANY)) {
                    Assert.assertNull("false gate must yield no page frames", cursor.next());
                    Assert.assertEquals("false gate size must be zero", 0, cursor.size());
                    Assert.assertTrue(cursor.supportsSizeCalculation());
                    RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(counter);
                    Assert.assertEquals("false gate must count zero rows", 0, counter.get());
                    Assert.assertEquals(0, cursor.getRemainingRowsInInterval());
                    // Metadata accessors delegate to the real base cursor and stay usable.
                    Assert.assertNotNull("column mapping must delegate to the base cursor", cursor.getColumnMapping());
                    cursor.toTop();
                    Assert.assertNull("still empty after toTop", cursor.next());
                }
            } finally {
                gate.close();
            }
        });
    }

    @Test
    public void testFalsePredicateDoesNotOpenOuterBase() throws Exception {
        // Deterministic outer-scan counter: with a false runtime constant the gate must return an
        // empty cursor WITHOUT calling getCursor() on the base factory.
        assertMemoryLeak(() -> {
            createTables();
            OpenCountingRecordCursorFactory base = new OpenCountingRecordCursorFactory(select("select * from t"));
            RuntimeConstGateRecordCursorFactory gate =
                    new RuntimeConstGateRecordCursorFactory(base, new ConstBoolFilter(false));
            try {
                try (RecordCursor cursor = gate.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse("false predicate must yield no rows", cursor.hasNext());
                }
                Assert.assertEquals("base must not be opened when the predicate is false", 0, base.openCount);
            } finally {
                gate.close();
            }
        });
    }

    @Test
    public void testGateOverParquetEndToEnd() throws Exception {
        // End-to-end both gate paths over a parquet base: FALSE yields no rows / a null aggregate
        // (driving the empty page-frame wrapper through the parallel group-by consumer), TRUE
        // delegates straight to the parquet scan.
        assertMemoryLeak(() -> {
            createTables();
            createParquetFile();
            assertRowCountSql(0, "select * from read_parquet('p.parquet') where (select b from x_false limit 1)");
            assertRowCountSql(10, "select * from read_parquet('p.parquet') where (select b from x_true limit 1)");
            printSql("select sum(v) from read_parquet('p.parquet') where (select b from x_false limit 1)");
            TestUtils.assertContains(sink, "null");
            printSql("select sum(v) from read_parquet('p.parquet') where (select b from x_true limit 1)");
            TestUtils.assertContains(sink, "55");
        });
    }

    @Test
    public void testJoinAsofConstSubQueryGates() throws Exception {
        // A runtime-const WHERE over an ASOF join gates the same way: false yields no rows without
        // scanning the join output, true delegates to it. The gate wraps the ASOF join factory.
        assertMemoryLeak(() -> {
            createAsofTables();
            createTables();
            try (RecordCursorFactory factory =
                         select("select * from a1 asof join a2 where (select b from x_false limit 1)")) {
                Assert.assertTrue(
                        "runtime-const WHERE over an ASOF join must gate",
                        hasFactory(factory, RuntimeConstGateRecordCursorFactory.class));
                Assert.assertFalse(
                        "the gated ASOF join must not carry a per-row post-join filter",
                        hasPostJoinFilter(factory));
            }
            assertRowCountSql(0, "select * from a1 asof join a2 where (select b from x_false limit 1)");
            // TRUE path: lock the 2-row projected content. Each a1 row picks up the single a2 row
            // (ts <= a1.ts). The output keeps a1.ts as the designated timestamp, in a1 order.
            assertQuery("select * from a1 asof join a2 where (select b from x_true limit 1)")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(
                            "ts\tk\tts1\tv\n" +
                                    "2020-01-01T00:00:00.000000Z\t1\t2020-01-01T00:00:00.000000Z\t100\n" +
                                    "2020-01-01T00:00:01.000000Z\t2\t2020-01-01T00:00:00.000000Z\t100\n"
                    );
        });
    }

    @Test
    public void testJoinBindVarWholePredicateGatesAndReEvaluates() throws Exception {
        // A $1 bind-variable whole predicate over a join is a runtime constant: it gates and must be
        // re-read on every open. A single compiled factory returns 0 rows when false and all rows
        // when true, proving the value is never baked at compile time.
        assertMemoryLeak(() -> {
            createJoinTables();
            bindVariableService.setBoolean(0, true);
            try (RecordCursorFactory factory =
                         select("select * from j1 join j2 on j1.k = j2.k where $1::boolean")) {
                Assert.assertTrue(
                        "bind-var whole predicate over a join must gate",
                        hasFactory(factory, RuntimeConstGateRecordCursorFactory.class));
                bindVariableService.setBoolean(0, false);
                assertRowCount(0, factory);
                bindVariableService.setBoolean(0, true);
                assertRowCount(2, factory);
                bindVariableService.setBoolean(0, false);
                assertRowCount(0, factory);
            }
        });
    }

    @Test
    public void testJoinConstWholeSubQueryGates() throws Exception {
        // A join whose whole constant WHERE is a runtime constant (boolean sub-query) is routed to
        // the last join model's postJoinWhereClause by SqlOptimiser.mergeConstIntoPostJoinWhereClause.
        // generateJoins now compiles that post-join filter once and, because the whole filter is a
        // runtime constant, gates the join output behind RuntimeConstGateRecordCursorFactory instead
        // of applying a serial/async per-row filter: false returns empty without scanning the join,
        // true delegates to it. This is the join analogue of the single-table generateFilter0 gate.
        assertMemoryLeak(() -> {
            createJoinTables();
            createTables();
            try (RecordCursorFactory factory =
                         select("select * from j1 join j2 on j1.k = j2.k where (select b from x_false limit 1)")) {
                Assert.assertTrue(
                        "runtime-const WHERE over a join must gate, not apply a per-row filter",
                        hasFactory(factory, RuntimeConstGateRecordCursorFactory.class));
                Assert.assertFalse(
                        "the gated join must not also carry a per-row post-join filter",
                        hasPostJoinFilter(factory));
            }
            assertRowCountSql(0, "select * from j1 join j2 on j1.k = j2.k where (select b from x_false limit 1)");
            assertRowCountSql(4, "select * from j1 cross join j2 where (select b from x_true limit 1)");
        });
    }

    @Test
    public void testJoinCrossConstSubQueryGates() throws Exception {
        // Same gate over a CROSS join: false yields no rows, true yields the full 2x2 product.
        assertMemoryLeak(() -> {
            createJoinTables();
            createTables();
            try (RecordCursorFactory factory =
                         select("select * from j1 cross join j2 where (select b from x_false limit 1)")) {
                Assert.assertTrue(
                        "runtime-const WHERE over a CROSS join must gate",
                        hasFactory(factory, RuntimeConstGateRecordCursorFactory.class));
            }
            assertRowCountSql(0, "select * from j1 cross join j2 where (select b from x_false limit 1)");
            assertRowCountSql(4, "select * from j1 cross join j2 where (select b from x_true limit 1)");
        });
    }

    @Test
    public void testJoinInnerConstSubQueryGates() throws Exception {
        // Same gate over an explicit INNER join.
        assertMemoryLeak(() -> {
            createJoinTables();
            createTables();
            try (RecordCursorFactory factory =
                         select("select * from j1 inner join j2 on j1.k = j2.k where (select b from x_false limit 1)")) {
                Assert.assertTrue(
                        "runtime-const WHERE over an INNER join must gate",
                        hasFactory(factory, RuntimeConstGateRecordCursorFactory.class));
            }
            assertRowCountSql(0, "select * from j1 inner join j2 on j1.k = j2.k where (select b from x_false limit 1)");
            assertRowCountSql(2, "select * from j1 inner join j2 on j1.k = j2.k where (select b from x_true limit 1)");
        });
    }

    @Test
    public void testJoinLeftOuterConstSubQueryGates() throws Exception {
        // Same gate over a LEFT OUTER join. j1 has a key (3) with no match in j2, which a LEFT join
        // keeps (NULL-extended). false yields no rows; true yields every left row (3 of them).
        assertMemoryLeak(() -> {
            execute("create table lj1 (k int, a int)");
            execute("insert into lj1 values (1, 10), (2, 20), (3, 30)");
            execute("create table lj2 (k int, b2 int)");
            execute("insert into lj2 values (1, 100), (2, 200)");
            createTables();
            try (RecordCursorFactory factory =
                         select("select * from lj1 left join lj2 on lj1.k = lj2.k where (select b from x_false limit 1)")) {
                Assert.assertTrue(
                        "runtime-const WHERE over a LEFT join must gate",
                        hasFactory(factory, RuntimeConstGateRecordCursorFactory.class));
            }
            assertRowCountSql(0, "select * from lj1 left join lj2 on lj1.k = lj2.k where (select b from x_false limit 1)");
            // TRUE path: lock the full projected content, including the NULL-extended row for the
            // unmatched left key (3). ORDER BY the left key makes the comparison deterministic.
            assertQuery("select * from lj1 left join lj2 on lj1.k = lj2.k where (select b from x_true limit 1) order by lj1.k")
                    .returns(
                            "k\ta\tk1\tb2\n" +
                                    "1\t10\t1\t100\n" +
                                    "2\t20\t2\t200\n" +
                                    "3\t30\tnull\tnull\n"
                    );
        });
    }

    @Test
    public void testJoinNowWholePredicateGates() throws Exception {
        // A now()-driven whole predicate over a join is a runtime constant and gates: a far-past
        // bound is always true (all rows), a far-future bound is false for the next century (none).
        assertMemoryLeak(() -> {
            createJoinTables();
            try (RecordCursorFactory factory =
                         select("select * from j1 join j2 on j1.k = j2.k where now() > '2000-01-01T00:00:00.000000Z'")) {
                Assert.assertTrue(
                        "now()-driven whole predicate over a join must gate",
                        hasFactory(factory, RuntimeConstGateRecordCursorFactory.class));
            }
            assertRowCountSql(2, "select * from j1 join j2 on j1.k = j2.k where now() > '2000-01-01T00:00:00.000000Z'");
            assertRowCountSql(0, "select * from j1 join j2 on j1.k = j2.k where now() > '2124-01-01T00:00:00.000000Z'");
        });
    }

    @Test
    public void testJoinPerRowPostJoinPredicateStaysNonGated() throws Exception {
        // A genuine per-row post-join predicate (referencing both sides) is NOT runtime-constant, so
        // it must keep the ordinary async/serial per-row filter and must NOT be gated (guards against
        // over-gating). j1.a + j2.b2 in {110, 210, 120, 220}; > 150 keeps 210 and 220 => 2 rows.
        assertMemoryLeak(() -> {
            createJoinTables();
            try (RecordCursorFactory factory =
                         select("select * from j1 cross join j2 where j1.a + j2.b2 > 150")) {
                Assert.assertFalse(
                        "a per-row post-join predicate must NOT be gated",
                        hasFactory(factory, RuntimeConstGateRecordCursorFactory.class));
                Assert.assertTrue(
                        "a per-row post-join predicate must keep the per-row filter",
                        hasPostJoinFilter(factory));
            }
            assertRowCountSql(2, "select * from j1 cross join j2 where j1.a + j2.b2 > 150");
        });
    }

    private static RecordCursorFactory findFactory(RecordCursorFactory f, Class<?> target) {
        while (f != null && !target.isInstance(f)) {
            f = f.getBaseFactory();
        }
        return f;
    }

    private static boolean hasFactory(RecordCursorFactory f, Class<?> target) {
        return findFactory(f, target) != null;
    }

    private static boolean hasPostJoinFilter(RecordCursorFactory f) {
        return hasFactory(f, io.questdb.griffin.engine.table.FilteredRecordCursorFactory.class)
                || hasFactory(f, io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory.class);
    }

    @Test
    public void testMultiRowSubQueryErrorPathDoesNotLeak() throws Exception {
        // The single-row cardinality check lives in the filter init the gate calls at open; the
        // error must propagate and leave nothing leaked (guarded by assertMemoryLeak).
        assertMemoryLeak(() -> {
            createTables();
            execute("create table x_multi (b boolean)");
            execute("insert into x_multi values (true), (false)");
            try (RecordCursorFactory factory = select("select * from t where (select b from x_multi)")) {
                try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("expected a cardinality error");
                } catch (Exception e) {
                    TestUtils.assertContains(e.getMessage(), "scalar sub-query returned more than one row");
                }
            }
        });
    }

    @Test
    public void testNowPredicateGates() throws Exception {
        // A now()-driven whole predicate is a runtime constant and gates: a far-past bound is
        // always true (all rows), a far-future bound is false for the next century (no rows).
        assertMemoryLeak(() -> {
            createTables();
            printSql("explain select * from t where now() > '2000-01-01T00:00:00.000000Z'");
            TestUtils.assertNotContains(sink, "Async Filter");
            TestUtils.assertContains(sink, "Filter");
            printSql("explain select * from t where now() > '2124-01-01T00:00:00.000000Z'");
            TestUtils.assertNotContains(sink, "Async Filter");
            TestUtils.assertContains(sink, "Filter");
            assertRowCountSql(1, "select * from t where now() > '2000-01-01T00:00:00.000000Z'");
            assertRowCountSql(0, "select * from t where now() > '2124-01-01T00:00:00.000000Z'");
        });
    }

    @Test
    public void testPlanGatesRuntimeConstantPredicate() throws Exception {
        // Before the fix this rendered "Async Filter workers: 1" over the full "Frame forward scan
        // on: t"; the gate renders an ordinary "Filter" over the base and never the async full scan.
        assertMemoryLeak(() -> {
            createTables();
            printSql("explain select * from t where (select b from x_false limit 1)");
            TestUtils.assertNotContains(sink, "Async Filter");
            TestUtils.assertContains(sink, "Filter");
            TestUtils.assertContains(sink, "filter: cursor");
            TestUtils.assertContains(sink, "Frame forward scan on: t");

            printSql("explain select * from t where not (select b from x_false limit 1)");
            TestUtils.assertNotContains(sink, "Async Filter");
            TestUtils.assertContains(sink, "not (cursor");
        });
    }

    @Test
    public void testTrueGatePageFrameDelegatesToBase() throws Exception {
        // supportsPageFrameCursor() reports the base's value and the true path hands out the base's
        // own page-frame cursor, so a parent parallel/vectorized operator keeps its full scan.
        assertMemoryLeak(() -> {
            createBigTable();
            RecordCursorFactory base = select("select * from big");
            RuntimeConstGateRecordCursorFactory gate =
                    new RuntimeConstGateRecordCursorFactory(base, new ConstBoolFilter(true));
            try {
                Assert.assertTrue("base must support page frames", base.supportsPageFrameCursor());
                Assert.assertTrue("gate must keep the base page-frame capability", gate.supportsPageFrameCursor());
                try (PageFrameCursor cursor = gate.getPageFrameCursor(sqlExecutionContext, ORDER_ANY)) {
                    // Sum rows straight off the delegated base frames (before any size() call, which
                    // advances the cursor to the end).
                    long rows = 0;
                    PageFrame frame;
                    while ((frame = cursor.next()) != null) {
                        rows += frame.getPartitionHi() - frame.getPartitionLo();
                    }
                    Assert.assertEquals("true gate must expose every base row via page frames", 1_000, rows);
                    cursor.toTop();
                    Assert.assertEquals(1_000, cursor.size());
                    cursor.toTop();
                    RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(counter);
                    Assert.assertEquals(1_000, counter.get());
                }
            } finally {
                gate.close();
            }
        });
    }

    @Test
    public void testTrueGatePreservesParallelAggregation() throws Exception {
        // End-to-end: a vectorized sum() over a page-frame table keeps the parallel page-frame path
        // ("GroupBy vectorized: true") ONLY when its base reports supportsPageFrameCursor(). The gate
        // now reports the base's value, so a true whole-runtime-constant WHERE preserves the
        // vectorized aggregation and drives the gate's true page-frame path: the plan is a fully
        // parallel "Async Group By ... vectorized: true" over the gate. Before sub-task 1 the gate
        // reported false and forced a serial GroupBy. The false path exercises the empty page-frame
        // wrapper end-to-end (reader acquired then released) under assertMemoryLeak.
        assertMemoryLeak(() -> {
            createBigTable();
            createTables();
            printSql("explain select sum(v) from big where (select b from x_true limit 1)");
            TestUtils.assertContains(sink, "Async Group By");
            TestUtils.assertContains(sink, "vectorized: true");
            TestUtils.assertContains(sink, "Frame forward scan on: big");
            TestUtils.assertNotContains(sink, "Async Filter");
            // true path: sum of 1..1000
            printSql("select sum(v) from big where (select b from x_true limit 1)");
            TestUtils.assertContains(sink, "500500");
            // false path: the vectorized aggregate drives the empty page-frame wrapper; one row, null
            printSql("select sum(v) from big where (select b from x_false limit 1)");
            TestUtils.assertContains(sink, "null");
            assertRowCountSql(1, "select sum(v) from big where (select b from x_false limit 1)");
        });
    }

    @Test
    public void testTruePredicateDelegatesToBase() throws Exception {
        // With a true runtime constant the gate delegates straight to the base cursor once, and
        // every base row flows through.
        assertMemoryLeak(() -> {
            createTables();
            OpenCountingRecordCursorFactory base = new OpenCountingRecordCursorFactory(select("select * from t"));
            RuntimeConstGateRecordCursorFactory gate =
                    new RuntimeConstGateRecordCursorFactory(base, new ConstBoolFilter(true));
            try {
                int rows = 0;
                try (RecordCursor cursor = gate.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        rows++;
                    }
                }
                Assert.assertEquals("true predicate must pass every base row", 1, rows);
                Assert.assertEquals("base must be opened exactly once when the predicate is true", 1, base.openCount);
            } finally {
                gate.close();
            }
        });
    }

    private static void assertRowCount(int expected, RecordCursorFactory factory) throws Exception {
        int rows = 0;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            while (cursor.hasNext()) {
                rows++;
            }
        }
        Assert.assertEquals(expected, rows);
    }

    private static void assertRowCountSql(int expected, String sql) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            assertRowCount(expected, factory);
        }
    }

    private void createBigTable() throws Exception {
        execute("create table big (ts timestamp, v int) timestamp(ts) partition by day");
        execute("insert into big select (x * 1_000_000)::timestamp, x::int from long_sequence(1_000)");
    }

    private void createAsofTables() throws Exception {
        execute("create table a1 (ts timestamp, k int) timestamp(ts) partition by day");
        execute("insert into a1 values ('2020-01-01T00:00:00.000000Z', 1), ('2020-01-01T00:00:01.000000Z', 2)");
        execute("create table a2 (ts timestamp, v int) timestamp(ts) partition by day");
        execute("insert into a2 values ('2020-01-01T00:00:00.000000Z', 100)");
    }

    private void createParquetFile() throws Exception {
        execute("create table p as (select x::int v, (x * 1_000_000)::timestamp ts from long_sequence(10))");
        try (
                Path path = new Path();
                PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader("p")
        ) {
            path.of(root).concat("p.parquet");
            PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
            PartitionEncoder.encode(partitionDescriptor, path);
            Assert.assertTrue(Files.exists(path.$()));
        }
        inputRoot = root;
    }

    private void createJoinTables() throws Exception {
        execute("create table j1 (k int, a int)");
        execute("insert into j1 values (1, 10), (2, 20)");
        execute("create table j2 (k int, b2 int)");
        execute("insert into j2 values (1, 100), (2, 200)");
    }

    private void createTables() throws Exception {
        execute("create table t (ts timestamp, v int, sym symbol index) timestamp(ts) partition by day");
        execute("insert into t values ('2018-01-01T00:00:00.000000Z', 1, 'a')");
        execute("create table x_false (b boolean)");
        execute("insert into x_false values (false)");
        execute("create table x_true (b boolean)");
        execute("insert into x_true values (true)");
    }

    private static final class ConstBoolFilter extends BooleanFunction {
        private final boolean value;

        private ConstBoolFilter(boolean value) {
            this.value = value;
        }

        @Override
        public boolean getBool(Record rec) {
            return value;
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
        }
    }

    private static final class OpenCountingRecordCursorFactory extends AbstractRecordCursorFactory {
        private final RecordCursorFactory base;
        int openCount;

        private OpenCountingRecordCursorFactory(RecordCursorFactory base) {
            super(base.getMetadata());
            this.base = base;
        }

        @Override
        public RecordCursorFactory getBaseFactory() {
            return base;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws io.questdb.griffin.SqlException {
            openCount++;
            return base.getCursor(executionContext);
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return base.recordCursorSupportsRandomAccess();
        }

        @Override
        protected void _close() {
            base.close();
        }
    }
}
