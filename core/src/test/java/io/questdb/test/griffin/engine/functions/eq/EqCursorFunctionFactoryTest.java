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

package io.questdb.test.griffin.engine.functions.eq;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.test.TestTimestampCounterFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@code col = (sub-query)} and {@code col != (sub-query)} operators, where the
 * right-hand side is a single-column scalar sub-query (cursor) executed once per query execution.
 * The {@code !=} / {@code <>} forms and the argument-swapped {@code (sub-query) = col} forms are
 * derived automatically from the {@code =} factories by {@code FunctionFactoryCache}.
 *
 * @see io.questdb.griffin.engine.functions.eq.EqDoubleCursorFunctionFactory
 * @see io.questdb.griffin.engine.functions.eq.EqIntCursorFunctionFactory
 * @see io.questdb.griffin.engine.functions.eq.EqLongCursorFunctionFactory
 */
public class EqCursorFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBareNullLeftCursorComparison() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x::int i FROM long_sequence(3))");

            assertQuery("SELECT null = (SELECT max(i) FROM t) eq")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
            assertQuery("SELECT null != (SELECT max(i) FROM t) ne")
                    .noLeakCheck()
                    .expectSize()
                    .returns("ne\ntrue\n");
            assertQuery("SELECT null <> (SELECT max(i) FROM t) ne")
                    .noLeakCheck()
                    .expectSize()
                    .returns("ne\ntrue\n");
            assertQuery("SELECT (SELECT max(i) FROM t) = null eq")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
            assertQuery("SELECT (SELECT max(i) FROM t) != null ne")
                    .noLeakCheck()
                    .expectSize()
                    .returns("ne\ntrue\n");
            assertQuery("SELECT (SELECT max(i) FROM t) <> null ne")
                    .noLeakCheck()
                    .expectSize()
                    .returns("ne\ntrue\n");

            for (String value : new String[]{
                    "1::byte", "1::short", "1", "1L", "1::float", "1.0", "1::timestamp", "1::timestamp_ns"
            }) {
                assertQuery("SELECT null = (SELECT " + value + ") eq")
                        .noLeakCheck()
                        .expectSize()
                        .returns("eq\nfalse\n");
            }

            assertQuery("SELECT null = (SELECT null) eq")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\n");
            assertQuery("SELECT null != (SELECT null) ne")
                    .noLeakCheck()
                    .expectSize()
                    .returns("ne\nfalse\n");
            assertQuery("SELECT null = (SELECT i FROM t WHERE 1 <> 1) eq")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\n");
            assertQuery("SELECT null != (SELECT i FROM t WHERE 1 <> 1) ne")
                    .noLeakCheck()
                    .expectSize()
                    .returns("ne\nfalse\n");
        });
    }

    @Test
    public void testBareNullLiteralDoesNotBindCursor() throws Exception {
        // A bare `null` literal is a scalar, never a cursor: `col = null` / `col != null` must
        // resolve to the scalar null-comparison factories (never the `=(?C)` cursor factory) and
        // must not throw at compile time. long_sequence yields no nulls, so `= null` matches
        // nothing and `!= null` matches every row.
        assertMemoryLeak(() -> {
            for (String type : new String[]{"double", "int", "long", "short", "byte"}) {
                execute("create table t as (select x::" + type + " c from long_sequence(5))");
                assertQuery("select c from t where c = null")
                        .noLeakCheck()
                        .returns("c\n");
                assertQuery("select count() n from t where c != null")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("n\n5\n");
                execute("drop table t");
            }
        });
    }

    @Test
    public void testCursorOnLeftViaArgSwap() throws Exception {
        // `(select ...) = col` binds to the argument-swapped overload (=(CI)/=(CL)/=(CD)) that
        // FunctionFactoryCache derives because the two argument types differ. The swap normalizes
        // the cursor back to the right, so results match the col-on-left form.
        assertMemoryLeak(() -> {
            execute("create table i as (select x::int i from long_sequence(5))");
            assertQuery("select i from i where (select max(i) from i) = i")
                    .noLeakCheck()
                    .returns("i\n5\n");
            execute("create table l as (select x::long l from long_sequence(5))");
            assertQuery("select l from l where (select min(l) from l) = l")
                    .noLeakCheck()
                    .returns("l\n1\n");
            execute("create table d as (select x::double d from long_sequence(5))");
            assertQuery("select d from d where (select max(d) from d) = d")
                    .noLeakCheck()
                    .returns("d\n5.0\n");
        });
    }

    @Test
    public void testEqAndNotEqDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double d from long_sequence(10))");
            // max(d) = 10.0
            assertQuery("select d from t where d = (select max(d) from t)")
                    .noLeakCheck()
                    .returns("d\n10.0\n");
            // != is derived automatically from the = factory
            assertQuery("select d from t where d != (select max(d) from t)")
                    .noLeakCheck()
                    .returns("d\n1.0\n2.0\n3.0\n4.0\n5.0\n6.0\n7.0\n8.0\n9.0\n");
            // <> is the same operator as !=
            assertQuery("select count() n from t where d <> (select max(d) from t)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("n\n9\n");
        });
    }

    @Test
    public void testEqAndNotEqInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            assertQuery("select i from t where i = (select max(i) from t)")
                    .noLeakCheck()
                    .returns("i\n10\n");
            assertQuery("select i from t where i != (select max(i) from t)")
                    .noLeakCheck()
                    .returns("i\n1\n2\n3\n4\n5\n6\n7\n8\n9\n");
        });
    }

    @Test
    public void testEqAndNotEqLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            assertQuery("select l from t where l = (select min(l) from t)")
                    .noLeakCheck()
                    .returns("l\n1\n");
            assertQuery("select l from t where l <> (select min(l) from t)")
                    .noLeakCheck()
                    .returns("l\n2\n3\n4\n5\n6\n7\n8\n9\n10\n");
        });
    }

    @Test
    public void testEqByteAndShortLeftOperands() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table s as (select x::short sh, x::byte b from long_sequence(5))");
            // short left, int cursor scalar (3) -> long comparison mode
            assertQuery("select sh from s where sh = (select 3)")
                    .noLeakCheck()
                    .returns("sh\n3\n");
            // byte left, int cursor scalar (3) -> long comparison mode
            assertQuery("select b from s where b != (select 3)")
                    .noLeakCheck()
                    .returns("b\n1\n2\n4\n5\n");
        });
    }

    @Test
    public void testEqFloatLeftOperand() throws Exception {
        // a FLOAT left operand routes to =(DC) (widened to DOUBLE up front); the comparison stays
        // exact for these small integral values
        assertMemoryLeak(() -> {
            execute("create table t as (select x::float price from long_sequence(10))");
            assertQuery("select price from t where price = (select 5)")
                    .noLeakCheck()
                    .returns("price\n5.0\n");
            assertQuery("select price from t where price != (select max(price) from t)")
                    .noLeakCheck()
                    .returns("price\n1.0\n2.0\n3.0\n4.0\n5.0\n6.0\n7.0\n8.0\n9.0\n");
        });
    }

    @Test
    public void testEqDoubleWithIntCursorScalar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::double d from long_sequence(10))");
            // an INT cursor scalar (max(i) = 5) reads via the double comparison mode
            assertQuery("select d from t where d = (select 5)")
                    .noLeakCheck()
                    .returns("d\n5.0\n");
        });
    }

    @Test
    public void testEqIntWithDoubleCursorScalar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // avg(i) = 5.5 -> double comparison mode; no int equals 5.5
            assertQuery("select i from t where i = (select avg(i) from t)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select count() n from t where i != (select avg(i) from t)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("n\n10\n");
            // an integral double scalar (6.0) still matches the equal int
            assertQuery("select i from t where i = (select max(i) / 2.0 + 1 from t)")
                    .noLeakCheck()
                    .returns("i\n6\n");
        });
    }

    @Test
    public void testEqIntWithLongCursorDoesNotNarrow() throws Exception {
        assertMemoryLeak(() -> {
            // int values near INT_MAX; the cursor scalar 5_000_000_000 overflows the int range
            execute("create table t as (select (2000000000 + x)::int i from long_sequence(3))");
            // none of the ints equals 5_000_000_000. If the long scalar were narrowed to int
            // (705032704) it could spuriously match, so this pins the long comparison mode.
            assertQuery("select i from t where i = (select 5000000000)")
                    .noLeakCheck()
                    .returns("i\n");
            // and every row is != the out-of-range scalar
            assertQuery("select count() n from t where i != (select 5000000000)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("n\n3\n");
        });
    }

    @Test
    public void testEqLongExactBeyondDoublePrecision() throws Exception {
        assertMemoryLeak(() -> {
            // 2^53 and 2^53 + 1 are indistinguishable as double but distinct as long; a long vs a
            // long cursor scalar compares exactly, so only the equal value matches.
            execute("create table t as (select 9007199254740993L l)"); // 2^53 + 1
            assertQuery("select l from t where l = (select 9007199254740993L)")
                    .noLeakCheck()
                    .returns("l\n9007199254740993\n");
            assertQuery("select l from t where l = (select 9007199254740992L)") // 2^53
                    .noLeakCheck()
                    .returns("l\n");
        });
    }

    @Test
    public void testEqTypedNumericCursorScalars() throws Exception {
        // pins the BYTE/SHORT/FLOAT/DOUBLE readers of the cursor scalar
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            assertQuery("select i from t where i = (select 5::byte)")
                    .noLeakCheck()
                    .returns("i\n5\n");
            assertQuery("select i from t where i = (select 6::short)")
                    .noLeakCheck()
                    .returns("i\n6\n");
            assertQuery("select i from t where i = (select 7::float)")
                    .noLeakCheck()
                    .returns("i\n7\n");
            assertQuery("select i from t where i = (select 8::double)")
                    .noLeakCheck()
                    .returns("i\n8\n");
        });
    }

    @Test
    public void testErrorMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i, x::long l, x::double d from long_sequence(3))");
            assertQuery("select i from t where i = (select max(i), 1 x from t)")
                    .fails(27, "select must provide exactly one column");
            assertQuery("select l from t where l = (select max(l), 1 x from t)")
                    .fails(27, "select must provide exactly one column");
            assertQuery("select d from t where d = (select max(d), 1 x from t)")
                    .fails(27, "select must provide exactly one column");
        });
    }

    @Test
    public void testErrorNonNumericCursorColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i, x::long l, x::double d from long_sequence(3))");
            assertQuery("select i from t where i = (select 'abc' from t)")
                    .fails(27, "cannot compare INT and STRING");
            assertQuery("select l from t where l = (select 'abc' from t)")
                    .fails(27, "cannot compare LONG and STRING");
            assertQuery("select d from t where d = (select 'abc' from t)")
                    .fails(27, "cannot compare DOUBLE and STRING");
        });
    }

    @Test
    public void testErrorUnsupportedLeftOperand() throws Exception {
        // A VARCHAR/STRING left operand re-routes to =(DC), whose guard rejects a non-DOUBLE/FLOAT
        // left operand (mirroring the < / > numeric cursor routing). The != form goes through the
        // same guard.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::varchar v, x::string s from long_sequence(3))");
            assertQuery("select v from t where v = (select 1.0)")
                    .fails(22, "left operand must be a DOUBLE or FLOAT, found: VARCHAR");
            assertQuery("select s from t where s != (select 1.0)")
                    .fails(22, "left operand must be a DOUBLE or FLOAT, found: STRING");
        });
    }

    @Test
    public void testMultiRowCursorFails() throws Exception {
        // A scalar sub-query yielding more than one row is an error, reported at the sub-query
        // position. The INT/LONG/DOUBLE cursor reader arms are separate, so each is asserted.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i, x::long l, x::double d from long_sequence(3))");
            assertQuery("select i from t where i = (select x::int from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            assertQuery("select l from t where l = (select x from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            assertQuery("select d from t where d = (select x::double from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            // the != form shares the same cold path, so it must reject multi-row too
            assertQuery("select i from t where i != (select x::int from long_sequence(2))")
                    .fails(28, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testNullAndEmptyCursor() throws Exception {
        assertMemoryLeak(() -> {
            for (String type : new String[]{"double", "int", "long", "short", "byte"}) {
                execute("create table t as (select x::" + type + " c from long_sequence(10))");
                final String empty = "c\n";
                // a null / empty cursor scalar is null: no non-null left value equals it, so `=`
                // matches nothing and `!=` matches every row (10)
                assertQuery("select c from t where c = (select null::long)")
                        .noLeakCheck()
                        .returns(empty);
                assertQuery("select c from t where c = (select max(c) from t where 1 <> 1)")
                        .noLeakCheck()
                        .returns(empty);
                assertQuery("select count() n from t where c != (select null::long)")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("n\n10\n");
                execute("drop table t");
            }
        });
    }

    @Test
    public void testNullLeftColumnFollowsNullEqualsNull() throws Exception {
        // long_sequence never yields null cells, so use an explicit null row. A null left value
        // equals a null cursor scalar (QuestDB's null == null), and never equals a non-null one.
        assertMemoryLeak(() -> {
            execute("create table t (id int, v long)");
            execute("insert into t values (1, null), (2, 5), (3, 8)");
            // null-left (id 1) never equals a non-null cursor scalar
            assertQuery("select id from t where v = (select max(v) from t)") // = 8
                    .noLeakCheck()
                    .returns("id\n3\n");
            // null == null: a null left value matches a null cursor scalar for =, and is excluded by !=
            assertQuery("select id from t where v = (select null)")
                    .noLeakCheck()
                    .returns("id\n1\n");
            assertQuery("select id from t where v != (select null)")
                    .noLeakCheck()
                    .returns("id\n2\n3\n");
        });
    }

    @Test
    public void testPlanRendersEqCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // the = cursor predicate is pushed into the async filter and renders inline, exactly
            // like the < / > numeric cursor comparisons
            assertQuery("select i from t where i = (select max(i) from t)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: i [thread-safe] = cursor\s
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [max(i)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testTimestampEqStillRoutesToTimestampFactory() throws Exception {
        // Adding the numeric =(DC/IC/LC) overloads must not steal timestamp equality: an exact
        // TIMESTAMP left operand still binds to =(NC) and parses a string cursor scalar.
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select timestamp_sequence(0, 1000000) ts from long_sequence(5)" +
                    ") timestamp(ts) partition by day");
            assertQuery("select ts from x where ts = (select max(ts) from x)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\n1970-01-01T00:00:04.000000Z\n");
            assertQuery("select ts from x where ts = (select '1970-01-01T00:00:00.000000Z')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\n1970-01-01T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testWorkerStateSharedExecutesCursorOnce() throws Exception {
        // The parallel async-filter path clones the predicate per worker and donates the owner's
        // cached scalar to every clone, so the scalar sub-query executes exactly once per query -
        // not once per worker. test_timestamp_counter() increments once per row the sub-query
        // cursor reads, so the counter equals the number of RHS executions.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 4);
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (_, compiler, ctx) -> {
                    execute(compiler, "create table src (ts timestamp)", ctx);
                    execute(compiler, "insert into src values (5000)", ctx);
                    execute(
                            compiler,
                            "create table t as (" +
                                    "  select x::long l, timestamp_sequence(0, 1000000) ts" +
                                    "  from long_sequence(10000)" +
                                    ") timestamp(ts) partition by day",
                            ctx
                    );

                    // non-thread-safe left operand still runs on the parallel filter; exactly one
                    // row (l = 5000) matches the cached scalar
                    TestTimestampCounterFactory.COUNTER.set(0);
                    try (RecordCursorFactory factory = compiler.compile(
                            "select count() c from t where l::string::long = (select test_timestamp_counter(ts)::long from src)",
                            ctx
                    ).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            TestUtils.assertCursor("c\n1\n", cursor, factory.getMetadata(), true, sink);
                        }
                        Assert.assertEquals(1, TestTimestampCounterFactory.COUNTER.get());

                        // re-executing the same compiled factory refreshes the cached scalar
                        execute(compiler, "update src set ts = 9000", ctx);
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            TestUtils.assertCursor("c\n1\n", cursor, factory.getMetadata(), true, sink);
                        }
                        Assert.assertEquals(2, TestTimestampCounterFactory.COUNTER.get());
                    }
                }, configuration, LOG);
            }
        });
    }
}
