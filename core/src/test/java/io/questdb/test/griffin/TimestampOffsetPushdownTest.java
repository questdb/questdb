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

import io.questdb.jit.JitUtil;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests for timestamp predicate pushdown through virtual models with dateadd offset.
 * These tests verify that:
 * 1. Predicates are correctly pushed down with offset adjustment
 * 2. The correct rows are returned after pushdown
 * 3. The SQL plan shows the expected interval filters
 */
public class TimestampOffsetPushdownTest extends AbstractCairoTest {

    @Test
    public void testAndOffsetWithSubQueryPredicateArg() throws Exception {
        // a sub-query expression node has a null token; and_offset intrinsic analysis
        // recurses into its predicate argument and used to NPE on it. It must fail
        // with a clean SQL error instead (and_offset is not a user-callable function).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("CREATE TABLE flags (b BOOLEAN);");
            execute("INSERT INTO flags VALUES (true);");
            assertException(
                    "select * from trades where and_offset((select b from flags limit 1), 'h', 1)",
                    27,
                    "unknown function name"

            );
        });
    }

    @Test
    public void testBetweenRuntimeLoNonConstHiFreesBoundFunction() throws Exception {
        // A runtime-constant BETWEEN lo bound parks in RuntimeIntervalModelBuilder.betweenBoundaryFunc
        // until the hi bound pairs with it and moves it into dynamicRangeList. A column-dependent hi
        // bound never pairs - BETWEEN stays a residual filter - and analyzeBetween0's finally then
        // dropped the parked reference without closing it, orphaning its native buffer for good.
        //
        // Nothing throws here: the query compiles and returns the right rows, so only assertMemoryLeak
        // sees it. alloc_ts() makes the orphan observable by holding a tracked native buffer.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2020-01-01T12:00:00.000000Z');");

            assertQuery("SELECT * FROM trades " +
                    "WHERE timestamp BETWEEN alloc_ts('2020-01-01T00:00:00.000000Z'::timestamp) " +
                    "AND dateadd('d', 1, timestamp)")
                    .timestamp("timestamp")
                    .returns("""
                            price\ttimestamp
                            100.0\t2020-01-01T12:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testBindVariableOffsetPredicateResidual() throws Exception {
        // A bind-variable bound on an offset-derived timestamp must return the same rows as the
        // equivalent literal form. It gets there without any offset machinery: :b0 parses to
        // BIND_VARIABLE, which isStaticTimestampPredicate() rejects, so SqlOptimiser never wraps the
        // predicate in and_offset and it stays an ordinary filter over the virtual column.
        //
        // The earlier comment here claimed this covered the "unknown function name: and_offset" crash.
        // It never did - that gate has always rejected a bind variable, so no wrapper is built for
        // this query and none of the rebuild code runs. testStrandedAndOffsetCompilesAsResidualFilter
        // and testNestedOffsetsCalendarUnitOnIndexedSymbolPath are the tests that actually reach it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES " +
                    "(100, '2020-01-01T00:30:00.000000Z')," +   // tt = 2019-12-31T23:30
                    "(150, '2020-06-01T00:30:00.000000Z')," +   // tt = 2020-05-31T23:30
                    "(200, '2020-12-01T00:30:00.000000Z');");   // tt = 2020-11-30T23:30

            // tt > :b0
            bindVariableService.clear();
            bindVariableService.setTimestamp("b0", parseFloorPartialTimestamp("2020-05-31T23:00:00.000000Z"));
            assertQuery("SELECT * FROM (SELECT dateadd('h',-1,timestamp) tt, price FROM trades) WHERE tt > :b0")
                    .timestamp("tt")
                    .returns("""
                            tt\tprice
                            2020-05-31T23:30:00.000000Z\t150.0
                            2020-11-30T23:30:00.000000Z\t200.0
                            """);

            // tt = :b0
            bindVariableService.clear();
            bindVariableService.setTimestamp("b0", parseFloorPartialTimestamp("2020-05-31T23:30:00.000000Z"));
            assertQuery("SELECT * FROM (SELECT dateadd('h',-1,timestamp) tt, price FROM trades) WHERE tt = :b0")
                    .timestamp("tt")
                    .returns("""
                            tt\tprice
                            2020-05-31T23:30:00.000000Z\t150.0
                            """);

            // tt != :b0
            bindVariableService.clear();
            bindVariableService.setTimestamp("b0", parseFloorPartialTimestamp("2020-05-31T23:30:00.000000Z"));
            assertQuery("SELECT * FROM (SELECT dateadd('h',-1,timestamp) tt, price FROM trades) WHERE tt != :b0")
                    .timestamp("tt")
                    .returns("""
                            tt\tprice
                            2019-12-31T23:30:00.000000Z\t100.0
                            2020-11-30T23:30:00.000000Z\t200.0
                            """);

            // tt in (:b0)
            bindVariableService.clear();
            bindVariableService.setTimestamp("b0", parseFloorPartialTimestamp("2020-05-31T23:30:00.000000Z"));
            assertQuery("SELECT * FROM (SELECT dateadd('h',-1,timestamp) tt, price FROM trades) WHERE tt in (:b0)")
                    .timestamp("tt")
                    .returns("""
                            tt\tprice
                            2020-05-31T23:30:00.000000Z\t150.0
                            """);

            // Control: the literal form still pushes down to an interval scan (unchanged behavior).
            assertQuery("SELECT * FROM (SELECT dateadd('h',-1,timestamp) tt, price FROM trades) WHERE tt > '2020-05-31T23:00:00.000000Z'")
                    .timestamp("tt")
                    .withPlanContaining("Interval forward scan on: trades")
                    .returns("""
                            tt\tprice
                            2020-05-31T23:30:00.000000Z\t150.0
                            2020-11-30T23:30:00.000000Z\t200.0
                            """);
        });
    }

    @Test
    public void testCastWrappedDynamicBoundOffsetPredicateRemainsAsFilter() throws Exception {
        // isStaticTimestampPredicate() treats a cast as transparent so that a static bound like
        // null::timestamp still pushes down (see testNullBoundOffsetPushdownReturnsEmpty). That
        // transparency must not leak a dynamic bound through: the recursion still walks the cast's
        // operand, so a bind variable under a cast stays a residual filter exactly as the bare one
        // does in testDynamicBoundOffsetPredicateRemainsAsFilter. Baking the offset into an interval
        // here would read the bind variable at parse time, before it is set.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T01:00:00.000000Z'),
                        ('2024-01-02T01:00:00.000000Z'),
                        ('2024-01-03T01:00:00.000000Z')
                    """);
            bindVariableService.setStr(0, "2024-01-02T00:00:00.000000Z");

            assertQuery("""
                    SELECT shifted
                    FROM (SELECT dateadd('h', -1, ts) shifted FROM t)
                    WHERE shifted = $1::timestamp
                    """)
                    .noLeakCheck()
                    .timestamp("shifted")
                    .withPlanContaining("Filter filter: $0::string::timestamp=shifted")
                    .returns("""
                            shifted
                            2024-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testConstantFalseResidualWithRuntimeBoundLatestOnFreesModel() throws Exception {
        // Companion to testUnsatisfiableKeyWithRuntimeBoundFreesModel for the OTHER early return: with
        // a latest-by clause, a residual filter that folds to a compile-time constant false (here
        // "1 = 2", which the intrinsic parser leaves as a residual rather than absorbing) makes
        // SqlCodeGenerator return an empty factory before buildIntervalModel() transfers ownership of
        // the interval-bound functions. The runtime timestamp bound compiled into the interval builder
        // must be freed here too. alloc_ts() makes the leak observable via its tracked native buffer.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL INDEX, price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES ('a', 100, '2020-01-01T12:00:00.000000Z');");
            assertQuery("SELECT * FROM trades " +
                    "WHERE timestamp > alloc_ts('2020-01-01T00:00:00.000000Z'::timestamp) " +
                    "AND 1 = 2 " +
                    "LATEST ON timestamp PARTITION BY sym")
                    .timestamp("timestamp")
                    .returns("sym\tprice\ttimestamp\n");
        });
    }

    @Test
    public void testDayOffsetPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            // Row 1: timestamp 2022-01-01 12:00 -> ts (after -1d) = 2021-12-31 12:00 (NOT in 2022)
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");
            // Row 2: timestamp 2022-01-02 12:00 -> ts (after -1d) = 2022-01-01 12:00 (in 2022)
            execute("INSERT INTO trades VALUES (150, '2022-01-02T12:00:00.000000Z');");
            // Row 3: timestamp 2023-01-01 12:00 -> ts (after -1d) = 2022-12-31 12:00 (in 2022)
            execute("INSERT INTO trades VALUES (200, '2023-01-01T12:00:00.000000Z');");
            // Row 4: timestamp 2023-01-02 12:00 -> ts (after -1d) = 2023-01-01 12:00 (NOT in 2022)
            execute("INSERT INTO trades VALUES (250, '2023-01-02T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022'
                    """;

            // Verify correct data: rows 2, 3 (ts values in 2022)
            // Plan shows interval pushdown with +1 day offset applied
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('d',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-02T00:00:00.000000Z","2023-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T12:00:00.000000Z\t150.0
                            2022-12-31T12:00:00.000000Z\t200.0
                            """);
        });
    }

    @Test
    public void testDynamicBoundOffsetPredicateRemainsAsFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T01:00:00.000000Z'),
                        ('2024-01-02T01:00:00.000000Z'),
                        ('2024-01-03T01:00:00.000000Z')
                    """);
            bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-02T00:00:00.000000Z"));

            assertQuery("""
                    SELECT shifted
                    FROM (SELECT dateadd('h', -1, ts) shifted FROM t)
                    WHERE shifted = $1
                    """)
                    .noLeakCheck()
                    .timestamp("shifted")
                    .withPlanContaining("Filter filter: $0::timestamp=shifted")
                    .returns("""
                            shifted
                            2024-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testEmptyOffsetInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01T01:00:00.000000Z')");

            assertQuery("""
                    SELECT shifted
                    FROM (SELECT dateadd('h', -1, ts) shifted FROM t)
                    WHERE shifted > NULL
                    """)
                    .noLeakCheck()
                    .timestamp("shifted")
                    .returns("shifted\n");
        });
    }

    @Test
    public void testExtractThrowAfterRuntimeBoundFreesModel() throws Exception {
        // extract() analyses an AND's rhs before its lhs, so the rhs bound is already compiled into the
        // model when the lhs conjunct throws. The exception unwound past the model and nothing freed it,
        // leaving the bound's native buffer retained until the pool happened to hand that slot out again.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            assertExceptionNoLeakCheck(
                    "SELECT * FROM trades " +
                            "WHERE timestamp IN 'garbage' " +
                            "AND timestamp > alloc_ts('2020-01-01T00:00:00.000000Z'::timestamp)",
                    40,
                    "Invalid date"
            );
        });
    }

    @Test
    public void testHandWrittenAndOffsetDynamicBoundFreesTempModel() throws Exception {
        // and_offset is registered in intrinsicOps by TOKEN, with no check that the node came from
        // SqlOptimiser#wrapInAndOffset, so a hand-written and_offset in a WHERE clause reaches
        // analyzeAndOffset having never passed isStaticTimestampPredicate(). That is the door through
        // which a dynamic bound - which the optimiser's gate would have rejected - does reach the
        // temp interval model. analyzeAndOffset must free it on the residual exit; alloc_ts() holds a
        // tracked native buffer, so assertMemoryLeak sees the orphan if it does not.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES " +
                    "(100, '2020-01-01T00:30:00.000000Z')," +
                    "(150, '2020-06-01T00:30:00.000000Z')," +
                    "(200, '2020-12-01T00:30:00.000000Z');");

            assertQuery("SELECT * FROM trades " +
                    "WHERE and_offset(timestamp > alloc_ts('2020-06-01T00:00:00.000000Z'::timestamp), 'h', 1)")
                    .timestamp("timestamp")
                    .returns("""
                            price\ttimestamp
                            200.0\t2020-12-01T00:30:00.000000Z
                            """);
        });
    }

    @Test
    public void testHandWrittenAndOffsetDynamicBoundStaysResidual() throws Exception {
        // Companion to testHandWrittenAndOffsetDynamicBoundFreesTempModel, pinning the RESULT rather
        // than the free. mergeWithAddMethod must refuse to consume a predicate whose source carries
        // runtime bounds: their values are unknown at parse time, so the calendar offset cannot be
        // baked into them. Consuming it returns every row instead of the matching one. A bind
        // variable is enough to reach this - no test-only function needed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES " +
                    "(100, '2020-01-01T00:30:00.000000Z')," +
                    "(150, '2020-06-01T00:30:00.000000Z')," +
                    "(200, '2020-12-01T00:30:00.000000Z');");

            bindVariableService.clear();
            bindVariableService.setTimestamp("b0", parseFloorPartialTimestamp("2020-06-01T00:00:00.000000Z"));
            assertQuery("SELECT * FROM trades WHERE and_offset(timestamp > :b0, 'h', 1)")
                    .timestamp("timestamp")
                    .returns("""
                            price\ttimestamp
                            200.0\t2020-12-01T00:30:00.000000Z
                            """);
        });
    }

    @Test
    public void testHandWrittenAndOffsetEmptyModelFreesBound() throws Exception {
        // The third free: two contradicting static conjuncts empty the model before the and_offset
        // predicate merges into it, so mergeWithAddMethod takes its isEmptySet() early return and owns
        // freeing whatever the temp model compiled. alloc_ts() makes that orphan observable.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES " +
                    "(100, '2020-01-01T00:30:00.000000Z')," +
                    "(150, '2020-06-01T00:30:00.000000Z')," +
                    "(200, '2020-12-01T00:30:00.000000Z');");

            assertQuery("SELECT * FROM trades " +
                    "WHERE timestamp > '2021-01-01' AND timestamp < '2019-01-01' " +
                    "AND and_offset(timestamp > alloc_ts('2020-06-01T00:00:00.000000Z'::timestamp), 'h', 1)")
                    .timestamp("timestamp")
                    .returns("price\ttimestamp\n");
        });
    }

    @Test
    public void testHandWrittenAndOffsetOverNonTimestampPredicateDoesNotDropIt() throws Exception {
        // and_offset is an internal pseudo-function with no FunctionFactory, but intrinsicOps
        // dispatches it on its token alone, so a hand-written call reached analyzeAndOffset
        // ungated. Over a non-timestamp predicate the analysis consumed the conjunct without ever
        // applying an interval - analyzeEquals0 set the key column and the merge reported full
        // representation - so the predicate silently vanished and the query returned rows that
        // fail it. A hand-written call now falls through to the function compiler instead.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ao (s SYMBOL, l LONG, b BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO ao VALUES
                        ('a', 9, true,  '2020-01-01T00:00:00.000000Z'),
                        ('b', 1, false, '2020-01-02T00:00:00.000000Z')
                    """);

            // the plain predicates, for reference
            assertQuery("SELECT s FROM ao WHERE s = 'a'").returns("s\na\n");
            assertQuery("SELECT l FROM ao WHERE l > 5").returns("l\n9\n");

            // Each of these used to compile and return the wrong rows. They are now rejected.
            //
            // a key predicate: used to return BOTH rows, the predicate having been consumed
            assertExceptionNoLeakCheck("SELECT s FROM ao WHERE and_offset(s = 'a', 'h', 1)", 23,
                    "unknown function name: and_offset");
            // a non-key predicate over a LONG column: used to build dateadd over a LONG
            assertExceptionNoLeakCheck("SELECT l FROM ao WHERE and_offset(l > 5, 'h', 1)", 23,
                    "unknown function name: and_offset");
            // a bare boolean column: used to drop the offset silently
            assertExceptionNoLeakCheck("SELECT b FROM ao WHERE and_offset(b, 'h', 1)", 23,
                    "unknown function name: and_offset");

            // the optimiser-generated wrapper over the designated timestamp still pushes down
            assertQuery("SELECT * FROM (SELECT dateadd('h', -1, ts) tt, s FROM ao) WHERE tt > '2020-01-01T12:00:00.000000Z'")
                    .timestamp("tt")
                    .returns("tt\ts\n2020-01-01T23:00:00.000000Z\tb\n");
        });
    }

    @Test
    public void testHandwrittenAndOffsetMixedTimestampAndColumnWrapsOnlyTimestamp() throws Exception {
        // A hand-written and_offset whose predicate mixes the designated timestamp with another column
        // passes analyzeAndOffset's referencesTimestamp guard (ts IS referenced), so it is not rejected.
        // The offset must then apply ONLY to the timestamp literal. Before the fix, wrapTimestampLiterals
        // wrapped every literal, rewriting `and_offset(ts>0 AND s=5, 'h', 1)` to `5=dateadd('h',-1,s)`,
        // which treats a numeric column as a timestamp (wrong rows) and, for a symbol/string column,
        // fails with a cast error. Now only the timestamp literal is wrapped and the sibling stays intact.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01T05:00:00.000000Z', 5), ('2024-01-01T06:00:00.000000Z', 9)");
            // the sibling numeric predicate stays s=5 (not dateadd('h',-1,s)=5, which returned no rows)
            assertQuery("SELECT * FROM t WHERE and_offset(ts > 0 AND s = 5, 'h', 1)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("filter: s=5")
                    .returns("ts\ts\n2024-01-01T05:00:00.000000Z\t5\n");

            // a symbol sibling used to fail with a cast error; now it is left untouched
            execute("CREATE TABLE t2 (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t2 VALUES ('2024-01-01T05:00:00.000000Z', 'a'), ('2024-01-01T06:00:00.000000Z', 'b')");
            assertQuery("SELECT * FROM t2 WHERE and_offset(ts > 0 AND sym = 'a', 'h', 1)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("filter: sym='a'")
                    .returns("ts\tsym\n2024-01-01T05:00:00.000000Z\ta\n");
        });
    }

    @Test
    public void testHandwrittenAndOffsetOverNonTimestampIsRejected() throws Exception {
        // and_offset is an internal pseudo-function SqlOptimiser inserts only over the designated
        // timestamp. A hand-written call over a numeric column, reaching the residual filter via an OR
        // branch (which skips interval extraction and analyzeAndOffset's guard), must be rejected as an
        // unknown function - not silently rebuilt into dateadd(...) over that column, which would treat
        // the number as a timestamp and drop rows. rebuildStrandedAndOffsets now gates on the wrapped
        // predicate referencing the designated timestamp.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (n INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (10, '2020-01-01T10:00:00.000000Z'),
                        (200000, '2020-01-02T10:00:00.000000Z')
                    """);
            // the residual predicate on the numeric column matches n=200000
            assertQuery("SELECT n FROM t WHERE ts < '2019-01-01' OR (n > 100) ORDER BY n")
                    .noLeakCheck().returns("n\n200000\n");
            // wrapped in a hand-written and_offset over the numeric column, it is rejected outright
            // (before the fix it was rewritten to dateadd('h', -5, n) > 100 and returned no rows)
            assertExceptionNoLeakCheck(
                    "SELECT n FROM t WHERE ts < '2019-01-01' OR and_offset(n > 100, 'h', 5) ORDER BY n",
                    43,
                    "unknown function name: and_offset(BOOLEAN,CHAR,INT)",
                    sqlExecutionContext

            );
        });
    }

    @Test
    public void testHourOffsetPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            // Row 1: timestamp 2022-01-01 00:30:00 -> ts (after -1h) = 2021-12-31 23:30:00 (NOT in 2022)
            execute("INSERT INTO trades VALUES (100, '2022-01-01T00:30:00.000000Z');");
            // Row 2: timestamp 2022-01-01 01:30:00 -> ts (after -1h) = 2022-01-01 00:30:00 (in 2022)
            execute("INSERT INTO trades VALUES (150, '2022-01-01T01:30:00.000000Z');");
            // Row 3: timestamp 2022-12-31 23:30:00 -> ts (after -1h) = 2022-12-31 22:30:00 (in 2022)
            execute("INSERT INTO trades VALUES (200, '2022-12-31T23:30:00.000000Z');");
            // Row 4: timestamp 2023-01-01 00:30:00 -> ts (after -1h) = 2022-12-31 23:30:00 (in 2022)
            execute("INSERT INTO trades VALUES (250, '2023-01-01T00:30:00.000000Z');");
            // Row 5: timestamp 2023-01-01 01:30:00 -> ts (after -1h) = 2023-01-01 00:30:00 (NOT in 2022)
            execute("INSERT INTO trades VALUES (300, '2023-01-01T01:30:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022'
                    """;

            // Verify correct data: rows 2, 3, 4 (ts values in 2022)
            // Plan shows interval pushdown with +1h offset applied
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:30:00.000000Z\t150.0
                            2022-12-31T22:30:00.000000Z\t200.0
                            2022-12-31T23:30:00.000000Z\t250.0
                            """);
        });
    }

    @Test
    public void testIntervalUnionOffsetPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T01:00:00.000000Z'),
                        ('2024-01-02T01:00:00.000000Z'),
                        ('2024-01-03T01:00:00.000000Z')
                    """);

            assertQuery("""
                    SELECT shifted
                    FROM (SELECT dateadd('h', -1, ts) shifted FROM t)
                    WHERE shifted IN ('2024-01-01', '2024-01-03')
                    """)
                    .noLeakCheck()
                    .timestamp("shifted")
                    .withPlanContaining("Interval forward scan on: t")
                    .returns("""
                            shifted
                            2024-01-01T00:00:00.000000Z
                            2024-01-03T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testIntervalUnionOffsetPushdownWithDynamicInnerBound() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T01:00:00.000000Z'),
                        ('2024-01-02T01:00:00.000000Z'),
                        ('2024-01-03T01:00:00.000000Z')
                    """);
            bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-01T00:00:00.000000Z"));

            assertQuery("""
                    SELECT shifted
                    FROM (
                        SELECT dateadd('h', -1, ts) shifted
                        FROM t
                        WHERE ts >= $1
                    )
                    WHERE shifted IN ('2024-01-01', '2024-01-03')
                    """)
                    .noLeakCheck()
                    .timestamp("shifted")
                    .withPlanContaining("Interval forward scan on: t")
                    .returns("""
                            shifted
                            2024-01-01T00:00:00.000000Z
                            2024-01-03T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testIntervalUnionOffsetPushdownWithDynamicInnerInRemainsAsFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-02T01:00:00.000000Z')");
            bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2024-01-02T01:00:00.000000Z"));

            assertQuery("""
                    SELECT shifted
                    FROM (
                        SELECT dateadd('h', -1, ts) shifted
                        FROM t
                        WHERE ts IN ($1, '2024-01-02T01:00:00.000000Z')
                    )
                    WHERE shifted IN ('2024-01-01', '2024-01-03')
                    """)
                    .noLeakCheck()
                    .timestamp("shifted")
                    .withPlanContaining("filter: ts in")
                    .returns("shifted\n");
        });
    }

    @Test
    public void testLargeOffsetNegationOverflowThrowsError() throws Exception {
        // Test for int overflow when negating the stride value.
        // The optimizer stores the INVERSE of the stride for pushdown.
        // If stride = Integer.MIN_VALUE (-2147483648), then inverse = 2147483648 which overflows int.
        assertMemoryLeak(() -> execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;"));

        // Use Integer.MIN_VALUE as stride - negating it causes overflow
        // -(-2147483648) = 2147483648 which exceeds Integer.MAX_VALUE
        // Error position 35 is at the stride argument "-2147483648"
        assertQuery("SELECT * FROM (" +
                "SELECT dateadd('s', -2147483648, timestamp) as ts, price FROM trades" +
                ") WHERE ts > '2022-01-01'")
                .fails(35, "timestamp offset value 2147483648 exceeds maximum allowed range for dateadd");
    }

    @Test
    public void testLargeOffsetThrowsError() throws Exception {
        // When dateadd offset exceeds Integer.MAX_VALUE (2,147,483,647), the optimizer
        // should throw an actionable error message rather than silently failing.
        // This is because dateadd has signature: dateadd(char, int, timestamp)
        // and the optimizer intrinsically understands this function for predicate pushdown.
        //
        // 3,000,000,000 seconds = ~95 years, which exceeds int range.
        assertMemoryLeak(() -> execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;"));

        // Use an offset of 3 billion seconds (exceeds Integer.MAX_VALUE of 2,147,483,647)
        // Error position 35 is at the stride argument "3000000000"
        assertQuery("SELECT * FROM (" +
                "SELECT dateadd('s', 3000000000, timestamp) as ts, price FROM trades" +
                ") WHERE ts > '2100-01-01'")
                .fails(35, "timestamp offset value -3000000000 exceeds maximum allowed range for dateadd");
    }

    @Test
    public void testLossyCastBoundKeepsResidualFilter() throws Exception {
        // A cast that truncates - here TIMESTAMP_NS down to TIMESTAMP - makes the interval analysis a
        // SUPERSET of the predicate, so removeAndIntrinsics applies the widened interval and returns
        // false to keep the predicate as a residual filter. analyzeAndOffset used to consume the
        // predicate anyway whenever intervals had been left behind, dropping the residual and
        // returning rows that fail the predicate.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2022-01-01T10:00:00.000000500Z'),
                        ('2022-01-01T11:00:00.000000000Z')
                    """);
            // Row 1 shifts to 09:00:00.000000500, whose cast to microseconds truncates to
            // 09:00:00.000000 - NOT greater than the bound, so it must not be returned. It sits
            // inside the widened scan interval, so only a surviving residual filter removes it.
            assertQuery("""
                    SELECT * FROM (SELECT dateadd('h',-1,ts) tt FROM t)
                    WHERE tt::timestamp > '2022-01-01T09:00:00.000000Z'
                    """)
                    .timestamp("tt")
                    .withPlanContaining("filter: 2022-01-01T09:00:00.000000Z<dateadd('h',-1,ts)::timestamp")
                    .returns("""
                            tt
                            2022-01-01T10:00:00.000000000Z
                            """);
            // The widened interval is still applied for pruning, so the scan is an interval scan
            // rather than a full table scan.
            assertQuery("""
                    SELECT * FROM (SELECT dateadd('h',-1,ts) tt FROM t)
                    WHERE tt::timestamp > '2022-01-01T09:00:00.000000Z'
                    """)
                    .timestamp("tt")
                    .withPlanContaining("Interval forward scan on: t")
                    .returns("""
                            tt
                            2022-01-01T10:00:00.000000000Z
                            """);
        });
    }

    @Test
    public void testMinuteOffsetPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T00:15:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T00:45:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('m', -30, timestamp) as ts, price FROM trades
                    ) WHERE ts >= '2022-01-01T00:00:00' AND ts < '2022-01-01T00:30:00'
                    """;

            // Row 1: ts = 2021-12-31 23:45 (NOT in range)
            // Row 2: ts = 2022-01-01 00:15 (in range)
            // Plan shows interval pushdown with +30 minute offset
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('m',-30,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T00:30:00.000000Z","2022-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:15:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testMixedPredicatesPushdown() throws Exception {
        // Test that non-timestamp predicates don't interfere with pushdown
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-01T03:30:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022' AND price > 120
                    """;

            // Verify plan shows interval pushdown AND filter for price
            String expectedPlan;
            if (JitUtil.isJitSupported()) {
                expectedPlan = """
                        VirtualRecord
                          functions: [dateadd('h',-1,timestamp),price]
                            Async JIT Filter workers: 1
                              filter: 120<price
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                        """;
            } else {
                expectedPlan = """
                        VirtualRecord
                          functions: [dateadd('h',-1,timestamp),price]
                            Async Filter workers: 1
                              filter: 120<price
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                        """;
            }

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan(expectedPlan)
                    .returns("""
                            ts\tprice
                            2022-01-01T01:30:00.000000Z\t150.0
                            2022-01-01T02:30:00.000000Z\t200.0
                            """);
        });
    }

    @Test
    public void testMonthOffsetPushdown() throws Exception {
        // Month offset IS pushed down with calendar-aware handling
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY MONTH;");
            // Row 1: timestamp 2022-02-15 -> ts (after -1M) = 2022-01-15 (NOT in Feb 2022)
            execute("INSERT INTO trades VALUES (100, '2022-02-15T12:00:00.000000Z');");
            // Row 2: timestamp 2022-03-15 -> ts (after -1M) = 2022-02-15 (in Feb 2022)
            execute("INSERT INTO trades VALUES (150, '2022-03-15T12:00:00.000000Z');");
            // Row 3: timestamp 2022-04-15 -> ts (after -1M) = 2022-03-15 (NOT in Feb 2022)
            execute("INSERT INTO trades VALUES (200, '2022-04-15T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('M', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022-02'
                    """;

            // Should only return row 2.
            // The plan still prunes with the +1 month calendar shift, but 'M' is not injective, so
            // the upper bound is widened past the day-of-month clamp stall and the predicate stays
            // as a filter:
            //   lower: 2022-02-01 + 1 month                    = 2022-03-01
            //   upper: 2022-02-28 23:59:59 + 1 month + 3 days  = 2022-03-31 23:59:59
            // Three days covers the timestamps the clamp folds onto the shifted bound; the filter
            // then drops the ones that do not satisfy the predicate. Pinning the narrow 2022-03-28
            // bound with no filter node is what dropped rows - see
            // testMonthOffsetPushdownKeepsDayClampedRows.
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('M',-1,timestamp),price]
                                Async Filter workers: 1
                                  filter: dateadd('M',-1,timestamp) in [1643673600000000,1646092799999999]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trades
                                          intervals: [("2022-03-01T00:00:00.000000Z","2022-03-31T23:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-02-15T12:00:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testMonthOffsetPushdownInexactLowerBoundKeepsFilter() throws Exception {
        // The LOWER bound is not automatically exact for a non-injective unit, so narrowing the upper
        // bound must not be read as licence to consume the predicate.
        // addMonths(2022-03-31, +1) clamps to 2022-04-30, and shifting that back gives 2022-03-30 -
        // one DAY below the bound. So the scan's first row (2022-04-30) does not satisfy the
        // predicate and the residual filter is what removes it. Consuming the predicate here would
        // return it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH;");
            execute("""
                    INSERT INTO m VALUES
                        ('2022-04-29T00:00:00.000000Z'),
                        ('2022-04-30T00:00:00.000000Z'),
                        ('2022-05-01T00:00:00.000000Z');
                    """);

            assertQuery("""
                    SELECT * FROM (
                        SELECT dateadd('M', -1, ts) AS tt FROM m
                    ) WHERE tt >= '2022-03-31T00:00:00.000000Z'
                    """)
                    .noLeakCheck()
                    .timestamp("tt")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('M',-1,ts)]
                                Async Filter workers: 1
                                  filter: dateadd('M',-1,ts)>=2022-03-31T00:00:00.000000Z
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: m
                                          intervals: [("2022-04-30T00:00:00.000000Z","MAX")]
                            """)
                    .returns("""
                            tt
                            2022-04-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testMonthOffsetPushdownKeepsDayClampedRows() throws Exception {
        // addMonths clamps the day of month, so 2022-03-29, -30 and -31 all shift back onto
        // 2022-02-28 and satisfy the predicate just as 2022-03-28 does. Shifting the upper bound
        // forward lands on 2022-03-28 - the FIRST timestamp of that clamp stall - so consuming the
        // predicate scanned the other three away. The bound must widen past the stall and the
        // predicate must stay behind as a residual filter to drop what the wider scan lets in.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH;");
            execute("""
                    INSERT INTO m VALUES
                        ('2022-03-28T00:00:00.000000Z'),
                        ('2022-03-29T00:00:00.000000Z'),
                        ('2022-03-30T00:00:00.000000Z'),
                        ('2022-03-31T00:00:00.000000Z'),
                        ('2022-04-01T00:00:00.000000Z');
                    """);

            assertQuery("""
                    SELECT * FROM (
                        SELECT dateadd('M', -1, ts) AS tt FROM m
                    ) WHERE tt <= '2022-02-28T00:00:00.000000Z'
                    """)
                    .noLeakCheck()
                    .timestamp("tt")
                    .returns("""
                            tt
                            2022-02-28T00:00:00.000000Z
                            2022-02-28T00:00:00.000000Z
                            2022-02-28T00:00:00.000000Z
                            2022-02-28T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testMonthOffsetPushdownKeepsDayClampedRowsNanos() throws Exception {
        // The nanosecond twin of testMonthOffsetPushdownKeepsDayClampedRows. The stall widening is
        // computed in the builder's own resolution, so a widening sized in microseconds would be a
        // thousand times too small here - 259 seconds instead of three days - and would scan away
        // exactly the clamped rows the widening exists to keep. Every other test in this file runs
        // on microseconds and would not notice.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY MONTH;");
            execute("""
                    INSERT INTO m VALUES
                        ('2022-03-28T00:00:00.000000000Z'),
                        ('2022-03-29T00:00:00.000000000Z'),
                        ('2022-03-30T00:00:00.000000000Z'),
                        ('2022-03-31T00:00:00.000000000Z'),
                        ('2022-04-01T00:00:00.000000000Z');
                    """);

            assertQuery("""
                    SELECT * FROM (
                        SELECT dateadd('M', -1, ts) AS tt FROM m
                    ) WHERE tt <= '2022-02-28T00:00:00.000000000Z'
                    """)
                    .noLeakCheck()
                    .timestamp("tt")
                    .returns("""
                            tt
                            2022-02-28T00:00:00.000000000Z
                            2022-02-28T00:00:00.000000000Z
                            2022-02-28T00:00:00.000000000Z
                            2022-02-28T00:00:00.000000000Z
                            """);
        });
    }

    @Test
    public void testMonthOffsetPushdownPositiveStrideKeepsDayClampedRows() throws Exception {
        // A POSITIVE dateadd stride makes the pushdown offset -1, and the old widening of
        // "offset + 1" collapsed that to 0 - which applyOffset short-circuits, leaving the upper
        // bound entirely unshifted. Sizing the widening in ticks instead removes that degenerate
        // case. addMonths clamps here too: 2022-01-29, -30 and -31 all land on 2022-02-28.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH;");
            execute("""
                    INSERT INTO m VALUES
                        ('2022-01-28T00:00:00.000000Z'),
                        ('2022-01-29T00:00:00.000000Z'),
                        ('2022-01-30T00:00:00.000000Z'),
                        ('2022-01-31T00:00:00.000000Z'),
                        ('2022-02-01T00:00:00.000000Z');
                    """);

            assertQuery("""
                    SELECT * FROM (
                        SELECT dateadd('M', 1, ts) AS tt FROM m
                    ) WHERE tt <= '2022-02-28T00:00:00.000000Z'
                    """)
                    .noLeakCheck()
                    .timestamp("tt")
                    .returns("""
                            tt
                            2022-02-28T00:00:00.000000Z
                            2022-02-28T00:00:00.000000Z
                            2022-02-28T00:00:00.000000Z
                            2022-02-28T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testMultiIntervalOffsetPushdown() throws Exception {
        // A predicate that extracts multiple disjoint intervals (e.g. tt != <lit> -> two ranges) must
        // push down the UNION of the offset-shifted ranges, not their per-interval intersection. The
        // and_offset merge previously intersected each shifted range in turn, collapsing 2+ disjoint
        // ranges to an empty scan; because analyzeAndOffset consumes the predicate (no residual filter),
        // the empty scan was the final result (0 rows instead of 2). The merge now unions the ranges.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES " +
                    "(1, '2022-01-01T10:00:00.000000Z')," +
                    "(2, '2022-06-01T10:00:00.000000Z')," +
                    "(3, '2022-12-01T10:00:00.000000Z');");

            // != -> two intervals around the shifted literal, unioned.
            assertQuery("""
                    SELECT * FROM (
                        SELECT dateadd('h', -1, ts) as tt, price FROM trades
                    ) WHERE tt != '2022-01-01T09:00:00.000000Z'
                    """)
                    .noLeakCheck()
                    .timestamp("tt")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,ts),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("MIN","2022-01-01T09:59:59.999999Z"),("2022-01-01T10:00:00.000001Z","MAX")]
                            """)
                    .returns("""
                            tt\tprice
                            2022-06-01T09:00:00.000000Z\t2.0
                            2022-12-01T09:00:00.000000Z\t3.0
                            """);

            // <> is the same shape.
            assertQuery("""
                    SELECT * FROM (
                        SELECT dateadd('h', -1, ts) as tt, price FROM trades
                    ) WHERE tt <> '2022-06-01T09:00:00.000000Z'
                    """)
                    .noLeakCheck()
                    .timestamp("tt")
                    .returns("""
                            tt\tprice
                            2022-01-01T09:00:00.000000Z\t1.0
                            2022-12-01T09:00:00.000000Z\t3.0
                            """);

            // IN (a, b) -> two intervals, unioned.
            assertQuery("""
                    SELECT * FROM (
                        SELECT dateadd('h', -1, ts) as tt, price FROM trades
                    ) WHERE tt IN ('2022-01-01T09:00:00.000000Z', '2022-06-01T09:00:00.000000Z')
                    """)
                    .noLeakCheck()
                    .timestamp("tt")
                    .returns("""
                            tt\tprice
                            2022-01-01T09:00:00.000000Z\t1.0
                            2022-06-01T09:00:00.000000Z\t2.0
                            """);

            // A multi-interval offset predicate intersected with a single-interval one on the same
            // (offset) column: union first, then intersect with the builder's own intervals.
            assertQuery("""
                    SELECT * FROM (
                        SELECT dateadd('h', -1, ts) as tt, price FROM trades
                    ) WHERE tt != '2022-01-01T09:00:00.000000Z' AND tt < '2022-12-01T09:00:00.000000Z'
                    """)
                    .noLeakCheck()
                    .timestamp("tt")
                    .returns("""
                            tt\tprice
                            2022-06-01T09:00:00.000000Z\t2.0
                            """);

            // Calendar-aware (month) offset variant of the multi-interval union.
            assertQuery("""
                    SELECT * FROM (
                        SELECT dateadd('M', -1, ts) as tt, price FROM trades
                    ) WHERE tt != '2021-12-01T10:00:00.000000Z'
                    """)
                    .noLeakCheck()
                    .timestamp("tt")
                    .returns("""
                            tt\tprice
                            2022-05-01T10:00:00.000000Z\t2.0
                            2022-11-01T10:00:00.000000Z\t3.0
                            """);

            // Controls: a single-interval offset predicate is unchanged.
            assertQuery("""
                    SELECT * FROM (
                        SELECT dateadd('h', -1, ts) as tt, price FROM trades
                    ) WHERE tt = '2022-01-01T09:00:00.000000Z'
                    """)
                    .noLeakCheck()
                    .timestamp("tt")
                    .returns("""
                            tt\tprice
                            2022-01-01T09:00:00.000000Z\t1.0
                            """);
        });
    }

    @Test
    public void testNegativeIntegerOffsetPushdown() throws Exception {
        // Verify that negative integer offsets (using unary minus) are correctly handled
        // This is a regression test for isConstantIntegerExpression handling of unary minus
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T02:00:00.000000Z');");

            // Use explicit negative value with unary minus
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022'
                    """;

            // Verify data correctness
            // Plan should show interval pushdown with +1h offset applied
            // Verifies unary minus is correctly parsed as integer
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T01:00:00.000000Z\t100.0
                            """);
        });
    }

    @Test
    public void testNestedOffsetsCalendarUnitComposesInOrder() throws Exception {
        // Two nested models carrying different offset units leave a genuinely nested wrapper,
        // and_offset(and_offset(pred,'M',o1),'h',o2). Rebuilding that residual must recurse into the
        // inner wrapper BEFORE wrapping the outer one: wrapTimestampLiteral only replaces LITERAL
        // nodes, so whichever pass runs first plants its dateadd at the leaf and the later pass nests
        // around it. Wrapping outer-first yields dateadd('M',1,dateadd('h',5,ts)) where the correct
        // composition is dateadd('h',5,dateadd('M',1,ts)). Calendar units do not commute with
        // fixed-tick units, so the reversed order drops the rows the day-of-month clamp folds onto
        // the bound - here Jan 29/30/31, which all clamp onto Feb 28.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY MONTH");
            execute("""
                    INSERT INTO tab VALUES
                        ('2022-01-28T20:00:00.000000Z',1),
                        ('2022-01-29T20:00:00.000000Z',2),
                        ('2022-01-30T20:00:00.000000Z',3),
                        ('2022-01-31T20:00:00.000000Z',4),
                        ('2022-02-01T20:00:00.000000Z',5)
                    """);
            assertQuery("""
                    SELECT tt, v FROM (
                      SELECT dateadd('h',5,t1) tt, v
                      FROM (SELECT dateadd('M',1,ts) t1, v FROM tab) timestamp(t1)
                    ) timestamp(tt)
                    WHERE tt = '2022-03-01T01:00:00.000000Z'
                    """)
                    .timestamp("tt")
                    // Pin the nesting order itself, not just the row set: the 'M' shift must be applied
                    // to ts FIRST, matching tt = dateadd('h',5,dateadd('M',1,ts)).
                    .withPlanContaining("dateadd('h',5,dateadd('M',1,ts))")
                    .returns("""
                            tt\tv
                            2022-03-01T01:00:00.000000Z\t1
                            2022-03-01T01:00:00.000000Z\t2
                            2022-03-01T01:00:00.000000Z\t3
                            2022-03-01T01:00:00.000000Z\t4
                            """);
        });
    }

    @Test
    public void testNestedOffsetsCalendarUnitOnIndexedSymbolPath() throws Exception {
        // The indexed-symbol filter path compiles intrinsicModel.filter through compileBooleanFilter
        // rather than generateFilter0, so it never reached the stranded-wrapper rebuild that
        // generateFilter0 performs. A nested and_offset left behind by rebuildAndOffsetResidual
        // therefore went straight to the function compiler and surfaced as
        // "unknown function name: and_offset(BOOLEAN,CHAR,INT)". Dropping the INDEX made the same
        // query compile. Rebuilding the nested wrapper at source removes it before any filter path
        // sees it; the second query is the no-pushdown oracle for the row count.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, s SYMBOL INDEX, v INT) TIMESTAMP(ts) PARTITION BY MONTH");
            execute("INSERT INTO tab SELECT dateadd('m',(x*53)::int,'2021-12-20T00:00:00.000000Z'),'k'||(x%3),x::int FROM long_sequence(4000)");
            assertQuery("""
                    SELECT count() FROM (
                      SELECT dateadd('h',1,t1) tt, s, v
                      FROM (SELECT dateadd('M',1,ts) t1, s, v FROM tab) timestamp(t1)
                    ) timestamp(tt)
                    WHERE tt IN '2022-02-28' AND s = 'k1'
                    """)
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            35
                            """);
            assertQuery("""
                    SELECT count() FROM tab
                    WHERE dateadd('h',1,dateadd('M',1,ts)) IN '2022-02-28' AND s = 'k1'
                    """)
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            35
                            """);
        });
    }

    @Test
    public void testNestedOffsetsPushdown() throws Exception {
        // Test that nested dateadd offsets both get applied
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            // With -1h then -1d offset, total is -25h
            // Row at 2022-01-02 02:00 -> after -1h = 2022-01-02 01:00 -> after -1d = 2022-01-01 01:00
            execute("INSERT INTO trades VALUES (100, '2022-01-02T02:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, ts1) as ts2, price FROM (
                            SELECT dateadd('h', -1, timestamp) as ts1, price FROM trades
                        )
                    ) WHERE ts2 >= '2022-01-01T00:00:00' AND ts2 < '2022-01-01T02:00:00'
                    """;

            // Plan shows combined offset: +1 day +1 hour = 25 hours
            // Range [2022-01-01 00:00, 2022-01-01 02:00) + 1d + 1h = [2022-01-02 01:00, 2022-01-02 03:00)
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts2")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('d',-1,ts1),price]
                                VirtualRecord
                                  functions: [dateadd('h',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trades
                                          intervals: [("2022-01-02T01:00:00.000000Z","2022-01-02T02:59:59.999999Z")]
                            """)
                    .returns("""
                            ts2\tprice
                            2022-01-01T01:00:00.000000Z\t100.0
                            """);
        });
    }

    @Test
    public void testNestedOffsetsYearPushdown() throws Exception {
        // Test nested offsets with year-based dateadd
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, ts1) as ts2, price FROM (
                            SELECT dateadd('h', -1, timestamp) as ts1, price FROM trades
                        )
                    ) WHERE ts2 IN '2022'
                    """;

            // Plan should show combined offset: +1 day +1 hour
            // 2022-01-01 00:00 + 1 day + 1 hour = 2022-01-02 01:00
            // 2022-12-31 23:59:59 + 1 day + 1 hour = 2023-01-02 00:59:59
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [dateadd('d',-1,ts1),price]
                                VirtualRecord
                                  functions: [dateadd('h',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trades
                                          intervals: [("2022-01-02T01:00:00.000000Z","2023-01-02T00:59:59.999999Z")]
                            """);
        });
    }

    @Test
    public void testNoOffsetNoPushdownMetadata() throws Exception {
        // When there's no dateadd, no ts_offset should appear and literal timestamp wins
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");

            String query = """
                    SELECT price, timestamp FROM trades
                    """;

            // Plan should NOT have ts_offset since there's no dateadd transformation
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            PageFrame
                                Row forward scan
                                Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testNonConstantOffsetNoPushdown() throws Exception {
        // Non-constant offset should not be pushed down
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, offset_val INT, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, 1, '2022-01-01T01:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', offset_val, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022'
                    """;

            // Plan should show filter at virtual level, NOT interval scan (no pushdown)
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: ts in [1640995200000000,1672531199999999]
                                VirtualRecord
                                  functions: [dateadd('h',offset_val,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testNonLiteralColumnWithTimestampPredicateAndNoPushdown() throws Exception {
        // Test that when a predicate references BOTH the timestamp AND a non-literal column,
        // the non-literal part should NOT be pushed down to the nested model.
        // With AND, the predicates can be split so the timestamp part is pushed down.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, quantity INT, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, 10, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, 20, '2022-01-01T02:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, 5, '2022-01-01T03:30:00.000000Z');");

            // Query with computed column (non-literal) and predicate referencing both timestamp AND computed column
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, price * quantity as total_value FROM trades
                    ) WHERE ts IN '2022' AND total_value > 1500
                    """;

            // Row 1: ts = 00:30, total_value = 1000 (NOT > 1500)
            // Row 2: ts = 01:30, total_value = 3000 (> 1500) - INCLUDED
            // Row 3: ts = 02:30, total_value = 1000 (NOT > 1500)
            // The timestamp predicate (ts IN '2022') should be pushed down with offset
            // The non-literal predicate (total_value > 1500) should stay at the outer level as a Filter
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            Filter filter: 1500<total_value
                                VirtualRecord
                                  functions: [dateadd('h',-1,timestamp),price*quantity]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trades
                                          intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\ttotal_value
                            2022-01-01T01:30:00.000000Z\t3000.0
                            """);
        });
    }

    @Test
    public void testNonLiteralColumnWithTimestampPredicateOrNoPushdown() throws Exception {
        // Test that when an OR predicate references BOTH the timestamp AND a non-literal column,
        // the ENTIRE predicate should NOT be pushed down because OR cannot be split.
        // This is a regression test for the case where isTimestampPredicate returns true
        // but the predicate also references other non-literal aliases that can't be resolved.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, quantity INT, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, 10, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, 20, '2022-01-01T02:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, 5, '2022-01-02T03:30:00.000000Z');");

            // Query with OR - this cannot be split, so it must stay at outer level
            // ts < '2022-01-01T01:00:00' would return no rows (all ts values are >= 01:00)
            // total_value > 1500 would return row 2 (3000)
            // The OR means: either early timestamp OR high total_value
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, price * quantity as total_value FROM trades
                    ) WHERE ts < '2022-01-01T01:00:00' OR total_value > 1500
                    """;

            // Row 1: ts = 00:30 (< 01:00? YES), total_value = 1000 - INCLUDED (ts condition true)
            // Row 2: ts = 01:30 (< 01:00? NO), total_value = 3000 (> 1500? YES) - INCLUDED (total_value condition true)
            // Row 3: ts = 02:30 next day (< 01:00? NO), total_value = 1000 (> 1500? NO) - NOT included
            // The OR predicate cannot be split, so it should stay at outer level as a Filter
            // The timestamp offset pushdown should NOT happen because the predicate
            // references non-literal columns (total_value) that can't be resolved in nested model
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            Filter filter: (ts<2022-01-01T01:00:00.000000Z or 1500<total_value)
                                VirtualRecord
                                  functions: [dateadd('h',-1,timestamp),price*quantity]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """)
                    .returns("""
                            ts\ttotal_value
                            2022-01-01T00:30:00.000000Z\t1000.0
                            2022-01-01T01:30:00.000000Z\t3000.0
                            """);
        });
    }

    @Test
    public void testNotInNullOffsetPushdownCompiles() throws Exception {
        // ts NOT IN NULL inverts to [Long.MIN_VALUE + 1, Long.MAX_VALUE] - a real lower bound one tick
        // above the NULL sentinel. Shifting it by the inverse offset underflows, which used to throw
        // and fail the whole query. The shift must collapse the bound to the open sentinel instead
        // and the query must return every row, exactly as the un-offset form does.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z'), " +
                    "(150, '2022-01-02T12:00:00.000000Z');");

            // CONTROL: no offset. Every row has a non-null designated timestamp.
            assertQuery("SELECT timestamp, price FROM trades WHERE timestamp NOT IN NULL")
                    .timestamp("timestamp")
                    .returns("""
                            timestamp\tprice
                            2022-01-01T12:00:00.000000Z\t100.0
                            2022-01-02T12:00:00.000000Z\t150.0
                            """);

            // Collapsing the bound to the open sentinel WIDENS the scan: the forward dateadd wraps,
            // so a source timestamp near the end of the range projects onto the NULL sentinel and
            // fails the predicate even though the open bound covers it. The predicate therefore
            // stays as a residual filter rather than being consumed. The rows are the same either
            // way here - the wrapping preimage is outside this table - but the plan keeps the check.
            assertQuery("SELECT * FROM (SELECT dateadd('h', 1, timestamp) as ts, price FROM trades) WHERE ts NOT IN NULL")
                    .timestamp("ts")
                    .withPlanContaining("filter: not (dateadd('h',1,timestamp) in [null])")
                    .returns("""
                            ts\tprice
                            2022-01-01T13:00:00.000000Z\t100.0
                            2022-01-02T13:00:00.000000Z\t150.0
                            """);
            // != NULL takes the same inversion through a different analyze method.
            assertQuery("SELECT * FROM (SELECT dateadd('d', 1, timestamp) as ts, price FROM trades) WHERE ts != NULL")
                    .timestamp("ts")
                    .returns("""
                            ts\tprice
                            2022-01-02T12:00:00.000000Z\t100.0
                            2022-01-03T12:00:00.000000Z\t150.0
                            """);
            // A negative stride shifts the bound the other way, so the union keeps its own constraint.
            assertQuery("SELECT * FROM (SELECT dateadd('h', -1, timestamp) as ts, price FROM trades) " +
                    "WHERE ts NOT IN NULL AND ts > '2022-01-02'")
                    .timestamp("ts")
                    .returns("""
                            ts\tprice
                            2022-01-02T11:00:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testNullBoundOffsetPushdownReturnsEmpty() throws Exception {
        // A NULL timestamp bound makes the inner predicate unsatisfiable, so the temp interval model
        // becomes an empty set. The merge must intersect this model to empty rather than consume
        // the predicate with no constraint; otherwise the offset pushdown returns every row instead of
        // none (the mirror of the multi-interval bug fixed in testMultiIntervalOffsetPushdown, and of
        // the self-comparison one in testSelfComparisonOffsetPushdownContradictionReturnsEmpty).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z'), " +
                    "(150, '2022-01-02T12:00:00.000000Z'), (200, '2023-01-01T12:00:00.000000Z');");

            final String greater = "SELECT * FROM (SELECT dateadd('d', -1, timestamp) as ts, price FROM trades) WHERE ts > null::timestamp";
            // The unsatisfiable model reaches the code generator as intrinsicValue = FALSE, so the scan
            // is skipped outright instead of opening an interval scan over an empty interval list.
            // This also pins isStaticTimestampPredicate() treating the cast bound as static: were the
            // "cast" FUNCTION node rejected, the predicate would degrade to a residual filter and the
            // plan would scan every row to return none.
            assertQuery(greater)
                    .timestamp("ts")
                    .withPlanContaining("Empty table")
                    .returns("ts\tprice\n");
            assertQuery("SELECT * FROM (SELECT dateadd('d', -1, timestamp) as ts, price FROM trades) WHERE ts < null::timestamp")
                    .timestamp("ts")
                    .returns("ts\tprice\n");
            assertQuery("SELECT * FROM (SELECT dateadd('d', -1, timestamp) as ts, price FROM trades) WHERE ts = cast(null as timestamp)")
                    .timestamp("ts")
                    .returns("ts\tprice\n");
        });
    }

    @Test
    public void testNullOffsetThrowsError() throws Exception {
        // Ensure a NULL stride is rejected
        assertMemoryLeak(() -> execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;"));
        assertQuery("SELECT * FROM (SELECT dateadd('h', NULL, timestamp) as ts, price FROM trades) WHERE ts IN '2022'")
                .fails(35, "`null` is not a valid stride");
    }

    @Test
    public void testOffsetShiftUnconstrainedSourceIntervalStaysUnconstrained() throws Exception {
        // "tt <= <Long.MAX_VALUE>" is a tautology: the source interval is open at BOTH ends and its
        // preimage is the whole domain, whatever the shift. Inverting the sentinels one at a time
        // truncates the upper one to Long.MAX_VALUE - shift and drops every timestamp whose forward
        // dateadd wraps to the bottom of the range - rows that satisfy a predicate every row
        // satisfies. Both spellings answered 1 of 2 rows, so invertConstantShift short-circuits the
        // open/open interval before it touches either bound.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tnf (ts TIMESTAMP_NS, v INT) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL");
            execute("INSERT INTO tnf VALUES ('2020-01-01T00:00:00Z', 1), ('2262-04-11T12:00:00Z', 2)");

            // The second row's shifted timestamp wraps to 1677, so the projection is no longer
            // ascending and cannot be asserted as a designated-timestamp cursor. Pin the row identity
            // instead: the tautology has to keep both rows whichever spelling carries it.
            final String bothRows = "v\n1\n2\n";
            assertQuery("SELECT v FROM tnf WHERE dateadd('d', 1, ts) <= 9223372036854775807")
                    .noLeakCheck()
                    .returns(bothRows);
            // The pushed spelling consumes the predicate, so its cursor is a plain scan with a
            // known size - which is exactly why a wrong interval silently returned one row.
            assertQuery("SELECT v FROM (SELECT dateadd('d', 1, ts) tt, v FROM tnf) WHERE tt <= 9223372036854775807")
                    .noLeakCheck()
                    .expectSize()
                    .returns(bothRows);
        });
    }

    @Test
    public void testOffsetShiftWideningBoundKeepsMatchingRows() throws Exception {
        // The mirror arm: a NEGATIVE stride stores a positive offset, so it is the UPPER boundary
        // that overflows and collapses to Long.MAX_VALUE. The rows that genuinely satisfy the
        // predicate have to survive the widened scan, so this pins a non-empty answer - an
        // assertion that only ever expects an empty set cannot tell a correct scan from one that
        // prunes everything away.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tnw (ts TIMESTAMP_NS, v INT) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO tnw VALUES ('1975-01-01T00:00:00Z', 1), ('1990-01-01T00:00:00Z', 2), ('2020-01-01T00:00:00Z', 3)");

            final String oneRow = "t\tv\n2011-12-21T23:34:33.709551616Z\t1\n";

            // The un-pushed spelling is the oracle: only the 1975 row projects below the bound.
            assertQuery("SELECT dateadd('d', -200_000, ts) t, v FROM tnw WHERE dateadd('d', -200_000, ts) < '2020-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .returns(oneRow);

            // The pushed spelling must agree, and must keep the predicate as a residual filter:
            // consuming it returned all three rows.
            assertQuery("SELECT t, v FROM (SELECT dateadd('d', -200_000, ts) t, v FROM tnw) WHERE t < '2020-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .withPlanContaining("filter: dateadd('d',-200000,ts)<")
                    .returns(oneRow);
        });
    }

    @Test
    public void testOffsetShiftWideningBoundKeepsPredicate() throws Exception {
        // The wrap check has four outcomes and only two of them decline. The other two collapse the
        // boundary to an open one - Long.MAX_VALUE for an overflowing upper bound, the NULL sentinel
        // for an underflowing lower one - which widens the scan to a superset of the rows the
        // predicate admits. Nothing reported that, so the caller consumed the predicate and the
        // widened scan answered on its own, returning rows that fail it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tns (ts TIMESTAMP_NS, v INT) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO tns VALUES ('2020-01-01T00:00:00Z', 1), ('2020-06-01T00:00:00Z', 2)");

            // dateadd('d', 200_000, ts) wraps past the end of the nanos range - Nanos.addDays is a
            // plain "nanos + days * DAY_NANOS" - so both rows project below the bound and neither
            // satisfies the predicate. The un-pushed spelling is the oracle.
            assertQuery("SELECT dateadd('d', 200_000, ts) t, v FROM tns WHERE dateadd('d', 200_000, ts) > '2000-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .returns("t\tv\n");

            // The pushed-down spelling must agree rather than returning every row.
            assertQuery("SELECT t, v FROM (SELECT dateadd('d', 200_000, ts) t, v FROM tns) WHERE t > '2000-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .returns("t\tv\n");

            // A two-sided predicate loses only the wrapped conjunct, so the answer is wrong without
            // being empty: the '<' bound survives and admits both rows.
            assertQuery("SELECT t, v FROM (SELECT dateadd('d', 200_000, ts) t, v FROM tns) WHERE t > '2000-01-01T00:00:00Z' AND t < '2100-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .returns("t\tv\n");
        });
    }

    @Test
    public void testOffsetShiftWrapIntoRangeKeepsMicrosPruningAndRows() throws Exception {
        // The micros counterpart of the nanos tests below. The designated-timestamp ceiling is
        // 9999-12-31, ~284000 years short of Long.MAX_VALUE, so no realistic stride can wrap a
        // storable timestamp into the requested range and the pushdown must stay - including the
        // OPEN upper bound, which the inverse would otherwise pin at the unreachable
        // Long.MAX_VALUE - shift. A stride large enough to wrap the shift itself is the one micros
        // shape that does lose rows without the guard.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tmu (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL");
            execute("INSERT INTO tmu VALUES ('2020-01-01T00:00:00Z', 1), ('2021-01-01T00:00:00Z', 2)");

            // Ordinary strides keep both bounds and both sentinels.
            assertQuery("SELECT tt, v FROM (SELECT dateadd('h', 3, ts) tt, v FROM tmu) WHERE tt > '2020-06-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("tt")
                    .withPlanContaining("Interval forward scan on: tmu")
                    .withPlanContaining("\"MAX\"")
                    .returns("tt\tv\n2021-01-01T03:00:00.000000Z\t2\n");
            assertQuery("SELECT tt, v FROM (SELECT dateadd('h', 3, ts) tt, v FROM tmu) WHERE tt < '2020-06-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("tt")
                    .withPlanContaining("Interval forward scan on: tmu")
                    .withPlanContaining("\"MIN\"")
                    .returns("tt\tv\n2020-01-01T03:00:00.000000Z\t1\n");

            // A stride big enough that the forward dateadd carries a storable micros timestamp past
            // Long.MAX_VALUE and back to the bottom of the range. Both rows then satisfy the bound,
            // and the pushed spelling pruned both away before the guard existed.
            final String bothWrapped = """
                    tt\tv
                    -290263-07-10T15:58:10.448384Z\t1
                    -290262-07-11T15:58:10.448384Z\t2
                    """;
            assertQuery("SELECT dateadd('w', 15_250_000, ts) tt, v FROM tmu WHERE dateadd('w', 15_250_000, ts) < '2020-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("tt")
                    .returns(bothWrapped);
            assertQuery("SELECT tt, v FROM (SELECT dateadd('w', 15_250_000, ts) tt, v FROM tmu) WHERE tt < '2020-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("tt")
                    .returns(bothWrapped);
        });
    }

    @Test
    public void testOffsetShiftWrapIntoRangeKeepsRowsOnOneSidedPredicates() throws Exception {
        // The BETWEEN twin below declines because the '>' boundary's OWN shift wraps. The one-sided
        // spellings have no wrapping boundary, so nothing declined and the pushdown was consumed with
        // an interval that models the wrong preimage:
        // - "t < bound" left the open LOWER sentinel where it was. The forward dateadd wraps every
        //   timestamp above Long.MAX_VALUE - shift back to the bottom of the range, so those rows do
        //   satisfy the predicate, yet the computed [open, bound - shift] scan pruned all of them.
        // - "t > bound" left the open UPPER sentinel where it was, which is the mirror error: the
        //   same wrapped rows do NOT satisfy that predicate, and the scan returned every one of them.
        // Both spellings now go through MonotonicTimestampFunction.invertConstantShift, the inverse
        // the row-filter spelling has always used, which declines the first and computes the finite
        // Long.MAX_VALUE - shift upper bound for the second.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tnt (ts TIMESTAMP_NS, v INT) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO tnt VALUES ('2020-01-01T00:00:00Z', 1), ('2211-01-01T00:00:00Z', 2), " +
                    "('2214-01-01T00:00:00Z', 3), ('2250-01-01T00:00:00Z', 4), ('2261-01-01T00:00:00Z', 5)");

            final String allFive = """
                    t\tv
                    1709-03-28T00:25:26.290448384Z\t1
                    1900-03-28T00:25:26.290448384Z\t2
                    1903-03-29T00:25:26.290448384Z\t3
                    1939-03-29T00:25:26.290448384Z\t4
                    1950-03-29T00:25:26.290448384Z\t5
                    """;

            // Every row's forward dateadd wraps back below the bound, so all five match.
            assertQuery("SELECT dateadd('d', 100_000, ts) t, v FROM tnt WHERE dateadd('d', 100_000, ts) < '2020-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .returns(allFive);
            assertQuery("SELECT t, v FROM (SELECT dateadd('d', 100_000, ts) t, v FROM tnt) WHERE t < '2020-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .returns(allFive);

            // The mirror: the same wrapped values are all BELOW the bound, so none match.
            assertQuery("SELECT dateadd('d', 100_000, ts) t, v FROM tnt WHERE dateadd('d', 100_000, ts) > '2020-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .returns("t\tv\n");
            assertQuery("SELECT t, v FROM (SELECT dateadd('d', 100_000, ts) t, v FROM tnt) WHERE t > '2020-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .returns("t\tv\n");

            // A NEGATIVE stride cannot wrap a non-negative designated timestamp out of the range, so
            // both sentinels keep their exact finite preimage and the pushdown stays. Pinned
            // non-empty: an assertion that only ever expects an empty set cannot tell a correct scan
            // from one that prunes everything away.
            final String twoRows = """
                    t\tv
                    1976-03-18T00:00:00.000000000Z\t4
                    1987-03-19T00:00:00.000000000Z\t5
                    """;
            assertQuery("SELECT dateadd('d', -100_000, ts) t, v FROM tnt WHERE dateadd('d', -100_000, ts) > '1950-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .returns(twoRows);
            assertQuery("SELECT t, v FROM (SELECT dateadd('d', -100_000, ts) t, v FROM tnt) WHERE t > '1950-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .returns(twoRows);
        });
    }

    @Test
    public void testOffsetShiftWrapIntoRangeKeepsRowsOnTwoConjunctPredicate() throws Exception {
        // The two-conjunct spelling of the BETWEEN window below: '>' and '<' become separate
        // and_offset nodes, so the decline that covers the pair inside one node does not apply. The
        // '<' shift does not wrap on its own, and it used to be consumed alone - pruning away the very
        // rows whose forward dateadd wrapped back into the window. The test comment on
        // testOffsetShiftWrappingBoundKeepsRowsOnBetweenPredicate recorded this shape as unfixed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tnt2 (ts TIMESTAMP_NS, v INT) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO tnt2 VALUES ('2020-01-01T00:00:00Z', 1), ('2211-01-01T00:00:00Z', 2), " +
                    "('2214-01-01T00:00:00Z', 3), ('2250-01-01T00:00:00Z', 4), ('2261-01-01T00:00:00Z', 5)");

            final String fourRows = """
                    t\tv
                    1900-03-28T00:25:26.290448384Z\t2
                    1903-03-29T00:25:26.290448384Z\t3
                    1939-03-29T00:25:26.290448384Z\t4
                    1950-03-29T00:25:26.290448384Z\t5
                    """;

            assertQuery("SELECT dateadd('d', 100_000, ts) t, v FROM tnt2 " +
                    "WHERE dateadd('d', 100_000, ts) > '1900-01-01T00:00:00Z' AND dateadd('d', 100_000, ts) < '2020-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .returns(fourRows);
            assertQuery("SELECT t, v FROM (SELECT dateadd('d', 100_000, ts) t, v FROM tnt2) " +
                    "WHERE t > '1900-01-01T00:00:00Z' AND t < '2020-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .returns(fourRows);
        });
    }

    @Test
    public void testOffsetShiftWrapIntoRangeOnNestedAndOffset() throws Exception {
        // Two stacked projections produce NESTED and_offset wrappers. Only the outermost one shifts
        // the designated timestamp itself; the inner one's input is the outer one's output, which
        // can exceed the driver's storage ceiling by the accumulated shift. The inner level
        // therefore has to fall back to Long.MAX_VALUE as its wrap ceiling, exactly as
        // MonotonicTimestampFunction.shiftInputCeiling does for a chain of shift functions.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tnn (ts TIMESTAMP_NS, v INT) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL");
            execute("INSERT INTO tnn VALUES ('2020-01-01T00:00:00Z', 1), ('2211-01-01T00:00:00Z', 2), " +
                    "('2261-01-01T00:00:00Z', 3)");

            // Every row's doubly-shifted timestamp wraps back below the bound, so all three match.
            final String allThree = """
                    t2\tv
                    1709-03-29T00:25:26.290448384Z\t1
                    1900-03-29T00:25:26.290448384Z\t2
                    1950-03-30T00:25:26.290448384Z\t3
                    """;
            assertQuery("SELECT dateadd('d', 1, dateadd('d', 100_000, ts)) t2, v FROM tnn " +
                    "WHERE dateadd('d', 1, dateadd('d', 100_000, ts)) < '1990-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .returns(allThree);
            assertQuery("SELECT t2, v FROM (SELECT dateadd('d', 1, t) t2, v FROM " +
                    "(SELECT dateadd('d', 100_000, ts) t, v FROM tnn)) WHERE t2 < '1990-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t2")
                    .returns(allThree);

            // Micros is where the two levels answer DIFFERENTLY, so it is the arm that discriminates
            // a polarity inversion. The inner level runs first, against the Long.MAX_VALUE ceiling
            // its already-shifted input demands, so it keeps the finite Long.MAX_VALUE - 1 day rather
            // than restoring the open sentinel. The outer level then subtracts its own 2 days from
            // that finite bound, pinning Long.MAX_VALUE - 3 days. Invert the polarity and the inner
            // level restores the sentinel instead, leaving Long.MAX_VALUE - 2 days - one day later.
            execute("CREATE TABLE tmn (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL");
            execute("INSERT INTO tmn VALUES ('2020-01-01T00:00:00Z', 1), ('2021-01-01T00:00:00Z', 2)");
            assertQuery("SELECT t2, v FROM (SELECT dateadd('d', 1, t) t2, v FROM " +
                    "(SELECT dateadd('d', 2, ts) t, v FROM tmn)) WHERE t2 > '2020-06-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t2")
                    .withPlanContaining("294247-01-07T04:00:54.775807Z")
                    .returns("t2\tv\n2021-01-04T00:00:00.000000Z\t2\n");
        });
    }

    @Test
    public void testOffsetShiftWrappingBoundKeepsRowsOnBetweenPredicate() throws Exception {
        // The preimage of a WRAPPED shift is not an interval. [lo - D, hi - D] with only lo - D
        // wrapping splits into two pieces, and collapsing the wrapped bound to the open sentinel
        // keeps the piece below hi - D while losing the one above lo - D outright. That is not a
        // superset, so no residual filter can repair it - a filter only ever removes rows. A wrap
        // therefore declines the pushdown outright instead of widening the bound.
        //
        // BETWEEN carries both bounds in ONE and_offset node, so the decline covers the pair. The
        // two-conjunct spelling of the same window is NOT fixed by this: '>' and '<' become
        // separate nodes, and the '<' shift does not wrap, so it is consumed on its own and prunes
        // away the very rows whose forward dateadd wrapped back into the window. Repairing that
        // needs the preimage modelled as a ring arc (one interval, or two when the arc crosses the
        // wrap point), which also covers the one-sided spelling that is wrong on master today.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tnt (ts TIMESTAMP_NS, v INT) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO tnt VALUES ('2020-01-01T00:00:00Z', 1), ('2211-01-01T00:00:00Z', 2), " +
                    "('2214-01-01T00:00:00Z', 3), ('2250-01-01T00:00:00Z', 4), ('2261-01-01T00:00:00Z', 5)");

            final String fourRows = "t\tv\n" +
                    "1900-03-28T00:25:26.290448384Z\t2\n" +
                    "1903-03-29T00:25:26.290448384Z\t3\n" +
                    "1939-03-29T00:25:26.290448384Z\t4\n" +
                    "1950-03-29T00:25:26.290448384Z\t5\n";

            // The un-pushed spelling is the oracle: four rows wrap back into the bounded window.
            assertQuery("SELECT dateadd('d', 100_000, ts) t, v FROM tnt " +
                    "WHERE dateadd('d', 100_000, ts) > '1900-01-01T00:00:00Z' AND dateadd('d', 100_000, ts) < '2020-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .returns(fourRows);

            // The pushed spelling must return them too. Widening the wrapped '>' bound to the open
            // sentinel while keeping the finite '<' bound pruned every one of them away.
            assertQuery("SELECT t, v FROM (SELECT dateadd('d', 100_000, ts) t, v FROM tnt) " +
                    "WHERE t BETWEEN '1900-01-01T00:00:00Z' AND '2020-01-01T00:00:00Z'")
                    .noLeakCheck()
                    .timestamp("t")
                    .returns(fourRows);
        });
    }

    @Test
    public void testOffsetShiftWrappingOutOfRangeDeclinesPushdown() throws Exception {
        // The overflow check detects a WRAP, not a mathematical excursion, and dateadd wraps too --
        // Nanos.addDays is a plain "nanos + days * DAY_NANOS". At 200_000 days the stride exceeds
        // 2^63, so the projection lands back inside the range about 37 years ABOVE the source
        // timestamp, and the rows genuinely satisfy the predicate. Declaring the scan empty here
        // would silently drop them; the pushdown has to decline and let the residual row filter
        // re-check each row with the same wrapping arithmetic. MonotonicTimestampFunction's
        // invertConstantShift already returns NONE for this hazard, so both spellings must agree.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP_NS, x INT) TIMESTAMP(ts) PARTITION BY YEAR;");
            execute("INSERT INTO tab VALUES ('2020-06-01T00:00:00.000000000Z', 1);");

            // What the projection actually produces: the wrap puts it in 2057, not out of range.
            assertQuery("SELECT dateadd('d', -200_000, ts) AS t, x FROM tab")
                    .timestamp("t")
                    .expectSize()
                    .returns("""
                            t\tx
                            2057-05-21T23:34:33.709551616Z\t1
                            """);

            // The pushed-down form must agree with it rather than returning nothing.
            assertQuery("SELECT * FROM (SELECT dateadd('d', -200_000, ts) AS t, x FROM tab) WHERE t > '2020-01-01'")
                    .timestamp("t")
                    .returns("""
                            t\tx
                            2057-05-21T23:34:33.709551616Z\t1
                            """);

            // The same predicate spelled without the sub-query goes through invertConstantShift,
            // which declines for the same reason. The two spellings must return the same rows.
            assertQuery("SELECT dateadd('d', -200_000, ts) AS t, x FROM tab WHERE dateadd('d', -200_000, ts) > '2020-01-01'")
                    .timestamp("t")
                    .returns("""
                            t\tx
                            2057-05-21T23:34:33.709551616Z\t1
                            """);
        });
    }

    @Test
    public void testOriginalTimestampPrecedenceNoOffsetPushdown() throws Exception {
        // When both original timestamp AND dateadd are in projection, filtering on the
        // dateadd column should NOT use and_offset pushdown (original timestamp is designated)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T00:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-01T02:30:00.000000Z');");

            // Query with both timestamp columns and filter on the dateadd column
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, timestamp as original_ts, price FROM trades
                    ) WHERE ts >= '2022-01-01T00:00:00' AND ts < '2022-01-01T01:00:00'
                    """;

            // Row 1: ts = 2021-12-31 23:30 (NOT in range)
            // Row 2: ts = 2022-01-01 00:30 (in range)
            // Row 3: ts = 2022-01-01 01:30 (NOT in range)
            // Plan should NOT show interval pushdown - filter stays at outer level
            // because original timestamp takes precedence (no ts_offset set)
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("original_ts")
                    .withPlan("""
                            Filter filter: (ts>=2022-01-01T00:00:00.000000Z and ts<2022-01-01T01:00:00.000000Z)
                                VirtualRecord
                                  functions: [dateadd('h',-1,timestamp),timestamp,price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """)
                    .returns("""
                            ts\toriginal_ts\tprice
                            2022-01-01T00:30:00.000000Z\t2022-01-01T01:30:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testOriginalTimestampPrecedenceOverDateadd() throws Exception {
        // Test that when BOTH the original timestamp AND a dateadd column are in the projection,
        // the original timestamp takes precedence as the designated timestamp.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:00:00.000000Z');");

            // Query where dateadd column comes before the literal timestamp column
            String query = """
                    SELECT dateadd('h', -1, timestamp) as ts, timestamp, price FROM trades
                    """;

            // The result should have 'timestamp' as the designated timestamp column (index 1),
            // not 'ts' (index 0), because the original timestamp takes precedence
            // Plan should show VirtualRecord - no ts_offset because original timestamp is present
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .expectSize()
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),timestamp,price]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trades
                            """)
                    .returns("""
                            ts\ttimestamp\tprice
                            2022-01-01T00:00:00.000000Z\t2022-01-01T01:00:00.000000Z\t100.0
                            2022-01-01T01:00:00.000000Z\t2022-01-01T02:00:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testOriginalTimestampPrecedenceOverDateaddQualifiedAlias() throws Exception {
        // Test that when the original timestamp is referenced through a table alias (t.timestamp),
        // it should still take precedence over dateadd columns.
        // This is a regression test for the case where Chars.equalsIgnoreCase fails to match
        // qualified column names like "t.timestamp" against "timestamp".
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:00:00.000000Z');");

            // Query with table alias and qualified timestamp reference
            // The inner query uses alias 't' and references t.timestamp
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, t.timestamp) as ts, t.timestamp as original_ts, price FROM trades t
                    ) WHERE ts >= '2022-01-01T00:00:00' AND ts < '2022-01-01T01:00:00'
                    """;

            // Row 1: ts = 2022-01-01 00:00 (in range), original_ts = 2022-01-01 01:00
            // Row 2: ts = 2022-01-01 01:00 (NOT in range)
            // Plan should NOT show interval pushdown because original timestamp (t.timestamp) is in projection
            // Filter should stay at outer level, not be pushed down with offset
            // Note: qualified column reference results in SelectedRecord wrapper and alias renaming
            assertQuery(query)
                    .noLeakCheck()
                    .withPlan("""
                            Filter filter: (ts>=2022-01-01T00:00:00.000000Z and ts<2022-01-01T01:00:00.000000Z)
                                VirtualRecord
                                  functions: [dateadd('h',-1,timestamp),original_ts,price]
                                    SelectedRecord
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                            """)
                    .returns("""
                            ts\toriginal_ts\tprice
                            2022-01-01T00:00:00.000000Z\t2022-01-01T01:00:00.000000Z\t100.0
                            """);
        });
    }

    @Test
    public void testPositiveOffsetPushdown() throws Exception {
        // Test positive offset (dateadd adds time, so pushdown subtracts)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            // Row 1: timestamp 2021-12-31 22:00 -> ts (after +2h) = 2022-01-01 00:00 (in 2022)
            execute("INSERT INTO trades VALUES (100, '2021-12-31T22:00:00.000000Z');");
            // Row 2: timestamp 2021-12-31 21:00 -> ts (after +2h) = 2021-12-31 23:00 (NOT in 2022)
            execute("INSERT INTO trades VALUES (150, '2021-12-31T21:00:00.000000Z');");
            // Row 3: timestamp 2022-12-31 22:00 -> ts (after +2h) = 2023-01-01 00:00 (NOT in 2022)
            execute("INSERT INTO trades VALUES (200, '2022-12-31T22:00:00.000000Z');");
            // Row 4: timestamp 2022-12-31 21:00 -> ts (after +2h) = 2022-12-31 23:00 (in 2022)
            execute("INSERT INTO trades VALUES (250, '2022-12-31T21:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', 2, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022'
                    """;

            // Verify correct data: rows 1, 4
            // Plan shows interval pushdown with -2h offset (inverse of +2)
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',2,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2021-12-31T22:00:00.000000Z","2022-12-31T21:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:00:00.000000Z\t100.0
                            2022-12-31T23:00:00.000000Z\t250.0
                            """);
        });
    }

    @Test
    public void testQualifiedColumnNameInDateaddExpression() throws Exception {
        // Test that qualified column names like "trades.timestamp" in the dateadd expression
        // are correctly recognized as referencing the designated timestamp
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:30:00.000000Z');");

            // Use qualified column reference in dateadd expression: trades.timestamp
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, trades.timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022'
                    """;

            // Verify correct data and plan shows interval pushdown - qualified trades.timestamp should be detected
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:30:00.000000Z\t100.0
                            2022-01-01T01:30:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testQualifiedColumnNameInPredicatePushdown() throws Exception {
        // Test that qualified column names like "v.ts" in WHERE clause are correctly matched
        // and rewritten for timestamp predicate pushdown
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:30:00.000000Z');");

            // Use explicit table alias with qualified column reference in WHERE
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, price FROM trades
                    ) v WHERE v.ts IN '2022'
                    """;

            // Verify correct data and plan shows interval pushdown even with qualified column name v.ts
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:30:00.000000Z\t100.0
                            2022-01-01T01:30:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testQualifiedPredicateWithQualifiedSourceTimestamp() throws Exception {
        // Test that when rewriting a qualified predicate (v.ts) to a qualified source (t.timestamp),
        // we don't produce a double-qualified result like "v.t.timestamp".
        // This is a regression test for rewriteColumnToken incorrectly preserving prefix.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:30:00.000000Z');");

            // Query with table alias 't' in dateadd, and outer alias 'v' with qualified predicate v.ts
            // The dateadd references t.timestamp, and the WHERE uses v.ts
            // When rewriting v.ts -> t.timestamp, we should get t.timestamp, not v.t.timestamp
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, t.timestamp) as ts, price FROM trades t
                    ) v WHERE v.ts IN '2022'
                    """;

            // Verify correct data and plan shows interval pushdown - the qualified references should be handled correctly
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:30:00.000000Z\t100.0
                            2022-01-01T01:30:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testQuotedColumnNamePushdown() throws Exception {
        // Test that quoted column names like "ts" are correctly matched for pushdown
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:30:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:30:00.000000Z');");

            // Use quoted column reference in WHERE clause
            String query = """
                    SELECT * FROM (
                        SELECT dateadd('h', -1, timestamp) as ts, price FROM trades
                    ) WHERE "ts" IN '2022'
                    """;

            // Verify correct data and plan shows interval pushdown even with quoted column name
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('h',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T01:00:00.000000Z","2023-01-01T00:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:30:00.000000Z\t100.0
                            2022-01-01T01:30:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testRejectPredicateAndWithNow() throws Exception {
        // Predicate ts > '2022-01-01' AND ts < now()
        // The constant part can be pushed down, but the now() part stays as filter
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts > '2022-01-01' AND ts < now()
                    """;

            // The constant part is pushed down, now() part stays as filter
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: ts<now()
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trades
                                          intervals: [("2022-01-02T00:00:00.000001Z","MAX")]
                            """);
        });
    }

    @Test
    public void testRejectPredicateBetweenWithNow() throws Exception {
        // Predicate ts BETWEEN ... AND now() should NOT be pushed down
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts BETWEEN dateadd('d', -7, now()) AND now()
                    """;

            // Plan should show Filter (not Interval forward scan)
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: ts between dateadd('d',-7,now()) and now()
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRejectPredicateOrWithNow() throws Exception {
        // Predicate ts > '2025-01-01' OR ts < now() should NOT be pushed down
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts > '2025-01-01' OR ts < now()
                    """;

            // Plan should show Filter (not Interval forward scan)
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: (2025-01-01T00:00:00.000000Z<ts or ts<now())
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRejectPredicateWithDateaddNow() throws Exception {
        // Predicate ts > dateadd('d', -7, now()) should NOT be pushed down
        // because dateadd uses now() instead of timestamp
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts > dateadd('d', -7, now())
                    """;

            // Plan should show Filter (not Interval forward scan) because dateadd doesn't use timestamp
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: dateadd('d',-7,now())<ts
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRejectPredicateWithDateaddSysdate() throws Exception {
        // Predicate ts > dateadd('h', -1, sysdate()) should NOT be pushed down
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts > dateadd('h', -1, sysdate())
                    """;

            // Plan should show Filter (not Interval forward scan)
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: dateadd('h',-1,sysdate())<ts
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRejectPredicateWithNow() throws Exception {
        // Predicate ts > now() should NOT be pushed down
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts > now()
                    """;

            // Plan should show Filter (not Interval forward scan) because now() is rejected
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: now()<ts
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRejectPredicateWithSysdate() throws Exception {
        // Predicate ts > sysdate() should NOT be pushed down
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts > sysdate()
                    """;

            // Plan should show Filter (not Interval forward scan) because sysdate() is rejected
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: sysdate()<ts
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRejectPredicateWithSystimestamp() throws Exception {
        // Predicate ts > systimestamp() should NOT be pushed down
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('d', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts > systimestamp()
                    """;

            // Plan should show Filter (not Interval forward scan) because systimestamp() is rejected
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: systimestamp()<ts
                                VirtualRecord
                                  functions: [dateadd('d',-1,timestamp),price]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRuntimeConstBoundOffsetDeclinesPushdown() throws Exception {
        // A runtime-constant bound must NOT be baked into an interval scan: its value is only known at
        // execution time, so isStaticTimestampPredicate() rejects the predicate and SqlOptimiser never
        // wraps it in and_offset. The predicate stays a plain residual filter over the virtual column
        // and the scan keeps its full frame.
        //
        // This test previously claimed to cover analyzeAndOffset's residual free of a compiled bound.
        // It never did: alloc_ts() is a general FUNCTION node, which is exactly what the gate above
        // rejects, so no wrapper - and no temp interval model - is ever built for it. Deleting that
        // free left the whole class green. The plan assertion below pins what the query actually
        // exercises, so the test fails if the bound ever starts being pushed into an interval scan.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES " +
                    "(100, '2020-01-01T00:30:00.000000Z')," +   // tt = 2019-12-31T23:30
                    "(150, '2020-06-01T00:30:00.000000Z')," +   // tt = 2020-05-31T23:30
                    "(200, '2020-12-01T00:30:00.000000Z');");   // tt = 2020-11-30T23:30

            assertQuery("SELECT * FROM (SELECT dateadd('h',-1,timestamp) tt, price FROM trades) " +
                    "WHERE tt > alloc_ts('2020-05-31T23:00:00.000000Z'::timestamp)")
                    .timestamp("tt")
                    .withPlanContaining("Frame forward scan on: trades")
                    .returns("""
                            tt\tprice
                            2020-05-31T23:30:00.000000Z\t150.0
                            2020-11-30T23:30:00.000000Z\t200.0
                            """);
        });
    }

    @Test
    public void testRuntimeConstBoundOffsetWithNullBoundReturnsEmpty() throws Exception {
        // Companion to testRuntimeConstBoundOffsetDeclinesPushdown. The NULL bound is static, so its
        // half IS analysed and empties the model; the runtime-constant half stays a residual filter.
        // The result must be empty rather than every row - the mirror of the multi-interval bug in
        // testMultiIntervalOffsetPushdown.
        //
        // Like its companion, this used to claim it covered mergeWithAddMethod's free on the
        // isEmptySet() early return. It does not, and cannot: no runtime-constant bound survives
        // isStaticTimestampPredicate(), so nothing owning native memory ever reaches that builder.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES " +
                    "(100, '2020-01-01T00:30:00.000000Z')," +
                    "(150, '2020-06-01T00:30:00.000000Z')," +
                    "(200, '2020-12-01T00:30:00.000000Z');");

            assertQuery("SELECT * FROM (SELECT dateadd('h',-1,timestamp) tt, price FROM trades) " +
                    "WHERE tt > alloc_ts('2020-05-31T23:00:00.000000Z'::timestamp) AND tt > null::timestamp")
                    .timestamp("tt")
                    .returns("tt\tprice\n");
        });
    }

    @Test
    public void testSecondOffsetPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T00:00:30.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T00:01:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('s', -30, timestamp) as ts, price FROM trades
                    ) WHERE ts >= '2022-01-01T00:00:00' AND ts < '2022-01-01T00:00:30'
                    """;

            // Row 1: ts = 2022-01-01 00:00:00 (in range)
            // Row 2: ts = 2022-01-01 00:00:30 (NOT in range, >= boundary)
            // Verify plan shows interval pushdown with +30 second offset
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('s',-30,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-01T00:00:30.000000Z","2022-01-01T00:00:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T00:00:00.000000Z\t100.0
                            """);
        });
    }

    @Test
    public void testSelfComparisonOffsetPushdownContradictionReturnsEmpty() throws Exception {
        // "ts != ts" is a contradiction that analyzeNotEquals0 folds by setting intrinsicValue = FALSE
        // alone - it never touches the interval builder. mergeIntervalModelWithAddMethod must carry that
        // FALSE across to this model and intersect it to empty; otherwise the builder sees no intervals,
        // reports the predicate as fully represented, and the caller consumes it with no constraint at
        // all - the offset pushdown then returns every row instead of none. Same shape as the NULL bound
        // fixed in testNullBoundOffsetPushdownReturnsEmpty, reached through a different analyze method.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z'), " +
                    "(150, '2022-01-02T12:00:00.000000Z'), (200, '2023-01-01T12:00:00.000000Z');");

            // CONTROL: without the offset the contradiction already folds to an empty scan.
            assertQuery("SELECT timestamp, price FROM trades WHERE timestamp != timestamp")
                    .timestamp("timestamp")
                    .returns("timestamp\tprice\n");

            assertQuery("SELECT * FROM (SELECT dateadd('d', -1, timestamp) as ts, price FROM trades) WHERE ts != ts")
                    .timestamp("ts")
                    .withPlanContaining("Empty table")
                    .returns("ts\tprice\n");
            // The <> spelling parses to the same node.
            assertQuery("SELECT * FROM (SELECT dateadd('d', -1, timestamp) as ts, price FROM trades) WHERE ts <> ts")
                    .timestamp("ts")
                    .returns("ts\tprice\n");
            // The contradiction must also win when the conjunction contributes a real interval first:
            // the FALSE check has to run before the interval merge, not after it.
            assertQuery("SELECT * FROM (SELECT dateadd('d', -1, timestamp) as ts, price FROM trades) " +
                    "WHERE ts > '2022-01-01' AND ts != ts")
                    .timestamp("ts")
                    .returns("ts\tprice\n");
            // The contradiction empties the model, so it must stay confined to the AND spine: an OR
            // branch alongside it still matches every row.
            assertQuery("SELECT * FROM (SELECT dateadd('d', -1, timestamp) as ts, price FROM trades) " +
                    "WHERE ts != ts OR price > 0")
                    .timestamp("ts")
                    .returns("""
                            ts\tprice
                            2021-12-31T12:00:00.000000Z\t100.0
                            2022-01-01T12:00:00.000000Z\t150.0
                            2022-12-31T12:00:00.000000Z\t200.0
                            """);
        });
    }

    @Test
    public void testSelfComparisonOffsetPushdownTautologyReturnsAllRows() throws Exception {
        // The twin of the contradiction above: "ts = ts" is a tautology that analyzeEquals0 consumes
        // without applying an interval. That is the one shape left that legitimately reaches
        // mergeWithAddMethod with no interval applied, so it pins the "consume the predicate" arm -
        // the offset scan must return every row, not none.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z'), " +
                    "(150, '2022-01-02T12:00:00.000000Z');");

            assertQuery("SELECT * FROM (SELECT dateadd('d', -1, timestamp) as ts, price FROM trades) WHERE ts = ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tprice
                            2021-12-31T12:00:00.000000Z\t100.0
                            2022-01-01T12:00:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testStrandedAndOffsetCompilesAsResidualFilter() throws Exception {
        // moveWhereInsideSubQueries pushes an and_offset wrapper onto whatever nested model it
        // finds. A model that never reaches interval extraction - here a sub-query carrying a
        // LIMIT - handed the wrapper straight to the function compiler, which failed with
        // "unknown function name: and_offset(BOOLEAN,CHAR,INT)", leaking an internal name to the
        // user. generateFilter0 now rebuilds any stranded wrapper into its dateadd residual.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO trades VALUES
                        ('2020-01-01T10:00:00.000000Z', 1.5),
                        ('2020-01-02T10:00:00.000000Z', 2.5)
                    """);
            // Both spellings of the bound reach the same stranded wrapper; the cast one is what
            // isStaticTimestampPredicate()'s cast arm newly admits.
            assertQuery("""
                    SELECT * FROM (SELECT dateadd('h',-1,ts) tt, price FROM (SELECT * FROM trades LIMIT 10))
                    WHERE tt > '2020-01-02T08:00:00.000000Z'
                    """)
                    .timestamp("tt")
                    .returns("""
                            tt\tprice
                            2020-01-02T09:00:00.000000Z\t2.5
                            """);
            assertQuery("""
                    SELECT * FROM (SELECT dateadd('h',-1,ts) tt, price FROM (SELECT * FROM trades LIMIT 10))
                    WHERE tt > '2020-01-02T08:00:00.000000Z'::timestamp
                    """)
                    .timestamp("tt")
                    .returns("""
                            tt\tprice
                            2020-01-02T09:00:00.000000Z\t2.5
                            """);
            // A bound that admits every row, to pin that the rebuilt residual is the original
            // predicate rather than an always-false or always-true stand-in.
            assertQuery("""
                    SELECT * FROM (SELECT dateadd('h',-1,ts) tt, price FROM (SELECT * FROM trades LIMIT 10))
                    WHERE tt > '2020-01-01T00:00:00.000000Z'
                    """)
                    .timestamp("tt")
                    .returns("""
                            tt\tprice
                            2020-01-01T09:00:00.000000Z\t1.5
                            2020-01-02T09:00:00.000000Z\t2.5
                            """);
        });
    }

    @Test
    public void testTimestampOverflowReturnsEmpty() throws Exception {
        // A bound the optimiser's own inverse-offset arithmetic pushes out of the timestamp range
        // used to fail the query. The user's query is valid, so it must not: the pushdown declines
        // and the dateadd stays a residual row filter, which answers with the same rows the
        // unpushed query would.
        //
        // Here that answer is no rows, but for the runtime evaluation's reason rather than the
        // pruner's: Micros.yearMicros clamps a negative-year underflow to Long.MIN_VALUE, so both
        // rows project to about Long.MIN_VALUE, which is not > '2022-01-01'. This test therefore
        // pins "no error"; testOffsetShiftWrappingOutOfRangeDeclinesPushdown is the one that pins
        // decline-versus-empty, where the two answers actually differ.
        //
        // Long.MAX_VALUE microseconds from epoch is about year 294247. dateadd('y', -300000, ts)
        // makes the optimiser store +300000 as the inverse offset, so pushing "ts > '2022-01-01'"
        // down asks for year 302022, past the end of the micros range.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z'), " +
                    "(150, '2023-01-02T12:00:00.000000Z');");

            assertQuery("SELECT * FROM (" +
                    "SELECT dateadd('y', -300000, timestamp) as ts, price FROM trades" +
                    ") WHERE ts > '2022-01-01'")
                    .timestamp("ts")
                    .returns("ts\tprice\n");

            // CONTROL: the mirror direction, where the shift stays in range, still returns its rows.
            // Without it an over-broad "empty" would pass the assertion above.
            assertQuery("SELECT * FROM (" +
                    "SELECT dateadd('y', -1, timestamp) as ts, price FROM trades" +
                    ") WHERE ts > '2020-06-01'")
                    .timestamp("ts")
                    .returns("""
                            ts\tprice
                            2021-01-01T12:00:00.000000Z\t100.0
                            2022-01-02T12:00:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testUnknownOffsetUnitOnIndexedSymbolPathReportsInvalidPeriod() throws Exception {
        // detectTimestampOffset's parseUnitCharacter accepts ANY single character, so an invalid
        // dateadd unit still gets wrapped in and_offset. analyzeAndOffset then bails at
        // getAddMethod(unit) == null, which used to leave the wrapper in the residual. On the
        // non-indexed path generateFilter0 rebuilt it and the user saw dateadd's own
        // "invalid time period" error; on the indexed-symbol path the wrapper reached the function
        // compiler and leaked the internal name instead. Both paths must report the real error.
        assertMemoryLeak(() -> execute(
                "CREATE TABLE tab (ts TIMESTAMP, s SYMBOL INDEX, v INT) TIMESTAMP(ts) PARTITION BY DAY"));

        // The three filter-compilation paths must all report the same error. Naming the unit pins
        // that the rebuilt dateadd carries the original token rather than some other bad unit.
        // Indexed symbol and LATEST ON compile intrinsicModel.filter directly; the plain predicate
        // goes through generateFilter0, which already rebuilt stranded wrappers.
        assertQuery("SELECT * FROM (SELECT dateadd('z',1,ts) tt, s, v FROM tab) timestamp(tt) "
                + "WHERE tt IN '2022-01-01' AND s = 'k1'")
                .fails(79, "invalid time period [unit=z]");
        assertQuery("SELECT * FROM (SELECT dateadd('z',1,ts) tt, s, v FROM tab) timestamp(tt) "
                + "WHERE tt IN '2022-01-01' LATEST ON tt PARTITION BY s")
                .fails(79, "invalid time period [unit=z]");
        assertQuery("SELECT * FROM (SELECT dateadd('z',1,ts) tt, s, v FROM tab) timestamp(tt) "
                + "WHERE tt IN '2022-01-01' AND v = 1")
                .fails(79, "invalid time period [unit=z]");
    }

    @Test
    public void testUnsatisfiableKeyWithRuntimeBoundFreesModel() throws Exception {
        // A contradictory symbol key makes the WHERE clause unsatisfiable, so SqlCodeGenerator returns
        // an empty factory early (intrinsicModel.intrinsicValue == FALSE) before it builds the interval
        // model (which would transfer ownership of interval-bound functions) or clears the interval
        // filters. A runtime-constant timestamp bound already compiled into the interval builder is then
        // orphaned. alloc_ts() makes the leak observable via its tracked native buffer.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL INDEX, price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES ('a', 100, '2020-01-01T12:00:00.000000Z');");
            assertQuery("SELECT * FROM trades " +
                    "WHERE timestamp > alloc_ts('2020-01-01T00:00:00.000000Z'::timestamp) " +
                    "AND sym = 'a' AND sym = 'b'")
                    .timestamp("timestamp")
                    .returns("sym\tprice\ttimestamp\n");
        });
    }

    @Test
    public void testUnsatisfiableKeyWithRuntimeBoundLatestOnFreesModel() throws Exception {
        // LATEST ON variant of testUnsatisfiableKeyWithRuntimeBoundFreesModel: the same unsatisfiable
        // key path with a latest-by clause must also free the runtime interval bound.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL INDEX, price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES ('a', 100, '2020-01-01T12:00:00.000000Z');");
            assertQuery("SELECT * FROM trades " +
                    "WHERE timestamp > alloc_ts('2020-01-01T00:00:00.000000Z'::timestamp) " +
                    "AND sym = 'a' AND sym = 'b' " +
                    "LATEST ON timestamp PARTITION BY sym")
                    .timestamp("timestamp")
                    .returns("sym\tprice\ttimestamp\n");
        });
    }

    @Test
    public void testViewPushdown() throws Exception {
        // Test that predicates are pushed down through views with dateadd offset
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T12:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-02T12:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-03T12:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (250, '2022-01-04T12:00:00.000000Z');");

            // Create a view that wraps dateadd on the timestamp
            execute("CREATE VIEW trades_offset AS SELECT dateadd('d', -1, timestamp) as ts, price FROM trades;");

            String query = "SELECT * FROM trades_offset WHERE ts IN '2022-01-01'";

            // Verify correct data: only the row where ts = 2022-01-01 (original timestamp = 2022-01-02)
            // Plan shows interval pushdown with +1 day offset applied through the view
            // ts IN '2022-01-01' means original timestamp must be in '2022-01-02'
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('d',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-02T00:00:00.000000Z","2022-01-02T23:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T12:00:00.000000Z\t150.0
                            """);
        });
    }

    @Test
    public void testWeekOffsetPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY MONTH;");
            execute("INSERT INTO trades VALUES (100, '2022-01-08T12:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-15T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('w', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts >= '2022-01-01' AND ts < '2022-01-08'
                    """;

            // Row 1: ts = 2022-01-01 12:00 (in range)
            // Row 2: ts = 2022-01-08 12:00 (NOT in range, >= boundary)
            // Verify plan shows interval pushdown with +1 week offset
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('w',-1,timestamp),price]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("2022-01-08T00:00:00.000000Z","2022-01-14T23:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-01-01T12:00:00.000000Z\t100.0
                            """);
        });
    }

    @Test
    public void testWindowFunctionPreservesTimestamp() throws Exception {
        // Window function should preserve the designated timestamp from the nested model
        // Row ordering is maintained because window functions process rows in order
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T00:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T01:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-01T02:00:00.000000Z');");

            // Query with window function - timestamp should be preserved
            String query = "SELECT timestamp, price, row_number() OVER (ORDER BY timestamp) as rn FROM trades";

            // Verify data is correct and ordered
            // Window functions don't support random access but may know size
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            timestamp\tprice\trn
                            2022-01-01T00:00:00.000000Z\t100.0\t1
                            2022-01-01T01:00:00.000000Z\t150.0\t2
                            2022-01-01T02:00:00.000000Z\t200.0\t3
                            """);
        });
    }

    @Test
    public void testWindowFunctionTimestampConsistency() throws Exception {
        // Verify that shifted and non-shifted timestamps produce consistent results
        // The window function should see the same row ordering in both cases
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T00:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T01:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-01T02:00:00.000000Z');");

            // Query 1: Without dateadd
            String queryNoShift = "SELECT timestamp, price, row_number() OVER (ORDER BY timestamp) as rn FROM trades";

            // Query 2: With dateadd (shift by 0 - effectively same timestamps)
            String queryWithShift = """
                    SELECT ts, price, row_number() OVER (ORDER BY ts) as rn
                    FROM (SELECT dateadd('h', 0, timestamp) as ts, price FROM trades)
                    """;

            // Both should produce the same row numbers
            String expectedNoShift = """
                    timestamp\tprice\trn
                    2022-01-01T00:00:00.000000Z\t100.0\t1
                    2022-01-01T01:00:00.000000Z\t150.0\t2
                    2022-01-01T02:00:00.000000Z\t200.0\t3
                    """;

            String expectedWithShift = """
                    ts\tprice\trn
                    2022-01-01T00:00:00.000000Z\t100.0\t1
                    2022-01-01T01:00:00.000000Z\t150.0\t2
                    2022-01-01T02:00:00.000000Z\t200.0\t3
                    """;

            // Window functions don't support random access but may know size
            assertQuery(queryNoShift)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expectedNoShift);
            assertQuery(queryWithShift)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expectedWithShift);
        });
    }

    @Test
    public void testWindowFunctionWithDateaddTimestamp() throws Exception {
        // Window function with dateadd on timestamp - should still work correctly
        // The dateadd-transformed column should be usable as timestamp
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-01T03:00:00.000000Z');");

            // Query with dateadd on timestamp and window function
            String query = """
                    SELECT ts, price, row_number() OVER (ORDER BY ts) as rn
                    FROM (SELECT dateadd('h', -1, timestamp) as ts, price FROM trades)
                    """;

            // Verify data is correct - dateadd shifts timestamps by -1 hour
            // Window functions don't support random access but may know size
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tprice\trn
                            2022-01-01T00:00:00.000000Z\t100.0\t1
                            2022-01-01T01:00:00.000000Z\t150.0\t2
                            2022-01-01T02:00:00.000000Z\t200.0\t3
                            """);
        });
    }

    @Test
    public void testWindowFunctionWithDateaddTimestampAndPredicate() throws Exception {
        // Window function with dateadd timestamp and WHERE clause
        // Predicate should NOT be pushed through window function (window needs all rows first)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute("INSERT INTO trades VALUES (100, '2022-01-01T01:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (150, '2022-01-01T02:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (200, '2022-01-01T03:00:00.000000Z');");
            execute("INSERT INTO trades VALUES (250, '2022-01-02T01:00:00.000000Z');");

            // Query: dateadd shifts by -1h, then window function, then filter
            // Row 1: ts = 00:00, rn = 1
            // Row 2: ts = 01:00, rn = 2
            // Row 3: ts = 02:00, rn = 3
            // Row 4: ts = 2022-01-02 00:00, rn = 4 (outside filter)
            String query = """
                    SELECT * FROM (
                        SELECT ts, price, row_number() OVER (ORDER BY ts) as rn
                        FROM (SELECT dateadd('h', -1, timestamp) as ts, price FROM trades)
                    ) WHERE ts IN '2022-01-01'
                    """;

            // Verify window function computes row numbers BEFORE filter is applied
            // All 4 rows get numbered, then we filter to 2022-01-01
            // Window functions don't support random access
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tprice\trn
                            2022-01-01T00:00:00.000000Z\t100.0\t1
                            2022-01-01T01:00:00.000000Z\t150.0\t2
                            2022-01-01T02:00:00.000000Z\t200.0\t3
                            """);
        });
    }

    @Test
    public void testYearOffsetPushdown() throws Exception {
        // Year offset IS pushed down with calendar-aware handling
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price DOUBLE, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY YEAR;");
            // Row 1: timestamp 2023-06-15 -> ts (after -1y) = 2022-06-15 (in 2022)
            execute("INSERT INTO trades VALUES (100, '2023-06-15T12:00:00.000000Z');");
            // Row 2: timestamp 2024-06-15 -> ts (after -1y) = 2023-06-15 (NOT in 2022)
            execute("INSERT INTO trades VALUES (150, '2024-06-15T12:00:00.000000Z');");
            // Row 3: timestamp 2022-06-15 -> ts (after -1y) = 2021-06-15 (NOT in 2022)
            execute("INSERT INTO trades VALUES (200, '2022-06-15T12:00:00.000000Z');");

            String query = """
                    SELECT * FROM (
                        SELECT dateadd('y', -1, timestamp) as ts, price FROM trades
                    ) WHERE ts IN '2022'
                    """;

            // Should only return row 1.
            // 'y' is not injective - it clamps 02-29 onto 02-28 - so the upper bound has to widen past
            // the clamp stall and the predicate stays behind as a residual filter:
            //   lower: 2022-01-01 + 1 year                    = 2023-01-01
            //   upper: 2022-12-31 23:59:59 + 1 year + 3 days  = 2024-01-03 23:59:59
            // Three days is the widest any day-of-month clamp can reach ('y' alone never needs more
            // than one, since it clamps only Feb 29 onto Feb 28). Widening by a whole extra YEAR
            // instead - which is what an earlier round did - stretched the scan to 2024-12-31,
            // doubling it for no gain. See testMonthOffsetPushdownKeepsDayClampedRows
            // for the rows the un-widened bound dropped.
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('y',-1,timestamp),price]
                                Async Filter workers: 1
                                  filter: dateadd('y',-1,timestamp) in [1640995200000000,1672531199999999]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trades
                                          intervals: [("2023-01-01T00:00:00.000000Z","2024-01-03T23:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tprice
                            2022-06-15T12:00:00.000000Z\t100.0
                            """);
        });
    }

    @Test
    public void testYearOffsetPushdownLeapDayBoundKeepsFilter() throws Exception {
        // The twin of testYearOffsetPushdown, on the LOWER bound. addYears clamps only Feb 29 onto
        // Feb 28, so its stall is one day rather than the three 'M' can reach.
        // addYears(2024-02-29, +1) clamps to 2025-02-28, which shifts back to 2024-02-28 - a day
        // below the bound - so the scan starts one row too early and the filter has to drop it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE y (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR;");
            execute("""
                    INSERT INTO y VALUES
                        ('2025-02-27T00:00:00.000000Z'),
                        ('2025-02-28T00:00:00.000000Z'),
                        ('2025-03-01T00:00:00.000000Z');
                    """);

            assertQuery("""
                    SELECT * FROM (
                        SELECT dateadd('y', -1, ts) AS tt FROM y
                    ) WHERE tt >= '2024-02-29T00:00:00.000000Z'
                    """)
                    .noLeakCheck()
                    .timestamp("tt")
                    .withPlan("""
                            VirtualRecord
                              functions: [dateadd('y',-1,ts)]
                                Async Filter workers: 1
                                  filter: dateadd('y',-1,ts)>=2024-02-29T00:00:00.000000Z
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: y
                                          intervals: [("2025-02-28T00:00:00.000000Z","MAX")]
                            """)
                    .returns("""
                            tt
                            2024-03-01T00:00:00.000000Z
                            """);
        });
    }
}
