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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshSqlExecutionContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.str.Utf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * CREATE LIVE VIEW refuses a bind variable anywhere in the SELECT it stores, at the variable's token.
 * <p>
 * CREATE compiles the stored SELECT in the caller's context, where the caller's bound values are visible, but
 * every refresh and every restart compiles the same text in the refresh job's context, with nothing bound. A
 * value bound at CREATE could never be reproduced: the view would hold NULL where the caller bound a value,
 * filter on nothing, or fail its first refresh or restart and go INVALID. So the parse CREATE runs over the
 * SELECT refuses each variable it reads, with the wording the determinism guard uses for the variable in WHERE
 * ({@link LiveViewProjectionRejectTest}), wherever it stands: the projection, an expression, WHERE, a window,
 * a sub-query, a set operation, ORDER BY, LIMIT or DECLARE. With several variables the first one written wins.
 * <p>
 * The refusal is CREATE's alone. The body of a view the SELECT reads is not the SELECT's text, a refresh and
 * EXPLAIN compile as before, and CREATE LIVE VIEW IF NOT EXISTS over an existing view compiles nothing, so a
 * view an earlier release stored with a variable keeps refreshing and its DDL keeps replaying.
 *
 * @see LiveViewCompileErrorPositionTest
 */
public class LiveViewBindVariableRejectTest extends AbstractLiveViewTest {
    // A caller that binds nothing
    private static final Binding NOTHING = service -> {
    };
    private static final String NON_DETERMINISTIC_MESSAGE = "non-deterministic function cannot be used in live view: ";
    private static final String PREFIX = "CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM BEGINNING AS ";
    // The rows of a sum(x) window per sym over base, for the syms other holds: A and B
    private static final String ROWS_OF_A_AND_B = """
            ts\tsym\ts
            2026-04-01T00:00:00.000000Z\tA\t1.0
            2026-04-01T00:00:01.000000Z\tB\t2.0
            2026-04-01T01:00:02.000000Z\tA\t4.0
            """;
    private static final String VIEW_SELECT_NO_WINDOW = "live view select must contain at least one window function";
    private static final String W = "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING)";

    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testAViewStoredWithABindVariableKeepsRefreshing() throws Exception {
        // A release before the refusal stored such views. The refresh compiles the stored SELECT in its own
        // context, with nothing bound, so the column reads NULL as it did then: the refusal is CREATE's, and it
        // must not turn a stored view INVALID on its next refresh or restart. A context that reports a live view
        // refresh creates the view the way that release did, since the parse is not armed there. The restart's
        // recovery and the refresh after it run in one refresh job: the job that compiles such a view defines
        // the variable in its own context only, so a cursor another job opens finds it undefined.
        assertMemoryLeak(() -> {
            createTables();
            final String select = "SELECT ts, sym, $1 v, " + W + " s FROM base";
            try (
                    SqlExecutionContextImpl releaseBeforeTheRefusal = new SqlExecutionContextImpl(engine, 1) {
                        @Override
                        public boolean isLiveViewRefresh() {
                            return true;
                        }
                    }.with(sqlExecutionContext.getSecurityContext(), bindVariableService)
            ) {
                bindVariableService.setStr(0, "abc");
                execute(PREFIX + select, releaseBeforeTheRefusal);
                bindVariableService.clear();
            }
            refresh();
            // a restart: the registry drops the view, and registers it again from disk
            engine.getLiveViewRegistry().clear();
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.releaseInactive();
            engine.buildViewGraphs();
            execute("INSERT INTO base VALUES ('2026-04-01T02:00:00.000000Z', 'A', 5), ('2026-04-01T02:00:01.000000Z', 'B', 6)");
            refresh();
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertFalse(String.valueOf(instance.getInvalidationReason()), instance.isInvalid());
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            ts\tsym\tv\ts
                            2026-04-01T00:00:00.000000Z\tA\t\t1.0
                            2026-04-01T00:00:01.000000Z\tB\t\t2.0
                            2026-04-01T01:00:02.000000Z\tA\t\t4.0
                            2026-04-01T01:00:03.000000Z\tC\t\t4.0
                            2026-04-01T02:00:00.000000Z\tA\t\t9.0
                            2026-04-01T02:00:01.000000Z\tB\t\t8.0
                            """);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "SELECT ts, sym, " + W + " s FROM base", "SELECT ts, sym, s FROM lv", LOG);

            // IF NOT EXISTS over the view compiles nothing, in the default context too
            bindVariableService.setStr(0, "abc");
            execute("CREATE LIVE VIEW IF NOT EXISTS lv FLUSH EVERY 1s START FROM BEGINNING AS " + select);
            bindVariableService.clear();
            assertShowCreate(select);
        });
    }

    @Test
    public void testAnchorExpressionKeepsItsOwnRefusal() throws Exception {
        // The CREATE statement's own parse checks the ANCHOR EXPRESSION before createLiveView compiles anything,
        // so a variable there keeps its own message. A variable elsewhere in an anchored view is refused like any
        // other.
        assertMemoryLeak(() -> {
            createTables();
            final String anchor = PREFIX + "SELECT ts, x, sum(x) OVER w AS s FROM base "
                    + "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION ts + $1)";
            bindVariableService.setInt(0, 1);
            assertCreateRejected("lv", anchor, positionOf(anchor, "$1"), "ANCHOR EXPRESSION must not reference bind variables");
            bindVariableService.clear();
            assertSelectRejected(
                    "SELECT ts, x, $1 v, sum(x) OVER w s FROM base "
                            + "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))",
                    "$1",
                    service -> service.setStr(0, "abc")
            );
        });
    }

    @Test
    public void testBareBindVariableColumnIsRejected() throws Exception {
        // A variable as a column of its own, however the column and the statement around it are written. Today an
        // unaliased one fails with an internal error, the window code generator looking it up by name.
        assertMemoryLeak(() -> {
            createTables();
            final Binding abc = service -> service.setStr(0, "abc");
            assertSelectRejected("SELECT ts, sym, $1, " + W + " s FROM base", "$1", abc);
            assertSelectRejected("SELECT ts, sym, ($1) v, " + W + " s FROM base", "$1", abc);
            assertSelectRejected("SELECT ts, sym, $1 AS \"select\", " + W + " s FROM base", "$1", abc);
            // the first of two
            assertRejected(PREFIX + "SELECT ts, sym, $1 v, $1 w, " + W + " s FROM base", "$1", "$1 v", abc);
            // after the window, aliased and not
            assertSelectRejected("SELECT ts, sym, " + W + " s, $1 v FROM base", "$1", abc);
            assertSelectRejected("SELECT ts, sym, " + W + " s, $1 FROM base", "$1", abc);
            // an alias that shadows the column the window reads
            assertSelectRejected("SELECT ts, sym, $1 x, " + W + " s FROM base", "$1", service -> service.setInt(0, 5));
            assertSelectRejected(
                    "SELECT ts, sym, $2 v, " + W + " s FROM base",
                    "$2",
                    service -> {
                        service.setStr(0, "zz");
                        service.setStr(1, "abc");
                    }
            );
            // indexes no caller can bind
            assertSelectRejected("SELECT ts, sym, $0 v, " + W + " s FROM base", "$0", NOTHING);
            assertSelectRejected("SELECT ts, sym, $99999999999 v, " + W + " s FROM base", "$99999999999", NOTHING);
            assertRejected(PREFIX + "(\n  SELECT ts, sym, $1 v, " + W + " s FROM base\n)", "$1", "$1", abc);
            assertRejected(PREFIX + "SELECT ts, sym,\n  $1 v,\n  " + W + " s\nFROM base", "$1", "$1", abc);
            assertRejected(
                    "create live view lv flush every 1s start from beginning as select ts, sym, $1 v, "
                            + "sum(x) over (partition by sym order by ts rows 10 preceding) s from base",
                    "$1",
                    "$1",
                    abc
            );
            assertRejected(PREFIX + "/* c */ SELECT ts, sym, $1 v, " + W + " s FROM base", "$1", "$1", abc);
        });
    }

    @Test
    public void testBindVariableComparedWithEveryColumnTypeIsRejected() throws Exception {
        // The refusal reads the token, not the type the variable would take, so it refuses the variable next to a
        // column of every type a base can store.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE base_types (
                      ts TIMESTAMP, sym SYMBOL, x INT, string_col STRING, varchar_col VARCHAR, char_col CHAR,
                      boolean_col BOOLEAN, byte_col BYTE, short_col SHORT, long_col LONG, float_col FLOAT,
                      double_col DOUBLE, date_col DATE, timestamp_col TIMESTAMP, uuid_col UUID, ipv4_col IPv4,
                      long256_col LONG256, geohash_col GEOHASH(5c), decimal_col DECIMAL(18,3), array_col DOUBLE[]
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
            final String[] conditions = {
                    "sym = $1", "x = $1", "string_col = $1", "varchar_col = $1", "char_col = $1", "boolean_col = $1",
                    "byte_col = $1", "short_col = $1", "long_col = $1", "float_col = $1", "double_col = $1",
                    "date_col = $1", "timestamp_col = $1", "uuid_col = $1", "ipv4_col = $1", "long256_col = $1",
                    "geohash_col = $1", "decimal_col = $1", "array_col[1] = $1"
            };
            for (String condition : conditions) {
                assertSelectRejected("SELECT ts, sym, " + W + " s FROM base_types WHERE " + condition, "$1", NOTHING);
            }
            execute("CREATE TABLE base_ns (ts TIMESTAMP_NS, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertSelectRejected("SELECT ts, sym, $1 v, " + W + " s FROM base_ns", "$1", service -> service.setStr(0, "abc"));
        });
    }

    @Test
    public void testBindVariableInDeclareIsRejected() throws Exception {
        // A DECLARE value is the SELECT's text too, read or not, and the refusal points at the variable in the
        // DECLARE block. A constant there still makes a view.
        assertMemoryLeak(() -> {
            createTables();
            final Binding abc = service -> service.setStr(0, "abc");
            assertSelectRejected("DECLARE @v := $1 SELECT ts, sym, @v v, " + W + " s FROM base", "$1", abc);
            assertSelectRejected("DECLARE @v := $1 SELECT ts, sym, " + W + " s FROM base", "$1", abc);
            assertSelectRejected(
                    "DECLARE @l := ($1, $2) SELECT ts, sym, " + W + " s FROM base WHERE sym IN @l",
                    "$1",
                    service -> {
                        service.setStr(0, "A");
                        service.setStr(1, "B");
                    }
            );
            assertViewMatchesPlainSelect("DECLARE @v := 'abc' SELECT ts, sym, @v v, " + W + " s FROM base");
        });
    }

    @Test
    public void testBindVariableInSubQueryOrSetOperationIsRejected() throws Exception {
        // A sub-query in WHERE and the branches of a set operation are parsed with the SELECT, and so are the CTE
        // bodies, SAMPLE BY operands, table functions, GROUP BY keys, ORDER BY and LIMIT inside them.
        assertMemoryLeak(() -> {
            createTables();
            final String where = "SELECT ts, sym, " + W + " s FROM base WHERE ";
            final Binding isTrue = service -> service.setBoolean(0, true);
            final Binding one = service -> service.setInt(0, 1);
            assertSelectRejected(where + "sym IN (SELECT sym FROM other WHERE $1)", "$1", isTrue);
            assertSelectRejected(where + "x > (SELECT min(x) FROM other WHERE $1)", "$1", isTrue);
            assertSelectRejected(where + "x IN (SELECT x FROM other WHERE x > $1)", "$1", one);
            assertSelectRejected(where + "sym IN (SELECT $1 FROM other)", "$1", service -> service.setStr(0, "A"));
            assertSelectRejected(where + "sym IN (SELECT sym FROM other LIMIT $1)", "$1", service -> service.setLong(0, 1));
            assertSelectRejected(
                    "SELECT b.ts, b.sym, sum(b.x) OVER (PARTITION BY b.sym ORDER BY b.ts ROWS 10 PRECEDING) s "
                            + "FROM base b JOIN other o ON b.sym = o.sym AND o.x = $1",
                    "$1",
                    one
            );
            assertSelectRejected("SELECT ts, sym, " + W + " s FROM base UNION ALL SELECT ts, sym, $1 FROM base", "$1", one);
            assertSelectRejected("SELECT ts, sym, " + W + " s FROM base EXCEPT SELECT ts, sym, $1 FROM base", "$1", one);
            assertSelectRejected("SELECT ts, sym, " + W + " s FROM base INTERSECT SELECT ts, sym, $1 FROM base", "$1", one);
            final Binding abc = service -> service.setStr(0, "abc");
            assertSelectRejected("SELECT ts, sym, " + W + " s FROM base ORDER BY ts, $1", "$1", abc);
            assertSelectRejected("SELECT ts, sym, " + W + " s FROM base ORDER BY $1", "$1", service -> service.setLong(0, 1));
            assertSelectRejected("SELECT ts, sym, " + W + " s FROM base LIMIT $1", "$1", service -> service.setLong(0, 2));
            assertSelectRejected("SELECT ts, sym, " + W + " s FROM base LIMIT -$1", "$1", service -> service.setLong(0, 2));
            // a CTE inside a FROM sub-query of a WHERE sub-query
            assertSelectRejected(where + "sym IN (SELECT sym FROM (WITH c AS (SELECT sym FROM other WHERE $1) SELECT sym FROM c))", "$1", isTrue);
            assertSelectRejected(where + "sym IN (SELECT sym FROM (WITH c AS (SELECT sym FROM other WHERE x > $1) SELECT sym FROM c))", "$1", one);
            assertSelectRejected(where + "x > (SELECT count() FROM (WITH c AS (SELECT sym FROM other WHERE $1) SELECT sym FROM c))", "$1", isTrue);
            // a top-level CTE that a WHERE sub-query reads
            assertSelectRejected(
                    "WITH c AS (SELECT sym FROM other WHERE $1) SELECT ts, sym, " + W + " s FROM base WHERE sym IN (SELECT sym FROM c)",
                    "$1",
                    isTrue
            );
            // SAMPLE BY operands inside a sub-query
            final String sampleBy = where + "x > (SELECT max(c) FROM (SELECT ts, count() c FROM other SAMPLE BY 1h ";
            assertSelectRejected(
                    sampleBy + "FROM $1 TO $2))",
                    "$1",
                    service -> {
                        service.setTimestamp(0, ts("2026-04-01T00:00:00.000000Z"));
                        service.setTimestamp(1, ts("2026-04-02T00:00:00.000000Z"));
                    }
            );
            assertSelectRejected(sampleBy + "FROM '2026-04-01' TO $1))", "$1", service -> service.setTimestamp(0, ts("2026-04-02T00:00:00.000000Z")));
            assertSelectRejected(sampleBy + "FILL($1)))", "$1", service -> service.setInt(0, 0));
            assertSelectRejected(sampleBy + "ALIGN TO CALENDAR TIME ZONE $1))", "$1", service -> service.setStr(0, "UTC"));
            assertSelectRejected(sampleBy + "ALIGN TO CALENDAR WITH OFFSET $1))", "$1", service -> service.setStr(0, "00:30"));
            // GROUP BY keys inside a sub-query
            assertSelectRejected(where + "x IN (SELECT max(x) FROM other GROUP BY sym, x + $1)", "$1", one);
            // FROM inside a sub-query: a table function's argument, and a bare variable the CREATE statement's own
            // parse refuses first, with its own message
            assertSelectRejected(where + "x > (SELECT max(x) FROM long_sequence($1))", "$1", service -> service.setLong(0, 2));
            final String fromVariable = PREFIX + where + "x > (SELECT max(x) FROM $1)";
            bindVariableService.setStr(0, "other");
            assertCreateRejected("lv", fromVariable, positionOf(fromVariable, "$1"), "function, literal or constant is expected");
            bindVariableService.clear();
            // a set operation inside a sub-query
            assertSelectRejected(
                    where + "sym IN (SELECT sym FROM other WHERE x > 0 UNION ALL SELECT sym FROM other WHERE $1)",
                    "$1",
                    isTrue
            );

            assertViewMatchesPlainSelect(where + "sym IN (SELECT sym FROM other)");
        });
    }

    @Test
    public void testBindVariableInWhereIsRejected() throws Exception {
        // Today the determinism guard refuses a variable in WHERE only under an operator that passes its
        // arguments' non-determinism on, and then at the operator. A bare variable, an IN list, a regex, and a
        // filter on the designated timestamp get past it or are refused for another reason.
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE base_idx (ts TIMESTAMP, sym SYMBOL INDEX, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String where = "SELECT ts, sym, " + W + " s FROM base WHERE ";
            final Binding isTrue = service -> service.setBoolean(0, true);
            final Binding one = service -> service.setInt(0, 1);
            final Binding a = service -> service.setStr(0, "A");
            final Binding aAndB = service -> {
                service.setStr(0, "A");
                service.setStr(1, "B");
            };
            final Binding halfPastMidnight = service -> service.setTimestamp(0, ts("2026-04-01T00:00:00.500000Z"));
            assertSelectRejected(where + "$1", "$1", isTrue);
            assertSelectRejected(where + "NOT $1", "$1", service -> service.setBoolean(0, false));
            assertSelectRejected(where + "$1 AND x > 0", "$1", isTrue);
            assertSelectRejected(where + "x = 1 OR $1", "$1", isTrue);
            assertSelectRejected(where + "x > $1", "$1", one);
            assertSelectRejected(where + "x BETWEEN $1 AND 10", "$1", one);
            assertSelectRejected(where + "x IN ($1, 2)", "$1", one);
            assertSelectRejected(where + "sym = $1", "$1", a);
            assertSelectRejected(where + "sym != $1", "$1", a);
            assertSelectRejected(where + "sym IN ($1, $2)", "$1", aAndB);
            assertSelectRejected(where + "sym IN ('A', $1)", "$1", service -> service.setStr(0, "B"));
            assertSelectRejected(where + "sym NOT IN ($1)", "$1", a);
            assertSelectRejected(where + "sym ~ $1", "$1", a);
            assertSelectRejected(where + "sym LIKE $1", "$1", a);
            assertSelectRejected(where + "x::string = $1", "$1", service -> service.setStr(0, "1"));
            assertSelectRejected(where + "ts > $1", "$1", halfPastMidnight);
            assertSelectRejected(where + "ts IN $1", "$1", service -> service.setStr(0, "2026-04-01T01"));
            assertSelectRejected(
                    where + "ts BETWEEN $1 AND $2",
                    "$1",
                    service -> {
                        service.setTimestamp(0, ts("2026-04-01T00:00:00.500000Z"));
                        service.setTimestamp(1, ts("2026-04-02T00:00:00.000000Z"));
                    }
            );
            assertSelectRejected(where + "ts > dateadd('s', 0, $1)", "$1", halfPastMidnight);
            assertSelectRejected(where + "x > 0 AND sym IN ($1, $2)", "$1", aAndB);
            assertSelectRejected("SELECT ts, sym, " + W + " s FROM base_idx WHERE sym = $1", "$1", a);
            assertSelectRejected(where + "sym = $1 LATEST ON ts PARTITION BY sym", "$1", a);
        });
    }

    @Test
    public void testBindVariableInWindowIsRejected() throws Exception {
        // A window function's arguments, its PARTITION BY keys, its ORDER BY and its frame bounds, written inline
        // or in a named WINDOW. The live view validator of a named WINDOW's ORDER BY runs on the CREATE
        // statement's own parse, before createLiveView compiles anything, so it keeps its message.
        assertMemoryLeak(() -> {
            createTables();
            final String frame = "PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING";
            final Binding seven = service -> service.setInt(0, 7);
            final Binding one = service -> service.setInt(0, 1);
            final Binding abc = service -> service.setStr(0, "abc");
            final Binding oneRow = service -> service.setLong(0, 1);
            assertSelectRejected("SELECT ts, sym, sum($1) OVER (" + frame + ") s FROM base", "$1", seven);
            assertSelectRejected("SELECT ts, sym, first_value($1) OVER (" + frame + ") s FROM base", "$1", seven);
            assertSelectRejected("SELECT ts, sym, max($1) OVER (" + frame + ") s FROM base", "$1", service -> service.setDouble(0, 1.5));
            assertSelectRejected("SELECT ts, sym, count($1) OVER (" + frame + ") s FROM base", "$1", one);
            assertSelectRejected("SELECT ts, sym, lag(x, 1, $1) OVER (" + frame + ") s FROM base", "$1", service -> service.setInt(0, 5));
            assertSelectRejected("SELECT ts, sym, lag(x, $1) OVER (" + frame + ") s FROM base", "$1", one);
            assertSelectRejected("SELECT ts, sym, sum(x + $1) OVER (" + frame + ") s FROM base", "$1", seven);
            assertSelectRejected("SELECT ts, sym, sum(x) OVER (PARTITION BY $1 ORDER BY ts ROWS 10 PRECEDING) s FROM base", "$1", abc);
            assertSelectRejected("SELECT ts, sym, sum(x) OVER (PARTITION BY sym, $1 ORDER BY ts ROWS 10 PRECEDING) s FROM base", "$1", abc);
            assertSelectRejected("SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY $1 ROWS 10 PRECEDING) s FROM base", "$1", abc);
            assertSelectRejected("SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS $1 PRECEDING) s FROM base", "$1", oneRow);
            assertSelectRejected(
                    "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN $1 PRECEDING AND CURRENT ROW) s FROM base",
                    "$1",
                    oneRow
            );
            assertSelectRejected(
                    "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN $1 PRECEDING AND CURRENT ROW) s FROM base",
                    "$1",
                    oneRow
            );
            assertSelectRejected("SELECT ts, sym, sum(x) OVER w s FROM base WINDOW w AS (PARTITION BY $1 ORDER BY ts ROWS 10 PRECEDING)", "$1", abc);
            assertSelectRejected("SELECT ts, sym, sum(x) OVER w s FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts ROWS $1 PRECEDING)", "$1", oneRow);

            final String namedOrderBy = PREFIX + "SELECT ts, sym, sum(x) OVER w s FROM base WINDOW w AS (PARTITION BY sym ORDER BY $1 ROWS 10 PRECEDING)";
            bindVariableService.setStr(0, "abc");
            assertCreateRejected("lv", namedOrderBy, positionOf(namedOrderBy, "$1"), "live view named WINDOW must ORDER BY ts");
            bindVariableService.clear();
        });
    }

    @Test
    public void testBindVariableInsideAnExpressionIsRejectedAtTheVariable() throws Exception {
        // Today the determinism guard refuses these at the function or operator around the variable. With several
        // variables, the first one written is refused.
        assertMemoryLeak(() -> {
            createTables();
            final Binding seven = service -> service.setInt(0, 7);
            final Binding abc = service -> service.setStr(0, "abc");
            final Binding five = service -> service.setStr(0, "5");
            final Binding oneAndTwo = service -> {
                service.setInt(0, 1);
                service.setInt(1, 2);
            };
            final String[] items = {"x + $1 v", "abs($1) v", "abs(abs(abs($1))) v"};
            for (String item : items) {
                assertSelectRejected("SELECT ts, sym, " + item + ", " + W + " s FROM base", "$1", seven);
            }
            assertSelectRejected("SELECT ts, sym, concat(sym, $1) v, " + W + " s FROM base", "$1", abc);
            assertSelectRejected("SELECT ts, sym, coalesce($1, 'z') v, " + W + " s FROM base", "$1", abc);
            assertSelectRejected("SELECT ts, sym, $1::int v, " + W + " s FROM base", "$1", five);
            assertSelectRejected("SELECT ts, sym, cast($1 AS int) v, " + W + " s FROM base", "$1", five);
            assertSelectRejected("SELECT ts, sym, CASE WHEN x > 1 THEN $1 END v, " + W + " s FROM base", "$1", abc);
            assertSelectRejected("SELECT ts, sym, CASE WHEN x > $1 THEN 'a' END v, " + W + " s FROM base", "$1", service -> service.setInt(0, 1));
            assertSelectRejected("SELECT ts, sym, CASE $1 WHEN 'abc' THEN 1 ELSE 2 END v, " + W + " s FROM base", "$1", abc);
            assertSelectRejected("SELECT ts, sym, x IN ($1) v, " + W + " s FROM base", "$1", service -> service.setInt(0, 1));
            assertSelectRejected("SELECT ts, sym, ARRAY[$1] v, " + W + " s FROM base", "$1", service -> service.setDouble(0, 1.5));
            assertSelectRejected("SELECT ts, sym, " + W + " + $1 s FROM base", "$1", seven);
            assertSelectRejected("SELECT ts, sym, $1 + $2 v, " + W + " s FROM base", "$1", oneAndTwo);
            assertSelectRejected("SELECT ts, sym, $2 + $1 v, " + W + " s FROM base", "$2", oneAndTwo);
        });
    }

    @Test
    public void testBoundValueOfEveryTypeIsRejected() throws Exception {
        // Whatever the caller binds, of any type, and with nothing bound: the refresh binds nothing, so no value
        // the caller binds at CREATE ever reaches the view. Today CREATE types the column from the value, and a
        // restart compiles the variable as STRING over a column of another type.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String select = "SELECT ts, sym, $1 v, " + W + " s FROM base";
            try (DirectArray array = new DirectArray()) {
                array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                array.setDimLen(0, 2);
                array.applyShape();
                array.putDouble(0, 1.5);
                array.putDouble(1, -2.5);
                final Binding[] bindings = {
                        NOTHING,
                        service -> service.setStr(0, "abc"),
                        service -> service.setStr(0, null),
                        service -> service.setVarchar(0, new Utf8String("abc")),
                        // a 4-byte and a 2-byte UTF-8 character
                        service -> service.setVarchar(0, new Utf8String("\uD83D\uDE00\u0436")),
                        service -> service.setChar(0, 'a'),
                        service -> service.setBoolean(0, true),
                        service -> service.setByte(0, (byte) 1),
                        service -> service.setShort(0, (short) 2),
                        service -> service.setInt(0, 3),
                        service -> service.setLong(0, 4),
                        service -> service.setFloat(0, 5.5f),
                        service -> service.setDouble(0, Double.NaN),
                        service -> service.setDate(0, 86_400_000L),
                        service -> service.setTimestamp(0, ts("2026-04-01T00:00:00.000000Z")),
                        service -> service.setTimestampNano(0, 1_775_001_600_000_000_000L),
                        service -> service.setUuid(0, 1, 2),
                        service -> service.setIPv4(0, "10.0.0.1"),
                        service -> service.setLong256(0, 1, 2, 3, 4),
                        service -> service.setGeoHash(0, 31, ColumnType.getGeoHashTypeWithBits(25)),
                        service -> service.setDecimal(0, 0, 0, 0, 12_345, ColumnType.getDecimalType(18, 3)),
                        service -> service.setArray(0, array)
                };
                for (Binding binding : bindings) {
                    assertSelectRejected(select, "$1", binding);
                }
            }
        });
    }

    @Test
    public void testExplainCreateLiveViewWithABindVariableStillPrintsThePlan() throws Exception {
        // EXPLAIN CREATE LIVE VIEW creates nothing, so the refusal does not apply to it: it prints the plan of
        // the SELECT under the statement's line, the variable as the SELECT's own plan shows it.
        assertMemoryLeak(() -> {
            createTables();
            final String select = "SELECT ts, sym, $1 v, " + W + " s FROM base";
            final String selectPlan = TestUtils.printSqlToString(engine, sqlExecutionContext, "EXPLAIN " + select, sink);
            Assert.assertTrue(selectPlan, selectPlan.contains("$0::string"));
            final StringBuilder expected = new StringBuilder("QUERY PLAN\nCreate live view\n");
            final String[] selectPlanLines = selectPlan.split("\n");
            Assert.assertEquals("QUERY PLAN", selectPlanLines[0]);
            for (int i = 1; i < selectPlanLines.length; i++) {
                expected.append("    ").append(selectPlanLines[i]).append('\n');
            }
            TestUtils.printSql(engine, sqlExecutionContext, "EXPLAIN " + PREFIX + select, sink);
            TestUtils.assertEquals(expected, sink);
            Assert.assertNull(engine.getTableTokenIfExists("lv"));
        });
    }

    @Test
    public void testNamedBindVariableIsRejected() throws Exception {
        // A named variable, bound or not, is refused by the name written, case kept.
        assertMemoryLeak(() -> {
            createTables();
            assertSelectRejected("SELECT ts, sym, :v v, " + W + " s FROM base", ":v", service -> service.setStr("v", "abc"));
            assertSelectRejected("SELECT ts, sym, :v v, " + W + " s FROM base", ":v", NOTHING);
            assertSelectRejected("SELECT ts, sym, x > :v v, " + W + " s FROM base", ":v", service -> service.setInt("v", 1));
            assertSelectRejected("SELECT ts, sym, " + W + " s FROM base WHERE sym = :v", ":v", service -> service.setStr("v", "A"));
            assertSelectRejected("SELECT ts, sym, :V v, " + W + " s FROM base", ":V", service -> service.setStr("V", "abc"));
        });
    }

    @Test
    public void testParserAcceptsBindVariablesAgainAfterTheCreateCompile() throws Exception {
        // The parser refuses a variable only while it parses the SELECT that CREATE LIVE VIEW stores, and drops
        // that state however the parse ends. A compiler goes back to its pool and parses whatever comes next, so
        // after a refusal and after an armed compile that succeeds it takes variables again, in a statement and
        // in a bare expression. The live view compile flag alone, which EXPLAIN's code generation sets, arms
        // nothing.
        assertMemoryLeak(() -> {
            createTables();
            final String select = "SELECT ts, sym, $1 v, " + W + " s FROM base";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                armCreateCompile();
                try {
                    Misc.free(compiler.compile(select, sqlExecutionContext).getRecordCursorFactory());
                    Assert.fail("the armed compile must refuse the variable");
                } catch (SqlException e) {
                    TestUtils.assertEquals(NON_DETERMINISTIC_MESSAGE + "$1", e.getFlyweightMessage());
                    Assert.assertEquals("SELECT ts, sym, ".length(), e.getPosition());
                } finally {
                    disarmCreateCompile();
                }
                assertBindVariablesAccepted(compiler);

                armCreateCompile();
                try {
                    Misc.free(compiler.compile("SELECT ts, sym, " + W + " s FROM base", sqlExecutionContext).getRecordCursorFactory());
                } finally {
                    disarmCreateCompile();
                }
                assertBindVariablesAccepted(compiler);

                sqlExecutionContext.setLiveViewCompile(true);
                try (RecordCursorFactory factory = compiler.compile(select, sqlExecutionContext).getRecordCursorFactory()) {
                    assertColumnTypes(factory.getMetadata());
                } finally {
                    sqlExecutionContext.setLiveViewCompile(false);
                }
            }
        });
    }

    @Test
    public void testRefreshCompileStillAcceptsABindVariable() throws Exception {
        // The refresh context reports a live view refresh, so its compiles are never armed: a view stored with a
        // variable keeps compiling, the variable unbound and typed STRING, as before the refusal.
        assertMemoryLeak(() -> {
            createTables();
            final LiveViewRefreshSqlExecutionContext refreshContext = new LiveViewRefreshSqlExecutionContext(engine, 0);
            Assert.assertFalse(refreshContext.allowNonDeterministicFunctions());
            refreshContext.setLiveViewCompile(true);
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile("SELECT ts, sym, $1 v, " + W + " s FROM base", refreshContext)
                            .getRecordCursorFactory()
            ) {
                assertColumnTypes(factory.getMetadata());
            } finally {
                refreshContext.setLiveViewCompile(false);
            }
        });
    }

    @Test
    public void testRefusalLeavesNothingBehindAndIfNotExistsStillReplays() throws Exception {
        // A refusal of each kind ends the CREATE before the view's table, files or registry entry exist, so the
        // name takes a SELECT without a variable. IF NOT EXISTS over an existing view compiles nothing, so an
        // earlier release's DDL that carries a variable keeps replaying; over a free name it creates, and so
        // refuses the variable.
        assertMemoryLeak(() -> {
            createTables();
            final Binding abc = service -> service.setStr(0, "abc");
            assertSelectRejected("SELECT ts, sym, $1 v, " + W + " s FROM base", "$1", abc);
            assertSelectRejected("SELECT ts, sym, " + W + " s FROM base WHERE x > $1", "$1", service -> service.setInt(0, 1));
            assertSelectRejected("SELECT ts, sym, sum(x) OVER (PARTITION BY $1 ORDER BY ts ROWS 10 PRECEDING) s FROM base", "$1", abc);
            assertSelectRejected(
                    "SELECT ts, sym, " + W + " s FROM base WHERE sym IN (SELECT sym FROM other WHERE $1)",
                    "$1",
                    service -> service.setBoolean(0, true)
            );
            assertSelectRejected("DECLARE @v := $1 SELECT ts, sym, @v v, " + W + " s FROM base", "$1", abc);
            assertSelectRejected("SELECT ts, sym, :v v, " + W + " s FROM base", ":v", service -> service.setStr("v", "abc"));
            assertNoLiveViewDirectory();
            assertQuery("SELECT count() FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);

            final String select = "SELECT ts, sym, 'abc' v, " + W + " s FROM base";
            execute(PREFIX + select);
            refresh();
            assertNoRefreshFaults("lv");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, select, "lv", LOG);

            final String withVariable = "SELECT ts, sym, $1 v, " + W + " s FROM base";
            bindVariableService.setStr(0, "abc");
            execute("CREATE LIVE VIEW IF NOT EXISTS lv FLUSH EVERY 1s START FROM BEGINNING AS " + withVariable);
            bindVariableService.clear();
            assertShowCreate(select);
            final String freeName = "CREATE LIVE VIEW IF NOT EXISTS lv2 FLUSH EVERY 1s START FROM BEGINNING AS " + withVariable;
            bindVariableService.setStr(0, "abc");
            assertCreateRejected("lv2", freeName, positionOf(freeName, "$1"), NON_DETERMINISTIC_MESSAGE + "$1");
            bindVariableService.clear();
        });
    }

    @Test
    public void testTextThatOnlyLooksLikeABindVariableIsNotRefused() throws Exception {
        // The refusal reads the variables the expression parser builds, not the raw text: a string, a comment, a
        // table alias and a quoted name are not variables. Each view holds the rows its SELECT returns run
        // plainly.
        assertMemoryLeak(() -> {
            createTables();
            execute(PREFIX + "SELECT ts, sym, '$1' v, " + W + " s FROM base");
            refresh();
            assertNoRefreshFaults("lv");
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            ts\tsym\tv\ts
                            2026-04-01T00:00:00.000000Z\tA\t$1\t1.0
                            2026-04-01T00:00:01.000000Z\tB\t$1\t2.0
                            2026-04-01T01:00:02.000000Z\tA\t$1\t4.0
                            2026-04-01T01:00:03.000000Z\tC\t$1\t4.0
                            """);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "SELECT ts, sym, '$1' v, " + W + " s FROM base", "lv", LOG);
            execute("DROP LIVE VIEW lv");
            drainWalQueue();

            final String[] selects = {
                    "SELECT ts, sym, " + W + " s -- $1\nFROM base",
                    "SELECT ts, sym, /* $1 */ " + W + " s FROM base",
                    "SELECT ts, sym, " + W + " s FROM base $1",
                    "SELECT ts, sym, 'abc' v, " + W + " s FROM base",
                    "SELECT ts, sym, x v, " + W + " s FROM base"
            };
            for (String select : selects) {
                assertViewMatchesPlainSelect(select);
            }

            // a quoted name is a column name, one the base does not have
            final String quoted = PREFIX + "SELECT ts, sym, \"$1\" v, " + W + " s FROM base";
            assertCreateRejected("lv", quoted, positionOf(quoted, "\"$1\""), "Invalid column: $1");
        });
    }

    @Test
    public void testViewBodiesAreNotTheSelectText() throws Exception {
        // The body of a view the SELECT reads is another statement's text, which CREATE VIEW accepted with its
        // variables, and the compile may drop them: a column the reader does not read, an OVERRIDABLE default
        // the reader overrides, an unused DECLARE or CTE column. So the refusal leaves view bodies alone, at any
        // depth and in the copies the parser makes of a view's declared sub-query or CTE, and the statement's own
        // variables around a view read are refused at their own token.
        assertMemoryLeak(() -> {
            createTables();
            final Binding isTrue = service -> service.setBoolean(0, true);
            createView("v_bind_col", "SELECT sym, $1 k FROM other", service -> service.setStr(0, "abc"));
            createView("pv", "DECLARE OVERRIDABLE @p := $1 SELECT sym FROM other WHERE x > @p", service -> service.setInt(0, 1));
            createView("v_in_bare", "SELECT sym FROM other WHERE $1", isTrue);
            createView("v_out_bare", "SELECT sym FROM v_in_bare", isTrue);
            createView("v_unused_declare", "DECLARE @u := $1 SELECT sym FROM other", service -> service.setStr(0, "abc"));
            createView("v_cte_unused", "WITH c AS (SELECT sym, $1 k FROM other) SELECT sym FROM c", service -> service.setStr(0, "abc"));
            createView("v_declared_query", "DECLARE @q := (SELECT sym FROM other WHERE $1) SELECT sym FROM other WHERE sym IN @q AND sym IN @q", isTrue);
            // a reader's IN filter on a CTE that filters on the variable would merge with that filter, and the
            // determinism guard refuses the merged AND, so the body reads its CTE twice through a set operation
            createView("v_cte_twice", "WITH c AS (SELECT sym FROM other WHERE $1) SELECT sym FROM c UNION ALL SELECT sym FROM c", isTrue);

            // the views whose variable the reader leaves unbound, and those it binds to true
            final ViewRead[] reads = {
                    new ViewRead("v_bind_col", NOTHING),
                    new ViewRead("v_unused_declare", NOTHING),
                    new ViewRead("v_cte_unused", NOTHING),
                    new ViewRead("v_out_bare", isTrue),
                    new ViewRead("v_declared_query", isTrue),
                    new ViewRead("v_cte_twice", isTrue)
            };
            final String overriddenDefault = readerOf("DECLARE @p := 1 SELECT sym FROM pv");
            // (a) the compile CREATE runs over the SELECT takes each one, and returns the rows of the plain SELECT
            for (ViewRead read : reads) {
                bind(read.binding);
                assertCreateCompileReturns(readerOf("SELECT sym FROM " + read.view), ROWS_OF_A_AND_B);
                bindVariableService.clear();
            }
            assertCreateCompileReturns(
                    overriddenDefault,
                    """
                            ts\tsym\ts
                            2026-04-01T00:00:01.000000Z\tB\t2.0
                            """
            );

            // (b) CREATE refuses every SELECT that reads a view, for a reason of its own: the plan check finds no
            // window function under the factory that wraps a plan reading a view
            for (ViewRead read : reads) {
                bind(read.binding);
                assertCreateRejected("lv", PREFIX + readerOf("SELECT sym FROM " + read.view), "CREATE LIVE VIEW ".length(), VIEW_SELECT_NO_WINDOW);
                bindVariableService.clear();
            }
            assertCreateRejected("lv", PREFIX + overriddenDefault, "CREATE LIVE VIEW ".length(), VIEW_SELECT_NO_WINDOW);

            // (c) the statement's own variable before a view read, after it, and in the value that overrides a
            // view's default
            assertSelectRejected(
                    "SELECT ts, sym, $1 v, " + W + " s FROM base WHERE sym IN (SELECT sym FROM v_bind_col)",
                    "$1",
                    service -> service.setStr(0, "abc")
            );
            assertSelectRejected(readerOf("SELECT sym FROM v_bind_col") + " AND x > $1", "$1", service -> service.setInt(0, 1));
            assertSelectRejected(readerOf("DECLARE @p := $1 SELECT sym FROM pv"), "$1", service -> service.setInt(0, 1));
        });
    }

    private static void armCreateCompile() {
        // the flags CairoEngine.createLiveView sets around its compile of the stored SELECT
        sqlExecutionContext.setLiveViewCompile(true);
        sqlExecutionContext.setAllowNonDeterministicFunction(false);
    }

    private static void assertBindVariablesAccepted(SqlCompiler compiler) throws SqlException {
        bindVariableService.clear();
        bindVariableService.setStr(0, "abc");
        TestUtils.printSql(compiler, sqlExecutionContext, "SELECT ts, sym, $1 v FROM base", sink);
        TestUtils.assertEquals(
                """
                        ts\tsym\tv
                        2026-04-01T00:00:00.000000Z\tA\tabc
                        2026-04-01T00:00:01.000000Z\tB\tabc
                        2026-04-01T01:00:02.000000Z\tA\tabc
                        2026-04-01T01:00:03.000000Z\tC\tabc
                        """,
                sink
        );
        bindVariableService.clear();
        final ExpressionNode expression = compiler.parseExpression("$1 + 1");
        Assert.assertEquals(ExpressionNode.BIND_VARIABLE, expression.lhs.type);
        sink.clear();
        expression.toSink(sink);
        TestUtils.assertEquals("$1 + 1", sink);
    }

    // The columns of SELECT ts, sym, $1 v, W s FROM base with nothing bound: the variable is a STRING
    private static void assertColumnTypes(RecordMetadata metadata) {
        Assert.assertEquals(4, metadata.getColumnCount());
        Assert.assertEquals(ColumnType.TIMESTAMP, metadata.getColumnType(0));
        Assert.assertEquals(ColumnType.SYMBOL, metadata.getColumnType(1));
        Assert.assertEquals("v", metadata.getColumnName(2));
        Assert.assertEquals(ColumnType.STRING, metadata.getColumnType(2));
        Assert.assertEquals(ColumnType.DOUBLE, metadata.getColumnType(3));
    }

    // Compiles select with the flags CairoEngine.createLiveView sets, and reads it with them reset, as the
    // refresh reads a view's factory.
    private static void assertCreateCompileReturns(String select, String expected) throws SqlException {
        final RecordCursorFactory factory;
        armCreateCompile();
        try {
            factory = select(select);
        } catch (SqlException e) {
            throw new AssertionError(select + ": [" + e.getPosition() + "] " + e.getFlyweightMessage(), e);
        } finally {
            disarmCreateCompile();
        }
        try (factory; RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
        }
    }

    // sql fails with an SqlException of expectedMessage at expectedPosition, and leaves no view named viewName
    private static void assertCreateRejected(String viewName, String sql, int expectedPosition, String expectedMessage) {
        try {
            execute(sql);
        } catch (SqlException e) {
            Assert.assertEquals(sql, expectedMessage, Chars.toString(e.getFlyweightMessage()));
            Assert.assertEquals(sql, expectedPosition, e.getPosition());
            Assert.assertNull(sql, engine.getTableTokenIfExists(viewName));
            return;
        } catch (Throwable e) {
            throw new AssertionError("the statement must fail with an SqlException: " + sql, e);
        }
        Assert.fail("the statement must fail: " + sql);
    }

    private static void assertNoLiveViewDirectory() {
        final File[] lvDirs = new File(engine.getConfiguration().getDbRoot()).listFiles(
                f -> f.getName().equals("lv") || f.getName().startsWith("lv~")
        );
        Assert.assertNotNull(lvDirs);
        Assert.assertEquals(0, lvDirs.length);
    }

    // CREATE LIVE VIEW sql, with binding applied, is refused for its variable token at the text at, which starts
    // with the token and occurs once in sql
    private static void assertRejected(String sql, String token, String at, Binding binding) throws SqlException {
        bind(binding);
        try {
            assertCreateRejected("lv", sql, positionOf(sql, at), NON_DETERMINISTIC_MESSAGE + token);
        } finally {
            bindVariableService.clear();
        }
    }

    // CREATE LIVE VIEW lv of select, with binding applied, is refused for its variable token, which occurs once
    private static void assertSelectRejected(String select, String token, Binding binding) throws SqlException {
        assertRejected(PREFIX + select, token, token, binding);
    }

    private static void bind(Binding binding) throws SqlException {
        bindVariableService.clear();
        binding.bind(bindVariableService);
    }

    private static void createTables() throws SqlException {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE other (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                  ('2026-04-01T00:00:00.000000Z', 'A', 1),
                  ('2026-04-01T00:00:01.000000Z', 'B', 2),
                  ('2026-04-01T01:00:02.000000Z', 'A', 3),
                  ('2026-04-01T01:00:03.000000Z', 'C', 4)""");
        execute("INSERT INTO other VALUES ('2026-04-01T00:00:00.000000Z', 'A', 1), ('2026-04-01T00:00:01.000000Z', 'B', 2)");
        drainWalQueue();
    }

    private static void createView(String name, String body, Binding binding) throws SqlException {
        bind(binding);
        execute("CREATE VIEW " + name + " AS (" + body + ")");
        bindVariableService.clear();
    }

    private static void disarmCreateCompile() {
        sqlExecutionContext.setLiveViewCompile(false);
        sqlExecutionContext.setAllowNonDeterministicFunction(true);
    }

    // Where text occurs in sql. It must occur exactly once, so the expected position cannot match by accident.
    private static int positionOf(String sql, String text) {
        final int position = sql.indexOf(text);
        Assert.assertTrue("'" + text + "' must occur once in: " + sql, position > -1 && position == sql.lastIndexOf(text));
        return position;
    }

    // The window SELECT over base that keeps the syms the query over a view returns
    private static String readerOf(String viewQuery) {
        return "SELECT ts, sym, " + W + " s FROM base WHERE sym IN (" + viewQuery + ")";
    }

    private void assertShowCreate(String select) throws Exception {
        assertQuery("SHOW CREATE LIVE VIEW lv")
                .noLeakCheck()
                .noRandomAccess()
                .returns("ddl\n"
                        + "CREATE LIVE VIEW 'lv' FLUSH EVERY 1s IN MEMORY 1s PARTITION BY DAY START FROM BEGINNING AS (\n"
                        + select + "\n"
                        + ");\n");
    }

    // CREATE LIVE VIEW of select creates the view, whose seeded rows are the rows of select run plainly. The view
    // is dropped afterwards.
    private void assertViewMatchesPlainSelect(String select) throws SqlException {
        execute(PREFIX + select);
        refresh();
        assertNoRefreshFaults("lv");
        TestUtils.assertSqlCursors(engine, sqlExecutionContext, select, "lv", LOG);
        execute("DROP LIVE VIEW lv");
        drainWalQueue();
        Assert.assertNull(engine.getTableTokenIfExists("lv"));
    }

    private void refresh() {
        drainWalQueue();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            driveRefreshToQuiescence(job);
        }
    }

    // What a caller binds before the statement
    @FunctionalInterface
    private interface Binding {
        void bind(BindVariableService service) throws SqlException;
    }

    // A view a live view's SELECT reads, and what the caller binds for the variable in its body
    private record ViewRead(String view, Binding binding) {
    }
}
