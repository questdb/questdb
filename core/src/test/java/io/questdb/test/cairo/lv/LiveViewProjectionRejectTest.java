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

import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * What a live view still turns away once projections around the window are admitted, and
 * what it says when it does.
 * <p>
 * Admitting projections widened the accepted shape considerably, so the interesting
 * question moved from "is this rejected" to "is this rejected for the reason the author
 * can act on". Every message below is asserted verbatim-ish rather than merely asserting
 * that CREATE failed: #7479 was filed because a reject fired with a message that named the
 * wrong cause, and a test that only checks for failure cannot tell the two apart.
 * <p>
 * Grouped by why the reject exists, because the groups have different futures. The shape
 * rejects are the ones a later change could lift; the purity and timestamp rejects are
 * contract, and lifting them would make a view that cannot be recomputed. So is the reject
 * of an output column whose type no table can store: the view's table takes the SELECT's
 * column types, and its WAL writer refuses such a column, so the view could never hold a
 * row. Those tests assert the message and position exactly.
 *
 * @see LiveViewProjectionTest
 * @see io.questdb.cairo.lv.LiveViewCompiledPlan
 */
public class LiveViewProjectionRejectTest extends AbstractLiveViewTest {

    private static final String FRAME = "PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING";
    private static final String WINDOW = "avg(px) OVER (" + FRAME + ")";

    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testCompiledOperationOwnsItsColumnPositions() throws Exception {
        // A compiled CREATE LIVE VIEW can run after its compiler parsed another one, as a PG
        // prepared statement executed again does. The second statement below puts every column
        // at another position and projects a type the check accepts, so a position read from
        // the parser's reusable state rather than the operation's own would point elsewhere.
        assertMemoryLeak(() -> {
            createBase();
            final String sqlA = lv("SELECT ts, sym, NULL n, " + WINDOW + " AS a FROM base");
            final String sqlB = "CREATE LIVE VIEW lv_other_name FLUSH EVERY 1s START FROM NOW AS "
                    + "SELECT ts, sym, NULL::double nn, " + WINDOW + " AS a FROM base";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (Operation opA = compiler.compile(sqlA, sqlExecutionContext).getOperation()) {
                    compiler.compile(sqlB, sqlExecutionContext);
                    try {
                        opA.execute(sqlExecutionContext, null);
                        Assert.fail("the first operation must reject its NULL column");
                    } catch (SqlException e) {
                        assertError(e, sqlA.indexOf("NULL n"), nullColumnMessage("n"));
                    }
                }
            }
            Assert.assertNull(engine.getTableTokenIfExists("lv"));
            Assert.assertNull(engine.getTableTokenIfExists("lv_other_name"));
        });
    }

    @Test
    public void testDesignatedTimestampFilterRejectSurvivesAProjection() throws Exception {
        // A WHERE on the designated timestamp compiles into an interval scan whose
        // predicate lives in the frame cursor rather than in a residual filter Function.
        // The refresh path applies only the residual filter, so every base row would slip
        // through. The projection above the window does not change that.
        assertMemoryLeak(() -> {
            createBase();
            assertRejected(
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base WHERE ts > '2026-08-07'",
                    "live view select cannot filter on the designated timestamp yet"
            );
        });
    }

    @Test
    public void testFirstUnstorableColumnIsReported() throws Exception {
        // With two offending columns, the reject names the one that comes first in the output,
        // whichever of the two types it has.
        assertMemoryLeak(() -> {
            createBase();
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, NULL a, interval(ts, ts) b, " + WINDOW + " AS w FROM base"),
                    "NULL a",
                    nullColumnMessage("a")
            );
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, interval(ts, ts) b, NULL a, " + WINDOW + " AS w FROM base"),
                    "interval(",
                    intervalColumnMessage("b")
            );
            // A view without the designated timestamp is turned away by the check that runs
            // before this one, at the view name: what it lacks comes before what it has.
            assertFailsExactly(
                    lv("SELECT sym, NULL n, " + WINDOW + " AS w FROM base"),
                    "CREATE LIVE VIEW ".length(),
                    "live view select must project the base table's designated timestamp 'ts' as a plain column"
            );
            Assert.assertNull(engine.getTableTokenIfExists("lv"));
        });
    }

    @Test
    public void testMultiPassWindowRejectSurvivesAProjection() throws Exception {
        // The multi-pass gate looks at the node the window is expected at. With a
        // projection on top, the cached factory sits one level down, and a root-only check
        // walks straight past it into the generic shape reject - trading a message that
        // says which capability is missing for one that does not. Each of these compiles
        // to a cached window under a projection.
        assertMemoryLeak(() -> {
            createBase();
            // A window ordered by something other than the designated timestamp.
            assertRejected(
                    "SELECT ts, sym, avg(px) OVER (PARTITION BY sym ORDER BY px ROWS 10 PRECEDING) + 1 AS c FROM base",
                    "requires caching or multi-pass evaluation"
            );
            // A descending scan over the designated timestamp.
            assertRejected(
                    "SELECT ts, sym, avg(px) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1 PRECEDING "
                            + "AND CURRENT ROW) + 1 AS c FROM base ORDER BY ts DESC",
                    "requires caching or multi-pass evaluation"
            );
        });
    }

    @Test
    public void testNonDeterministicProjectionIsRejected() throws Exception {
        // A live view's contents must be a pure function of the base table: the forward
        // drain never retracts a row, and an O3 or checkpoint replay recomputes ranges, so
        // a projection that cannot reproduce its own output would let the view diverge from
        // any recompute. The guard already covered the window arguments and the WHERE; it
        // covers the projections around the window on the same terms.
        assertMemoryLeak(() -> {
            createBase();
            assertRejected(
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") + rnd_double() AS c FROM base",
                    "non-deterministic function cannot be used in live view: rnd_double"
            );
            assertRejected(
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") + now()::long AS c FROM base",
                    "non-deterministic function cannot be used in live view: now"
            );
            // A pre-window projection is held to the same standard.
            assertRejected(
                    "SELECT ts, sym, px * rnd_double() AS p2, avg(px) OVER (" + FRAME + ") AS c FROM base",
                    "non-deterministic function cannot be used in live view: rnd_double"
            );
            // A bind variable has no value to reproduce at all: the view recompiles from
            // stored SQL on restart, with nothing bound.
            assertRejected(
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base WHERE px > $1",
                    "non-deterministic function cannot be used in live view"
            );
        });
    }

    @Test
    public void testProjectionOverAWindowFreeScanStillNamesTheMissingWindow() throws Exception {
        // The one case where the original message is the right one. A projection admits
        // nothing on its own - the view still needs a window function - and blaming the
        // projection here would repeat the mistake in the other direction.
        assertMemoryLeak(() -> {
            createBase();
            assertRejected(
                    "SELECT ts, sym, px * 2 AS p2 FROM base",
                    "live view select must contain at least one window function"
            );
            assertRejected(
                    "SELECT ts, sym, px FROM base",
                    "live view select must contain at least one window function"
            );
        });
    }

    @Test
    public void testResultSetShapesAroundTheWindowNameTheClause() throws Exception {
        // A projection is a per-row transform, so the refresh path can rebuild it. These
        // are not: a limit, a sort and a grouping are properties of the whole result set,
        // and no per-row rebuild reproduces one. They stay rejected, but the message now
        // names the clause instead of claiming the window function is missing - which is
        // what sent the reporter of #7479 looking in the wrong place.
        assertMemoryLeak(() -> {
            createBase();
            assertRejected(
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base LIMIT 10",
                    "LIMIT over a window function is not supported yet"
            );
            assertRejected(
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base ORDER BY sym DESC",
                    "ORDER BY over a window function is not supported yet"
            );
            assertRejected(
                    "SELECT ts, sym, sum(px - avg(px) OVER (" + FRAME + ")) AS c FROM base",
                    "GROUP BY over a window function is not supported yet"
            );
        });
    }

    @Test
    public void testShapesRejectedBeforeTheProjectionGateIsReached() throws Exception {
        // Turned away earlier than the shape walk, and pinned here so a later change to
        // that walk cannot silently start admitting one of them. The messages are not the
        // walk's own and are asserted as they stand rather than reworded.
        assertMemoryLeak(() -> {
            createBase();
            // A join is refused by the shape walk, but at the base-scan end rather than at
            // the window: the tree reaches a join factory where the leaf scan belongs.
            assertRejected(
                    "SELECT b.ts, b.sym, b.px - avg(b.px) OVER (PARTITION BY b.sym ORDER BY b.ts ROWS 10 PRECEDING) AS c "
                            + "FROM base b JOIN base b2 ON b.sym = b2.sym",
                    "live view select must be a simple scan of a single WAL base table"
            );
            // A subquery is refused by the parser's single-base-table rule.
            assertRejected(
                    "SELECT ts, sym, c FROM (SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base)",
                    "live view requires a single base table in FROM clause"
            );
            // DISTINCT over a window function is refused by the SQL compiler itself.
            assertRejected(
                    "SELECT DISTINCT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base",
                    "Window function is not allowed in context of aggregation"
            );
            // A set operation reaches the shape walk with a union factory at the root,
            // whose base chain never passes through the window, so the walk cannot see the
            // window function and reports it missing. The message understates the cause;
            // it is pinned as it stands so a change to it is a deliberate one.
            assertRejected(
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base "
                            + "UNION ALL SELECT ts, sym, px FROM base",
                    "live view select must contain at least one window function"
            );
        });
    }

    @Test
    public void testTextArrayHasNoCastToAdvise() throws Exception {
        // The text[] reject advises no cast because no cast takes a text[]. This pins that: should
        // one appear, this fails, and the reject's message should start advising it.
        assertMemoryLeak(() -> {
            createBase();
            final String sql = "SELECT ts, sym, current_schemas(px > 0)::string v FROM base";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("::"),
                    "there is no matching function `cast` with the argument types: (text[], STRING)"
            );
        });
    }

    @Test
    public void testTimestampMustSurviveTheProjection() throws Exception {
        // Every drain path stamps its rows with the view's designated timestamp, so a view
        // that projects none cannot refresh at all. Until projections were admitted, the
        // only way to lose it was to leave it out of the SELECT; now it can also be
        // computed away or shadowed by a derived column wearing its name. CREATE used to
        // accept all of these and hand back a view that faulted its way to INVALID on the
        // refresh worker, which is a worse answer than a reject: the operator has a table
        // that exists and never works.
        assertMemoryLeak(() -> {
            createBase();
            // Left out of the SELECT entirely - reachable with no projection at all, and
            // the spelling that predates them.
            assertRejected(
                    "SELECT sym, avg(px) OVER (" + FRAME + ") AS c FROM base",
                    "live view select must project the base table's designated timestamp 'ts' as a plain column"
            );
            // Left out of a projected SELECT.
            assertRejected(
                    "SELECT sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base",
                    "live view select must project the base table's designated timestamp 'ts' as a plain column"
            );
            // Computed, and aliased back to the base's own timestamp name. The output
            // column is called ts and is not the designated timestamp, so a message that
            // reported what it found would read "expected 'ts', got 'ts'".
            assertRejected(
                    "SELECT ts + 1 AS ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base",
                    "live view select must project the base table's designated timestamp 'ts' as a plain column"
            );
            // Carried through but renamed: a different reject, and one that can name both
            // sides because a designated timestamp does reach the output.
            assertRejected(
                    "SELECT ts AS t, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base",
                    "live view select cannot override the designated timestamp; expected 'ts', got 't'"
            );
        });
    }

    @Test
    public void testUnstorableColumnBehindAnEarlierRejectGetsThatReject() throws Exception {
        // A shape refused before the SELECT compiles, or a column the compiler itself refuses,
        // keeps its own message when it also carries an unstorable type.
        assertMemoryLeak(() -> {
            createBase();
            assertRejected(
                    "SELECT ts, sym, n, " + WINDOW + " AS a FROM (SELECT ts, sym, px, NULL n FROM base)",
                    "live view requires a single base table in FROM clause"
            );
            assertRejected(
                    "WITH c AS (SELECT ts, sym, px, NULL n FROM base) SELECT ts, sym, n, " + WINDOW + " AS a FROM c",
                    "live view requires a single base table in FROM clause"
            );
            assertRejected(
                    "SELECT ts, sym, sum(NULL) OVER (" + FRAME + ") AS a FROM base",
                    "window function sum does not support an untyped NULL argument"
            );
            // A constant text[] is refused when the window factory builds a column over it.
            assertRejected(
                    "SELECT ts, sym, " + WINDOW + " AS a, current_schemas(true) v FROM base",
                    "unsupported column type text[]"
            );
        });
    }

    @Test
    public void testUnstorableColumnPositionWhateverSurroundsIt() throws Exception {
        // The position comes from the parsed projection, one item per output column, so it has
        // to land on the offending expression whatever names, quotes, comments, parentheses,
        // variables or clauses surround it. The message names the column as the view's table
        // would: an unaliased column by its generated name.
        assertMemoryLeak(() -> {
            createBase();
            // unaliased
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, NULL, " + WINDOW + " AS a FROM base"),
                    "NULL,",
                    nullColumnMessage("NULL")
            );
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, interval(ts, ts), " + WINDOW + " AS a FROM base"),
                    "interval(",
                    intervalColumnMessage("interval")
            );
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, " + WINDOW + " AS a, 1:2 FROM base"),
                    ":2 FROM",
                    intervalColumnMessage("column")
            );
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, current_schemas(" + WINDOW + " > 0) FROM base"),
                    "current_schemas(",
                    textArrayColumnMessage("current_schemas")
            );
            // a quoted alias, and a keyword one
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, " + WINDOW + " AS a, NULL \"a b\" FROM base"),
                    "NULL \"a b\"",
                    nullColumnMessage("a b")
            );
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, " + WINDOW + " AS a, NULL AS \"select\" FROM base"),
                    "NULL AS",
                    nullColumnMessage("select")
            );
            // after the window, and computed from its value
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, " + WINDOW + " AS a, NULL n FROM base"),
                    "NULL n",
                    nullColumnMessage("n")
            );
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, " + WINDOW + " AS a, CASE WHEN " + WINDOW + " > 0 THEN NULL END n FROM base"),
                    "CASE",
                    nullColumnMessage("n")
            );
            // a parenthesised body
            assertUnstorableColumnRejected(
                    "CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS (SELECT ts, sym, NULL n, " + WINDOW + " AS a FROM base)",
                    "NULL n",
                    nullColumnMessage("n")
            );
            // a comment that spells the same token, ahead of the real one
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, /* NULL */ NULL n, " + WINDOW + " AS a FROM base"),
                    "NULL n",
                    nullColumnMessage("n")
            );
            // a variable: the parser substitutes the declared expression, so its position is in the DECLARE
            assertUnstorableColumnRejected(
                    lv("DECLARE @n := NULL SELECT ts, sym, @n n, " + WINDOW + " AS a FROM base"),
                    "NULL SELECT",
                    nullColumnMessage("n")
            );
            // a duplicated column, which the compiler renames
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, px, px, NULL n, " + WINDOW + " AS a FROM base"),
                    "NULL n",
                    nullColumnMessage("n")
            );
            // an alias on a base column, which a mapping below the window applies
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym AS s2, NULL n, avg(px) OVER (PARTITION BY s2 ORDER BY ts ROWS 10 PRECEDING) AS a FROM base"),
                    "NULL n",
                    nullColumnMessage("n")
            );
            // a named window
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, avg(px) OVER w AS a, NULL n FROM base WINDOW w AS (" + FRAME + ")"),
                    "NULL n",
                    nullColumnMessage("n")
            );
            // a filter on a DOUBLE and on a SYMBOL, and the ORDER BY a live view admits
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, NULL n, " + WINDOW + " AS a FROM base WHERE px > 0"),
                    "NULL n",
                    nullColumnMessage("n")
            );
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, NULL n, " + WINDOW + " AS a FROM base WHERE sym = 'A'"),
                    "NULL n",
                    nullColumnMessage("n")
            );
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, NULL n, " + WINDOW + " AS a FROM base ORDER BY ts"),
                    "NULL n",
                    nullColumnMessage("n")
            );
        });
    }

    @Test
    public void testUnstorableColumnRejectLeavesNothingBehind() throws Exception {
        // The reject runs before the view's table, files or registry entry exist, so it leaves
        // none of them behind and the name stays free.
        assertMemoryLeak(() -> {
            createBase();
            final String sql = lv("SELECT ts, sym, NULL n, " + WINDOW + " AS a FROM base");
            assertUnstorableColumnRejected(sql, "NULL n", nullColumnMessage("n"));
            assertNoLiveViewDirectory();
            assertQuery("SELECT count() FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);
            // the same statement again gets the same answer
            assertUnstorableColumnRejected(sql, "NULL n", nullColumnMessage("n"));
            // IF NOT EXISTS over a free name creates, so it checks the columns too
            final String ifNotExistsSql = "CREATE LIVE VIEW IF NOT EXISTS lv2 FLUSH EVERY 1s START FROM NOW AS "
                    + "SELECT ts, sym, NULL n, " + WINDOW + " AS a FROM base";
            assertUnstorableColumnRejected("lv2", ifNotExistsSql, "NULL n", nullColumnMessage("n"));

            // the name takes the spelling the message advises, and the view refreshes
            final String select = "SELECT ts, sym, NULL::double n, " + WINDOW + " AS a FROM base";
            execute(lv(select));
            execute("""
                    INSERT INTO base (ts, sym, px) VALUES
                      ('2026-08-07T12:00:00.000000Z','A',10.0),
                      ('2026-08-07T12:00:01.000000Z','B',20.0),
                      ('2026-08-07T12:00:02.000000Z','A',12.0)""");
            refresh();
            assertNoRefreshFaults("lv");
            assertQuery("SELECT * FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tn\ta
                            2026-08-07T12:00:00.000000Z\tA\tnull\t10.0
                            2026-08-07T12:00:01.000000Z\tB\tnull\t20.0
                            2026-08-07T12:00:02.000000Z\tA\tnull\t11.0
                            """);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, select, "lv", LOG);

            // IF NOT EXISTS over the view that now exists is a no-op that compiles nothing, as a
            // replayed DDL needs, so the NULL column it carries is not refused
            execute(sql.replace("CREATE LIVE VIEW lv", "CREATE LIVE VIEW IF NOT EXISTS lv"));
            assertQuery("SELECT \"column\", type FROM table_columns('lv')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\ttype
                            ts\tTIMESTAMP
                            sym\tSYMBOL
                            n\tDOUBLE
                            a\tDOUBLE
                            """);
        });
    }

    @Test
    public void testUnstorableColumnTypeIsRejectedAtTheColumn() throws Exception {
        // Every spelling of each type no table stores that a projection can produce: an untyped
        // NULL, including a cast to a type that has no NULL of its own, an INTERVAL, and a
        // text[] computed from the window value. The message advises a cast only where one
        // keeps the value: any typed NULL for a NULL; for an INTERVAL its two bounds, which
        // keep the interval's precision, where a cast to a string keeps milliseconds only.
        assertMemoryLeak(() -> {
            createBase();
            // NULL
            assertUnstorableColumnRejected(lv("SELECT ts, sym, NULL n, " + WINDOW + " AS a FROM base"), "NULL n", nullColumnMessage("n"));
            assertUnstorableColumnRejected(lv("SELECT ts, sym, NaN n, " + WINDOW + " AS a FROM base"), "NaN", nullColumnMessage("n"));
            assertUnstorableColumnRejected(lv("SELECT ts, sym, null n, " + WINDOW + " AS a FROM base"), "null n", nullColumnMessage("n"));
            assertUnstorableColumnRejected(lv("SELECT ts, sym, (NULL) n, " + WINDOW + " AS a FROM base"), "NULL) n", nullColumnMessage("n"));
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, CASE WHEN px > 0 THEN NULL END n, " + WINDOW + " AS a FROM base"),
                    "CASE",
                    nullColumnMessage("n")
            );
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, CASE WHEN px > 0 THEN NULL ELSE NULL END n, " + WINDOW + " AS a FROM base"),
                    "CASE",
                    nullColumnMessage("n")
            );
            assertUnstorableColumnRejected(lv("SELECT ts, sym, NULL::text[] n, " + WINDOW + " AS a FROM base"), "::text[]", nullColumnMessage("n"));
            assertUnstorableColumnRejected(lv("SELECT ts, sym, NULL::regclass n, " + WINDOW + " AS a FROM base"), "::regclass", nullColumnMessage("n"));
            // INTERVAL: a timestamp interval, a NULL one, and the raw one the slice operator makes
            assertUnstorableColumnRejected(lv("SELECT ts, sym, interval(ts, ts) iv, " + WINDOW + " AS a FROM base"), "interval(", intervalColumnMessage("iv"));
            assertUnstorableColumnRejected(lv("SELECT ts, sym, NULL::interval iv, " + WINDOW + " AS a FROM base"), "::interval", intervalColumnMessage("iv"));
            assertUnstorableColumnRejected(lv("SELECT ts, sym, cast(NULL AS interval) iv, " + WINDOW + " AS a FROM base"), "cast(", intervalColumnMessage("iv"));
            assertUnstorableColumnRejected(lv("SELECT ts, sym, 1:2 iv, " + WINDOW + " AS a FROM base"), ":2 iv", intervalColumnMessage("iv"));
            // text[]: a constant one never gets this far, see testUnstorableColumnBehindAnEarlierRejectGetsThatReject
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, current_schemas(" + WINDOW + " > 0) v FROM base"),
                    "current_schemas(",
                    textArrayColumnMessage("v")
            );
        });
    }

    @Test
    public void testUnstorableIntervalOnANanosecondBase() throws Exception {
        // Over a TIMESTAMP_NS base the interval's type carries the nanosecond kind above its
        // tag, and the full type is not in the set of types no table stores: only its tag is.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base_ns (ts TIMESTAMP_NS, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertUnstorableColumnRejected(
                    lv("SELECT ts, sym, interval(ts, ts) iv, " + WINDOW + " AS a FROM base_ns"),
                    "interval(",
                    intervalColumnMessage("iv")
            );
        });
    }

    @Test
    public void testUnstorableNullColumnMatchesCreateTableAsSelect() throws Exception {
        // CREATE TABLE AS SELECT refuses an untyped NULL column with this message; a live view
        // gives the same SELECT the same words, at the column rather than at the statement start.
        assertMemoryLeak(() -> {
            createBase();
            assertFailsExactly("CREATE TABLE t AS (SELECT ts, sym, NULL n FROM base)", 0, nullColumnMessage("n"));
            Assert.assertNull(engine.getTableTokenIfExists("t"));
            assertUnstorableColumnRejected(lv("SELECT ts, sym, NULL n, " + WINDOW + " AS a FROM base"), "NULL n", nullColumnMessage("n"));
        });
    }

    @Test
    public void testWindowContractRejectsSurviveAProjection() throws Exception {
        // The per-window-function contract runs on the window factory the walk found, so
        // wrapping the call in an expression must not route around it. Both of these are
        // about what the function keeps across rows, which arithmetic around its result
        // does not change.
        assertMemoryLeak(() -> {
            createBase();
            assertRejected(
                    "SELECT ts, sym, lead(px) OVER (" + FRAME + ") + 1 AS c FROM base",
                    "lead() is not supported in live views; use lag() for lookback"
            );
            assertRejected(
                    "SELECT ts, sym, row_number() OVER (PARTITION BY sym ORDER BY ts) + 1 AS c FROM base",
                    "live view unbounded window must have an ANCHOR clause"
            );
        });
    }

    private static void assertError(SqlException e, int expectedPosition, String expectedMessage) {
        TestUtils.assertEquals(expectedMessage, e.getFlyweightMessage());
        Assert.assertEquals(expectedPosition, e.getPosition());
    }

    private static void assertFailsExactly(String sql, int expectedPosition, String expectedMessage) throws Exception {
        try {
            execute(sql);
            Assert.fail("the statement must fail: " + sql);
        } catch (SqlException e) {
            assertError(e, expectedPosition, expectedMessage);
        }
    }

    private static void assertNoLiveViewDirectory() {
        final File[] lvDirs = new File(engine.getConfiguration().getDbRoot()).listFiles(
                f -> f.getName().equals("lv") || f.getName().startsWith("lv~")
        );
        Assert.assertNotNull(lvDirs);
        Assert.assertEquals(0, lvDirs.length);
    }

    private static void assertUnstorableColumnRejected(String sql, String positionToken, String expectedMessage) throws Exception {
        assertUnstorableColumnRejected("lv", sql, positionToken, expectedMessage);
    }

    // The expected position is where positionToken occurs in sql; it must occur exactly once.
    private static void assertUnstorableColumnRejected(
            String viewName,
            String sql,
            String positionToken,
            String expectedMessage
    ) throws Exception {
        final int position = sql.indexOf(positionToken);
        Assert.assertTrue("'" + positionToken + "' must occur once in: " + sql, position > -1 && position == sql.lastIndexOf(positionToken));
        assertFailsExactly(sql, position, expectedMessage);
        Assert.assertNull(engine.getTableTokenIfExists(viewName));
    }

    private static String intervalColumnMessage(String columnName) {
        return "cannot create INTERVAL-type column, please use interval_start(" + columnName + "), interval_end(" + columnName + ")";
    }

    private static String lv(String selectSql) {
        return "CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " + selectSql;
    }

    // CREATE TABLE AS SELECT's wording for the same column
    private static String nullColumnMessage(String columnName) {
        return "cannot create NULL-type column, please use type cast, e.g. " + columnName + "::type";
    }

    private static String textArrayColumnMessage(String columnName) {
        return "cannot create text[]-type column [column=" + columnName + "]";
    }

    private void assertRejected(String selectSql, String expectedMessage) throws Exception {
        assertQuery("CREATE LIVE VIEW lv_rejected FLUSH EVERY 1s START FROM NOW AS " + selectSql)
                .failsWith(expectedMessage);
    }

    private void createBase() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void refresh() {
        drainWalQueue();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            driveRefreshToQuiescence(job);
        }
    }
}
