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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.ResourcePoolSupervisor;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.std.Chars;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.function.Supplier;

/**
 * Where CREATE LIVE VIEW reports an error raised while it compiles its SELECT or its ANCHOR clause.
 * <p>
 * CairoEngine.createLiveView compiles the stored SELECT text on its own, so the compile positions every
 * error in that text. The statement records where the text starts, and createLiveView adds that offset
 * to the position of each exception the compile positions in the text and creates for the throw:
 * SqlException, ImplicitCastException and CairoException itself. The CairoException subclasses the
 * reader pool throws carry no position in the text, and some of them are objects every throw shares,
 * so they keep their own.
 * <p>
 * createLiveView then compiles the anchor from a text the parser renders from the typed ANCHOR
 * EXPRESSION, or synthesizes from ANCHOR DAILY. The statement does not contain that text, so an error
 * that compile positions points at the root token of the typed expression instead, or at the ANCHOR
 * keyword for ANCHOR DAILY. An anchor that compiles but fails the anchor checks (its type, a constant,
 * a non-deterministic or aggregate function) is refused at the ANCHOR keyword.
 *
 * @see LiveViewProjectionRejectTest
 */
public class LiveViewCompileErrorPositionTest extends AbstractLiveViewTest {
    // The SELECT of a view with one named window, up to the window's ANCHOR clause
    private static final String ANCHORED_SELECT = "SELECT ts, sym, sum(x) OVER w a FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts ";
    private static final String NON_DETERMINISTIC_MESSAGE = "non-deterministic function cannot be used in live view: rnd_double";
    private static final String PREFIX = "CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM BEGINNING AS ";
    private static final String W = "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING)";
    // What failing_constant() throws when a compile folds it, while a test arms it.
    private static volatile Supplier<CairoException> anchorConstantFailure;
    // What the engine throws instead of opening a reader of the base table, while a test arms it.
    private static volatile Supplier<CairoException> baseReaderFailure;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Installed before the engine is built; AbstractCairoTest.tearDownStatic resets the factory for
        // the next class. On the CREATE path only the code generator opens a reader through this
        // overload, after the optimiser has read the base's metadata, so an armed failure leaves the
        // compile the way the reader pool's own does: a locked entry, a closed pool or a failed open.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public TableReader getReader(
                    TableToken tableToken,
                    long metadataVersion,
                    @Nullable ResourcePoolSupervisor<TableReader> readerPoolSupervisor
            ) {
                final Supplier<CairoException> failure = baseReaderFailure;
                if (failure != null && Chars.equals(tableToken.getTableName(), "base")) {
                    throw failure.get();
                }
                return super.getReader(tableToken, metadataVersion, readerPoolSupervisor);
            }

            @Override
            protected Iterable<FunctionFactory> getFunctionFactories() {
                final ArrayList<FunctionFactory> factories = new ArrayList<>();
                super.getFunctionFactories().forEach(factories::add);
                factories.add(new FailingConstantFunctionFactory());
                return factories;
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @After
    public void disarmFailures() {
        anchorConstantFailure = null;
        baseReaderFailure = null;
    }

    @Test
    public void testAnchorCompileErrorLeavesTheNameFree() throws Exception {
        // An anchor compile error of each kind ends the CREATE before the view's table, files or registry
        // entry exist, so nothing is left behind and the name takes an anchor that compiles.
        assertMemoryLeak(() -> {
            createBase();
            final String badColumn = anchored("timestamp_floor('1d', nope)");
            assertCreateRejected(badColumn, SqlException.class, positionOf(badColumn, "timestamp_floor"), "Invalid column: nope");
            final String cast = anchored("ts::long + 'a'::int");
            assertCreateRejected(cast, ImplicitCastException.class, positionOf(cast, "+"), "inconvertible value: a [CHAR -> INT]");
            final String division = anchored("ts::long + (1.00m / 0.00m)::long");
            assertCreateRejected(division, CairoException.class, positionOf(division, "+"), "'/' operation failed: Division by zero");
            final String daily = anchoredBy("ANCHOR DAILY '00:00' 'Not/AZone'");
            assertCreateRejected(daily, SqlException.class, positionOf(daily, "ANCHOR"), "invalid timezone: Not/AZone");
            assertNoLiveViewDirectory();
            assertQuery("SELECT count() FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);

            final String select = ANCHORED_SELECT + "ANCHOR EXPRESSION timestamp_floor('1d', ts))";
            execute(PREFIX + select);
            Assert.assertNotNull(engine.getTableTokenIfExists("lv"));
            // IF NOT EXISTS over the view that now exists compiles nothing, so an anchor that fails to
            // compile is not refused, and the view keeps its own anchor
            execute("CREATE LIVE VIEW IF NOT EXISTS lv FLUSH EVERY 1s START FROM BEGINNING AS "
                    + ANCHORED_SELECT + "ANCHOR EXPRESSION no_such_fn(ts))");
            assertQuery("SHOW CREATE LIVE VIEW lv")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("ddl\n"
                            + "CREATE LIVE VIEW 'lv' FLUSH EVERY 1s IN MEMORY 1s PARTITION BY DAY START FROM BEGINNING AS (\n"
                            + select + "\n"
                            + ");\n");
        });
    }

    @Test
    public void testAnchorCompileErrorMatchesThePlainSelect() throws Exception {
        // The same expression in a plain SELECT fails with the same kind and message. Where the offending
        // token is the expression's root, the plain SELECT points at it too, so the CREATE reports it at
        // the plain SELECT's offset into the expression. Where a nested token offends, the plain SELECT
        // points at that token and the CREATE at the root.
        assertMemoryLeak(() -> {
            createBase();
            final String[] rootErrors = {"no_such_fn(ts)", "timestamp_floor(sym, ts)", "nope", "\"nope\""};
            for (String expression : rootErrors) {
                final String sql = anchored(expression);
                final Rejection create = executeRejected(sql);
                Assert.assertNull(engine.getTableTokenIfExists("lv"));
                final Rejection select = compileRejected("SELECT " + expression + " FROM base");
                create.assertEquals(
                        sql,
                        select.type,
                        positionOf(sql, expression) + select.position - "SELECT ".length(),
                        select.message
                );
            }
            // {the expression, its root token}
            final String[][] nestedErrors = {
                    {"timestamp_floor('1d', nope)", "timestamp_floor"},
                    {"ts::long / no_such_fn(ts)", "/"},
                    {"ts::long + 'a'::int", "+"},
                    {"ts::long + (1.00m / 0.00m)::long", "+"}
            };
            for (String[] c : nestedErrors) {
                final String sql = anchored(c[0]);
                final Rejection create = executeRejected(sql);
                Assert.assertNull(engine.getTableTokenIfExists("lv"));
                final Rejection select = compileRejected("SELECT " + c[0] + " FROM base");
                create.assertEquals(sql, select.type, positionOf(sql, c[1]), select.message);
            }
        });
    }

    @Test
    public void testAnchorCompileErrorPositionWhateverPrecedesIt() throws Exception {
        // The anchor's root token sits wherever the statement leaves it: behind lower case, newlines, a
        // parenthesised body, the optional clauses, multi-byte characters, a DECLARE, a quoted view name,
        // another named window or a long comment.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE TABLE base_ns (ts TIMESTAMP_NS, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String anchor = "ANCHOR EXPRESSION timestamp_floor('1d', nope)";
            final String[] statements = {
                    "create live view lv flush every 1s start from beginning as select ts, sym, sum(x) over w a from base "
                            + "window w as (partition by sym order by ts anchor expression timestamp_floor('1d', nope))",
                    // the anchor on a line of its own
                    "CREATE LIVE VIEW lv\n"
                            + "FLUSH EVERY 1s START FROM BEGINNING AS\n"
                            + "SELECT ts, sym, sum(x) OVER w a\n"
                            + "FROM base\n"
                            + "WINDOW w AS (\n"
                            + "  PARTITION BY sym ORDER BY ts\n"
                            + "  " + anchor + "\n"
                            + ")",
                    PREFIX + "(\n  " + ANCHORED_SELECT + anchor + ")\n)",
                    "CREATE LIVE VIEW IF NOT EXISTS lv FLUSH EVERY 1s IN MEMORY 2s PARTITION BY DAY START FROM '2026-01-01' AS "
                            + ANCHORED_SELECT + anchor + ")",
                    // a 4-byte and a 2-byte UTF-8 character in a literal of the SELECT and in a comment before the anchor
                    PREFIX + "SELECT ts, sym, '\uD83D\uDE00\u0436' e, sum(x) OVER w a FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts "
                            + "ANCHOR EXPRESSION /* \uD83D\uDE00 \u0436 */ timestamp_floor('1d', nope))",
                    // the variable sits in the DECLARE ahead of the SELECT, which reads it; the anchor does not
                    PREFIX + "DECLARE @k := 1 SELECT ts, sym, x + @k y, sum(x) OVER w a FROM base "
                            + "WINDOW w AS (PARTITION BY sym ORDER BY ts " + anchor + ")",
                    // the anchored window second of two
                    PREFIX + "SELECT ts, sym, sum(x) OVER w1 b, sum(x) OVER w a FROM base "
                            + "WINDOW w1 AS (PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING), w AS (PARTITION BY sym ORDER BY ts " + anchor + ")",
                    PREFIX + ANCHORED_SELECT + "ANCHOR EXPRESSION /* " + "c".repeat(70_000) + " */ timestamp_floor('1d', nope))",
                    PREFIX + "SELECT ts, sym, sum(x) OVER w a FROM base_ns WINDOW w AS (PARTITION BY sym ORDER BY ts " + anchor + ")"
            };
            for (String sql : statements) {
                assertCreateRejected(sql, SqlException.class, positionOf(sql, "timestamp_floor"), "Invalid column: nope");
            }
            // a keyword as the view name, quoted
            final String quotedName = "CREATE LIVE VIEW \"select\" FLUSH EVERY 1s START FROM BEGINNING AS " + ANCHORED_SELECT + anchor + ")";
            assertCreateRejected("select", quotedName, SqlException.class, positionOf(quotedName, "timestamp_floor"), "Invalid column: nope");
        });
    }

    @Test
    public void testAnchorDailyCompileErrorReportsTheAnchorKeyword() throws Exception {
        // createLiveView compiles ANCHOR DAILY as the timestamp_floor call it stands for, a text the
        // statement does not contain. An invalid time zone, the one error that call raises, points at the
        // ANCHOR keyword. Left out: a designated timestamp whose name needs quotes, which the synthesized
        // call drops (a PRE-EXISTING bug), so its CREATE fails for that reason.
        assertMemoryLeak(() -> {
            createBase();
            // {the anchor clause, the message}
            final String[][] cases = {
                    {"ANCHOR DAILY '00:00' 'Not/AZone'", "invalid timezone: Not/AZone"},
                    {"ANCHOR DAILY '08:30' 'Not/AZone'", "invalid timezone: Not/AZone"},
                    {"ANCHOR DAILY '00:00' '+25:00'", "invalid timezone: +25:00"},
                    {"ANCHOR DAILY '00:00' ''", "invalid timezone: "}
            };
            for (String[] c : cases) {
                final String sql = anchoredBy(c[0]);
                assertCreateRejected(sql, SqlException.class, positionOf(sql, "ANCHOR"), c[1]);
            }
            final String lowerCase = "create live view lv flush every 1s start from beginning as select ts, sym, sum(x) over w a from base "
                    + "window w as (partition by sym order by ts anchor daily '00:00' 'Not/AZone')";
            assertCreateRejected(lowerCase, SqlException.class, positionOf(lowerCase, "anchor"), "invalid timezone: Not/AZone");
            execute("CREATE TABLE base_ns (ts TIMESTAMP_NS, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String nanos = PREFIX + "SELECT ts, sym, sum(x) OVER w a FROM base_ns "
                    + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00' 'Not/AZone')";
            assertCreateRejected(nanos, SqlException.class, positionOf(nanos, "ANCHOR"), "invalid timezone: Not/AZone");
        });
    }

    @Test
    public void testAnchorErrorAfterBaseColumnChangeReportsItsRootToken() throws Exception {
        // A compiled CREATE LIVE VIEW executed after the column its anchor reads changed type. The SELECT
        // still compiles, and the anchor compile fails the way the plain SELECT of the expression does, at
        // the root token; a type no view anchors on is refused at the ANCHOR keyword. The base takes a valid
        // view afterwards.
        assertMemoryLeak(() -> {
            final String sql = PREFIX + "SELECT ts, sym, l, sum(x) OVER w a FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION l + 0)";
            final String plainSelect = "SELECT l + 0 FROM base";

            final Rejection toBoolean = executeAfterBaseColumnChange(sql, "ALTER TABLE base ALTER COLUMN l TYPE BOOLEAN");
            final Rejection plainBoolean = compileRejected(plainSelect);
            plainBoolean.assertEquals(plainSelect, SqlException.class, positionOf(plainSelect, "+"), "there is no matching operator `+` with the argument types: BOOLEAN + INT");
            toBoolean.assertEquals(sql, plainBoolean.type, positionOf(sql, "+"), plainBoolean.message);
            assertBaseTakesAnchoredView();

            // the same name for a column of another type
            final Rejection toUuid = executeAfterBaseColumnChange(sql, "ALTER TABLE base DROP COLUMN l", "ALTER TABLE base ADD COLUMN l UUID");
            final Rejection plainUuid = compileRejected(plainSelect);
            plainUuid.assertEquals(plainSelect, SqlException.class, positionOf(plainSelect, "+"), "there is no matching operator `+` with the argument types: UUID + INT");
            toUuid.assertEquals(sql, plainUuid.type, positionOf(sql, "+"), plainUuid.message);
            assertBaseTakesAnchoredView();

            final Rejection toDouble = executeAfterBaseColumnChange(sql, "ALTER TABLE base ALTER COLUMN l TYPE DOUBLE");
            toDouble.assertEquals(sql, SqlException.class, positionOf(sql, "ANCHOR"), "ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got DOUBLE");
            assertBaseTakesAnchoredView();
        });
    }

    @Test
    public void testAnchorExpressionCastAndCairoErrorsReportItsRootToken() throws Exception {
        // Constant folding in the anchor compile raises an ImplicitCastException for a failing cast,
        // positioned in the rendered text or not at all, and a CairoException for failing DECIMAL
        // arithmetic. Each points at the root token of the typed expression.
        assertMemoryLeak(() -> {
            createBase();
            final String[][] casts = {
                    {"ts::long + 12345::DECIMAL(4,2)::long", "inconvertible value: 12345 [INT -> DECIMAL(4,2)]"},
                    // the cast sets no position
                    {"ts::long + 'a'::int", "inconvertible value: a [CHAR -> INT]"}
            };
            for (String[] c : casts) {
                final String sql = anchored(c[0]);
                assertCreateRejected(sql, ImplicitCastException.class, positionOf(sql, "+"), c[1]);
                assertLiveViewCompileFlagReset();
            }
            // {the expression, its root token, the message}
            final String[][] arithmetic = {
                    {"ts::long + (1.00m / 0.00m)::long", "+", "'/' operation failed: Division by zero"},
                    {
                            "ts::long * (" + "9".repeat(76) + "m * 10m)::long",
                            "* (",
                            "'*' operation failed: Overflow in multiplication (256-bit \u00d7 64-bit): product exceeds 256-bit capacity"
                    }
            };
            for (String[] c : arithmetic) {
                final String sql = anchored(c[0]);
                assertCreateRejected(sql, CairoException.class, positionOf(sql, c[1]), c[2]);
                assertLiveViewCompileFlagReset();
            }
        });
    }

    @Test
    public void testAnchorExpressionCompileErrorReportsItsRootToken() throws Exception {
        // createLiveView compiles the anchor from a text the parser renders from the typed expression, so
        // the compile positions its errors in that text. Each error points at the root token of the typed
        // ANCHOR EXPRESSION instead: the outermost call, the operator applied last, or the column.
        // Parentheses and comments are not tokens of the expression. Left out: shapes the rendering does
        // not carry over faithfully (CASE, BETWEEN, a cast of a parenthesised IN, a DECLARE variable, a
        // quoted name that is a keyword or holds blanks; PRE-EXISTING bugs), whose CREATE fails or
        // succeeds for reasons of its own.
        assertMemoryLeak(() -> {
            createBase();
            // {the expression, its root token, the message}
            final String[][] cases = {
                    {"no_such_fn(ts)", "no_such_fn", "unknown function name: no_such_fn(TIMESTAMP)"},
                    {
                            "timestamp_floor(sym, ts)",
                            "timestamp_floor",
                            "there is no matching function `timestamp_floor` with the argument types: (SYMBOL, TIMESTAMP)"
                    },
                    {"timestamp_floor('1d', nope)", "timestamp_floor", "Invalid column: nope"},
                    {"timestamp_floor('1x', ts)", "timestamp_floor", "invalid unit '1x'"},
                    {"ts::long / no_such_fn(ts)", "/", "unknown function name: no_such_fn(TIMESTAMP)"},
                    {"-no_such_fn(ts)", "-no_such_fn", "unknown function name: no_such_fn(TIMESTAMP)"},
                    {"(no_such_fn(ts))", "no_such_fn", "unknown function name: no_such_fn(TIMESTAMP)"},
                    {"nope::long", "::", "Invalid column: nope"},
                    {"CAST(nope AS LONG)", "CAST", "Invalid column: nope"},
                    // left-associative: the second minus applies last
                    {"ts::long - 1 - nope", "- nope", "Invalid column: nope"},
                    // the plus binds weaker than the multiplication, so it applies last
                    {"ts::long + 2 * nope", "+", "Invalid column: nope"},
                    {"nope", "nope", "Invalid column: nope"},
                    {"/* c */ no_such_fn(ts)", "no_such_fn", "unknown function name: no_such_fn(TIMESTAMP)"},
                    {"length('a' || nope)", "length", "Invalid column: nope"},
                    {"ts::long + count()", "+", "Aggregate function cannot be passed as an argument"},
                    // the anchor reads the base columns the SELECT reads, and the SELECT does not read v
                    {"v::long", "::", "Invalid column: v"},
                    {"timestamp_floor(\n    '1d',\n    nope\n  )", "timestamp_floor", "Invalid column: nope"},
                    // a quoted name starts at its opening quote, as the plain SELECT reports it
                    {"\"nope\"", "\"nope\"", "Invalid column: nope"}
            };
            for (String[] c : cases) {
                final String sql = anchored(c[0]);
                assertCreateRejected(sql, SqlException.class, positionOf(sql, c[1]), c[2]);
                assertLiveViewCompileFlagReset();
            }
        });
    }

    @Test
    public void testAnchorFunctionIsFreedAfterTheAnchorChecks() throws Exception {
        // trim() holds native buffers from the moment the compile creates it, so assertMemoryLeak sees an
        // anchor function the CREATE does not free, whether the anchor checks refuse it or pass it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT, vc VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String select = "SELECT ts, sym, vc, sum(x) OVER w a FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION ";
            final String refused = PREFIX + select + "trim(vc))";
            assertCreateRejected(
                    refused,
                    SqlException.class,
                    positionOf(refused, "ANCHOR"),
                    "ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got VARCHAR"
            );
            execute(PREFIX + select + "length(trim(vc)))");
            Assert.assertNotNull(engine.getTableTokenIfExists("lv"));
        });
    }

    @Test
    public void testAnchorOnBlankNamedColumnIsAccepted() throws Exception {
        // A LONG column the SELECT reads is a valid anchor, whatever its name. The parser renders a quoted
        // name without its quotes, so the anchor text of a column named " " holds no token and
        // createLiveView has nothing to compile: the CREATE goes on. The refresh cannot compile that text
        // (a PRE-EXISTING bug of the rendering), so only the CREATE outcome is pinned, which stays right
        // once the rendering keeps the quotes.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT, \" \" LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String select = "SELECT ts, sym, \" \", sum(x) OVER w a FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION \" \")";
            execute(PREFIX + select);
            Assert.assertNotNull(engine.getTableTokenIfExists("lv"));
            assertLiveViewCompileFlagReset();
            assertQuery("SHOW CREATE LIVE VIEW lv")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("ddl\n"
                            + "CREATE LIVE VIEW 'lv' FLUSH EVERY 1s IN MEMORY 1s PARTITION BY DAY START FROM BEGINNING AS (\n"
                            + select + "\n"
                            + ");\n");
            assertQuery("SELECT view_status, invalidation_reason FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_status\tinvalidation_reason
                            active\t
                            """);
        });
    }

    @Test
    public void testAnchorPurityRejectStaysAtTheAnchorKeyword() throws Exception {
        // An anchor that compiles but returns a type no view anchors on, folds to a constant or aggregates
        // is refused at the ANCHOR keyword, not at the expression's root token.
        assertMemoryLeak(() -> {
            createBase();
            final String[][] cases = {
                    {"sym", "ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got SYMBOL"},
                    {"length('hello')", "ANCHOR EXPRESSION must not be a constant"},
                    {"sum(x)", "ANCHOR EXPRESSION must not contain aggregation; sum() is not allowed"}
            };
            for (String[] c : cases) {
                final String sql = anchored(c[0]);
                assertCreateRejected(sql, SqlException.class, positionOf(sql, "ANCHOR"), c[1]);
            }
            final String doubleAnchor = PREFIX + "SELECT ts, sym, v, sum(x) OVER w a FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION v)";
            assertCreateRejected(
                    doubleAnchor,
                    SqlException.class,
                    positionOf(doubleAnchor, "ANCHOR"),
                    "ANCHOR EXPRESSION must return TIMESTAMP, LONG, or INT; got DOUBLE"
            );
        });
    }

    @Test
    public void testAnchorReparseErrorReportsItsRootToken() throws Exception {
        // createLiveView parses the rendered anchor text again before it compiles it, and that parse fails
        // on its own for a searched CASE, whose rendering does not parse back (a PRE-EXISTING bug: the
        // valid CASE anchor is refused). Its error points at the root token too. Once the rendering is
        // fixed this CREATE succeeds, and the test needs another input that fails the parse.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT, l LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String sql = anchored("CASE WHEN x > 0 THEN ts::long ELSE l END");
            assertCreateRejected(sql, SqlException.class, positionOf(sql, "CASE"), "missing arguments");
            assertNoLiveViewDirectory();
        });
    }

    @Test
    public void testCompileCairoErrorReportsTheCreateStatementPosition() throws Exception {
        // DECIMAL arithmetic that constant folding cannot complete raises a CairoException at the operator.
        assertMemoryLeak(() -> {
            createBase();
            final String division = PREFIX + "SELECT ts, sym, 1.00m / 0.00m q, " + W + " s FROM base";
            assertCreateRejected(division, CairoException.class, positionOf(division, "/"), "'/' operation failed: Division by zero");
            final String parenthesisedDivision = PREFIX + "(  SELECT ts, sym, 1.00m / 0.00m q, " + W + " s FROM base )";
            assertCreateRejected(
                    parenthesisedDivision,
                    CairoException.class,
                    positionOf(parenthesisedDivision, "/"),
                    "'/' operation failed: Division by zero"
            );
            final String overflow = PREFIX + "SELECT ts, sym, " + "9".repeat(76) + "m * 10m q, " + W + " s FROM base";
            assertCreateRejected(
                    overflow,
                    CairoException.class,
                    positionOf(overflow, "*"),
                    "'*' operation failed: Overflow in multiplication (256-bit \u00d7 64-bit): product exceeds 256-bit capacity"
            );
        });
    }

    @Test
    public void testCompileErrorLeavesTheNameFree() throws Exception {
        // A compile error of each kind ends the CREATE before the view's table, files or registry entry
        // exist, so nothing is left behind and the name takes a SELECT that compiles.
        assertMemoryLeak(() -> {
            createBase();
            final String nonDeterministic = PREFIX + "SELECT ts, sym, rnd_double() + " + W + " s FROM base";
            assertCreateRejected(nonDeterministic, SqlException.class, positionOf(nonDeterministic, "rnd_double"), NON_DETERMINISTIC_MESSAGE);
            final String division = PREFIX + "SELECT ts, sym, 1.00m / 0.00m q, " + W + " s FROM base";
            assertCreateRejected(division, CairoException.class, positionOf(division, "/"), "'/' operation failed: Division by zero");
            final String cast = PREFIX + "SELECT ts, sym, 12345::DECIMAL(4,2) d, " + W + " s FROM base";
            assertCreateRejected(cast, ImplicitCastException.class, positionOf(cast, "12345"), "inconvertible value: 12345 [INT -> DECIMAL(4,2)]");
            assertNoLiveViewDirectory();
            assertQuery("SELECT count() FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            0
                            """);

            final String select = "SELECT ts, sym, " + W + " s FROM base";
            execute(PREFIX + select);
            Assert.assertNotNull(engine.getTableTokenIfExists("lv"));
            // IF NOT EXISTS over the view that now exists compiles nothing, so a body that fails to compile
            // is not refused, and the view keeps its own body
            execute("CREATE LIVE VIEW IF NOT EXISTS lv FLUSH EVERY 1s START FROM BEGINNING AS SELECT ts, sym, rnd_double() r, "
                    + W + " s FROM base");
            assertQuery("SHOW CREATE LIVE VIEW lv")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("ddl\n"
                            + "CREATE LIVE VIEW 'lv' FLUSH EVERY 1s IN MEMORY 1s PARTITION BY DAY START FROM BEGINNING AS (\n"
                            + select + "\n"
                            + ");\n");
        });
    }

    @Test
    public void testCompileErrorPositionMatchesExplainSelectAndCreateTableAsSelect() throws Exception {
        // EXPLAIN CREATE LIVE VIEW plans the SELECT it parsed out of the whole statement, so it positions
        // an error in the statement on its own; the plain SELECT positions it in the SELECT text. The
        // CREATE reports the same kind and message as both, at EXPLAIN's position less "EXPLAIN ", which
        // is the plain SELECT's position plus where the SELECT starts. CREATE TABLE AS SELECT, which also
        // compiles its SELECT text on its own, points into its statement the same way.
        assertMemoryLeak(() -> {
            createBase();
            final String[] bodies = {
                    "SELECT ts, sym, no_such_fn(x) f, " + W + " s FROM base",
                    "SELECT ts, sym, 1.00m / 0.00m q, " + W + " s FROM base",
                    "SELECT ts, sym, lag(NULL) OVER (PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING) l, " + W + " s FROM base",
                    "SELECT ts, v, percentile_cont(v, 0.5) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) p FROM base",
                    "SELECT ts, sym, 12345::DECIMAL(4,2) d, " + W + " s FROM base"
            };
            for (String body : bodies) {
                final String createSql = PREFIX + body;
                final Rejection create = executeRejected(createSql);
                Assert.assertNull(engine.getTableTokenIfExists("lv"));
                final Rejection explain = compileRejected("EXPLAIN " + createSql);
                final Rejection select = compileRejected(body);
                create.assertEquals(createSql, explain.type, explain.position - "EXPLAIN ".length(), explain.message);
                create.assertEquals(createSql, select.type, PREFIX.length() + select.position, select.message);
            }
            final String ctas = "CREATE TABLE t AS (SELECT ts, sym, no_such_fn(x) f FROM base)";
            executeRejected(ctas).assertEquals(ctas, SqlException.class, positionOf(ctas, "no_such_fn"), "unknown function name: no_such_fn(INT)");
            Assert.assertNull(engine.getTableTokenIfExists("t"));
        });
    }

    @Test
    public void testCompileErrorPositionWhateverPrecedesTheSelect() throws Exception {
        // The SELECT text starts wherever the statement leaves it: after a parenthesis, blanks, comments,
        // newlines, the optional clauses, a quoted name or multi-byte characters. The stored text keeps a
        // leading comment or DECLARE and drops the blanks and the parenthesis around it.
        assertMemoryLeak(() -> {
            createBase();
            final String select = "SELECT ts, sym, rnd_double() r, " + W + " s FROM base";
            final String[] statements = {
                    PREFIX + "(\n   " + select + "\n  )",
                    PREFIX + "\n   " + select,
                    PREFIX + "/* c */ " + select,
                    PREFIX + "-- c\n  " + select,
                    PREFIX + "( /* c */ " + select + ")",
                    // the variable's expression sits in the DECLARE, ahead of the SELECT keyword
                    PREFIX + "DECLARE @r := rnd_double() SELECT ts, sym, @r r, " + W + " s FROM base",
                    // IF NOT EXISTS over a free name creates, so it compiles the SELECT
                    "CREATE LIVE VIEW IF NOT EXISTS lv FLUSH EVERY 1s START FROM BEGINNING AS " + select,
                    "CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 2s PARTITION BY DAY START FROM '2026-01-01' AS " + select,
                    "create live view lv flush every 1s start from beginning as select ts, sym, rnd_double() r, "
                            + "sum(x) over (partition by sym order by ts rows 10 preceding) s from base",
                    // a 4-byte and a 2-byte UTF-8 character in a comment before the SELECT, and in a literal inside it
                    "CREATE LIVE VIEW lv /* \uD83D\uDE00 \u0436\u0438\u0432\u043E\u0439 */ FLUSH EVERY 1s START FROM BEGINNING AS " + select,
                    PREFIX + "SELECT ts, sym, '\uD83D\uDE00\u0436' e, rnd_double() r, " + W + " s FROM base"
            };
            for (String sql : statements) {
                assertCreateRejected(sql, SqlException.class, positionOf(sql, "rnd_double"), NON_DETERMINISTIC_MESSAGE);
            }
            // a keyword as the view name, quoted
            final String quotedName = "CREATE LIVE VIEW \"select\" FLUSH EVERY 1s START FROM BEGINNING AS " + select;
            assertCreateRejected("select", quotedName, SqlException.class, positionOf(quotedName, "rnd_double"), NON_DETERMINISTIC_MESSAGE);
            // six lines, the error on the fourth
            final String multiLine = "CREATE LIVE VIEW lv\n"
                    + "FLUSH EVERY 1s START FROM BEGINNING AS\n"
                    + "SELECT ts, sym,\n"
                    + "  lag(NULL) OVER (PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING) l,\n"
                    + "  " + W + " s\n"
                    + "FROM base";
            assertCreateRejected(
                    multiLine,
                    SqlException.class,
                    positionOf(multiLine, "lag"),
                    "window function lag does not support an untyped NULL argument; cast it to a concrete type, e.g. null::double"
            );
        });
    }

    @Test
    public void testCompileErrorReportsTheCreateStatementPosition() throws Exception {
        // Each SELECT below fails while createLiveView compiles it. The error points at the offending token
        // of the CREATE statement, where an error the parser raises points too.
        assertMemoryLeak(() -> {
            createBase();
            final String[][] cases = {
                    // the non-determinism guard createLiveView arms for the compile
                    {"SELECT ts, sym, rnd_double() + " + W + " s FROM base", "rnd_double", NON_DETERMINISTIC_MESSAGE},
                    {"SELECT ts, sym, no_such_fn(x) f, " + W + " s FROM base", "no_such_fn", "unknown function name: no_such_fn(INT)"},
                    {"SELECT ts, sym, coalesce(NULL, NULL) c, " + W + " s FROM base", "coalesce", "coalesce cannot be used with NULL data type"},
                    {"SELECT ts, sym, long_sequence(1) c, " + W + " s FROM base", "long_sequence", "cursor function cannot be used as a column [column=c]"},
                    {
                            "SELECT ts, sym, lag(NULL) OVER (PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING) l, " + W + " s FROM base",
                            "lag",
                            "window function lag does not support an untyped NULL argument; cast it to a concrete type, e.g. null::double"
                    },
                    {
                            "SELECT ts, sym, lag('1d'::interval) OVER (PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING) l, " + W + " s FROM base",
                            "::",
                            "there is no matching function `cast` with the argument types: (STRING, INTERVAL)"
                    },
                    // a column the base does not have: in WHERE, in a named window, in a WHERE subquery
                    {"SELECT ts, sym, " + W + " s FROM base WHERE nope > 1", "nope", "Invalid column: nope"},
                    {"SELECT ts, sym, sum(x) OVER w s FROM base WINDOW w AS (PARTITION BY nope ORDER BY ts ROWS 10 PRECEDING)", "nope", "Invalid column: nope"},
                    {"SELECT ts, sym, " + W + " s FROM base WHERE sym IN (SELECT nope FROM base)", "nope", "Invalid column: nope"},
                    // raised at 0 in the SELECT text, which is where the SELECT starts in the statement
                    {"SELECT ts, sym, " + W + " AS a, current_schemas(true) v FROM base", "SELECT", "unsupported column type text[]"}
            };
            for (String[] c : cases) {
                final String sql = PREFIX + c[0];
                assertCreateRejected(sql, SqlException.class, positionOf(sql, c[1]), c[2]);
            }
            // createLiveView's own checks after the compile point into the statement already, and keep
            // doing so: a missing designated timestamp at the view name, a column no table stores at the column
            assertCreateRejected(
                    PREFIX + "SELECT sym, " + W + " s FROM base",
                    SqlException.class,
                    "CREATE LIVE VIEW ".length(),
                    "live view select must project the base table's designated timestamp 'ts' as a plain column"
            );
            final String nullColumn = PREFIX + "SELECT ts, sym, NULL n, " + W + " s FROM base";
            assertCreateRejected(
                    nullColumn,
                    SqlException.class,
                    positionOf(nullColumn, "NULL n"),
                    "cannot create NULL-type column, please use type cast, e.g. n::type"
            );
        });
    }

    @Test
    public void testCompileImplicitCastErrorReportsTheCreateStatementPosition() throws Exception {
        // A constant cast that fails while the compile folds it raises an ImplicitCastException at the
        // cast's argument or at the cast. A cast that sets no position leaves 0, the start of the SELECT text.
        assertMemoryLeak(() -> {
            createBase();
            final String[][] cases = {
                    {"SELECT ts, sym, 12345::DECIMAL(4,2) d, " + W + " s FROM base", "12345", "inconvertible value: 12345 [INT -> DECIMAL(4,2)]"},
                    {"SELECT ts, sym, (-1234567L)::DECIMAL(4,2) d, " + W + " s FROM base", "-1234567L", "inconvertible value: -1234567 [LONG -> DECIMAL(4,2)]"},
                    {"SELECT ts, sym, CAST(99.99m AS DECIMAL(3,2)) d, " + W + " s FROM base", "CAST", "inconvertible value: 99.99 [DECIMAL(4,2) -> DECIMAL(3,2)]"},
                    {"SELECT ts, sym, 'a'::INT i, " + W + " s FROM base", "SELECT", "inconvertible value: a [CHAR -> INT]"},
                    // the stored text of a parenthesised body starts at the comment after the parenthesis
                    {"( /* c */ SELECT ts, sym, 'a'::INT i, " + W + " s FROM base )", "/* c */", "inconvertible value: a [CHAR -> INT]"}
            };
            for (String[] c : cases) {
                final String sql = PREFIX + c[0];
                assertCreateRejected(sql, ImplicitCastException.class, positionOf(sql, c[1]), c[2]);
            }
        });
    }

    @Test
    public void testCompiledOperationKeepsItsAnchorPosition() throws Exception {
        // A compiled CREATE LIVE VIEW can run after its compiler parsed another one, as a PG prepared
        // statement executed again does. The second statement's anchor starts elsewhere, so a position read
        // from the parser's reusable state or its pooled nodes rather than the operation's own would point
        // elsewhere.
        assertMemoryLeak(() -> {
            createBase();
            final String sqlA = anchored("timestamp_floor('1d', nope)");
            final String sqlB = "CREATE LIVE VIEW lv_b FLUSH EVERY 1s IN MEMORY 2s START FROM NOW AS (  SELECT ts, sym, sum(x) OVER w a FROM base "
                    + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION /* b */ ts::long + no_such_fn(ts)))";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (Operation opA = compiler.compile(sqlA, sqlExecutionContext).getOperation()) {
                    compiler.compile(sqlB, sqlExecutionContext);
                    Rejection rejection = null;
                    try {
                        opA.execute(sqlExecutionContext, null);
                    } catch (Throwable e) {
                        rejection = Rejection.of(e);
                    }
                    Assert.assertNotNull("the first operation must reject its anchor", rejection);
                    rejection.assertEquals(sqlA, SqlException.class, positionOf(sqlA, "timestamp_floor"), "Invalid column: nope");
                }
            }
            Assert.assertNull(engine.getTableTokenIfExists("lv"));
            Assert.assertNull(engine.getTableTokenIfExists("lv_b"));
        });
    }

    @Test
    public void testCompiledOperationKeepsItsSelectPosition() throws Exception {
        // A compiled CREATE LIVE VIEW can run after its compiler parsed another one, as a PG prepared
        // statement executed again does. The second statement starts its SELECT elsewhere, so a position
        // read from the parser's reusable state rather than the operation's own would point elsewhere.
        assertMemoryLeak(() -> {
            createBase();
            final String sqlA = PREFIX + "SELECT ts, sym, rnd_double() r, " + W + " s FROM base";
            final String sqlB = "CREATE LIVE VIEW lv_b FLUSH EVERY 1s IN MEMORY 2s START FROM NOW AS (  SELECT ts, sym, " + W + " s FROM base)";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (Operation opA = compiler.compile(sqlA, sqlExecutionContext).getOperation()) {
                    compiler.compile(sqlB, sqlExecutionContext);
                    Rejection rejection = null;
                    try {
                        opA.execute(sqlExecutionContext, null);
                    } catch (Throwable e) {
                        rejection = Rejection.of(e);
                    }
                    Assert.assertNotNull("the first operation must reject its SELECT", rejection);
                    rejection.assertEquals(sqlA, SqlException.class, positionOf(sqlA, "rnd_double"), NON_DETERMINISTIC_MESSAGE);
                }
            }
            Assert.assertNull(engine.getTableTokenIfExists("lv"));
            Assert.assertNull(engine.getTableTokenIfExists("lv_b"));
        });
    }

    @Test
    public void testPooledCairoExceptionFromTheAnchorCompileKeepsItsPosition() throws Exception {
        // Constant folding lets out what evaluating a constant throws, as it is: failing_constant() throws
        // the armed exception when the anchor compile folds it. A locked entry's flyweight of the thread and
        // a closed pool's one instance carry no position in the anchor and never reset theirs, so CREATE
        // must not write one into them, on any execution. A CairoException created for the throw points at
        // the root token like any other.
        assertMemoryLeak(() -> {
            createBase();
            final String sql = anchored("ts::long + failing_constant()");

            final int lockedPosition = EntryLockedException.instance("anchor").getPosition();
            anchorConstantFailure = () -> EntryLockedException.instance("anchor");
            for (int i = 0; i < 2; i++) {
                final CairoException e = executeRejectedWithCairoException(sql);
                Assert.assertSame(EntryLockedException.class, e.getClass());
                TestUtils.assertEquals("table busy [reason=anchor]", e.getFlyweightMessage());
                Assert.assertEquals(lockedPosition, e.getPosition());
                Assert.assertNull(engine.getTableTokenIfExists("lv"));
            }
            anchorConstantFailure = null;
            Assert.assertEquals(lockedPosition, EntryLockedException.instance("x").getPosition());
            assertLiveViewCompileFlagReset();

            final int closedPosition = PoolClosedException.INSTANCE.getPosition();
            anchorConstantFailure = () -> PoolClosedException.INSTANCE;
            for (int i = 0; i < 2; i++) {
                final CairoException e = executeRejectedWithCairoException(sql);
                Assert.assertSame(PoolClosedException.INSTANCE, e);
                TestUtils.assertEquals("", e.getFlyweightMessage());
                Assert.assertEquals(closedPosition, e.getPosition());
                Assert.assertNull(engine.getTableTokenIfExists("lv"));
            }
            anchorConstantFailure = null;
            Assert.assertEquals(closedPosition, PoolClosedException.INSTANCE.getPosition());

            anchorConstantFailure = () -> CairoException.critical(0).put("anchor fold failed");
            assertCreateRejected(sql, CairoException.class, positionOf(sql, "+"), "anchor fold failed");
            anchorConstantFailure = null;

            execute(sql);
            Assert.assertNotNull(engine.getTableTokenIfExists("lv"));
        });
    }

    @Test
    public void testPooledCairoExceptionFromTheCompileKeepsItsPosition() throws Exception {
        // The code generator opens the base's reader and lets the reader pool's failure out of the compile
        // as it is. A locked entry throws a flyweight of the thread and a closed pool the one instance every
        // thread shares; neither resets its position, and neither carries a position in the SELECT text,
        // so CREATE must not write one into them, on any execution. A failed open throws a CairoException
        // of its own, created for the throw, and that one points at the SELECT like any error at 0.
        assertMemoryLeak(() -> {
            createBase();
            final String sql = PREFIX + "SELECT ts, sym, " + W + " s FROM base";

            final int lockedPosition = EntryLockedException.instance("p35").getPosition();
            baseReaderFailure = () -> EntryLockedException.instance("p35");
            for (int i = 0; i < 2; i++) {
                final CairoException e = executeRejectedWithCairoException(sql);
                Assert.assertSame(EntryLockedException.class, e.getClass());
                TestUtils.assertEquals("table busy [reason=p35]", e.getFlyweightMessage());
                Assert.assertEquals(lockedPosition, e.getPosition());
                Assert.assertNull(engine.getTableTokenIfExists("lv"));
            }
            baseReaderFailure = null;
            Assert.assertEquals(lockedPosition, EntryLockedException.instance("p35").getPosition());

            final int closedPosition = PoolClosedException.INSTANCE.getPosition();
            baseReaderFailure = () -> PoolClosedException.INSTANCE;
            for (int i = 0; i < 2; i++) {
                final CairoException e = executeRejectedWithCairoException(sql);
                Assert.assertSame(PoolClosedException.INSTANCE, e);
                TestUtils.assertEquals("", e.getFlyweightMessage());
                Assert.assertEquals(closedPosition, e.getPosition());
                Assert.assertNull(engine.getTableTokenIfExists("lv"));
            }
            baseReaderFailure = null;
            Assert.assertEquals(closedPosition, PoolClosedException.INSTANCE.getPosition());

            baseReaderFailure = () -> CairoException.critical(0).put("p35 reader failed");
            assertCreateRejected(sql, CairoException.class, positionOf(sql, "SELECT"), "p35 reader failed");
            baseReaderFailure = null;

            execute(sql);
            Assert.assertNotNull(engine.getTableTokenIfExists("lv"));
        });
    }

    // The CREATE statement whose one named window carries ANCHOR EXPRESSION expression
    private static String anchored(String expression) {
        return anchoredBy("ANCHOR EXPRESSION " + expression);
    }

    // The CREATE statement whose one named window ends in anchorClause
    private static String anchoredBy(String anchorClause) {
        return PREFIX + ANCHORED_SELECT + anchorClause + ")";
    }

    // The base, whatever a failed CREATE did to it, takes a view anchored on its designated timestamp; the
    // view and the base are dropped afterwards.
    private static void assertBaseTakesAnchoredView() throws SqlException {
        Assert.assertNull(engine.getTableTokenIfExists("lv"));
        execute(PREFIX + ANCHORED_SELECT + "ANCHOR EXPRESSION ts::long)");
        Assert.assertNotNull(engine.getTableTokenIfExists("lv"));
        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
        drainWalQueue();
    }

    private static void assertCreateRejected(String sql, Class<? extends Throwable> expectedType, int expectedPosition, String expectedMessage) {
        assertCreateRejected("lv", sql, expectedType, expectedPosition, expectedMessage);
    }

    private static void assertCreateRejected(
            String viewName,
            String sql,
            Class<? extends Throwable> expectedType,
            int expectedPosition,
            String expectedMessage
    ) {
        executeRejected(sql).assertEquals(sql, expectedType, expectedPosition, expectedMessage);
        Assert.assertNull(engine.getTableTokenIfExists(viewName));
    }

    // CREATE LIVE VIEW sets the live-view-compile flag of the context only while it compiles. A flag left
    // set would let a plain query take the ANCHOR clause, which only a live view acts on.
    private static void assertLiveViewCompileFlagReset() {
        Assert.assertFalse(sqlExecutionContext.isLiveViewCompile());
        final String sql = "SELECT sum(x) OVER (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00') FROM base";
        compileRejected(sql).assertEquals(sql, SqlException.class, positionOf(sql, "ANCHOR"), "ANCHOR is only supported in a live view query");
    }

    private static void assertNoLiveViewDirectory() {
        final File[] lvDirs = new File(engine.getConfiguration().getDbRoot()).listFiles(
                f -> f.getName().equals("lv") || f.getName().startsWith("lv~")
        );
        Assert.assertNotNull(lvDirs);
        Assert.assertEquals(0, lvDirs.length);
    }

    private static Rejection compileRejected(String sql) {
        final RecordCursorFactory factory;
        try {
            factory = select(sql);
        } catch (Throwable e) {
            return Rejection.of(e);
        }
        Misc.free(factory);
        throw new AssertionError("the query must fail to compile: " + sql);
    }

    private static void createBase() throws SqlException {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    // Creates the base with one row, compiles sql, changes the base and executes the compiled statement,
    // which must fail
    private static Rejection executeAfterBaseColumnChange(String sql, String... changes) throws SqlException {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT, l LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO base VALUES ('2026-01-01T00:00:00.000000Z', 'a', 1, 5)");
        drainWalQueue();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (Operation op = compiler.compile(sql, sqlExecutionContext).getOperation()) {
                for (String change : changes) {
                    execute(change);
                }
                drainWalQueue();
                try {
                    op.execute(sqlExecutionContext, null);
                } catch (Throwable e) {
                    return Rejection.of(e);
                }
            }
        }
        throw new AssertionError("the compiled statement must fail after the change: " + sql);
    }

    private static Rejection executeRejected(String sql) {
        try {
            execute(sql);
        } catch (Throwable e) {
            return Rejection.of(e);
        }
        throw new AssertionError("the statement must fail: " + sql);
    }

    private static CairoException executeRejectedWithCairoException(String sql) {
        try {
            execute(sql);
        } catch (CairoException e) {
            return e;
        } catch (Throwable e) {
            throw new AssertionError("the statement must fail with a CairoException: " + sql, e);
        }
        throw new AssertionError("the statement must fail: " + sql);
    }

    // Where token occurs in sql. It must occur exactly once, so the expected position cannot match by accident.
    private static int positionOf(String sql, String token) {
        final int position = sql.indexOf(token);
        Assert.assertTrue("'" + token + "' must occur once in: " + sql, position > -1 && position == sql.lastIndexOf(token));
        return position;
    }

    // failing_constant(): a LONG constant, 0 unless a test arms anchorConstantFailure. A compile folds a
    // constant by evaluating it, so the armed exception leaves the compile as it is thrown.
    private static class FailingConstantFunctionFactory implements FunctionFactory {

        @Override
        public String getSignature() {
            return "failing_constant()";
        }

        @Override
        public Function newInstance(
                int position,
                ObjList<Function> args,
                IntList argPositions,
                CairoConfiguration configuration,
                SqlExecutionContext sqlExecutionContext
        ) {
            return new LongFunction() {
                @Override
                public long getLong(Record rec) {
                    final Supplier<CairoException> failure = anchorConstantFailure;
                    if (failure != null) {
                        throw failure.get();
                    }
                    return 0;
                }

                @Override
                public boolean isConstant() {
                    return true;
                }
            };
        }
    }

    // What a failed statement threw, copied out at once: SqlException is a flyweight of the thread that the
    // next one may overwrite.
    private record Rejection(Class<? extends Throwable> type, int position, String message) {

        static Rejection of(Throwable e) {
            if (e instanceof FlyweightMessageContainer container) {
                return new Rejection(e.getClass(), container.getPosition(), Chars.toString(container.getFlyweightMessage()));
            }
            throw new AssertionError("the failure carries no position", e);
        }

        void assertEquals(String sql, Class<? extends Throwable> expectedType, int expectedPosition, String expectedMessage) {
            Assert.assertEquals(sql, expectedType, type);
            Assert.assertEquals(sql, expectedMessage, message);
            Assert.assertEquals(sql, expectedPosition, position);
        }
    }
}
