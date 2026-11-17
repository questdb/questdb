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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.ResourcePoolSupervisor;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.std.Chars;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.function.Supplier;

/**
 * Where CREATE LIVE VIEW reports an error raised while it compiles its SELECT.
 * <p>
 * CairoEngine.createLiveView compiles the stored SELECT text on its own, so the compile positions every
 * error in that text. The statement records where the text starts, and createLiveView adds that offset
 * to the position of each exception the compile positions in the text and creates for the throw:
 * SqlException, ImplicitCastException and CairoException itself. The CairoException subclasses the
 * reader pool throws carry no position in the text, and some of them are objects every throw shares,
 * so they keep their own.
 *
 * @see LiveViewProjectionRejectTest
 */
public class LiveViewCompileErrorPositionTest extends AbstractLiveViewTest {
    private static final String NON_DETERMINISTIC_MESSAGE = "non-deterministic function cannot be used in live view: rnd_double";
    private static final String PREFIX = "CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM BEGINNING AS ";
    private static final String W = "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING)";
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
        };
        AbstractCairoTest.setUpStatic();
    }

    @After
    public void disarmBaseReaderFailure() {
        baseReaderFailure = null;
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
