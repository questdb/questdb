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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ALTER MATERIALIZED VIEW ... ADD COLUMN and DROP COLUMN change a view's columns in place, one column per
 * statement, and RENAME COLUMN fails with the workaround in its message. The view's refresh maps the columns of
 * its SQL to the view's by name, so a column the SQL does not produce reads NULL, and a column of the SQL the view
 * lacks is ignored; materialized_views() reports both in unmapped_view_columns and ignored_query_columns.
 * <p>
 * Expected rows come from the base rows each test inserts: a view bucket holds the last price and amount of its
 * symbol's base rows within the hour.
 */
public class MatViewColumnDdlTest extends AbstractCairoTest {
    private static final String RENAME_HINT = "cannot modify materialized view: renaming materialized view columns is not supported; "
            + "add the new column, change the view SQL with ALTER MATERIALIZED VIEW ... AS, then drop the old column";
    private static final String VIEW_SQL = "SELECT ts, sym, last(price) AS price, last(amount) AS amount FROM base SAMPLE BY 1h";

    @Test
    public void testAddColumnAuthorizationErrorFailsStatement() throws Exception {
        // ADD COLUMN compiles the view's SQL under the caller's context. A caller who may not read the base gets the
        // authorization error, and the statement fails instead of skipping the cast check as for a SQL that does
        // not compile. DROP COLUMN only parses the SQL, which reads no table.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            insertBaseRows();
            drainWalAndMatViewQueues();
            final TableToken baseToken = engine.verifyTableName("base");
            final AllowAllSecurityContext noBaseSelect = new AllowAllSecurityContext() {
                @Override
                public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
                    denySelectOnBase(tableToken, baseToken);
                }

                @Override
                public void authorizeSelectOnAnyColumn(TableToken tableToken) {
                    denySelectOnBase(tableToken, baseToken);
                }
            };
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(noBaseSelect)) {
                try {
                    execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT", context);
                    Assert.fail("ADD COLUMN must fail without SELECT on the base table");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isAuthorizationError());
                    TestUtils.assertContains(e.getFlyweightMessage(), "no SELECT on base");
                }
                execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN amount", context);
            }
            drainWalAndMatViewQueues();
            assertColumns("price_1h", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    price\tDOUBLE
                    """);
        });
    }

    @Test
    public void testAddColumnAfterViewStateRemoved() throws Exception {
        // A concurrent DROP MATERIALIZED VIEW can remove the view's state after ADD COLUMN resolved the view: the
        // check then has nowhere to keep the mapping, and materialized_views() reports it unknown.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            insertBaseRows();
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("price_1h");
            engine.getMatViewStateStore().removeViewState(viewToken);
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT");
            assertMappingUnknown("price_1h");
            drainWalQueue();
            assertColumns("price_1h", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    price\tDOUBLE
                    amount\tDOUBLE
                    extra\tINT
                    """);
            execute("DROP MATERIALIZED VIEW price_1h");
            drainWalQueue();
            Assert.assertNull(engine.getTableTokenIfExists("price_1h"));
        });
    }

    @Test
    public void testAddColumnCheckCancelled() throws Exception {
        // A statement cancelled while ADD COLUMN's check compiles the view's SQL fails, and adds nothing.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            insertBaseRows();
            drainWalAndMatViewQueues();
            final TableToken baseToken = engine.verifyTableName("base");
            final AllowAllSecurityContext cancelOnBaseRead = new AllowAllSecurityContext() {
                @Override
                public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
                    if (tableToken.equals(baseToken)) {
                        throw CairoException.queryCancelled();
                    }
                }
            };
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(cancelOnBaseRead)) {
                try {
                    execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT", context);
                    Assert.fail("a cancelled ADD COLUMN must fail");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCancellation());
                    TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                }
            }
            drainWalQueue();
            assertColumns("price_1h", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    price\tDOUBLE
                    amount\tDOUBLE
                    """);
            assertMapping("price_1h", "", "");
        });
    }

    @Test
    public void testAddColumnCheckSkippedWhenQueryFails() throws Exception {
        // A query that compiles but cannot open its cursor gives ADD COLUMN's check no columns either, so the check
        // is skipped and logged as for a SQL that does not compile.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            insertBaseRows();
            drainWalAndMatViewQueues();
            final TableToken baseToken = engine.verifyTableName("base");
            final AllowAllSecurityContext failOnBaseRead = new AllowAllSecurityContext() {
                @Override
                public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
                    if (tableToken.equals(baseToken)) {
                        throw CairoException.nonCritical().put("could not open base");
                    }
                }
            };
            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(failOnBaseRead)) {
                execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT", context);
                logCapture.waitForRegex("could not compile materialized view SQL, skipping the type check of the new column \\[view=price_1h~\\d+, column=extra, error=could not open base\\]");
            } finally {
                logCapture.stop();
            }
            assertMappingUnknown("price_1h");
            drainWalQueue();
            assertColumns("price_1h", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    price\tDOUBLE
                    amount\tDOUBLE
                    extra\tINT
                    """);
        });
    }

    @Test
    public void testAddColumnOptions() throws Exception {
        // ADD COLUMN takes the column options of ALTER TABLE: SYMBOL CAPACITY, CACHE or NOCACHE, and INDEX.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            insertBaseRows();
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN tag SYMBOL CAPACITY 512 NOCACHE INDEX");
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN IF NOT EXISTS tag SYMBOL");
            drainWalAndMatViewQueues();
            assertQuery("SELECT \"column\", type, indexed, symbolCached, symbolCapacity FROM table_columns('price_1h') WHERE \"column\" = 'tag'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\ttype\tindexed\tsymbolCached\tsymbolCapacity
                            tag\tSYMBOL\ttrue\tfalse\t512
                            """);
            final String sql = "ALTER MATERIALIZED VIEW price_1h ADD COLUMN IF NOT EXISTS tag INT";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("INT"),
                    "column already exists with a different column type [current type=SYMBOL, requested type=INT]"
            );
            assertMapping("price_1h", "tag", "");
            assertViewValid("price_1h");
        });
    }

    @Test
    public void testAddColumnReadsNullUntilSqlProducesIt() throws Exception {
        // A passthrough view's SQL, SELECT *, produces every base column, also one the base gains later. The view
        // ignores such a column until view ADD COLUMN adds it; from then on, refresh writes it in the rows it
        // recomputes. A passthrough view refreshes the microseconds that base rows touch, so the rows refreshed
        // before the view had the column read NULL until RANGE recomputes them. A column the SQL does not produce
        // reads NULL in every row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW trades_copy AS (SELECT * FROM trades) PARTITION BY DAY");
            execute("INSERT INTO trades VALUES ('a', 1.5, '2024-01-01T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertMapping("trades_copy", "", "");

            execute("ALTER TABLE trades ADD COLUMN qty INT");
            execute("INSERT INTO trades VALUES ('b', 2.5, '2024-01-01T00:20:00.000000Z', 2)");
            drainWalAndMatViewQueues();
            assertMapping("trades_copy", "", "qty");
            assertQuery("trades_copy").timestamp("ts").noLeakCheck().expectSize().returns("""
                    sym\tprice\tts
                    a\t1.5\t2024-01-01T00:10:00.000000Z
                    b\t2.5\t2024-01-01T00:20:00.000000Z
                    """);

            execute("ALTER MATERIALIZED VIEW trades_copy ADD COLUMN qty INT");
            // the check that ADD COLUMN runs maps the SQL's qty to the new column
            assertMapping("trades_copy", "", "");
            drainWalAndMatViewQueues();
            assertQuery("trades_copy").timestamp("ts").noLeakCheck().expectSize().returns("""
                    sym\tprice\tts\tqty
                    a\t1.5\t2024-01-01T00:10:00.000000Z\tnull
                    b\t2.5\t2024-01-01T00:20:00.000000Z\tnull
                    """);

            execute("INSERT INTO trades VALUES ('c', 3.5, '2024-01-01T00:30:00.000000Z', 3)");
            drainWalAndMatViewQueues();
            assertQuery("trades_copy").timestamp("ts").noLeakCheck().expectSize().returns("""
                    sym\tprice\tts\tqty
                    a\t1.5\t2024-01-01T00:10:00.000000Z\tnull
                    b\t2.5\t2024-01-01T00:20:00.000000Z\tnull
                    c\t3.5\t2024-01-01T00:30:00.000000Z\t3
                    """);

            // RANGE recomputes the rows from the base, which holds no qty for the row written before the base had it
            execute("REFRESH MATERIALIZED VIEW trades_copy RANGE FROM '2024-01-01T00:00' TO '2024-01-01T00:59'");
            drainWalAndMatViewQueues();
            assertQuery("trades_copy").timestamp("ts").noLeakCheck().expectSize().returns("""
                    sym\tprice\tts\tqty
                    a\t1.5\t2024-01-01T00:10:00.000000Z\tnull
                    b\t2.5\t2024-01-01T00:20:00.000000Z\t2
                    c\t3.5\t2024-01-01T00:30:00.000000Z\t3
                    """);

            // the SQL produces no note: the column stays NULL, also in the rows refreshed after it was added
            execute("ALTER MATERIALIZED VIEW trades_copy ADD COLUMN note INT");
            assertMapping("trades_copy", "note", "");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW trades_copy RANGE FROM '2024-01-01T00:00' TO '2024-01-01T00:59'");
            drainWalAndMatViewQueues();
            // a RANGE refresh reports its status at the next refresh
            execute("INSERT INTO trades VALUES ('d', 4.5, '2024-01-01T00:40:00.000000Z', 4)");
            drainWalAndMatViewQueues();
            assertQuery("trades_copy").timestamp("ts").noLeakCheck().expectSize().returns("""
                    sym\tprice\tts\tqty\tnote
                    a\t1.5\t2024-01-01T00:10:00.000000Z\tnull\tnull
                    b\t2.5\t2024-01-01T00:20:00.000000Z\t2\tnull
                    c\t3.5\t2024-01-01T00:30:00.000000Z\t3\tnull
                    d\t4.5\t2024-01-01T00:40:00.000000Z\t4\tnull
                    """);
            assertMapping("trades_copy", "note", "");
            assertViewValid("trades_copy");
        });
    }

    @Test
    public void testAlterMatViewForms() throws Exception {
        // The forms ALTER MATERIALIZED VIEW has besides the column DDL keep their grammar. A statement of no known
        // form lists them all, AS and WITH BASE included.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            insertBaseRows();
            drainWalAndMatViewQueues();
            final String expected = "'add', 'alter', 'as', 'drop', 'rename', 'resume', 'set', 'suspend' or 'with' expected";
            for (String sql : new String[]{
                    "ALTER MATERIALIZED VIEW price_1h",
                    "ALTER MATERIALIZED VIEW price_1h;"
            }) {
                final int semicolon = sql.indexOf(';');
                assertExceptionNoLeakCheck(sql, semicolon > -1 ? semicolon : sql.length(), expected);
            }
            for (String form : new String[]{"foobar", "AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h)", "WITH BASE base AS (SELECT 1)"}) {
                final String sql = "ALTER MATERIALIZED VIEW price_1h " + form;
                assertExceptionNoLeakCheck(sql, sql.indexOf(form), expected);
            }
            final String rebase = "ALTER MATERIALIZED VIEW price_1h REBASE foo";
            assertExceptionNoLeakCheck(rebase, rebase.indexOf("foo"), "'wal' expected");

            execute("ALTER MATERIALIZED VIEW price_1h ALTER COLUMN sym ADD INDEX");
            execute("ALTER MATERIALIZED VIEW price_1h SET TTL 2 WEEKS");
            execute("ALTER MATERIALIZED VIEW price_1h SET EXPIRE ROWS WHEN price < 2.0");
            drainWalQueue();
            assertQuery("SELECT expire_clause IS NULL no_policy FROM materialized_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("no_policy\nfalse\n");
            execute("ALTER MATERIALIZED VIEW price_1h DROP EXPIRE");
            drainWalQueue();
            assertQuery("SELECT expire_clause IS NULL no_policy FROM materialized_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("no_policy\ntrue\n");
            execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("price_1h")));
            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("price_1h")));
            assertQuery("SELECT \"column\", indexed FROM table_columns('price_1h') WHERE \"column\" = 'sym'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("column\tindexed\nsym\ttrue\n");
            assertQuery("SELECT ttlValue, ttlUnit FROM tables() WHERE table_name = 'price_1h'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("ttlValue\tttlUnit\n2\tWEEK\n");
            assertViewValid("price_1h");
        });
    }

    @Test
    public void testAddColumnValidationOnlyKeepsMapping() throws Exception {
        // A statement that is only validated reports what the check finds, and changes nothing.
        Assert.assertFalse(RecordToRowCopierUtils.isCopierConvertible(ColumnType.SYMBOL, ColumnType.LONG));
        assertMemoryLeak(() -> {
            createBase();
            createView();
            insertBaseRows();
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN sym");
            drainWalQueue();
            try (
                    SqlExecutionContextImpl validationContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                validationContext.setValidationOnly(true);
                compiler.compile("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT", validationContext);
                final String sql = "ALTER MATERIALIZED VIEW price_1h ADD COLUMN sym LONG";
                try {
                    compiler.compile(sql, validationContext);
                    Assert.fail("the cast check must run on validation too");
                } catch (SqlException e) {
                    TestUtils.assertEquals("inconvertible types: SYMBOL -> LONG [from=sym, to=sym]", e.getFlyweightMessage());
                    Assert.assertEquals(sql.indexOf("sym LONG"), e.getPosition());
                }
            }
            // the mapping the last refresh built, before the view dropped sym
            assertMapping("price_1h", "", "");
            assertColumns("price_1h", """
                    column\ttype
                    ts\tTIMESTAMP
                    price\tDOUBLE
                    amount\tDOUBLE
                    """);
        });
    }

    @Test
    public void testCastRefusal() throws Exception {
        // When the view's SQL produces the added column's name, refresh writes that column into the new one, so ADD
        // COLUMN refuses a type that has no copier conversion from the SQL's, with the error refresh would give.
        Assert.assertFalse(RecordToRowCopierUtils.isCopierConvertible(ColumnType.SYMBOL, ColumnType.LONG));
        Assert.assertTrue(RecordToRowCopierUtils.isCopierConvertible(ColumnType.SYMBOL, ColumnType.VARCHAR));
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, tag SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW trades_copy AS (SELECT ts, sym, tag, price FROM trades) PARTITION BY DAY");
            execute("INSERT INTO trades VALUES ('a', 'x', 1.5, '2024-01-01T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW trades_copy DROP COLUMN tag");
            drainWalAndMatViewQueues();

            final String addLong = "ALTER MATERIALIZED VIEW trades_copy ADD COLUMN tag LONG";
            assertExceptionNoLeakCheck(addLong, addLong.indexOf("tag LONG"), "inconvertible types: SYMBOL -> LONG [from=tag, to=tag]");
            // the names compare ignoring case, and the error names each column as its side spells it
            final String addUpperLong = "ALTER MATERIALIZED VIEW trades_copy ADD COLUMN TAG LONG";
            assertExceptionNoLeakCheck(addUpperLong, addUpperLong.indexOf("TAG LONG"), "inconvertible types: SYMBOL -> LONG [from=tag, to=TAG]");
            assertColumns("trades_copy", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    price\tDOUBLE
                    """);

            // a type with a conversion takes the SQL's values from the next refresh on
            execute("ALTER MATERIALIZED VIEW trades_copy ADD COLUMN tag VARCHAR");
            drainWalAndMatViewQueues();
            execute("INSERT INTO trades VALUES ('b', 'y', 2.5, '2024-01-01T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("trades_copy").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tprice\ttag
                    2024-01-01T00:10:00.000000Z\ta\t1.5\t
                    2024-01-01T00:20:00.000000Z\tb\t2.5\ty
                    """);
            assertMapping("trades_copy", "", "");
            assertViewValid("trades_copy");
        });
    }

    @Test
    public void testCastRefusalSkippedWhenSqlDoesNotCompile() throws Exception {
        // A view whose SQL a base table change broke can still take new columns: the cast check needs the SQL's
        // columns, so it is skipped and logged, and the column mapping reads NULL, as after any failed compile.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, tag SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW trades_copy AS (SELECT ts, sym, tag, price FROM trades) PARTITION BY DAY");
            execute("INSERT INTO trades VALUES ('a', 'x', 1.5, '2024-01-01T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW trades_copy DROP COLUMN tag");
            drainWalAndMatViewQueues();
            assertMapping("trades_copy", "", "");

            // the view's SQL reads price, so it no longer compiles
            execute("ALTER TABLE trades DROP COLUMN price");
            drainWalAndMatViewQueues();

            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try {
                execute("ALTER MATERIALIZED VIEW trades_copy ADD COLUMN tag LONG");
                logCapture.waitForRegex("could not compile materialized view SQL, skipping the type check of the new column \\[view=trades_copy~\\d+, column=tag, error=Invalid column: price\\]");
            } finally {
                logCapture.stop();
            }
            assertMappingUnknown("trades_copy");
            drainWalQueue();
            assertColumns("trades_copy", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    price\tDOUBLE
                    tag\tLONG
                    """);
        });
    }

    @Test
    public void testColumnsDifferFromSqlAfterAddAndDrop() throws Exception {
        // After ADD and DROP COLUMN, the view's columns no longer match its SQL's output one to one: refresh copies
        // the matched columns through a projection, leaves the added column NULL and ignores the dropped one.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            insertBaseRows();
            drainWalAndMatViewQueues();
            Assert.assertNull(getViewState("price_1h").getRecordProjectionForTesting());

            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT");
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN price");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('b', 5.5, 50.0, '2024-01-01T01:20:00.000000Z')");
            drainWalAndMatViewQueues();
            // only the 01:00 bucket is recomputed; the 00:00 one keeps the amounts it had
            final String expected = """
                    ts\tsym\tamount\textra
                    2024-01-01T00:00:00.000000Z\ta\t30.0\tnull
                    2024-01-01T00:00:00.000000Z\tb\t20.0\tnull
                    2024-01-01T01:00:00.000000Z\ta\t40.0\tnull
                    2024-01-01T01:00:00.000000Z\tb\t50.0\tnull
                    """;
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(expected);
            Assert.assertNotNull(getViewState("price_1h").getRecordProjectionForTesting());
            assertMapping("price_1h", "extra", "price");

            // FULL rebuilds every bucket through the same mapping
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(expected);
            assertViewValid("price_1h");
            // SHOW CREATE prints the SQL as stored, which produces the columns the SQL produces
            assertQuery("SHOW CREATE MATERIALIZED VIEW price_1h").noLeakCheck().noRandomAccess().returns(
                    "ddl\nCREATE MATERIALIZED VIEW 'price_1h' WITH BASE 'base' REFRESH IMMEDIATE AS (\n"
                            + VIEW_SQL + "\n) PARTITION BY DAY;\n"
            );
        });
    }

    @Test
    public void testDropColumnBetweenBatchCommits() throws Exception {
        // A view DROP COLUMN that lands between two batch commits of a running refresh reaches the refresh's WAL
        // writer at its next commit, which frees the column's buffer. The refresh rebuilds its copier for the
        // view's new columns and writes the rest of its rows without the dropped column.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE, 10);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 2);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym VARCHAR, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH WAL");
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT sym, last(price) AS price, ts FROM trades SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("INSERT INTO trades SELECT concat('sym', x), x, timestamp_sequence('2022-02-24', 1000000L*60*60*2) FROM long_sequence(30)");
            drainWalQueue();

            final AtomicInteger batchCommits = new AtomicInteger();
            final LogCapture logCapture = new LogCapture();
            try (MatViewRefreshJob job = createMatViewRefreshJob()) {
                job.setOnBatchCommittedForTesting(() -> {
                    if (batchCommits.getAndIncrement() == 0) {
                        try {
                            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN price");
                        } catch (SqlException e) {
                            throw new AssertionError(e);
                        }
                    }
                });
                logCapture.start();
                drainMatViewQueue(job);
                logCapture.drain();
            } finally {
                logCapture.stop();
            }
            drainWalQueue();
            Assert.assertTrue(batchCommits.get() > 1);
            logCapture.assertOnlyOnce("view schema changed during refresh, rebuilding record copier \\[view=price_1h~\\d+");

            assertViewValid("price_1h");
            // one base row every 2 hours from 2022-02-24, one view bucket each
            final StringBuilder expected = new StringBuilder("sym\tts\n");
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
            for (int i = 1; i <= 30; i++) {
                expected.append("sym").append(i).append('\t')
                        .append(LocalDateTime.of(2022, 2, 24, 0, 0).plusHours(2L * (i - 1)).format(formatter))
                        .append('\n');
            }
            assertQuery("price_1h ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(expected.toString());
            assertMapping("price_1h", "", "price");
        });
    }

    @Test
    public void testDropColumnAtIntervalGapCommit() throws Exception {
        // A refresh commits the rows of one refresh interval before it moves to the next, across a gap. A view DROP
        // COLUMN right after that commit reaches the refresh's WAL writer at its next commit; the copier built for
        // the next interval's rows, and the mapping the view reports, follow the view's new columns.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            insertBaseRows();
            drainWalAndMatViewQueues();
            // Pins the cost model's gap threshold to 5 seconds, far below the 2-day gaps below, so the refresh keeps
            // the intervals apart whatever the machine's commit and scan speeds.
            final MatViewState state = getViewState("price_1h");
            Assert.assertTrue(state.tryLock());
            try {
                state.setRefreshMetricsForTesting(5_000_000L, 1L, 1L);
            } finally {
                state.unlock();
            }
            execute("INSERT INTO base VALUES ('b', 5.5, 50.0, '2024-01-01T05:10:00.000000Z')");
            execute("INSERT INTO base VALUES ('b', 6.5, 60.0, '2024-01-03T05:10:00.000000Z')");
            execute("INSERT INTO base VALUES ('b', 7.5, 70.0, '2024-01-05T05:10:00.000000Z')");
            drainWalQueue();

            final AtomicInteger gapCommits = new AtomicInteger();
            final LogCapture logCapture = new LogCapture();
            try (MatViewRefreshJob job = createMatViewRefreshJob()) {
                job.setOnBatchCommittedForTesting(() -> {
                    if (gapCommits.getAndIncrement() == 0) {
                        try {
                            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN price");
                        } catch (SqlException e) {
                            throw new AssertionError(e);
                        }
                    }
                });
                logCapture.start();
                drainMatViewQueue(job);
                logCapture.drain();
            } finally {
                logCapture.stop();
            }
            drainWalQueue();
            Assert.assertEquals(2, gapCommits.get());
            logCapture.assertOnlyOnce("view schema changed during refresh, rebuilding record copier \\[view=price_1h~\\d+");

            assertViewValid("price_1h");
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tamount
                    2024-01-01T00:00:00.000000Z\ta\t30.0
                    2024-01-01T00:00:00.000000Z\tb\t20.0
                    2024-01-01T01:00:00.000000Z\ta\t40.0
                    2024-01-01T05:00:00.000000Z\tb\t50.0
                    2024-01-03T05:00:00.000000Z\tb\t60.0
                    2024-01-05T05:00:00.000000Z\tb\t70.0
                    """);
            assertMapping("price_1h", "", "price");
        });
    }

    @Test
    public void testDropColumnOfPendingAddColumn() throws Exception {
        // ADD and DROP COLUMN read the view's columns from its sequencer, which holds the changes still in the view's
        // WAL: a column added but not yet applied can be dropped, and cannot be added twice.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            insertBaseRows();
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT");
            final String addAgain = "ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT";
            assertExceptionNoLeakCheck(addAgain, addAgain.indexOf("extra"), "column 'extra' already exists");
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN extra");
            final String dropAgain = "ALTER MATERIALIZED VIEW price_1h DROP COLUMN extra";
            assertExceptionNoLeakCheck(dropAgain, dropAgain.indexOf("extra"), "Invalid column: extra");
            drainWalAndMatViewQueues();
            assertColumns("price_1h", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    price\tDOUBLE
                    amount\tDOUBLE
                    """);
            assertViewValid("price_1h");
        });
    }

    @Test
    public void testDropProducedColumnAndTimestamp() throws Exception {
        // Dropping a column the SQL produces makes refresh ignore it. The designated timestamp cannot go.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            insertBaseRows();
            drainWalAndMatViewQueues();

            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN price");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('a', 5.5, 50.0, '2024-01-01T00:50:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tamount
                    2024-01-01T00:00:00.000000Z\ta\t50.0
                    2024-01-01T00:00:00.000000Z\tb\t20.0
                    2024-01-01T01:00:00.000000Z\ta\t40.0
                    """);
            assertMapping("price_1h", "", "price");
            assertViewValid("price_1h");

            for (String timestamp : new String[]{"ts", "\"ts\"", "TS"}) {
                final String sql = "ALTER MATERIALIZED VIEW price_1h DROP COLUMN " + timestamp;
                assertExceptionNoLeakCheck(sql, sql.lastIndexOf(timestamp), "cannot drop the designated timestamp column of a materialized view");
            }
            final String dropMissing = "ALTER MATERIALIZED VIEW price_1h DROP COLUMN no_such";
            assertExceptionNoLeakCheck(dropMissing, dropMissing.indexOf("no_such"), "Invalid column: no_such");
            final String dropDropped = "ALTER MATERIALIZED VIEW price_1h DROP COLUMN price";
            assertExceptionNoLeakCheck(dropDropped, dropDropped.lastIndexOf("price"), "Invalid column: price");
            final String dropNothing = "ALTER MATERIALIZED VIEW price_1h DROP COLUMN";
            assertExceptionNoLeakCheck(dropNothing, dropNothing.length(), "column name expected");
            drainWalQueue();
            assertColumns("price_1h", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    amount\tDOUBLE
                    """);
        });
    }

    @Test
    public void testFrozenZoneInsertFillsAddedColumn() throws Exception {
        // Below a view's REFRESH LIMIT, where refresh no longer writes, INSERT can fill a column the view gained
        // with ADD COLUMN.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO trades VALUES ('a', 2.0, '2024-09-10T11:00')");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL DEFERRED AS SELECT sym, last(price) AS price, ts FROM trades SAMPLE BY 1h");
            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 1 HOUR");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN note INT");
            drainWalAndMatViewQueues();

            // the anchor is min(11:00, now) = 11:00, so rows up to 10:00 lie in the frozen zone
            currentMicros = parseFloorPartialTimestamp("2024-09-10T12:30:00.000000Z");
            execute("INSERT INTO price_1h (sym, price, ts, note) VALUES ('a', 1.5, '2024-09-10T09:00:00.000000Z', 7)");
            drainWalAndMatViewQueues();
            assertQuery("price_1h").timestamp("ts").noLeakCheck().expectSize().returns("""
                    sym\tprice\tts\tnote
                    a\t1.5\t2024-09-10T09:00:00.000000Z\t7
                    """);
        });
    }

    @Test
    public void testGeneratedNameRefusal() throws Exception {
        // A view created before CREATE required explicit column names, whose SQL relies on generated names, copies
        // its SQL's columns by position, which a column added or dropped would shift. ADD and DROP COLUMN refuse it
        // until ALTER ... AS gives its columns explicit names. The check parses the SQL only, so it holds after a
        // base table change breaks the SQL's compile.
        assertMemoryLeak(() -> {
            createBase();
            executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW older AS (SELECT ts, sym, avg(price), avg(amount) FROM base SAMPLE BY 1h) PARTITION BY DAY");
            insertBaseRows();
            drainWalAndMatViewQueues();
            final String columns = """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    avg\tDOUBLE
                    avg1\tDOUBLE
                    """;
            assertColumns("older", columns);
            assertGeneratedNamesRefused("older", "sym");

            // the SQL reads amount, so it no longer compiles
            execute("ALTER TABLE base DROP COLUMN amount");
            drainWalAndMatViewQueues();
            assertGeneratedNamesRefused("older", "sym");
            drainWalQueue();
            assertColumns("older", columns);

            // an unaliased reference to a column whose name is longer than 64 characters has a generated name too
            final String longName = "c".repeat(65);
            execute("CREATE TABLE wide (" + longName + " DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW wide_copy AS (SELECT ts, " + longName + " FROM wide) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertGeneratedNamesRefused("wide_copy", longName);
        });
    }

    @Test
    public void testMappingColumnsAfterAddColumn() throws Exception {
        // The mapping is unknown until the view's SQL compiles in this process. ADD COLUMN's check maps the SQL's
        // columns to the view's columns and the new one as soon as the statement returns.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            assertMappingUnknown("price_1h");
            insertBaseRows();
            drainWalAndMatViewQueues();
            assertMapping("price_1h", "", "");

            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT");
            assertMapping("price_1h", "extra", "");
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN note INT");
            assertMapping("price_1h", "extra,note", "");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('b', 5.5, 50.0, '2024-01-01T01:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertMapping("price_1h", "extra,note", "");
            assertViewValid("price_1h");
        });
    }

    @Test
    public void testMappingColumnsAfterDropColumn() throws Exception {
        // DROP COLUMN only parses the view's SQL, so the mapping changes at the next refresh, which compiles the SQL
        // for the view's new columns.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            insertBaseRows();
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN amount");
            drainWalQueue();
            assertMapping("price_1h", "", "");
            execute("INSERT INTO base VALUES ('b', 5.5, 50.0, '2024-01-01T01:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertMapping("price_1h", "", "amount");
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN price");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('b', 6.5, 60.0, '2024-01-01T01:30:00.000000Z')");
            drainWalAndMatViewQueues();
            // in the order of the SQL's columns
            assertMapping("price_1h", "", "price,amount");
            assertViewValid("price_1h");
        });
    }

    @Test
    public void testMappingColumnsNullAfterFailedCompile() throws Exception {
        // A refresh whose compile fails leaves the mapping unknown.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            insertBaseRows();
            drainWalAndMatViewQueues();
            assertMapping("price_1h", "", "");

            // the view's SQL reads amount, so it no longer compiles; FULL compiles it before it truncates
            execute("ALTER TABLE base DROP COLUMN amount");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            assertMappingUnknown("price_1h");
            // the compile error points at amount in the view's SQL
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'price_1h'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tinvalidation_reason\ninvalid\t[" + VIEW_SQL.indexOf("amount") + "]: Invalid column: amount\n");
        });
    }

    @Test
    public void testMappingColumnsNullAfterRestart() throws Exception {
        // The mapping lives in memory only: a restarted server knows it again only once the view's SQL compiles.
        assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration conf = new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath()) {
                @Override
                public int getCommitMode() {
                    return configuration.getCommitMode();
                }
            };
            try (CairoEngine first = new CairoEngine(conf)) {
                first.load();
                first.execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                first.execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) AS price, last(amount) AS amount FROM base SAMPLE BY 1h) PARTITION BY DAY");
                first.execute("INSERT INTO base VALUES ('a', 1.5, 10.0, '2024-01-01T00:10:00.000000Z')");
                drainWalAndMatViewQueues(first);
                first.execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT");
                drainWalAndMatViewQueues(first);
                assertMapping(first, "price_1h", "extra", "");
            }
            try (CairoEngine second = new CairoEngine(conf)) {
                second.load();
                second.hydrateMatViewStateStore();
                assertMapping(second, "price_1h", null, null);
                second.execute("INSERT INTO base VALUES ('b', 2.5, 20.0, '2024-01-01T01:10:00.000000Z')");
                drainWalAndMatViewQueues(second);
                assertMapping(second, "price_1h", "extra", "");
            }
        });
    }

    @Test
    public void testMappingColumnsOnPositionalView() throws Exception {
        // A view whose SQL relies on generated names copies by position, which leaves no column out.
        assertMemoryLeak(() -> {
            createBase();
            executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW older AS (SELECT ts, sym, avg(price), avg(amount) FROM base SAMPLE BY 1h) PARTITION BY DAY");
            insertBaseRows();
            drainWalAndMatViewQueues();
            Assert.assertTrue(getViewState("older").isRecordCopyPositional());
            assertMapping("older", "", "");
        });
    }

    @Test
    public void testOneColumnPerStatement() throws Exception {
        // A materialized view is a WAL table, and a WAL table takes one structural change per statement.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("price_1h");
            final long seqTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn();
            for (String sql : new String[]{
                    "ALTER MATERIALIZED VIEW price_1h ADD COLUMN a INT, b INT",
                    "ALTER MATERIALIZED VIEW price_1h ADD COLUMN IF NOT EXISTS sym SYMBOL, b INT",
                    "ALTER MATERIALIZED VIEW price_1h ADD a INT, b INT",
                    "ALTER MATERIALIZED VIEW price_1h DROP COLUMN price, amount"
            }) {
                assertExceptionNoLeakCheck(sql, sql.indexOf(','), "materialized views accept one column per statement");
            }
            Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn());

            // a statement may end with a semicolon
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN a INT;");
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN price;");
            // IF NOT EXISTS of a column the view has changes nothing
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN IF NOT EXISTS sym SYMBOL");
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN IF NOT EXISTS sym SYMBOL;");
            drainWalQueue();
            assertColumns("price_1h", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    amount\tDOUBLE
                    a\tINT
                    """);

            // a table takes several columns per statement
            execute("CREATE TABLE plain (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("ALTER TABLE plain ADD COLUMN b INT, c INT");
            execute("ALTER TABLE plain ADD COLUMN IF NOT EXISTS a INT, d INT");
            execute("ALTER TABLE plain DROP COLUMN b, c");
            assertColumns("plain", """
                    column\ttype
                    a\tINT
                    ts\tTIMESTAMP
                    d\tINT
                    """);
        });
    }

    @Test
    public void testReadOnlyContextRefused() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            createView();
            drainWalAndMatViewQueues();
            try (SqlExecutionContext readOnly = new SqlExecutionContextImpl(engine, 1).with(ReadOnlySecurityContext.INSTANCE)) {
                for (String sql : new String[]{
                        "ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT",
                        "ALTER MATERIALIZED VIEW price_1h DROP COLUMN price"
                }) {
                    try {
                        execute(sql, readOnly);
                        Assert.fail("a read-only context must not change a view's columns: " + sql);
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isAuthorizationError());
                        TestUtils.assertContains(e.getFlyweightMessage(), "Write permission denied");
                    }
                }
            }
            drainWalQueue();
            assertColumns("price_1h", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    price\tDOUBLE
                    amount\tDOUBLE
                    """);
        });
    }

    @Test
    public void testRenameColumnHint() throws Exception {
        // A view's columns follow its SQL by name, so a view column cannot be renamed in place; the error describes
        // the add, alter, drop workaround. ALTER TABLE on the view fails the same way for RENAME COLUMN, and with
        // the usual error for anything else.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            drainWalAndMatViewQueues();
            for (String sql : new String[]{
                    "ALTER MATERIALIZED VIEW price_1h RENAME COLUMN sym TO sym2",
                    "ALTER MATERIALIZED VIEW price_1h rename column sym TO sym2",
                    "ALTER TABLE price_1h RENAME COLUMN sym TO sym2",
                    "ALTER TABLE price_1h RENAME COLUMN no_such TO x"
            }) {
                assertExceptionNoLeakCheck(sql, sql.toUpperCase().indexOf("RENAME"), RENAME_HINT);
            }
            final String renameTo = "ALTER MATERIALIZED VIEW price_1h RENAME TO price_2h";
            assertExceptionNoLeakCheck(renameTo, renameTo.indexOf("TO"), "'column' expected");
            final String renameNothing = "ALTER MATERIALIZED VIEW price_1h RENAME";
            assertExceptionNoLeakCheck(renameNothing, renameNothing.length(), "'column' expected");
            for (String sql : new String[]{
                    "ALTER TABLE price_1h RENAME TO price_2h",
                    "ALTER TABLE price_1h RENAME",
                    "ALTER TABLE price_1h ADD COLUMN extra INT",
                    "ALTER TABLE price_1h"
            }) {
                assertExceptionNoLeakCheck(sql, sql.indexOf("price_1h"), "cannot modify materialized view [view=price_1h]");
            }
            drainWalQueue();
            assertColumns("price_1h", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    price\tDOUBLE
                    amount\tDOUBLE
                    """);
        });
    }

    @Test
    public void testUnparsableSqlRefused() throws Exception {
        // A SQL that does not parse may rely on generated names too, so ADD and DROP COLUMN refuse it. No CREATE
        // stores such a SQL; the test puts one in the view's definition.
        assertMemoryLeak(() -> {
            createBase();
            createView();
            drainWalAndMatViewQueues();
            final MatViewDefinition definition = engine.getDependentViewGraph().getViewDefinition(engine.verifyTableName("price_1h"));
            Assert.assertNotNull(definition);
            final String viewSql = definition.getMatViewSql();
            try {
                definition.setMatViewSqlForTesting("SELECT ts, sym, last(price) AS price FROM base SAMPLE BY");
                assertSqlRefused("could not parse materialized view SQL, replace it with ALTER MATERIALIZED VIEW ... AS first [error=");
                definition.setMatViewSqlForTesting("INSERT INTO base VALUES ('a', 1.5, 10.0, '2024-01-01T00:10:00.000000Z')");
                assertSqlRefused("could not parse materialized view SQL, replace it with ALTER MATERIALIZED VIEW ... AS first "
                        + "[error=materialized view SQL is not a query]");
            } finally {
                definition.setMatViewSqlForTesting(viewSql);
            }
            drainWalQueue();
            assertColumns("price_1h", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    price\tDOUBLE
                    amount\tDOUBLE
                    """);
            assertQuery("SELECT count() FROM base").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    private void assertColumns(String viewName, String expected) throws Exception {
        assertQuery("SELECT \"column\", type FROM table_columns('" + viewName + "')")
                .noLeakCheck()
                .noRandomAccess()
                .returns(expected);
    }

    private void assertGeneratedNamesRefused(String viewName, String dropColumnName) throws Exception {
        final String message = "materialized view SQL relies on generated column names, "
                + "give its columns explicit names with ALTER MATERIALIZED VIEW ... AS first";
        final String add = "ALTER MATERIALIZED VIEW " + viewName + " ADD COLUMN extra INT";
        assertExceptionNoLeakCheck(add, add.indexOf(viewName), message);
        final String drop = "ALTER MATERIALIZED VIEW " + viewName + " DROP COLUMN " + dropColumnName;
        assertExceptionNoLeakCheck(drop, drop.indexOf(viewName), message);
    }

    private void assertMapping(String viewName, String unmappedViewColumns, String ignoredQueryColumns) throws Exception {
        assertMapping(engine, viewName, unmappedViewColumns, ignoredQueryColumns);
    }

    // Asserts the column mapping materialized_views() reports for the view; null stands for NULL.
    private void assertMapping(CairoEngine engine, String viewName, String unmappedViewColumns, String ignoredQueryColumns) throws Exception {
        final String expected = "unmapped_view_columns\tignored_query_columns\tunmapped_is_null\tignored_is_null\n"
                + (unmappedViewColumns != null ? unmappedViewColumns : "") + '\t'
                + (ignoredQueryColumns != null ? ignoredQueryColumns : "") + '\t'
                + (unmappedViewColumns == null) + '\t'
                + (ignoredQueryColumns == null) + '\n';
        try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1)) {
            assertQuery("SELECT unmapped_view_columns, ignored_query_columns, unmapped_view_columns IS NULL unmapped_is_null, "
                    + "ignored_query_columns IS NULL ignored_is_null FROM materialized_views() WHERE view_name = '" + viewName + "'")
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
        }
    }

    private void assertMappingUnknown(String viewName) throws Exception {
        assertMapping(viewName, null, null);
    }

    // Asserts that ADD and DROP COLUMN fail on the view price_1h with a message that starts with the given prefix.
    private static void assertSqlRefused(String messagePrefix) throws Exception {
        for (String sql : new String[]{
                "ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT",
                "ALTER MATERIALIZED VIEW price_1h DROP COLUMN amount"
        }) {
            try {
                execute(sql);
                Assert.fail("the statement must fail: " + sql);
            } catch (SqlException e) {
                Assert.assertEquals(sql, sql.indexOf("price_1h"), e.getPosition());
                Assert.assertTrue(e.getFlyweightMessage().toString(), Chars.startsWith(e.getFlyweightMessage(), messagePrefix));
            }
        }
    }

    private void assertViewValid(String viewName) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\nvalid\t\n");
    }

    private static void createBase() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private static void createView() throws SqlException {
        execute("CREATE MATERIALIZED VIEW price_1h AS (" + VIEW_SQL + ") PARTITION BY DAY");
    }

    private static void denySelectOnBase(TableToken tableToken, TableToken baseToken) {
        if (tableToken.equals(baseToken)) {
            throw CairoException.authorization().put("no SELECT on base");
        }
    }

    private static MatViewState getViewState(String viewName) {
        final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName(viewName));
        Assert.assertNotNull(state);
        return state;
    }

    private static void insertBaseRows() throws SqlException {
        execute("""
                INSERT INTO base VALUES
                ('a', 1.5, 10.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.5, 20.0, '2024-01-01T00:20:00.000000Z'),
                ('a', 3.5, 30.0, '2024-01-01T00:40:00.000000Z'),
                ('a', 4.5, 40.0, '2024-01-01T01:10:00.000000Z')
                """);
    }
}
