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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.ExpiryReadPolicy;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.SqlParser;
import io.questdb.griffin.engine.ops.CreateMatViewOperationImpl;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.str.Sinkable;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * CREATE MATERIALIZED VIEW v (col TYPE, ...) AS (...) creates the view with exactly the listed columns, in the
 * list's order, with the listed types and SYMBOL attributes. The list takes CREATE TABLE's column grammar, so its
 * errors are CREATE TABLE's at the same offset from the list. Refresh fills each listed column from the query
 * column of the same name, as after view ADD or DROP COLUMN: a listed column the query does not produce stays
 * empty, a query column the list lacks is ignored. Unlike PostgreSQL's names-only list, which renames the query's
 * columns by position, each entry has a type and maps by name.
 * <p>
 * Oracles: CREATE TABLE with the same definitions (its columns, its errors and the values it gives a column an
 * INSERT omits), reference queries over the view's SQL, ALTER ... SET EXPIRE on the same view, and the statement
 * text each test writes for the naming hint.
 */
public class MatViewColumnListTest extends AbstractCairoTest {
    // a column name longer than the default cairo.sql.column.alias.generated.max.size, 64
    private static final String LONG_NAME = "c" + "x".repeat(69);
    private static final String LONG_SPACED = "Avg temp " + "y".repeat(61);
    private static final String NAMING_HINT_SUFFIX = " AS your_column_name";
    private static final String SAMPLE_SQL = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h";
    private static final String VIEW_SQL = "SELECT ts, sym, last(price) AS price, last(amount) AS amount FROM base SAMPLE BY 1h";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // the production default, which names an unaliased column after its expression, cut to the maximum size
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
    }

    @Test
    public void testCreateTableGrammarErrors() throws Exception {
        // Each list is refused while the list parses, so CREATE TABLE with the same list and the same tail gives the
        // error the view must give, at the same offset from the list.
        assertMemoryLeak(() -> {
            createBase();
            final String tail = " AS (" + SAMPLE_SQL + ")";
            final String[][] cases = {
                    {"(a INT, A INT)", tail},
                    {"(ts TIMESTAMP, \"\u00e4\" INT, \"\u00c4\" INT)", tail},
                    {"(select INT)", tail},
                    {"(\"a/b\" INT)", tail},
                    {"(a, b)", tail},
                    {"()", tail},
                    {"(a INT", tail},
                    {"(a INT", ""},
                    {"(a INT,", tail},
                    {"(a INT,", ""},
                    {"(a FOO)", tail},
                    {"(a INTERVAL)", tail},
                    {"(a INT INDEX)", tail},
                    {"(a INT[])", tail},
                    {"(a DOUBLE])", tail},
                    {"(a)", tail},
                    {"(a INT,, b INT)", tail},
                    {"(a SYMBOL INDEX TYPE NOSUCH)", tail},
                    {"(a SYMBOL INDEX TYPE BITMAP INCLUDE (b))", tail},
                    {"(a SYMBOL INDEX INCLUDE ())", tail},
                    {"(a SYMBOL CAPACITY 3 CACHE INDEX TYPE POSTING CAPACITY 8)", tail},
                    {"(a SYMBOL CAPACITY 0)", tail},
                    {"(a DECIMAL(80, 2))", tail},
                    {"(a GEOHASH(61b))", tail},
                    {"(a GEOHASH)", tail},
                    {"(a INT NOT x)", tail},
            };
            for (String[] c : cases) {
                assertSameErrorAsCreateTable(c[0], c[1], c[1]);
            }
            // CREATE TABLE resolves INCLUDE names when it builds the operation, after the whole statement parsed
            final String tableTail = " TIMESTAMP(ts) PARTITION BY DAY WAL";
            assertSameErrorAsCreateTable("(ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (zz), price DOUBLE)", tableTail, tail);
            assertSameErrorAsCreateTable("(ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (sym), price DOUBLE)", tableTail, tail);
            assertSameErrorAsCreateTable("(ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (price, PRICE), price DOUBLE)", tableTail, tail);
            // the statement without the fault succeeds
            execute("CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE)" + tail);
            Assert.assertNotNull(engine.getTableTokenIfExists("v"));
        });
    }

    @Test
    public void testCreateWithoutColumnListUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            // end of input after the view name names the list among the expected tokens
            assertCreateRefused("CREATE MATERIALIZED VIEW v", 26, "'(' or 'as' or 'with' or 'refresh' expected");
            // a wrong token after the name keeps its message
            assertCreateRefused("CREATE MATERIALIZED VIEW v select AS (" + SAMPLE_SQL + ")", 27, "'refresh' or 'as' expected");
            // the list goes before WITH BASE
            final String afterBase = "CREATE MATERIALIZED VIEW v WITH BASE base (ts TIMESTAMP) AS (" + SAMPLE_SQL + ")";
            assertCreateRefused(afterBase, afterBase.indexOf("(ts"), "'refresh' or 'as' expected");
            // the list is not a query
            final String noAs = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL) (" + SAMPLE_SQL + ")";
            assertCreateRefused(noAs, noAs.indexOf("(SELECT"), "'refresh' or 'as' expected");
            final String eof = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL)";
            assertCreateRefused(eof, eof.length(), "'as' or 'with' or 'refresh' expected");
        });
    }

    @Test
    public void testDesignatedTimestampChecks() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE TABLE base_ns (sym SYMBOL, price DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE base_uuid (u UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // TIMESTAMP(col) names a column the list lacks, or a listed column that is not a timestamp
            String sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, price DOUBLE) AS (" + SAMPLE_SQL + ") TIMESTAMP(zz)";
            assertCreateRefused(sql, sql.lastIndexOf("zz"), "invalid designated timestamp column [name=zz]");
            sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, price DOUBLE) AS (" + SAMPLE_SQL + ") TIMESTAMP(price)";
            assertCreateRefused(sql, sql.lastIndexOf("price"), "TIMESTAMP column expected [actual=DOUBLE, columnName=price]");

            // a listed TIMESTAMP(col) the query does not produce
            sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, price DOUBLE, ts2 TIMESTAMP) AS (" + SAMPLE_SQL + ") TIMESTAMP(ts2)";
            assertCreateRefused(sql, sql.lastIndexOf("ts2"), "materialized view query does not produce the designated timestamp column [column=ts2]");

            // the query column of TIMESTAMP(col) must take the base table's designated timestamp, whatever the list says
            sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, x TIMESTAMP) AS (SELECT ts, sym, last(price) AS x FROM base SAMPLE BY 1h) TIMESTAMP(x)";
            assertCreateRefused(sql, sql.lastIndexOf("(x)") + 1, CreateMatViewOperationImpl.AGGREGATING_TIMESTAMP_ERROR);
            sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, ts2 TIMESTAMP, price DOUBLE) AS (SELECT ts, dateadd('h', 1, ts) AS ts2, price FROM base) TIMESTAMP(ts2)";
            assertCreateRefused(sql, sql.lastIndexOf("ts2"), CreateMatViewOperationImpl.PASSTHROUGH_TIMESTAMP_ERROR);

            // the view's designated timestamp must be listed
            sql = "CREATE MATERIALIZED VIEW v (sym SYMBOL, price DOUBLE) AS (" + SAMPLE_SQL + ")";
            assertCreateRefused(sql, sql.indexOf("sym SYMBOL"), "column list must include the designated timestamp column [column=ts]");
            sql = "CREATE MATERIALIZED VIEW v (sym SYMBOL, price DOUBLE) AS (SELECT timestamp_floor('1h', ts) AS bucket, sym, last(price) AS price FROM base)";
            assertCreateRefused(sql, sql.indexOf("sym SYMBOL"), "column list must include the designated timestamp column [column=bucket]");

            // with the query's timestamp type
            sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP_NS, sym SYMBOL, price DOUBLE) AS (" + SAMPLE_SQL + ")";
            assertCreateRefused(sql, sql.indexOf("TIMESTAMP_NS"), "timestamp type mismatch between materialized view and query [view=TIMESTAMP_NS, query=TIMESTAMP]");
            sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, price DOUBLE) AS (SELECT ts, sym, last(price) AS price FROM base_ns SAMPLE BY 1h)";
            assertCreateRefused(sql, sql.indexOf("TIMESTAMP"), "timestamp type mismatch between materialized view and query [view=TIMESTAMP, query=TIMESTAMP_NS]");

            // every listed column the query produces must take the query column's type
            sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, price UUID) AS (" + SAMPLE_SQL + ")";
            assertCreateRefused(sql, sql.indexOf("UUID"), "inconvertible types: DOUBLE -> UUID [from=price, to=price]");
            sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, Price DOUBLE[]) AS (" + SAMPLE_SQL + ")";
            assertCreateRefused(sql, sql.indexOf("DOUBLE[]"), "inconvertible types: DOUBLE -> DOUBLE[] [from=price, to=Price]");
            sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, price GEOHASH(5c)) AS (" + SAMPLE_SQL + ")";
            assertCreateRefused(sql, sql.indexOf("GEOHASH"), "inconvertible types: DOUBLE -> GEOHASH(5c) [from=price, to=price]");
            sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, u DECIMAL(18,3)) AS (SELECT ts, u FROM base_uuid)";
            assertCreateRefused(sql, sql.indexOf("DECIMAL"), "inconvertible types: UUID -> DECIMAL(18,3) [from=u, to=u]");

            // without a list, TIMESTAMP(col) names a column of the query, as before
            sql = "CREATE MATERIALIZED VIEW v AS (SELECT ts, ts AS ts2, price FROM base) TIMESTAMP(nosuch)";
            assertCreateRefused(sql, sql.lastIndexOf("nosuch"), "TIMESTAMP column does not exist [name=nosuch]");
            execute("CREATE MATERIALIZED VIEW v5 AS (SELECT ts, ts AS ts2, price FROM base) TIMESTAMP(ts2)");

            // corrected, each statement succeeds
            execute("CREATE MATERIALIZED VIEW v1 (ts TIMESTAMP_NS, sym SYMBOL, price DOUBLE) AS (SELECT ts, sym, last(price) AS price FROM base_ns SAMPLE BY 1h)");
            execute("CREATE MATERIALIZED VIEW v2 (sym SYMBOL, price DOUBLE, TS TIMESTAMP) AS (" + SAMPLE_SQL + ")");
            execute("CREATE MATERIALIZED VIEW v3 (sym SYMBOL, price DOUBLE, bucket TIMESTAMP) AS (SELECT timestamp_floor('1h', ts) AS bucket, sym, last(price) AS price FROM base ORDER BY bucket)");
            execute("CREATE MATERIALIZED VIEW v4 (ts TIMESTAMP, ts2 TIMESTAMP, price DOUBLE) AS (SELECT ts, ts AS ts2, price FROM base) TIMESTAMP(ts2)");
            insertBaseRows();
            drainWalAndMatViewQueues();
            assertColumns("v1", """
                    column\ttype\tdesignated
                    ts\tTIMESTAMP_NS\ttrue
                    sym\tSYMBOL\tfalse
                    price\tDOUBLE\tfalse
                    """);
            assertColumns("v2", """
                    column\ttype\tdesignated
                    sym\tSYMBOL\tfalse
                    price\tDOUBLE\tfalse
                    TS\tTIMESTAMP\ttrue
                    """);
            assertColumns("v3", """
                    column\ttype\tdesignated
                    sym\tSYMBOL\tfalse
                    price\tDOUBLE\tfalse
                    bucket\tTIMESTAMP\ttrue
                    """);
            assertColumns("v4", """
                    column\ttype\tdesignated
                    ts\tTIMESTAMP\tfalse
                    ts2\tTIMESTAMP\ttrue
                    price\tDOUBLE\tfalse
                    """);
            assertSqlCursors("SELECT sym, price, ts AS TS FROM (" + SAMPLE_SQL + ") ORDER BY ts, sym", "SELECT * FROM v2 ORDER BY TS, sym");
            assertSqlCursors(
                    "SELECT sym, price, bucket FROM (SELECT timestamp_floor('1h', ts) AS bucket, sym, last(price) AS price FROM base ORDER BY bucket) ORDER BY bucket, sym",
                    "SELECT * FROM v3 ORDER BY bucket, sym"
            );
            assertSqlCursors("SELECT ts, ts AS ts2, price FROM base", "v4");
            assertSqlCursors("v4", "v5");
            assertColumns("v5", """
                    column\ttype\tdesignated
                    ts\tTIMESTAMP\tfalse
                    ts2\tTIMESTAMP\ttrue
                    price\tDOUBLE\tfalse
                    """);
        });
    }

    @Test
    public void testExplicitNamesHookDoesNotApply() throws Exception {
        // A list view refreshes by name only, so the test hook that lets a CREATE rely on generated names does not
        // apply to it; the hook's check refuses the column with the message a CREATE without the hook gives. The
        // legacy alias rules name avg(price) avg, a valid name, which the hook would let through.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
            createBase();
            final String query = "SELECT ts, avg(price) FROM base SAMPLE BY 1h";
            final String noList = "CREATE MATERIALIZED VIEW v AS (" + query + ")";
            final String list = "(ts TIMESTAMP, avg DOUBLE)";
            final String withList = "CREATE MATERIALIZED VIEW v " + list + " AS (" + query + ")";
            int expectedPosition = -1;
            String expectedMessage = null;
            try {
                execute(noList);
                Assert.fail("the generated name must be refused");
            } catch (SqlException e) {
                expectedPosition = e.getPosition() + list.length() + 1;
                expectedMessage = e.getFlyweightMessage().toString();
            }
            SqlParser.setMatViewExplicitNamesCheckSkipped(true);
            try {
                assertCreateRefused(withList, expectedPosition, expectedMessage);
                // the hook still applies without a list, and refresh copies the view by position
                execute(noList);
                Assert.assertNotNull(engine.getTableTokenIfExists("v"));
            } finally {
                SqlParser.setMatViewExplicitNamesCheckSkipped(false);
            }
            insertBaseRows();
            drainWalAndMatViewQueues();
            assertQuery("v")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tavg
                            2024-01-01T00:00:00.000000Z\t2.5
                            2024-01-01T01:00:00.000000Z\t4.5
                            """);
            final String add = "ALTER MATERIALIZED VIEW v ADD COLUMN extra INT";
            assertExceptionNoLeakCheck(add, add.indexOf("v ADD"), "materialized view SQL relies on generated column names");
        });
    }

    @Test
    public void testExpireRowsAgainstList() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            final String list = "(ts TIMESTAMP, sym SYMBOL, price DOUBLE, extra INT, region SYMBOL, x LONG128)";
            // a scalar WHEN on a list-only column
            assertCreateAndAlterAgree(list, VIEW_SQL, "", "WHEN extra > 0", null, 3);
            // KEEP LATEST on a list-only key: every row has a NULL region, so the latest row stays
            assertCreateAndAlterAgree(list, VIEW_SQL, "", "KEEP LATEST PARTITION BY region", null, 1);
            assertCreateAndAlterAgree(list, VIEW_SQL, "", "KEEP LATEST PARTITION BY x", null, 1);
            assertCreateAndAlterAgree(list, VIEW_SQL, "", "KEEP 2 HIGHEST ON price PARTITION BY sym", null, 3);
            assertCreateAndAlterAgree(list, VIEW_SQL, "", "WHEN price < max(price) OVER (PARTITION BY sym)", null, 2);
            // a query column the list lacks is no column of the view
            assertCreateAndAlterAgree(list, VIEW_SQL, "", "WHEN amount > 0", "invalid EXPIRE ROWS predicate: Invalid column: amount", 0);
            assertCreateAndAlterAgree(list, VIEW_SQL, "", "KEEP LATEST PARTITION BY amount", "invalid EXPIRE ROWS KEEP LATEST PARTITION BY column: amount", 0);
            assertCreateAndAlterAgree(list, VIEW_SQL, "", "KEEP HIGHEST ON amount PARTITION BY sym", "invalid EXPIRE ROWS KEEP column: amount", 0);
            assertCreateAndAlterAgree(
                    "(ts TIMESTAMP, sym SYMBOL, price DOUBLE, \"__qdb_re_keep\" BOOLEAN)",
                    VIEW_SQL,
                    "",
                    "KEEP HIGHEST ON price PARTITION BY sym",
                    "EXPIRE ROWS KEEP / window retention cannot be used on a view with a column named '__qdb_re_keep'",
                    0
            );
            // the view's designated timestamp is the list's TIMESTAMP(ts2), including for RANGE frames
            assertCreateAndAlterAgree(
                    "(ts TIMESTAMP, ts2 TIMESTAMP, price DOUBLE)",
                    "SELECT ts, ts AS ts2, price FROM base",
                    " TIMESTAMP(ts2)",
                    "WHEN price < max(price) OVER (ORDER BY ts2 RANGE BETWEEN 1 HOUR PRECEDING AND CURRENT ROW)",
                    null,
                    -1
            );
            assertCreateAndAlterAgree(
                    "(ts TIMESTAMP, ts2 TIMESTAMP, sym SYMBOL, price DOUBLE)",
                    "SELECT ts, ts AS ts2, sym, price FROM base",
                    " TIMESTAMP(ts2)",
                    "KEEP LATEST ON ts2 PARTITION BY sym",
                    null,
                    2
            );
            // The query of a GROUP BY timestamp_floor() view without ORDER BY has no designated timestamp, the list
            // does. Such a view does not refresh, with or without a list, a known gap tracked separately, so both
            // views read no row. With the list, a RANGE frame orders by the list's designated timestamp. (Without a
            // list, CREATE refuses a RANGE-frame window policy on this shape, another gap tracked separately.)
            final String floor = "SELECT timestamp_floor('1h', ts) AS ts, sym, last(price) AS price FROM base";
            assertCreateAndAlterAgree("(ts TIMESTAMP, sym SYMBOL, price DOUBLE)", floor, "", "KEEP LATEST PARTITION BY sym", null, -1);
            assertCreateAndAlterAgree("(ts TIMESTAMP, sym SYMBOL, price DOUBLE)", floor, "", "KEEP 2 HIGHEST ON price PARTITION BY sym", null, -1);
            assertCreateAndAlterAgree(
                    "(ts TIMESTAMP, sym SYMBOL, price DOUBLE)",
                    floor,
                    "",
                    "WHEN price < max(price) OVER (ORDER BY ts RANGE BETWEEN 1 HOUR PRECEDING AND CURRENT ROW)",
                    null,
                    -1
            );
            final String orderedFloor = floor + " ORDER BY ts";
            assertCreateAndAlterAgree("(ts TIMESTAMP, sym SYMBOL, price DOUBLE)", orderedFloor, "", "KEEP LATEST PARTITION BY sym", null, 2);
            assertCreateAndAlterAgree("(ts TIMESTAMP, sym SYMBOL, price DOUBLE)", orderedFloor, "", "KEEP 2 HIGHEST ON price PARTITION BY sym", null, 3);
            // the timestamp in another letter case than the query's
            assertCreateAndAlterAgree("(TS TIMESTAMP, sym SYMBOL, price DOUBLE)", SAMPLE_SQL, "", "KEEP LATEST PARTITION BY sym", null, 2);
            assertCreateAndAlterAgree("(TS TIMESTAMP, sym SYMBOL, price DOUBLE)", SAMPLE_SQL, "", "KEEP LATEST ON TS PARTITION BY sym", null, 2);
            // without a list, the policy binds against the query's columns, as before
            assertCreateAndAlterAgree("", VIEW_SQL, "", "KEEP LATEST PARTITION BY sym", null, 2);
            assertCreateAndAlterAgree("", VIEW_SQL, "", "WHEN amount > 25", null, 1);
            assertCreateAndAlterAgree("", VIEW_SQL, "", "WHEN extra > 0", "invalid EXPIRE ROWS predicate: Invalid column: extra", 0);
        });
    }

    @Test
    public void testIfNotExists() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE MATERIALIZED VIEW v (sym SYMBOL, ts TIMESTAMP) AS (" + SAMPLE_SQL + ")");
            execute("CREATE MATERIALIZED VIEW IF NOT EXISTS v (ts TIMESTAMP, price DOUBLE, extra INT) AS (" + SAMPLE_SQL + ")");
            drainWalAndMatViewQueues();
            assertColumns("v", """
                    column\ttype\tdesignated
                    sym\tSYMBOL\tfalse
                    ts\tTIMESTAMP\ttrue
                    """);
            // parse errors of the list still fire
            final String badList = "CREATE MATERIALIZED VIEW IF NOT EXISTS v (ts TIMESTAMP, price FOO) AS (" + SAMPLE_SQL + ")";
            try {
                execute(badList);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(badList.indexOf("FOO"), e.getPosition());
                TestUtils.assertEquals("unsupported column type: FOO", e.getFlyweightMessage());
            }
            final String onTable = "CREATE MATERIALIZED VIEW IF NOT EXISTS base (ts TIMESTAMP, price DOUBLE) AS (" + SAMPLE_SQL + ")";
            try {
                execute(onTable);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(onTable.indexOf("base"), e.getPosition());
                TestUtils.assertEquals("table or view with the requested name already exists", e.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testIndexClauseNamesListedColumn() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String list = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, price DOUBLE, region SYMBOL) AS (" + SAMPLE_SQL + ")";
            String sql = list + ", INDEX (zz)";
            assertCreateRefused(sql, sql.lastIndexOf("zz"), "Invalid column: zz");
            sql = list + ", INDEX (price)";
            assertCreateRefused(sql, sql.lastIndexOf("price"), "indexes are supported only for SYMBOL columns [columnName=price, columnType=DOUBLE]");
            sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL INDEX, price DOUBLE) AS (" + SAMPLE_SQL + "), INDEX (sym)";
            assertCreateRefused(sql, sql.lastIndexOf("sym"), "duplicate index clause");
            // a list-only SYMBOL may take an index, as in CREATE TABLE
            execute(list + ", INDEX (region CAPACITY 256), INDEX (sym TYPE POSTING)");
            execute("CREATE TABLE oracle (ts TIMESTAMP, sym SYMBOL, price DOUBLE, region SYMBOL), INDEX (region CAPACITY 256), INDEX (sym TYPE POSTING) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertSameColumnsAsCreateTable("v", "oracle");
            assertQuery("SELECT \"column\", indexed, indexBlockCapacity, indexType FROM table_columns('v') WHERE indexed")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\tindexed\tindexBlockCapacity\tindexType
                            sym\ttrue\t256\tPOSTING
                            region\ttrue\t256\tBITMAP
                            """);
        });
    }

    @Test
    public void testLetterCaseAndComments() throws Exception {
        // keywords and types in any letter case, and comments between the list's tokens
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            execute("""
                    create materialized view v (
                        -- the bucket
                        ts timestamp,
                        /* the key */ SYM symbol capacity 64 /* cached */ cache,
                        Price Double -- the last price
                    ) as (select ts, sym, last(price) as price from base sample by 1h)""");
            drainWalAndMatViewQueues();
            assertQuery("SELECT \"column\", type, designated, symbolCached, symbolCapacity FROM table_columns('v')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\ttype\tdesignated\tsymbolCached\tsymbolCapacity
                            ts\tTIMESTAMP\ttrue\tfalse\t0
                            SYM\tSYMBOL\tfalse\ttrue\t64
                            Price\tDOUBLE\tfalse\tfalse\t0
                            """);
            assertSqlCursors("SELECT ts, sym AS SYM, price AS Price FROM (" + SAMPLE_SQL + ") ORDER BY ts, sym", "SELECT * FROM v ORDER BY ts, SYM");
            assertMapping("v", "", "");
        });
    }

    @Test
    public void testListColumnsAndRefreshByName() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW v (sym SYMBOL, price DOUBLE, extra INT, ts TIMESTAMP) AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertColumns("v", """
                    column\ttype\tdesignated
                    sym\tSYMBOL\tfalse
                    price\tDOUBLE\tfalse
                    extra\tINT\tfalse
                    ts\tTIMESTAMP\ttrue
                    """);
            assertQuery("SELECT * FROM v WHERE sym = 'a'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            sym\tprice\textra\tts
                            a\t3.5\tnull\t2024-01-01T00:00:00.000000Z
                            a\t4.5\tnull\t2024-01-01T01:00:00.000000Z
                            """);
            assertSqlCursors("SELECT sym, price, CAST(NULL AS INT) extra, ts FROM (" + VIEW_SQL + ") ORDER BY ts, sym", "SELECT * FROM v ORDER BY ts, sym");
            assertMapping("v", "extra", "amount");
            assertViewValid("v");

            // incremental refresh and REFRESH FULL keep filling by name
            execute("INSERT INTO base VALUES ('b', 5.5, 50.0, '2024-01-01T01:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertSqlCursors("SELECT sym, price, CAST(NULL AS INT) extra, ts FROM (" + VIEW_SQL + ") ORDER BY ts, sym", "SELECT * FROM v ORDER BY ts, sym");
            execute("REFRESH MATERIALIZED VIEW v FULL");
            drainWalAndMatViewQueues();
            assertSqlCursors("SELECT sym, price, CAST(NULL AS INT) extra, ts FROM (" + VIEW_SQL + ") ORDER BY ts, sym", "SELECT * FROM v ORDER BY ts, sym");
            assertViewValid("v");

            // the same state built by CREATE and view ADD/DROP COLUMN gives the same rows and mapping
            execute("CREATE MATERIALIZED VIEW w AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW w ADD COLUMN extra INT");
            execute("ALTER MATERIALIZED VIEW w DROP COLUMN amount");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW w FULL");
            drainWalAndMatViewQueues();
            assertSqlCursors("SELECT sym, price, extra, ts FROM w ORDER BY ts, sym", "SELECT * FROM v ORDER BY ts, sym");
            assertMapping("w", "extra", "amount");
        });
    }

    @Test
    public void testMappingSummaryNullUntilFirstCompile() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW v (ts TIMESTAMP, avg_price DOUBLE) REFRESH MANUAL DEFERRED AS (SELECT ts, avg(price) AS price FROM base SAMPLE BY 1h)");
            drainWalAndMatViewQueues();
            assertMapping("v", null, null);
            execute("REFRESH MATERIALIZED VIEW v FULL");
            drainWalAndMatViewQueues();
            // PostgreSQL's list would rename price to avg_price by position; this one maps by name
            assertMapping("v", "avg_price", "price");
            assertQuery("v")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tavg_price
                            2024-01-01T00:00:00.000000Z\tnull
                            2024-01-01T01:00:00.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testModelToSink() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final ExecutionModel model = compiler.generateExecutionModel(
                        "CREATE MATERIALIZED VIEW v (sym SYMBOL CAPACITY 64 NOCACHE, price DOUBLE, ts TIMESTAMP) AS ("
                                + SAMPLE_SQL + "), INDEX (sym CAPACITY 512) TIMESTAMP(ts)",
                        sqlExecutionContext
                );
                Assert.assertEquals(ExecutionModel.CREATE_MAT_VIEW, model.getModelType());
                sink.clear();
                ((Sinkable) model).toSink(sink);
                TestUtils.assertEquals(
                        "create materialized view v (sym SYMBOL capacity 64 nocache index capacity 512, price DOUBLE, ts TIMESTAMP)"
                                + " with base base refresh immediate as (select-choose ts, sym, last(price) price from"
                                + " (base sample by 1h align to calendar with offset '00:00')) timestamp(ts)",
                        sink
                );
                // without a list, the INDEX clause prints after the query
                final ExecutionModel noList = compiler.generateExecutionModel(
                        "CREATE MATERIALIZED VIEW v AS (" + SAMPLE_SQL + "), INDEX (sym CAPACITY 512)",
                        sqlExecutionContext
                );
                sink.clear();
                ((Sinkable) noList).toSink(sink);
                TestUtils.assertEquals(
                        "create materialized view v with base base refresh immediate as (select-choose ts, sym, last(price) price from"
                                + " (base sample by 1h align to calendar with offset '00:00')), index(sym capacity 512)",
                        sink
                );
                // CREATE TABLE prints its columns as the list does
                final ExecutionModel table = compiler.generateExecutionModel(
                        "CREATE TABLE t (a INT, s SYMBOL CAPACITY 64 NOCACHE INDEX CAPACITY 512)",
                        sqlExecutionContext
                );
                sink.clear();
                ((Sinkable) table).toSink(sink);
                TestUtils.assertEquals("create atomic table t (a INT, s SYMBOL capacity 64 nocache index capacity 512)", sink);
            }
        });
    }

    @Test
    public void testNamingCheckAliasMaxSize4() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_GENERATED_MAX_SIZE, 4);
            execute("CREATE TABLE b4 (ts TIMESTAMP, sym SYMBOL, price DOUBLE, pric DOUBLE, \"select\" DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO b4 VALUES ('2024-01-01T00:10:00.000000Z', 'a', 1.5, 2.5, 3.5)");
            drainWalQueue();
            // the oracle: refresh copies the SQL without a list by position, so view ADD COLUMN refuses it
            execute("CREATE MATERIALIZED VIEW o AS (SELECT ts, sym, price FROM b4)");
            drainWalAndMatViewQueues();
            final String add = "ALTER MATERIALIZED VIEW o ADD COLUMN extra INT";
            assertExceptionNoLeakCheck(add, add.indexOf("o ADD"), "materialized view SQL relies on generated column names");

            String sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, price DOUBLE) AS (SELECT ts, sym, price FROM b4)";
            assertCreateRefused(sql, sql.indexOf("price FROM"), "column 'price' requires an explicit alias. Use: price" + NAMING_HINT_SUFFIX);
            sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, \"select\" DOUBLE) AS (SELECT ts, \"select\" FROM b4)";
            assertCreateRefused(sql, sql.indexOf("\"select\" FROM"), "column 'select' requires an explicit alias. Use: \"select\"" + NAMING_HINT_SUFFIX);

            execute("CREATE MATERIALIZED VIEW v1 (ts TIMESTAMP, pric DOUBLE) AS (SELECT ts, pric FROM b4)");
            execute("CREATE MATERIALIZED VIEW v2 (ts TIMESTAMP, sym SYMBOL, price DOUBLE) AS (SELECT ts, sym, price AS price FROM b4)");
            execute("CREATE MATERIALIZED VIEW v3 (ts TIMESTAMP, \"select\" DOUBLE) AS (SELECT ts, \"select\" AS \"select\" FROM b4)");
            drainWalAndMatViewQueues();
            assertSqlCursors("SELECT ts, pric FROM b4", "v1");
            assertSqlCursors("SELECT ts, sym, price AS price FROM b4", "v2");
            assertSqlCursors("SELECT ts, \"select\" AS \"select\" FROM b4", "v3");
            assertMapping("v2", "", "");
        });
    }

    @Test
    public void testNamingCheckAliasMaxSize128() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_GENERATED_MAX_SIZE, 128);
        assertNamingCheckRefusesLongReference();
    }

    @Test
    public void testNamingCheckAliasMaxSizeDefault() throws Exception {
        assertMemoryLeak(() -> {
            final String name64 = "c" + "x".repeat(63);
            final String name65 = "c" + "x".repeat(64);
            execute("CREATE TABLE nb (ts TIMESTAMP, " + name64 + " DOUBLE, " + name65 + " DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO nb VALUES ('2024-01-01T00:10:00.000000Z', 1.5, 2.5)");
            drainWalQueue();
            final String sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, " + name65 + " DOUBLE) AS (SELECT ts, " + name65 + " FROM nb)";
            assertCreateRefused(sql, sql.indexOf(name65 + " FROM"), "column '" + name65 + "' requires an explicit alias. Use: " + name65 + NAMING_HINT_SUFFIX);
            execute("CREATE MATERIALIZED VIEW v (" + name64 + " DOUBLE, ts TIMESTAMP) AS (SELECT ts, " + name64 + " FROM nb)");
            drainWalAndMatViewQueues();
            assertSqlCursors("SELECT " + name64 + ", ts FROM nb", "v");
        });
    }

    @Test
    public void testNamingCheckInEveryContext() throws Exception {
        assertMemoryLeak(() -> {
            createNamingBase();
            execute("CREATE TABLE other (ts TIMESTAMP, k SYMBOL, " + LONG_NAME + " DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO other VALUES ('2024-01-01T00:05:00.000000Z', 'a', 7.5)");
            drainWalQueue();
            final String list = "(ts TIMESTAMP, v DOUBLE)";
            // a sub-query
            assertNamingRefusedThenFixed(
                    list,
                    "",
                    "SELECT ts, v FROM (SELECT ts, " + LONG_NAME + " FROM nbase)",
                    LONG_NAME,
                    LONG_NAME,
                    1
            );
            // a CTE
            assertNamingRefusedThenFixed(
                    list,
                    "",
                    "WITH c AS (SELECT ts, " + LONG_NAME + " FROM nbase) SELECT ts, v FROM c",
                    LONG_NAME,
                    LONG_NAME,
                    1
            );
            // a join branch of a WITH BASE view
            assertNamingRefusedThenFixed(
                    list,
                    " WITH BASE nbase",
                    "SELECT n.ts, last(o.v) AS v FROM nbase n JOIN (SELECT k, " + LONG_NAME + " FROM other) o ON (k) SAMPLE BY 1h",
                    LONG_NAME,
                    LONG_NAME,
                    1
            );
            // the second branch of a UNION inside a join branch
            assertNamingRefusedThenFixed(
                    list,
                    " WITH BASE nbase",
                    "SELECT n.ts, last(o.v) AS v FROM nbase n JOIN (SELECT k, " + LONG_NAME + " AS v FROM other UNION ALL SELECT k, "
                            + LONG_NAME + " FROM other) o ON (k) SAMPLE BY 1h",
                    LONG_NAME,
                    LONG_NAME,
                    2
            );
            // a declared variable: the hint names the variable where the select list uses it
            final String declare = "DECLARE @c := " + LONG_NAME + " SELECT ts, @c FROM nbase";
            final String sql = "CREATE MATERIALIZED VIEW v " + list + " AS (" + declare + ")";
            assertCreateRefused(sql, sql.indexOf("@c FROM"), "column '" + LONG_NAME + "' requires an explicit alias. Use: @c" + NAMING_HINT_SUFFIX);
            execute("CREATE MATERIALIZED VIEW v " + list + " AS (DECLARE @c := " + LONG_NAME + " SELECT ts, @c AS v FROM nbase)");
            drainWalAndMatViewQueues();
            assertSqlCursors("SELECT ts, " + LONG_NAME + " AS v FROM nbase", "v");
            execute("DROP MATERIALIZED VIEW v");

            // a regular view's own text is not part of the SQL: its generated name reaches the query as a column name
            execute("CREATE VIEW rv AS (SELECT ts, " + LONG_NAME + " FROM nbase)");
            final String cut = LONG_NAME.substring(0, 64);
            execute("CREATE MATERIALIZED VIEW v (" + cut + " DOUBLE, ts TIMESTAMP) AS (SELECT * FROM rv)");
            drainWalAndMatViewQueues();
            assertSqlCursors("SELECT " + LONG_NAME + " AS " + cut + ", ts FROM nbase", "v");
            execute("DROP MATERIALIZED VIEW v");
            final String overView = "CREATE MATERIALIZED VIEW v " + list + " AS (SELECT ts, " + LONG_NAME + " FROM rv)";
            assertCreateRefused(overView, overView.indexOf(LONG_NAME + " FROM"), "column '" + LONG_NAME + "' requires an explicit alias. Use: " + LONG_NAME + NAMING_HINT_SUFFIX);
        });
    }

    @Test
    public void testNamingCheckLegacyAliasMode() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
        assertNamingCheckRefusesLongReference();
    }

    @Test
    public void testNamingCheckSpellings() throws Exception {
        // A reference to a column whose name is longer than 64 characters makes refresh copy the SQL by position,
        // which a list view cannot take. The hint names the column in full and repeats the select item as written,
        // so that following it names the same column.
        assertMemoryLeak(() -> {
            createNamingBase();
            final String list = "(ts TIMESTAMP, v DOUBLE)";
            final String quoted = '"' + LONG_SPACED + '"';
            final String[][] cases = {
                    // select list, item as written, column name
                    {"ts, " + LONG_NAME + " FROM nbase", LONG_NAME, LONG_NAME},
                    {"b.ts, b." + LONG_NAME + " FROM nbase b", "b." + LONG_NAME, LONG_NAME},
                    {"\"b\".ts, \"b\"." + LONG_NAME + " FROM nbase \"b\"", "\"b\"." + LONG_NAME, LONG_NAME},
                    {"ts, " + quoted + " FROM nbase", quoted, LONG_SPACED},
                    {"b.ts, b." + quoted + " FROM nbase b", "b." + quoted, LONG_SPACED},
                    {"ts, (" + LONG_NAME + ") FROM nbase", "(" + LONG_NAME + ")", LONG_NAME},
                    {"ts, " + LONG_NAME + "--c\n FROM nbase", LONG_NAME, LONG_NAME},
                    {"ts, " + LONG_NAME + "/*c*/ FROM nbase", LONG_NAME, LONG_NAME},
                    {"ts, " + LONG_NAME + "\n FROM nbase", LONG_NAME, LONG_NAME},
            };
            for (String[] c : cases) {
                assertNamingRefusedThenFixed(list, "", "SELECT " + c[0], c[1], c[2], 1);
            }
        });
    }

    @Test
    public void testParquetColumnSettingsRefused() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            String sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, price DOUBLE PARQUET(plain)) AS (" + SAMPLE_SQL + ")";
            assertCreateRefused(sql, sql.indexOf("PARQUET"), "PARQUET column settings are not supported on materialized views");
            sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL INDEX PARQUET(plain), price DOUBLE) AS (" + SAMPLE_SQL + ")";
            assertCreateRefused(sql, sql.indexOf("PARQUET"), "PARQUET column settings are not supported on materialized views");
            sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL CAPACITY 8 PARQUET(plain), price DOUBLE) AS (" + SAMPLE_SQL + ")";
            assertCreateRefused(sql, sql.indexOf("PARQUET"), "PARQUET column settings are not supported on materialized views");
            // CREATE TABLE keeps accepting them
            execute("CREATE TABLE t (ts TIMESTAMP, price DOUBLE PARQUET(plain)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        });
    }

    @Test
    public void testPassthroughInheritsNothingFromBase() throws Exception {
        // the list is authoritative: no SYMBOL capacity copied from the base, no index inherited
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ib (ts TIMESTAMP, sym SYMBOL CAPACITY 1024 INDEX, sym2 SYMBOL CAPACITY 2048, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO ib VALUES ('2024-01-01T00:10:00.000000Z', 'a', 'x', 1.5), ('2024-01-01T00:20:00.000000Z', 'b', 'y', 2.5)");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, sym2 SYMBOL INDEX, price DOUBLE) AS (SELECT * FROM ib)");
            execute("CREATE TABLE oracle (ts TIMESTAMP, sym SYMBOL, sym2 SYMBOL INDEX, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalAndMatViewQueues();
            assertSameColumnsAsCreateTable("v", "oracle");
            assertQuery("SELECT \"column\", indexed, symbolCapacity FROM table_columns('v') WHERE type = 'SYMBOL'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\tindexed\tsymbolCapacity
                            sym\tfalse\t128
                            sym2\ttrue\t128
                            """);
            assertSqlCursors("ib", "v");
            assertQuery("SELECT ts, price FROM v WHERE sym2 = 'y'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tprice
                            2024-01-01T00:20:00.000000Z\t2.5
                            """);
        });
    }

    @Test
    public void testRestart() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW v (sym SYMBOL, price DOUBLE, extra INT, ts TIMESTAMP) AS (" + VIEW_SQL + ")");
            execute("CREATE MATERIALIZED VIEW d (price DOUBLE, ts TIMESTAMP, sym SYMBOL) REFRESH MANUAL DEFERRED AS (" + VIEW_SQL + ")");
            drainWalAndMatViewQueues();
            final String columns = """
                    column\ttype\tdesignated
                    sym\tSYMBOL\tfalse
                    price\tDOUBLE\tfalse
                    extra\tINT\tfalse
                    ts\tTIMESTAMP\ttrue
                    """;
            assertColumns("v", columns);
            engine.releaseInactive();
            engine.releaseAllWriters();
            try (
                    CairoEngine restarted = new CairoEngine(configuration);
                    SqlExecutionContext context = TestUtils.createSqlExecutionCtx(restarted)
            ) {
                restarted.load();
                restarted.buildViewGraphs();
                assertQuery("SELECT \"column\", type, designated FROM table_columns('v')")
                        .withEngine(restarted).withContext(context).noLeakCheck().noRandomAccess().returns(columns);
                // the mapping is unknown until the SQL compiles in this process
                assertMapping(restarted, context, "v", null, null);
                restarted.execute("INSERT INTO base VALUES ('b', 5.5, 50.0, '2024-01-01T01:20:00.000000Z')", context);
                restarted.execute("REFRESH MATERIALIZED VIEW d FULL", context);
                drainWalAndMatViewQueues(restarted);
                assertMapping(restarted, context, "v", "extra", "amount");
                assertMapping(restarted, context, "d", "", "amount");
                try (SqlCompiler compiler = restarted.getSqlCompiler()) {
                    TestUtils.assertSqlCursors(
                            compiler,
                            context,
                            "SELECT sym, price, CAST(NULL AS INT) extra, ts FROM (" + VIEW_SQL + ") ORDER BY ts, sym",
                            "SELECT * FROM v ORDER BY ts, sym",
                            LOG
                    );
                    TestUtils.assertSqlCursors(
                            compiler,
                            context,
                            "SELECT price, ts, sym FROM (" + VIEW_SQL + ") ORDER BY ts, sym",
                            "SELECT * FROM d ORDER BY ts, sym",
                            LOG
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolAttributesFollowConfiguration() throws Exception {
        setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_CAPACITY, 512);
        setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_CACHE_FLAG, "false");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ib (ts TIMESTAMP, sym SYMBOL CAPACITY 1024 CACHE INDEX, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, s2 SYMBOL INDEX, price DOUBLE) AS (SELECT * FROM ib)");
            execute("CREATE TABLE oracle (ts TIMESTAMP, sym SYMBOL, s2 SYMBOL INDEX, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalAndMatViewQueues();
            assertSameColumnsAsCreateTable("v", "oracle");
            assertQuery("SELECT \"column\", symbolCached, symbolCapacity FROM table_columns('v') WHERE type = 'SYMBOL'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\tsymbolCached\tsymbolCapacity
                            sym\tfalse\t512
                            s2\tfalse\t512
                            """);
        });
    }

    @Test
    public void testSymbolCapacityBelowDistinctValues() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            execute("""
                    INSERT INTO base
                    SELECT ('s' || x)::SYMBOL, x::DOUBLE, (x * 2)::DOUBLE, '2024-01-01T00:00:00.000000Z'::TIMESTAMP + x * 60_000_000L
                    FROM long_sequence(1_000)
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL CAPACITY 2 CACHE, price DOUBLE) AS (" + VIEW_SQL + ")");
            drainWalAndMatViewQueues();
            final String reference = "SELECT ts, sym, price FROM (" + VIEW_SQL + ") ORDER BY ts, sym";
            assertSqlCursors(reference, "SELECT * FROM v ORDER BY ts, sym");
            execute("""
                    INSERT INTO base
                    SELECT ('t' || x)::SYMBOL, x::DOUBLE, x::DOUBLE, '2024-01-02T00:00:00.000000Z'::TIMESTAMP + x * 60_000_000L
                    FROM long_sequence(1_000)
                    """);
            drainWalAndMatViewQueues();
            assertSqlCursors(reference, "SELECT * FROM v ORDER BY ts, sym");
            execute("REFRESH MATERIALIZED VIEW v FULL");
            drainWalAndMatViewQueues();
            assertSqlCursors(reference, "SELECT * FROM v ORDER BY ts, sym");
            assertQuery("SELECT count() count, count_distinct(sym) count_distinct FROM v")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tcount_distinct
                            2000\t2000
                            """);
        });
    }

    @Test
    public void testTypesAndUnwrittenValuesMatchCreateTable() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            final String list = "(ts TIMESTAMP, bo BOOLEAN, by BYTE, sh SHORT, c CHAR, i INT, l LONG, f FLOAT, d DOUBLE, dt DATE,"
                    + " t TIMESTAMP, tn TIMESTAMP_NS, s STRING, vc VARCHAR, u UUID, ip IPv4, l128 LONG128, l256 LONG256, bin BINARY,"
                    + " g1 GEOHASH(1b), g2 GEOHASH(2c), g5 GEOHASH(5c), g12 GEOHASH(12c), g60 GEOHASH(60b),"
                    + " d2 DECIMAL(2,1), d4 DECIMAL(4,2), d9 DECIMAL(9,3), d18 DECIMAL(18,3), d38 DECIMAL(38,10), d76 DECIMAL(76,10),"
                    + " a1 DOUBLE[], a2 DOUBLE[][], a3 DOUBLE[][][], a32 DOUBLE" + "[]".repeat(32) + ","
                    + " sy1 SYMBOL CAPACITY 64, sy2 SYMBOL CACHE, sy3 SYMBOL NOCACHE, sy4 SYMBOL INDEX, sy5 SYMBOL INDEX CAPACITY 512,"
                    + " sy6 SYMBOL INDEX TYPE BITMAP, sy7 SYMBOL INDEX TYPE POSTING, sy8 SYMBOL INDEX TYPE POSTING DELTA,"
                    + " sy9 SYMBOL INDEX TYPE POSTING EF, sy10 SYMBOL INDEX TYPE POSTING INCLUDE (i, l))";
            execute("CREATE MATERIALIZED VIEW v " + list + " AS (SELECT ts FROM base)");
            execute("CREATE TABLE oracle " + list + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO oracle (ts) SELECT ts FROM base");
            drainWalAndMatViewQueues();
            assertSameColumnsAsCreateTable("v", "oracle");
            // a column nothing writes reads as one an INSERT omits
            assertSqlCursors("oracle", "v");
            assertQuery("SELECT by, sh, bo, c, c IS NULL c_null, c = '' c_empty, i, d, vc IS NULL vc_null, l128 IS NULL l128_null,"
                    + " d76 IS NULL d76_null, sy1 IS NULL sy1_null FROM v LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            by\tsh\tbo\tc\tc_null\tc_empty\ti\td\tvc_null\tl128_null\td76_null\tsy1_null
                            0\t0\tfalse\t\ttrue\tfalse\tnull\tnull\ttrue\ttrue\ttrue\ttrue
                            """);
            execute("REFRESH MATERIALIZED VIEW v FULL");
            drainWalAndMatViewQueues();
            assertSqlCursors("oracle", "v");
        });
    }

    @Test
    public void testValuesThroughMappedColumns() throws Exception {
        // matched columns keep their values at the edges of each type, and another listed type takes the copier's
        // implicit cast, which an explicit CAST of the same value gives
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE vb (
                        ts TIMESTAMP, i INT, l LONG, f FLOAT, d DOUBLE, vc VARCHAR, sym SYMBOL, s STRING, l128 LONG128,
                        dec DECIMAL(76,0), big LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
            execute("""
                    INSERT INTO vb VALUES
                    ('2024-01-01T00:00:00.000000Z', -2147483647, -9223372036854775807, 1.5, -0.0, '', 'a', 'x', to_long128(1, 2),
                     9999999999999999999999999999999999999999999999999999999999999999999999999999::DECIMAL(76,0), 3_000_000_000),
                    ('2024-01-01T00:01:00.000000Z', 2147483647, 9223372036854775807, -1.5, 4.9E-324, NULL, NULL, NULL, NULL,
                     -9999999999999999999999999999999999999999999999999999999999999999999999999999::DECIMAL(76,0), -1),
                    ('2024-01-01T00:02:00.000000Z', NULL, NULL, NULL, 'Infinity'::DOUBLE, '\uD83D\uDE00\u00e9', '', '', to_long128(-1, -1),
                     NULL, NULL),
                    ('2024-01-01T00:03:00.000000Z', 0, 0, 0.0, '-Infinity'::DOUBLE, 'z', 'b', 'y', to_long128(0, 0),
                     0::DECIMAL(76,0), 0),
                    ('2024-01-01T00:04:00.000000Z', 1, 1, 1.0, 'NaN'::DOUBLE, 'w', 'c', 'w', NULL, 1::DECIMAL(76,0), 1),
                    ('2024-01-01T00:05:00.000000Z', 2, 2, 2.0, 1.0005, 'q', 'd', 'q', NULL, 2::DECIMAL(76,0), 2)
                    """);
            drainWalQueue();
            execute("""
                    CREATE MATERIALIZED VIEW same (ts TIMESTAMP, i INT, l LONG, f FLOAT, d DOUBLE, vc VARCHAR, sym SYMBOL, s STRING,
                        l128 LONG128, dec DECIMAL(76,0), n INT)
                    AS (SELECT ts, i, l, f, d, vc, sym, s, l128, dec, NULL AS n FROM vb)""");
            execute("""
                    CREATE MATERIALIZED VIEW other (ts TIMESTAMP, i LONG, f DOUBLE, sym VARCHAR, vc SYMBOL, s VARCHAR)
                    AS (SELECT ts, i, f, sym, vc, s FROM vb)""");
            drainWalAndMatViewQueues();
            assertSqlCursors("SELECT ts, i, l, f, d, vc, sym, s, l128, dec, CAST(NULL AS INT) n FROM vb", "same");
            assertSqlCursors(
                    "SELECT ts, CAST(i AS LONG) i, CAST(f AS DOUBLE) f, CAST(sym AS VARCHAR) sym, CAST(vc AS SYMBOL) vc,"
                            + " CAST(s AS VARCHAR) s FROM vb",
                    "other"
            );
            // a narrowing pair behaves as on a view whose column came from ADD COLUMN
            execute("CREATE MATERIALIZED VIEW narrow (ts TIMESTAMP, big INT) AS (SELECT ts, big FROM vb)");
            execute("CREATE MATERIALIZED VIEW narrow_add AS (SELECT ts, big FROM vb)");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW narrow_add DROP COLUMN big");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW narrow_add ADD COLUMN big INT");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW narrow_add FULL");
            drainWalAndMatViewQueues();
            assertSqlCursors("narrow_add", "narrow");
        });
    }

    @Test
    public void testViewsOverListView() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW v (sym SYMBOL, price DOUBLE, extra INT, ts TIMESTAMP) AS (" + VIEW_SQL + ")");
            drainWalAndMatViewQueues();
            // a list view over a list view
            execute("CREATE MATERIALIZED VIEW v2 (ts TIMESTAMP, hi DOUBLE, extra LONG) AS (SELECT ts, max(price) AS hi, last(extra) AS extra FROM v SAMPLE BY 1d)");
            execute("CREATE VIEW pv AS (SELECT ts, sym, price FROM v WHERE sym = 'a')");
            drainWalAndMatViewQueues();
            assertQuery("v2")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\thi\textra
                            2024-01-01T00:00:00.000000Z\t4.5\tnull
                            """);
            assertQuery("pv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tprice
                            2024-01-01T00:00:00.000000Z\ta\t3.5
                            2024-01-01T01:00:00.000000Z\ta\t4.5
                            """);
            assertQuery("SELECT ts, max(price) hi FROM pv SAMPLE BY 1d")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\thi
                            2024-01-01T00:00:00.000000Z\t4.5
                            """);
            // a self-UNION and a CTE as the list view's SQL
            execute("""
                    CREATE MATERIALIZED VIEW u (ts TIMESTAMP, n LONG) WITH BASE base AS (
                        WITH a AS (SELECT ts, sym FROM base WHERE sym = 'a'), b AS (SELECT ts, sym FROM base WHERE sym = 'b')
                        SELECT ts, count() AS n FROM (a UNION ALL b) TIMESTAMP(ts) SAMPLE BY 1h
                    )""");
            drainWalAndMatViewQueues();
            assertQuery("u")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tn
                            2024-01-01T00:00:00.000000Z\t3
                            2024-01-01T01:00:00.000000Z\t1
                            """);
        });
    }

    @Test
    public void testBaseColumnTypeDriftInvalidatesAsAfterAddColumn() throws Exception {
        // a listed column whose query column changes to a type it does not take fails the next refresh, as a column
        // that view ADD COLUMN added does
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tb (ts TIMESTAMP, x VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO tb VALUES ('2024-01-01T00:10:00.000000Z', '1.2.3.4')");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW v (ts TIMESTAMP, x IPv4) AS (SELECT ts, x FROM tb)");
            execute("CREATE MATERIALIZED VIEW w AS (SELECT ts, x FROM tb)");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW w DROP COLUMN x");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW w ADD COLUMN x IPv4");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW w FULL");
            drainWalAndMatViewQueues();
            assertQuery("v")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx
                            2024-01-01T00:10:00.000000Z\t1.2.3.4
                            """);
            assertSqlCursors("w", "v");
            execute("ALTER TABLE tb ALTER COLUMN x TYPE DOUBLE");
            execute("INSERT INTO tb VALUES ('2024-01-01T00:20:00.000000Z', '2.5')");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'v'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_status\tinvalidation_reason
                            invalid\t[-1]: inconvertible types: DOUBLE -> IPv4 [from=x, to=x]
                            """);
            assertSqlCursors(
                    "SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'w'",
                    "SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'v'"
            );
        });
    }

    @Test
    public void testColumnsAddedToBaseAfterDataExists() throws Exception {
        // a listed column the query does not produce yet takes the base column that a later ADD COLUMN gives the query
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, price DOUBLE, extra INT) AS (SELECT * FROM base)");
            drainWalAndMatViewQueues();
            assertMapping("v", "extra", "amount");
            execute("ALTER TABLE base ADD COLUMN extra INT");
            execute("INSERT INTO base VALUES ('b', 5.5, 50.0, '2024-01-01T01:20:00.000000Z', 42)");
            drainWalAndMatViewQueues();
            assertQuery("SELECT * FROM v WHERE extra IS NOT NULL")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tprice\textra
                            2024-01-01T01:20:00.000000Z\tb\t5.5\t42
                            """);
            assertSqlCursors("SELECT ts, sym, price, extra FROM base", "v");
            assertMapping("v", "", "amount");
            assertViewValid("v");
            execute("REFRESH MATERIALIZED VIEW v FULL");
            drainWalAndMatViewQueues();
            assertSqlCursors("SELECT ts, sym, price, extra FROM base", "v");
        });
    }

    @Test
    public void testCoveringAndBitmapIndexReads() throws Exception {
        // an aggregating view may carry a covering POSTING index from its list; reads use the indexes after
        // incremental refreshes that rewrite the same buckets
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW vp (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE) AS (" + SAMPLE_SQL + ")");
            execute("CREATE MATERIALIZED VIEW vb (ts TIMESTAMP, sym SYMBOL INDEX, price DOUBLE) AS (" + SAMPLE_SQL + ")");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('a', 5.5, 50.0, '2024-01-01T00:50:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('a', 0.5, 5.0, '2024-01-01T00:05:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base VALUES ('b', 6.5, 60.0, '2024-01-01T01:30:00.000000Z')");
            drainWalAndMatViewQueues();
            final String byKey = """
                    ts\tprice
                    2024-01-01T00:00:00.000000Z\t5.5
                    2024-01-01T01:00:00.000000Z\t4.5
                    """;
            final String latest = """
                    ts\tsym\tprice
                    2024-01-01T01:00:00.000000Z\ta\t4.5
                    2024-01-01T01:00:00.000000Z\tb\t6.5
                    """;
            assertQuery("SELECT ts, price FROM vp WHERE sym = 'a'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex")
                    .returns(byKey);
            assertQuery("SELECT ts, price FROM vb WHERE sym = 'a'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: sym")
                    .returns(byKey);
            // LATEST ON takes a BITMAP index only, on a table too
            assertQuery("SELECT * FROM (vb LATEST ON ts PARTITION BY sym) ORDER BY sym")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("LatestByAllIndexed")
                    .returns(latest);
            assertQuery("SELECT * FROM (vp LATEST ON ts PARTITION BY sym) ORDER BY sym")
                    .noLeakCheck()
                    .expectSize()
                    .returns(latest);
            for (String view : new String[]{"vp", "vb"}) {
                assertSqlCursors(
                        "SELECT ts, price FROM (" + SAMPLE_SQL + ") WHERE sym = 'a'",
                        "SELECT ts, price FROM " + view + " WHERE sym = 'a'"
                );
            }
        });
    }

    @Test
    public void testDeferredViewWhoseBaseLosesQueryColumn() throws Exception {
        // the mapping stays unknown while the SQL does not compile, and ALTER ... AS that fixes the SQL reports it
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL, price DOUBLE) REFRESH MANUAL DEFERRED AS (" + VIEW_SQL + ")");
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base DROP COLUMN amount");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW v FULL");
            drainWalAndMatViewQueues();
            assertMapping("v", null, null);
            assertQuery("SELECT view_status FROM materialized_views() WHERE view_name = 'v'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\ninvalid\n");
            execute("ALTER MATERIALIZED VIEW v AS (" + SAMPLE_SQL + ")");
            assertMapping("v", "", "");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW v FULL");
            drainWalAndMatViewQueues();
            assertViewValid("v");
            assertSqlCursors("SELECT ts, sym, price FROM (" + SAMPLE_SQL + ") ORDER BY ts, sym", "SELECT * FROM v ORDER BY ts, sym");
        });
    }

    @Test
    public void testIdentityCopierForListOfQueryColumns() throws Exception {
        // a list equal to the query's columns copies without a projection, as a view without a list does
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW same (ts TIMESTAMP, sym SYMBOL, price DOUBLE) AS (" + SAMPLE_SQL + ")");
            execute("CREATE MATERIALIZED VIEW reordered (sym SYMBOL, ts TIMESTAMP, price DOUBLE) AS (" + SAMPLE_SQL + ")");
            drainWalAndMatViewQueues();
            Assert.assertNull(getViewState("same").getRecordProjectionForTesting());
            Assert.assertNotNull(getViewState("reordered").getRecordProjectionForTesting());
            assertSqlCursors("SELECT ts, sym, price FROM (" + SAMPLE_SQL + ") ORDER BY ts, sym", "SELECT * FROM same ORDER BY ts, sym");
            assertSqlCursors("SELECT sym, ts, price FROM (" + SAMPLE_SQL + ") ORDER BY ts, sym", "SELECT * FROM reordered ORDER BY ts, sym");
        });
    }

    @Test
    public void testIfNotExistsLosesToConcurrentCreate() throws Exception {
        // another CREATE of the same name wins while this one compiles: this one does nothing, and the view keeps the
        // winner's columns
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            final String winner = "CREATE MATERIALIZED VIEW v (sym SYMBOL, ts TIMESTAMP) AS (" + SAMPLE_SQL + ")";
            final String loser = "CREATE MATERIALIZED VIEW IF NOT EXISTS v (ts TIMESTAMP, price DOUBLE, extra INT) AS (" + SAMPLE_SQL + ")";
            final int borrows = createMatViewOnBorrow(loser, winner);
            Assert.assertTrue(borrows > 0);
            drainWalAndMatViewQueues();
            assertColumns("v", """
                    column\ttype\tdesignated
                    sym\tSYMBOL\tfalse
                    ts\tTIMESTAMP\ttrue
                    """);
            assertMapping("v", "", "price");
            assertSqlCursors("SELECT sym, ts FROM (" + SAMPLE_SQL + ") ORDER BY ts, sym", "SELECT * FROM v ORDER BY ts, sym");
        });
    }

    @Test
    public void testRefreshAndStorageClausesUnchanged() throws Exception {
        // the list changes the view's columns only: with each REFRESH form and storage clause the view's definition
        // is the one the same CREATE without the list gives
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            final String[][] forms = {
                    // between the list and AS, after the query
                    {"", ""},
                    {" WITH BASE base", ""},
                    {" REFRESH IMMEDIATE", " PARTITION BY HOUR TTL 2 DAYS"},
                    {" REFRESH MANUAL", " PARTITION BY MONTH"},
                    {" REFRESH IMMEDIATE DEFERRED", " PARTITION BY YEAR"},
                    {" REFRESH MANUAL DEFERRED", ""},
                    {" REFRESH EVERY 1h DEFERRED START '2024-01-01T00:00:00.000000Z' TIME ZONE 'Europe/London'", ""},
                    {" REFRESH MANUAL PERIOD (LENGTH 1h TIME ZONE 'Europe/London' DELAY 10m)", ""},
                    {" REFRESH IMMEDIATE PERIOD (SAMPLE BY INTERVAL)", ""},
            };
            final String[] queries = {
                    SAMPLE_SQL,
                    "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h ALIGN TO CALENDAR TIME ZONE 'Europe/London' WITH OFFSET '00:15'",
            };
            int n = 0;
            for (String[] form : forms) {
                for (String query : queries) {
                    if (form[0].contains("PERIOD") && query.contains("OFFSET")) {
                        continue;
                    }
                    final String list = "lv" + n;
                    final String noList = "nv" + n;
                    n++;
                    execute("CREATE MATERIALIZED VIEW " + list + " (ts TIMESTAMP, sym SYMBOL, price DOUBLE)" + form[0] + " AS (" + query + ")" + form[1]);
                    execute("CREATE MATERIALIZED VIEW " + noList + form[0] + " AS (" + query + ")" + form[1]);
                    drainWalAndMatViewQueues();
                    sink.clear();
                    printSql("SHOW CREATE MATERIALIZED VIEW " + noList, sink);
                    final String expected = sink.toString().replace(noList, list);
                    sink.clear();
                    printSql("SHOW CREATE MATERIALIZED VIEW " + list, sink);
                    TestUtils.assertEquals(form[0] + form[1], expected, sink);
                    assertSqlCursors("SELECT ts, sym, price FROM " + noList, "SELECT ts, sym, price FROM " + list);
                }
            }
            // a non-WAL base is refused with a list too
            execute("CREATE TABLE nw (ts TIMESTAMP, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            final String sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, price DOUBLE) AS (SELECT ts, last(price) AS price FROM nw SAMPLE BY 1h)";
            assertCreateRefused(sql, sql.indexOf("nw SAMPLE"), "base table has to be WAL enabled");
        });
    }

    @Test
    public void testRetriedCompileKeepsTheList() throws Exception {
        // a row-expiry policy change while the CREATE compiles makes it compile the query again; the second pass
        // starts from the same list
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            final int once = createMatViewOnBorrow("CREATE MATERIALIZED VIEW v1 (sym SYMBOL, extra INT, ts TIMESTAMP) AS (" + VIEW_SQL + ")", null);
            final int retried = createMatViewOnBorrow("CREATE MATERIALIZED VIEW v2 (sym SYMBOL, extra INT, ts TIMESTAMP) AS (" + VIEW_SQL + ")", "");
            Assert.assertTrue("once: " + once + ", retried: " + retried, retried > once);
            drainWalAndMatViewQueues();
            for (String view : new String[]{"v1", "v2"}) {
                assertColumns(view, """
                        column\ttype\tdesignated
                        sym\tSYMBOL\tfalse
                        extra\tINT\tfalse
                        ts\tTIMESTAMP\ttrue
                        """);
                assertSqlCursors("SELECT sym, CAST(NULL AS INT) extra, ts FROM (" + VIEW_SQL + ") ORDER BY ts, sym", "SELECT * FROM " + view + " ORDER BY ts, sym");
            }
        });
    }

    @Test
    public void testValidationOnlyAndExplain() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String sql = "CREATE MATERIALIZED VIEW v (sym SYMBOL, extra INT, ts TIMESTAMP) AS (" + SAMPLE_SQL + ")";
            final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            context.setValidationOnly(true);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compile(sql, context);
                final String bad = "CREATE MATERIALIZED VIEW v (sym SYMBOL, extra FOO, ts TIMESTAMP) AS (" + SAMPLE_SQL + ")";
                try {
                    compiler.compile(bad, context);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals(bad.indexOf("FOO"), e.getPosition());
                    TestUtils.assertEquals("unsupported column type: FOO", e.getFlyweightMessage());
                }
            } finally {
                context.setValidationOnly(false);
            }
            Assert.assertNull(engine.getTableTokenIfExists("v"));
            // EXPLAIN explains the query, which the list does not change
            assertSqlCursors("EXPLAIN CREATE MATERIALIZED VIEW v AS (" + SAMPLE_SQL + ")", "EXPLAIN " + sql);
            Assert.assertNull(engine.getTableTokenIfExists("v"));
        });
    }

    @Test
    public void testWideListWithExpireRows() throws Exception {
        // a list of 1,000 columns, most of them list-only, probes the policy over a source of as many columns
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            drainWalQueue();
            final StringBuilder list = new StringBuilder("(ts TIMESTAMP, sym SYMBOL, price DOUBLE, k SYMBOL");
            for (int i = 0; i < 996; i++) {
                list.append(", c").append(i).append(i % 2 == 0 ? " INT" : " VARCHAR");
            }
            list.append(')');
            assertCreateAndAlterAgree(list.toString(), VIEW_SQL, "", "KEEP LATEST PARTITION BY k", null, 1);
            assertCreateAndAlterAgree(list.toString(), VIEW_SQL, "", "WHEN price < max(price) OVER (PARTITION BY sym)", null, 2);
        });
    }

    private int createMatViewOnBorrow(String ddl, @Nullable String onFirstBorrow) throws Exception {
        // onFirstBorrow: null - nothing; empty - publish a row-expiry policy change; otherwise - run it
        final AtomicInteger readerBorrows = new AtomicInteger();
        try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1) {
            @Override
            public TableReader getReader(TableToken tableToken, long version) {
                onBorrow(tableToken);
                return super.getReader(tableToken, version);
            }

            @Override
            public TableReader getReader(TableToken tableToken) {
                onBorrow(tableToken);
                return super.getReader(tableToken);
            }

            private void onBorrow(TableToken tableToken) {
                // REJECT mode is on only while the CREATE compiles the view's query
                if (getExpiryReadPolicy() == ExpiryReadPolicy.REJECT
                        && !tableToken.isMatView()
                        && readerBorrows.getAndIncrement() == 0
                        && onFirstBorrow != null) {
                    if (onFirstBorrow.isEmpty()) {
                        engine.getMetadataCache().publishExpiryPolicyUpdate();
                    } else {
                        try {
                            engine.execute(onFirstBorrow);
                        } catch (SqlException e) {
                            throw new AssertionError(e);
                        }
                    }
                }
            }
        }.with(AllowAllSecurityContext.INSTANCE)) {
            execute(ddl, context);
        }
        return readerBorrows.get();
    }

    private static MatViewState getViewState(String viewName) {
        final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName(viewName));
        Assert.assertNotNull(state);
        return state;
    }

    private void assertNamingCheckRefusesLongReference() throws Exception {
        assertMemoryLeak(() -> {
            createNamingBase();
            final String sql = "CREATE MATERIALIZED VIEW v (ts TIMESTAMP, v DOUBLE) AS (SELECT ts, " + LONG_NAME + " FROM nbase)";
            assertCreateRefused(sql, sql.indexOf(LONG_NAME), "column '" + LONG_NAME + "' requires an explicit alias. Use: " + LONG_NAME + NAMING_HINT_SUFFIX);
            // the oracle: without a list the same SQL copies by position
            execute("CREATE MATERIALIZED VIEW o AS (SELECT ts, " + LONG_NAME + " FROM nbase)");
            drainWalAndMatViewQueues();
            final String add = "ALTER MATERIALIZED VIEW o ADD COLUMN extra INT";
            assertExceptionNoLeakCheck(add, add.indexOf("o ADD"), "materialized view SQL relies on generated column names");
            execute("CREATE MATERIALIZED VIEW v (ts TIMESTAMP, v DOUBLE) AS (SELECT ts, " + LONG_NAME + " AS v FROM nbase)");
            drainWalAndMatViewQueues();
            assertSqlCursors("SELECT ts, " + LONG_NAME + " AS v FROM nbase", "v");
        });
    }

    private void assertColumns(String viewName, String expected) throws Exception {
        assertQuery("SELECT \"column\", type, designated FROM table_columns('" + viewName + "')")
                .noLeakCheck()
                .noRandomAccess()
                .returns(expected);
    }

    // Creates the list view with the policy, and the same list view without it followed by ALTER ... SET EXPIRE: both
    // reach the same verdict and, for a refusal, the same message. An accepted policy leaves a readable view with
    // expectedCount rows (-1: any count, the same on both); expectedError null means accepted.
    private void assertCreateAndAlterAgree(
            String list,
            String query,
            String suffix,
            String clause,
            @Nullable String expectedError,
            long expectedCount
    ) throws Exception {
        execute("CREATE MATERIALIZED VIEW va " + list + " AS (" + query + ")" + suffix);

        drainWalAndMatViewQueues();
        String alterError = null;
        try {
            execute("ALTER MATERIALIZED VIEW va SET EXPIRE ROWS " + clause);
        } catch (SqlException e) {
            alterError = e.getFlyweightMessage().toString();
        }
        final String create = "CREATE MATERIALIZED VIEW vc " + list + " AS (" + query + ")" + suffix + " EXPIRE ROWS " + clause;
        String createError = null;
        try {
            execute(create);
        } catch (SqlException e) {
            createError = e.getFlyweightMessage().toString();
            Assert.assertEquals(create, create.indexOf("vc"), e.getPosition());
        }
        drainWalAndMatViewQueues();
        Assert.assertEquals("ALTER: " + clause, expectedError, alterError);
        Assert.assertEquals("CREATE: " + clause, expectedError, createError);
        if (expectedError == null) {
            final String alterCount = count("va");
            final String createCount = count("vc");
            Assert.assertEquals(clause, alterCount, createCount);
            if (expectedCount > -1) {
                Assert.assertEquals(clause, "n\n" + expectedCount + "\n", createCount);
            }
            printSql("SELECT * FROM vc");
            execute("DROP MATERIALIZED VIEW vc");
        } else {
            Assert.assertNull(engine.getTableTokenIfExists("vc"));
        }
        execute("DROP MATERIALIZED VIEW va");
        drainWalAndMatViewQueues();
    }

    private static void assertCreateRefused(String sql, int position, String message) throws Exception {
        try {
            execute(sql);
            Assert.fail("CREATE must fail: " + sql);
        } catch (Throwable e) {
            if (!(e instanceof FlyweightMessageContainer container)) {
                throw e;
            }
            TestUtils.assertEquals(sql, message, container.getFlyweightMessage());
            Assert.assertEquals(sql, position, container.getPosition());
        }
        Assert.assertNull(engine.getTableTokenIfExists("v"));
    }

    private void assertMapping(String viewName, String unmapped, String ignored) throws Exception {
        assertMapping(engine, sqlExecutionContext, viewName, unmapped, ignored);
    }

    // null stands for NULL
    private void assertMapping(CairoEngine engine, SqlExecutionContext context, String viewName, String unmapped, String ignored) throws Exception {
        assertQuery("SELECT unmapped_view_columns AS unmapped_view_columns, ignored_query_columns AS ignored_query_columns, "
                + "unmapped_view_columns IS NULL unmapped_is_null, ignored_query_columns IS NULL ignored_is_null "
                + "FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .withEngine(engine)
                .withContext(context)
                .noLeakCheck()
                .noRandomAccess()
                .returns("unmapped_view_columns\tignored_query_columns\tunmapped_is_null\tignored_is_null\n"
                        + (unmapped != null ? unmapped : "") + '\t'
                        + (ignored != null ? ignored : "") + '\t'
                        + (unmapped == null) + '\t'
                        + (ignored == null) + '\n');
    }

    // The refused SQL names the column at the given occurrence of the item in the query; the hint repeats the item
    // as written. The SQL with that occurrence aliased as the hint tells creates the view, which fills v by name.
    private void assertNamingRefusedThenFixed(
            String list,
            String withBase,
            String query,
            String item,
            String columnName,
            int occurrence
    ) throws Exception {
        final String prefix = "CREATE MATERIALIZED VIEW v " + list + withBase + " AS (";
        int lo = -1;
        for (int i = 0; i < occurrence; i++) {
            lo = query.indexOf(item, lo + 1);
        }
        Assert.assertTrue(lo > -1);
        final String sql = prefix + query + ")";
        assertCreateRefused(sql, prefix.length() + lo, "column '" + columnName + "' requires an explicit alias. Use: " + item + NAMING_HINT_SUFFIX);
        final String fixed = query.substring(0, lo) + item + " AS v" + query.substring(lo + item.length());
        execute(prefix + fixed + ")");
        drainWalAndMatViewQueues();
        assertSqlCursors("SELECT ts, v FROM (" + fixed + ")", "v");
        execute("DROP MATERIALIZED VIEW v");
        drainWalAndMatViewQueues();
    }

    // CREATE TABLE with the same list and tail refuses the list with the same message, at the same offset from it.
    private static void assertSameErrorAsCreateTable(String list, String tableTail, String viewTail) throws Exception {
        final String tablePrefix = "CREATE TABLE oracle ";
        final String viewPrefix = "CREATE MATERIALIZED VIEW v ";
        String tableMessage = null;
        int tablePosition = -1;
        try {
            execute(tablePrefix + list + tableTail);
        } catch (SqlException e) {
            tableMessage = e.getFlyweightMessage().toString();
            tablePosition = e.getPosition();
        }
        Assert.assertNotNull("CREATE TABLE accepted " + list, tableMessage);
        assertCreateRefused(viewPrefix + list + viewTail, tablePosition - tablePrefix.length() + viewPrefix.length(), tableMessage);
    }

    private static void assertSameColumnsAsCreateTable(String view, String table) throws SqlException {
        final String columns = "\"column\", type, indexed, indexBlockCapacity, symbolCached, symbolCapacity, designated, upsertKey, indexType, indexInclude";
        assertSqlCursors("SELECT " + columns + " FROM table_columns('" + table + "')", "SELECT " + columns + " FROM table_columns('" + view + "')");
    }

    private void assertViewValid(String viewName) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\nvalid\t\n");
    }

    private static String count(String view) throws SqlException {
        sink.clear();
        printSql("SELECT count() n FROM " + view, sink);
        return sink.toString();
    }

    private static void createBase() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private static void createNamingBase() throws SqlException {
        execute("CREATE TABLE nbase (ts TIMESTAMP, k SYMBOL, price DOUBLE, " + LONG_NAME + " DOUBLE, \"" + LONG_SPACED + "\" DOUBLE, \"select\" DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO nbase VALUES ('2024-01-01T00:10:00.000000Z', 'a', 1.5, 2.5, 3.5, 4.5), ('2024-01-01T01:10:00.000000Z', 'a', 5.5, 6.5, 7.5, 8.5)");
        drainWalQueue();
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
