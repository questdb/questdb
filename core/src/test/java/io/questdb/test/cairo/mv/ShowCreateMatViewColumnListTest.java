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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * SHOW CREATE MATERIALIZED VIEW, and SHOW CREATE DATABASE, which prints the same, print the column list of CREATE
 * MATERIALIZED VIEW v (col TYPE, ...) AS (...) TIMESTAMP(ts) for a view whose columns differ from those a CREATE
 * without a list builds from the printed SQL: as many, with the same names in the same letter case, of the same types,
 * in the same order, with every SYMBOL column NOCACHE. The list states each column's name, quoted where SQL needs it,
 * type, SYMBOL cache flag and index, so that it takes no default of the server that runs it.
 * <p>
 * Oracle: the round trip. The view's DDL is written by hand from that layout. Running it after the view is dropped
 * creates a view with the same columns, definition and rows after a FULL refresh, for which SHOW CREATE prints the same
 * DDL again.
 */
public class ShowCreateMatViewColumnListTest extends AbstractCairoTest {
    private static final String VIEW_COLUMNS = "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE,\n\tprice DOUBLE,\n\tamount DOUBLE";
    private static final String VIEW_SQL = "SELECT ts, sym, last(price) AS price, last(amount) AS amount FROM base SAMPLE BY 1h";

    @Test
    public void testAlterAsKeepsColumnsTheSqlNoLongerBuilds() throws Exception {
        // ALTER ... AS keeps the view's columns and maps the new SQL's columns by name, ignoring letter case
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createView("reordered");
            createView("renamed");
            createView("narrowed");
            drainWalAndMatViewQueues();
            final String reorderedSql = "SELECT ts, sym, last(amount) AS amount, last(price) AS price FROM base SAMPLE BY 1h";
            final String renamedSql = "SELECT ts, sym AS SYM, last(price) AS Price, last(amount) AS amount FROM base SAMPLE BY 1h";
            final String narrowedSql = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h";
            execute("ALTER MATERIALIZED VIEW reordered AS (" + reorderedSql + ")");
            execute("ALTER MATERIALIZED VIEW renamed AS (" + renamedSql + ")");
            execute("ALTER MATERIALIZED VIEW narrowed AS (" + narrowedSql + ")");
            drainWalAndMatViewQueues();
            assertShowCreateReplays("reordered", listDdl("reordered", VIEW_COLUMNS, reorderedSql));
            assertShowCreateReplays("renamed", listDdl("renamed", VIEW_COLUMNS, renamedSql));
            assertShowCreateReplays("narrowed", listDdl("narrowed", VIEW_COLUMNS, narrowedSql));
        });
    }

    @Test
    public void testBaseColumnTypeDrift() throws Exception {
        // the views keep their column types when the base's change, and their SQL builds the new ones
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.5, 10, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.5, 20, '2024-01-01T00:20:00.000000Z'),
                    ('a', 4.5, 40, '2024-01-01T01:10:00.000000Z')
                    """);
            final String sql = "SELECT ts, sym, last(price) AS price, last(qty) AS qty FROM base SAMPLE BY 1h";
            execute("CREATE MATERIALIZED VIEW v AS (" + sql + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base ALTER COLUMN price TYPE FLOAT");
            execute("ALTER TABLE base ALTER COLUMN qty TYPE LONG");
            drainWalAndMatViewQueues();
            assertShowCreateReplays("v", listDdl("v", "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE,\n\tprice DOUBLE,\n\tqty INT", sql));
            assertQuery("v ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tprice\tqty
                    2024-01-01T00:00:00.000000Z\ta\t1.5\t10
                    2024-01-01T00:00:00.000000Z\tb\t2.5\t20
                    2024-01-01T01:00:00.000000Z\ta\t4.5\t40
                    """);
        });
    }

    @Test
    public void testColumnDdlPrintsList() throws Exception {
        // view ADD and DROP COLUMN make the view's columns differ from its SQL's; a view without either prints no list
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createView("added");
            createView("dropped");
            createView("both");
            createView("unchanged");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW added ADD COLUMN extra INT");
            execute("ALTER MATERIALIZED VIEW dropped DROP COLUMN price");
            execute("ALTER MATERIALIZED VIEW both ADD COLUMN extra INT");
            execute("ALTER MATERIALIZED VIEW both DROP COLUMN price");
            drainWalAndMatViewQueues();
            assertShowCreateReplays("added", listDdl("added", VIEW_COLUMNS + ",\n\textra INT", VIEW_SQL));
            assertShowCreateReplays("dropped", listDdl("dropped", "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE,\n\tamount DOUBLE", VIEW_SQL));
            assertShowCreateReplays("both", listDdl("both", "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE,\n\tamount DOUBLE,\n\textra INT", VIEW_SQL));
            assertShowCreateReplays("unchanged", "CREATE MATERIALIZED VIEW 'unchanged' WITH BASE 'base' REFRESH IMMEDIATE AS (\n" + VIEW_SQL + "\n) PARTITION BY DAY;");
            // every view column only the list has, and a view whose list holds nothing but the timestamp of the SQL
            execute("CREATE MATERIALIZED VIEW extras (ts TIMESTAMP, e1 INT, e2 VARCHAR) AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertShowCreateReplays("extras", listDdl("extras", "\tts TIMESTAMP,\n\te1 INT,\n\te2 VARCHAR", VIEW_SQL), "ts");
        });
    }

    @Test
    public void testColumnListOnEmptyBase() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            createView("v");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW v ADD COLUMN extra INT");
            drainWalAndMatViewQueues();
            assertShowCreateReplays("v", listDdl("v", VIEW_COLUMNS + ",\n\textra INT", VIEW_SQL));
            assertQuery("SELECT count() FROM v").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testDesignatedTimestampNotFirst() throws Exception {
        // TIMESTAMP(...) names the view's designated timestamp wherever the list has it, and a name SQL needs quoted
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            final String middleSql = "SELECT sym, ts, last(price) AS price FROM base SAMPLE BY 1h";
            final String lastSql = "SELECT sym, last(price) AS price, ts FROM base SAMPLE BY 1h";
            final String quotedSql = "SELECT ts AS \"bucket ts\", sym, last(price) AS price FROM base SAMPLE BY 1h";
            execute("CREATE MATERIALIZED VIEW middle AS (" + middleSql + ") PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW last AS (" + lastSql + ") PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW quoted AS (" + quotedSql + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW middle ADD COLUMN extra INT");
            execute("ALTER MATERIALIZED VIEW last ADD COLUMN extra INT");
            execute("ALTER MATERIALIZED VIEW quoted ADD COLUMN extra INT");
            drainWalAndMatViewQueues();
            assertShowCreateReplays(
                    "middle",
                    listDdl("middle", "\tsym SYMBOL NOCACHE,\n\tts TIMESTAMP,\n\tprice DOUBLE,\n\textra INT", middleSql)
            );
            assertShowCreateReplays(
                    "last",
                    listDdl("last", "\tsym SYMBOL NOCACHE,\n\tprice DOUBLE,\n\tts TIMESTAMP,\n\textra INT", lastSql)
            );
            assertShowCreateReplays(
                    "quoted",
                    "CREATE MATERIALIZED VIEW 'quoted' (\n\t\"bucket ts\" TIMESTAMP,\n\tsym SYMBOL NOCACHE,\n\tprice DOUBLE,\n\textra INT\n"
                            + ") WITH BASE 'base' REFRESH IMMEDIATE AS (\n" + quotedSql + "\n) TIMESTAMP(\"bucket ts\") PARTITION BY DAY;",
                    "\"bucket ts\", sym"
            );
        });
    }

    @Test
    public void testIndexes() throws Exception {
        // the list states each index with its type, and a BITMAP index with its capacity, which CAPACITY alone means
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            execute("CREATE MATERIALIZED VIEW indexed AS (" + VIEW_SQL + "), INDEX (sym CAPACITY 128) PARTITION BY DAY");
            createView("posting");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW indexed ADD COLUMN b SYMBOL INDEX CAPACITY 300");
            execute("ALTER MATERIALIZED VIEW indexed ADD COLUMN p SYMBOL NOCACHE INDEX TYPE POSTING");
            execute("ALTER MATERIALIZED VIEW indexed ADD COLUMN pd SYMBOL INDEX TYPE POSTING DELTA");
            execute("ALTER MATERIALIZED VIEW posting ALTER COLUMN sym ADD INDEX TYPE POSTING EF");
            execute("ALTER MATERIALIZED VIEW posting ADD COLUMN extra INT");
            drainWalAndMatViewQueues();
            final String indexedDdl = listDdl(
                    "indexed",
                    "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE INDEX CAPACITY 128,\n\tprice DOUBLE,\n\tamount DOUBLE,\n"
                            + "\tb SYMBOL CACHE INDEX CAPACITY 512,\n\tp SYMBOL NOCACHE INDEX TYPE POSTING,\n"
                            + "\tpd SYMBOL CACHE INDEX TYPE POSTING DELTA",
                    VIEW_SQL
            );
            assertShowCreateReplays("indexed", indexedDdl);
            assertShowCreateReplays(
                    "posting",
                    listDdl("posting", "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE INDEX TYPE POSTING EF,\n\tprice DOUBLE,\n\tamount DOUBLE,\n\textra INT", VIEW_SQL)
            );

            // a server whose default index type and block size differ builds the same indexes from the list
            setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, "POSTING");
            setProperty(PropertyKey.CAIRO_INDEX_VALUE_BLOCK_SIZE, 1024);
            assertShowCreateReplays("indexed", indexedDdl);
        });
    }

    @Test
    public void testIndexCapacityAboveListMaximum() throws Exception {
        // ALTER stores BITMAP capacities above the largest the list takes, which comes instead
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createView("v");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW v ADD COLUMN big SYMBOL INDEX CAPACITY 10000000");
            execute("ALTER MATERIALIZED VIEW v ALTER COLUMN sym ADD INDEX CAPACITY 8388608");
            drainWalAndMatViewQueues();
            final String capacities = "SELECT \"column\", indexBlockCapacity FROM table_columns('v') WHERE indexed";
            assertQuery(capacities).noLeakCheck().noRandomAccess().returns("column\tindexBlockCapacity\nsym\t8388608\nbig\t16777216\n");
            final String ddl = listDdl(
                    "v",
                    "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE INDEX CAPACITY 8388608,\n\tprice DOUBLE,\n\tamount DOUBLE,\n"
                            + "\tbig SYMBOL CACHE INDEX CAPACITY 8388608",
                    VIEW_SQL
            );
            assertQuery("SHOW CREATE MATERIALIZED VIEW v").noLeakCheck().noRandomAccess().returns("ddl\n" + ddl + "\n");
            execute("DROP MATERIALIZED VIEW v");
            drainWalAndMatViewQueues();
            execute(ddl);
            drainWalAndMatViewQueues();
            assertQuery(capacities).noLeakCheck().noRandomAccess().returns("column\tindexBlockCapacity\nsym\t8388608\nbig\t8388608\n");
            assertQuery("SHOW CREATE MATERIALIZED VIEW v").noLeakCheck().noRandomAccess().returns("ddl\n" + ddl + "\n");
        });
    }

    @Test
    public void testIncludedColumns() throws Exception {
        // INCLUDE lists the covered columns, the designated timestamp that the index adds included, quoted where SQL
        // needs it, and skips a covered column that DROP COLUMN removed
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            final String sql = "SELECT ts, sym, last(price) AS \"select\", last(amount) AS amount FROM base SAMPLE BY 1h";
            // extra, which the SQL lacks, brings the list
            execute("CREATE MATERIALIZED VIEW v (ts TIMESTAMP, sym SYMBOL NOCACHE INDEX TYPE POSTING INCLUDE (\"select\", amount),"
                    + " \"select\" DOUBLE, amount DOUBLE, extra INT) AS (" + sql + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertShowCreateReplays(
                    "v",
                    listDdl("v", "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE INDEX TYPE POSTING INCLUDE (\"select\", amount, ts),\n"
                            + "\t\"select\" DOUBLE,\n\tamount DOUBLE,\n\textra INT", sql)
            );
            execute("ALTER MATERIALIZED VIEW v DROP COLUMN amount");
            drainWalAndMatViewQueues();
            assertShowCreateReplays(
                    "v",
                    listDdl("v", "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE INDEX TYPE POSTING INCLUDE (\"select\", ts),\n\t\"select\" DOUBLE,\n\textra INT", sql)
            );

            // without the timestamp, an index whose only covered column is dropped covers none
            setProperty(PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, "false");
            execute("CREATE MATERIALIZED VIEW w (ts TIMESTAMP, sym SYMBOL NOCACHE INDEX TYPE POSTING INCLUDE (amount), amount DOUBLE, extra INT)"
                    + " AS (" + sql + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertQuery("SELECT indexInclude FROM table_columns('w') WHERE \"column\" = 'sym'").noLeakCheck().noRandomAccess()
                    .returns("indexInclude\namount\n");
            execute("ALTER MATERIALIZED VIEW w DROP COLUMN amount");
            drainWalAndMatViewQueues();
            assertShowCreateReplays("w", listDdl("w", "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE INDEX TYPE POSTING,\n\textra INT", sql));
        });
    }

    @Test
    public void testListPrintedWhenSqlNoLongerCompiles() throws Exception {
        // Without a compile SHOW CREATE cannot tell whether the SQL builds the view's columns, so it prints them. The
        // DDL fails where the SQL does not compile, and creates the same view where it does.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createView("v");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW v ADD COLUMN extra INT");
            execute("ALTER TABLE base DROP COLUMN amount");
            drainWalAndMatViewQueues();
            final String ddl = listDdl("v", VIEW_COLUMNS + ",\n\textra INT", VIEW_SQL);
            assertQuery("SHOW CREATE MATERIALIZED VIEW v").noLeakCheck().noRandomAccess().returns("ddl\n" + ddl + "\n");
            execute("DROP MATERIALIZED VIEW v");
            drainWalAndMatViewQueues();
            assertException(ddl, ddl.indexOf("amount) AS amount"), "Invalid column: amount");
            Assert.assertNull(engine.getTableTokenIfExists("v"));

            execute("ALTER TABLE base ADD COLUMN amount DOUBLE");
            drainWalAndMatViewQueues();
            execute(ddl);
            drainWalAndMatViewQueues();
            assertQuery("SELECT \"column\", type FROM table_columns('v')").noLeakCheck().noRandomAccess().returns("""
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    price\tDOUBLE
                    amount\tDOUBLE
                    extra\tINT
                    """);
            assertQuery("SHOW CREATE MATERIALIZED VIEW v").noLeakCheck().noRandomAccess().returns("ddl\n" + ddl + "\n");
        });
    }

    @Test
    public void testNames() throws Exception {
        // a name is written as it is when it is a plain identifier and not a keyword, and double-quoted otherwise;
        // words of the list's grammar that are no keywords need no quotes
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createView("v");
            drainWalAndMatViewQueues();
            final String[] added = {
                    "\"select\"", "\"from\"", "\"order\"", "\"my col\"", "prix\u00e9", "\"\uD834\uDD1Enote\"", "\"9lives\"", "MixedCase",
                    "index", "capacity", "nocache", "cache", "include", "timestamp", "type", "key", "by"
            };
            final String[] printed = {
                    "\"select\"", "\"from\"", "\"order\"", "\"my col\"", "\"prix\u00e9\"", "\"\uD834\uDD1Enote\"", "\"9lives\"", "MixedCase",
                    "index", "capacity", "nocache", "cache", "include", "timestamp", "type", "key", "by"
            };
            final StringBuilder columns = new StringBuilder(VIEW_COLUMNS);
            for (int i = 0; i < added.length; i++) {
                execute("ALTER MATERIALIZED VIEW v ADD COLUMN " + added[i] + " INT");
                columns.append(",\n\t").append(printed[i]).append(" INT");
            }
            drainWalAndMatViewQueues();
            assertShowCreateReplays("v", listDdl("v", columns.toString(), VIEW_SQL));
        });
    }

    @Test
    public void testOlderViewsThatNameTheirColumnsWithBareCalls() throws Exception {
        // The SQL of a view created before CREATE required explicit names may name its columns and call a
        // zero-argument function bare. Its refresh maps it by name, so each call keeps its own name as its alias,
        // whatever view column sits at its position.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            final String countSql = "SELECT ts, sym, count FROM base SAMPLE BY 1h";
            final String countSqlNamed = "SELECT ts, sym, count() AS count FROM base SAMPLE BY 1h";
            for (String name : new String[]{"n_add", "n_drop", "n_swap", "n_same"}) {
                executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW " + name + " AS (" + countSql + ") PARTITION BY DAY");
            }
            executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW n_alias AS (SELECT ts, sym, count AS c FROM base SAMPLE BY 1h) PARTITION BY DAY");
            executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW n_pi AS (SELECT ts, sym, pi, avg(price) AS avg_price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW n_add ADD COLUMN extra INT");
            execute("ALTER MATERIALIZED VIEW n_drop DROP COLUMN count");
            // as many columns as the SQL has, so position alone would name count() after extra
            execute("ALTER MATERIALIZED VIEW n_swap ADD COLUMN extra LONG");
            execute("ALTER MATERIALIZED VIEW n_swap DROP COLUMN count");
            execute("ALTER MATERIALIZED VIEW n_alias ADD COLUMN extra INT");
            execute("ALTER MATERIALIZED VIEW n_pi DROP COLUMN pi");
            drainWalAndMatViewQueues();

            assertShowCreateReplays("n_add", listDdl("n_add", "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE,\n\tcount LONG,\n\textra INT", countSqlNamed));
            assertShowCreateReplays("n_drop", listDdl("n_drop", "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE", countSqlNamed));
            assertShowCreateReplays("n_swap", listDdl("n_swap", "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE,\n\textra LONG", countSqlNamed));
            assertQuery("SELECT count(), count(extra) FROM n_swap").noLeakCheck().noRandomAccess().expectSize().returns("count\tcount1\n3\t0\n");
            assertShowCreateReplays(
                    "n_alias",
                    listDdl("n_alias", "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE,\n\tc LONG,\n\textra INT", "SELECT ts, sym, count() AS c FROM base SAMPLE BY 1h")
            );
            assertShowCreateReplays(
                    "n_pi",
                    listDdl("n_pi", "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE,\n\tavg_price DOUBLE", "SELECT ts, sym, pi() AS pi, avg(price) AS avg_price FROM base SAMPLE BY 1h")
            );
            assertShowCreateReplays(
                    "n_same",
                    "CREATE MATERIALIZED VIEW 'n_same' WITH BASE 'base' REFRESH IMMEDIATE AS (\n" + countSqlNamed + "\n) PARTITION BY DAY;"
            );
        });
    }

    @Test
    public void testReusedFactoryAndCursor() throws Exception {
        // a compiled SHOW CREATE reads the view's columns and SQL afresh on each execution, and on each rewind
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createView("v");
            drainWalAndMatViewQueues();
            final String noList = "CREATE MATERIALIZED VIEW 'v' WITH BASE 'base' REFRESH IMMEDIATE AS (\n" + VIEW_SQL + "\n) PARTITION BY DAY;";
            final String withList = listDdl("v", VIEW_COLUMNS + ",\n\textra INT", VIEW_SQL);
            try (RecordCursorFactory factory = select("SHOW CREATE MATERIALIZED VIEW v")) {
                assertDdl(factory, noList);
                execute("ALTER MATERIALIZED VIEW v ADD COLUMN extra INT");
                drainWalAndMatViewQueues();
                assertDdl(factory, withList);
                // the SQL no longer compiles
                execute("ALTER TABLE base DROP COLUMN amount");
                drainWalAndMatViewQueues();
                assertDdl(factory, withList);
                execute("ALTER TABLE base ADD COLUMN amount DOUBLE");
                drainWalAndMatViewQueues();
                assertDdl(factory, withList);
                execute("ALTER MATERIALIZED VIEW v DROP COLUMN extra");
                drainWalAndMatViewQueues();
                assertDdl(factory, noList);
            }
        });
    }

    @Test
    public void testRefreshAndStorageClauses() throws Exception {
        // the list goes between the view's name and its REFRESH clause, and TIMESTAMP(...) between the SQL and its
        // storage clauses; a row-expiry policy may read a column only the list has
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            execute("CREATE MATERIALIZED VIEW manual (ts TIMESTAMP, sym SYMBOL NOCACHE, price DOUBLE, extra INT) WITH BASE base"
                    + " REFRESH MANUAL DEFERRED AS (" + VIEW_SQL + ") PARTITION BY WEEK TTL 3 WEEKS EXPIRE ROWS WHEN extra < 0");
            execute("CREATE MATERIALIZED VIEW timer (ts TIMESTAMP, sym SYMBOL NOCACHE, price DOUBLE, extra INT)"
                    + " REFRESH EVERY 1h START '2024-01-01T00:00:00.000000Z' TIME ZONE 'Europe/London' AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String columns = " (\n\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE,\n\tprice DOUBLE,\n\textra INT\n)";
            assertShowCreateReplays(
                    "manual",
                    "CREATE MATERIALIZED VIEW 'manual'" + columns + " WITH BASE 'base' REFRESH MANUAL DEFERRED AS (\n" + VIEW_SQL
                            + "\n) TIMESTAMP(ts) PARTITION BY WEEK TTL 3 WEEKS EXPIRE ROWS WHEN extra < 0;"
            );
            assertShowCreateReplays(
                    "timer",
                    "CREATE MATERIALIZED VIEW 'timer'" + columns + " WITH BASE 'base' REFRESH EVERY 1h START '2024-01-01T00:00:00.000000Z'"
                            + " TIME ZONE 'Europe/London' AS (\n" + VIEW_SQL + "\n) TIMESTAMP(ts) PARTITION BY DAY;"
            );
        });
    }

    @Test
    public void testRestart() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createView("v");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW v ADD COLUMN extra INT");
            drainWalAndMatViewQueues();
            final String expected = "ddl\n" + listDdl("v", VIEW_COLUMNS + ",\n\textra INT", VIEW_SQL) + "\n";
            engine.releaseInactive();
            engine.releaseAllWriters();
            try (
                    CairoEngine restarted = new CairoEngine(configuration);
                    SqlExecutionContext context = TestUtils.createSqlExecutionCtx(restarted)
            ) {
                restarted.load();
                restarted.buildViewGraphs();
                assertQuery("SHOW CREATE MATERIALIZED VIEW v").withEngine(restarted).withContext(context)
                        .noLeakCheck().noRandomAccess().returns(expected);
            }
        });
    }

    @Test
    public void testShowCreateDatabaseReplays() throws Exception {
        // a dump replays a view whose columns differ from its SQL with those columns, so that the objects that read a
        // column only the view has replay too
        // SHOW CREATE TABLE does not state WAL, which the base needs
        setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, "true");
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            createView("parent");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW parent ADD COLUMN extra INT");
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW child AS (SELECT ts, sym, max(extra) AS max_extra FROM parent SAMPLE BY 1d) PARTITION BY MONTH");
            execute("CREATE VIEW reader AS (SELECT ts, sym, extra FROM parent)");
            drainWalAndMatViewQueues();
            drainWalAndViewQueues();
            final ObjList<String> dump = dumpDatabase();
            Assert.assertEquals(4, dump.size());
            final String parentDdl = listDdl("parent", VIEW_COLUMNS + ",\n\textra INT", VIEW_SQL);
            Assert.assertTrue(dump.toString(), dump.contains(parentDdl));
            execute("DROP VIEW reader");
            execute("DROP MATERIALIZED VIEW child");
            execute("DROP MATERIALIZED VIEW parent");
            execute("DROP TABLE base");
            drainWalAndMatViewQueues();
            for (int i = 0, n = dump.size(); i < n; i++) {
                execute(dump.getQuick(i));
            }
            drainWalAndMatViewQueues();
            drainWalAndViewQueues();
            final ObjList<String> redump = dumpDatabase();
            Assert.assertEquals(dump.size(), redump.size());
            for (int i = 0, n = dump.size(); i < n; i++) {
                Assert.assertEquals(dump.getQuick(i), redump.getQuick(i));
            }
            assertQuery("SELECT \"column\" FROM table_columns('reader')").noLeakCheck().noRandomAccess().returns("column\nts\nsym\nextra\n");
        });
    }

    @Test
    public void testSqlContexts() throws Exception {
        // SHOW CREATE gives one row in a sub-query and a CTE
        assertMemoryLeak(() -> {
            createBase();
            createView("v");
            drainWalAndMatViewQueues();
            final String subQuery = "SELECT count() FROM (SHOW CREATE MATERIALIZED VIEW v) WHERE ddl LIKE '%) TIMESTAMP(ts) PARTITION BY%'";
            final String cte = "WITH s AS (SHOW CREATE MATERIALIZED VIEW v) SELECT ddl LIKE 'CREATE MATERIALIZED VIEW ''v'' (%' listed FROM s";
            assertQuery(subQuery).noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
            assertQuery(cte).noLeakCheck().noRandomAccess().returns("listed\nfalse\n");
            execute("ALTER MATERIALIZED VIEW v ADD COLUMN extra INT");
            drainWalAndMatViewQueues();
            assertQuery(subQuery).noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            assertQuery(cte).noLeakCheck().noRandomAccess().returns("listed\ntrue\n");
        });
    }

    @Test
    public void testSymbolCacheFlag() throws Exception {
        // A CREATE without a list makes every SYMBOL column NOCACHE, so a cached one comes with the list. The list
        // states the flag either way.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            final String sql = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h";
            execute("CREATE MATERIALIZED VIEW cached (ts TIMESTAMP, sym SYMBOL, price DOUBLE) AS (" + sql + ") PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW uncached (ts TIMESTAMP, sym SYMBOL NOCACHE, price DOUBLE) AS (" + sql + ") PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW added AS (" + sql + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW added ADD COLUMN tag SYMBOL");
            drainWalAndMatViewQueues();
            final String cachedDdl = listDdl("cached", "\tts TIMESTAMP,\n\tsym SYMBOL CACHE,\n\tprice DOUBLE", sql);
            final String addedDdl = listDdl("added", "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE,\n\tprice DOUBLE,\n\ttag SYMBOL CACHE", sql);
            assertShowCreateReplays("cached", cachedDdl);
            assertShowCreateReplays("uncached", "CREATE MATERIALIZED VIEW 'uncached' WITH BASE 'base' REFRESH IMMEDIATE AS (\n" + sql + "\n) PARTITION BY DAY;");
            assertShowCreateReplays("added", addedDdl);

            // a server that does not cache SYMBOL columns by default builds the same columns from the list
            setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_CACHE_FLAG, "false");
            assertShowCreateReplays("cached", cachedDdl);
            assertShowCreateReplays("added", addedDdl);
        });
    }

    @Test
    public void testTypes() throws Exception {
        // the list spells each type as SHOW CREATE TABLE does, which CREATE reads back as the same type
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            final String columns = "ts TIMESTAMP, bo BOOLEAN, by BYTE, sh SHORT, c CHAR, i INT, l LONG, f FLOAT, d DOUBLE, dt DATE,"
                    + " t TIMESTAMP, tn TIMESTAMP_NS, s STRING, vc VARCHAR, u UUID, ip IPv4, l256 LONG256, bin BINARY,"
                    + " g1 GEOHASH(1b), g2 GEOHASH(2c), g5 GEOHASH(5c), g12 GEOHASH(12c), g60 GEOHASH(60b),"
                    + " d2 DECIMAL(2,1), d4 DECIMAL(4,2), d9 DECIMAL(9,3), d18 DECIMAL(18,3), d38 DECIMAL(38,10), d76 DECIMAL(76,10),"
                    + " a1 DOUBLE[], a2 DOUBLE[][], a3 DOUBLE[][][], a32 DOUBLE" + "[]".repeat(32) + ","
                    + " sy1 SYMBOL CAPACITY 64, sy2 SYMBOL CACHE, sy3 SYMBOL NOCACHE, sy4 SYMBOL INDEX, sy5 SYMBOL INDEX CAPACITY 512,"
                    + " sy6 SYMBOL INDEX TYPE BITMAP, sy7 SYMBOL INDEX TYPE POSTING, sy8 SYMBOL INDEX TYPE POSTING DELTA,"
                    + " sy9 SYMBOL INDEX TYPE POSTING EF, sy10 SYMBOL INDEX TYPE POSTING INCLUDE (i, l)";
            final String printed = "\tts TIMESTAMP,\n\tbo BOOLEAN,\n\tby BYTE,\n\tsh SHORT,\n\tc CHAR,\n\ti INT,\n\tl LONG,\n\tf FLOAT,"
                    + "\n\td DOUBLE,\n\tdt DATE,\n\tt TIMESTAMP,\n\ttn TIMESTAMP_NS,\n\ts STRING,\n\tvc VARCHAR,\n\tu UUID,\n\tip IPv4,"
                    + "\n\tl256 LONG256,\n\tbin BINARY,\n\tg1 GEOHASH(1b),\n\tg2 GEOHASH(2c),\n\tg5 GEOHASH(5c),\n\tg12 GEOHASH(12c),"
                    + "\n\tg60 GEOHASH(12c),\n\td2 DECIMAL(2,1),\n\td4 DECIMAL(4,2),\n\td9 DECIMAL(9,3),\n\td18 DECIMAL(18,3),"
                    + "\n\td38 DECIMAL(38,10),\n\td76 DECIMAL(76,10),\n\ta1 DOUBLE[],\n\ta2 DOUBLE[][],\n\ta3 DOUBLE[][][],"
                    + "\n\ta32 DOUBLE" + "[]".repeat(32) + ",\n\tsy1 SYMBOL CACHE,\n\tsy2 SYMBOL CACHE,\n\tsy3 SYMBOL NOCACHE,"
                    + "\n\tsy4 SYMBOL CACHE INDEX CAPACITY 256,\n\tsy5 SYMBOL CACHE INDEX CAPACITY 512,"
                    + "\n\tsy6 SYMBOL CACHE INDEX CAPACITY 256,\n\tsy7 SYMBOL CACHE INDEX TYPE POSTING,"
                    + "\n\tsy8 SYMBOL CACHE INDEX TYPE POSTING DELTA,\n\tsy9 SYMBOL CACHE INDEX TYPE POSTING EF,"
                    + "\n\tsy10 SYMBOL CACHE INDEX TYPE POSTING INCLUDE (i, l, ts)";
            final String sql = "SELECT ts FROM base";
            execute("CREATE MATERIALIZED VIEW listed (" + columns + ") AS (" + sql + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertShowCreateReplays("listed", listDdl("listed", printed, sql), "ts");

            // the same columns added to a view one by one, after the SQL's own, but for the INCLUDE that ADD COLUMN
            // does not take
            execute("CREATE MATERIALIZED VIEW added AS (" + sql + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String[] definitions = columns.substring("ts TIMESTAMP, ".length()).split(", (?![^()]*\\))");
            for (int i = 0, n = definitions.length - 1; i < n; i++) {
                execute("ALTER MATERIALIZED VIEW added ADD COLUMN " + definitions[i]);
            }
            drainWalAndMatViewQueues();
            assertShowCreateReplays("added", listDdl("added", printed.substring(0, printed.lastIndexOf(",\n")), sql), "ts");
        });
    }

    private static void assertDdl(RecordCursorFactory factory, String expected) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            for (int pass = 0; pass < 2; pass++) {
                Assert.assertTrue(cursor.hasNext());
                TestUtils.assertEquals(expected, record.getVarcharA(0).toString());
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
            }
        }
    }

    private void assertShowCreateReplays(String viewName, String expectedDdl) throws Exception {
        assertShowCreateReplays(viewName, expectedDdl, "ts, sym");
    }

    // Asserts that SHOW CREATE prints the view's DDL as expected, and that running it after the view is dropped creates
    // a view with the same columns, definition and rows after a FULL refresh, for which SHOW CREATE prints the same.
    private void assertShowCreateReplays(String viewName, String expectedDdl, String orderBy) throws Exception {
        execute("REFRESH MATERIALIZED VIEW " + viewName + " FULL");
        drainWalAndMatViewQueues();
        assertQuery("SHOW CREATE MATERIALIZED VIEW " + viewName).noLeakCheck().noRandomAccess().returns("ddl\n" + expectedDdl + "\n");
        final String before = snapshot(viewName, orderBy);

        execute("DROP MATERIALIZED VIEW " + viewName);
        drainWalAndMatViewQueues();
        execute(expectedDdl);
        drainWalAndMatViewQueues();
        execute("REFRESH MATERIALIZED VIEW " + viewName + " FULL");
        drainWalAndMatViewQueues();
        TestUtils.assertEquals(expectedDdl, before, snapshot(viewName, orderBy));
        assertQuery("SHOW CREATE MATERIALIZED VIEW " + viewName).noLeakCheck().noRandomAccess().returns("ddl\n" + expectedDdl + "\n");
    }

    private static void createBase() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private static void createView(String viewName) throws SqlException {
        execute("CREATE MATERIALIZED VIEW " + viewName + " AS (" + VIEW_SQL + ") PARTITION BY DAY");
    }

    private static ObjList<String> dumpDatabase() throws Exception {
        final ObjList<String> statements = new ObjList<>();
        try (RecordCursorFactory factory = select("SHOW CREATE DATABASE"); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                statements.add(record.getVarcharA(0).toString());
            }
        }
        return statements;
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

    // The DDL of a view on base, REFRESH IMMEDIATE and PARTITION BY DAY, that SHOW CREATE prints with the column list.
    private static String listDdl(String viewName, String columnLines, String sql) {
        return "CREATE MATERIALIZED VIEW '" + viewName + "' (\n" + columnLines + "\n) WITH BASE 'base' REFRESH IMMEDIATE AS (\n"
                + sql + "\n) TIMESTAMP(ts) PARTITION BY DAY;";
    }

    // The columns, storage, definition, column mapping, status and rows of a view.
    private static String snapshot(String viewName, String orderBy) throws SqlException {
        final StringBuilder snapshot = new StringBuilder();
        final String[] queries = {
                "SELECT \"column\", type, indexed, indexType, indexInclude, symbolCached, designated,"
                        + " CASE WHEN indexType = 'BITMAP' THEN indexBlockCapacity END bitmapCapacity"
                        + " FROM table_columns('" + viewName + "')",
                "SELECT designatedTimestamp, partitionBy, ttlValue, ttlUnit FROM tables() WHERE table_name = '" + viewName + "'",
                "SELECT base_table_name, refresh_type, timer_time_zone, timer_start, timer_interval, timer_interval_unit,"
                        + " period_length, period_length_unit, period_delay, period_delay_unit, expire_clause,"
                        + " unmapped_view_columns, ignored_query_columns, view_status"
                        + " FROM materialized_views() WHERE view_name = '" + viewName + "'",
                "SELECT * FROM " + viewName + " ORDER BY " + orderBy
        };
        final StringSink result = new StringSink();
        for (String query : queries) {
            printSql(query, result);
            snapshot.append(result);
        }
        return snapshot.toString();
    }
}
