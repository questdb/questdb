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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.ExpiryReadPolicy;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * CREATE MATERIALIZED VIEW requires every column of the view's SQL to have a name that does not depend on the
 * server version or settings: a computed column needs an explicit alias, a plain column reference keeps its
 * column's name unless another column takes it, and a zero-argument function needs its parentheses. Views created
 * before the rules keep working, and SHOW CREATE prints their SQL with the names and parentheses it needs to replay.
 * <p>
 * The generated names in the expected messages follow the alias rules of each
 * {@code cairo.sql.column.alias.expression.enabled} setting: the legacy rules name a function call after the
 * function, a cast "cast", an operator expression "column", a decimal constant "column" with a sequence number, a
 * string constant after its text, and a repeated column reference after the column with a sequence number; the
 * expression rules name a column after its expression text, and a repeated column reference after the column with
 * "_2".
 */
public class MatViewExplicitColumnNamesTest extends AbstractCairoTest {
    private static final String[] ALIAS_EXPRESSION_MODES = {"false", "true"};

    @Test
    public void testBareZeroArgFunctionRejected() throws Exception {
        // In CREATE a bare name is a column. One that names no column but a zero-argument function fails at its
        // position, asking for the parentheses, wherever the SQL's own select lists use it, explicitly aliased,
        // quoted, in another case or in a sub-query.
        assertMemoryLeak(() -> {
            createBase();
            for (String mode : ALIAS_EXPRESSION_MODES) {
                setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, mode);
                assertCreateFails("SELECT ts, sym, count FROM base SAMPLE BY 1h", "count FROM", "count is not a column; write count() to call the function");
                assertCreateFails("SELECT ts, sym, pi, avg(price) AS avg_price FROM base SAMPLE BY 1h", "pi,", "pi is not a column; write pi() to call the function");
                assertCreateFails("SELECT ts, sym, \"pi\", avg(price) AS avg_price FROM base SAMPLE BY 1h", "\"pi\"", "pi is not a column; write pi() to call the function");
                assertCreateFails("SELECT ts, sym, PI AS p, avg(price) AS avg_price FROM base SAMPLE BY 1h", "PI AS", "PI is not a column; write PI() to call the function");
                assertCreateFails("SELECT ts, sym, pi FROM base", "pi FROM", "pi is not a column; write pi() to call the function");
                assertCreateFails(
                        "SELECT ts, sym, avg(price) AS avg_price FROM base WHERE price < (SELECT pi FROM long_sequence(1)) SAMPLE BY 1h",
                        "pi FROM long",
                        "pi is not a column; write pi() to call the function"
                );
                assertCreateFails(
                        "WITH c AS (SELECT ts, sym, count FROM base SAMPLE BY 1h) SELECT ts, sym, count FROM c",
                        "count FROM base",
                        "count is not a column; write count() to call the function"
                );
                // a name that is neither a column nor a zero-argument function keeps its error
                assertCreateFails("SELECT ts, sym, no_such_column FROM base", "no_such_column", "Invalid column: no_such_column");
            }

            // With the parentheses written, the views compile, and so does a column named like the function.
            execute("CREATE MATERIALIZED VIEW counts AS (SELECT ts, sym, count() AS count FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW pis AS (SELECT ts, sym, pi() AS pi, PI() AS p FROM base) PARTITION BY DAY");
            execute("CREATE TABLE counted (sym SYMBOL, count LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW counted_copy AS (SELECT ts, sym, count FROM counted) PARTITION BY DAY");
            insertBaseRows();
            execute("INSERT INTO counted VALUES ('a', 7, '2024-01-01T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("counts ORDER BY ts, sym").timestamp("ts").expectSize().noLeakCheck().returns("""
                    ts\tsym\tcount
                    2024-01-01T00:00:00.000000Z\ta\t2
                    2024-01-01T00:00:00.000000Z\tb\t1
                    2024-01-01T01:00:00.000000Z\ta\t1
                    """);
            assertQuery("SELECT count(*) n, min(pi) min_pi, max(p) max_p FROM pis").noLeakCheck().noRandomAccess().expectSize().returns("""
                    n\tmin_pi\tmax_p
                    4\t3.141592653589793\t3.141592653589793
                    """);
            assertQuery("counted_copy").timestamp("ts").expectSize().noLeakCheck().returns("""
                    ts\tsym\tcount
                    2024-01-01T00:10:00.000000Z\ta\t7
                    """);
        });
    }

    @Test
    public void testComputedColumnRequiresAlias() throws Exception {
        // Under both alias rules, an unaliased function call, operator expression or constant fails at its first
        // character, and the hint quotes it as the SQL writes it, not the name the parser generated for it.
        assertMemoryLeak(() -> {
            createBase();
            for (String mode : ALIAS_EXPRESSION_MODES) {
                setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, mode);
                assertCreateFails("SELECT ts, sym, avg(price) FROM base SAMPLE BY 1h", "avg(price)", aliasRequired("avg(price)"));
                assertCreateFails("SELECT ts, sym, count() FROM base SAMPLE BY 1h", "count()", aliasRequired("count()"));
                assertCreateFails(
                        "SELECT ts, sym, sum(price) * 2 FROM base SAMPLE BY 1h",
                        "sum(price) * 2",
                        aliasRequired("sum(price) * 2")
                );
                assertCreateFails("SELECT ts, sym, price::INT FROM base", "price::INT", aliasRequired("price::INT"));
                assertCreateFails("SELECT ts, sym, 42 FROM base", "42", aliasRequired("42"));
                assertCreateFails("SELECT ts, sym, 1.5 FROM base", "1.5", aliasRequired("1.5"));
                assertCreateFails("SELECT ts, sym, 'abc' FROM base", "'abc'", aliasRequired("'abc'"));
                // the item spans lines and comments as the SQL writes it, without the whitespace around it
                assertCreateFails(
                        "SELECT ts, sym,\n    max(price) /* top */\n        - min(price)\n FROM base SAMPLE BY 1h",
                        "max(price) /*",
                        aliasRequired("max(price) /* top */\n        - min(price)")
                );
                // the hint names neither the expression cut to the generated name's size nor a column
                final String longColumn = "c" + "x".repeat(70);
                assertCreateFails(
                        "SELECT ts, sym, " + longColumn + " + price FROM base",
                        longColumn + " + price",
                        aliasRequired(longColumn + " + price")
                );
                // a declared variable is the item the SQL writes
                assertCreateFails("DECLARE @c := price + 1 SELECT ts, sym, @c FROM base", "@c FROM", aliasRequired("@c"));
                // an explicit alias has to be a valid column name
                assertCreateFails(
                        "SELECT ts, sym, max(price) AS \"a,b\" FROM base SAMPLE BY 1h",
                        "\"a,b\"",
                        "column alias 'a,b' contains unsupported characters"
                );
                assertCreateFails(
                        "SELECT ts, sym, max(price) AS \"*\" FROM base SAMPLE BY 1h",
                        "\"*\"",
                        "column alias '*' contains unsupported characters"
                );
                // the alias is required in every select model of the SQL's own text
                assertCreateFails(
                        "SELECT ts, sym, avg_price FROM (SELECT ts, sym, avg(price) AS avg_price, count() FROM base SAMPLE BY 1h)",
                        "count()",
                        aliasRequired("count()")
                );
                assertCreateFails(
                        "WITH c AS (SELECT ts, sym, max(price) FROM base SAMPLE BY 1h) SELECT * FROM c",
                        "max(price)",
                        aliasRequired("max(price)")
                );
            }
        });
    }

    @Test
    public void testComputedColumnRequiresAliasInModelsNotReadingBase() throws Exception {
        // The rules apply to join and union branches that do not read the base table too: the names of their
        // columns reach the view's columns through wildcards and plain references as well.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE TABLE rates (sym SYMBOL, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String mode : ALIAS_EXPRESSION_MODES) {
                setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, mode);
                assertDdlFails(
                        "CREATE MATERIALIZED VIEW v WITH BASE base AS (SELECT b.ts, b.sym, last(b.price) AS price, last(r.max) AS rate "
                                + "FROM base b JOIN (SELECT sym, max(rate) FROM rates) r ON (sym) SAMPLE BY 1h) PARTITION BY DAY",
                        "max(rate)",
                        aliasRequired("max(rate)")
                );
                assertDdlFails(
                        "CREATE MATERIALIZED VIEW v WITH BASE base AS (SELECT b.ts, b.sym, last(b.price) AS price, last(k.rate) AS rate "
                                + "FROM base b JOIN (SELECT sym, rate FROM rates UNION ALL SELECT sym, rate * 2 FROM rates) k ON (sym) "
                                + "SAMPLE BY 1h) PARTITION BY DAY",
                        "rate * 2",
                        aliasRequired("rate * 2")
                );
            }
            // aliased, both views compile
            execute("CREATE MATERIALIZED VIEW v1 WITH BASE base AS (SELECT b.ts, b.sym, last(b.price) AS price, last(r.max_rate) AS rate "
                    + "FROM base b JOIN (SELECT sym, max(rate) AS max_rate FROM rates) r ON (sym) SAMPLE BY 1h) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW v2 WITH BASE base AS (SELECT b.ts, b.sym, last(b.price) AS price, last(k.rate) AS rate "
                    + "FROM base b JOIN (SELECT sym, rate FROM rates UNION ALL SELECT sym, rate * 2 AS rate FROM rates) k ON (sym) "
                    + "SAMPLE BY 1h) PARTITION BY DAY");
            assertQuery("SELECT view_name, view_status FROM materialized_views ORDER BY view_name").noLeakCheck().returns("""
                    view_name\tview_status
                    v1\tvalid
                    v2\tvalid
                    """);
        });
    }

    @Test
    public void testCreateReadingRegularView() throws Exception {
        // The models of a regular view that the SQL reads keep the older check, which only rejects a generated name
        // that is not a valid column name: under the legacy alias rules the view's unaliased avg(price) is "avg"
        // and passes, under the expression rules it is "avg(price)" and fails. A bare name in the view's text stays
        // a call. The view's text is not the SQL's, which the user can change. The SQL's own select list over the
        // regular view needs its aliases under either rule.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            execute("CREATE VIEW v_avg AS (SELECT ts, sym, avg(price), pi FROM base SAMPLE BY 1h)");
            drainWalAndViewQueues();
            for (String mode : ALIAS_EXPRESSION_MODES) {
                setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, mode);
                assertDdlFails(
                        "CREATE MATERIALIZED VIEW v WITH BASE base AS (SELECT ts, sym, max(pi) FROM v_avg SAMPLE BY 1d) PARTITION BY DAY",
                        "max(pi)",
                        aliasRequired("max(pi)")
                );
            }
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
            execute("CREATE MATERIALIZED VIEW mv WITH BASE base AS (SELECT ts, sym, max(avg) AS max_avg, max(pi) AS max_pi FROM v_avg SAMPLE BY 1d) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertQuery("mv ORDER BY ts, sym").timestamp("ts").expectSize().noLeakCheck().returns("""
                    ts\tsym\tmax_avg\tmax_pi
                    2024-01-01T00:00:00.000000Z\ta\t4.5\t3.141592653589793
                    2024-01-01T00:00:00.000000Z\tb\t2.5\t3.141592653589793
                    """);
            execute("DROP MATERIALIZED VIEW mv");

            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
            final String ddl = "CREATE MATERIALIZED VIEW mv WITH BASE base AS (SELECT ts, sym, max(\"avg(price)\") AS max_avg FROM v_avg SAMPLE BY 1d) PARTITION BY DAY";
            try {
                execute(ddl);
                Assert.fail("CREATE must fail: " + ddl);
            } catch (SqlException e) {
                // the error names the column's generated name: the select item lies in the view's text
                TestUtils.assertEquals(aliasRequired("avg(price)"), e.getFlyweightMessage());
            }
            Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testDuplicateColumnReferenceRequiresAlias() throws Exception {
        // A plain column reference whose name another column of its select list took gets a generated name, so it
        // needs an alias, whether the other column is a reference to the same column or an explicit alias. The error
        // names the column, not the generated name, and quotes the reference as the SQL writes it.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE TABLE dim (sym SYMBOL, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String mode : ALIAS_EXPRESSION_MODES) {
                setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, mode);
                assertCreateFails("SELECT ts, sym, price, price FROM base", "price FROM", aliasRequired("price", "price"));
                assertCreateFails("SELECT b.ts, b.sym, b.price, b.price FROM base b", "b.price FROM", aliasRequired("price", "b.price"));
                assertCreateFails("SELECT ts, price AS sym, sym FROM base", "sym FROM", aliasRequired("sym", "sym"));
                assertCreateFails(
                        "SELECT ts, sym, avg_price FROM (SELECT ts, sym, sym, avg(price) AS avg_price FROM base SAMPLE BY 1h)",
                        "sym, avg(price)",
                        aliasRequired("sym", "sym")
                );
                // a second wildcard has a generated name, and a wildcard cannot have an alias
                assertCreateFails("SELECT b.*, * FROM base b", "* FROM", wildcardRefused("*"));
                assertDdlFails(
                        "CREATE MATERIALIZED VIEW v WITH BASE base AS (SELECT b.*, d.* FROM base b ASOF JOIN dim d ON (sym)) PARTITION BY DAY",
                        "d.*",
                        wildcardRefused("d.*")
                );
            }
            execute("CREATE MATERIALIZED VIEW v AS (SELECT ts, sym, price, price AS price2 FROM base) PARTITION BY DAY");
            insertBaseRows();
            drainWalAndMatViewQueues();
            assertQuery("SELECT * FROM v WHERE sym = 'b'").timestamp("ts").noLeakCheck().returns("""
                    ts\tsym\tprice\tprice2
                    2024-01-01T00:20:00.000000Z\tb\t2.5\t2.5
                    """);
        });
    }

    @Test
    public void testPlainColumnReferencesNeedNoAlias() throws Exception {
        // Plain column references keep the names of their columns, with or without a table prefix or quotes, and
        // so do the columns a wildcard brings and the columns a sub-query names explicitly.
        assertMemoryLeak(() -> {
            createBase();
            for (String mode : ALIAS_EXPRESSION_MODES) {
                setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, mode);
                final String[] queries = {
                        "SELECT ts, sym, price FROM base",
                        "SELECT b.ts, b.sym, b.price FROM base b",
                        "SELECT \"ts\", \"sym\", \"price\" FROM base",
                        "SELECT * FROM base",
                        "SELECT b.* FROM base b",
                        "base WHERE price > 0",
                        "SELECT ts, sym, avg_price FROM (SELECT ts, sym, avg(price) AS avg_price FROM base SAMPLE BY 1h)",
                        "WITH c AS (SELECT ts, sym, price FROM base) SELECT ts, sym, max(price) AS max_price FROM c SAMPLE BY 1h"
                };
                for (String query : queries) {
                    execute("CREATE MATERIALIZED VIEW v AS (" + query + ") PARTITION BY DAY");
                    assertQuery("SELECT view_sql, view_status FROM materialized_views").noLeakCheck().noRandomAccess().returns(
                            "view_sql\tview_status\n" + query + "\tvalid\n"
                    );
                    execute("DROP MATERIALIZED VIEW v");
                }
            }
        });
    }

    @Test
    public void testOlderViewsWithBareCallsKeepRefreshing() throws Exception {
        // Views created before the rules may call zero-argument functions without parentheses. Their refresh
        // keeps reading such a name as the call, so they keep refreshing after base inserts and write the values
        // the calls compute.
        assertMemoryLeak(() -> {
            createBase();
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
            createOlderView("count_mv", "SELECT ts, sym, count FROM base SAMPLE BY 1h");
            createOlderView("pi_mv", "SELECT ts, sym, pi, avg(price) FROM base SAMPLE BY 1h");
            insertBaseRows();
            drainWalAndMatViewQueues();
            assertQuery("count_mv ORDER BY ts, sym").timestamp("ts").expectSize().noLeakCheck().returns("""
                    ts\tsym\tcount
                    2024-01-01T00:00:00.000000Z\ta\t2
                    2024-01-01T00:00:00.000000Z\tb\t1
                    2024-01-01T01:00:00.000000Z\ta\t1
                    """);
            assertQuery("pi_mv ORDER BY ts, sym").timestamp("ts").expectSize().noLeakCheck().returns("""
                    ts\tsym\tpi\tavg
                    2024-01-01T00:00:00.000000Z\ta\t3.141592653589793\t2.5
                    2024-01-01T00:00:00.000000Z\tb\t3.141592653589793\t2.5
                    2024-01-01T01:00:00.000000Z\ta\t3.141592653589793\t4.5
                    """);

            // under the server's default alias rules, which name avg(price) "avg(price)", too
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 6.5, 60.0, '2024-01-01T00:50:00.000000Z'),
                    ('c', 8.5, 80.0, '2024-01-02T05:00:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            assertQuery("count_mv ORDER BY ts, sym").timestamp("ts").expectSize().noLeakCheck().returns("""
                    ts\tsym\tcount
                    2024-01-01T00:00:00.000000Z\ta\t3
                    2024-01-01T00:00:00.000000Z\tb\t1
                    2024-01-01T01:00:00.000000Z\ta\t1
                    2024-01-02T05:00:00.000000Z\tc\t1
                    """);
            assertQuery("pi_mv ORDER BY ts, sym").timestamp("ts").expectSize().noLeakCheck().returns("""
                    ts\tsym\tpi\tavg
                    2024-01-01T00:00:00.000000Z\ta\t3.141592653589793\t3.8333333333333335
                    2024-01-01T00:00:00.000000Z\tb\t3.141592653589793\t2.5
                    2024-01-01T01:00:00.000000Z\ta\t3.141592653589793\t4.5
                    2024-01-02T05:00:00.000000Z\tc\t3.141592653589793\t8.5
                    """);
            assertQuery("SELECT view_name, view_sql, view_status FROM materialized_views ORDER BY view_name").noLeakCheck().returns("""
                    view_name\tview_sql\tview_status
                    count_mv\tSELECT ts, sym, count FROM base SAMPLE BY 1h\tvalid
                    pi_mv\tSELECT ts, sym, pi, avg(price) FROM base SAMPLE BY 1h\tvalid
                    """);
            // the view whose SQL relies on the generated name avg copies by position
            final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName("pi_mv"));
            Assert.assertNotNull(state);
            Assert.assertTrue(state.isRecordCopyPositional());
        });
    }

    @Test
    public void testShowCreateOlderViewWithBarePi() throws Exception {
        // SHOW CREATE writes the parentheses of a bare call, and an alias naming the view column the call feeds.
        assertMemoryLeak(() -> {
            createBase();
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
            createOlderView("pi_mv", "SELECT ts, sym, pi, avg(price) AS avg_price FROM base SAMPLE BY 1h");
            createOlderView("quoted_pi_mv", "SELECT ts, sym, \"pi\", PI AS p, avg(price) AS avg_price FROM base SAMPLE BY 1h");
            insertBaseRows();
            drainWalAndMatViewQueues();
            assertQuery("pi_mv ORDER BY ts, sym").timestamp("ts").expectSize().noLeakCheck().returns("""
                    ts\tsym\tpi\tavg_price
                    2024-01-01T00:00:00.000000Z\ta\t3.141592653589793\t2.5
                    2024-01-01T00:00:00.000000Z\tb\t3.141592653589793\t2.5
                    2024-01-01T01:00:00.000000Z\ta\t3.141592653589793\t4.5
                    """);
            assertShowCreateReplays("pi_mv", "SELECT ts, sym, pi() AS pi, avg(price) AS avg_price FROM base SAMPLE BY 1h");
            // a quoted bare name loses its quotes as a function name, and an aliased one keeps its alias
            assertShowCreateReplays("quoted_pi_mv", "SELECT ts, sym, pi() AS pi, PI() AS p, avg(price) AS avg_price FROM base SAMPLE BY 1h");
        });
    }

    @Test
    public void testShowCreateOlderViewWithPre5802Names() throws Exception {
        // A view created before questdb/questdb#5802, or under the legacy alias rules, named count() "count" and
        // avg(price) "avg". SHOW CREATE aliases each generated name to the view column it feeds, under either
        // alias rule, so its output replays to the same columns.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            for (String mode : ALIAS_EXPRESSION_MODES) {
                setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
                createOlderView("counts", "SELECT ts, sym, count(), avg(price), max(amount) FROM base SAMPLE BY 1h");
                drainWalAndMatViewQueues();
                setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, mode);
                assertQuery("counts ORDER BY ts, sym").timestamp("ts").expectSize().noLeakCheck().returns("""
                        ts\tsym\tcount\tavg\tmax
                        2024-01-01T00:00:00.000000Z\ta\t2\t2.5\t30.0
                        2024-01-01T00:00:00.000000Z\tb\t1\t2.5\t20.0
                        2024-01-01T01:00:00.000000Z\ta\t1\t4.5\t40.0
                        """);
                assertShowCreateReplays(
                        "counts",
                        "SELECT ts, sym, count() AS count, avg(price) AS avg, max(amount) AS max FROM base SAMPLE BY 1h"
                );
                execute("DROP MATERIALIZED VIEW counts");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testShowCreateCompileFailures() throws Exception {
        // SHOW CREATE prints the stored SQL when the compile of an older view's SQL fails, a stale table reference
        // and a missing table included, and fails with the cancellation when the statement is cancelled meanwhile.
        // Without the compile it cannot tell whether the SQL builds the view's columns, so it prints them as a
        // column list.
        assertMemoryLeak(() -> {
            createBase();
            createOlderView("counts", "SELECT ts, sym, count FROM base SAMPLE BY 1h");
            drainWalAndMatViewQueues();
            final String showCreate = "SHOW CREATE MATERIALIZED VIEW counts";
            final String stored = """
                    ddl
                    CREATE MATERIALIZED VIEW 'counts' (
                    	ts TIMESTAMP,
                    	sym SYMBOL NOCACHE,
                    	count LONG
                    ) WITH BASE 'base' REFRESH IMMEDIATE AS (
                    SELECT ts, sym, count FROM base SAMPLE BY 1h
                    ) TIMESTAMP(ts) PARTITION BY DAY;
                    """;
            final TableToken baseToken = engine.verifyTableName("base");
            // the optimiser reads the base table's columns, the code generator then opens the table
            try (SqlExecutionContextImpl context = newFailingReaderContext(TableReferenceOutOfDateException.of(baseToken), false)) {
                assertQuery(showCreate).withContext(context).noLeakCheck().noRandomAccess().returns(stored);
            }
            try (SqlExecutionContextImpl context = newFailingReaderContext(CairoException.tableDoesNotExist("base"), false)) {
                assertQuery(showCreate).withContext(context).noLeakCheck().noRandomAccess().returns(stored);
            }
            try (SqlExecutionContextImpl context = newFailingReaderContext(CairoException.nonCritical().put("could not open the table"), true)) {
                assertQuery(showCreate).withContext(context).noLeakCheck().noRandomAccess().returns(stored);
            }
            try (SqlExecutionContextImpl context = newFailingReaderContext(CairoException.queryCancelled(), true)) {
                assertQuery(showCreate).withContext(context).noLeakCheck().fails(-1, "cancelled by user");
                // the failure leaves the context reading bare function names as calls and tables as before
                assertQuery("SELECT pi FROM long_sequence(1)").withContext(context).noLeakCheck().expectSize().returns("pi\n3.141592653589793\n");
                Assert.assertEquals(ExpiryReadPolicy.FILTER, context.getExpiryReadPolicy());
            }
            assertQuery(showCreate).noLeakCheck().noRandomAccess().returns("""
                    ddl
                    CREATE MATERIALIZED VIEW 'counts' WITH BASE 'base' REFRESH IMMEDIATE AS (
                    SELECT ts, sym, count() AS count FROM base SAMPLE BY 1h
                    ) PARTITION BY DAY;
                    """);
        });
    }

    @Test
    public void testShowCreateDatabaseAddsExplicitNames() throws Exception {
        // SHOW CREATE DATABASE prints each view's DDL as SHOW CREATE MATERIALIZED VIEW does.
        assertMemoryLeak(() -> {
            createBase();
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
            createOlderView("counts", "SELECT ts, sym, count FROM base SAMPLE BY 1h");
            execute("CREATE MATERIALIZED VIEW maxes AS (SELECT ts, sym, max(price) AS max_price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            printSql("SHOW CREATE DATABASE");
            TestUtils.assertContains(sink, """
                    CREATE MATERIALIZED VIEW 'counts' WITH BASE 'base' REFRESH IMMEDIATE AS (
                    SELECT ts, sym, count() AS count FROM base SAMPLE BY 1h
                    ) PARTITION BY DAY;""");
            TestUtils.assertContains(sink, """
                    CREATE MATERIALIZED VIEW 'maxes' WITH BASE 'base' REFRESH IMMEDIATE AS (
                    SELECT ts, sym, max(price) AS max_price FROM base SAMPLE BY 1h
                    ) PARTITION BY DAY;""");
        });
    }

    @Test
    public void testShowCreateKeepsCommentsAndContext() throws Exception {
        // The alias goes right after its column's expression, ahead of a comment that follows it, and SHOW CREATE
        // leaves the caller's context reading bare function names as calls.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
            createOlderView("counts", "SELECT ts, sym, count() -- per hour\n, max(price) /* the top */ FROM base SAMPLE BY 1h");
            drainWalAndMatViewQueues();
            assertShowCreateReplays(
                    "counts",
                    "SELECT ts, sym, count() AS count -- per hour\n, max(price) AS max /* the top */ FROM base SAMPLE BY 1h"
            );
            assertQuery("SELECT pi FROM long_sequence(1)").noLeakCheck().expectSize().returns("pi\n3.141592653589793\n");
        });
    }

    @Test
    public void testShowCreateLongColumnNameReference() throws Exception {
        // Under the default alias rules, a reference to a column name longer than 64 characters gets the name cut
        // to that size, which depends on the setting, so SHOW CREATE writes it as an alias.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
            final String venue = "venue_" + "v".repeat(64);
            final String venue64 = venue.substring(0, 64);
            execute("CREATE TABLE base (" + venue + " SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertBaseRows();
            execute("CREATE MATERIALIZED VIEW venues AS (SELECT ts, " + venue + ", max(price) AS max_price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertQuery("SELECT * FROM venues WHERE " + venue64 + " = 'b'").timestamp("ts").noLeakCheck().returns(
                    "ts\t" + venue64 + "\tmax_price\n2024-01-01T00:00:00.000000Z\tb\t2.5\n"
            );
            assertShowCreateReplays(
                    "venues",
                    "SELECT ts, " + venue + " AS " + venue64 + ", max(price) AS max_price FROM base SAMPLE BY 1h",
                    "ts, " + venue64
            );
        });
    }

    @Test
    public void testShowCreateOlderPassthroughView() throws Exception {
        // A passthrough view's refresh reads every bare name as a column, and so does SHOW CREATE. The generated
        // name of the view's computed column is a keyword, which the alias quotes.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
            createOlderView("doubled", "SELECT *, price * 2 FROM base");
            drainWalAndMatViewQueues();
            assertShowCreateReplays("doubled", "SELECT *, price * 2 AS \"column\" FROM base", "ts, sym");
        });
    }

    @Test
    public void testShowCreateOlderViewNestedNames() throws Exception {
        // A column of a model below the top level keeps the name that the models above it reference, unless a
        // wildcard of the top-level select list brings it to a view column, whose name it then takes.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE TABLE rates (sym SYMBOL, rate DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertBaseRows();
            execute("INSERT INTO rates VALUES ('a', 0.5, '2024-01-01T00:00:00.000000Z'), ('b', 0.25, '2024-01-01T00:00:00.000000Z')");
            final String[][] cases = {
                    // the view's SQL, the alias rules to print it under, the printed SQL
                    {
                            "SELECT ts, sym, avg FROM (SELECT ts, sym, avg(price) FROM base SAMPLE BY 1h)",
                            "false",
                            "SELECT ts, sym, avg FROM (SELECT ts, sym, avg(price) AS avg FROM base SAMPLE BY 1h)"
                    },
                    {
                            "SELECT * FROM (SELECT ts, sym, count() FROM base SAMPLE BY 1h)",
                            "true",
                            "SELECT * FROM (SELECT ts, sym, count() AS count FROM base SAMPLE BY 1h)"
                    },
                    {
                            "WITH c AS (SELECT ts, sym, max(price) FROM base SAMPLE BY 1h) SELECT * FROM c",
                            "true",
                            "WITH c AS (SELECT ts, sym, max(price) AS max FROM base SAMPLE BY 1h) SELECT * FROM c"
                    },
                    {
                            "SELECT b.ts, b.sym, last(r.max) AS rate FROM base b JOIN (SELECT sym, max(rate) FROM rates) r ON (sym) SAMPLE BY 1h",
                            "false",
                            "SELECT b.ts, b.sym, last(r.max) AS rate FROM base b JOIN (SELECT sym, max(rate) AS max FROM rates) r ON (sym) SAMPLE BY 1h"
                    },
                    {
                            // a wildcard of the top-level select list does not reach the innermost count()
                            "SELECT * FROM (SELECT ts, sym, max_price FROM (SELECT ts, sym, max(price) AS max_price, count() FROM base SAMPLE BY 1h))",
                            "false",
                            "SELECT * FROM (SELECT ts, sym, max_price FROM (SELECT ts, sym, max(price) AS max_price, count() AS count FROM base SAMPLE BY 1h))"
                    },
                    {
                            // a declared sub-query that the query reads twice is one text, which gets its names once
                            "DECLARE @p := (SELECT pi FROM long_sequence(1)) SELECT ts, sym, avg(price) FROM base WHERE price < @p OR price > @p SAMPLE BY 1h",
                            "true",
                            "DECLARE @p := (SELECT pi() FROM long_sequence(1)) SELECT ts, sym, avg(price) AS avg FROM base WHERE price < @p OR price > @p SAMPLE BY 1h"
                    }
            };
            for (String[] c : cases) {
                setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
                createOlderView("nested", "WITH BASE base", c[0]);
                drainWalAndMatViewQueues();
                setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, c[1]);
                assertShowCreateReplays("nested", c[2]);
                execute("DROP MATERIALIZED VIEW nested");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testShowCreateOlderViewWithDuplicateAndKeywordNames() throws Exception {
        // A repeated column reference takes the name of the view column it feeds, and an alias that is a keyword
        // comes quoted.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            for (String mode : ALIAS_EXPRESSION_MODES) {
                setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
                createOlderView("dups", "SELECT ts, sym, sym, max(price) * 2, last(amount)::INT FROM base SAMPLE BY 1h");
                drainWalAndMatViewQueues();
                setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, mode);
                assertQuery("dups ORDER BY ts, sym").timestamp("ts").expectSize().noLeakCheck().returns("""
                        ts\tsym\tsym1\tcolumn\tcast
                        2024-01-01T00:00:00.000000Z\ta\ta\t7.0\t30
                        2024-01-01T00:00:00.000000Z\tb\tb\t5.0\t20
                        2024-01-01T01:00:00.000000Z\ta\ta\t9.0\t40
                        """);
                assertShowCreateReplays(
                        "dups",
                        "SELECT ts, sym, sym AS sym1, max(price) * 2 AS \"column\", last(amount)::INT AS \"cast\" FROM base SAMPLE BY 1h"
                );
                execute("DROP MATERIALIZED VIEW dups");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testShowCreateOlderViewNames() throws Exception {
        // A name is written as it is when it is a plain identifier and not a keyword, and double-quoted otherwise.
        // The text of a regular view that the SQL reads stays as it is.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, prix\u00e9 DOUBLE, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.5, 1.5, 10.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.5, 2.5, 20.0, '2024-01-01T00:20:00.000000Z')
                    """);
            execute("CREATE VIEW v_avg AS (SELECT ts, sym, avg(price) FROM base SAMPLE BY 1h)");
            drainWalAndViewQueues();
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
            createOlderView("upper", "SELECT ts, sym, Count() FROM base SAMPLE BY 1h");
            createOlderView("digits", "SELECT ts, sym, 42 FROM base");
            createOlderView("accents", "SELECT ts, sym, prix\u00e9, prix\u00e9 FROM base");
            createOlderView("over_view", "WITH BASE base", "SELECT ts, sym, max(avg) FROM v_avg SAMPLE BY 1d");
            drainWalAndMatViewQueues();
            assertShowCreateReplays("upper", "SELECT ts, sym, Count() AS Count FROM base SAMPLE BY 1h");
            assertShowCreateReplays("digits", "SELECT ts, sym, 42 AS \"42\" FROM base");
            assertShowCreateReplays("accents", "SELECT ts, sym, prix\u00e9, prix\u00e9 AS \"prix\u00e91\" FROM base");
            assertQuery("SHOW CREATE MATERIALIZED VIEW over_view").noLeakCheck().noRandomAccess().returns("""
                    ddl
                    CREATE MATERIALIZED VIEW 'over_view' WITH BASE 'base' REFRESH IMMEDIATE AS (
                    SELECT ts, sym, max(avg) AS max FROM v_avg SAMPLE BY 1d
                    ) PARTITION BY DAY;
                    """);
        });
    }

    @Test
    public void testShowCreateOlderViewWithExpireRows() throws Exception {
        // SHOW CREATE prints a row-expiry policy next to the names it adds.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
            executeWithoutExplicitNamesCheck(
                    "CREATE MATERIALIZED VIEW counts AS (SELECT ts, sym, count() FROM base SAMPLE BY 1h) PARTITION BY DAY EXPIRE ROWS WHEN count < 2"
            );
            drainWalAndMatViewQueues();
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
            assertQuery("SHOW CREATE MATERIALIZED VIEW counts").noLeakCheck().noRandomAccess().returns("""
                    ddl
                    CREATE MATERIALIZED VIEW 'counts' WITH BASE 'base' REFRESH IMMEDIATE AS (
                    SELECT ts, sym, count() AS count FROM base SAMPLE BY 1h
                    ) PARTITION BY DAY EXPIRE ROWS WHEN count < 2;
                    """);
        });
    }

    @Test
    public void testShowCreatePrintsSqlAsStoredWhenItCannotAddNames() throws Exception {
        // SHOW CREATE prints the stored SQL as it is when the SQL no longer compiles, when it no longer produces
        // as many columns as the view has, when the names it would add do not replay under the rules, and when
        // the SQL needs no names. In the first two cases, the SQL no longer builds the view's columns, and SHOW
        // CREATE prints them as a column list.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRows();
            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
            // the outer avg references the generated name of the legacy rules, which the expression rules change
            createOlderView("renamed", "SELECT ts, sym, avg FROM (SELECT ts, sym, avg(price) FROM base SAMPLE BY 1h)");
            // the stored wildcard brings a column that the base gains later
            createOlderView("widened", "SELECT *, price * 2 FROM base");
            // the expression rules name the nested count() "count()", which is no valid alias where the base is read
            createOlderView("drifted", "SELECT ts, sym, count FROM (SELECT ts, sym, count() FROM base SAMPLE BY 1h)");
            // the wildcard brings the columns of a regular view that ALTER VIEW reorders
            execute("CREATE VIEW v_base AS (SELECT ts, sym, price FROM base)");
            drainWalAndViewQueues();
            createOlderView("reordered", "WITH BASE base", "SELECT *, price * 2 FROM v_base");
            execute("CREATE MATERIALIZED VIEW named AS (SELECT ts, sym, max(price) AS max_price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base ADD COLUMN extra INT");
            drainWalAndMatViewQueues();
            execute("ALTER VIEW v_base AS (SELECT sym, ts, price FROM base)");
            drainWalAndViewQueues();

            setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
            assertShowCreateSql(
                    "renamed",
                    "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE,\n\tavg DOUBLE\n",
                    "SELECT ts, sym, avg FROM (SELECT ts, sym, avg(price) FROM base SAMPLE BY 1h)",
                    "ts"
            );
            assertShowCreateSql(
                    "widened",
                    "\tsym SYMBOL NOCACHE,\n\tprice DOUBLE,\n\tamount DOUBLE,\n\tts TIMESTAMP,\n\t\"column\" DOUBLE\n",
                    "SELECT *, price * 2 FROM base",
                    "ts"
            );
            assertShowCreateSql("drifted", "SELECT ts, sym, count FROM (SELECT ts, sym, count() FROM base SAMPLE BY 1h)");
            assertShowCreateSql("named", "SELECT ts, sym, max(price) AS max_price FROM base SAMPLE BY 1h");
            assertQuery("SHOW CREATE MATERIALIZED VIEW reordered").noLeakCheck().noRandomAccess().returns("""
                    ddl
                    CREATE MATERIALIZED VIEW 'reordered' (
                    	ts TIMESTAMP,
                    	sym SYMBOL NOCACHE,
                    	price DOUBLE,
                    	"column" DOUBLE
                    ) WITH BASE 'base' REFRESH IMMEDIATE AS (
                    SELECT *, price * 2 FROM v_base
                    ) TIMESTAMP(ts) PARTITION BY DAY;
                    """);
        });
    }

    @Test
    public void testShowCreatePrintsSqlAsStoredWhenItIsNoQuery() throws Exception {
        // A definition file whose SQL is not a query, which no CREATE writes, comes as stored, with the view's
        // columns as a column list.
        assertMemoryLeak(() -> {
            createBase();
            createOlderView("counts", "SELECT ts, sym, count FROM base SAMPLE BY 1h");
            final TableToken viewToken = engine.verifyTableName("counts");
            final MatViewDefinition definition = engine.getDependentViewGraph().getViewDefinition(viewToken);
            Assert.assertNotNull(definition);
            final String sql = "INSERT INTO base VALUES ('a', 1.5, 10.0, '2024-01-01T00:10:00.000000Z')";
            // writes the file from the view's definition with the SQL replaced, leaving the definition in memory as it is
            final String viewSql = definition.getMatViewSql();
            definition.setMatViewSqlForTesting(sql);
            try (
                    Path path = new Path();
                    BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode())
            ) {
                writer.of(path.of(configuration.getDbRoot()).concat(viewToken).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME).$());
                MatViewDefinition.append(definition, writer);
            } finally {
                definition.setMatViewSqlForTesting(viewSql);
            }
            assertShowCreateSql("counts", "\tts TIMESTAMP,\n\tsym SYMBOL NOCACHE,\n\tcount LONG\n", sql, "ts");
            assertQuery("SELECT count(*) FROM base").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    private static String aliasRequired(String item) {
        return aliasRequired(item, item);
    }

    // The error of a select item without an alias that names the column and quotes the item.
    private static String aliasRequired(String column, String item) {
        return "column '" + column + "' requires an explicit alias. Use: " + item + " AS your_column_name";
    }

    private static String wildcardRefused(String item) {
        return "a select list of a materialized view takes one wildcard, list the columns of " + item + " by name";
    }

    // Asserts that CREATE MATERIALIZED VIEW v AS (query) fails with the message at the first occurrence of
    // offendingText, and creates no view.
    private static void assertCreateFails(String query, String offendingText, String message) throws Exception {
        assertDdlFails("CREATE MATERIALIZED VIEW v AS (" + query + ") PARTITION BY DAY", offendingText, message);
    }

    private static void assertDdlFails(String ddl, String offendingText, String message) throws Exception {
        final int position = ddl.indexOf(offendingText);
        Assert.assertTrue(offendingText, position > -1);
        try {
            execute(ddl);
            Assert.fail("CREATE must fail: " + ddl);
        } catch (SqlException e) {
            TestUtils.assertEquals(ddl, message, e.getFlyweightMessage());
            Assert.assertEquals(ddl, position, e.getPosition());
        }
        Assert.assertNull(engine.getTableTokenIfExists("v"));
    }

    // Asserts that SHOW CREATE prints the view's DDL with the SQL as expected.
    private void assertShowCreateSql(String viewName, String expectedSql) throws Exception {
        assertQuery("SHOW CREATE MATERIALIZED VIEW " + viewName).noLeakCheck().noRandomAccess().returns(
                "ddl\nCREATE MATERIALIZED VIEW '" + viewName + "' WITH BASE 'base' REFRESH IMMEDIATE AS (\n"
                        + expectedSql + "\n) PARTITION BY DAY;\n"
        );
    }

    // Asserts that SHOW CREATE prints the view's DDL with the column list's lines, the SQL and the designated
    // timestamp as expected.
    private void assertShowCreateSql(String viewName, String expectedColumns, String expectedSql, String expectedTimestamp) throws Exception {
        assertQuery("SHOW CREATE MATERIALIZED VIEW " + viewName).noLeakCheck().noRandomAccess().returns(
                "ddl\nCREATE MATERIALIZED VIEW '" + viewName + "' (\n" + expectedColumns + ") WITH BASE 'base' REFRESH IMMEDIATE AS (\n"
                        + expectedSql + "\n) TIMESTAMP(" + expectedTimestamp + ") PARTITION BY DAY;\n"
        );
    }

    private void assertShowCreateReplays(String viewName, String expectedSql) throws Exception {
        assertShowCreateReplays(viewName, expectedSql, "ts, sym");
    }

    // Asserts that SHOW CREATE prints the view's SQL as expected, and that running its output again, after the view
    // is dropped, creates a view with the same columns and rows, which stores the printed SQL, so that SHOW CREATE
    // prints the same again. orderBy orders the view's rows for the comparison.
    private void assertShowCreateReplays(String viewName, String expectedSql, String orderBy) throws Exception {
        final String ddl = "CREATE MATERIALIZED VIEW '" + viewName + "' WITH BASE 'base' REFRESH IMMEDIATE AS (\n"
                + expectedSql + "\n) PARTITION BY DAY;";
        assertShowCreateSql(viewName, expectedSql);
        printSql(viewName + " ORDER BY " + orderBy);
        final String rows = sink.toString();

        execute("DROP MATERIALIZED VIEW " + viewName);
        drainWalAndMatViewQueues();
        execute(ddl);
        drainWalAndMatViewQueues();
        assertQuery(viewName + " ORDER BY " + orderBy).timestamp("ts").expectSize().noLeakCheck().returns(rows);
        // SHOW CREATE prints the SQL on lines of its own, which the view created from its output stores
        assertQuery("SELECT view_sql, view_status FROM materialized_views WHERE view_name = '" + viewName + "'").noLeakCheck().noRandomAccess().returns(
                "view_sql\tview_status\n\n" + expectedSql + "\n\tvalid\n"
        );
        assertQuery("SHOW CREATE MATERIALIZED VIEW " + viewName).noLeakCheck().noRandomAccess().returns("ddl\n" + ddl + "\n");
    }

    // A context whose table reads fail while it compiles a materialized view's SQL under the view's read policy, as
    // SHOW CREATE does: in the optimiser, or, when isGenerationFailing, in the code generator.
    private static SqlExecutionContextImpl newFailingReaderContext(RuntimeException failure, boolean isGenerationFailing) {
        return new SqlExecutionContextImpl(engine, 1) {
            @Override
            public TableReader getReader(TableToken tableToken, long version) {
                if (isGenerationFailing && getExpiryReadPolicy() == ExpiryReadPolicy.REJECT) {
                    throw failure;
                }
                return super.getReader(tableToken, version);
            }

            @Override
            public TableReader getReader(TableToken tableToken) {
                if (!isGenerationFailing && getExpiryReadPolicy() == ExpiryReadPolicy.REJECT) {
                    throw failure;
                }
                return super.getReader(tableToken);
            }
        }.with(AllowAllSecurityContext.INSTANCE);
    }

    private static void createBase() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    // Creates a view the way versions before the explicit name rules did.
    private static void createOlderView(String viewName, String query) throws SqlException {
        createOlderView(viewName, "", query);
    }

    private static void createOlderView(String viewName, String withBase, String query) throws SqlException {
        executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW " + viewName + " " + withBase + " AS (" + query + ") PARTITION BY DAY");
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
