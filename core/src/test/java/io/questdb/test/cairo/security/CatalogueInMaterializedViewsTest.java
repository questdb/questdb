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

package io.questdb.test.cairo.security;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.SqlExecutionRequirements;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Materialized and live views refresh detached from any caller, under a context that sees every
 * object, and every reader of such a view reads what the refresh saw. So they may use the functions
 * and SHOW statements that list objects or read their metadata, see
 * {@link SqlExecutionRequirements#DISCLOSES_OBJECTS}, only where a SYSTEM ADMIN writes them: in their
 * own SQL, never through a regular view, which anyone who may alter it can change after CREATE. A
 * system view, which no user may alter, counts as their own SQL. The refresh compiles the views anew,
 * so it is where that holds.
 * <p>
 * The open-source security contexts see everything, so the tests run as a principal who may create
 * and alter views, but may not see the objects named secret*, and is no SYSTEM ADMIN.
 */
public class CatalogueInMaterializedViewsTest extends AbstractCairoTest {
    // the functions that list objects or read their metadata, see SqlExecutionRequirements.DISCLOSES_OBJECTS
    private static final String[] CATALOGUE_FUNCTIONS = {
            "tables()", "all_tables()", "information_schema.tables()", "information_schema.columns()",
            "information_schema.questdb_columns()", "pg_catalog.pg_class()", "pg_class()",
            "pg_catalog.pg_attribute()", "pg_catalog.pg_attrdef()", "views()", "materialized_views()",
            "live_views()", "wal_tables()", "table_storage()", "reader_pool()", "writer_pool()",
            "table_columns('visible_t')", "table_partitions('visible_t')", "wal_transactions('visible_t')",
            "query_activity()", "export_activity()"
    };
    // the SHOW statements that list objects or read their metadata, and the names errors give them
    private static final String[][] CATALOGUE_STATEMENTS = {
            {"SHOW TABLES", "SHOW TABLES"},
            {"SHOW COLUMNS FROM visible_t", "SHOW COLUMNS"},
            {"SHOW PARTITIONS FROM visible_t", "SHOW PARTITIONS"},
            {"SHOW CREATE TABLE visible_t", "SHOW CREATE TABLE"},
            {"SHOW CREATE VIEW visible_v", "SHOW CREATE VIEW"},
            {"SHOW CREATE MATERIALIZED VIEW visible_mv", "SHOW CREATE MATERIALIZED VIEW"},
            {"SHOW CREATE LIVE VIEW visible_lv", "SHOW CREATE LIVE VIEW"},
            {"SHOW CREATE DATABASE", "SHOW CREATE DATABASE"}
    };
    private static final String MV_SQL = "SELECT t.ts, n.table_name, count() c FROM visible_t t CROSS JOIN names n SAMPLE BY 1d";

    @Test
    public void testCatalogueFromSystemViewRequiresSystemAdmin() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            // no user may alter, replace or drop a system view, so what it lists is fixed at CREATE
            execute("CREATE VIEW 'sys.names' AS (SELECT table_name FROM tables())");
            execute("CREATE VIEW v_from AS (SELECT table_name FROM tables())");
            drainWalAndViewQueues();
            final String sql = "CREATE MATERIALIZED VIEW mv WITH BASE visible_t AS (SELECT t.ts, count() c "
                    + "FROM visible_t t CROSS JOIN 'sys.names' SAMPLE BY 1d) PARTITION BY DAY";
            try (SqlExecutionContext nonAdmin = newContext(new NoSystemAdminSecurityContext())) {
                assertExceptionNoLeakCheck(
                        sql,
                        sql.indexOf("'sys.names'"),
                        "catalogue function from view sys.names cannot be used in materialized view without SYSTEM ADMIN: tables",
                        nonAdmin
                );
                Assert.assertNull(engine.getTableTokenIfExists("mv"));
            }
            // a regular view the materialized view reads next to it still rejects the function outright
            assertFromViewRejected(
                    "FROM visible_t t CROSS JOIN 'sys.names' CROSS JOIN v_from",
                    "v_from",
                    "catalogue function from view v_from",
                    "tables"
            );

            // a SYSTEM ADMIN may, and the refresh keeps compiling what CREATE accepted
            execute(sql);
            drainWalAndMatViewQueues();
            assertMatViewStatus("mv", "valid", "");
            execute("INSERT INTO visible_t VALUES ('2024-01-02', 3, 'b')");
            drainWalAndMatViewQueues();
            assertMatViewStatus("mv", "valid", "");
            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            assertMatViewStatus("mv", "valid", "");
            assertQuery("SELECT count() FROM mv")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n2\n");
        });
    }

    @Test
    public void testCatalogueFromViewIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            execute("CREATE VIEW v_from AS (SELECT table_name FROM tables())");
            execute("CREATE VIEW v_outer AS (SELECT * FROM v_from)");
            execute("CREATE VIEW v_sub AS (SELECT 'x' table_name FROM visible_t WHERE visible_col < (SELECT count() FROM tables()))");
            execute("CREATE VIEW v_select AS (SELECT count() c FROM (SELECT table_partitions('visible_t')))");
            execute("CREATE VIEW v_bare AS (SELECT table_name FROM tables)");
            execute("CREATE VIEW v_show AS (SELECT * FROM (SHOW TABLES))");
            execute("CREATE VIEW v_show_sub AS (SELECT 'x' table_name FROM visible_t WHERE visible_col < (SELECT count() FROM (SHOW COLUMNS FROM visible_t)))");
            drainWalAndViewQueues();
            // not even a SYSTEM ADMIN may: the definition of a view may change after CREATE
            assertFromViewRejected("FROM visible_t t CROSS JOIN v_from", "v_from", "catalogue function from view v_from", "tables");
            assertFromViewRejected("FROM visible_t t CROSS JOIN v_outer", "v_outer", "catalogue function from view v_outer", "tables");
            assertFromViewRejected("FROM visible_t t CROSS JOIN v_sub", "v_sub", "catalogue function from view v_sub", "tables");
            assertFromViewRejected("FROM visible_t t CROSS JOIN v_select", "v_select", "catalogue function from view v_select", "table_partitions");
            assertFromViewRejected("FROM visible_t t CROSS JOIN v_bare", "v_bare", "catalogue function from view v_bare", "tables");
            assertFromViewRejected("FROM visible_t t CROSS JOIN v_show", "v_show", "catalogue statement from view v_show", "SHOW TABLES");
            assertFromViewRejected("FROM visible_t t CROSS JOIN v_show_sub", "v_show_sub", "catalogue statement from view v_show_sub", "SHOW COLUMNS");
            // a view in a sub-query of the materialized view
            assertFromViewRejected(
                    "FROM visible_t t WHERE visible_col < (SELECT count() FROM v_from)",
                    "v_from",
                    "catalogue function from view v_from",
                    "tables"
            );
            // the functions and statements the materialized view writes itself are its own
            execute("CREATE VIEW v_plain AS (SELECT ts, visible_col FROM visible_t)");
            drainWalAndViewQueues();
            execute("CREATE MATERIALIZED VIEW mv WITH BASE visible_t AS (SELECT ts, count() c FROM v_plain "
                    + "WHERE visible_col < (SELECT count() FROM tables()) SAMPLE BY 1d) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertMatViewStatus("mv", "valid", "");
        });
    }

    @Test
    public void testCatalogueFunctionsRequireSystemAdmin() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext nonAdmin = newContext(new NoSystemAdminSecurityContext())) {
                // also files() and glob(), which list the files of the database root
                final String[] functions = new String[CATALOGUE_FUNCTIONS.length + 2];
                System.arraycopy(CATALOGUE_FUNCTIONS, 0, functions, 0, CATALOGUE_FUNCTIONS.length);
                functions[CATALOGUE_FUNCTIONS.length] = "files('" + root + "')";
                functions[CATALOGUE_FUNCTIONS.length + 1] = "glob('" + root + "/*')";
                for (String function : functions) {
                    final String sql = "CREATE MATERIALIZED VIEW mv WITH BASE visible_t AS (SELECT t.ts, count() c "
                            + "FROM visible_t t CROSS JOIN " + function + " SAMPLE BY 1d) PARTITION BY DAY";
                    final String name = function.substring(0, function.indexOf('('));
                    // NoSystemAdminSecurityContext.isSystemAdmin() is true, as is that of the built-in admin who
                    // assumed a service account: authorizeSystemAdmin() decides
                    assertExceptionNoLeakCheck(
                            sql,
                            sql.indexOf(function),
                            "catalogue function cannot be used in materialized view without SYSTEM ADMIN: " + name,
                            nonAdmin
                    );
                    Assert.assertNull(function, engine.getTableTokenIfExists("mv"));

                    execute(sql);
                    drainWalAndMatViewQueues();
                    assertMatViewStatus("mv", "valid", "");
                    execute("DROP MATERIALIZED VIEW mv");
                    drainWalAndMatViewQueues();
                }
            }
        });
    }

    @Test
    public void testCatalogueStatementsRequireSystemAdmin() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext nonAdmin = newContext(new NoSystemAdminSecurityContext())) {
                for (String[] statement : CATALOGUE_STATEMENTS) {
                    final String sql = "CREATE MATERIALIZED VIEW mv WITH BASE visible_t AS (SELECT ts, count() c FROM visible_t "
                            + "WHERE visible_col < (SELECT count() FROM (" + statement[0] + ")) SAMPLE BY 1d) PARTITION BY DAY";
                    assertExceptionNoLeakCheck(
                            sql,
                            sql.indexOf(statement[0]),
                            "catalogue statement cannot be used in materialized view without SYSTEM ADMIN: " + statement[1],
                            nonAdmin
                    );
                    Assert.assertNull(statement[0], engine.getTableTokenIfExists("mv"));

                    execute(sql);
                    drainWalAndMatViewQueues();
                    assertMatViewStatus("mv", "valid", "");
                    execute("DROP MATERIALIZED VIEW mv");
                    drainWalAndMatViewQueues();
                }
                // the statements that list no objects need no SYSTEM ADMIN
                for (String statement : new String[]{"SHOW TIME ZONE", "SHOW SERVER_VERSION", "SHOW PARAMETERS"}) {
                    engine.execute("CREATE MATERIALIZED VIEW mv WITH BASE visible_t AS (SELECT ts, count() c FROM visible_t "
                            + "WHERE visible_col < (SELECT count() FROM (" + statement + ")) SAMPLE BY 1d) PARTITION BY DAY", nonAdmin);
                    drainWalAndMatViewQueues();
                    assertMatViewStatus("mv", "valid", "");
                    execute("DROP MATERIALIZED VIEW mv");
                    drainWalAndMatViewQueues();
                }
            }
        });
    }

    @Test
    public void testIncrementalRefreshRejectsCatalogueFunctionFromAlteredView() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext hidingContext = newContext(new HidingNonAdminSecurityContext())) {
                createViewAndMatView(hidingContext);
                engine.execute("ALTER VIEW names AS (SELECT table_name FROM tables())", hidingContext);
                drainWalAndViewQueues();
                execute("INSERT INTO visible_t VALUES ('2024-01-02', 3, 'b')");
                drainWalAndMatViewQueues();
                assertRefreshRejected(hidingContext, "catalogue function from view names cannot be used in materialized view: tables");
            }
        });
    }

    @Test
    public void testLiveViewCatalogueRequiresSystemAdmin() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext nonAdmin = newContext(new NoSystemAdminSecurityContext())) {
                final String select = "SELECT ts, visible_col, count(*) OVER (PARTITION BY g ORDER BY ts "
                        + "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rn FROM visible_t ";
                final String[][] filters = {
                        {"WHERE g IN (SELECT table_name FROM tables())", "tables()", "catalogue function cannot be used in live view without SYSTEM ADMIN: tables"},
                        {"WHERE visible_col < (SELECT count() FROM (SHOW TABLES))", "SHOW TABLES", "catalogue statement cannot be used in live view without SYSTEM ADMIN: SHOW TABLES"}
                };
                for (String[] filter : filters) {
                    final String selectSql = select + filter[0];
                    // CREATE LIVE VIEW compiles the SELECT on its own, so the position is relative to it
                    assertExceptionNoLeakCheck(
                            "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + selectSql,
                            selectSql.indexOf(filter[1]),
                            filter[2],
                            nonAdmin
                    );
                    Assert.assertNull(filter[0], engine.getTableTokenIfExists("lv"));

                    execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + selectSql);
                    drainWalAndViewQueues();
                    Assert.assertNotNull(filter[0], engine.getTableTokenIfExists("lv"));
                    execute("DROP LIVE VIEW lv");
                    drainWalAndViewQueues();
                }
            }
        });
    }

    @Test
    public void testRefreshRejectsCatalogueFunctionFromAlteredView() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext hidingContext = newContext(new HidingNonAdminSecurityContext())) {
                createViewAndMatView(hidingContext);
                engine.execute("ALTER VIEW names AS (SELECT table_name FROM tables())", hidingContext);
                drainWalAndViewQueues();
                engine.execute("REFRESH MATERIALIZED VIEW mvn FULL", hidingContext);
                drainWalAndMatViewQueues();
                assertRefreshRejected(hidingContext, "catalogue function from view names cannot be used in materialized view: tables");
            }
        });
    }

    @Test
    public void testRefreshRejectsCatalogueFunctionFromNestedView() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext hidingContext = newContext(new HidingNonAdminSecurityContext())) {
                engine.execute("CREATE VIEW inner_names AS (SELECT 'none' AS table_name)", hidingContext);
                engine.execute("CREATE VIEW names AS (SELECT table_name FROM inner_names)", hidingContext);
                drainWalAndViewQueues();
                createMatView(hidingContext);
                // the view the materialized view reads stays as it was, the one nested in it changes
                engine.execute("ALTER VIEW inner_names AS (SELECT table_name FROM tables())", hidingContext);
                drainWalAndViewQueues();
                engine.execute("REFRESH MATERIALIZED VIEW mvn FULL", hidingContext);
                drainWalAndMatViewQueues();
                assertRefreshRejected(hidingContext, "catalogue function from view names cannot be used in materialized view: tables");
            }
        });
    }

    @Test
    public void testRefreshRejectsCatalogueFunctionFromRecreatedView() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext hidingContext = newContext(new HidingNonAdminSecurityContext())) {
                createViewAndMatView(hidingContext);
                // the materialized view names the view, so it reads whichever view has that name
                engine.execute("DROP VIEW names", hidingContext);
                engine.execute("CREATE VIEW names AS (SELECT table_name FROM tables())", hidingContext);
                drainWalAndViewQueues();
                engine.execute("REFRESH MATERIALIZED VIEW mvn FULL", hidingContext);
                drainWalAndMatViewQueues();
                assertRefreshRejected(hidingContext, "catalogue function from view names cannot be used in materialized view: tables");
            }
        });
    }

    @Test
    public void testRefreshRejectsCatalogueFunctionFromReplacedView() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext hidingContext = newContext(new HidingNonAdminSecurityContext())) {
                createViewAndMatView(hidingContext);
                engine.execute("CREATE OR REPLACE VIEW names AS (SELECT table_name FROM tables())", hidingContext);
                drainWalAndViewQueues();
                engine.execute("REFRESH MATERIALIZED VIEW mvn FULL", hidingContext);
                drainWalAndMatViewQueues();
                assertRefreshRejected(hidingContext, "catalogue function from view names cannot be used in materialized view: tables");
            }
        });
    }

    @Test
    public void testRefreshRejectsCatalogueFunctionWithoutParenthesesFromView() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext hidingContext = newContext(new HidingNonAdminSecurityContext())) {
                createViewAndMatView(hidingContext);
                // a name that no table has resolves to the function of that name
                engine.execute("ALTER VIEW names AS (SELECT table_name FROM tables)", hidingContext);
                drainWalAndViewQueues();
                engine.execute("REFRESH MATERIALIZED VIEW mvn FULL", hidingContext);
                drainWalAndMatViewQueues();
                assertRefreshRejected(hidingContext, "catalogue function from view names cannot be used in materialized view: tables");
            }
        });
    }

    @Test
    public void testRefreshRejectsCatalogueStatementFromAlteredView() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext hidingContext = newContext(new HidingNonAdminSecurityContext())) {
                createViewAndMatView(hidingContext);
                engine.execute("ALTER VIEW names AS (SELECT table_name FROM (SHOW TABLES))", hidingContext);
                drainWalAndViewQueues();
                engine.execute("REFRESH MATERIALIZED VIEW mvn FULL", hidingContext);
                drainWalAndMatViewQueues();
                assertRefreshRejected(hidingContext, "catalogue statement from view names cannot be used in materialized view: SHOW TABLES");
            }
        });
    }

    @Test
    public void testSystemAdminCatalogueFunctionListsEveryObject() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            // A SYSTEM ADMIN who writes a catalogue function in a materialized view opts in to what its refresh
            // lists, protected objects included: SELECT on the view is what controls who reads that.
            execute("CREATE MATERIALIZED VIEW mv_all AS (SELECT t.ts, n.table_name, count() c "
                    + "FROM visible_t t CROSS JOIN tables() n SAMPLE BY 1d) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertMatViewStatus("mv_all", "valid", "");
            try (SqlExecutionContext hidingContext = newContext(new HidingNonAdminSecurityContext())) {
                final StringSink sink = new StringSink();
                engine.print("SELECT DISTINCT table_name FROM mv_all", sink, hidingContext);
                Assert.assertTrue(sink.toString(), Chars.contains(sink, "secret_t"));
                engine.print("SELECT table_name FROM tables()", sink, hidingContext);
                Assert.assertFalse(sink.toString(), Chars.contains(sink, "secret_t"));
            }
        });
    }

    private static void assertFromViewRejected(String from, String viewName, String expectedPrefix, String name) throws Exception {
        final String sql = "CREATE MATERIALIZED VIEW mv WITH BASE visible_t AS (SELECT t.ts, count() c " + from
                + " SAMPLE BY 1d) PARTITION BY DAY";
        // reported at the name of the view the materialized view reads, its own SQL is in the view
        assertExceptionNoLeakCheck(
                sql,
                sql.indexOf(viewName),
                expectedPrefix + " cannot be used in materialized view: " + name,
                sqlExecutionContext
        );
        Assert.assertNull(from, engine.getTableTokenIfExists("mv"));
    }

    private static void createMatView(SqlExecutionContext context) throws Exception {
        engine.execute("CREATE MATERIALIZED VIEW mvn AS (" + MV_SQL + ") PARTITION BY DAY", context);
        drainWalAndMatViewQueues();
    }

    private static void createObjects() throws Exception {
        execute("CREATE TABLE visible_t (ts TIMESTAMP, visible_col INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE secret_t (ts TIMESTAMP, secret_col INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO visible_t VALUES ('2024-01-01', 2, 'a')");
        execute("CREATE VIEW visible_v AS (SELECT ts, visible_col FROM visible_t)");
        execute("CREATE MATERIALIZED VIEW visible_mv AS (SELECT ts, max(visible_col) mx FROM visible_t SAMPLE BY 1d) PARTITION BY DAY");
        execute("CREATE LIVE VIEW visible_lv FLUSH EVERY 1s START FROM NOW AS "
                + "SELECT ts, visible_col, count(*) OVER (PARTITION BY visible_col ORDER BY ts "
                + "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rn FROM visible_t");
        drainWalAndMatViewQueues();
        drainWalAndViewQueues();
    }

    // the materialized view of the review that reported the leak: it reads a view that lists nothing yet
    private static void createViewAndMatView(SqlExecutionContext context) throws Exception {
        engine.execute("CREATE VIEW names AS (SELECT 'none' AS table_name)", context);
        drainWalAndViewQueues();
        createMatView(context);
    }

    private static SqlExecutionContext newContext(SecurityContext securityContext) {
        return new SqlExecutionContextImpl(engine, 1).with(securityContext, bindVariableService, null, -1, null);
    }

    private void assertMatViewStatus(String viewName, String status, String invalidationReason) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\n" + status + '\t' + invalidationReason + '\n');
    }

    // The refresh invalidates the materialized view, rather than store what the view now lists: the objects
    // the principal who changed the view may not see among them.
    private void assertRefreshRejected(SqlExecutionContext hidingContext, String expectedReason) throws Exception {
        assertMatViewStatus("mvn", "invalid", "[" + MV_SQL.indexOf("names") + "]: " + expectedReason);
        final StringSink sink = new StringSink();
        engine.print("SELECT DISTINCT table_name FROM mvn", sink, hidingContext);
        Assert.assertFalse(sink.toString(), Chars.contains(sink, "secret"));
    }

    // may create and alter views and materialized views, but may not see the objects named secret*
    // and is no SYSTEM ADMIN
    private static final class HidingNonAdminSecurityContext extends AllowAllSecurityContext {
        @Override
        public void authorizeSystemAdmin() {
            throw CairoException.authorization().put("system admin required");
        }

        @Override
        public boolean isTableVisible(TableToken tableToken) {
            return !Chars.startsWithIgnoreCase(tableToken.getTableName(), "secret");
        }

        @Override
        protected SecurityContext newPrincipalContext(CharSequence principal) {
            return this;
        }
    }
}
