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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.ShowPartitionsRecordCursorFactory;
import io.questdb.std.Chars;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Covers {@link SecurityContext#isTableVisible(TableToken)} plumbing: catalogue queries must not
 * list an object the principal may not see, and statements that name one must fail exactly like
 * they do for a missing object. The open-source security contexts see everything, so the tests
 * run under a context that hides every object whose name starts with "secret".
 */
public class TableVisibilityTest extends AbstractCairoTest {
    // catalogue queries that list objects or their columns by name
    private static final String[] NAMED_CATALOGUE_QUERIES = {
            "tables()",
            "all_tables()",
            "SHOW TABLES",
            "information_schema.tables()",
            "information_schema.columns()",
            "information_schema.questdb_columns()",
            "pg_catalog.pg_class()",
            "pg_catalog.pg_attribute()",
            "views()",
            "materialized_views()",
            "live_views()",
            "wal_tables()",
            "table_storage()",
            "SHOW CREATE DATABASE"
    };

    @Test
    public void testCatalogueFunctionsAreRejectedInMaterializedViews() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            // A materialized view refreshes detached from any caller, under a context that sees every
            // object, and every reader of the view would read what that context saw.
            final String[] functions = {
                    "tables()", "all_tables()", "information_schema.tables()", "information_schema.columns()",
                    "information_schema.questdb_columns()", "pg_catalog.pg_class()", "pg_class()",
                    "pg_catalog.pg_attribute()", "pg_catalog.pg_attrdef()", "views()", "materialized_views()",
                    "live_views()", "wal_tables()", "table_storage()", "reader_pool()", "writer_pool()",
                    "table_columns('visible_t')", "table_partitions('visible_t')", "wal_transactions('visible_t')",
                    "query_activity()", "export_activity()", "files('" + root + "')", "glob('" + root + "/*')"
            };
            for (String function : functions) {
                final String sql = "CREATE MATERIALIZED VIEW mv_catalogue AS (SELECT v.ts, count() c FROM visible_t v CROSS JOIN "
                        + function + " SAMPLE BY 1d) PARTITION BY DAY";
                final String failure = executionFailureOf(sql, sqlExecutionContext);
                TestUtils.assertContains(function + ": " + failure, failure, "function cannot be used in materialized view");
                Assert.assertNull(engine.getTableTokenIfExists("mv_catalogue"));
            }
        });
    }

    @Test
    public void testCatalogueQueriesHideInvisibleObjects() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                final StringSink hidden = new StringSink();
                final StringSink all = new StringSink();
                for (String sql : NAMED_CATALOGUE_QUERIES) {
                    engine.print(sql, all, sqlExecutionContext);
                    Assert.assertTrue(sql + " must list the hidden objects to allow-all\n" + all, Chars.contains(all, "secret"));

                    engine.print(sql, hidden, hidingContext);
                    Assert.assertFalse(sql + " must not list hidden objects\n" + hidden, Chars.contains(hidden, "secret"));
                    Assert.assertTrue(sql + " must list the visible objects\n" + hidden, Chars.contains(hidden, "visible"));
                }
            }
        });
    }

    @Test
    public void testCatalogueQueryVisibilityIsDecidedPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            final StringSink sink = new StringSink();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                for (String sql : NAMED_CATALOGUE_QUERIES) {
                    // a factory compiled for one principal can be reused for another through the select caches
                    try (RecordCursorFactory factory = select(sql)) {
                        print(factory, sqlExecutionContext, sink);
                        Assert.assertTrue(sql + '\n' + sink, Chars.contains(sink, "secret"));

                        print(factory, hidingContext, sink);
                        Assert.assertFalse(sql + '\n' + sink, Chars.contains(sink, "secret"));

                        print(factory, sqlExecutionContext, sink);
                        Assert.assertTrue(sql + '\n' + sink, Chars.contains(sink, "secret"));
                    }
                }
            }
        });
    }

    @Test
    public void testCopyToFailsLikeMissingTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE secret_t (x INT)");
            Assert.assertNotNull(engine.getTableTokenIfExists("secret_t"));
            Assert.assertNull(engine.getTableTokenIfExists(missingNameOf("secret_t")));
            try (SqlExecutionContext hidingContext = new SqlExecutionContextImpl(engine, 1).with(new HidingSecurityContext() {
                @Override
                public void authorizeSelectOnAnyColumn(TableToken tableToken) {
                    throw CairoException.authorization().put("select denied");
                }
            })) {
                final String sql = "COPY %s TO 'out' WITH FORMAT PARQUET";
                TestUtils.assertContains(failureOf(String.format(sql, missingNameOf("secret_t")), hidingContext), "table does not exist");
                assertMaskedLikeMissing(sql, "secret_t", hidingContext);
            }
        });
    }

    @Test
    public void testCursorSizeDoesNotDiscloseInvisibleObjects() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                for (String sql : NAMED_CATALOGUE_QUERIES) {
                    // count() answers from the cursor size when the cursor knows it, so a size that
                    // counted the hidden objects would disclose how many there are
                    try (
                            RecordCursorFactory factory = select(sql, hidingContext);
                            RecordCursor cursor = factory.getCursor(hidingContext)
                    ) {
                        final long size = cursor.size();
                        long rowCount = 0;
                        while (cursor.hasNext()) {
                            rowCount++;
                        }
                        if (size != -1) {
                            Assert.assertEquals(sql, rowCount, size);
                        }
                    }
                }
                final StringSink sink = new StringSink();
                engine.print("SELECT count() FROM tables() WHERE table_name LIKE 'visible%'", sink, hidingContext);
                assertQuery("SELECT count() FROM tables()")
                        .withContext(hidingContext)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(sink.toString());
            }
        });
    }

    @Test
    public void testDropAllLeavesInvisibleObjects() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext noDropContext = new SqlExecutionContextImpl(engine, 1).with(new HidingNoDropSecurityContext())) {
                // the failure report names every object the drop could not remove, and must name only
                // those the principal may see
                final String failure = executionFailureOf("DROP ALL", noDropContext);
                TestUtils.assertContains(failure, "visible_t");
                Assert.assertFalse(failure, Chars.contains(failure, "secret"));
            }
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                engine.execute("DROP ALL", hidingContext);
            }
            drainWalAndMatViewQueues();
            for (String name : new String[]{"visible_t", "visible_nw", "visible_v", "visible_mv", "visible_lv"}) {
                Assert.assertNull(name, engine.getTableTokenIfExists(name));
            }
            assertHiddenObjectsIntact();
        });
    }

    @Test
    public void testDropStatementsFailLikeMissingObjects() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                assertExecutionMaskedLikeMissing("DROP TABLE %s", "secret_t", hidingContext);
                // the kind checks must not disclose the object either
                assertExecutionMaskedLikeMissing("DROP TABLE %s", "secret_v", hidingContext);
                assertExecutionMaskedLikeMissing("DROP TABLE %s", "secret_mv", hidingContext);
                assertExecutionMaskedLikeMissing("DROP TABLE %s", "secret_lv", hidingContext);
                assertExecutionMaskedLikeMissing("DROP VIEW %s", "secret_v", hidingContext);
                assertExecutionMaskedLikeMissing("DROP VIEW %s", "secret_t", hidingContext);
                assertExecutionMaskedLikeMissing("DROP MATERIALIZED VIEW %s", "secret_mv", hidingContext);
                assertExecutionMaskedLikeMissing("DROP MATERIALIZED VIEW %s", "secret_t", hidingContext);
                assertExecutionMaskedLikeMissing("DROP LIVE VIEW %s", "secret_lv", hidingContext);
                assertExecutionMaskedLikeMissing("DROP LIVE VIEW %s", "secret_t", hidingContext);
            }
            assertHiddenObjectsIntact();
        });
    }

    @Test
    public void testFilesRequiresSystemAdmin() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(new NoSystemAdminSecurityContext())) {
                assertAuthorizationFailure("SELECT * FROM files('" + root + "')", context, "system admin required");
            }
        });
    }

    @Test
    public void testHydrateTableMetadataRequiresSystemAdminBeforeResolvingNames() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(new NoSystemAdminSecurityContext())) {
                // compilation must not tell an existing table from a missing one before it authorizes, and
                // the denial is an authorization error, not a fault of the function factory
                assertAuthorizationFailure("SELECT hydrate_table_metadata('secret_t')", context, "system admin required");
                assertAuthorizationFailure("SELECT hydrate_table_metadata('missing_t')", context, "system admin required");
            }
        });
    }

    @Test
    public void testIfExistsStatementsIgnoreInvisibleObjects() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                final String[] statements = {
                        "DROP TABLE IF EXISTS %s", "DROP VIEW IF EXISTS %s", "DROP MATERIALIZED VIEW IF EXISTS %s",
                        "DROP LIVE VIEW IF EXISTS %s", "TRUNCATE TABLE IF EXISTS %s"
                };
                final String[] hiddenNames = {"secret_t", "secret_v", "secret_mv", "secret_lv"};
                for (String statement : statements) {
                    for (String hiddenName : hiddenNames) {
                        // does nothing, exactly like it does for a missing object
                        engine.execute(String.format(statement, "missing_object"), hidingContext);
                        engine.execute(String.format(statement, hiddenName), hidingContext);
                    }
                }
            }
            drainWalAndMatViewQueues();
            assertHiddenObjectsIntact();
        });
    }

    @Test
    public void testPgAttrDefHidesInvisibleTables() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            final int secretTableId = engine.verifyTableName("secret_t").getTableId();
            final int visibleTableId = engine.verifyTableName("visible_t").getTableId();
            final String secretRows = "SELECT count() FROM pg_catalog.pg_attrdef() WHERE adrelid = " + secretTableId;
            final String visibleRows = "SELECT count() FROM pg_catalog.pg_attrdef() WHERE adrelid = " + visibleTableId;
            assertQuery(secretRows).noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                assertQuery(secretRows).withContext(hidingContext).noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
                assertQuery(visibleRows).withContext(hidingContext).noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
            }
        });
    }

    @Test
    public void testPoolsRequireSystemAdmin() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            // the pools list every table with a pooled reader or writer, so only admins may see them
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(new NoSystemAdminSecurityContext())) {
                assertAuthorizationFailure("SELECT * FROM reader_pool()", context, "system admin required");
                assertAuthorizationFailure("SELECT * FROM writer_pool()", context, "system admin required");
            }
            final StringSink sink = new StringSink();
            engine.print("SELECT table_name FROM reader_pool()", sink, sqlExecutionContext);
            TestUtils.assertContains(sink, "secret_t");
            engine.print("SELECT table_name FROM writer_pool()", sink, sqlExecutionContext);
            TestUtils.assertContains(sink, "secret_nw");
        });
    }

    @Test
    public void testReplaceViewDoesNotReplaceInvisibleView() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            final TableToken viewToken = engine.verifyTableName("secret_v");
            final String viewSql = engine.getViewGraph().getViewDefinition(viewToken).getViewSql();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                // the name collides like any taken name does, rather than replacing a view the principal
                // may not see
                final String failure = executionFailureOf("CREATE OR REPLACE VIEW secret_v AS (SELECT ts FROM visible_t)", hidingContext);
                TestUtils.assertContains(failure, "already exists");
            }
            Assert.assertEquals(viewToken, engine.verifyTableName("secret_v"));
            TestUtils.assertEquals(viewSql, engine.getViewGraph().getViewDefinition(viewToken).getViewSql());
        });
    }

    @Test
    public void testSelectListTableNameFunctionsNextToViewsFailLikeMissingTables() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            execute("CREATE VIEW visible_proj AS (SELECT count() c FROM (SELECT table_partitions('secret_t')))");
            execute("CREATE VIEW visible_proj_outer AS (SELECT * FROM visible_proj)");
            drainWalAndViewQueues();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                // a table-name function in the caller's own select list is read as the caller, also when
                // the caller reads a view
                final String[] callerTemplates = {
                        "SELECT table_partitions('%s')",
                        "SELECT table_partitions('%s') FROM visible_v",
                        "SELECT table_partitions('%s') FROM (visible_v)",
                        "SELECT count() FROM (SELECT table_partitions('%s') FROM visible_v)",
                        "SELECT table_partitions('%s') FROM visible_t CROSS JOIN visible_v",
                        "SELECT table_partitions('%s') FROM visible_v CROSS JOIN visible_t",
                        "SELECT c, table_partitions('%s') FROM visible_proj",
                        "SELECT count() FROM (SELECT table_partitions('%s') FROM visible_proj_outer)",
                        "WITH w AS (SELECT * FROM visible_v) SELECT table_partitions('%s') FROM w",
                        "SELECT * FROM visible_v WHERE visible_col > (SELECT count() FROM (SELECT table_partitions('%s') FROM visible_v))",
                        "SELECT count() FROM visible_v v CROSS JOIN LATERAL (SELECT table_partitions('%s') FROM visible_t t WHERE t.ts = v.ts)",
                        "visible_proj UNION ALL SELECT count() FROM (SELECT table_partitions('%s'))",
                        "SELECT * FROM visible_proj UNION ALL SELECT count() FROM (SELECT table_partitions('%s'))",
                        "SELECT * FROM (visible_proj UNION ALL SELECT count() FROM (SELECT table_partitions('%s')))",
                };
                for (String template : callerTemplates) {
                    assertMaskedLikeMissing(template, "secret_t", hidingContext);
                    // the same statement reads the table for a principal who may see it
                    try (
                            RecordCursorFactory factory = select(String.format(template, "secret_t"));
                            RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                    ) {
                        Assert.assertTrue(template, cursor.hasNext());
                    }
                }
            }
        });
    }

    @Test
    public void testShowStatementsFailLikeMissingObjects() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                assertMaskedLikeMissing("SHOW CREATE TABLE %s", "secret_t", hidingContext);
                assertMaskedLikeMissing("SHOW CREATE TABLE %s", "secret_v", hidingContext);
                assertMaskedLikeMissing("SHOW CREATE TABLE %s", "secret_mv", hidingContext);
                assertMaskedLikeMissing("SHOW CREATE VIEW %s", "secret_v", hidingContext);
                assertMaskedLikeMissing("SHOW CREATE VIEW %s", "secret_t", hidingContext);
                assertMaskedLikeMissing("SHOW CREATE MATERIALIZED VIEW %s", "secret_mv", hidingContext);
                assertMaskedLikeMissing("SHOW CREATE MATERIALIZED VIEW %s", "secret_t", hidingContext);
                assertMaskedLikeMissing("SHOW CREATE LIVE VIEW %s", "secret_lv", hidingContext);
                assertMaskedLikeMissing("SHOW COLUMNS FROM %s", "secret_t", hidingContext);
                assertMaskedLikeMissing("SHOW PARTITIONS FROM %s", "secret_t", hidingContext);
                assertMaskedLikeMissing("SELECT * FROM table_columns('%s')", "secret_t", hidingContext);
                assertMaskedLikeMissing("SELECT * FROM table_partitions('%s')", "secret_t", hidingContext);
                assertMaskedLikeMissing("SELECT * FROM wal_transactions('%s')", "secret_t", hidingContext);
                assertMaskedLikeMissing("SELECT wait_wal_table('%s')", "secret_t", hidingContext);
            }
        });
    }

    @Test
    public void testShowStatementsRecheckVisibilityPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            final StringSink sink = new StringSink();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                // factories compiled for a principal who may see the object, executed for one who may not
                assertCursorFails("SHOW CREATE TABLE secret_t", hidingContext, "table does not exist [table=secret_t]", sink);
                assertCursorFails("SHOW CREATE VIEW secret_v", hidingContext, "view does not exist [view=secret_v]", sink);
                assertCursorFails("SHOW CREATE MATERIALIZED VIEW secret_mv", hidingContext, "materialized view does not exist [view=secret_mv]", sink);
                assertCursorFails("SHOW CREATE LIVE VIEW secret_lv", hidingContext, "live view does not exist [view=secret_lv]", sink);
                assertCursorFails("SHOW COLUMNS FROM secret_t", hidingContext, "table does not exist [table=secret_t]", sink);
                assertCursorFails("SHOW PARTITIONS FROM secret_t", hidingContext, "table does not exist [table=secret_t]", sink);
                assertCursorFails("SELECT * FROM wal_transactions('secret_t')", hidingContext, "table does not exist: secret_t", sink);
            }
        });
    }

    @Test
    public void testStatementsModifyingInvisibleObjectsFailLikeMissingObjects() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                assertMaskedLikeMissing("ALTER TABLE %s ADD COLUMN c INT", "secret_t", hidingContext);
                // a column that does not exist must not fail differently from one that does
                assertMaskedLikeMissing("ALTER TABLE %s DROP COLUMN no_such_col", "secret_t", hidingContext);
                assertMaskedLikeMissing("ALTER TABLE %s DROP COLUMN secret_col", "secret_t", hidingContext);
                assertMaskedLikeMissing("ALTER TABLE %s ADD COLUMN c INT", "secret_v", hidingContext);
                assertMaskedLikeMissing("ALTER VIEW %s AS (SELECT ts, visible_col FROM visible_t)", "secret_v", hidingContext);
                assertMaskedLikeMissing("ALTER MATERIALIZED VIEW %s SET REFRESH LIMIT 1 HOUR", "secret_mv", hidingContext);
                assertMaskedLikeMissing("INSERT INTO %s VALUES ('2024-01-02', 1)", "secret_t", hidingContext);
                assertMaskedLikeMissing("INSERT INTO %s (no_such_col) VALUES (1)", "secret_t", hidingContext);
                assertMaskedLikeMissing("INSERT INTO %s SELECT * FROM visible_t", "secret_t", hidingContext);
                assertMaskedLikeMissing("UPDATE %s SET secret_col = 1", "secret_t", hidingContext);
                assertMaskedLikeMissing("VACUUM TABLE %s", "secret_t", hidingContext);
                // LIKE would copy the schema of the table into one the principal may see
                assertMaskedLikeMissing("CREATE TABLE copy_t (LIKE %s)", "secret_t", hidingContext);
                assertMaskedLikeMissing("INSERT INTO %s VALUES ('2024-01-02', 1)", "secret_v", hidingContext);
                assertMaskedLikeMissing("UPDATE %s SET visible_col = 1", "secret_v", hidingContext);
                assertExecutionMaskedLikeMissing("TRUNCATE TABLE %s", "secret_t", hidingContext);
                assertExecutionMaskedLikeMissing("TRUNCATE TABLE visible_t, %s", "secret_t", hidingContext);
                assertExecutionMaskedLikeMissing("RENAME TABLE %s TO renamed_t", "secret_t", hidingContext);
                assertExecutionMaskedLikeMissing("RENAME TABLE %s TO renamed_t", "secret_v", hidingContext);
                assertExecutionMaskedLikeMissing("REFRESH MATERIALIZED VIEW %s FULL", "secret_mv", hidingContext);
                assertExecutionMaskedLikeMissing("REFRESH MATERIALIZED VIEW %s FULL", "secret_t", hidingContext);
                assertExecutionMaskedLikeMissing("COMPILE VIEW %s", "secret_v", hidingContext);
                assertExecutionMaskedLikeMissing("COMPILE VIEW %s", "secret_t", hidingContext);
                assertExecutionMaskedLikeMissing("ALTER LIVE VIEW %s SUSPEND WAL", "secret_lv", hidingContext);
                // new objects must not read the schema of objects the principal may not see
                assertExecutionMaskedLikeMissing("CREATE TABLE t_new AS (SELECT * FROM %s)", "secret_t", hidingContext);
                assertExecutionMaskedLikeMissing("CREATE VIEW v_new AS (SELECT * FROM %s)", "secret_t", hidingContext);
                assertExecutionMaskedLikeMissing("CREATE VIEW v_new AS (SELECT * FROM %s)", "secret_v", hidingContext);
                assertExecutionMaskedLikeMissing(
                        "CREATE MATERIALIZED VIEW mv_new AS (SELECT ts, max(secret_col) mx FROM %s SAMPLE BY 1d) PARTITION BY DAY",
                        "secret_t",
                        hidingContext
                );
                // the base table of a new view must not disclose what kind of object it is either
                assertExecutionMaskedLikeMissing(
                        "CREATE MATERIALIZED VIEW mv_new WITH BASE %s AS (SELECT ts, max(visible_col) mx FROM visible_t SAMPLE BY 1d) PARTITION BY DAY",
                        "secret_v",
                        hidingContext
                );
                assertExecutionMaskedLikeMissing(
                        "CREATE LIVE VIEW lv_new FLUSH EVERY 1s START FROM NOW AS SELECT ts, secret_col FROM %s",
                        "secret_t",
                        hidingContext
                );
                assertExecutionMaskedLikeMissing(
                        "CREATE LIVE VIEW lv_new FLUSH EVERY 1s START FROM NOW AS SELECT ts, visible_col FROM %s",
                        "secret_lv",
                        hidingContext
                );
                // the ORDER BY check of a live view window names the designated timestamp of its base table
                assertExecutionMaskedLikeMissing(
                        "CREATE LIVE VIEW lv_new FLUSH EVERY 1s START FROM NOW AS SELECT ts, secret_col, count(*) OVER w AS c FROM %s "
                                + "WINDOW w AS (PARTITION BY secret_col ORDER BY secret_col ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
                        "secret_t",
                        hidingContext
                );
            }
            assertHiddenObjectsIntact();
        });
    }

    @Test
    public void testStatementsReadingInvisibleObjectsFailLikeMissingObjects() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                assertMaskedLikeMissing("SELECT * FROM %s", "secret_t", hidingContext);
                assertMaskedLikeMissing("SELECT * FROM %s", "SeCrEt_T", hidingContext);
                // a column that does not exist must not fail differently from one that does
                assertMaskedLikeMissing("SELECT no_such_col FROM %s", "secret_t", hidingContext);
                assertMaskedLikeMissing("SELECT secret_col FROM %s", "secret_t", hidingContext);
                assertMaskedLikeMissing("SELECT * FROM visible_t CROSS JOIN %s", "secret_t", hidingContext);
                assertMaskedLikeMissing("SELECT * FROM visible_t WHERE ts IN (SELECT ts FROM %s)", "secret_t", hidingContext);
                assertMaskedLikeMissing("WITH c AS (SELECT * FROM %s) SELECT * FROM c", "secret_t", hidingContext);
                assertMaskedLikeMissing("EXPLAIN SELECT * FROM %s", "secret_t", hidingContext);
                assertMaskedLikeMissing("SELECT * FROM %s", "secret_v", hidingContext);
                assertMaskedLikeMissing("SELECT * FROM %s", "SeCrEt_V", hidingContext);
                assertMaskedLikeMissing("SELECT * FROM visible_t CROSS JOIN %s", "secret_v", hidingContext);
                assertMaskedLikeMissing("SELECT * FROM %s", "secret_mv", hidingContext);
                assertMaskedLikeMissing("SELECT * FROM %s", "secret_lv", hidingContext);
            }
        });
    }

    @Test
    public void testViewSelectDenialIsAnAuthorizationError() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            execute("CREATE VIEW visible_columns AS (SELECT * FROM table_columns('secret_t'))");
            execute("CREATE VIEW visible_parts AS (SELECT * FROM table_partitions('secret_t'))");
            execute("CREATE VIEW visible_txns AS (SELECT * FROM wal_transactions('secret_t'))");
            execute("CREATE VIEW visible_show_columns AS (SELECT * FROM (SHOW COLUMNS FROM secret_t))");
            drainWalAndViewQueues();
            // the objects these views name are read through the view, so a principal who may see the views
            // but not read them is denied with an authorization error, not with one the factory wraps
            try (SqlExecutionContext noViewSelect = new SqlExecutionContextImpl(engine, 1).with(new HidingSecurityContext() {
                @Override
                public void authorizeSelect(ViewDefinition viewDefinition) {
                    throw CairoException.authorization().put("view select denied");
                }
            })) {
                for (String view : new String[]{"visible_columns", "visible_parts", "visible_txns", "visible_show_columns"}) {
                    assertAuthorizationFailure("SELECT * FROM " + view, noViewSelect, "view select denied");
                }
            }
        });
    }

    @Test
    public void testVisibleViewProjectionReadsInvisibleTableNameFunctions() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            // a table-name function in a select list of a view reads its argument through the view,
            // like one in its FROM clause
            execute("CREATE VIEW visible_proj AS (SELECT count() c FROM (SELECT table_partitions('secret_t')))");
            execute("CREATE VIEW visible_proj_from AS (SELECT count() c FROM (SELECT ts, table_partitions('secret_t') FROM visible_t))");
            execute("CREATE VIEW visible_proj_outer AS (SELECT * FROM visible_proj)");
            drainWalAndViewQueues();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                assertQuery("SELECT * FROM visible_proj").withContext(hidingContext).noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");
                assertQuery("SELECT * FROM visible_proj_from").withContext(hidingContext).noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");
                assertQuery("SELECT * FROM visible_proj_outer").withContext(hidingContext).noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");
            }
            // The outer view's grant, not the inner view's visibility, covers a nested expansion.
            try (SqlExecutionContext hidingInnerView = new SqlExecutionContextImpl(engine, 1).with(new HidingSecurityContext() {
                @Override
                public boolean isTableVisible(TableToken tableToken) {
                    return super.isTableVisible(tableToken) && !Chars.equals(tableToken.getTableName(), "visible_proj");
                }
            })) {
                assertQuery("SELECT * FROM visible_proj_outer").withContext(hidingInnerView).noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");
            }
        });
    }

    @Test
    public void testVisibleViewReadsInvisibleObjects() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            // a view is accessed as a whole: what it reads stays readable through it, even when the
            // principal may not see those objects directly
            execute("CREATE VIEW visible_v1 AS (SELECT ts, secret_col FROM secret_t)");
            execute("CREATE VIEW visible_v2 AS (SELECT ts, visible_col FROM secret_v)");
            drainWalAndViewQueues();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                assertQuery("SELECT * FROM visible_v1")
                        .withContext(hidingContext)
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("ts\tsecret_col\n2024-01-01T00:00:00.000000Z\t1\n");
                assertQuery("SELECT * FROM visible_v2")
                        .withContext(hidingContext)
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("ts\tvisible_col\n2024-01-01T00:00:00.000000Z\t2\n");
            }
        });
    }

    @Test
    public void testVisibleViewReadsInvisibleShowStatements() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            // a SHOW statement in a view reads the object it names through the view, like a table-name
            // function does, in a sub-query of the view's expression as well as in its FROM clause
            execute("CREATE VIEW visible_show_columns AS (SELECT * FROM (SHOW COLUMNS FROM secret_t))");
            execute("CREATE VIEW visible_show_partitions AS (SELECT * FROM (SHOW PARTITIONS FROM secret_t))");
            execute("CREATE VIEW visible_show_create_table AS (SELECT * FROM (SHOW CREATE TABLE secret_t))");
            execute("CREATE VIEW visible_show_create_view AS (SELECT * FROM (SHOW CREATE VIEW secret_v))");
            execute("CREATE VIEW visible_show_create_mv AS (SELECT * FROM (SHOW CREATE MATERIALIZED VIEW secret_mv))");
            execute("CREATE VIEW visible_show_sub AS (SELECT visible_col FROM visible_t WHERE visible_col >= (SELECT count() FROM (SHOW COLUMNS FROM secret_t)))");
            execute("CREATE VIEW visible_show_outer AS (SELECT * FROM visible_show_columns)");
            drainWalAndViewQueues();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                assertQuery("SELECT count() FROM visible_show_columns").withContext(hidingContext).noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
                assertQuery("SELECT count() FROM visible_show_partitions").withContext(hidingContext).noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
                assertQuery("SELECT count() FROM visible_show_create_table").withContext(hidingContext).noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
                assertQuery("SELECT count() FROM visible_show_create_view").withContext(hidingContext).noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
                assertQuery("SELECT count() FROM visible_show_create_mv").withContext(hidingContext).noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
                assertQuery("SELECT * FROM visible_show_sub").withContext(hidingContext).noLeakCheck().returns("visible_col\n2\n");
                assertQuery("SELECT count() FROM visible_show_outer").withContext(hidingContext).noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
                // the principal's own SHOW statements still hide the objects
                assertMaskedLikeMissing("SELECT * FROM (SHOW COLUMNS FROM %s)", "secret_t", hidingContext);
                assertMaskedLikeMissing("SELECT * FROM (SHOW CREATE TABLE %s)", "secret_t", hidingContext);
            }
            // A factory compiled while the view was visible must not bypass a later visibility change.
            for (String view : new String[]{"visible_show_columns", "visible_show_partitions", "visible_show_create_table", "visible_show_create_view", "visible_show_create_mv"}) {
                try (
                        RecordCursorFactory factory = select("SELECT * FROM " + view);
                        SqlExecutionContext hidingView = new SqlExecutionContextImpl(engine, 1).with(new HidingSecurityContext() {
                            @Override
                            public boolean isTableVisible(TableToken tableToken) {
                                return super.isTableVisible(tableToken) && !Chars.equals(tableToken.getTableName(), view);
                            }
                        })
                ) {
                    try (RecordCursor ignored = factory.getCursor(hidingView)) {
                        Assert.fail("a cached view cursor must recheck the view's visibility: " + view);
                    } catch (Throwable th) {
                        if (!(th instanceof FlyweightMessageContainer container)) {
                            throw th;
                        }
                        TestUtils.assertContains(view, container.getFlyweightMessage(), "does not exist");
                    }
                }
            }
        });
    }

    @Test
    public void testVisibleViewReadsInvisibleTableNameFunctions() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            execute("CREATE VIEW visible_parts AS (SELECT * FROM table_partitions('secret_t'))");
            execute("CREATE VIEW visible_columns AS (SELECT * FROM table_columns('secret_t'))");
            execute("CREATE VIEW visible_txns AS (SELECT * FROM wal_transactions('secret_t'))");
            execute("CREATE VIEW visible_outer AS (SELECT * FROM visible_parts)");
            drainWalAndViewQueues();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                assertQuery("SELECT count() FROM visible_parts").withContext(hidingContext).noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
                assertQuery("SELECT count() FROM visible_outer").withContext(hidingContext).noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
                assertQuery("SELECT count() FROM visible_columns").withContext(hidingContext).noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
                final StringSink sink = new StringSink();
                engine.print("SELECT count() FROM visible_txns", sink, hidingContext);
                Assert.assertFalse(sink.toString(), Chars.equals(sink, "count\n0\n"));
                assertMaskedLikeMissing("SELECT * FROM table_partitions('%s')", "secret_t", hidingContext);
            }
            // The outer view's grant, not the inner view's visibility, covers a nested expansion.
            try (SqlExecutionContext hidingInnerView = new SqlExecutionContextImpl(engine, 1).with(new HidingSecurityContext() {
                @Override
                public boolean isTableVisible(TableToken tableToken) {
                    return super.isTableVisible(tableToken) && !Chars.equals(tableToken.getTableName(), "visible_parts");
                }
            })) {
                assertQuery("SELECT count() FROM visible_outer").withContext(hidingInnerView).noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            }
            // A factory compiled while the view was visible must not bypass a later visibility change.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM visible_parts");
                    SqlExecutionContext hidingView = new SqlExecutionContextImpl(engine, 1).with(new HidingSecurityContext() {
                        @Override
                        public boolean isTableVisible(TableToken tableToken) {
                            return super.isTableVisible(tableToken) && !Chars.equals(tableToken.getTableName(), "visible_parts");
                        }
                    })
            ) {
                try (RecordCursor ignored = factory.getCursor(hidingView)) {
                    Assert.fail("a cached view cursor must recheck the view's visibility");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist");
                }
            }
            final TableToken oldView = engine.verifyTableName("visible_parts");
            try (
                    RecordCursorFactory oldFunction = new ShowPartitionsRecordCursorFactory(
                            engine.verifyTableName("secret_t"), ColumnType.TIMESTAMP_MICRO, 0,
                            new SqlExecutionContext.TableFunctionView(engine.getViewGraph().getViewDefinition(oldView))
                    );
                    SqlExecutionContext hidingContext = newHidingContext()
            ) {
                execute("CREATE OR REPLACE VIEW visible_parts AS (SELECT * FROM table_partitions('visible_t'))");
                drainWalAndViewQueues();
                // the old function's plan is stale: the caller recompiles it against the new definition
                try (RecordCursor ignored = oldFunction.getCursor(hidingContext)) {
                    Assert.fail("a replaced view must not authorize the old table function");
                } catch (TableReferenceOutOfDateException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cached query plan cannot be used");
                }
            }
            try (RecordCursorFactory cached = select("SELECT * FROM visible_parts")) {
                execute("DROP VIEW visible_parts");
                execute("CREATE VIEW visible_parts AS (SELECT * FROM table_partitions('visible_t'))");
                drainWalAndViewQueues();
                try (RecordCursor ignored = cached.getCursor(sqlExecutionContext)) {
                    Assert.fail("cached cursor must not read an old view after DROP + CREATE");
                } catch (TableReferenceOutOfDateException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cached query plan cannot be used");
                }
            }
        });
    }

    @Test
    public void testVisibleViewSubQueryReadsInvisibleTableNameFunctions() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            execute("CREATE TABLE visible_names (ts TIMESTAMP, name SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO visible_names VALUES ('2024-01-01', 'secret_col'), ('2024-01-01', 'other')");
            drainWalQueue();
            // a table-name function in a sub-query of a view reads its argument through the view,
            // like one in its FROM clause, but the tables such a sub-query reads are read as the caller
            execute("CREATE VIEW visible_in AS (SELECT name FROM visible_names WHERE name IN (SELECT \"column\" FROM table_columns('secret_t')))");
            execute("CREATE VIEW visible_table_in AS (SELECT name FROM visible_names WHERE name IN (SELECT 'secret_col' FROM secret_t))");
            drainWalAndViewQueues();
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                assertQuery("SELECT * FROM visible_in")
                        .withContext(hidingContext)
                        .noLeakCheck()
                        .returns("name\nsecret_col\n");
                assertMaskedLikeMissing("SELECT name FROM visible_names WHERE name IN (SELECT \"column\" FROM table_columns('%s'))", "secret_t", hidingContext);
                assertFailure("SELECT * FROM visible_table_in", hidingContext, "table does not exist [table=secret_t]");
            }
        });
    }

    @Test
    public void testWalRecoveryOfInvisibleTableRequiresPermission() throws Exception {
        assertMemoryLeak(() -> {
            createObjects();
            final TableToken tableToken = engine.verifyTableName("secret_t");
            try (SqlExecutionContext hidingContext = newHidingContext()) {
                engine.execute("ALTER TABLE secret_t SUSPEND WAL", hidingContext);
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
                engine.execute("ALTER TABLE secret_t RESUME WAL", hidingContext);
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
                assertExecutionMaskedLikeMissing("ALTER TABLE %s ADD COLUMN c INT", "secret_t", hidingContext);
            }
            try (SqlExecutionContext denied = new SqlExecutionContextImpl(engine, 1).with(new HidingNoWalSecurityContext())) {
                assertExecutionMaskedLikeMissing("ALTER TABLE %s RESUME WAL", "secret_t", denied);
                assertExecutionMaskedLikeMissing("ALTER TABLE %s SUSPEND WAL", "secret_t", denied);
                assertExecutionMaskedLikeMissing("ALTER TABLE %s REBASE WAL", "secret_t", denied);
            }
        });
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, temp.newFolder("export").getAbsolutePath());
        AbstractCairoTest.setUpStatic();
    }

    // Asserts that the statement fails with an authorization error, which the protocols report as denied
    // access, rather than with an error that wraps it.
    private static void assertAuthorizationFailure(CharSequence sql, SqlExecutionContext context, String expectedMessage) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final CompiledQuery cq = compiler.compile(sql, context);
            try (RecordCursorFactory factory = cq.getRecordCursorFactory(); RecordCursor cursor = factory.getCursor(context)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                    // drain
                }
            }
            Assert.fail("expected an authorization failure: " + sql);
        } catch (CairoException e) {
            Assert.assertTrue(sql + ": " + e.getFlyweightMessage(), e.isAuthorizationError());
            TestUtils.assertEquals(sql.toString(), expectedMessage, e.getFlyweightMessage());
        }
    }

    private static void assertCursorFails(CharSequence sql, SqlExecutionContext context, String expectedMessage, StringSink sink) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            print(factory, sqlExecutionContext, sink);
            final int headerEnd = sink.toString().indexOf('\n');
            Assert.assertTrue(sql + " must return a row for allow-all: " + sink, headerEnd >= 0 && headerEnd < sink.length() - 1);
            try (RecordCursor cursor = factory.getCursor(context)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                    // drain
                }
                Assert.fail("expected a failure: " + sql);
            } catch (Throwable th) {
                if (!(th instanceof FlyweightMessageContainer container)) {
                    throw th;
                }
                TestUtils.assertEquals(expectedMessage, container.getFlyweightMessage());
            }
        }
    }

    // Like assertMaskedLikeMissing(), but executes the statement, for statements that act when they
    // execute rather than when they compile, e.g. DROP.
    private static void assertExecutionMaskedLikeMissing(String sqlTemplate, String hiddenName, SqlExecutionContext hidingContext) throws Exception {
        final String missingName = missingNameOf(hiddenName);
        final String missing = executionFailureOf(String.format(sqlTemplate, missingName), hidingContext);
        final String hidden = executionFailureOf(String.format(sqlTemplate, hiddenName), hidingContext);
        Assert.assertEquals(String.format(sqlTemplate, hiddenName), missing.replace(missingName, hiddenName), hidden);
    }

    private static void assertFailure(CharSequence sql, SqlExecutionContext context, String expectedMessage) throws Exception {
        TestUtils.assertContains(failureOf(sql, context), expectedMessage);
    }

    // Asserts that the statement fails for the hidden object exactly like it does for a missing one of
    // the same length: same exception type, same position and same message, apart from the name.
    private static void assertMaskedLikeMissing(String sqlTemplate, String hiddenName, SqlExecutionContext hidingContext) throws Exception {
        final String missingName = missingNameOf(hiddenName);
        final String missing = failureOf(String.format(sqlTemplate, missingName), hidingContext);
        final String hidden = failureOf(String.format(sqlTemplate, hiddenName), hidingContext);
        Assert.assertEquals(String.format(sqlTemplate, hiddenName), missing.replace(missingName, hiddenName), hidden);
    }

    private static void createObjects() throws Exception {
        execute("CREATE TABLE visible_t (ts TIMESTAMP, visible_col INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE secret_t (ts TIMESTAMP, secret_col INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE visible_nw (ts TIMESTAMP, visible_nw_col INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("CREATE TABLE secret_nw (ts TIMESTAMP, secret_nw_col INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO secret_t VALUES ('2024-01-01', 1)");
        execute("INSERT INTO visible_t VALUES ('2024-01-01', 2)");
        execute("INSERT INTO secret_nw VALUES ('2024-01-01', 3)");
        execute("INSERT INTO visible_nw VALUES ('2024-01-01', 4)");
        execute("CREATE VIEW secret_v AS (SELECT ts, visible_col FROM visible_t)");
        execute("CREATE VIEW visible_v AS (SELECT ts, visible_col FROM visible_t)");
        execute("CREATE MATERIALIZED VIEW secret_mv AS (SELECT ts, max(visible_col) secret_mv_col FROM visible_t SAMPLE BY 1d) PARTITION BY DAY");
        execute("CREATE MATERIALIZED VIEW visible_mv AS (SELECT ts, max(visible_col) visible_mv_col FROM visible_t SAMPLE BY 1d) PARTITION BY DAY");
        execute(
                "CREATE LIVE VIEW secret_lv FLUSH EVERY 1s START FROM NOW AS " +
                        "SELECT ts, visible_col, count(*) OVER (PARTITION BY visible_col ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rn FROM visible_t"
        );
        execute(
                "CREATE LIVE VIEW visible_lv FLUSH EVERY 1s START FROM NOW AS " +
                        "SELECT ts, visible_col, count(*) OVER (PARTITION BY visible_col ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rn FROM visible_t"
        );
        drainWalAndMatViewQueues();
        drainWalAndViewQueues();
        // park a reader of every table in the reader pool, and the writers of the non-WAL tables
        // in the writer pool, so that reader_pool() and writer_pool() have something to list
        final StringSink sink = new StringSink();
        for (String table : new String[]{"visible_t", "secret_t", "visible_nw", "secret_nw"}) {
            engine.print("SELECT * FROM " + table, sink, sqlExecutionContext);
        }
    }

    private static String executionFailureOf(CharSequence sql, SqlExecutionContext context) throws Exception {
        try {
            engine.execute(sql, context);
        } catch (Throwable th) {
            if (th instanceof FlyweightMessageContainer container) {
                return th.getClass().getSimpleName() + '@' + container.getPosition() + ": " + container.getFlyweightMessage();
            }
            throw th;
        }
        Assert.fail("expected a failure: " + sql);
        return null;
    }

    private static String failureOf(CharSequence sql, SqlExecutionContext context) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final CompiledQuery cq = compiler.compile(sql, context);
            try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                if (factory != null) {
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                            // drain
                        }
                    }
                }
            }
        } catch (Throwable th) {
            if (th instanceof FlyweightMessageContainer container) {
                return th.getClass().getSimpleName() + '@' + container.getPosition() + ": " + container.getFlyweightMessage();
            }
            throw th;
        }
        Assert.fail("expected a failure: " + sql);
        return null;
    }

    // a name of the same length and case pattern, so both names spell the same error positions
    private static String missingNameOf(String hiddenName) {
        final StringBuilder missingName = new StringBuilder();
        for (int i = 0, n = hiddenName.length(); i < n; i++) {
            final char c = hiddenName.charAt(i);
            missingName.append(c == '_' ? '_' : Character.isUpperCase(c) ? 'M' : 'm');
        }
        return missingName.toString();
    }

    private static SqlExecutionContext newHidingContext() {
        return new SqlExecutionContextImpl(engine, 1).with(new HidingSecurityContext(), bindVariableService, null, -1, null);
    }

    private static void print(RecordCursorFactory factory, SqlExecutionContext context, StringSink sink) throws Exception {
        sink.clear();
        try (RecordCursor cursor = factory.getCursor(context)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
        }
    }

    // The statements under test must have left the hidden objects as they were.
    private void assertHiddenObjectsIntact() throws Exception {
        for (String name : new String[]{"secret_t", "secret_nw", "secret_v", "secret_mv", "secret_lv"}) {
            Assert.assertNotNull(name, engine.getTableTokenIfExists(name));
        }
        Assert.assertNull(engine.getTableTokenIfExists("renamed_t"));
        assertQuery("SELECT * FROM secret_t")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns("ts\tsecret_col\n2024-01-01T00:00:00.000000Z\t1\n");
        assertQuery("SELECT * FROM secret_nw")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns("ts\tsecret_nw_col\n2024-01-01T00:00:00.000000Z\t3\n");
    }

    // like HidingSecurityContext, but may drop nothing
    private static final class HidingNoDropSecurityContext extends HidingSecurityContext {
        @Override
        public void authorizeLiveViewDrop(TableToken tableToken) {
            throw CairoException.authorization().put("drop denied");
        }

        @Override
        public void authorizeMatViewDrop(TableToken tableToken) {
            throw CairoException.authorization().put("drop denied");
        }

        @Override
        public void authorizeTableDrop(TableToken tableToken) {
            throw CairoException.authorization().put("drop denied");
        }

        @Override
        public void authorizeViewDrop(TableToken tableToken) {
            throw CairoException.authorization().put("drop denied");
        }
    }

    private static final class HidingNoWalSecurityContext extends HidingSecurityContext {
        @Override
        public void authorizeRebaseWal(TableToken tableToken) {
            throw CairoException.authorization().put("wal denied");
        }

        @Override
        public void authorizeResumeWal(TableToken tableToken) {
            throw CairoException.authorization().put("wal denied");
        }
    }

    // may see every object except those named secret*
    private static class HidingSecurityContext extends AllowAllSecurityContext {
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
