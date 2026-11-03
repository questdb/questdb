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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlCompilerFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionRequirements;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A SQL parser callback declares what the SHOW statements it compiles require, as Enterprise declares that SHOW
 * CREATE DATABASE with ACL categories needs the Enterprise security context of its caller. The compilers of this
 * engine declare that for SHOW TABLES while {@link #isShowTablesAdministrative} is set.
 */
public class ShowExecutionRequirementsTest extends AbstractCairoTest {
    private static final String LIVE_VIEW_REJECTION = "administrative statement cannot be used in live view: SHOW TABLES";
    private static final String MAT_VIEW_REJECTION = "administrative statement cannot be used in materialized view: SHOW TABLES";
    private static final String PIVOT_QUERY = """
            SELECT * FROM data PIVOT (SUM(val) FOR cat IN (
                SELECT table_name FROM (SHOW TABLES) WHERE table_name = 'data'
            ))""";
    private static final String VIEW_PIVOT_REJECTION = "administrative statement cannot be used in view PIVOT IN subquery: SHOW TABLES";
    private static volatile boolean isShowTablesAdministrative = true;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = configuration -> new CairoEngine(configuration) {
            @Override
            public SqlCompilerFactory getSqlCompilerFactory() {
                return AdministrativeShowCompiler::new;
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testLiveViewRejectsAdministrativeShow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String select = "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rn "
                    + "FROM base WHERE x < (SELECT count() FROM (SHOW TABLES))";
            // CREATE LIVE VIEW compiles the SELECT on its own, so the position is relative to it
            assertQuery("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + select)
                    .noLeakCheck()
                    .fails(select.indexOf("SHOW"), LIVE_VIEW_REJECTION);
            Assert.assertNull(engine.getTableTokenIfExists("lv"));
        });
    }

    @Test
    public void testMatViewRejectsAdministrativeShowInJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String sql = """
                    CREATE MATERIALIZED VIEW mv WITH BASE base AS (
                        SELECT base.ts, count() c FROM base CROSS JOIN (SHOW TABLES) SAMPLE BY 1d
                    ) PARTITION BY DAY""";
            assertQuery(sql).noLeakCheck().fails(sql.indexOf("SHOW"), MAT_VIEW_REJECTION);
            Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testMatViewRejectsAdministrativeShowInSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String sql = """
                    CREATE MATERIALIZED VIEW mv AS (
                        SELECT ts, count() c FROM base WHERE x < (SELECT count() FROM (SHOW TABLES)) SAMPLE BY 1d
                    ) PARTITION BY DAY""";
            assertQuery(sql).noLeakCheck().fails(sql.indexOf("SHOW"), MAT_VIEW_REJECTION);
            Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testViewBodyAcceptsAdministrativeShow() throws Exception {
        // only a PIVOT IN subquery runs while the view compiler derives a view's columns, a read of the view runs the rest
        assertMemoryLeak(() -> {
            createData();
            execute("CREATE VIEW v AS (SELECT table_name FROM (SHOW TABLES) WHERE table_name = 'data')");
            execute("COMPILE VIEW v");
            drainViewQueue();
            assertViewStatus("v", "valid", "");
            assertQuery("SELECT * FROM v")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            table_name
                            data
                            """);
        });
    }

    @Test
    public void testViewPivotAcceptsAdministrativeShowInUnreadColumn() throws Exception {
        // a sub-query nested in a view's PIVOT IN subquery runs only if the subquery's plan keeps it, and one in a column
        // that nothing reads never runs
        assertMemoryLeak(() -> {
            createData();
            execute("""
                    CREATE VIEW v_column AS (
                        SELECT cat, CASE WHEN val > (SELECT count() FROM (SHOW TABLES)) THEN 1 ELSE 0 END n FROM data
                    )""");
            execute("""
                    CREATE VIEW pv AS (
                        SELECT * FROM data PIVOT (SUM(val) FOR cat IN (SELECT cat FROM v_column))
                    )""");
            execute("COMPILE VIEW pv");
            drainViewQueue();
            assertViewStatus("pv", "valid", "");
            assertQuery("SELECT * FROM pv")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            data\tother
                            42\t7
                            """);
        });
    }

    @Test
    public void testViewPivotRejectsAdministrativeShow() throws Exception {
        assertMemoryLeak(() -> {
            createData();
            final String create = "CREATE VIEW pv AS (" + PIVOT_QUERY + ")";
            assertQuery(create).noLeakCheck().fails(create.indexOf("SHOW"), VIEW_PIVOT_REJECTION);
            Assert.assertNull(engine.getTableTokenIfExists("pv"));

            execute("CREATE VIEW pv AS (SELECT 1 x FROM long_sequence(1))");
            for (String sql : new String[]{"ALTER VIEW pv AS " + PIVOT_QUERY, "CREATE OR REPLACE VIEW pv AS " + PIVOT_QUERY}) {
                assertQuery(sql).noLeakCheck().fails(sql.indexOf("SHOW"), VIEW_PIVOT_REJECTION);
            }
            Assert.assertFalse(sqlExecutionContext.isViewCompile());
            Assert.assertFalse(sqlExecutionContext.isViewMetadataSubquery());

            // the same PIVOT runs ad hoc, as its caller
            assertQuery(PIVOT_QUERY)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            data
                            42
                            """);
        });
    }

    @Test
    public void testViewPivotRejectsAdministrativeShowInNestedSubquery() throws Exception {
        // the optimiser creates a SHOW cursor in a sub-query nested in a view's PIVOT IN subquery before it knows whether
        // the subquery's plan keeps the sub-query; this plan does, so the view rejects the statement
        assertMemoryLeak(() -> {
            createData();
            final String create = """
                    CREATE VIEW pv AS (
                        SELECT * FROM data PIVOT (SUM(val) FOR cat IN (
                            SELECT CASE WHEN (SELECT count() FROM (SHOW TABLES)) > 0 THEN 'data' ELSE 'other' END
                            FROM long_sequence(1)
                        ))
                    )""";
            assertQuery(create).noLeakCheck().fails(create.indexOf("SHOW"), VIEW_PIVOT_REJECTION);
            Assert.assertNull(engine.getTableTokenIfExists("pv"));
            Assert.assertFalse(sqlExecutionContext.isViewCompile());
            Assert.assertFalse(sqlExecutionContext.isViewMetadataSubquery());
        });
    }

    @Test
    public void testViewPivotRejectsAdministrativeShowInStoredView() throws Exception {
        assertMemoryLeak(() -> {
            createData();
            // stored while the statement needs no caller, as a definition stored before the restriction
            isShowTablesAdministrative = false;
            try {
                execute("CREATE VIEW pv AS (" + PIVOT_QUERY + ")");
                drainViewQueue();
                assertViewStatus("pv", "valid", "");
            } finally {
                isShowTablesAdministrative = true;
            }
            // COMPILE VIEW fails at the view's name, and queues the view compiler job, which invalidates the view
            assertQuery("COMPILE VIEW pv").noLeakCheck().fails(13, VIEW_PIVOT_REJECTION);
            drainViewQueue();
            assertViewStatus("pv", "invalid", VIEW_PIVOT_REJECTION);

            // a view over pv reports the statement at pv's name, not at its position in pv's definition
            execute("CREATE VIEW other AS (SELECT 1 x FROM long_sequence(1))");
            for (String sql : new String[]{
                    "CREATE VIEW reader AS (SELECT * FROM pv)",
                    "ALTER VIEW other AS SELECT * FROM pv",
                    "CREATE OR REPLACE VIEW other AS SELECT * FROM pv"
            }) {
                assertQuery(sql).noLeakCheck().fails(sql.lastIndexOf("pv"), VIEW_PIVOT_REJECTION);
            }
            Assert.assertNull(engine.getTableTokenIfExists("reader"));

            // a read of the view runs the statement as its reader
            assertQuery("SELECT * FROM pv")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            data
                            42
                            """);
        });
    }

    private static void createData() throws SqlException {
        execute("CREATE TABLE data (cat VARCHAR, val INT)");
        execute("INSERT INTO data VALUES ('data', 42), ('other', 7)");
    }

    private void assertViewStatus(String viewName, String status, String invalidationReason) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\n" + status + '\t' + invalidationReason + '\n');
    }

    private static class AdministrativeShowCompiler extends SqlCompilerImpl {
        AdministrativeShowCompiler(CairoEngine engine) {
            super(engine);
        }

        @Override
        public int getShowExecutionRequirements(IQueryModel model) {
            return isShowTablesAdministrative && model.getShowKind() == IQueryModel.SHOW_TABLES
                    ? SqlExecutionRequirements.REQUIRES_ENTERPRISE_SECURITY_CONTEXT
                    : SqlExecutionRequirements.NONE;
        }
    }
}
