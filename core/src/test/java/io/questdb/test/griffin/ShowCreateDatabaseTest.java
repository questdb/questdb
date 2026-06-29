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

import io.questdb.PropertyKey;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.ShowCreateDatabaseRecordCursorFactory;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.QueryModelWrapper;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ShowCreateDatabaseTest extends AbstractCairoTest {

    @Test
    public void testEmptyDatabaseReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> statements = dumpDatabase();
            Assert.assertEquals(0, statements.size());
        });
    }

    @Test
    public void testExcludesSystemTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp, v double) timestamp(ts) partition by day wal");
            execute("create table bar (ts timestamp, v double) timestamp(ts) partition by day wal");
            drainWalQueue();

            final ObjList<String> statements = dumpDatabase();
            // exactly the two user tables are dumped; a leaked system/telemetry table would push the count up
            Assert.assertEquals(2, statements.size());
            final String dump = statements.toString();
            Assert.assertTrue(dump.contains("CREATE TABLE 'bar'"));
            Assert.assertTrue(dump.contains("CREATE TABLE 'foo'"));
            Assert.assertFalse("system tables must not be dumped", dump.contains("sys."));
            Assert.assertFalse("telemetry tables must not be dumped", dump.contains("telemetry"));
        });
    }

    @Test
    public void testObjectOrderingTablesThenMatViewsThenViews() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, s symbol, v double) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select ts, s, avg(v) v from base sample by 1d) partition by day");
            execute("create view v as (select ts, s from base)");
            drainWalQueue();

            final String dump = dumpDatabase().toString();
            final int tableIdx = dump.indexOf("CREATE TABLE 'base'");
            final int matViewIdx = dump.indexOf("CREATE MATERIALIZED VIEW 'mv'");
            final int viewIdx = dump.indexOf("CREATE VIEW 'v'");

            Assert.assertTrue("table DDL must be present", tableIdx >= 0);
            Assert.assertTrue("materialized view DDL must be present", matViewIdx >= 0);
            Assert.assertTrue("view DDL must be present", viewIdx >= 0);
            Assert.assertTrue("table must precede materialized view", tableIdx < matViewIdx);
            Assert.assertTrue("materialized view must precede view", matViewIdx < viewIdx);
        });
    }

    @Test
    public void testIncludeAndExcludeFilterCategories() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            execute("create table base (ts timestamp, s symbol, v double) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select ts, s, avg(v) v from base sample by 1d) partition by day");
            execute("create view v as (select ts, s from base)");
            drainWalQueue();

            final String tablesOnly = dump("SHOW CREATE DATABASE INCLUDE (TABLES)");
            Assert.assertTrue(tablesOnly, tablesOnly.contains("CREATE TABLE 'base'"));
            Assert.assertFalse(tablesOnly, tablesOnly.contains("CREATE MATERIALIZED VIEW"));
            Assert.assertFalse(tablesOnly, tablesOnly.contains("CREATE VIEW 'v'"));

            final String schema = dump("SHOW CREATE DATABASE INCLUDE (SCHEMA)");
            Assert.assertTrue(schema, schema.contains("CREATE TABLE 'base'"));
            Assert.assertTrue(schema, schema.contains("CREATE MATERIALIZED VIEW 'mv'"));
            Assert.assertTrue(schema, schema.contains("CREATE VIEW 'v'"));

            final String noViews = dump("SHOW CREATE DATABASE EXCLUDE (VIEWS)");
            Assert.assertTrue(noViews, noViews.contains("CREATE TABLE 'base'"));
            Assert.assertTrue(noViews, noViews.contains("CREATE MATERIALIZED VIEW 'mv'"));
            Assert.assertFalse(noViews, noViews.contains("CREATE VIEW 'v'"));

            // default is INCLUDE ALL
            Assert.assertEquals(dump("SHOW CREATE DATABASE"), dump("SHOW CREATE DATABASE INCLUDE ALL"));
        });
    }

    @Test
    public void testIncludeRejectsMalformedClauses() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck("SHOW CREATE DATABASE INCLUDE (BOGUS)", 30, "unexpected category");
            assertExceptionNoLeakCheck("SHOW CREATE DATABASE INCLUDE ()", 30, "unexpected category");
            assertExceptionNoLeakCheck("SHOW CREATE DATABASE INCLUDE (TABLES,)", 37, "unexpected category");
            assertExceptionNoLeakCheck("SHOW CREATE DATABASE INCLUDE TABLES", 29, "'ALL' or '('");
            assertExceptionNoLeakCheck("SHOW CREATE DATABASE INCLUDE ALL garbage", 33, "garbage");
            // a non-INCLUDE/EXCLUDE token after DATABASE is left for the trailing-token check
            assertExceptionNoLeakCheck("SHOW CREATE DATABASE garbage", 21, "garbage");
            // a category list must be comma-separated
            assertExceptionNoLeakCheck("SHOW CREATE DATABASE INCLUDE (TABLES VIEWS)", 37, "',' or ')'");
        });
    }

    @Test
    public void testAclCategoriesParseToEmptyDumpInOss() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            execute("create table base (ts timestamp, s symbol, v double) timestamp(ts) partition by day wal");
            drainWalQueue();
            // the ACL categories are valid grammar but select no schema, so in OSS (no ACL) the dump is empty
            for (String category : new String[]{"USERS", "GROUPS", "SERVICE_ACCOUNTS", "PERMISSIONS", "ACL"}) {
                Assert.assertEquals(category, 0, dumpDatabase("SHOW CREATE DATABASE INCLUDE (" + category + ")").size());
            }
            // mixing an ACL category with a schema category still emits the schema object
            final String mixed = dump("SHOW CREATE DATABASE INCLUDE (PERMISSIONS, TABLES)");
            Assert.assertTrue(mixed, mixed.contains("CREATE TABLE 'base'"));
            // ALL is also accepted as a parenthesised category, equivalent to the bare default
            Assert.assertEquals(dump("SHOW CREATE DATABASE"), dump("SHOW CREATE DATABASE INCLUDE (ALL)"));
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo (ts timestamp, s symbol) timestamp(ts) partition by year bypass wal");
            assertQuery("SHOW CREATE DATABASE")
                    .noLeakCheck()
                    .assertsPlan("show_create_database\n");
        });
    }

    @Test
    public void testMatViewWithTypedFilterDependencyOrdering() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            execute("create table base (ts timestamp, s symbol, d double, i int, l long, b boolean) timestamp(ts) partition by day wal");
            // a typed WHERE filter makes the compiled plan render double/int/long/boolean constants; walking that
            // plan to collect the base table exercises the value-rendering paths of the token collector
            execute("create materialized view mv with base base as (" +
                    "select ts, avg(d) d from base where d > 1.5 and i = 3 and l <> 100000000000 and b = true sample by 1h" +
                    ") partition by day");
            drainWalQueue();
            final String dump = dump("SHOW CREATE DATABASE");
            final int baseIdx = dump.indexOf("CREATE TABLE 'base'");
            final int mvIdx = dump.indexOf("CREATE MATERIALIZED VIEW 'mv'");
            Assert.assertTrue(dump, baseIdx >= 0 && mvIdx >= 0);
            Assert.assertTrue("base table must precede the mat view that reads it", baseIdx < mvIdx);
        });
    }

    @Test
    public void testMatViewCompileFailureFallsBackToBaseTable() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            execute("create table a_base (ts timestamp, k symbol, v double) timestamp(ts) partition by day wal");
            execute("create table z_dim (ts timestamp, k symbol, label string) timestamp(ts) partition by day wal");
            execute("create materialized view m_mv with base a_base as " +
                    "(select a_base.ts, z_dim.label, avg(a_base.v) v from a_base join z_dim on k sample by 1h) partition by day");
            drainWalQueue();

            // drop the joined dimension so the mat view SQL no longer compiles; dependency resolution must
            // swallow the compile failure and fall back to ordering by the base table, still emitting the view
            execute("drop table z_dim");
            drainWalQueue();

            final String dump = dump("SHOW CREATE DATABASE");
            Assert.assertTrue(dump, dump.contains("CREATE MATERIALIZED VIEW 'm_mv'"));
            Assert.assertTrue(dump, dump.contains("CREATE TABLE 'a_base'"));
        });
    }

    @Test
    public void testQueryModelWrapperShowCreateDatabaseInclude() {
        // the wrapper getter delegates; the setter is unsupported (a SHOW CREATE DATABASE model is never wrapped)
        final QueryModel delegate = QueryModel.FACTORY.newInstance();
        delegate.setShowCreateDatabaseInclude(ShowCreateDatabaseRecordCursorFactory.INCLUDE_SCHEMA);
        final QueryModelWrapper wrapper = new QueryModelWrapper();
        wrapper.setDelegate(delegate);
        Assert.assertEquals(ShowCreateDatabaseRecordCursorFactory.INCLUDE_SCHEMA, wrapper.getShowCreateDatabaseInclude());
        try {
            wrapper.setShowCreateDatabaseInclude(ShowCreateDatabaseRecordCursorFactory.INCLUDE_ALL);
            Assert.fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
            // the wrapper forbids mutating the shared delegate
        }
    }

    @Test
    public void testShowCreateUnknownObjectTypeFails() throws Exception {
        assertMemoryLeak(() -> assertExceptionNoLeakCheck(
                "SHOW CREATE WAREHOUSE",
                12,
                "expected 'TABLE' or 'VIEW' or 'MATERIALIZED VIEW' or 'DATABASE'"
        ));
    }

    @Test
    public void testIncludeViewsAndMatViewsInIsolation() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            execute("create table base (ts timestamp, s symbol, v double) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select ts, s, avg(v) v from base sample by 1d) partition by day");
            execute("create view v as (select ts, s from base)");
            drainWalQueue();

            final String viewsOnly = dump("SHOW CREATE DATABASE INCLUDE (VIEWS)");
            Assert.assertTrue(viewsOnly, viewsOnly.contains("CREATE VIEW 'v'"));
            Assert.assertFalse(viewsOnly, viewsOnly.contains("CREATE TABLE"));
            Assert.assertFalse(viewsOnly, viewsOnly.contains("CREATE MATERIALIZED VIEW"));

            final String matViewsOnly = dump("SHOW CREATE DATABASE INCLUDE (MATERIALIZED_VIEWS)");
            Assert.assertTrue(matViewsOnly, matViewsOnly.contains("CREATE MATERIALIZED VIEW 'mv'"));
            Assert.assertFalse(matViewsOnly, matViewsOnly.contains("CREATE TABLE"));
            Assert.assertFalse(matViewsOnly, matViewsOnly.contains("CREATE VIEW 'v'"));

            // EXCLUDE ALL -> empty dump
            Assert.assertEquals(0, dumpDatabase("SHOW CREATE DATABASE EXCLUDE ALL").size());
        });
    }

    @Test
    public void testFilteredSchemaDumpReplays() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            execute("create table base (ts timestamp, s symbol index, v double) timestamp(ts) partition by day wal dedup upsert keys(ts, s)");
            execute("create view v as (select ts, s from base)");
            drainWalQueue();

            final ObjList<String> before = dumpDatabase("SHOW CREATE DATABASE INCLUDE (SCHEMA)");
            execute("drop view v");
            execute("drop table base");
            drainWalQueue();
            for (int i = 0, n = before.size(); i < n; i++) {
                execute(before.getQuick(i));
            }
            drainWalQueue();

            final ObjList<String> after = dumpDatabase("SHOW CREATE DATABASE INCLUDE (SCHEMA)");
            Assert.assertEquals(before.size(), after.size());
            for (int i = 0, n = before.size(); i < n; i++) {
                Assert.assertEquals("statement " + i + " differs", before.getQuick(i), after.getQuick(i));
            }
        });
    }

    @Test
    public void testMatViewJoinDependencyOrdering() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            // m_mv joins z_join, which sorts AFTER the materialized view; ordering by base table
            // alone would emit m_mv before z_join and the replay would fail
            execute("create table a_base (ts timestamp, k symbol, v double) timestamp(ts) partition by day wal");
            execute("create table z_join (ts timestamp, k symbol, label string) timestamp(ts) partition by day wal");
            execute("create materialized view m_mv with base a_base as " +
                    "(select a_base.ts, z_join.label, avg(a_base.v) v from a_base join z_join on k sample by 1h) partition by day");
            drainWalQueue();

            final ObjList<String> before = dumpDatabase();
            final String dump = before.toString();
            final int joinIdx = dump.indexOf("CREATE TABLE 'z_join'");
            final int mvIdx = dump.indexOf("CREATE MATERIALIZED VIEW 'm_mv'");
            Assert.assertTrue(joinIdx >= 0 && mvIdx >= 0);
            Assert.assertTrue("joined table must precede the materialized view that reads it", joinIdx < mvIdx);

            execute("drop materialized view m_mv");
            execute("drop table z_join");
            execute("drop table a_base");
            drainWalQueue();
            for (int i = 0, n = before.size(); i < n; i++) {
                execute(before.getQuick(i));
            }
            drainWalQueue();

            final ObjList<String> after = dumpDatabase();
            Assert.assertEquals(before.size(), after.size());
            for (int i = 0, n = before.size(); i < n; i++) {
                Assert.assertEquals("statement " + i + " differs", before.getQuick(i), after.getQuick(i));
            }
        });
    }

    @Test
    public void testDeterministicSiblingDependencyOrdering() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            // a_view reads y_tab and z_tab, both of which sort AFTER a_view; the dependency sort must
            // emit the two siblings in a stable (name) order, not plan/graph iteration order
            execute("create table y_tab (k symbol, vy double)");
            execute("create table z_tab (k symbol, vz double)");
            execute("create view a_view as (select y_tab.k, y_tab.vy, z_tab.vz from y_tab join z_tab on k)");
            drainWalQueue();

            final ObjList<String> before = dumpDatabase();
            final String dump = before.toString();
            final int yIdx = dump.indexOf("CREATE TABLE 'y_tab'");
            final int zIdx = dump.indexOf("CREATE TABLE 'z_tab'");
            final int viewIdx = dump.indexOf("CREATE VIEW 'a_view'");
            Assert.assertTrue(yIdx >= 0 && zIdx >= 0 && viewIdx >= 0);
            Assert.assertTrue("dependencies must precede the dependent view", yIdx < viewIdx && zIdx < viewIdx);
            Assert.assertTrue("sibling dependencies must be emitted in deterministic name order", yIdx < zIdx);

            execute("drop view a_view");
            execute("drop table y_tab");
            execute("drop table z_tab");
            drainWalQueue();
            for (int i = 0, n = before.size(); i < n; i++) {
                execute(before.getQuick(i));
            }
            drainWalQueue();

            final ObjList<String> after = dumpDatabase();
            Assert.assertEquals(before.size(), after.size());
            for (int i = 0, n = before.size(); i < n; i++) {
                Assert.assertEquals("statement " + i + " differs", before.getQuick(i), after.getQuick(i));
            }
        });
    }

    @Test
    public void testMatViewMultiJoinDependencyOrdering() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            // m_mv joins two dimension tables, both sorting AFTER the mat view; the plan walk must
            // collect both joined tables so they are emitted before the mat view
            execute("create table a_base (ts timestamp, k symbol, v double) timestamp(ts) partition by day wal");
            execute("create table y_dim (ts timestamp, k symbol, y string) timestamp(ts) partition by day wal");
            execute("create table z_dim (ts timestamp, k symbol, z string) timestamp(ts) partition by day wal");
            execute("create materialized view m_mv with base a_base as " +
                    "(select a_base.ts, y_dim.y, z_dim.z, avg(a_base.v) v from a_base join y_dim on k join z_dim on k sample by 1h) partition by day");
            drainWalQueue();

            final ObjList<String> before = dumpDatabase();
            final String dump = before.toString();
            final int yIdx = dump.indexOf("CREATE TABLE 'y_dim'");
            final int zIdx = dump.indexOf("CREATE TABLE 'z_dim'");
            final int mvIdx = dump.indexOf("CREATE MATERIALIZED VIEW 'm_mv'");
            Assert.assertTrue(yIdx >= 0 && zIdx >= 0 && mvIdx >= 0);
            Assert.assertTrue("both joined tables must precede the materialized view", yIdx < mvIdx && zIdx < mvIdx);

            execute("drop materialized view m_mv");
            execute("drop table z_dim");
            execute("drop table y_dim");
            execute("drop table a_base");
            drainWalQueue();
            for (int i = 0, n = before.size(); i < n; i++) {
                execute(before.getQuick(i));
            }
            drainWalQueue();

            final ObjList<String> after = dumpDatabase();
            Assert.assertEquals(before.size(), after.size());
            for (int i = 0, n = before.size(); i < n; i++) {
                Assert.assertEquals("statement " + i + " differs", before.getQuick(i), after.getQuick(i));
            }
        });
    }

    @Test
    public void testReplayRecreatesEveryObject() throws Exception {
        assertMemoryLeak(() -> {
            // SHOW CREATE TABLE emits WAL implicitly, so replay needs the WAL default on
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            execute("create table base (ts timestamp, s symbol index, v double) timestamp(ts) partition by day wal dedup upsert keys(ts, s)");
            execute("create table plain (a int, b string)");
            execute("create materialized view mv as (select ts, s, avg(v) v from base sample by 1h) partition by day");
            execute("create view v as (select ts, s from base)");
            drainWalQueue();

            final String inventoryBefore = inventory();
            final ObjList<String> before = dumpDatabase();
            Assert.assertEquals(4, before.size());

            // tear everything down in reverse dependency order
            execute("drop view v");
            execute("drop materialized view mv");
            execute("drop table base");
            execute("drop table plain");
            drainWalQueue();

            // replaying the whole dump on the empty database must recreate every object
            for (int i = 0, n = before.size(); i < n; i++) {
                execute(before.getQuick(i));
            }
            drainWalQueue();

            Assert.assertEquals(inventoryBefore, inventory());

            // the dump is a fixpoint: re-dumping after replay yields byte-identical DDL,
            // including the materialized view and view bodies
            final ObjList<String> after = dumpDatabase();
            Assert.assertEquals(before.size(), after.size());
            for (int i = 0, n = before.size(); i < n; i++) {
                Assert.assertEquals("statement " + i + " differs", before.getQuick(i), after.getQuick(i));
            }
        });
    }

    @Test
    public void testTableDdlRoundTripIsByteIdentical() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            execute("create table base (ts timestamp, s symbol index, v double) timestamp(ts) partition by day wal dedup upsert keys(ts, s)");
            execute("create table plain (a int, b string)");
            drainWalQueue();

            final ObjList<String> before = dumpDatabase();
            Assert.assertEquals(2, before.size());

            execute("drop table base");
            execute("drop table plain");
            drainWalQueue();

            for (int i = 0, n = before.size(); i < n; i++) {
                execute(before.getQuick(i));
            }
            drainWalQueue();

            final ObjList<String> after = dumpDatabase();
            Assert.assertEquals(before.size(), after.size());
            for (int i = 0, n = before.size(); i < n; i++) {
                Assert.assertEquals("statement " + i + " differs", before.getQuick(i), after.getQuick(i));
            }
        });
    }

    @Test
    public void testTablesDumpExercisesCursorContract() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            execute("create table alpha (ts timestamp, v double) timestamp(ts) partition by day wal");
            execute("create table beta (ts timestamp, s symbol) timestamp(ts) partition by day wal");
            drainWalQueue();

            // Drive the cursor through the standard builder so the second cursor pass, toTop()
            // re-read, calculateSize() cross-check, and variable-column checks all run against
            // ShowCreateDatabaseCursor. The hand-rolled dumpDatabase() helper only iterates once,
            // so a toTop() reset bug on this multi-row cursor would otherwise pass undetected.
            assertQuery("SHOW CREATE DATABASE INCLUDE (TABLES)")
                    .noRandomAccess()
                    .returns("""
                            ddl
                            CREATE TABLE 'alpha' (\s
                            \tts TIMESTAMP,
                            \tv DOUBLE
                            ) timestamp(ts) PARTITION BY DAY;
                            CREATE TABLE 'beta' (\s
                            \tts TIMESTAMP,
                            \ts SYMBOL
                            ) timestamp(ts) PARTITION BY DAY;
                            """);
        });
    }

    @Test
    public void testViewOnViewDependencyOrdering() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            execute("create table base (ts timestamp, s symbol, v double) timestamp(ts) partition by day wal");
            // a_view depends on z_view: a plain alphabetical order (a before z) would be unreplayable
            execute("create view z_view as (select ts, s, v from base)");
            execute("create view a_view as (select ts, s from z_view)");
            drainWalQueue();

            final ObjList<String> before = dumpDatabase();
            final String dump = before.toString();
            final int baseIdx = dump.indexOf("CREATE TABLE 'base'");
            final int zIdx = dump.indexOf("CREATE VIEW 'z_view'");
            final int aIdx = dump.indexOf("CREATE VIEW 'a_view'");
            Assert.assertTrue(baseIdx >= 0 && zIdx >= 0 && aIdx >= 0);
            Assert.assertTrue("base must precede z_view", baseIdx < zIdx);
            Assert.assertTrue("dependency z_view must precede dependent a_view", zIdx < aIdx);

            // drop in reverse dependency order, then the dump must replay cleanly
            execute("drop view a_view");
            execute("drop view z_view");
            execute("drop table base");
            drainWalQueue();
            for (int i = 0, n = before.size(); i < n; i++) {
                execute(before.getQuick(i));
            }
            drainWalQueue();

            final ObjList<String> after = dumpDatabase();
            Assert.assertEquals(before.size(), after.size());
            for (int i = 0, n = before.size(); i < n; i++) {
                Assert.assertEquals("statement " + i + " differs", before.getQuick(i), after.getQuick(i));
            }
        });
    }

    @Test
    public void testMatViewOnViewDependencyOrdering() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            // a_mv reads s_view, which is inlined to its base table during compilation, so the plan
            // walk never sees s_view. The view name must still be collected as a dependency, otherwise
            // a_mv (which sorts before s_view) emits first and the replay fails: s_view does not exist
            execute("create table base (ts timestamp, s symbol, v double) timestamp(ts) partition by day wal");
            execute("create view s_view as (select ts, s, v from base)");
            execute("create materialized view a_mv as (select ts, s, avg(v) v from s_view sample by 1d) partition by day");
            drainWalQueue();

            final ObjList<String> before = dumpDatabase();
            final String dump = before.toString();
            final int viewIdx = dump.indexOf("CREATE VIEW 's_view'");
            final int mvIdx = dump.indexOf("CREATE MATERIALIZED VIEW 'a_mv'");
            Assert.assertTrue(viewIdx >= 0 && mvIdx >= 0);
            Assert.assertTrue("the view must precede the materialized view that reads it", viewIdx < mvIdx);

            execute("drop materialized view a_mv");
            execute("drop view s_view");
            execute("drop table base");
            drainWalQueue();
            for (int i = 0, n = before.size(); i < n; i++) {
                execute(before.getQuick(i));
            }
            drainWalQueue();

            final ObjList<String> after = dumpDatabase();
            Assert.assertEquals(before.size(), after.size());
            for (int i = 0, n = before.size(); i < n; i++) {
                Assert.assertEquals("statement " + i + " differs", before.getQuick(i), after.getQuick(i));
            }
        });
    }

    @Test
    public void testViewOnMatViewDependencyOrdering() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            // a_view reads z_mv (a materialized view); matviews are physical and not inlined, so the
            // view graph records the dependency by name. a_view (sorting first) must still emit after z_mv
            execute("create table base (ts timestamp, s symbol, v double) timestamp(ts) partition by day wal");
            execute("create materialized view z_mv as (select ts, s, avg(v) v from base sample by 1d) partition by day");
            execute("create view a_view as (select ts, s from z_mv)");
            drainWalQueue();

            final ObjList<String> before = dumpDatabase();
            final String dump = before.toString();
            final int mvIdx = dump.indexOf("CREATE MATERIALIZED VIEW 'z_mv'");
            final int viewIdx = dump.indexOf("CREATE VIEW 'a_view'");
            Assert.assertTrue(mvIdx >= 0 && viewIdx >= 0);
            Assert.assertTrue("the materialized view must precede the view that reads it", mvIdx < viewIdx);

            execute("drop view a_view");
            execute("drop materialized view z_mv");
            execute("drop table base");
            drainWalQueue();
            for (int i = 0, n = before.size(); i < n; i++) {
                execute(before.getQuick(i));
            }
            drainWalQueue();

            final ObjList<String> after = dumpDatabase();
            Assert.assertEquals(before.size(), after.size());
            for (int i = 0, n = before.size(); i < n; i++) {
                Assert.assertEquals("statement " + i + " differs", before.getQuick(i), after.getQuick(i));
            }
        });
    }

    @Test
    public void testCancellationPropagates() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (ts timestamp, v double) timestamp(ts) partition by day bypass wal");
            execute("create table t2 (ts timestamp, v double) timestamp(ts) partition by day bypass wal");

            // a cancelled dump must surface immediately, never be mistaken for a benign drop race
            final Throwable thrown = runExpectingFailure(new HookedDatabaseFactory(
                    ShowCreateDatabaseRecordCursorFactory.INCLUDE_TABLES,
                    token -> "t2".contentEquals(token.getTableName())
                            ? new ThrowingObjectFactory(null, CairoException.queryCancelled())
                            : null
            ));

            Assert.assertTrue("cancellation must propagate", thrown instanceof CairoException);
            Assert.assertTrue("cancellation must not be swallowed as a drop race", ((CairoException) thrown).isCancellation());
        });
    }

    @Test
    public void testDumpHonorsCircuitBreaker() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (ts timestamp, v double) timestamp(ts) partition by day bypass wal");
            execute("create table t2 (ts timestamp, v double) timestamp(ts) partition by day bypass wal");

            // the whole dump is built eagerly, so it must consult the circuit breaker per object to stay
            // cancellable even when no per-object factory happens to throw (e.g. a plain tables-only dump)
            final SqlExecutionCircuitBreaker original = sqlExecutionContext.getCircuitBreaker();
            final CountingSqlExecutionCircuitBreaker counting = new CountingSqlExecutionCircuitBreaker(original);
            ((SqlExecutionContextImpl) sqlExecutionContext).with(counting);
            try {
                final ObjList<String> statements = dumpDatabase("SHOW CREATE DATABASE INCLUDE (TABLES)");
                Assert.assertEquals(2, statements.size());
                Assert.assertTrue("dump must consult the circuit breaker at least once per object", counting.getCheckCount() >= 2);
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(original);
            }
        });
    }

    @Test
    public void testFilteredDumpEmitsDependentWithoutDependency() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            execute("create table base (ts timestamp, s symbol, v double) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select ts, s, avg(v) v from base sample by 1d) partition by day");
            drainWalQueue();

            // INCLUDE (MATERIALIZED_VIEWS) excludes the base table from the dump, but the mat view is still
            // emitted and its DDL references the absent base - a deliberately partial dump. The factory logs a
            // warning for this case; here we pin the observable contract: the dependent is present, the
            // filtered-out dependency is not.
            final String matViewsOnly = dump("SHOW CREATE DATABASE INCLUDE (MATERIALIZED_VIEWS)");
            Assert.assertTrue(matViewsOnly, matViewsOnly.contains("CREATE MATERIALIZED VIEW 'mv'"));
            Assert.assertTrue("mat view DDL still references its base table", matViewsOnly.contains("base"));
            Assert.assertFalse("filtered-out base table must not be emitted", matViewsOnly.contains("CREATE TABLE 'base'"));
        });
    }

    @Test
    public void testConcurrentDropOfFirstTableSkipsItAndEmitsRest() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table aaa_victim (ts timestamp, v double) timestamp(ts) partition by day bypass wal");
            execute("create table bbb_keep (ts timestamp, v double) timestamp(ts) partition by day bypass wal");
            execute("create table ccc_keep (ts timestamp, v double) timestamp(ts) partition by day bypass wal");

            // aaa_victim sorts first, so the very first emit resolves a table this hook drops in the same
            // window. Proves the loop skips a vanished object even at position zero and still emits the rest.
            final boolean[] fired = {false};
            final ObjList<String> statements = drive(new HookedDatabaseFactory(
                    ShowCreateDatabaseRecordCursorFactory.INCLUDE_TABLES,
                    token -> {
                        if (!fired[0] && "aaa_victim".contentEquals(token.getTableName())) {
                            fired[0] = true;
                            executeQuietly("drop table aaa_victim");
                        }
                        return null; // resolve the real per-object factory against the now-dropped table
                    }
            ));

            Assert.assertTrue("drop hook must have fired", fired[0]);
            final String dump = statements.toString();
            Assert.assertEquals(dump, 2, statements.size());
            Assert.assertFalse("dropped table must be skipped", dump.contains("aaa_victim"));
            Assert.assertTrue(dump, dump.contains("CREATE TABLE 'bbb_keep'"));
            Assert.assertTrue(dump, dump.contains("CREATE TABLE 'ccc_keep'"));
        });
    }

    @Test
    public void testConcurrentDropOfMatViewSkipsIt() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            execute("create table base (ts timestamp, s symbol, v double) timestamp(ts) partition by day wal");
            execute("create materialized view mv_drop as (select ts, s, avg(v) v from base sample by 1d) partition by day");
            drainWalQueue();

            final boolean[] fired = {false};
            final ObjList<String> statements = drive(new HookedDatabaseFactory(
                    ShowCreateDatabaseRecordCursorFactory.INCLUDE_SCHEMA,
                    token -> {
                        if (!fired[0] && "mv_drop".contentEquals(token.getTableName())) {
                            fired[0] = true;
                            executeQuietly("drop materialized view mv_drop");
                            drainWalQueue();
                        }
                        return null;
                    }
            ));

            Assert.assertTrue("materialized view drop hook must have fired", fired[0]);
            final String dump = statements.toString();
            Assert.assertEquals(dump, 1, statements.size());
            Assert.assertTrue(dump, dump.contains("CREATE TABLE 'base'"));
            Assert.assertFalse("dropped materialized view must be skipped", dump.contains("mv_drop"));
        });
    }

    @Test
    public void testConcurrentDropOfViewSkipsIt() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, true);
            execute("create table base (ts timestamp, s symbol, v double) timestamp(ts) partition by day wal");
            execute("create view v_drop as (select ts, s from base)");
            drainWalQueue();

            // A view reads its DDL from an on-disk definition that lingers briefly after a DROP, so the view
            // factory would not throw and the dump would emit stale DDL. The pre-emit liveness check skips the
            // view as soon as it leaves the registry. Drop v_drop while the earlier 'base' object is emitting,
            // so the liveness check (not the per-object factory) is what catches the vanish.
            final boolean[] fired = {false};
            final ObjList<String> statements = drive(new HookedDatabaseFactory(
                    ShowCreateDatabaseRecordCursorFactory.INCLUDE_SCHEMA,
                    token -> {
                        if (!fired[0] && "base".contentEquals(token.getTableName())) {
                            fired[0] = true;
                            executeQuietly("drop view v_drop");
                        }
                        return null;
                    }
            ));

            Assert.assertTrue("view drop hook must have fired", fired[0]);
            final String dump = statements.toString();
            Assert.assertEquals(dump, 1, statements.size());
            Assert.assertTrue(dump, dump.contains("CREATE TABLE 'base'"));
            Assert.assertFalse("dropped view must be skipped", dump.contains("v_drop"));
        });
    }

    @Test
    public void testPreEmitLivenessCheckSkipsTableDroppedBeforeEmit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table aaa_keep (ts timestamp, v double) timestamp(ts) partition by day bypass wal");
            execute("create table zzz_victim (ts timestamp, v double) timestamp(ts) partition by day bypass wal");

            // Drop the later object while the earlier one is emitting: the victim never reaches its per-object
            // factory because the pre-emit liveness check skips it first. Distinct from the backstop tests,
            // where the drop lands inside the victim's own factory and the catch path handles it.
            final boolean[] fired = {false};
            final ObjList<String> statements = drive(new HookedDatabaseFactory(
                    ShowCreateDatabaseRecordCursorFactory.INCLUDE_TABLES,
                    token -> {
                        if (!fired[0] && "aaa_keep".contentEquals(token.getTableName())) {
                            fired[0] = true;
                            executeQuietly("drop table zzz_victim");
                        }
                        return null;
                    }
            ));

            Assert.assertTrue("drop hook must have fired", fired[0]);
            final String dump = statements.toString();
            Assert.assertEquals(dump, 1, statements.size());
            Assert.assertTrue(dump, dump.contains("CREATE TABLE 'aaa_keep'"));
            Assert.assertFalse("object dropped before its emit must be skipped", dump.contains("zzz_victim"));
        });
    }

    @Test
    public void testConcurrentRecreateUnderSameNameSkipsStaleObject() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table aaa_keep (ts timestamp, v double) timestamp(ts) partition by day bypass wal");
            execute("create table victim (ts timestamp, v double) timestamp(ts) partition by day bypass wal");

            // drop then immediately recreate victim under the same name: the snapshotted token is now stale,
            // so the per-object SHOW CREATE resolves a different identity. The best-effort dump skips it rather
            // than emitting a freshly created object that was never part of the snapshot.
            final boolean[] fired = {false};
            final ObjList<String> statements = drive(new HookedDatabaseFactory(
                    ShowCreateDatabaseRecordCursorFactory.INCLUDE_TABLES,
                    token -> {
                        if (!fired[0] && "victim".contentEquals(token.getTableName())) {
                            fired[0] = true;
                            executeQuietly("drop table victim");
                            executeQuietly("create table victim (ts timestamp, x int) timestamp(ts) partition by day bypass wal");
                        }
                        return null;
                    }
            ));

            Assert.assertTrue("recreate hook must have fired", fired[0]);
            final String dump = statements.toString();
            Assert.assertEquals(dump, 1, statements.size());
            Assert.assertTrue(dump, dump.contains("CREATE TABLE 'aaa_keep'"));
            Assert.assertFalse("stale victim must be skipped", dump.contains("victim"));
        });
    }

    @Test
    public void testGenuineCairoExceptionForPresentObjectPropagates() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (ts timestamp, v double) timestamp(ts) partition by day bypass wal");
            execute("create table t2 (ts timestamp, v double) timestamp(ts) partition by day bypass wal");

            // t2 is never dropped; a non-cancellation failure for a still-present object is a real error
            // and must abort the dump rather than be silently skipped.
            final Throwable thrown = runExpectingFailure(new HookedDatabaseFactory(
                    ShowCreateDatabaseRecordCursorFactory.INCLUDE_TABLES,
                    token -> "t2".contentEquals(token.getTableName())
                            ? new ThrowingObjectFactory(null, CairoException.nonCritical().put("disk read boom"))
                            : null
            ));

            Assert.assertTrue("a genuine error must propagate", thrown instanceof CairoException);
            Assert.assertFalse("a genuine error must not be flagged as cancellation", ((CairoException) thrown).isCancellation());
            TestUtils.assertContains(((FlyweightMessageContainer) thrown).getFlyweightMessage(), "disk read boom");
        });
    }

    @Test
    public void testGenuineSqlExceptionForPresentObjectPropagates() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (ts timestamp, v double) timestamp(ts) partition by day bypass wal");
            execute("create table t2 (ts timestamp, v double) timestamp(ts) partition by day bypass wal");

            // t2 is never dropped; a delegate that fails for a still-present object is a real error and must
            // abort the dump rather than be mistaken for a vanished object.
            final Throwable thrown = runExpectingFailure(new HookedDatabaseFactory(
                    ShowCreateDatabaseRecordCursorFactory.INCLUDE_TABLES,
                    token -> "t2".contentEquals(token.getTableName())
                            ? new ThrowingObjectFactory(SqlException.$(0, "boom while building t2"), null)
                            : null
            ));

            Assert.assertTrue("a genuine error must propagate", thrown instanceof SqlException);
            TestUtils.assertContains(((FlyweightMessageContainer) thrown).getFlyweightMessage(), "boom while building t2");
        });
    }

    @Test
    public void testTableReferenceOutOfDateIsSkipped() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (ts timestamp, v double) timestamp(ts) partition by day bypass wal");
            execute("create table t2 (ts timestamp, v double) timestamp(ts) partition by day bypass wal");

            // a stale-reference signal means the object's identity changed between snapshot and emit;
            // the dump skips it deterministically without consulting the registry.
            final boolean[] fired = {false};
            final ObjList<String> statements = drive(new HookedDatabaseFactory(
                    ShowCreateDatabaseRecordCursorFactory.INCLUDE_TABLES,
                    token -> {
                        if ("t2".contentEquals(token.getTableName())) {
                            fired[0] = true;
                            return new ThrowingObjectFactory(null, TableReferenceOutOfDateException.of(token));
                        }
                        return null;
                    }
            ));

            Assert.assertTrue("stale-reference hook must have fired", fired[0]);
            final String dump = statements.toString();
            Assert.assertEquals(dump, 1, statements.size());
            Assert.assertTrue(dump, dump.contains("CREATE TABLE 't1'"));
            Assert.assertFalse("stale-reference object must be skipped", dump.contains("t2"));
        });
    }

    @Test
    public void testTimeoutInterruptionPropagates() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (ts timestamp, v double) timestamp(ts) partition by day bypass wal");
            execute("create table t2 (ts timestamp, v double) timestamp(ts) partition by day bypass wal");

            // a timed-out dump must surface, never be mistaken for a benign drop race
            final Throwable thrown = runExpectingFailure(new HookedDatabaseFactory(
                    ShowCreateDatabaseRecordCursorFactory.INCLUDE_TABLES,
                    token -> "t2".contentEquals(token.getTableName())
                            ? new ThrowingObjectFactory(null, CairoException.queryTimedOut())
                            : null
            ));

            Assert.assertTrue("timeout must propagate", thrown instanceof CairoException);
            Assert.assertTrue("timeout must not be swallowed as a drop race", ((CairoException) thrown).isInterruption());
        });
    }

    private static String inventory() throws Exception {
        sink.clear();
        printSql("SELECT table_name, table_type FROM tables() ORDER BY table_name, table_type");
        return sink.toString();
    }

    private static String dump(String sql) throws Exception {
        return dumpDatabase(sql).toString();
    }

    private static ObjList<String> dumpDatabase() throws Exception {
        return dumpDatabase("SHOW CREATE DATABASE");
    }

    private static ObjList<String> dumpDatabase(String sql) throws Exception {
        final ObjList<String> statements = new ObjList<>();
        try (RecordCursorFactory factory = select(sql)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    statements.add(record.getVarcharA(0).toString());
                }
            }
        }
        return statements;
    }

    // drives a directly-constructed dump factory through the cursor, collecting one statement per row
    private static ObjList<String> drive(ShowCreateDatabaseRecordCursorFactory factory) throws SqlException {
        final ObjList<String> statements = new ObjList<>();
        try (RecordCursorFactory f = factory; RecordCursor cursor = f.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                statements.add(record.getVarcharA(0).toString());
            }
        }
        return statements;
    }

    // runs DDL from inside a per-object hook, where checked exceptions cannot propagate
    private static void executeQuietly(String sql) {
        try {
            execute(sql);
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }

    // drives a dump expected to fail, returning the thrown error (or null) while always closing the factory
    private static Throwable runExpectingFailure(ShowCreateDatabaseRecordCursorFactory factory) {
        try (RecordCursorFactory f = factory) {
            try (RecordCursor cursor = f.getCursor(sqlExecutionContext)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                    // drain
                }
            }
            return null;
        } catch (Throwable t) {
            return t;
        }
    }

    private static GenericRecordMetadata stubMetadata() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("ddl", ColumnType.VARCHAR));
        return metadata;
    }

    @FunctionalInterface
    private interface PerObjectHook {
        // Returns a replacement per-object factory for this token, or null to fall back to the real one.
        // The hook may perform a side effect (such as dropping the object) to simulate a concurrent DDL
        // landing between the token snapshot and this object's emit.
        RecordCursorFactory replace(TableToken token);
    }

    // A dump factory that routes each per-object SHOW CREATE through a test hook, so a test can drop the
    // object or substitute a throwing delegate exactly at emit time.
    private static final class HookedDatabaseFactory extends ShowCreateDatabaseRecordCursorFactory {
        private final PerObjectHook hook;

        private HookedDatabaseFactory(int includeMask, PerObjectHook hook) {
            super(includeMask);
            this.hook = hook;
        }

        @Override
        protected RecordCursorFactory matViewFactory(TableToken token) {
            final RecordCursorFactory replacement = hook.replace(token);
            return replacement != null ? replacement : super.matViewFactory(token);
        }

        @Override
        protected RecordCursorFactory tableFactory(TableToken token) {
            final RecordCursorFactory replacement = hook.replace(token);
            return replacement != null ? replacement : super.tableFactory(token);
        }

        @Override
        protected RecordCursorFactory viewFactory(TableToken token) {
            final RecordCursorFactory replacement = hook.replace(token);
            return replacement != null ? replacement : super.viewFactory(token);
        }
    }

    // A per-object factory whose getCursor always throws, simulating a delegate failure during emit.
    private static final class ThrowingObjectFactory extends AbstractRecordCursorFactory {
        private final RuntimeException runtimeError;
        private final SqlException sqlError;

        private ThrowingObjectFactory(SqlException sqlError, RuntimeException runtimeError) {
            super(stubMetadata());
            this.sqlError = sqlError;
            this.runtimeError = runtimeError;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            if (sqlError != null) {
                throw sqlError;
            }
            throw runtimeError;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("throwing");
        }
    }
}
