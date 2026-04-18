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

package io.questdb.test.cairo;

import io.questdb.cairo.DdlListener;
import io.questdb.cairo.DefaultDdlListener;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.metrics.QueryTracingJob;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static io.questdb.cairo.TableUtils.TABLE_DOES_NOT_EXIST;
import static io.questdb.test.tools.TestUtils.assertContains;
import static io.questdb.test.tools.TestUtils.assertEquals;

public class DdlListenerTest extends AbstractCairoTest {
    @After
    public void tearDown() throws Exception {
        // reset DDL listener
        engine.setDdlListener(DefaultDdlListener.INSTANCE);
        super.tearDown();
    }

    @Test
    public void testDdlListenerWithTable() throws Exception {
        assertMemoryLeak(() -> {
            final int[] callbackCounters = new int[6];

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    assertEquals("admin", securityContext.getPrincipal());
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertTrue(Chars.equals("z", columnName));
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName) {
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertTrue(Chars.equals("v", columnName));
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertTrue(Chars.equals("z", oldColumnName));
                    Assert.assertTrue(Chars.equals("v", newColumnName));
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(TableToken tableToken) {
                    assertEquals("tab2", tableToken.getTableName());
                    callbackCounters[3]++;
                }

                @Override
                public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                    assertEquals("admin", securityContext.getPrincipal());
                    assertEquals("tab", tableToken.getTableName());
                    // TABLE_KIND_REGULAR_TABLE is used for tables, views and mat views too
                    // table kind only distinguishes between regular vs. temp parquet export tables
                    Assert.assertEquals(TableUtils.TABLE_KIND_REGULAR_TABLE, tableKind);
                    callbackCounters[4]++;
                }

                @Override
                public void onTableRenamed(TableToken oldTableToken, TableToken newTableToken) {
                    assertEquals("tab", oldTableToken.getTableName());
                    assertEquals("tab2", newTableToken.getTableName());
                    callbackCounters[5]++;
                }
            });

            execute("CREATE TABLE tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("ALTER TABLE tab ADD COLUMN z VARCHAR");
            drainWalQueue();
            execute("ALTER TABLE tab RENAME COLUMN z TO v");
            drainWalQueue();
            execute("ALTER TABLE tab DROP COLUMN v");
            drainWalQueue();
            execute("RENAME TABLE tab TO tab2");
            drainWalQueue();
            execute("DROP TABLE tab2");
            drainWalQueue();

            for (int callbackCounter : callbackCounters) {
                Assert.assertEquals(1, callbackCounter);
            }
        });
    }

    @Test
    public void testDdlListenerDropAllNonWal() throws Exception {
        assertMemoryLeak(() -> {
            final int[] callbackCounters = new int[6];
            final Set<String> droppedNames = new HashSet<>();

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(TableToken tableToken) {
                    droppedNames.add(tableToken.getTableName());
                    callbackCounters[3]++;
                }

                @Override
                public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                    callbackCounters[4]++;
                }

                @Override
                public void onTableRenamed(TableToken oldTableToken, TableToken newTableToken) {
                    callbackCounters[5]++;
                }
            });

            // Create 2 non-WAL tables and 1 view
            execute("CREATE TABLE tab1 (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE tab2 (ts TIMESTAMP, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE VIEW v AS (SELECT ts, min(x) FROM tab1 SAMPLE BY 1m)");

            execute("DROP ALL");

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(3, callbackCounters[3]);
            Assert.assertEquals(3, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);
            Assert.assertEquals(Set.of("tab1", "tab2", "v"), droppedNames);

            droppedNames.clear();
            execute("CREATE TABLE tab1 (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE tab2 (ts TIMESTAMP, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            execute("DROP ALL TABLES");

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(5, callbackCounters[3]);
            Assert.assertEquals(5, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);
            Assert.assertEquals(Set.of("tab1", "tab2"), droppedNames);
        });
    }

    @Test
    public void testDdlListenerBypassedForSystemTables() throws Exception {
        assertMemoryLeak(() -> {
            final DdlListener custom = new DefaultDdlListener() {
            };
            engine.setDdlListener(custom);

            Assert.assertSame(DefaultDdlListener.INSTANCE, engine.getDdlListener(QueryTracingJob.TABLE_NAME));
            Assert.assertSame(custom, engine.getDdlListener("user_table"));
        });
    }

    @Test
    public void testDdlListenerDropAll() throws Exception {
        assertMemoryLeak(() -> {
            final int[] callbackCounters = new int[6];
            final Set<String> droppedNames = new HashSet<>();

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(TableToken tableToken) {
                    droppedNames.add(tableToken.getTableName());
                    callbackCounters[3]++;
                }

                @Override
                public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                    callbackCounters[4]++;
                }

                @Override
                public void onTableRenamed(TableToken oldTableToken, TableToken newTableToken) {
                    callbackCounters[5]++;
                }
            });

            // Create 2 tables, 1 materialized view, 1 view
            execute("CREATE TABLE tab1 (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE tab2 (ts TIMESTAMP, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, avg(x) FROM tab1 SAMPLE BY 1m) PARTITION BY DAY");
            drainWalQueue();
            execute("CREATE VIEW v AS (SELECT ts, avg(x) FROM tab1 SAMPLE BY 1m)");
            drainWalQueue();

            execute("DROP ALL");
            drainWalQueue();

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(4, callbackCounters[3]);
            Assert.assertEquals(4, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);
            Assert.assertEquals(Set.of("tab1", "tab2", "mv", "v"), droppedNames);

            droppedNames.clear();
            execute("CREATE TABLE tab1 (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE tab2 (ts TIMESTAMP, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            execute("DROP ALL TABLES");
            drainWalQueue();

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(6, callbackCounters[3]);
            Assert.assertEquals(6, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);
            Assert.assertEquals(Set.of("tab1", "tab2"), droppedNames);
        });
    }

    @Test
    public void testDdlListenerOnColumnDrop() throws Exception {
        assertMemoryLeak(() -> {
            final int[] callCount = new int[1];

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName) {
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertTrue(Chars.equals("z", columnName));
                    callCount[0]++;
                }
            });

            execute("CREATE TABLE tab(ts TIMESTAMP, x LONG, z INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            try (TableWriter writer = getWriter("tab")) {
                writer.removeColumn("z", AllowAllSecurityContext.INSTANCE);
            }

            Assert.assertEquals(1, callCount[0]);

            // cleanup
            execute("DROP TABLE tab");
        });
    }

    @Test
    public void testDdlListenerExceptionPropagation() throws Exception {
        assertMemoryLeak(() -> {
            final DefaultDdlListener throwingListener = new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    throw new RuntimeException("onColumnAdded");
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName) {
                    throw new RuntimeException("onColumnDropped");
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    throw new RuntimeException("onColumnRenamed");
                }

                @Override
                public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                    throw new RuntimeException("onTableOrViewOrMatViewCreated");
                }

                @Override
                public void onTableOrViewOrMatViewDropped(TableToken tableToken) {
                    throw new RuntimeException("onTableOrViewOrMatViewDropped");
                }

                @Override
                public void onTableRenamed(TableToken oldTableToken, TableToken newTableToken) {
                    throw new RuntimeException("onTableRenamed");
                }
            };

            // set up table with no-op listener, then switch to the throwing one
            execute("CREATE TABLE tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            engine.setDdlListener(throwingListener);

            final TableToken tableToken = engine.verifyTableName("tab");

            // ADD COLUMN
            assertListenerException("onColumnAdded",
                    () -> execute("ALTER TABLE tab ADD COLUMN z VARCHAR"));
            drainWalQueue();
            // ADD COLUMN is not transactional, callback of the listener is fired after the column has been added.
            // The column should exist even when the callback fails.
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                Assert.assertEquals(3, metadata.getColumnIndexQuiet("z"));
            }

            // RENAME COLUMN
            assertListenerException("onColumnRenamed",
                    () -> execute("ALTER TABLE tab RENAME COLUMN z TO v"));
            drainWalQueue();
            // RENAME COLUMN is not transactional, callback of the listener is fired after the column has been renamed.
            // The column should be renamed even when the callback fails.
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                Assert.assertEquals(3, metadata.getColumnIndexQuiet("v"));
                Assert.assertEquals(-1, metadata.getColumnIndexQuiet("z"));
            }

            // DROP COLUMN
            assertListenerException("onColumnDropped",
                    () -> execute("ALTER TABLE tab DROP COLUMN v"));
            drainWalQueue();
            // DROP COLUMN is not transactional, callback of the listener is fired after the column has been dropped.
            // The column should be removed even when the callback fails.
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                Assert.assertEquals(-1, metadata.getColumnIndexQuiet("v"));
            }

            // CREATE MATERIALIZED VIEW
            assertListenerException("onTableOrViewOrMatViewCreated",
                    () -> execute("CREATE MATERIALIZED VIEW mv3 AS (SELECT ts, avg(x) AS avg_x FROM tab SAMPLE BY 10m) PARTITION BY DAY"));
            drainWalQueue();
            // CREATE MATERIALIZED VIEW is transactional - the mat view is not created, if the DDL listener throws
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("mv3"));
            Assert.assertNull(engine.getTableTokenIfExists("mv3"));

            // CREATE VIEW
            assertListenerException("onTableOrViewOrMatViewCreated",
                    () -> execute("CREATE VIEW v3 AS (SELECT ts, avg(x) AS avg_x FROM tab SAMPLE BY 10m)"));
            drainWalQueue();
            // CREATE VIEW is transactional - the view is not created, if the DDL listener throws
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("v3"));
            Assert.assertNull(engine.getTableTokenIfExists("v3"));

            // RENAME TABLE
            assertListenerException("onTableRenamed",
                    () -> execute("RENAME TABLE tab TO tab2"));
            drainWalQueue();
            // RENAME TABLE is not transactional, callback of the listener is fired after the table has been renamed.
            // The rename should succeed even when the callback fails.
            Assert.assertNull(engine.getTableTokenIfExists("tab"));
            Assert.assertNotNull(engine.getTableTokenIfExists("tab2"));

            // DROP TABLE
            assertListenerException("onTableOrViewOrMatViewDropped",
                    () -> execute("DROP TABLE tab2"));
            drainWalQueue();
            // DROP TABLE is not transactional, callback of the listener is fired after the table has been dropped.
            // The drop should succeed even when the callback fails.
            Assert.assertNull(engine.getTableTokenIfExists("tab2"));

            // CREATE TABLE - non-WAL
            assertListenerException("onTableOrViewOrMatViewCreated",
                    () -> execute("CREATE TABLE tab3(ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL"));
            // CREATE TABLE is transactional - the table is not created, if the DDL listener throws
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("tab3"));
            Assert.assertNull(engine.getTableTokenIfExists("tab3"));

            // CREATE TABLE - WAL
            assertListenerException("onTableOrViewOrMatViewCreated",
                    () -> execute("CREATE TABLE tab3(ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL"));
            drainWalQueue();
            // CREATE TABLE is transactional - the table is not created, if the DDL listener throws
            Assert.assertEquals(TABLE_DOES_NOT_EXIST, engine.getTableStatus("tab3"));
            Assert.assertNull(engine.getTableTokenIfExists("tab3"));

            // cleanup
            engine.setDdlListener(DefaultDdlListener.INSTANCE);
        });
    }

    @Test
    public void testDdlListenerWithNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            final int[] callbackCounters = new int[6];

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    assertEquals("admin", securityContext.getPrincipal());
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertTrue(Chars.equals("z", columnName));
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName) {
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertTrue(Chars.equals("v", columnName));
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertTrue(Chars.equals("z", oldColumnName));
                    Assert.assertTrue(Chars.equals("v", newColumnName));
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(TableToken tableToken) {
                    assertEquals("tab2", tableToken.getTableName());
                    callbackCounters[3]++;
                }

                @Override
                public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                    assertEquals("admin", securityContext.getPrincipal());
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertEquals(TableUtils.TABLE_KIND_REGULAR_TABLE, tableKind);
                    callbackCounters[4]++;
                }

                @Override
                public void onTableRenamed(TableToken oldTableToken, TableToken newTableToken) {
                    assertEquals("tab", oldTableToken.getTableName());
                    assertEquals("tab2", newTableToken.getTableName());
                    callbackCounters[5]++;
                }
            });

            execute("CREATE TABLE tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("ALTER TABLE tab ADD COLUMN z VARCHAR");
            execute("ALTER TABLE tab RENAME COLUMN z TO v");
            execute("ALTER TABLE tab DROP COLUMN v");
            execute("RENAME TABLE tab TO tab2");
            execute("DROP TABLE tab2");

            for (int callbackCounter : callbackCounters) {
                Assert.assertEquals(1, callbackCounter);
            }
        });
    }

    @Test
    public void testDdlListenerWithMatView() throws Exception {
        assertMemoryLeak(() -> {
            engine.setDdlListener(DefaultDdlListener.INSTANCE);

            execute("CREATE TABLE tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            final int[] callbackCounters = new int[6];

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(TableToken tableToken) {
                    assertEquals("mv", tableToken.getTableName());
                    callbackCounters[3]++;
                }

                @Override
                public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                    assertEquals("admin", securityContext.getPrincipal());
                    assertEquals("mv", tableToken.getTableName());
                    // TABLE_KIND_REGULAR_TABLE is used for tables, views and mat views too
                    // table kind only distinguishes between regular vs. temp parquet export tables
                    Assert.assertEquals(TableUtils.TABLE_KIND_REGULAR_TABLE, tableKind);
                    callbackCounters[4]++;
                }

                @Override
                public void onTableRenamed(TableToken oldTableToken, TableToken newTableToken) {
                    callbackCounters[5]++;
                }
            });

            execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m) PARTITION BY DAY");
            drainWalQueue();
            execute("DROP MATERIALIZED VIEW mv");
            drainWalQueue();

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(1, callbackCounters[3]);
            Assert.assertEquals(1, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);

            // cleanup
            engine.setDdlListener(DefaultDdlListener.INSTANCE);
            execute("DROP TABLE tab");
            drainWalQueue();
        });
    }

    @Test
    public void testDdlListenerWithView() throws Exception {
        assertMemoryLeak(() -> {
            engine.setDdlListener(DefaultDdlListener.INSTANCE);

            execute("CREATE TABLE tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            final int[] callbackCounters = new int[6];

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(TableToken tableToken) {
                    assertEquals("v", tableToken.getTableName());
                    callbackCounters[3]++;
                }

                @Override
                public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                    assertEquals("admin", securityContext.getPrincipal());
                    assertEquals("v", tableToken.getTableName());
                    // TABLE_KIND_REGULAR_TABLE is used for tables, views and mat views too
                    // table kind only distinguishes between regular vs. temp parquet export tables
                    Assert.assertEquals(TableUtils.TABLE_KIND_REGULAR_TABLE, tableKind);
                    callbackCounters[4]++;
                }

                @Override
                public void onTableRenamed(TableToken oldTableToken, TableToken newTableToken) {
                    callbackCounters[5]++;
                }
            });

            execute("CREATE VIEW v AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m)");
            drainWalQueue();
            execute("ALTER VIEW v AS (SELECT ts, max(x) FROM tab SAMPLE BY 10m)");
            drainWalQueue();
            execute("DROP VIEW v");
            drainWalQueue();

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(1, callbackCounters[3]);
            Assert.assertEquals(1, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);

            // cleanup
            engine.setDdlListener(DefaultDdlListener.INSTANCE);
            execute("DROP TABLE tab");
            drainWalQueue();
        });
    }

    @Test
    public void testDropIfExistsNonWal() throws Exception {
        assertMemoryLeak(() -> {
            final int[] callbackCounters = new int[6];

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(TableToken tableToken) {
                    callbackCounters[3]++;
                }

                @Override
                public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                    callbackCounters[4]++;
                }

                @Override
                public void onTableRenamed(TableToken oldTableToken, TableToken newTableToken) {
                    callbackCounters[5]++;
                }
            });

            // dropping non-existent table should not fire
            execute("DROP TABLE IF EXISTS tab");

            for (int callbackCounter : callbackCounters) {
                Assert.assertEquals(0, callbackCounter);
            }

            // create and then drop should fire
            execute("CREATE TABLE IF NOT EXISTS tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("DROP TABLE IF EXISTS tab");

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(1, callbackCounters[3]);
            Assert.assertEquals(1, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);
        });
    }

    @Test
    public void testDropIfExists() throws Exception {
        assertMemoryLeak(() -> {
            final int[] callbackCounters = new int[6];

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(TableToken tableToken) {
                    callbackCounters[3]++;
                }

                @Override
                public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                    callbackCounters[4]++;
                }

                @Override
                public void onTableRenamed(TableToken oldTableToken, TableToken newTableToken) {
                    callbackCounters[5]++;
                }
            });

            execute("DROP TABLE IF EXISTS tab");
            drainWalQueue();
            execute("DROP MATERIALIZED VIEW IF EXISTS mv");
            drainWalQueue();
            execute("DROP VIEW IF EXISTS v");
            drainWalQueue();

            for (int callbackCounter : callbackCounters) {
                Assert.assertEquals(0, callbackCounter);
            }

            execute("CREATE TABLE IF NOT EXISTS tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m) PARTITION BY DAY");
            drainWalQueue();
            execute("CREATE VIEW IF NOT EXISTS v AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m)");
            drainWalQueue();

            execute("DROP VIEW IF EXISTS v");
            drainWalQueue();
            execute("DROP MATERIALIZED VIEW IF EXISTS mv");
            drainWalQueue();
            execute("DROP TABLE IF EXISTS tab");
            drainWalQueue();

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(3, callbackCounters[3]);
            Assert.assertEquals(3, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);
        });
    }

    @Test
    public void testCreateIfNotExistsNonWal() throws Exception {
        assertMemoryLeak(() -> {
            final int[] callbackCounters = new int[6];

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(TableToken tableToken) {
                    callbackCounters[3]++;
                }

                @Override
                public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                    callbackCounters[4]++;
                }

                @Override
                public void onTableRenamed(TableToken oldTableToken, TableToken newTableToken) {
                    callbackCounters[5]++;
                }
            });

            execute("CREATE TABLE IF NOT EXISTS tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(0, callbackCounters[3]);
            Assert.assertEquals(1, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);

            // clear counters
            callbackCounters[4] = 0;

            // second time should not fire
            execute("CREATE TABLE IF NOT EXISTS tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            for (int callbackCounter : callbackCounters) {
                Assert.assertEquals(0, callbackCounter);
            }

            // cleanup
            engine.setDdlListener(DefaultDdlListener.INSTANCE);
            execute("DROP TABLE tab");
        });
    }

    @Test
    public void testCreateIfNotExists() throws Exception {
        assertMemoryLeak(() -> {
            final int[] callbackCounters = new int[6];

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(TableToken tableToken) {
                    callbackCounters[3]++;
                }

                @Override
                public void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                    callbackCounters[4]++;
                }

                @Override
                public void onTableRenamed(TableToken oldTableToken, TableToken newTableToken) {
                    callbackCounters[5]++;
                }
            });

            execute("CREATE TABLE IF NOT EXISTS tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m) PARTITION BY DAY");
            drainWalQueue();
            execute("CREATE VIEW IF NOT EXISTS v AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m)");
            drainWalQueue();

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(0, callbackCounters[3]);
            Assert.assertEquals(3, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);

            // clear counters
            callbackCounters[4] = 0;

            // second time should not fire
            execute("CREATE TABLE IF NOT EXISTS tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m) PARTITION BY DAY");
            drainWalQueue();
            execute("CREATE VIEW IF NOT EXISTS v AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m)");
            drainWalQueue();

            for (int callbackCounter : callbackCounters) {
                Assert.assertEquals(0, callbackCounter);
            }

            // cleanup
            engine.setDdlListener(DefaultDdlListener.INSTANCE);
            execute("DROP VIEW v");
            execute("DROP MATERIALIZED VIEW mv");
            execute("DROP TABLE tab");
            drainWalQueue();
        });
    }

    @Test
    public void testDropAllContinuesWhenListenerThrows() throws Exception {
        assertMemoryLeak(() -> {
            final Set<String> droppedNames = new HashSet<>();

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onTableOrViewOrMatViewDropped(TableToken tableToken) {
                    droppedNames.add(tableToken.getTableName());
                    throw new RuntimeException("listener error on " + tableToken);
                }
            });

            execute("CREATE TABLE tab1 (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE tab2 (ts TIMESTAMP, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE tab3 (ts TIMESTAMP, z INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            try {
                execute("DROP ALL");
                Assert.fail("expected CairoException for listener failures");
            } catch (Exception e) {
                // DROP ALL collects listener failures and reports them
                assertContains(e.getMessage(), "Failures while dropping tables, views and materialized views [");
                assertContains(e.getMessage(), "'tab1': listener error");
                assertContains(e.getMessage(), "'tab2': listener error");
                assertContains(e.getMessage(), "'tab3': listener error");
            }

            // All 3 tables must have been dropped despite the listener throwing on each one
            Assert.assertEquals(Set.of("tab1", "tab2", "tab3"), droppedNames);
            Assert.assertNull(engine.getTableTokenIfExists("tab1"));
            Assert.assertNull(engine.getTableTokenIfExists("tab2"));
            Assert.assertNull(engine.getTableTokenIfExists("tab3"));
        });
    }

    private static void assertListenerException(String expectedMessage, ThrowingRunnable action) {
        try {
            action.run();
            Assert.fail("expected listener exception: " + expectedMessage);
        } catch (Throwable t) {
            for (Throwable cause = t; cause != null; cause = cause.getCause()) {
                if (expectedMessage.equals(cause.getMessage())) {
                    return;
                }
            }
            Assert.fail("expected cause with message '" + expectedMessage + "' but got: " + t);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
