/*******************************************************************************
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
import io.questdb.metrics.QueryTracingJob;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

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
                public void onColumnDropped(TableToken tableToken, CharSequence columnName, boolean cascadePermissions) {
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertTrue(Chars.equals("v", columnName));
                    Assert.assertFalse(cascadePermissions);
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
                public void onTableOrViewOrMatViewDropped(String tableName, boolean cascadePermissions) {
                    assertEquals("tab2", tableName);
                    Assert.assertFalse(cascadePermissions);
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

            engine.execute("CREATE TABLE tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            engine.execute("ALTER TABLE tab ADD COLUMN z VARCHAR");
            drainWalQueue();
            engine.execute("ALTER TABLE tab RENAME COLUMN z TO v");
            drainWalQueue();
            engine.execute("ALTER TABLE tab DROP COLUMN v");
            drainWalQueue();
            engine.execute("RENAME TABLE tab TO tab2");
            drainWalQueue();
            engine.execute("DROP TABLE tab2");
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

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName, boolean cascadePermissions) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(String tableName, boolean cascadePermissions) {
                    Assert.assertFalse(cascadePermissions);
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
            engine.execute("CREATE TABLE tab1 (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            engine.execute("CREATE TABLE tab2 (ts TIMESTAMP, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            engine.execute("CREATE VIEW v AS (SELECT ts, avg(x) FROM tab1 SAMPLE BY 1m)");

            engine.execute("DROP ALL");

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(3, callbackCounters[3]);
            Assert.assertEquals(3, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);

            engine.execute("CREATE TABLE tab1 (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            engine.execute("CREATE TABLE tab2 (ts TIMESTAMP, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            engine.execute("DROP ALL TABLES");

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(5, callbackCounters[3]);
            Assert.assertEquals(5, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);
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

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName, boolean cascadePermissions) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(String tableName, boolean cascadePermissions) {
                    Assert.assertFalse(cascadePermissions);
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
            engine.execute("CREATE TABLE tab1 (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            engine.execute("CREATE TABLE tab2 (ts TIMESTAMP, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            engine.execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, avg(x) FROM tab1 SAMPLE BY 1m) PARTITION BY DAY");
            drainWalQueue();
            engine.execute("CREATE VIEW v AS (SELECT ts, avg(x) FROM tab1 SAMPLE BY 1m)");
            drainWalQueue();

            engine.execute("DROP ALL");
            drainWalQueue();

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(4, callbackCounters[3]);
            Assert.assertEquals(4, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);

            engine.execute("CREATE TABLE tab1 (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            engine.execute("CREATE TABLE tab2 (ts TIMESTAMP, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            engine.execute("DROP ALL TABLES");
            drainWalQueue();

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(6, callbackCounters[3]);
            Assert.assertEquals(6, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);
        });
    }

    @Test
    public void testDdlListenerCascadePermissionsOnColumnDrop() throws Exception {
        assertMemoryLeak(() -> {
            final boolean[] cascadeReceived = new boolean[1];
            final int[] callCount = new int[1];

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName, boolean cascadePermissions) {
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertTrue(Chars.equals("z", columnName));
                    cascadeReceived[0] = cascadePermissions;
                    callCount[0]++;
                }
            });

            engine.execute("CREATE TABLE tab(ts TIMESTAMP, x LONG, z INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            try (TableWriter writer = getWriter("tab")) {
                writer.removeColumn("z", AllowAllSecurityContext.INSTANCE, true);
            }

            Assert.assertEquals(1, callCount[0]);
            Assert.assertTrue(cascadeReceived[0]);

            // cleanup
            engine.execute("DROP TABLE tab");
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
                public void onColumnDropped(TableToken tableToken, CharSequence columnName, boolean cascadePermissions) {
                    assertEquals("tab", tableToken.getTableName());
                    Assert.assertTrue(Chars.equals("v", columnName));
                    Assert.assertFalse(cascadePermissions);
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
                public void onTableOrViewOrMatViewDropped(String tableName, boolean cascadePermissions) {
                    assertEquals("tab2", tableName);
                    Assert.assertFalse(cascadePermissions);
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

            engine.execute("CREATE TABLE tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            engine.execute("ALTER TABLE tab ADD COLUMN z VARCHAR");
            engine.execute("ALTER TABLE tab RENAME COLUMN z TO v");
            engine.execute("ALTER TABLE tab DROP COLUMN v");
            engine.execute("RENAME TABLE tab TO tab2");
            engine.execute("DROP TABLE tab2");

            for (int callbackCounter : callbackCounters) {
                Assert.assertEquals(1, callbackCounter);
            }
        });
    }

    @Test
    public void testDdlListenerWithMatView() throws Exception {
        assertMemoryLeak(() -> {
            engine.setDdlListener(DefaultDdlListener.INSTANCE);

            engine.execute("CREATE TABLE tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            final int[] callbackCounters = new int[6];

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName, boolean cascadePermissions) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(String tableName, boolean cascadePermissions) {
                    assertEquals("mv", tableName);
                    Assert.assertFalse(cascadePermissions);
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

            engine.execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m) PARTITION BY DAY");
            drainWalQueue();
            engine.execute("DROP MATERIALIZED VIEW mv");
            drainWalQueue();

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(1, callbackCounters[3]);
            Assert.assertEquals(1, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);

            // cleanup
            engine.setDdlListener(DefaultDdlListener.INSTANCE);
            engine.execute("DROP TABLE tab");
            drainWalQueue();
        });
    }

    @Test
    public void testDdlListenerWithView() throws Exception {
        assertMemoryLeak(() -> {
            engine.setDdlListener(DefaultDdlListener.INSTANCE);

            engine.execute("CREATE TABLE tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            final int[] callbackCounters = new int[6];

            engine.setDdlListener(new DefaultDdlListener() {
                @Override
                public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
                    callbackCounters[0]++;
                }

                @Override
                public void onColumnDropped(TableToken tableToken, CharSequence columnName, boolean cascadePermissions) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(String tableName, boolean cascadePermissions) {
                    assertEquals("v", tableName);
                    Assert.assertFalse(cascadePermissions);
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

            engine.execute("CREATE VIEW v AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m)");
            drainWalQueue();
            engine.execute("ALTER VIEW v AS (SELECT ts, max(x) FROM tab SAMPLE BY 10m)");
            drainWalQueue();
            engine.execute("DROP VIEW v");
            drainWalQueue();

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(1, callbackCounters[3]);
            Assert.assertEquals(1, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);

            // cleanup
            engine.setDdlListener(DefaultDdlListener.INSTANCE);
            engine.execute("DROP TABLE tab");
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
                public void onColumnDropped(TableToken tableToken, CharSequence columnName, boolean cascadePermissions) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(String tableName, boolean cascadePermissions) {
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
            engine.execute("DROP TABLE IF EXISTS tab");

            for (int callbackCounter : callbackCounters) {
                Assert.assertEquals(0, callbackCounter);
            }

            // create and then drop should fire
            engine.execute("CREATE TABLE IF NOT EXISTS tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            engine.execute("DROP TABLE IF EXISTS tab");

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
                public void onColumnDropped(TableToken tableToken, CharSequence columnName, boolean cascadePermissions) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(String tableName, boolean cascadePermissions) {
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

            engine.execute("DROP TABLE IF EXISTS tab");
            drainWalQueue();
            engine.execute("DROP MATERIALIZED VIEW IF EXISTS mv");
            drainWalQueue();
            engine.execute("DROP VIEW IF EXISTS v");
            drainWalQueue();

            for (int callbackCounter : callbackCounters) {
                Assert.assertEquals(0, callbackCounter);
            }

            engine.execute("CREATE TABLE IF NOT EXISTS tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            engine.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m) PARTITION BY DAY");
            drainWalQueue();
            engine.execute("CREATE VIEW IF NOT EXISTS v AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m)");
            drainWalQueue();

            engine.execute("DROP VIEW IF EXISTS v");
            drainWalQueue();
            engine.execute("DROP MATERIALIZED VIEW IF EXISTS mv");
            drainWalQueue();
            engine.execute("DROP TABLE IF EXISTS tab");
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
                public void onColumnDropped(TableToken tableToken, CharSequence columnName, boolean cascadePermissions) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(String tableName, boolean cascadePermissions) {
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

            engine.execute("CREATE TABLE IF NOT EXISTS tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            Assert.assertEquals(0, callbackCounters[0]);
            Assert.assertEquals(0, callbackCounters[1]);
            Assert.assertEquals(0, callbackCounters[2]);
            Assert.assertEquals(0, callbackCounters[3]);
            Assert.assertEquals(1, callbackCounters[4]);
            Assert.assertEquals(0, callbackCounters[5]);

            // clear counters
            callbackCounters[4] = 0;

            // second time should not fire
            engine.execute("CREATE TABLE IF NOT EXISTS tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            for (int callbackCounter : callbackCounters) {
                Assert.assertEquals(0, callbackCounter);
            }

            // cleanup
            engine.execute("DROP TABLE tab");
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
                public void onColumnDropped(TableToken tableToken, CharSequence columnName, boolean cascadePermissions) {
                    callbackCounters[1]++;
                }

                @Override
                public void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName) {
                    callbackCounters[2]++;
                }

                @Override
                public void onTableOrViewOrMatViewDropped(String tableName, boolean cascadePermissions) {
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

            engine.execute("CREATE TABLE IF NOT EXISTS tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            engine.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m) PARTITION BY DAY");
            drainWalQueue();
            engine.execute("CREATE VIEW IF NOT EXISTS v AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m)");
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
            engine.execute("CREATE TABLE IF NOT EXISTS tab(ts TIMESTAMP, x LONG, y BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            engine.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m) PARTITION BY DAY");
            drainWalQueue();
            engine.execute("CREATE VIEW IF NOT EXISTS v AS (SELECT ts, avg(x) FROM tab SAMPLE BY 1m)");
            drainWalQueue();

            for (int callbackCounter : callbackCounters) {
                Assert.assertEquals(0, callbackCounter);
            }

            // cleanup
            engine.execute("DROP VIEW v");
            engine.execute("DROP MATERIALIZED VIEW mv");
            engine.execute("DROP TABLE tab");
            drainWalQueue();
        });
    }
}
