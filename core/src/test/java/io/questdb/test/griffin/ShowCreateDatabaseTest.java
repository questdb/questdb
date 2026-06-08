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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
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
            // touch a system table so it materializes in the registry
            execute("create table bar (ts timestamp, v double) timestamp(ts) partition by day wal");
            drainWalQueue();

            final String dump = dumpDatabase().toString();
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

    private static String inventory() throws Exception {
        sink.clear();
        printSql("SELECT table_name, table_type FROM tables() ORDER BY table_name, table_type");
        return sink.toString();
    }

    private static ObjList<String> dumpDatabase() throws Exception {
        final ObjList<String> statements = new ObjList<>();
        try (RecordCursorFactory factory = select("SHOW CREATE DATABASE")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    statements.add(record.getVarcharA(0).toString());
                }
            }
        }
        return statements;
    }
}
