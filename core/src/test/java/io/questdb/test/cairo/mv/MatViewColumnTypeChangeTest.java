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

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class MatViewColumnTypeChangeTest extends AbstractCairoTest {

    @Test
    public void testFullRefreshConvertsColumnAfterBaseColumnTypeChange() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 5), ('2024-01-01T01:00:00.000000Z', 7)");
            execute("CREATE MATERIALIZED VIEW mv AS SELECT ts, max(x) m FROM base SAMPLE BY 1h");
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base ALTER COLUMN x TYPE LONG");
            execute("INSERT INTO base VALUES ('2024-01-01T02:00:00.000000Z', 9)");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid\tchange column type operation\n");

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid\t\n");
            assertQuery("SELECT ts, m, typeOf(m) FROM mv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tm\ttypeOf
                            2024-01-01T00:00:00.000000Z\t5\tINT
                            2024-01-01T01:00:00.000000Z\t7\tINT
                            2024-01-01T02:00:00.000000Z\t9\tINT
                            """);
        });
    }

    @Test
    public void testFullRefreshInvalidatesViewWhenBaseColumnTypeBecomesInconvertible() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00.000000Z', 5), ('2024-01-01T01:00:00.000000Z', 7)");
            execute("CREATE MATERIALIZED VIEW mv AS SELECT ts, first(x) f FROM base SAMPLE BY 1h");
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base ALTER COLUMN x TYPE SYMBOL");
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid\t[-1]: view column type does not match its query, recreate the view [column=f, view=LONG, query=SYMBOL]\n");

            execute("DROP MATERIALIZED VIEW mv");
            execute("CREATE MATERIALIZED VIEW mv AS SELECT ts, first(x) f FROM base SAMPLE BY 1h");
            drainWalAndMatViewQueues();
            assertViewStatus("valid\t\n");
            assertQuery("SELECT * FROM mv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tf
                            2024-01-01T00:00:00.000000Z\t5
                            2024-01-01T01:00:00.000000Z\t7
                            """);
        });
    }

    @Test
    public void testRefreshInvalidatesViewWrittenByOlderBuild() throws Exception {
        assertMemoryLeak(() -> {
            // The fixture holds base table b (ts TIMESTAMP, l LONG256) and two views over it, written by a build
            // whose first() over LONG256 returned LONG: mv_first (SELECT ts, first(l) f FROM b SAMPLE BY 1h)
            // stores f as LONG, and mv_count (SELECT ts, count() c FROM b SAMPLE BY 1h) has no such column.
            loadFixture();
            assertQuery("SELECT * FROM mv_first")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tf
                            2024-01-01T00:00:00.000000Z\t17
                            2024-01-01T01:00:00.000000Z\t49
                            """);

            execute("INSERT INTO b VALUES ('2024-01-01T02:00:00.000000Z', 0x51)");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM materialized_views() ORDER BY view_name")
                    .noLeakCheck()
                    .returns("""
                            view_name\tview_status\tinvalidation_reason
                            mv_count\tvalid\t
                            mv_first\tinvalid\t[-1]: view column type does not match its query, recreate the view [column=f, view=LONG, query=LONG256]
                            """);
            assertQuery("SELECT * FROM mv_first")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tf
                            2024-01-01T00:00:00.000000Z\t17
                            2024-01-01T01:00:00.000000Z\t49
                            """);
            assertQuery("SELECT * FROM mv_count")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tc
                            2024-01-01T00:00:00.000000Z\t2
                            2024-01-01T01:00:00.000000Z\t2
                            2024-01-01T02:00:00.000000Z\t1
                            """);
        });
    }

    // Unpacks the archived table directories into the db root and registers the views they hold, as a restart on
    // the upgraded build does. The fixture's entries are rooted at the table directories.
    private static void loadFixture() throws Exception {
        final byte[] buffer = new byte[64 * 1024];
        final File root = new File(configuration.getDbRoot().toString());
        try (InputStream is = MatViewColumnTypeChangeTest.class.getResourceAsStream("/mat-view-long256/first-long-10.1.1-SNAPSHOT.zip")) {
            Assert.assertNotNull(is);
            try (ZipInputStream zip = new ZipInputStream(is)) {
                ZipEntry ze;
                while ((ze = zip.getNextEntry()) != null) {
                    final File dest = new File(root, ze.getName());
                    if (ze.isDirectory()) {
                        Assert.assertTrue(dest.isDirectory() || dest.mkdirs());
                    } else {
                        final File dir = dest.getParentFile();
                        Assert.assertTrue(dir.isDirectory() || dir.mkdirs());
                        try (FileOutputStream fos = new FileOutputStream(dest)) {
                            int n;
                            while ((n = zip.read(buffer, 0, buffer.length)) > 0) {
                                fos.write(buffer, 0, n);
                            }
                        }
                    }
                    zip.closeEntry();
                }
            }
        }
        engine.reloadTableNames();
        engine.reconcileTableNameRegistryState();
        engine.buildViewGraphs();
    }

    private void assertViewStatus(String expected) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'mv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\n" + expected);
    }
}
