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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Deferred 1 — per-table {@code commit_mode} override. A table can pick its commit mode
 * independently of the global {@code cairo.commit.mode} via {@code CREATE TABLE ... WITH
 * commit_mode='...'} / {@code ALTER TABLE ... SET PARAM commit_mode='...'}. Stored in {@code _meta};
 * a {@link CommitMode#UNSET} sentinel means "defer to the global mode" (back-compat for tables that
 * predate the field).
 */
public class PerTableCommitModeTest extends AbstractCairoTest {

    // ---------- Task 1: _meta field + UNSET back-compat + accessor ----------

    /**
     * A table created WITHOUT an explicit commit_mode stores {@link CommitMode#UNSET} in its _meta,
     * and that survives a writer close/reopen (round-trips through the on-disk _meta).
     */
    @Test
    public void testDefaultCommitModeIsUnsetAndRoundTrips() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute("create table t1 (ts timestamp, v long) timestamp(ts) partition by day wal");
            TableToken tt = engine.verifyTableName("t1");
            try (TableWriter w = getWriter(tt)) {
                Assert.assertEquals(CommitMode.UNSET, w.getMetadata().getCommitMode());
            }
            // Force the writer + metadata cache to drop, then reopen: the stored UNSET must reload.
            engine.releaseInactive();
            try (TableWriter w = getWriter(tt)) {
                Assert.assertEquals(CommitMode.UNSET, w.getMetadata().getCommitMode());
            }
        });
    }

    /**
     * A table created WITH commit_mode='adaptive' stores ADAPTIVE in _meta and round-trips it through
     * a writer reopen (so the value is persisted, not just held in memory).
     */
    @Test
    public void testStoredCommitModeRoundTrips() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute("create table t2 (ts timestamp, v long) timestamp(ts) partition by day wal " +
                    "with commit_mode='adaptive'");
            TableToken tt = engine.verifyTableName("t2");
            try (TableWriter w = getWriter(tt)) {
                Assert.assertEquals(CommitMode.ADAPTIVE, w.getMetadata().getCommitMode());
            }
            engine.releaseInactive();
            try (TableWriter w = getWriter(tt)) {
                Assert.assertEquals(CommitMode.ADAPTIVE, w.getMetadata().getCommitMode());
            }
        });
    }

    // ---------- Task 2: CREATE TABLE ... WITH commit_mode ----------

    /**
     * CREATE WITH commit_mode='adaptive' under a global nosync instance: wal_tables() reports the
     * per-table effective mode ('adaptive'), not the global ('nosync').
     */
    @Test
    public void testCreateWithCommitModeShowsInWalTables() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute("create table a (ts timestamp, v long) timestamp(ts) partition by day wal " +
                    "with commit_mode='adaptive'");
            execute("create table b (ts timestamp, v long) timestamp(ts) partition by day wal");
            assertQuery("select name, commitMode from wal_tables() order by name")
                    .noLeakCheck()
                    .returns("name\tcommitMode\n" +
                            "a\tadaptive\n" +
                            "b\tnosync\n");
        });
    }

    @Test
    public void testCreateWithUnknownCommitModeFails() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("create table bad (ts timestamp, v long) timestamp(ts) partition by day wal " +
                        "with commit_mode='turbo'");
                Assert.fail("expected SqlException for unknown commit_mode");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "commit_mode");
            }
        });
    }

    @Test
    public void testCreateWithEachCommitModeRoundTrips() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            assertCreatedMode("cm_sync", "sync", CommitMode.SYNC);
            assertCreatedMode("cm_async", "async", CommitMode.ASYNC);
            assertCreatedMode("cm_nosync", "nosync", CommitMode.NOSYNC);
            assertCreatedMode("cm_adaptive", "adaptive", CommitMode.ADAPTIVE);
        });
    }

    // ---------- Task 3: ALTER TABLE ... SET PARAM commit_mode ----------

    @Test
    public void testAlterSetParamCommitMode() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, v long) timestamp(ts) partition by day wal");
            TableToken tt = engine.verifyTableName("c");
            try (TableWriter w = getWriter(tt)) {
                Assert.assertEquals(CommitMode.UNSET, w.getMetadata().getCommitMode());
            }

            execute("alter table c set param commit_mode='sync'");
            drainWalQueue();
            try (TableWriter w = getWriter(tt)) {
                Assert.assertEquals(CommitMode.SYNC, w.getMetadata().getCommitMode());
            }
            // wal_tables() reflects the altered effective mode.
            assertQuery("select name, commitMode from wal_tables() where name = 'c'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("name\tcommitMode\n" +
                            "c\tsync\n");

            // Revert to global with 'unset'.
            execute("alter table c set param commit_mode='unset'");
            drainWalQueue();
            try (TableWriter w = getWriter(tt)) {
                Assert.assertEquals(CommitMode.UNSET, w.getMetadata().getCommitMode());
            }
            assertQuery("select name, commitMode from wal_tables() where name = 'c'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("name\tcommitMode\n" +
                            "c\tnosync\n");
        });
    }

    @Test
    public void testAlterSetParamUnknownCommitModeFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table d (ts timestamp, v long) timestamp(ts) partition by day wal");
            try {
                execute("alter table d set param commit_mode='turbo'");
                Assert.fail("expected SqlException for unknown commit_mode");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "commit_mode");
            }
        });
    }

    private static void assertCreatedMode(String table, String modeName, int expected) throws SqlException {
        execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal " +
                "with commit_mode='" + modeName + "'");
        TableToken tt = engine.verifyTableName(table);
        try (TableWriter w = getWriter(tt)) {
            Assert.assertEquals("mode for " + modeName, expected, w.getMetadata().getCommitMode());
        }
    }
}
