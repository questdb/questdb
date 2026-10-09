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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Checks that a SYNC commit keeps a within-page column tail durable under per-inode journaling (ext4
 * {@code fast_commit}), not only under the shared journal of default ext4 and xfs. Under per-inode journaling an
 * {@code fdatasync} of {@code _cv} journals only that inode, so the columns' within-page extent conversions are
 * lost on a crash unless the column flush itself journals them.
 * <p>
 * The test enables the batched column flush ({@code CAIRO_ADAPTIVE_EPOCH_COLUMN_SYNC_BATCHED}), which ends with
 * one {@code syncfs(columnFd)}, but a SYNC commit flushes its columns per file in {@code TableWriter.syncColumns()};
 * the batched passes run only for the adaptive durable epoch.
 * <p>
 * A 16 MiB data-append page makes the columns extend once on the first commits and then take many commits within
 * the allocated page, with no extend and so no per-column {@code fdatasync}. {@link BatchedFlushDurabilityCrashTest}
 * runs a wide workload under the shared journal. Linux only: the batched path is guarded on {@link Os#isLinux()}.
 */
public class BatchedFlushSharedJournalDependencyTest extends AbstractCrashConsistencyTest {

    // A large append page: after the first extending commits allocate it, later commits append within it,
    // with no extend and so no per-column fdatasync.
    private static final long BIG_PAGE = 16 * Numbers.SIZE_1MB;
    // Commits past the page-allocating extends that build a within-page tail. A wide row takes a few hundred
    // bytes, so the 16 MiB page holds many thousands of rows and the tail stays within it.
    private static final int EXTEND_PHASE = 8;   // first commits that allocate/extend the column pages
    private static final int WITHIN_PAGE_TAIL = 40; // further commits that stay within the allocated page

    @Test
    public void testWithinPageBatchedSyncSurvivesUnderPerInodeJournalViaSyncfs() throws Exception {
        Assume.assumeTrue("batched SYNC flush is Linux-only", Os.isLinux());
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        // Force the batched path on: on an ext4 fast_commit mount the production config disables batching
        // and falls back to per-file fsync. The test harness builds its config with detection off, so this
        // property is the deterministic enable.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_COLUMN_SYNC_BATCHED, "true");
        // A large append page forces the within-page, no-extend regime after the initial allocation.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, String.valueOf(BIG_PAGE));
        try {
            Assert.assertEquals("test requires SYNC commit mode",
                    CommitMode.SYNC, engine.getConfiguration().getCommitMode());
            Assert.assertTrue("test must exercise the BATCHED SYNC path",
                    engine.getConfiguration().isAdaptiveEpochColumnSyncBatched());
            runWithCrashFacade(() -> {
                // Per-inode journaling, as on ext4 fast_commit: an fsync of _cv does not journal the columns.
                crashFf.modelSharedJournal = false;

                StringBuilder ddl = new StringBuilder("create table w (ts timestamp");
                for (int c = 0; c < 20; c++) {
                    ddl.append(", f").append(c).append(c % 2 == 0 ? " long" : " double");
                }
                ddl.append(", s1 string, s2 string, v varchar");
                ddl.append(", sym1 symbol index, sym2 symbol index");
                ddl.append(") timestamp(ts) partition by none");
                execute(ddl.toString());

                // Seed row: its first commit extends every column from 0 to the 16 MiB page, and the commit's
                // column flush journals the extents. The test does not call markDurableBaseline: that would seed
                // journaledDataEnd to the fallocate-inflated file length and hide the within-page behaviour, so
                // the commit path's own flush does the journaling.
                insertRow(0);

                int row = 1;
                // Phase 1: allocate and extend the column pages. These commits cross from 0 into the 16 MiB
                // page (the widest aux vectors may touch a second page), and each commit's column flush
                // journals them.
                for (int i = 0; i < EXTEND_PHASE; i++, row++) {
                    insertRow(row);
                }
                // Everything committed so far is journaled by its commit's column flush -> the durable floor.
                final long durableFloor = rowCount("w");

                // Phase 2: within-page commits. The rows append into the already-mapped page, so no column
                // extends and no extend-time fdatasync runs.
                for (int i = 0; i < WITHIN_PAGE_TAIL; i++, row++) {
                    insertRow(row);
                }
                final long committed = rowCount("w");
                Assert.assertEquals("within-page tail must all be committed pre-crash",
                        durableFloor + WITHIN_PAGE_TAIL, committed);

                crashAndReopen();

                // Under per-inode journaling the within-page tail must survive the crash. rowCountTolerant maps a
                // loud reopen corruption to 0; the assertion requires the full set.
                long after = rowCountTolerant("w");
                Assert.assertEquals(
                        "within-page batched-SYNC tail must SURVIVE under per-inode journaling via syncfs " +
                                "(committed=" + committed + ", durableFloor=" + durableFloor + ", survived=" + after + ")",
                        committed, after);
                assertAllRowsCorrect(committed);
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_COLUMN_SYNC_BATCHED, "true");
            setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, String.valueOf(2097152L));
        }
    }

    private static String sym1Of(int i) {
        return "s1v" + (i % 7);
    }

    private static String sym2Of(int i) {
        return "s2v" + (i % 3);
    }

    private static String vOf(int i) {
        return "varchar-payload-" + String.format("%08d", i);
    }

    private static String s1Of(int i) {
        return "s1-" + String.format("%08d", i);
    }

    private static String s2Of(int i) {
        return "second-string-value-" + String.format("%010d", i);
    }

    private void insertRow(int i) {
        StringBuilder sql = new StringBuilder("insert into w values (").append((long) i * 1_000_000L);
        for (int c = 0; c < 20; c++) {
            sql.append(", ").append(c % 2 == 0 ? Long.toString(i) : (i + ".5"));
        }
        sql.append(", '").append(s1Of(i)).append('\'');
        sql.append(", '").append(s2Of(i)).append('\'');
        sql.append(", '").append(vOf(i)).append('\'');
        sql.append(", '").append(sym1Of(i)).append('\'');
        sql.append(", '").append(sym2Of(i)).append('\'');
        sql.append(')');
        try {
            execute(sql.toString());
        } catch (SqlException e) {
            throw new RuntimeException("insertRow(" + i + ") failed", e);
        }
    }

    private long rowCount(String fromAndWhere) {
        try (RecordCursorFactory f = select("select count() from " + fromAndWhere)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Record r = c.getRecord();
                return c.hasNext() ? r.getLong(0) : -1;
            }
        } catch (SqlException e) {
            throw new RuntimeException("rowCount failed for: " + fromAndWhere, e);
        }
    }

    /**
     * After the crash, asserts that every surviving row carries the values written for its timestamp.
     */
    private void assertAllRowsCorrect(long expectedRows) {
        final String sql = "select ts, f0, s1, s2, v, sym1, sym2 from w order by ts";
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Record r = c.getRecord();
                int i = 0;
                while (c.hasNext()) {
                    Assert.assertEquals("ts row " + i, (long) i * 1_000_000L, r.getTimestamp(0));
                    Assert.assertEquals("f0 row " + i, (long) i, r.getLong(1));
                    Assert.assertEquals("s1 row " + i, s1Of(i), str(r.getStrA(2)));
                    Assert.assertEquals("s2 row " + i, s2Of(i), str(r.getStrA(3)));
                    Assert.assertEquals("v row " + i, vOf(i), str(r.getVarcharA(4)));
                    Assert.assertEquals("sym1 row " + i, sym1Of(i), str(r.getSymA(5)));
                    Assert.assertEquals("sym2 row " + i, sym2Of(i), str(r.getSymA(6)));
                    i++;
                }
                Assert.assertEquals("rows iterated", expectedRows, i);
            }
        } catch (SqlException e) {
            throw new RuntimeException("assertAllRowsCorrect failed: " + sql, e);
        }
    }

    private static String str(CharSequence cs) {
        return cs == null ? null : cs.toString();
    }

    private static String str(io.questdb.std.str.Utf8Sequence u) {
        return u == null ? null : u.toString();
    }

    /**
     * Like {@link #rowCount} but tolerant of a loud crash-corruption signal. A column or index file truncated
     * to its last journaled extent makes the reopen after the crash fail loudly: a SIGBUS (InternalError), a
     * CairoException or CairoError, or a SqlException at reader open such as "bitmap index value file too
     * short [... actual=0 ...]". The workload ran cleanly before the crash, so such a failure proves the
     * within-page data was not durable, and the method returns 0 rows.
     */
    private long rowCountTolerant(String table) {
        try {
            return rowCount(table);
        } catch (io.questdb.cairo.CairoException | io.questdb.cairo.CairoError | InternalError e) {
            // Loud corruption/SIGBUS from a truncated column -> within-page data was not durable.
            return 0;
        } catch (RuntimeException e) {
            // rowCount wraps SqlException/Cairo failures in RuntimeException; a reopen-time corruption signal
            // (truncated column or index file) equally proves loss.
            Throwable c = e.getCause();
            if (c instanceof io.questdb.cairo.CairoException
                    || c instanceof io.questdb.cairo.CairoError
                    || c instanceof io.questdb.griffin.SqlException) {
                return 0;
            }
            throw e;
        }
    }
}
