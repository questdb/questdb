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
 * Executable evidence that the Linux SYNC-mode batched column flush's WITHIN-PAGE durability DEPENDS on the
 * SHARED-JOURNAL semantics of default ext4 (jbd2) and xfs — and is NOT guaranteed under ext4
 * {@code fast_commit}'s per-inode journaling.
 *
 * <p>It runs the SAME wide-table SYNC workload that {@link BatchedFlushDurabilityCrashTest} proves durable
 * under the default {@code modelSharedJournal=true}, but flips the crash model to
 * {@code modelSharedJournal=false} and crashes after a strictly WITHIN-PAGE commit (no column file extended
 * since the last fdatasync). In the batched path, a within-page commit flushes each column with only
 * {@code msync(MS_ASYNC)} + {@code sync_file_range(WAIT_AFTER)} (data-at-device, NOT journaled) and takes NO
 * per-column fdatasync (nothing extended); the column data is made durable ONLY by the single {@code _cv}
 * {@code fdatasync}. Under a shared journal that {@code _cv} commit also journals the columns' extent
 * conversions; under per-inode journaling it journals only {@code _cv}'s own inode, so the columns' at-device
 * within-page bytes are NEVER journaled and revert on crash.
 *
 * <p>This test therefore asserts the within-page tail is LOST (fewer-than-committed rows survive, or a loud
 * Cairo error). That LOSS is the proof: it makes the optimization's dependency on shared-journal semantics
 * EXPLICIT and testable. If it ever STOPPED losing data, the batched path would be fsyncing the columns more
 * than expected (or a column extended unexpectedly) — investigate before relaxing this test.
 *
 * <p>A LARGE data-append page (16 MiB) is used precisely so the columns extend ONCE on the first commits and
 * then absorb many subsequent commits WITHIN the already-allocated page — manufacturing the within-page,
 * no-extend regime the optimization optimises and that this test characterises. Linux-only (the batched path
 * is guarded on {@link Os#isLinux()}; elsewhere SYNC falls back to per-file {@code sync(false)}).
 */
public class BatchedFlushSharedJournalDependencyTest extends AbstractCrashConsistencyTest {

    // A large append page so that, after the first few extending commits allocate the page, subsequent
    // commits append strictly WITHIN it (no extend -> no per-column fdatasync -> _cv flush is the only thing
    // that could make the column data durable).
    private static final long BIG_PAGE = 16 * Numbers.SIZE_1MB;
    // Enough commits to comfortably exceed the initial page-allocating extends and accumulate a within-page
    // tail. 16 MiB / (a few hundred bytes per wide row) is many thousands of rows, so this stays within-page.
    private static final int EXTEND_PHASE = 8;   // first commits that allocate/extend the column pages
    private static final int WITHIN_PAGE_TAIL = 40; // further commits that stay within the allocated page

    @Test
    public void testWithinPageBatchedSyncLosesDataUnderPerInodeJournal() throws Exception {
        Assume.assumeTrue("batched SYNC flush is Linux-only", Os.isLinux());
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        // LARGE append page: force the within-page (no-extend) regime after the initial allocation.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, String.valueOf(BIG_PAGE));
        try {
            Assert.assertEquals("test requires SYNC commit mode",
                    CommitMode.SYNC, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                // Per-inode journaling world (ext4 fast_commit): fsync of _cv does NOT journal the columns.
                crashFf.modelSharedJournal = false;

                StringBuilder ddl = new StringBuilder("create table w (ts timestamp");
                for (int c = 0; c < 20; c++) {
                    ddl.append(", f").append(c).append(c % 2 == 0 ? " long" : " double");
                }
                ddl.append(", s1 string, s2 string, v varchar");
                ddl.append(", sym1 symbol index, sym2 symbol index");
                ddl.append(") timestamp(ts) partition by none");
                execute(ddl.toString());

                // Seed row: its first commit extends every column from 0 to the 16 MiB page, so the column
                // gets its OWN extend fdatasync -> the seed is journaled regardless of the shared-journal flag.
                // (We deliberately do NOT call markDurableBaseline: that seeds journaledDataEnd to the
                // fallocate-inflated file LENGTH, which would mask the within-page loss we are characterising.
                // Letting the extend fdatasyncs do the journaling is both faithful and the point of the test.)
                insertRow(0);

                int row = 1;
                // Phase 1: allocate/extend the column pages. These commits cross from 0 into the 16 MiB page
                // (and the first few may touch a 2nd page for the widest aux vectors), so they DO take
                // per-column extend fdatasyncs -> their rows are independently journaled (the durable floor).
                for (int i = 0; i < EXTEND_PHASE; i++, row++) {
                    insertRow(row);
                }
                // Everything committed so far is journaled by its own extend fdatasync -> the durable floor.
                final long durableFloor = rowCount("w");

                // Phase 2: within-page commits. With a 16 MiB page these rows append into the already-mapped
                // page, so NO column extends -> NO per-column fdatasync -> the column data is at the device
                // (msync-async + sync_file_range) but its extent conversion is journaled ONLY if _cv's
                // fdatasync shares the journal. Under modelSharedJournal=false it does NOT.
                for (int i = 0; i < WITHIN_PAGE_TAIL; i++, row++) {
                    insertRow(row);
                }
                final long committed = rowCount("w");
                Assert.assertEquals("within-page tail must all be committed pre-crash",
                        durableFloor + WITHIN_PAGE_TAIL, committed);

                crashAndReopen();

                // THE FINDING: under per-inode journaling the within-page tail's column/index data was never
                // journaled, so it is LOST. We accept either a truncated row set (count < committed) or a loud
                // failure on reopen (a truncated column / index file -> Cairo error / SqlException / SIGBUS,
                // mapped to 0 by rowCountTolerant). What we forbid is the optimization SILENTLY keeping all
                // committed rows, which would mean the within-page data was durable WITHOUT a shared journal,
                // contradicting the model. (Contrast: BatchedFlushDurabilityCrashTest runs the SAME workload
                // under the default modelSharedJournal=true and keeps EVERY row -> the dependency is real.)
                long after = rowCountTolerant("w");
                Assert.assertTrue(
                        "within-page batched-SYNC data must be LOST under per-inode journaling " +
                                "(committed=" + committed + ", durableFloor=" + durableFloor + ", survived=" + after + ")",
                        after < committed);
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
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
     * Like {@link #rowCount} but tolerant of a loud crash-corruption signal. A column / index file truncated
     * to its last-journaled extent makes the post-crash reopen fail LOUDLY — e.g. a SIGBUS (InternalError),
     * a CairoException/CairoError, or a SqlException at reader-open time such as "bitmap index value file too
     * short [... actual=0 ...]". The workload compiled and ran cleanly BEFORE the crash, so any such failure
     * AFTER it is itself proof the within-page data was not durable; we map it to "0 rows survived".
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
