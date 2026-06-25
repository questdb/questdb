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
 * Executable evidence that the {@code syncfs}-based batched column flush's WITHIN-PAGE durability is NO LONGER
 * dependent on the SHARED-JOURNAL semantics of default ext4 (jbd2) / xfs: it now holds even under ext4
 * {@code fast_commit}'s PER-INODE journaling. This is the inverse-mutation proof of the salvage fix.
 *
 * <p><b>History.</b> The original batched path relied on a FOREIGN flush — the single {@code _cv}
 * {@code fdatasync} — to journal the columns' within-page extent conversions. That only works under a shared
 * journal; under per-inode journaling {@code fdatasync(_cv)} journals only {@code _cv}'s own inode, so the
 * columns' at-device within-page bytes were NEVER journaled and reverted on crash. A real ext4 power-cut
 * harness PROVED that loss (committed columns read back as zeros). The fix replaces that foreign-flush
 * reliance with ONE explicit {@code syncfs(columnFd)} after the columns are drained: {@code syncfs(2)}
 * journals EVERY inode's pending extent conversions for the whole filesystem in one device flush, regardless
 * of journal policy.
 *
 * <p>This test runs the SAME wide-table within-page SYNC workload as before but under
 * {@code modelSharedJournal=false} (per-inode journaling), and now asserts the within-page tail SURVIVES —
 * because the batched path's {@code syncfs} journaled the columns even though no shared journal and no
 * per-column {@code fdatasync} was involved. Contrast: {@link BatchedFlushDurabilityCrashTest} runs the wide
 * workload under the default {@code modelSharedJournal=true} and also keeps every row. The two together show
 * the salvaged path is durable under BOTH journal policies. If this test ever STARTED losing the within-page
 * tail again, the batched path's {@code syncfs} was dropped or stopped journaling the columns — the exact
 * corruption the fix closes; investigate immediately.
 *
 * <p>A LARGE data-append page (16 MiB) is used precisely so the columns extend ONCE on the first commits and
 * then absorb many subsequent commits WITHIN the already-allocated page — manufacturing the within-page,
 * no-extend regime where the OLD path lost data and the {@code syncfs} path now does not. Linux-only (the
 * batched path is guarded on {@link Os#isLinux()}; elsewhere SYNC falls back to per-file {@code sync(false)}).
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
    public void testWithinPageBatchedSyncSurvivesUnderPerInodeJournalViaSyncfs() throws Exception {
        Assume.assumeTrue("batched SYNC flush is Linux-only", Os.isLinux());
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        // FORCE the batched path on. This test PROVES the syncfs-based batched path is durable even under
        // per-inode journaling, so it must actually run the batched path: if CI ran on an ext4 fast_commit
        // mount the production config would auto-disable batching (falling back to per-file fsync) and this
        // test would no longer characterise the optimization. The test harness builds its config with
        // detection OFF, so this property is the raw, deterministic enable.
        setProperty(PropertyKey.CAIRO_COMMIT_SYNC_COLUMN_BATCHED, "true");
        // LARGE append page: force the within-page (no-extend) regime after the initial allocation.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, String.valueOf(BIG_PAGE));
        try {
            Assert.assertEquals("test requires SYNC commit mode",
                    CommitMode.SYNC, engine.getConfiguration().getCommitMode());
            Assert.assertTrue("test must exercise the BATCHED SYNC path",
                    engine.getConfiguration().isBatchedColumnSyncEnabled());
            runWithCrashFacade(() -> {
                // Per-inode journaling world (ext4 fast_commit): an fsync of _cv does NOT journal the columns.
                // The OLD path lost the within-page tail here; the syncfs in the batched path now journals the
                // columns regardless, so the tail must SURVIVE.
                crashFf.modelSharedJournal = false;

                StringBuilder ddl = new StringBuilder("create table w (ts timestamp");
                for (int c = 0; c < 20; c++) {
                    ddl.append(", f").append(c).append(c % 2 == 0 ? " long" : " double");
                }
                ddl.append(", s1 string, s2 string, v varchar");
                ddl.append(", sym1 symbol index, sym2 symbol index");
                ddl.append(") timestamp(ts) partition by none");
                execute(ddl.toString());

                // Seed row: its first commit extends every column from 0 to the 16 MiB page. Each commit's
                // batched flush ends in one syncfs, which journals the columns' extents -> the seed is durable
                // regardless of the shared-journal flag. (We deliberately do NOT call markDurableBaseline: that
                // would seed journaledDataEnd to the fallocate-inflated file LENGTH, masking the within-page
                // behaviour we are characterising. Letting the real commit path's syncfs do the journaling is
                // both faithful and the point of the test.)
                insertRow(0);

                int row = 1;
                // Phase 1: allocate/extend the column pages. These commits cross from 0 into the 16 MiB page
                // (and the first few may touch a 2nd page for the widest aux vectors). They are journaled by
                // each commit's syncfs -> the durable floor.
                for (int i = 0; i < EXTEND_PHASE; i++, row++) {
                    insertRow(row);
                }
                // Everything committed so far is journaled by its commit's syncfs -> the durable floor.
                final long durableFloor = rowCount("w");

                // Phase 2: within-page commits. With a 16 MiB page these rows append into the already-mapped
                // page, so NO column extends -> NO per-column fdatasync. The column data reaches the device via
                // msync-async + sync_file_range, and the batched path's ONE syncfs(columnFd) then journals
                // every column's within-page extent conversion in a single device flush — independent of the
                // journal policy. So even under modelSharedJournal=false this tail is durable.
                for (int i = 0; i < WITHIN_PAGE_TAIL; i++, row++) {
                    insertRow(row);
                }
                final long committed = rowCount("w");
                Assert.assertEquals("within-page tail must all be committed pre-crash",
                        durableFloor + WITHIN_PAGE_TAIL, committed);

                crashAndReopen();

                // THE FINDING (post-fix): under per-inode journaling the within-page tail SURVIVES, because the
                // batched path's syncfs journaled the columns' extent conversions even though no shared journal
                // and no per-column fdatasync was involved. This is the inverse of the pre-fix behaviour (the
                // OLD foreign-_cv-flush path lost this exact tail) and the executable proof the salvage works.
                // rowCountTolerant only maps a LOUD reopen corruption to 0; here we require the full, clean set.
                long after = rowCountTolerant("w");
                Assert.assertEquals(
                        "within-page batched-SYNC tail must SURVIVE under per-inode journaling via syncfs " +
                                "(committed=" + committed + ", durableFloor=" + durableFloor + ", survived=" + after + ")",
                        committed, after);
                assertAllRowsCorrect(committed);
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_COMMIT_SYNC_COLUMN_BATCHED, "true");
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

    /** After the crash, assert every surviving row carries exactly the values written for its timestamp. */
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
