package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * End-to-end durability proof for the Linux SYNC-mode batched column flush
 * ({@link io.questdb.cairo.TableWriter#syncColumns()} -> syncColumnsBatchedSync()): per-file
 * {@code msync(MS_SYNC)} device flushes are replaced by {@code msync(MS_ASYNC)} +
 * {@code sync_file_range(WAIT_AFTER)} per file plus a SINGLE batched device flush from the {@code _cv}
 * commit. This test exercises the scheme on a REALISTIC WIDTH (30+ columns spanning fixed-width,
 * string, varchar, and indexed symbol) under repeated commits that force file extends, then simulates
 * power loss mid-stream and asserts EVERY committed row (and indexed-symbol lookups) survives.
 *
 * <p>It runs against the same crash model ({@link CrashFaultFilesFacade}) that the Stage-1 self-tests
 * pin down: that model promotes device-cache content to durable on ONE device flush (the batching
 * semantic) and makes {@code sync_file_range} a NO-OP unless the file was first msync'd. So if the
 * batched path ever dropped the {@code sync_file_range} drain, or advanced a {@code lastSynced*}
 * watermark in kick/drain (wrongly skipping an extend fdatasync), committed rows would read back
 * MISSING here and the test would go RED. (Verified by hand: skipping the drain reddens this test.)
 *
 * <p>A minimal mmap append-page size forces the wide columns to cross page boundaries during the run,
 * so the extend-fdatasync branch of {@code syncFlushFinishIfExtended()} is taken repeatedly.
 *
 * <p>Linux-only: the batched path is guarded on {@link Os#isLinux()} (non-Linux falls back to the
 * per-file {@code sync(false)} which this test is not trying to characterise), so it is skipped
 * elsewhere.
 */
public class BatchedFlushDurabilityCrashTest extends AbstractCrashConsistencyTest {

    private static final int COMMITS = 250;
    private static final long MIN_PAGE = Files.PAGE_SIZE; // 4096 on Linux x86; forces extends

    @Test
    public void testWideTableBatchedSyncSurvivesCrash() throws Exception {
        Assume.assumeTrue("batched SYNC flush is Linux-only", Os.isLinux());
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        // FORCE the batched path on regardless of this CI machine's filesystem. The TableWriter gate is
        // `... && configuration.isBatchedColumnSyncEnabled()`; if CI happened to run on an ext4 fast_commit
        // mount the production config would disable batching and this test would silently stop exercising
        // the batched path. The test harness builds its config with detection OFF, so this property is the
        // raw, deterministic enable.
        setProperty(PropertyKey.CAIRO_COMMIT_SYNC_COLUMN_BATCHED, "true");
        // Tiny append page so the (many, wide) column files extend repeatedly during the run, exercising
        // the per-file extend fdatasync in syncFlushFinishIfExtended() alongside the batched device flush.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, String.valueOf(MIN_PAGE));
        try {
            Assert.assertEquals("test requires SYNC commit mode",
                    CommitMode.SYNC, engine.getConfiguration().getCommitMode());
            Assert.assertTrue("test must exercise the BATCHED SYNC path",
                    engine.getConfiguration().isBatchedColumnSyncEnabled());
            runWithCrashFacade(() -> {
                // A WIDE table: a fixed-width spread, two strings, a varchar, and TWO indexed symbols.
                // That mix drives every batched mem family through the 3-phase flush: column data (.d)
                // + aux (.i) for var-size types, symbol char/offset (.c/.o) + symbol .k/.v index.
                StringBuilder ddl = new StringBuilder("create table w (ts timestamp");
                for (int c = 0; c < 20; c++) {
                    ddl.append(", f").append(c).append(c % 2 == 0 ? " long" : " double");
                }
                ddl.append(", s1 string, s2 string, v varchar");
                ddl.append(", sym1 symbol index, sym2 symbol index");
                ddl.append(") timestamp(ts) partition by none");
                execute(ddl.toString());

                // Seed row is "old, already journaled"; everything after the baseline must survive on its own.
                insertRow(0);
                markDurableBaseline();

                for (int i = 1; i <= COMMITS; i++) {
                    insertRow(i); // each insert is its own SYNC commit -> batched flush + _cv + _txn
                }

                crashAndReopen();

                // 1) Row COUNT: every committed row (seed + COMMITS) is durable. A dropped drain or a
                //    wrongly-skipped extend fdatasync would truncate the tail -> fewer rows -> RED.
                Assert.assertEquals("committed row count after crash", COMMITS + 1, rowCount("w"));

                // 2) CONTENT: each surviving row matches what we wrote (no silent corruption), across a
                //    fixed-width column, both strings, the varchar, and the symbols.
                assertAllRowsCorrect();

                // 3) INDEXED lookups: the symbol .k/.v index (also batched) resolves every committed key.
                //    sym1 has 7 distinct values; each appears ceil/floor of (COMMITS+1)/7 times.
                long viaIndex = 0;
                for (int k = 0; k < 7; k++) {
                    viaIndex += rowCount("w where sym1 = 's1v" + k + "'");
                }
                Assert.assertEquals("indexed sym1 lookups must cover every row", COMMITS + 1, viaIndex);

                // A specific indexed point lookup returns the right fixed-width payload for its rows.
                assertIndexedRowsCorrect();
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_COMMIT_SYNC_COLUMN_BATCHED, "true");
            setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, String.valueOf(2097152L));
        }
    }

    /** sym1 value for row i (7 distinct -> exercises a multi-key index). */
    private static String sym1Of(int i) {
        return "s1v" + (i % 7);
    }

    /** sym2 value for row i (3 distinct). */
    private static String sym2Of(int i) {
        return "s2v" + (i % 3);
    }

    /** Varchar value for row i; >9 bytes so it takes the split (out-of-line) varchar layout. */
    private static String vOf(int i) {
        return "varchar-payload-" + String.format("%08d", i);
    }

    /** First string for row i. */
    private static String s1Of(int i) {
        return "s1-" + String.format("%08d", i);
    }

    /** Second string for row i (different length so aux offsets vary). */
    private static String s2Of(int i) {
        return "second-string-value-" + String.format("%010d", i);
    }

    private void assertAllRowsCorrect() {
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
                Assert.assertEquals("rows iterated", COMMITS + 1, i);
            }
        } catch (SqlException e) {
            throw new RuntimeException("assertAllRowsCorrect failed: " + sql, e);
        }
    }

    /** Point-lookup on the index: every row with sym1='s1v3' must carry its correct f0 (=ts/1e6). */
    private void assertIndexedRowsCorrect() {
        final String sql = "select ts, f0, sym1 from w where sym1 = 's1v3' order by ts";
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Record r = c.getRecord();
                long seen = 0;
                while (c.hasNext()) {
                    long ts = r.getTimestamp(0);
                    long row = ts / 1_000_000L;
                    Assert.assertEquals("indexed key mismatch", "s1v3", str(r.getSymA(2)));
                    Assert.assertEquals("indexed f0 mismatch for row " + row, row, r.getLong(1));
                    Assert.assertEquals("row " + row + " must map to sym1 bucket 3", 3L, row % 7);
                    seen++;
                }
                Assert.assertTrue("expected at least one indexed row", seen > 0);
            }
        } catch (SqlException e) {
            throw new RuntimeException("assertIndexedRowsCorrect failed: " + sql, e);
        }
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

    private static String str(CharSequence cs) {
        return cs == null ? null : cs.toString();
    }

    private static String str(io.questdb.std.str.Utf8Sequence u) {
        return u == null ? null : u.toString();
    }
}
