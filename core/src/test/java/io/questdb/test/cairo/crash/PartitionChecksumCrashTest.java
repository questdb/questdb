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

package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ChecksumTrailer;
import io.questdb.cairo.PartitionChecksumSidecar;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Crash consistency for the per-partition data checksum sidecar.
 * <p>
 * The sidecar publishes a claim ABOUT bytes that live in other files, so a crash can land between
 * the bytes becoming durable and the claim becoming durable, and it can land inside the claim
 * itself. The bar is deliberately one-sided:
 * <pre>
 *   After a crash at ANY durability op, the partition must EITHER verify cleanly, OR read as
 *   uncovered -- but it must NEVER report data corruption, because the data is intact and only the
 *   sidecar is behind.
 * </pre>
 * A false corruption verdict is the worst outcome this design can produce: the bytes are fine, the
 * table is fine, and the database condemns it. That is strictly worse than having no checksum at
 * all, which is why every crash point is swept rather than one chosen one, and why each iteration
 * asserts the injected crash actually fired -- a sweep whose injection never fires proves nothing.
 */
public class PartitionChecksumCrashTest extends AbstractCrashConsistencyTest {

    /**
     * Iterations in which a generation actually survived and files were really verified.
     */
    private int verifiedIterations;
    /**
     * Covered files actually hashed and compared across the sweep.
     */
    private int verifiedFiles;

    @Test
    public void testCrashSweepNeverReportsCorruptionOnIntactData() throws Exception {
        final int ops = countOps();
        Assert.assertTrue("expected a real durability-op sequence to sweep, got " + ops, ops >= 4);

        for (int crashAt = 1; crashAt <= ops; crashAt++) {
            final int point = crashAt;
            runWithCrashFacade(() -> {
                final String t = "pchk" + point; // the db root persists across iterations
                execute("create table " + t + " (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into " + t + " values ('2024-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                // Seal 2024-01-01 BEFORE the baseline. A partition is only sealed once a later one
                // is written, so taking the baseline after the first insert would leave the sidecar
                // entirely post-baseline -- the crash would roll it away every time and the sweep
                // would verify nothing.
                execute("insert into " + t + " values ('2024-01-02T00:00:00.000000Z', 2)");
                drainWalQueue();
                markDurableBaseline();

                crashFf.armCrashAt(point);
                try {
                    execute("insert into " + t + " values ('2024-01-03T00:00:00.000000Z', 3)");
                    drainWalQueue();
                } catch (CrashSimulationError | CairoException e) {
                    // expected at most crash points
                }
                Assert.assertFalse("crash armed at op " + point + " never fired", crashFf.isCrashArmed());
                crashAndReopen();

                assertVerifiesOrUncovered(t, "2024-01-01", point);
            });
        }
        // Non-vacuity. assertVerifiesOrUncovered returns early when coverage is ABSENT, so a sweep
        // in which the sidecar never survived would pass against ANY implementation -- including one
        // that writes no checksums at all.
        Assert.assertTrue(
                "no crash iteration produced surviving coverage, so this sweep verified nothing",
                verifiedIterations > 0
        );
        Assert.assertTrue(
                "coverage survived but no file was actually hashed and compared",
                verifiedFiles > 0
        );
    }

    /**
     * Same one-sided bar as the sweep above, but under {@code commit_mode='nosync'}, and with the
     * durability half of the oracle deliberately removed.
     * <p>
     * NOSYNC is not crash-safe and is not meant to be -- ADAPTIVE is the mode designed for that -- so
     * this asserts NOTHING about rows surviving. A nosync crash may lose the tail of a partition, or
     * the partition, or the table; all of those are correct outcomes for the mode. The bar is only:
     * <pre>
     *   losing data under nosync must not be REPORTED AS CORRUPTION.
     * </pre>
     * Nosync is the one mode with no protection on either side of the sidecar-vs-columns race.
     * SYNC/ASYNC flush the sidecar under the same {@code appliesColumnSync} predicate as the columns
     * it covers, so their relative order survives a crash. ADAPTIVE flushes neither, but recovery
     * rolls back to a validated epoch and re-derives coverage. Under NOSYNC neither is flushed and
     * nothing re-derives, so writeback can land {@code _chk}'s recorded length while the column's
     * tail is still lost -- and {@code TableReader.verifyPartitionStructure} turns
     * {@code actual < recorded} into a hard {@code CairoException}. That would condemn a partition
     * for doing exactly what nosync promises.
     */
    @Test
    public void testNosyncCrashIsNotReportedAsCorruption() throws Exception {
        // Deliberately NOT a crash-point sweep. The sweep arms a crash at the Nth durability op, and
        // nosync performs none -- the first attempt at this test failed with "crash armed at op 1
        // never fired", which is the harness telling the truth: an op-driven instrument cannot reach
        // a mode that emits no ops. The skew is constructed directly instead, with markFileDurable,
        // which models the kernel writing back one file's dirty mmap pages while its neighbours stay
        // lost -- precisely what nosync permits and what no msync from QuestDB orders.
        runWithCrashFacade(() -> {
            execute("create table pchkns (ts timestamp, v long) timestamp(ts) partition by day wal");
            // Per-table override rather than a global one: this is the effective-mode path an
            // operator actually takes, and it leaves the rest of the harness on its own default.
            execute("alter table pchkns set param commit_mode='nosync'");
            execute("insert into pchkns values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            // A partition is only sealed once a later one is written, so 2024-01-02 is what gives
            // 2024-01-01 a sidecar at all.
            execute("insert into pchkns values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            markDurableBaseline();

            // Shorten a covered column and promote ONLY that file to durable. Growing the partition
            // instead does not work: an O3 insert into 2024-01-01 publishes a NEW partition version
            // directory, which the crash rolls away wholesale, leaving the baseline pair
            // self-consistent -- the first attempt at this test failed exactly that way. Losing a
            // tail models what nosync actually permits: the column's last pages never reached the
            // platter while the sidecar's record of them did.
            final File dir = partitionDir("pchkns", "2024-01-01");
            final String victim = shortenOneCoveredFile(dir);
            crashFf.markFileDurable(victim);
            crashAndReopen();

            assertHazardConstructed("2024-01-01");
            // Deliberately NOT assertVerifiesOrUncovered: that bar encodes the ADAPTIVE premise
            // "the data is intact and only the sidecar is behind", so a MISMATCH there means intact
            // data was condemned. Here the data really is short -- that is what nosync permits -- so
            // the sidecar is entitled to notice. The only question that matters is what the DATABASE
            // does about it.
            assertNoChecksumVerdictOnRead("pchkns", -1);
        });
    }

    @Test
    public void testTornSlotFallsBackRatherThanCondemningTheData() throws Exception {
        // Two generations, then destroy the newer slot. The reader must fall back to the older one,
        // and under no circumstances read a torn SIDECAR as evidence about the DATA.
        runWithCrashFacade(() -> {
            execute("create table pchktorn (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into pchktorn values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            execute("insert into pchktorn values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            execute("insert into pchktorn values ('2024-01-03T00:00:00.000000Z', 3)");
            drainWalQueue();
            markDurableBaseline();

            final File dir = partitionDir("pchktorn", "2024-01-01");
            // The whole second slot: whichever generation lives there must be discarded entirely.
            try (Path p = new Path()) {
                p.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
                crashFf.tornTail(p.$(), PartitionChecksumSidecar.HEADER_SIZE, 64);
            }
            crashAndReopen();

            assertVerifiesOrUncovered("pchktorn", "2024-01-01", -1);
        });
    }

    @Test
    public void testTornHeaderDegradesToUncovered() throws Exception {
        // Losing the header costs DETECTION, never availability. Condemning a partition whose
        // sidecar header no longer parses would take out a healthy table.
        runWithCrashFacade(() -> {
            execute("create table pchkhdr (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into pchkhdr values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            execute("insert into pchkhdr values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            markDurableBaseline();

            final File dir = partitionDir("pchkhdr", "2024-01-01");
            try (Path p = new Path()) {
                p.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
                crashFf.tornTail(p.$(), 0, PartitionChecksumSidecar.HEADER_SIZE);
            }
            crashAndReopen();

            assertVerifiesOrUncovered("pchkhdr", "2024-01-01", -1);
            // and the table itself still reads
            Assert.assertEquals(2L, rowCount("pchkhdr"));
        });
    }

    /**
     * The bar. Coverage may be PRESENT_OK or ABSENT; when it is PRESENT_OK every covered file must
     * verify against its own bytes. A MISMATCH means intact data was condemned.
     */
    private void assertVerifiesOrUncovered(String tableName, String partitionName, int crashPoint) {
        final File dir;
        try {
            dir = partitionDirOrNull(tableName, partitionName);
        } catch (CairoException e) {
            return; // the table did not survive the crash at all: acceptable
        }
        if (dir == null) {
            return; // partition gone: acceptable rollback outcome
        }
        try (Path chk = new Path(); Path data = new Path();
             PartitionChecksumSidecar sidecar = new PartitionChecksumSidecar()) {
            chk.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
            sidecar.of(configuration.getFilesFacade(), chk, configuration.getPartitionChecksumBlockSize());
            if (sidecar.coverage() != ChecksumTrailer.PRESENT_OK) {
                return; // uncovered: the safe degradation
            }
            final int n = sidecar.fileCount();
            if (n > 0) {
                verifiedIterations++;
            }
            for (int i = 0; i < n; i++) {
                data.of(dir.getAbsolutePath()).concat(sidecar.fileName(i));
                final int verdict = sidecar.verifyFile(configuration.getFilesFacade(), data.$(), i);
                if (verdict != ChecksumTrailer.ABSENT) {
                    verifiedFiles++;
                }
                if (verdict == ChecksumTrailer.MISMATCH) {
                    Assert.fail("crash at op " + crashPoint + " left intact data condemned: "
                            + sidecar.fileName(i) + " in " + dir.getName()
                            + " reported corrupt at block " + sidecar.lastMismatchBlock());
                }
            }
        }
    }

    private int countOps() throws Exception {
        final int[] ops = new int[1];
        runWithCrashFacade(() -> {
            execute("create table pchkprobe (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into pchkprobe values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            markDurableBaseline();
            final int before = crashFf.durabilityOpCount();
            execute("insert into pchkprobe values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            ops[0] = crashFf.durabilityOpCount() - before;
        });
        return ops[0];
    }

    private File partitionDir(String tableName, String partitionName) {
        final File dir = partitionDirOrNull(tableName, partitionName);
        Assert.assertNotNull("partition directory not found for " + partitionName, dir);
        return dir;
    }

    private File partitionDirOrNull(String tableName, String partitionName) {
        final File tableDir = new File(
                configuration.getDbRoot().toString(),
                engine.verifyTableName(tableName).getDirName()
        );
        final File[] candidates = tableDir.listFiles();
        if (candidates == null) {
            return null;
        }
        File best = null;
        for (File f : candidates) {
            if (!f.isDirectory()) {
                continue;
            }
            final String name = f.getName();
            if ((name.equals(partitionName) || name.startsWith(partitionName + "."))
                    && (best == null || name.compareTo(best.getName()) > 0)) {
                best = f;
            }
        }
        return best;
    }

    private long rowCount(String table) {
        try (io.questdb.cairo.sql.RecordCursorFactory f = select("select count() from " + table)) {
            try (io.questdb.cairo.sql.RecordCursor c = f.getCursor(sqlExecutionContext)) {
                return c.hasNext() ? c.getRecord().getLong(0) : 0L;
            }
        } catch (io.questdb.griffin.SqlException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Truncates the first non-empty file the sidecar covers and returns its absolute path. Models the
     * column's trailing pages never reaching the platter while the sidecar's record of them did --
     * a state nosync permits, because it orders neither.
     */
    private String shortenOneCoveredFile(File dir) {
        try (Path chk = new Path(); PartitionChecksumSidecar sidecar = new PartitionChecksumSidecar()) {
            chk.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
            sidecar.of(configuration.getFilesFacade(), chk, configuration.getPartitionChecksumBlockSize());
            Assert.assertEquals(
                    "no sidecar to build the skew from", ChecksumTrailer.PRESENT_OK, sidecar.coverage()
            );
            for (int i = 0, n = sidecar.fileCount(); i < n; i++) {
                final long recorded = sidecar.fileLength(i);
                if (recorded <= 1) {
                    continue;
                }
                final File victim = new File(dir, sidecar.fileName(i).toString());
                if (!victim.exists()) {
                    continue;
                }
                try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(victim, "rw")) {
                    raf.setLength(recorded - 1);
                } catch (java.io.IOException e) {
                    throw new RuntimeException("could not shorten " + victim, e);
                }
                return victim.getAbsolutePath();
            }
        }
        Assert.fail("sidecar covered no file long enough to shorten");
        return null;
    }

    /**
     * Fails unless the crash really left the sidecar claiming a length its covered file no longer
     * has. Without this the test is vacuous: if the skew was never constructed, "no corruption was
     * reported" is true of an implementation that cannot report corruption at all, and true of one
     * that would have condemned the partition given the chance.
     */
    private void assertHazardConstructed(String partitionName) {
        final File dir = partitionDirOrNull("pchkns", partitionName);
        Assert.assertNotNull("partition vanished, so the skew was never constructed", dir);
        try (Path chk = new Path(); Path data = new Path();
             PartitionChecksumSidecar sidecar = new PartitionChecksumSidecar()) {
            chk.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
            sidecar.of(configuration.getFilesFacade(), chk, configuration.getPartitionChecksumBlockSize());
            Assert.assertEquals(
                    "sidecar did not survive the crash, so the skew was never constructed",
                    ChecksumTrailer.PRESENT_OK, sidecar.coverage()
            );
            boolean shortFileFound = false;
            for (int i = 0, n = sidecar.fileCount(); i < n; i++) {
                data.of(dir.getAbsolutePath()).concat(sidecar.fileName(i));
                final long actual = configuration.getFilesFacade().length(data.$());
                if (actual >= 0 && actual < sidecar.fileLength(i)) {
                    shortFileFound = true;
                    break;
                }
            }
            Assert.assertTrue(
                    "no covered file came back shorter than recorded: the sidecar-ahead-of-columns "
                            + "skew this test exists to exercise was not constructed",
                    shortFileFound
            );
        }
    }

    /**
     * Scans column data so the partition is really opened -- {@code verifyPartitionStructure} runs
     * inside {@code openPartition0}, and nothing else calls it. It must NOT be a {@code count()}:
     * {@code CountRecordCursorFactory} answers from {@code baseCursor.size()}, so a count-based
     * version of this assertion passes without ever opening a partition, i.e. vacuously. Rows read
     * are counted for exactly that reason.
     * <p>
     * The row count itself is not asserted: under nosync any number is legitimate, including zero and
     * including the table being gone. Only a CHECKSUM verdict fails this -- every other failure is
     * nosync behaving as documented, so matching on the verdict text rather than the exception type
     * is deliberate.
     */
    private void assertNoChecksumVerdictOnRead(String tableName, int crashPoint) {
        int rowsScanned = 0;
        try (io.questdb.cairo.sql.RecordCursorFactory f = select("select ts, v from " + tableName)) {
            try (io.questdb.cairo.sql.RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final io.questdb.cairo.sql.Record r = c.getRecord();
                while (c.hasNext()) {
                    r.getTimestamp(0);
                    r.getLong(1);
                    rowsScanned++;
                }
            }
        } catch (Throwable th) {
            final String msg = String.valueOf(th.getMessage());
            if (msg.contains("shorter than recorded") || msg.contains("failed checksum verification")) {
                Assert.fail("crash at op " + crashPoint + " under nosync reported LOST data as CORRUPT: " + msg);
            }
            return; // anything else: nosync lost data, or the table did not survive. Both are correct.
        }
        // Completing the scan without an exception only means something if a partition was opened.
        Assert.assertTrue(
                "scan returned no rows, so no partition was opened and this assertion proved nothing",
                rowsScanned > 0
        );
    }
}
