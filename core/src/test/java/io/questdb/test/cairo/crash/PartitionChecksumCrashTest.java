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

    /** Iterations in which a generation actually survived and files were really verified. */
    private int verifiedIterations;
    /** Covered files actually hashed and compared across the sweep. */
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
}
