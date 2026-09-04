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

import io.questdb.PropertyKey;
import io.questdb.mp.Job;
import io.questdb.cairo.PartitionChecksumScrubJob;
import io.questdb.cairo.PartitionChecksumSidecar;
import io.questdb.cairo.TableToken;
import io.questdb.std.Os;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * The scrub is the only place block hashes are actually verified -- partition open is structural by
 * design -- so without it the whole vector is written and never read.
 */
public class PartitionChecksumScrubJobTest extends AbstractCairoTest {

    /**
     * The shipped default is OFF ({@code cairo.partition.checksum.scrub.bytes.per.second} = 0), and
     * off must mean the job touches nothing at all -- not "runs but finds nothing". Nothing asserted
     * that before: every other test sets no rate and reaches the scrub through {@code runFully()},
     * which bypasses the guard entirely, so a regression that started scrubbing unthrottled by
     * default would not have failed a single test.
     */
    @Test
    public void testDisabledByDefaultDoesNoWork() throws Exception {
        assertMemoryLeak(() -> {
            createSealed("s7");
            engine.getCorruptPartitionRegistry().clear();
            flipByteInFirstCoveredFile(partitionDir("s7", "2024-01-01"));

            final PartitionChecksumScrubJob job = new PartitionChecksumScrubJob(engine);
            for (int i = 0; i < 5; i++) {
                Assert.assertFalse("a disabled scrub must report no work done", job.run(Job.RUNNING_STATUS));
                Os.sleep(5);
            }
            Assert.assertEquals("a disabled scrub must not hash a single byte", 0L, job.bytesHashed());
            // And it must not condemn: the partition IS corrupt, so a job that ran would have found it.
            Assert.assertTrue("a disabled scrub must reach no verdict, even on genuinely corrupt bytes",
                    engine.getCorruptPartitionRegistry().isEmpty());
        });
    }

    /**
     * Drives the PRODUCTION entry point, {@code runSerially()} via {@code run()}, rather than
     * {@code runFully()}.
     * <p>
     * Every other test in this class calls {@code runFully()}, whose own javadoc says it "ignores the
     * throttle. For tests." -- so before this test {@code runSerially()} had ZERO covered instructions
     * (JaCoCo: mi=9, ci=0 on its first line). That left the entire throttle untested: the
     * enable/rate guard, the budget accrual arithmetic, the first-tick stamp initialisation, the
     * table-cursor rotation, and the {@code spent > 0} contract that tells the worker pool whether
     * this job did work.
     * <p>
     * The throttle is exactly what earns this feature the right to exist as a background job -- the
     * documented worry is a scrub perturbing engine resource accounting, and the rate limit is the
     * mitigation. A defect in the budget arithmetic (a bad divisor, an overflow, a cursor that never
     * advances) could not have been caught by any existing test.
     */
    @Test
    public void testProductionPathThrottlesThenCondemns() throws Exception {
        // Read in the job's CONSTRUCTOR, so it must be set before the job is built.
        setProperty(PropertyKey.CAIRO_PARTITION_CHECKSUM_SCRUB_BYTES_PER_SECOND, "104857600");
        assertMemoryLeak(() -> {
            createSealed("s6");
            engine.getCorruptPartitionRegistry().clear();
            final File dir = partitionDir("s6", "2024-01-01");
            flipByteInFirstCoveredFile(dir);

            final PartitionChecksumScrubJob job = new PartitionChecksumScrubJob(engine);

            // First tick banks the clock stamp and MUST do nothing: budgetStampMs starts at 0, so
            // elapsedMs is 0, so no budget accrues and the job returns false before touching a table.
            // Asserting this pins the documented "0 disables / nothing before time passes" behaviour
            // rather than leaving it to chance.
            Assert.assertFalse("the first tick has no elapsed time, so it must not scrub",
                    job.run(Job.RUNNING_STATUS));
            Assert.assertEquals("nothing may be hashed before any budget has accrued",
                    0L, job.bytesHashed());

            // Let real time pass -- the job reads configuration.getMillisecondClock(), which is the
            // real wall clock and is not redirected by setCurrentMicros. A generous rate keeps the
            // wait short while still exercising the accrual arithmetic.
            long deadline = System.currentTimeMillis() + 30_000;
            while (engine.getCorruptPartitionRegistry().isEmpty() && System.currentTimeMillis() < deadline) {
                Os.sleep(20);
                job.run(Job.RUNNING_STATUS);
            }

            final TableToken token = engine.verifyTableName("s6");
            final String reason = engine.getCorruptPartitionRegistry().reasonFor(token, dir.getName());
            Assert.assertNotNull("the throttled production path must still reach and condemn the"
                    + " corrupted partition", reason);
            Assert.assertTrue("the production path must have hashed real bytes", job.bytesHashed() > 0);
        });
    }

    @Test
    public void testCorruptedBlockIsFoundAndCondemned() throws Exception {
        assertMemoryLeak(() -> {
            createSealed("s1");
            engine.getCorruptPartitionRegistry().clear();
            final File dir = partitionDir("s1", "2024-01-01");
            flipByteInFirstCoveredFile(dir);

            final PartitionChecksumScrubJob job = new PartitionChecksumScrubJob(engine);
            job.runFully();

            final TableToken token = engine.verifyTableName("s1");
            final String reason = engine.getCorruptPartitionRegistry().reasonFor(token, dir.getName());
            Assert.assertNotNull("the scrub must condemn a partition whose bytes no longer hash", reason);
            Assert.assertTrue("the verdict must localise the fault to a block: " + reason,
                    reason.contains("block"));
            Assert.assertTrue("the scrub must actually have hashed bytes", job.bytesHashed() > 0);
        });
    }

    @Test
    public void testCondemnedPartitionFailsOnlyTheQueriesTouchingIt() throws Exception {
        // The verdict has to reach the read path, and it has to be SCOPED. A registry that condemns a
        // partition but never fails a query is decoration; one that fails the whole table is just
        // suspension with extra steps.
        assertMemoryLeak(() -> {
            createSealed("s5");
            engine.getCorruptPartitionRegistry().clear();
            final File dir = partitionDir("s5", "2024-01-01");
            flipByteInFirstCoveredFile(dir);

            new PartitionChecksumScrubJob(engine).runFully();
            Assert.assertFalse("precondition: the scrub must have condemned it",
                    engine.getCorruptPartitionRegistry().isEmpty());

            engine.releaseInactive();
            try {
                sumV("s5", "where ts < '2024-01-02'");
                Assert.fail("a query touching the condemned partition must fail");
            } catch (io.questdb.cairo.CairoException e) {
                io.questdb.test.tools.TestUtils.assertContains(
                        e.getFlyweightMessage(), "failed checksum verification");
            }

            // ... and the healthy partition still answers.
            Assert.assertEquals(99L, sumV("s5", "where ts >= '2024-01-02'"));
        });
    }

    @Test
    public void testClearingTheVerdictRestoresTheQuery() throws Exception {
        assertMemoryLeak(() -> {
            createSealed("s6");
            engine.getCorruptPartitionRegistry().clear();
            final File dir = partitionDir("s6", "2024-01-01");
            engine.getCorruptPartitionRegistry().condemn(
                    engine.verifyTableName("s6"), dir.getName(), "synthetic");
            engine.releaseInactive();
            try {
                sumV("s6", "");
                Assert.fail("expected the verdict to fail the query");
            } catch (io.questdb.cairo.CairoException ignored) {
            }

            engine.getCorruptPartitionRegistry().clear(engine.verifyTableName("s6"), dir.getName());
            engine.releaseInactive();
            Assert.assertEquals(165L, sumV("s6", ""));
        });
    }

    @Test
    public void testHealthyTableProducesNoVerdict() throws Exception {
        // Negative control for the test above. Also asserts the job really hashed bytes: without that
        // this passes against a job that does nothing at all.
        assertMemoryLeak(() -> {
            createSealed("s2");
            engine.getCorruptPartitionRegistry().clear();

            final PartitionChecksumScrubJob job = new PartitionChecksumScrubJob(engine);
            job.runFully();

            Assert.assertTrue("the scrub hashed nothing, so a clean result proves nothing",
                    job.bytesHashed() > 0);
            Assert.assertTrue("a healthy table must not be condemned",
                    engine.getCorruptPartitionRegistry().isEmpty());
        });
    }

    @Test
    public void testUncoveredPartitionIsSkippedSilently() throws Exception {
        // Absent coverage is the upgrade-on-write state, not a fault.
        assertMemoryLeak(() -> {
            createSealed("s3");
            engine.getCorruptPartitionRegistry().clear();
            final File chk = new File(partitionDir("s3", "2024-01-01"), PartitionChecksumSidecar.FILE_NAME);
            Assert.assertTrue(chk.exists());
            Assert.assertTrue(chk.delete());

            final PartitionChecksumScrubJob job = new PartitionChecksumScrubJob(engine);
            job.runFully();

            Assert.assertTrue("an uncovered partition must not be condemned",
                    engine.getCorruptPartitionRegistry().isEmpty());
        });
    }

    @Test
    public void testVanishedFileProducesNoVerdict() throws Exception {
        // Purges, drops and O3 rewrites race the scrub constantly. A file that disappears under it must
        // not be reported as corruption on an otherwise healthy table.
        assertMemoryLeak(() -> {
            createSealed("s4");
            engine.getCorruptPartitionRegistry().clear();
            final File dir = partitionDir("s4", "2024-01-01");
            deleteFirstCoveredFile(dir);

            final PartitionChecksumScrubJob job = new PartitionChecksumScrubJob(engine);
            job.runFully();

            Assert.assertTrue("a vanished file is not corruption",
                    engine.getCorruptPartitionRegistry().isEmpty());
        });
    }

    private long sumV(String table, String where) {
        try (io.questdb.cairo.sql.RecordCursorFactory f = select("select sum(v) from " + table + " " + where)) {
            try (io.questdb.cairo.sql.RecordCursor c = f.getCursor(sqlExecutionContext)) {
                return c.hasNext() ? c.getRecord().getLong(0) : 0L;
            }
        } catch (io.questdb.griffin.SqlException e) {
            throw new RuntimeException(e);
        }
    }

    private void createSealed(String table) throws Exception {
        execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
        for (int i = 0; i < 12; i++) {
            execute("insert into " + table + " values ('2024-01-01T0" + (i % 10) + ":00:0" + (i % 10)
                    + ".00000" + i + "Z', " + i + ")");
        }
        execute("insert into " + table + " values ('2024-01-02T00:00:00.000000Z', 99)");
        drainWalQueue();
        engine.releaseInactive();
    }

    private void deleteFirstCoveredFile(File dir) {
        try (Path chk = new Path(); PartitionChecksumSidecar sidecar = new PartitionChecksumSidecar()) {
            chk.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
            sidecar.of(configuration.getFilesFacade(), chk, configuration.getPartitionChecksumBlockSize());
            Assert.assertTrue(sidecar.fileCount() > 0);
            final File victim = new File(dir, sidecar.fileName(0).toString());
            Assert.assertTrue("could not delete " + victim, victim.delete());
        }
    }

    private void flipByteInFirstCoveredFile(File dir) {
        try (Path chk = new Path(); Path data = new Path();
             PartitionChecksumSidecar sidecar = new PartitionChecksumSidecar()) {
            chk.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
            sidecar.of(configuration.getFilesFacade(), chk, configuration.getPartitionChecksumBlockSize());
            Assert.assertTrue("nothing covered to corrupt", sidecar.fileCount() > 0);
            data.of(dir.getAbsolutePath()).concat(sidecar.fileName(0));
            final io.questdb.std.FilesFacade ff = configuration.getFilesFacade();
            final long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            final long fd = ff.openRW(data.$(), 0);
            try {
                ff.read(fd, buf, 1, 0);
                Unsafe.getUnsafe().putByte(buf, (byte) (Unsafe.getUnsafe().getByte(buf) ^ 0xFF));
                ff.write(fd, buf, 1, 0);
            } finally {
                ff.close(fd);
                Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private File partitionDir(String tableName, String partitionName) {
        final File tableDir = new File(
                configuration.getDbRoot().toString(),
                engine.verifyTableName(tableName).getDirName()
        );
        final File[] candidates = tableDir.listFiles();
        Assert.assertNotNull(candidates);
        File best = null;
        for (File f : candidates) {
            if (f.isDirectory()
                    && (f.getName().equals(partitionName) || f.getName().startsWith(partitionName + "."))
                    && (best == null || f.getName().compareTo(best.getName()) > 0)) {
                best = f;
            }
        }
        Assert.assertNotNull("partition directory not found for " + partitionName, best);
        return best;
    }
}
