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

import io.questdb.cairo.ChecksumTrailer;
import io.questdb.cairo.PartitionChecksumSidecar;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Coverage must never outlive the bytes it describes.
 * <p>
 * A sealed partition carries a block-hash vector. Anything that rewrites those bytes afterwards must
 * leave the partition either RE-COVERED (hashes match the new bytes) or UNCOVERED -- never covered by
 * hashes describing the previous contents, which reads as data corruption on a healthy table.
 * <p>
 * Most mutation paths get this for free: partition directories are versioned, so an O3 rewrite lands
 * in a NEW directory that starts with no sidecar. This test exists for the ones that do not, and it
 * is written as a MATRIX rather than a list of known-bad paths on purpose -- the dangerous case is
 * the mutation nobody thought to enumerate. Each case asserts the invariant directly instead of
 * asserting that some particular implementation detail fired.
 */
public class PartitionChecksumInvalidationTest extends AbstractCairoTest {

    @Test
    public void testAddColumn() throws Exception {
        assertMutationNeverLeavesStaleCoverage("addcol", t -> {
            execute("alter table " + t + " add column extra double");
            execute("insert into " + t + " values ('2024-01-01T06:00:00.000000Z', 7, 'x', 1.5)");
        });
    }

    @Test
    public void testAddIndex() throws Exception {
        assertMutationNeverLeavesStaleCoverage("addidx", t ->
                execute("alter table " + t + " alter column s add index"));
    }

    @Test
    public void testBackfillIntoSealedPartition() throws Exception {
        // O3 into an already-sealed partition: the classic in-place-looking mutation.
        assertMutationNeverLeavesStaleCoverage("backfill", t ->
                execute("insert into " + t + " values ('2024-01-01T05:00:00.000000Z', 99, 'z')"));
    }

    @Test
    public void testConvertToParquet() throws Exception {
        assertMutationNeverLeavesStaleCoverage("parquet", t ->
                execute("alter table " + t + " convert partition to parquet where ts = '2024-01-01'"));
    }

    @Test
    public void testDropColumn() throws Exception {
        assertMutationNeverLeavesStaleCoverage("dropcol", t ->
                execute("alter table " + t + " drop column v"));
    }

    @Test
    public void testRenameColumn() throws Exception {
        assertMutationNeverLeavesStaleCoverage("rencol", t ->
                execute("alter table " + t + " rename column v to v2"));
    }

    @Test
    public void testSquashPartitions() throws Exception {
        assertMutationNeverLeavesStaleCoverage("sqshtab", t -> {
            // No SQL for this; squash is reachable only through the writer API.
            try (io.questdb.cairo.TableWriter w = getWriter(engine.verifyTableName(t))) {
                w.squashAllPartitionsIntoOne();
            }
        });
    }

    @Test
    public void testUpdate() throws Exception {
        assertMutationNeverLeavesStaleCoverage("upd", t ->
                execute("update " + t + " set v = v + 1000 where ts = '2024-01-01T00:00:00.000000Z'"));
    }

    @Test
    public void testHarnessDetectsStaleCoverage() throws Exception {
        // NEGATIVE CONTROL for the matrix above. Every case there passes by finding no stale coverage,
        // which is indistinguishable from a harness that cannot detect it. Here the bytes of a covered
        // file are changed WITHOUT re-sealing -- exactly what an unhandled in-place mutation would do --
        // and the same helper must fail. If this test ever stops failing-then-passing, the matrix is
        // proving nothing.
        assertMemoryLeak(() -> {
            execute("create table stale (ts timestamp, v long, s symbol) timestamp(ts) partition by day wal");
            execute("insert into stale values ('2024-01-01T00:00:00.000000Z', 1, 'a')");
            execute("insert into stale values ('2024-01-02T00:00:00.000000Z', 2, 'b')");
            drainWalQueue();
            engine.releaseInactive();
            Assert.assertEquals(ChecksumTrailer.PRESENT_OK, coverageOf("stale", "2024-01-01"));

            flipByteInFirstCoveredFile("stale", "2024-01-01");

            boolean detected = false;
            try {
                assertNoStaleCoverage("stale", "2024-01-01");
            } catch (AssertionError expected) {
                detected = true;
            }
            Assert.assertTrue(
                    "the harness did not notice bytes changing under valid coverage, so the matrix above"
                            + " cannot be trusted",
                    detected
            );
        });
    }

    @FunctionalInterface
    private interface Mutation {
        void apply(String tableName) throws Exception;
    }

    /**
     * Seals a partition, checks it really is covered, applies {@code mutation}, then asserts the
     * partition is not left claiming hashes for bytes that have changed.
     */
    private void assertMutationNeverLeavesStaleCoverage(String tableName, Mutation mutation) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table " + tableName
                    + " (ts timestamp, v long, s symbol) timestamp(ts) partition by day wal");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00.000000Z', 1, 'a')");
            execute("insert into " + tableName + " values ('2024-01-01T01:00:00.000000Z', 2, 'b')");
            // A later partition seals 2024-01-01.
            execute("insert into " + tableName + " values ('2024-01-02T00:00:00.000000Z', 3, 'c')");
            drainWalQueue();

            // Precondition. Without this the whole matrix would pass against a build that writes no
            // checksums at all.
            Assert.assertEquals(
                    "the partition must be covered BEFORE the mutation, or this case proves nothing",
                    ChecksumTrailer.PRESENT_OK,
                    coverageOf(tableName, "2024-01-01")
            );

            mutation.apply(tableName);
            drainWalQueue();

            final int after = coverageOfOrAbsent(tableName, "2024-01-01");
            System.out.println("[matrix] " + tableName + ": post-mutation coverage="
                    + (after == ChecksumTrailer.PRESENT_OK ? "PRESENT_OK" : "ABSENT"));
            assertNoStaleCoverage(tableName, "2024-01-01");
            // The table must still be usable: a case that destroyed the partition would otherwise
            // satisfy the invariant vacuously.
            assertSqlReturnsRows(tableName);
        });
    }

    private void assertNoStaleCoverage(String tableName, String partitionName) {
        final File dir = partitionDirOrNull(tableName, partitionName);
        if (dir == null) {
            return; // partition gone entirely: acceptable
        }
        try (Path chk = new Path(); Path data = new Path();
             PartitionChecksumSidecar sidecar = new PartitionChecksumSidecar()) {
            chk.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
            sidecar.of(configuration.getFilesFacade(), chk, configuration.getPartitionChecksumBlockSize());
            if (sidecar.coverage() != ChecksumTrailer.PRESENT_OK) {
                return; // uncovered: the safe outcome
            }
            for (int i = 0, n = sidecar.fileCount(); i < n; i++) {
                data.of(dir.getAbsolutePath()).concat(sidecar.fileName(i));
                final int verdict = sidecar.verifyFile(configuration.getFilesFacade(), data.$(), i);
                if (verdict == ChecksumTrailer.MISMATCH) {
                    Assert.fail("stale coverage after mutation: " + sidecar.fileName(i) + " in "
                            + dir.getName() + " is claimed at block " + sidecar.lastMismatchBlock()
                            + " but its bytes have changed");
                }
            }
        }
    }

    private void assertSqlReturnsRows(String tableName) {
        try (io.questdb.cairo.sql.RecordCursorFactory f = select("select count() from " + tableName)) {
            try (io.questdb.cairo.sql.RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Assert.assertTrue("table must still be queryable after the mutation", c.hasNext());
                Assert.assertTrue(c.getRecord().getLong(0) > 0);
            }
        } catch (io.questdb.griffin.SqlException e) {
            throw new RuntimeException(e);
        }
    }

    private int coverageOf(String tableName, String partitionName) {
        final File dir = partitionDirOrNull(tableName, partitionName);
        Assert.assertNotNull("partition directory not found for " + partitionName, dir);
        try (Path chk = new Path(); PartitionChecksumSidecar sidecar = new PartitionChecksumSidecar()) {
            chk.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
            sidecar.of(configuration.getFilesFacade(), chk, configuration.getPartitionChecksumBlockSize());
            return sidecar.coverage();
        }
    }

    private int coverageOfOrAbsent(String tableName, String partitionName) {
        final File dir = partitionDirOrNull(tableName, partitionName);
        if (dir == null) {
            return ChecksumTrailer.ABSENT;
        }
        try (Path chk = new Path(); PartitionChecksumSidecar sidecar = new PartitionChecksumSidecar()) {
            chk.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
            sidecar.of(configuration.getFilesFacade(), chk, configuration.getPartitionChecksumBlockSize());
            return sidecar.coverage();
        }
    }

    /** Flips one byte inside the first file the sidecar covers, leaving the sidecar untouched. */
    private void flipByteInFirstCoveredFile(String tableName, String partitionName) {
        final File dir = partitionDirOrNull(tableName, partitionName);
        Assert.assertNotNull(dir);
        try (Path chk = new Path(); Path data = new Path();
             PartitionChecksumSidecar sidecar = new PartitionChecksumSidecar()) {
            chk.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
            sidecar.of(configuration.getFilesFacade(), chk, configuration.getPartitionChecksumBlockSize());
            Assert.assertTrue("nothing covered to corrupt", sidecar.fileCount() > 0);
            data.of(dir.getAbsolutePath()).concat(sidecar.fileName(0));
            final io.questdb.std.FilesFacade ff = configuration.getFilesFacade();
            final long buf = io.questdb.std.Unsafe.malloc(1, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            final long fd = ff.openRW(data.$(), 0);
            try {
                ff.read(fd, buf, 1, 0);
                io.questdb.std.Unsafe.getUnsafe().putByte(
                        buf, (byte) (io.questdb.std.Unsafe.getUnsafe().getByte(buf) ^ 0xFF));
                ff.write(fd, buf, 1, 0);
            } finally {
                ff.close(fd);
                io.questdb.std.Unsafe.free(buf, 1, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            }
        }
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
}
