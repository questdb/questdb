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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ChecksumTrailer;
import io.questdb.cairo.PartitionChecksumSidecar;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * The write path: a sealed partition gains a checksum vector that actually verifies against its own
 * bytes, and every failure to produce one costs DETECTION rather than ingestion.
 */
public class PartitionChecksumWritePathTest extends AbstractCairoTest {

    @Test
    public void testChecksumsDisabledLeavesNoSidecar() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_CHECKSUM_ENABLED, "false");
        assertMemoryLeak(() -> {
            execute("create table t3 (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into t3 values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into t3 values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            Assert.assertFalse(
                    "the feature must be genuinely off, not merely quiet",
                    new File(partitionDir("t3", "2024-01-01"), PartitionChecksumSidecar.FILE_NAME).exists()
            );
        });
    }

    @Test
    public void testCoverageSurvivesFurtherAppendsToTheSamePartition() throws Exception {
        // The old last block was partial and has now grown. If firstDirtyBlock skipped it, the stored
        // hash would be stale and this fails.
        assertMemoryLeak(() -> {
            execute("create table t2 (ts timestamp, v long) timestamp(ts) partition by day wal");
            for (int i = 0; i < 40; i++) {
                execute("insert into t2 values ('2024-01-01T00:00:0" + (i % 10) + ".0000" + (10 + i) + "Z', " + i + ")");
                drainWalQueue();
            }
            execute("insert into t2 values ('2024-01-02T00:00:00.000000Z', 999)");
            drainWalQueue();
            assertPartitionVerifies("t2", "2024-01-01");
        });
    }

    @Test
    public void testActivePartitionIsNeverSealed() throws Exception {
        // The last partition is the live append target and its files are PRE-EXTENDED, so a length
        // recorded now is contradicted by the truncate at close and the reader calls an intact file
        // truncated. This broke once already: the O3 arming site tests against the
        // lastPartitionTimestamp FIELD, captured before the commit, so the insert that creates a new
        // final partition armed that brand-new active partition. Silent, and only visible as an
        // unexpected file on disk -- hence this test.
        assertMemoryLeak(() -> {
            execute("create table lastp (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into lastp values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into lastp values ('2024-01-02T00:00:00.000000Z', 2)");
            execute("insert into lastp values ('2024-01-03T00:00:00.000000Z', 3)");
            drainWalQueue();

            Assert.assertTrue(
                    "sealed partitions must be covered, or this test proves nothing",
                    new File(partitionDir("lastp", "2024-01-01"), PartitionChecksumSidecar.FILE_NAME).exists()
            );
            Assert.assertTrue(
                    new File(partitionDir("lastp", "2024-01-02"), PartitionChecksumSidecar.FILE_NAME).exists()
            );
            Assert.assertFalse(
                    "the ACTIVE partition must not be sealed while it is still being appended to",
                    new File(partitionDir("lastp", "2024-01-03"), PartitionChecksumSidecar.FILE_NAME).exists()
            );
        });
    }

    @Test
    public void testSealedPartitionGainsCoverageThatVerifies() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into t values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into t values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            assertPartitionVerifies("t", "2024-01-01");
        });
    }

    @Test
    public void testSidecarWriteFailureLeavesIngestionHealthy() throws Exception {
        // Default mode. This file carries no durability claim and is fully re-derivable, so failing
        // to open it must cost DETECTION, never writes.
        assertMemoryLeak(sidecarOpenFailingFacade(), () -> {
            execute("create table t4 (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into t4 values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into t4 values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            Assert.assertEquals(2L, rowCount("t4"));
        });
    }

    @Test
    public void testStrictModeSidecarFailureIsFatal() throws Exception {
        // The exemption from fail-stop is a DEFAULT, not an unconditional rule.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_CHECKSUM_STRICT, "true");
        assertMemoryLeak(sidecarOpenFailingFacade(), () -> {
            execute("create table t5 (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into t5 values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into t5 values ('2024-01-02T00:00:00.000000Z', 2)");
            boolean failed = false;
            try {
                drainWalQueue();
                failed = rowCount("t5") != 2L;
            } catch (CairoException | CairoError e) {
                failed = true;
            }
            Assert.assertTrue("strict mode must not silently degrade to unverified", failed);
        });
    }

    /**
     * Opens the partition's sidecar and verifies every file it covers against that file's real bytes.
     * Asserts a non-zero file count first: a helper that verifies nothing passes against anything.
     */
    private void assertPartitionVerifies(String tableName, String partitionName) {
        final File dir = partitionDir(tableName, partitionName);
        try (Path chk = new Path(); Path data = new Path();
             PartitionChecksumSidecar sidecar = new PartitionChecksumSidecar()) {
            chk.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
            sidecar.of(configuration.getFilesFacade(), chk, configuration.getPartitionChecksumBlockSize());
            Assert.assertEquals(
                    "sealed partition " + partitionName + " has no valid checksum generation",
                    ChecksumTrailer.PRESENT_OK,
                    sidecar.coverage()
            );
            final int n = sidecar.fileCount();
            Assert.assertTrue("the sidecar covers no files, so verifying it proves nothing", n > 0);
            for (int i = 0; i < n; i++) {
                data.of(dir.getAbsolutePath()).concat(sidecar.fileName(i));
                Assert.assertEquals(
                        "covered file " + sidecar.fileName(i) + " does not verify against its own bytes",
                        ChecksumTrailer.PRESENT_OK,
                        sidecar.verifyFile(configuration.getFilesFacade(), data.$(), i)
                );
            }
        }
    }

    /** Finds the on-disk partition directory, whose name carries a name txn suffix after a rewrite. */
    private File partitionDir(String tableName, String partitionName) {
        final File tableDir = new File(
                configuration.getDbRoot().toString(),
                engine.verifyTableName(tableName).getDirName()
        );
        final File[] candidates = tableDir.listFiles();
        Assert.assertNotNull("table directory not found: " + tableDir, candidates);
        File best = null;
        for (File f : candidates) {
            if (!f.isDirectory()) {
                continue;
            }
            final String name = f.getName();
            if (name.equals(partitionName) || name.startsWith(partitionName + ".")) {
                // Highest name txn wins: that is the live version of the partition.
                if (best == null || name.length() > best.getName().length() || name.compareTo(best.getName()) > 0) {
                    best = f;
                }
            }
        }
        Assert.assertNotNull("partition directory not found for " + partitionName + " in " + tableDir, best);
        return best;
    }

    private FilesFacade sidecarOpenFailingFacade() {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, PartitionChecksumSidecar.FILE_NAME)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
    }

    private long rowCount(String table) {
        try (RecordCursorFactory f = select("select count() from " + table)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                return c.hasNext() ? r.getLong(0) : 0L;
            }
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }
}
