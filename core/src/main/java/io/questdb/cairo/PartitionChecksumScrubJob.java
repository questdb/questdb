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

package io.questdb.cairo;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.Path;

/**
 * Verifies per-partition block hashes in the background, at a bounded rate.
 * <p>
 * This is the only place block hashes are actually checked. Partition open is deliberately structural
 * (lengths only) because column files are mmap'd and hashing there would put O(bytes) on the query
 * path, so without this job the vector is written and never read.
 * <p>
 * Three rules keep it from doing harm:
 * <ul>
 *   <li><b>Throttled.</b> At most {@code cairo.partition.checksum.scrub.bytes.per.second} bytes are
 *       hashed per wall-clock second. 0 disables the job.</li>
 *   <li><b>A vanished file is not corruption.</b> Purges, drops and O3 rewrites race the scrub
 *       constantly; a file that disappears mid-scan yields no verdict.</li>
 *   <li><b>An uncovered partition is not a fault.</b> Absent coverage is the upgrade-on-write state,
 *       skipped silently.</li>
 * </ul>
 */
public class PartitionChecksumScrubJob extends SynchronizedJob {
    private static final Log LOG = LogFactory.getLog(PartitionChecksumScrubJob.class);

    private final long bytesPerSecond;
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final ObjHashSet<TableToken> tableTokens = new ObjHashSet<>();
    private long bytesHashedTotal;
    private long budgetBytes;
    private long budgetStampMs;
    private int tableCursor;

    public PartitionChecksumScrubJob(CairoEngine engine) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = configuration.getFilesFacade();
        this.bytesPerSecond = configuration.getPartitionChecksumScrubBytesPerSecond();
    }

    /**
     * Bytes hashed since this job was created. Lets a test assert the job did real work.
     */
    public long bytesHashed() {
        return bytesHashedTotal;
    }

    /**
     * Runs the scrub to completion over every table, ignoring the throttle. For tests.
     */
    public void runFully() {
        engine.getTableTokens(tableTokens, false);
        for (int i = 0, n = tableTokens.size(); i < n; i++) {
            scrubTable(tableTokens.get(i), Long.MAX_VALUE);
        }
    }

    @Override
    protected boolean runSerially() {
        if (!configuration.isPartitionChecksumEnabled() || bytesPerSecond <= 0) {
            return false;
        }
        final long nowMs = configuration.getMillisecondClock().getTicks();
        if (budgetStampMs == 0) {
            budgetStampMs = nowMs;
        }
        final long elapsedMs = nowMs - budgetStampMs;
        if (elapsedMs > 0) {
            budgetBytes = Math.min(bytesPerSecond, budgetBytes + bytesPerSecond * elapsedMs / 1000);
            budgetStampMs = nowMs;
        }
        if (budgetBytes <= 0) {
            return false;
        }

        engine.getTableTokens(tableTokens, false);
        if (tableTokens.size() == 0) {
            return false;
        }
        if (tableCursor >= tableTokens.size()) {
            tableCursor = 0; // resumable: start the sweep again rather than tracking a global cursor
        }
        final TableToken token = tableTokens.get(tableCursor++);
        final long spent = scrubTable(token, budgetBytes);
        budgetBytes -= spent;
        return spent > 0;
    }

    /**
     * Hashes up to {@code budget} bytes of one table's SEALED partitions. Returns bytes hashed.
     * <p>
     * Enumerates through a {@link TableReader} rather than walking the directory. That is what makes
     * the scan sound: the reader pins a txn, so the partition VERSIONS it reports cannot be purged or
     * replaced underneath the scrub, and QuestDB writes an O3 rewrite into a NEW directory version
     * rather than mutating a pinned one. Walking the filesystem blind is what made the scrub condemn
     * eight healthy partitions in O3Test.
     * <p>
     * The last partition is skipped unconditionally: it is the active append target, is never sealed,
     * and its files legitimately change while being read.
     */
    private long scrubTable(TableToken token, long budget) {
        long spent = 0;
        try (TableReader reader = engine.getReader(token)) {
            final int partitionCount = reader.getPartitionCount();
            final int timestampType = reader.getMetadata().getTimestampType();
            final int partitionBy = reader.getPartitionedBy();
            if (!PartitionBy.isPartitioned(partitionBy)) {
                return 0;
            }
            for (int i = 0; i < partitionCount - 1 && spent < budget; i++) {
                if (reader.getPartitionFormat(i) != PartitionFormat.NATIVE) {
                    continue; // parquet verifies its own page CRCs
                }
                final Path path = Path.getThreadLocal(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForNativePartition(
                        path,
                        timestampType,
                        partitionBy,
                        reader.getPartitionTimestampByIndex(i),
                        reader.getPartitionNameTxnByIndex(i)
                );
                final java.io.File dir = new java.io.File(path.toString());
                if (!dir.isDirectory()) {
                    continue;
                }
                try (PartitionChecksumSidecar sidecar = new PartitionChecksumSidecar()) {
                    spent += scrubPartition(token, dir, sidecar, budget - spent);
                }
            }
        } catch (Throwable th) {
            // Diagnostic only: never take a worker thread, or a table, down.
            LOG.error().$("partition checksum scrub failed [table=").$(token)
                    .$(", error=").$(th.getMessage()).I$();
        }
        return spent;
    }

    /**
     * Second opinion on a mismatch, from a sidecar re-read from disk.
     * <p>
     * Returns false -- no verdict -- when the generation moved (the partition was re-sealed, so the
     * bytes changed legitimately) or when the re-check passes. A false positive here takes a healthy
     * partition offline, so the burden of proof sits on the accusation, not on the data.
     */
    private boolean confirmMismatch(java.io.File dir, Path data, int entryIndex, long generationBefore) {
        try (Path chk = new Path(); PartitionChecksumSidecar fresh = new PartitionChecksumSidecar()) {
            chk.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
            fresh.of(ff, chk, configuration.getPartitionChecksumBlockSize());
            if (fresh.coverage() != ChecksumTrailer.PRESENT_OK || fresh.generation() != generationBefore) {
                return false;
            }
            return fresh.verifyFile(ff, data.$(), entryIndex) == ChecksumTrailer.MISMATCH;
        } catch (Throwable th) {
            return false;
        }
    }

    private long scrubPartition(TableToken token, java.io.File dir, PartitionChecksumSidecar sidecar, long budget) {
        long spent = 0;
        try (Path chk = new Path(); Path data = new Path()) {
            chk.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
            if (!ff.exists(chk.$())) {
                return 0; // uncovered: upgrade-on-write, not a fault
            }
            sidecar.of(ff, chk, configuration.getPartitionChecksumBlockSize());
            if (sidecar.coverage() != ChecksumTrailer.PRESENT_OK) {
                return 0;
            }
            for (int i = 0, n = sidecar.fileCount(); i < n && spent < budget; i++) {
                data.of(dir.getAbsolutePath()).concat(sidecar.fileName(i));
                final long len = ff.length(data.$());
                if (len < 0) {
                    continue; // vanished under us: a purge racing the scrub is not corruption
                }
                final long generationBefore = sidecar.generation();
                final int verdict = sidecar.verifyFile(ff, data.$(), i);
                spent += Math.min(len, sidecar.fileLength(i));
                if (verdict == ChecksumTrailer.MISMATCH) {
                    // A mismatch is NOT yet a verdict. The scrub reads the sidecar and the data at
                    // different instants and holds no lock, so a partition rewritten in between
                    // mismatches for entirely healthy reasons -- registering the job in the shared
                    // worker pool made O3Test condemn eight healthy partitions this way. Corroborate
                    // against a freshly-read sidecar and require the generation to be unchanged: if
                    // the partition was re-sealed under us, the bytes changed legitimately and there
                    // is nothing to report.
                    final String detail = sidecar.fileName(i) + " block " + sidecar.lastMismatchBlock();
                    if (confirmMismatch(dir, data, i, generationBefore)) {
                        engine.getCorruptPartitionRegistry().condemn(token, dir.getName(), detail);
                    } else {
                        LOG.info().$("checksum mismatch not corroborated, partition changed under the scrub [path=")
                                .$(dir.getAbsolutePath()).I$();
                    }
                    break; // one verdict per partition is enough to fail its queries
                }
            }
            bytesHashedTotal += spent;
        } finally {
            sidecar.close();
        }
        return spent;
    }

}
