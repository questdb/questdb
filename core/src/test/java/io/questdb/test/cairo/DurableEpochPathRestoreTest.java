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
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A helper handed someone else's {@link Path} must give it back at the length it received it.
 * <p>
 * This is not style. {@code DurableEpochManifest.write} left the caller's path at
 * {@code <table>/_epoch.manifest.<generation>}, and on Linux nobody noticed because the very next call --
 * {@code TableWriter.fsyncEpochDirectory} -- did a {@code trimTo} on the way past. That {@code trimTo}
 * lived INSIDE an {@code if (!Os.isWindows())} guard, so on Windows the whole cleanup vanished along with
 * the directory fsync it was bracketed with. The stale suffix then survived into the next consumer of the
 * path, which concatenated onto it and tried to open
 * {@code <table>/_epoch.manifest.1/_meta.swp}. Every structural DDL on an adaptive table failed at that
 * open -- 60 times in one CI leg -- and, because the swap died before reaching its own fsync, it also hid
 * the separate read-only-handle barrier bug underneath it (see {@code WriteAccessBarrierSweepTest}).
 * <p>
 * The lesson generalises past this one method: <b>a platform guard must not wrap a side effect that is not
 * platform-specific</b>. Restoring a path is not a Windows concern. Neither test can be written on the
 * platform that breaks, so the contract is asserted directly instead -- and it is observable on Linux,
 * because the path is left dirty on EVERY platform. Only the accidental cleanup was Linux-only.
 */
public class DurableEpochPathRestoreTest extends AbstractCairoTest {

    @Test
    public void testFsyncDirectoryRestoresTheCallersPath() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = enrolledAdaptiveTable("dir_restore");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                final int rootLen = path.size();
                path.concat("some_child_component");

                DurableEpochManifest.fsyncDirectory(configuration, path, rootLen);

                // Must hold on Windows too, where the fsync itself is impossible and skipped. A guard that
                // returns early may not also skip putting the caller's path back.
                Assert.assertEquals(
                        "fsyncDirectory must leave the path at rootLen on every platform, got: " + path,
                        rootLen,
                        path.size()
                );
            }
        });
    }

    @Test
    public void testWriteRestoresTheCallersPath() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = enrolledAdaptiveTable("write_restore");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                final int rootLen = path.size();

                DurableEpochManifest.write(configuration, token, path, rootLen, 0, 1, 1, 0, 0);

                Assert.assertEquals(
                        "write must hand the caller's path back at rootLen, got: " + path,
                        rootLen,
                        path.size()
                );
            }
        });
    }

    /**
     * The manifest's REJECTION contract: three cases, each asserting {@code validate()} returns false.
     * <p>
     * The coverage pass on PR #7411 found {@code DurableEpochManifest.validate()} is entered by the
     * existing suites and ALWAYS SUCCEEDS -- its happy path is covered and every rejection branch had
     * zero coverage: the {@code _txn.epoch} payload checksum, the {@code _cv.epoch} payload checksum,
     * and the {@code metadataVersion} agreement. Those are three of the four conditions the PR
     * describes as proving an epoch self-consistent before recovery adopts it, so the guarantee rested
     * on the passing case alone.
     * <p>
     * The torn-copy crash tests do not reach them. They corrupt {@code _txn.epoch} and recovery
     * rejects it earlier, inside {@code RecoveryCoordinator}'s own decode, so they exercise that layer
     * instead. Calling {@code validate()} directly is what separates the two.
     */
    @Test
    public void testValidateRejectsATamperedTxnEpochPayload() throws Exception {
        assertMemoryLeak(() -> assertTamperedPayloadIsRejected("mftxn", TableUtils.TXN_FILE_NAME));
    }

    @Test
    public void testValidateRejectsATamperedCvEpochPayload() throws Exception {
        assertMemoryLeak(() -> assertTamperedPayloadIsRejected("mfcv", TableUtils.COLUMN_VERSION_FILE_NAME));
    }

    @Test
    public void testValidateRejectsADisagreeingMetadataVersion() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = enrolledAdaptiveTable("mfver");
            try (Path p = new Path()) {
                p.of(configuration.getDbRoot()).concat(token);
                final int rootLen = p.size();
                final long[] h = manifestHeader(token);
                Assert.assertTrue("precondition: the untouched epoch must validate",
                        DurableEpochManifest.validate(configuration, token, p, rootLen,
                                (int) h[0], h[1], h[2], h[3], h[4]));
                p.trimTo(rootLen);
                // Identical bytes on disk; only the caller's claimed metadataVersion differs. This is
                // the _txn <-> _meta agreement, and nothing exercised its failure.
                Assert.assertFalse("a metadataVersion disagreeing with the manifest must be rejected",
                        DurableEpochManifest.validate(configuration, token, p, rootLen,
                                (int) h[0], h[1], h[2], h[3], h[4] + 1));
            }
        });
    }

    /**
     * Builds an enrolled table, proves its manifest validates, flips one byte inside the named
     * {@code .epoch} payload, and requires the manifest to reject it. The precondition matters: without
     * it a build where validate() always returned false would pass every case here.
     */
    private void assertTamperedPayloadIsRejected(String table, String payloadName) throws Exception {
        final TableToken token = enrolledAdaptiveTable(table);
        try (Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(token);
            final int rootLen = p.size();
            final long[] h = manifestHeader(token);
            Assert.assertTrue("precondition: the untouched epoch must validate",
                    DurableEpochManifest.validate(configuration, token, p, rootLen,
                            (int) h[0], h[1], h[2], h[3], h[4]));

            final java.io.File payload = new java.io.File(
                    new java.io.File(configuration.getDbRoot().toString(), token.getDirName()),
                    payloadName + TableUtils.EPOCH_COPY_SUFFIX + "." + (int) h[0]);
            Assert.assertTrue("precondition: " + payload.getName() + " must exist", payload.exists());
            try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(payload, "rw")) {
                raf.seek(raf.length() / 2);
                final int b = raf.read();
                raf.seek(raf.length() / 2);
                raf.write(b ^ 0xFF);
            }

            p.trimTo(rootLen);
            Assert.assertFalse("a tampered " + payload.getName() + " must fail the manifest checksum",
                    DurableEpochManifest.validate(configuration, token, p, rootLen,
                            (int) h[0], h[1], h[2], h[3], h[4]));
        }
    }

    /**
     * {generation, seqTxn, txn, columnVersion, metadataVersion} read straight out of the manifest, so
     * the control call is fed exactly what the file claims rather than values guessed by the test.
     */
    private long[] manifestHeader(TableToken token) throws Exception {
        final java.io.File dir = new java.io.File(configuration.getDbRoot().toString(), token.getDirName());
        final java.io.File manifest = new java.io.File(dir, DurableEpochManifest.FILE_NAME + ".0");
        Assert.assertTrue("precondition: a generation-0 manifest must exist", manifest.exists());
        final byte[] buf = new byte[104];
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(manifest, "r")) {
            raf.readFully(buf, 0, Math.min(buf.length, (int) raf.length()));
        }
        final java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(buf).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        return new long[]{bb.getInt(12), bb.getLong(24), bb.getLong(32), bb.getLong(40), bb.getLong(96)};
    }

    /**
     * A WAL table enrolled in adaptive, so its generation-zero epoch payloads exist on disk for the
     * manifest write to checksum.
     */
    private TableToken enrolledAdaptiveTable(String name) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "0");
        execute("CREATE TABLE " + name + " (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + name + " VALUES (1, '2024-06-10T00:00:00.000000Z')");
        drainWalQueue();
        return engine.verifyTableName(name);
    }
}
