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
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A helper handed someone else's {@link Path} must give it back at the length it received it.
 * <p>
 * {@code DurableEpochManifest.write} and {@code DurableEpochManifest.fsyncDirectory} receive the table path.
 * If either leaves a suffix such as {@code _epoch.manifest.<generation>} on it, the next consumer of the path
 * concatenates onto the suffix, and every structural DDL on an adaptive table fails to open its swap file. A
 * platform guard must not wrap a side effect that is not platform-specific: on Windows the directory fsync is
 * skipped, but the path must still be restored. The tests assert the contract directly, so it is observable on
 * every platform.
 * <p>
 * The class also covers the rejection branches of {@code DurableEpochManifest.validate()}.
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
     * {@code DurableEpochManifest.validate()} rejects a manifest whose {@code _txn}, {@code _meta} or
     * {@code _cv} section checksum does not match, or whose metadataVersion disagrees with the caller's. The
     * torn-copy crash tests do not reach these branches: recovery rejects a corrupt saved {@code _txn} earlier,
     * in {@code RecoveryCoordinator}'s own decode, so these tests call {@code validate()} directly.
     */
    @Test
    public void testValidateRejectsATamperedTxnEpochPayload() throws Exception {
        assertMemoryLeak(() -> assertTamperedPayloadIsRejected("mftxn", TableUtils.TXN_FILE_NAME));
    }

    @Test
    public void testValidateRejectsATamperedMetaEpochPayload() throws Exception {
        assertMemoryLeak(() -> assertTamperedPayloadIsRejected("mfmeta", TableUtils.META_FILE_NAME));
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
                // the _txn <-> _meta agreement.
                Assert.assertFalse("a metadataVersion disagreeing with the manifest must be rejected",
                        DurableEpochManifest.validate(configuration, token, p, rootLen,
                                (int) h[0], h[1], h[2], h[3], h[4] + 1));
            }
        });
    }

    /**
     * Builds an enrolled table, proves its manifest validates, flips one byte inside the named file's
     * section, and requires the manifest to reject it. The precondition keeps a validate() that always
     * returns false from passing every case.
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

            final long[] section = TestUtils.savePointSection(configuration.getFilesFacade(), p, configuration.getDbRoot(), token, (int) h[0], payloadName);
            final java.io.File payload = new java.io.File(p.toString());
            try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(payload, "rw")) {
                final long middle = section[0] + section[1] / 2;
                raf.seek(middle);
                final int b = raf.read();
                raf.seek(middle);
                raf.write(b ^ 0xFF);
            }

            p.of(configuration.getDbRoot()).concat(token);
            Assert.assertFalse("a tampered " + payloadName + " section must fail the manifest checksum",
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
