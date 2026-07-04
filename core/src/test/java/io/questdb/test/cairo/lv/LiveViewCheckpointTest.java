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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointBlockType;
import io.questdb.cairo.lv.LiveViewCheckpointManifest;
import io.questdb.cairo.lv.LiveViewCheckpointReader;
import io.questdb.cairo.lv.LiveViewCheckpointWriter;
import io.questdb.cairo.lv.LiveViewRecovery;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Zip;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Round-trip tests for the live view checkpoint ({@code .cp}) file framing
 * and checkpoint manifest. These tests
 * exercise the pure format with synthetic block payloads - they do not
 * involve the live view runtime.
 */
public class LiveViewCheckpointTest extends AbstractCairoTest {

    @Test
    public void testCorruptedCrcIsDetected() throws Exception {
        assertMemoryLeak(() -> {
            final long lvSeqTxn = 99;
            try (Path liveViewDir = newLiveViewDir()) {
                try (LiveViewCheckpointWriter writer = new LiveViewCheckpointWriter(configuration)) {
                    writer.of(liveViewDir.$(), lvSeqTxn);
                    writer.writeManifestBlock(new LiveViewCheckpointManifest()
                            .setLvSeqTxn(lvSeqTxn)
                            .setLvRowPosition(0)
                            .setBaseSeqTxn(0)
                            .setMaxTimestamp(0)
                            .setKind(LiveViewCheckpointManifest.KIND_STEADY)
                            .addWindowName("w"));
                    writer.commit(Long.MIN_VALUE);
                }
                try (Path cpPath = openHeadPath(liveViewDir, lvSeqTxn)) {
                    // Flip a byte inside the manifest payload (past header).
                    overwriteByteInFile(configuration, cpPath, LiveViewCheckpointWriter.FILE_HEADER_SIZE + 8, (byte) 0xAB);
                    try (LiveViewCheckpointReader reader = new LiveViewCheckpointReader(configuration)) {
                        try {
                            reader.of(cpPath.$());
                            Assert.fail("expected CRC mismatch");
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getFlyweightMessage().toString(),
                                    e.getFlyweightMessage().toString().contains("CRC mismatch"));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testCorruptedVersionFieldIsCaughtByCrcNotVersionCheck() throws Exception {
        // A bit-rotted version field must NOT masquerade as a compatibility
        // break. The version field (offset 4) is covered by the CRC, so
        // corrupting it without fixing the trailer trips the CRC check first.
        // The reader reports plain corruption (errno != LV_CHECKPOINT_FILE_
        // VERSION_MISMATCH) so the caller unlinks the head and replays from
        // the lower bound instead of invalidating a live view over recoverable
        // bit rot. Pre-fix the version check ran before the CRC and this same
        // corruption was mis-reported as a version mismatch.
        assertMemoryLeak(() -> {
            final long lvSeqTxn = 23;
            try (Path liveViewDir = newLiveViewDir()) {
                try (LiveViewCheckpointWriter writer = new LiveViewCheckpointWriter(configuration)) {
                    writer.of(liveViewDir.$(), lvSeqTxn);
                    writer.writeManifestBlock(new LiveViewCheckpointManifest()
                            .setLvSeqTxn(lvSeqTxn)
                            .setLvRowPosition(0)
                            .setBaseSeqTxn(0)
                            .setMaxTimestamp(0)
                            .setKind(LiveViewCheckpointManifest.KIND_STEADY));
                    writer.commit(Long.MIN_VALUE);
                }
                try (Path cpPath = openHeadPath(liveViewDir, lvSeqTxn)) {
                    // Corrupt the version field but leave the stale CRC trailer,
                    // exactly what bit rot in that field looks like on disk.
                    overwriteIntInFile(configuration, cpPath, 4, LiveViewCheckpointReader.SUPPORTED_VERSION_MAX + 1);
                    try (LiveViewCheckpointReader reader = new LiveViewCheckpointReader(configuration)) {
                        try {
                            reader.of(cpPath.$());
                            Assert.fail("expected CRC mismatch");
                        } catch (CairoException e) {
                            Assert.assertNotEquals(
                                    "a bit-rotted version field must be recoverable corruption, not a compatibility break",
                                    CairoException.LV_CHECKPOINT_FILE_VERSION_MISMATCH,
                                    e.getErrno()
                            );
                            Assert.assertTrue(e.getFlyweightMessage().toString(),
                                    e.getFlyweightMessage().toString().contains("CRC mismatch"));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testMultipleBlocksRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            final long lvSeqTxn = 42;
            try (Path liveViewDir = newLiveViewDir()) {
                try (LiveViewCheckpointWriter writer = new LiveViewCheckpointWriter(configuration)) {
                    writer.of(liveViewDir.$(), lvSeqTxn);

                    writer.writeManifestBlock(new LiveViewCheckpointManifest()
                            .setLvSeqTxn(lvSeqTxn)
                            .setLvRowPosition(0)
                            .setBaseSeqTxn(0)
                            .setMaxTimestamp(0)
                            .setKind(LiveViewCheckpointManifest.KIND_STEADY)
                            .addWindowName("w"));

                    final MemoryA anchor = writer.beginBlock(LiveViewCheckpointBlockType.BLOCK_WINDOW_ANCHOR);
                    anchor.putLong(10);
                    anchor.putLong(20);
                    writer.endBlock();

                    final MemoryA snap = writer.beginBlock(LiveViewCheckpointBlockType.BLOCK_FUNCTION_SNAPSHOT);
                    snap.putInt(7);
                    snap.putInt(13);
                    writer.endBlock();

                    writer.commit(Long.MIN_VALUE);
                }

                try (Path cpPath = openHeadPath(liveViewDir, lvSeqTxn);
                     LiveViewCheckpointReader reader = new LiveViewCheckpointReader(configuration)) {
                    reader.of(cpPath.$());
                    Assert.assertEquals(3, reader.getBlockCount());

                    final LiveViewCheckpointReader.BlockCursor cursor = reader.getCursor();
                    Assert.assertTrue(cursor.hasNext());
                    LiveViewCheckpointReader.ReadableBlock block = cursor.next();
                    Assert.assertEquals(LiveViewCheckpointBlockType.BLOCK_MANIFEST, block.type());

                    Assert.assertTrue(cursor.hasNext());
                    block = cursor.next();
                    Assert.assertEquals(LiveViewCheckpointBlockType.BLOCK_WINDOW_ANCHOR, block.type());
                    Assert.assertEquals(Long.BYTES * 2L, block.size());
                    Assert.assertEquals(10L, block.getLong(0));
                    Assert.assertEquals(20L, block.getLong(Long.BYTES));

                    Assert.assertTrue(cursor.hasNext());
                    block = cursor.next();
                    Assert.assertEquals(LiveViewCheckpointBlockType.BLOCK_FUNCTION_SNAPSHOT, block.type());
                    Assert.assertEquals(Integer.BYTES * 2L, block.size());
                    Assert.assertEquals(7, block.getInt(0));
                    Assert.assertEquals(13, block.getInt(Integer.BYTES));

                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testNullWindowNameInManifestIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            final long lvSeqTxn = 7;
            try (Path liveViewDir = newLiveViewDir()) {
                try (LiveViewCheckpointWriter writer = new LiveViewCheckpointWriter(configuration)) {
                    writer.of(liveViewDir.$(), lvSeqTxn);
                    // Hand-craft a MANIFEST block carrying a null-encoded window name.
                    // writeManifestBlock cannot produce one (addWindowName is non-null),
                    // so write the block fields directly and emit a null string slot.
                    final MemoryA sink = writer.beginBlock(LiveViewCheckpointBlockType.BLOCK_MANIFEST);
                    sink.putLong(lvSeqTxn);                                // lvSeqTxn
                    sink.putLong(0);                                       // lvRowPosition
                    sink.putLong(0);                                       // baseSeqTxn
                    sink.putLong(0);                                       // maxTimestamp
                    sink.putByte(LiveViewCheckpointManifest.KIND_STEADY);  // kind
                    sink.putInt(1);                                        // windowCount
                    sink.putStr(null);                                     // null window name
                    writer.endBlock();
                    writer.commit(Long.MIN_VALUE);
                }
                // The file itself is structurally valid (magic, version, CRC all check
                // out), so of() succeeds; the corruption surfaces only when the manifest
                // parser hits the null name. It must throw, not NPE.
                try (Path cpPath = openHeadPath(liveViewDir, lvSeqTxn);
                     LiveViewCheckpointReader reader = new LiveViewCheckpointReader(configuration)) {
                    reader.of(cpPath.$());
                    try {
                        reader.readManifestInto(new LiveViewCheckpointManifest());
                        Assert.fail("expected null window name to be rejected");
                    } catch (CairoException e) {
                        Assert.assertTrue(
                                e.getFlyweightMessage().toString(),
                                e.getFlyweightMessage().toString().contains("null window name")
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testPriorHeadIsUnlinkedOnCommit() throws Exception {
        assertMemoryLeak(() -> {
            final long priorLvSeqTxn = 100;
            final long newLvSeqTxn = 200;
            try (Path liveViewDir = newLiveViewDir()) {
                try (LiveViewCheckpointWriter writer = new LiveViewCheckpointWriter(configuration)) {
                    writer.of(liveViewDir.$(), priorLvSeqTxn);
                    writer.writeManifestBlock(new LiveViewCheckpointManifest()
                            .setLvSeqTxn(priorLvSeqTxn)
                            .setLvRowPosition(0)
                            .setBaseSeqTxn(0)
                            .setMaxTimestamp(0)
                            .setKind(LiveViewCheckpointManifest.KIND_STEADY));
                    writer.commit(Long.MIN_VALUE);

                    writer.of(liveViewDir.$(), newLvSeqTxn);
                    writer.writeManifestBlock(new LiveViewCheckpointManifest()
                            .setLvSeqTxn(newLvSeqTxn)
                            .setLvRowPosition(0)
                            .setBaseSeqTxn(0)
                            .setMaxTimestamp(0)
                            .setKind(LiveViewCheckpointManifest.KIND_STEADY));
                    writer.commit(priorLvSeqTxn);
                }

                try (Path priorPath = openHeadPath(liveViewDir, priorLvSeqTxn);
                     Path newPath = openHeadPath(liveViewDir, newLvSeqTxn)) {
                    final FilesFacade ff = configuration.getFilesFacade();
                    Assert.assertFalse("prior .cp should be unlinked", ff.exists(priorPath.$()));
                    Assert.assertTrue("new .cp should exist", ff.exists(newPath.$()));
                }
            }
        });
    }

    @Test
    public void testRejectsFileWithBadMagic() throws Exception {
        assertMemoryLeak(() -> {
            final long lvSeqTxn = 7;
            try (Path liveViewDir = newLiveViewDir()) {
                try (LiveViewCheckpointWriter writer = new LiveViewCheckpointWriter(configuration)) {
                    writer.of(liveViewDir.$(), lvSeqTxn);
                    writer.writeManifestBlock(new LiveViewCheckpointManifest()
                            .setLvSeqTxn(lvSeqTxn)
                            .setLvRowPosition(0)
                            .setBaseSeqTxn(0)
                            .setMaxTimestamp(0)
                            .setKind(LiveViewCheckpointManifest.KIND_STEADY));
                    writer.commit(Long.MIN_VALUE);
                }
                try (Path cpPath = openHeadPath(liveViewDir, lvSeqTxn)) {
                    overwriteIntInFile(configuration, cpPath, 0, 0xDEAD_BEEF);
                    try (LiveViewCheckpointReader reader = new LiveViewCheckpointReader(configuration)) {
                        try {
                            reader.of(cpPath.$());
                            Assert.fail("expected magic mismatch");
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getFlyweightMessage().toString(),
                                    e.getFlyweightMessage().toString().contains("magic mismatch"));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testRejectsFileWithFormatVersionTooNew() throws Exception {
        // The file's formatVersion lives at byte offset 4 (after magic). A
        // value above SUPPORTED_VERSION_MAX means the file was written by a
        // newer server; the reader signals it with the dedicated errno so
        // the caller invalidates the LV rather than treating the file as
        // corrupt. The version check runs AFTER the CRC check, so the version
        // field is rewritten with a matching CRC to model a genuine
        // compatibility break (intact file, unsupported version). A version
        // field corrupted without fixing the CRC is instead recoverable bit
        // rot - see testCorruptedVersionFieldIsCaughtByCrcNotVersionCheck.
        assertMemoryLeak(() -> {
            final long lvSeqTxn = 17;
            try (Path liveViewDir = newLiveViewDir()) {
                try (LiveViewCheckpointWriter writer = new LiveViewCheckpointWriter(configuration)) {
                    writer.of(liveViewDir.$(), lvSeqTxn);
                    writer.writeManifestBlock(new LiveViewCheckpointManifest()
                            .setLvSeqTxn(lvSeqTxn)
                            .setLvRowPosition(0)
                            .setBaseSeqTxn(0)
                            .setMaxTimestamp(0)
                            .setKind(LiveViewCheckpointManifest.KIND_STEADY));
                    writer.commit(Long.MIN_VALUE);
                }
                try (Path cpPath = openHeadPath(liveViewDir, lvSeqTxn)) {
                    overwriteIntAndFixCrc(configuration, cpPath, 4, LiveViewCheckpointReader.SUPPORTED_VERSION_MAX + 1);
                    try (LiveViewCheckpointReader reader = new LiveViewCheckpointReader(configuration)) {
                        try {
                            reader.of(cpPath.$());
                            Assert.fail("expected format version too new");
                        } catch (CairoException e) {
                            Assert.assertEquals(
                                    "version mismatch must be tagged so the LV gets invalidated, not unlinked",
                                    CairoException.LV_CHECKPOINT_FILE_VERSION_MISMATCH,
                                    e.getErrno()
                            );
                            Assert.assertTrue(e.getFlyweightMessage().toString(),
                                    e.getFlyweightMessage().toString().contains("format version too new"));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testRejectsFileWithFormatVersionTooOld() throws Exception {
        // Symmetric case: formatVersion below SUPPORTED_VERSION_MIN signals a
        // file too old for this server to read. As above, the CRC is fixed up
        // so this models a genuine version break rather than corruption.
        assertMemoryLeak(() -> {
            final long lvSeqTxn = 19;
            try (Path liveViewDir = newLiveViewDir()) {
                try (LiveViewCheckpointWriter writer = new LiveViewCheckpointWriter(configuration)) {
                    writer.of(liveViewDir.$(), lvSeqTxn);
                    writer.writeManifestBlock(new LiveViewCheckpointManifest()
                            .setLvSeqTxn(lvSeqTxn)
                            .setLvRowPosition(0)
                            .setBaseSeqTxn(0)
                            .setMaxTimestamp(0)
                            .setKind(LiveViewCheckpointManifest.KIND_STEADY));
                    writer.commit(Long.MIN_VALUE);
                }
                try (Path cpPath = openHeadPath(liveViewDir, lvSeqTxn)) {
                    overwriteIntAndFixCrc(configuration, cpPath, 4, LiveViewCheckpointReader.SUPPORTED_VERSION_MIN - 1);
                    try (LiveViewCheckpointReader reader = new LiveViewCheckpointReader(configuration)) {
                        try {
                            reader.of(cpPath.$());
                            Assert.fail("expected format version too old");
                        } catch (CairoException e) {
                            Assert.assertEquals(
                                    "version mismatch must be tagged so the LV gets invalidated, not unlinked",
                                    CairoException.LV_CHECKPOINT_FILE_VERSION_MISMATCH,
                                    e.getErrno()
                            );
                            Assert.assertTrue(e.getFlyweightMessage().toString(),
                                    e.getFlyweightMessage().toString().contains("format version too old"));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSweepEmptyDirReturnsLongNull() throws Exception {
        assertMemoryLeak(() -> {
            try (Path liveViewDir = newLiveViewDir();
                 Path scratch = new Path()) {
                final StringSink nameSink = new StringSink();
                final long head = LiveViewRecovery.sweepCheckpoints(
                        configuration.getFilesFacade(),
                        scratch,
                        liveViewDir,
                        100L,
                        nameSink
                );
                Assert.assertEquals(Numbers.LONG_NULL, head);
            }
        });
    }

    @Test
    public void testSweepKeepsOnlyHighestWithinWatermark() throws Exception {
        assertMemoryLeak(() -> {
            try (Path liveViewDir = newLiveViewDir();
                 Path scratch = new Path()) {
                // Write three valid .cp files at lvSeqTxn 1, 2, 3.
                for (long n = 1; n <= 3; n++) {
                    writeMinimalCheckpoint(liveViewDir, n);
                }
                final StringSink nameSink = new StringSink();
                final long head = LiveViewRecovery.sweepCheckpoints(
                        configuration.getFilesFacade(),
                        scratch,
                        liveViewDir,
                        5L,
                        nameSink
                );
                Assert.assertEquals(3L, head);
                Assert.assertTrue("highest .cp survives", existsCp(liveViewDir, 3L));
                Assert.assertFalse("older .cp retired", existsCp(liveViewDir, 1L));
                Assert.assertFalse("older .cp retired", existsCp(liveViewDir, 2L));
            }
        });
    }

    @Test
    public void testSweepUnlinksCpAheadOfWatermark() throws Exception {
        assertMemoryLeak(() -> {
            try (Path liveViewDir = newLiveViewDir();
                 Path scratch = new Path()) {
                writeMinimalCheckpoint(liveViewDir, 7L);
                writeMinimalCheckpoint(liveViewDir, 15L);
                final StringSink nameSink = new StringSink();
                final long head = LiveViewRecovery.sweepCheckpoints(
                        configuration.getFilesFacade(),
                        scratch,
                        liveViewDir,
                        10L,
                        nameSink
                );
                // 15 was ahead of the watermark (lost _txn advance scenario);
                // sweep removes it. 7 survives as the head.
                Assert.assertEquals(7L, head);
                Assert.assertTrue("watermark-aligned .cp survives", existsCp(liveViewDir, 7L));
                Assert.assertFalse(".cp ahead of watermark unlinked", existsCp(liveViewDir, 15L));
            }
        });
    }

    @Test
    public void testSweepRemovesCpTmpOrphans() throws Exception {
        assertMemoryLeak(() -> {
            try (Path liveViewDir = newLiveViewDir();
                 Path scratch = new Path()) {
                // Plant a .cp.tmp orphan and a valid .cp side by side.
                touchEmptyFile(liveViewDir, "0000000000000004.cp.tmp");
                writeMinimalCheckpoint(liveViewDir, 4L);
                final StringSink nameSink = new StringSink();
                final long head = LiveViewRecovery.sweepCheckpoints(
                        configuration.getFilesFacade(),
                        scratch,
                        liveViewDir,
                        100L,
                        nameSink
                );
                Assert.assertEquals(4L, head);
                Assert.assertFalse(".cp.tmp orphan unlinked", existsRaw(liveViewDir, "0000000000000004.cp.tmp"));
                Assert.assertTrue(".cp survives", existsCp(liveViewDir, 4L));
            }
        });
    }

    @Test
    public void testWriterCommitWithBlockOpenThrows() throws Exception {
        assertMemoryLeak(() -> {
            try (Path liveViewDir = newLiveViewDir();
                 LiveViewCheckpointWriter writer = new LiveViewCheckpointWriter(configuration)) {
                writer.of(liveViewDir.$(), 1);
                writer.beginBlock(LiveViewCheckpointBlockType.BLOCK_MANIFEST);
                try {
                    writer.commit(Long.MIN_VALUE);
                    Assert.fail("expected block-in-progress error");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getFlyweightMessage().toString(),
                            e.getFlyweightMessage().toString().contains("block in progress"));
                }
            }
        });
    }

    private static void overwriteByteInFile(CairoConfiguration configuration, Path path, long offset, byte value) {
        try (MemoryCMARW mem = Vm.getCMARWInstance()) {
            mem.of(
                    configuration.getFilesFacade(),
                    path.$(),
                    configuration.getFilesFacade().getPageSize(),
                    offset + Byte.BYTES,
                    MemoryTag.MMAP_DEFAULT,
                    CairoConfiguration.O_NONE
            );
            mem.putByte(offset, value);
            mem.sync(false);
        }
    }

    private static void overwriteIntInFile(CairoConfiguration configuration, Path path, long offset, int value) {
        try (MemoryCMARW mem = Vm.getCMARWInstance()) {
            mem.of(
                    configuration.getFilesFacade(),
                    path.$(),
                    configuration.getFilesFacade().getPageSize(),
                    offset + Integer.BYTES,
                    MemoryTag.MMAP_DEFAULT,
                    CairoConfiguration.O_NONE
            );
            mem.putInt(offset, value);
            mem.sync(false);
        }
    }

    // Overwrites an int, then recomputes the CRC32 trailer over the header +
    // blocks so the file stays structurally intact. Mimics a genuine
    // version-mismatch file (valid CRC, out-of-range version) rather than bit
    // rot, which the plain overwriteIntInFile leaves behind (stale CRC). The
    // value write reuses overwriteIntInFile (which may round the sub-page .cp
    // up to a page boundary); bodyEnd is recomputed from the post-write length
    // so it matches what the reader observes, and the CRC is written back with
    // a direct pwrite so the file length is left untouched.
    private static void overwriteIntAndFixCrc(CairoConfiguration configuration, Path path, long offset, int value) {
        final FilesFacade ff = configuration.getFilesFacade();
        overwriteIntInFile(configuration, path, offset, value);
        final long fileSize = ff.length(path.$());
        final long bodyEnd = fileSize - LiveViewCheckpointWriter.FILE_TRAILER_SIZE;
        final int crc;
        try (MemoryCMR ro = Vm.getCMRInstance()) {
            ro.of(
                    ff,
                    path.$(),
                    ff.getPageSize(),
                    fileSize,
                    MemoryTag.MMAP_DEFAULT,
                    CairoConfiguration.O_NONE,
                    -1
            );
            crc = Zip.crc32(0, ro.addressOf(0), (int) bodyEnd);
        }
        final long buf = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putInt(buf, crc);
            final long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
            try {
                ff.write(fd, buf, Integer.BYTES, bodyEnd);
            } finally {
                ff.close(fd);
            }
        } finally {
            Unsafe.free(buf, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static Path openHeadPath(Path liveViewDir, long lvSeqTxn) {
        final Path path = new Path();
        path.of(liveViewDir)
                .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                .slash();
        LiveViewCheckpointWriter.appendCpFileName(path, lvSeqTxn);
        return path;
    }

    private static boolean existsCp(Path liveViewDir, long lvSeqTxn) {
        try (Path probe = openHeadPath(liveViewDir, lvSeqTxn)) {
            return configuration.getFilesFacade().exists(probe.$());
        }
    }

    private static boolean existsRaw(Path liveViewDir, CharSequence fileName) {
        try (Path probe = new Path()) {
            probe.of(liveViewDir).concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME).slash().put(fileName);
            return configuration.getFilesFacade().exists(probe.$());
        }
    }

    private static void touchEmptyFile(Path liveViewDir, CharSequence fileName) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path probe = new Path()) {
            probe.of(liveViewDir).concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME).slash().put(fileName);
            long fd = ff.openRW(probe.$(), CairoConfiguration.O_NONE);
            ff.close(fd);
        }
    }

    private static void writeMinimalCheckpoint(Path liveViewDir, long lvSeqTxn) {
        try (LiveViewCheckpointWriter w = new LiveViewCheckpointWriter(configuration)) {
            w.of(liveViewDir.$(), lvSeqTxn);
            w.writeManifestBlock(new LiveViewCheckpointManifest()
                    .setLvSeqTxn(lvSeqTxn)
                    .setLvRowPosition(0)
                    .setBaseSeqTxn(0)
                    .setMaxTimestamp(0)
                    .setKind(LiveViewCheckpointManifest.KIND_STEADY));
            // commit with no prior; the sweep is the unlink driver, not the
            // writer.
            w.commit(Long.MIN_VALUE);
        }
    }

    private Path newLiveViewDir() {
        final Path liveViewDir = new Path();
        liveViewDir.of(configuration.getDbRoot()).concat("lv_cp_test").slash();
        final FilesFacade ff = configuration.getFilesFacade();
        ff.mkdirs(liveViewDir, configuration.getMkDirMode());
        // Wipe any stale state from prior tests in the same class run.
        final Path checkpointsDir = Path.PATH.get();
        checkpointsDir.of(liveViewDir).concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME).slash();
        ff.rmdir(checkpointsDir);
        ff.mkdirs(checkpointsDir, configuration.getMkDirMode());
        return liveViewDir;
    }
}
