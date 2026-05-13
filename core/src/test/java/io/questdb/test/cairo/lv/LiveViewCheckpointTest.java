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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Round-trip tests for the live view checkpoint ({@code .cp}) file framing
 * (RFC 123 §"{@code .cp} file framing", §"Checkpoint manifest"). These tests
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

                    final MemoryA anchor = writer.beginBlock(LiveViewCheckpointBlockType.BLOCK_ANCHOR);
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
                    Assert.assertEquals(LiveViewCheckpointBlockType.BLOCK_ANCHOR, block.type());
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

    private static Path openHeadPath(Path liveViewDir, long lvSeqTxn) {
        final Path path = new Path();
        path.of(liveViewDir)
                .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                .slash();
        LiveViewCheckpointWriter.appendCpFileName(path, lvSeqTxn);
        return path;
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
