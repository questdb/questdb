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
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class PartitionChecksumHashingTest extends AbstractCairoTest {

    /** A small block size keeps the fixtures cheap without changing any of the arithmetic. */
    private static final int BS = 4096;

    @FunctionalInterface
    private interface FixtureBody {
        void run(FilesFacade ff, Path dataPath, PartitionChecksumSidecar sidecar);
    }

    @Test
    public void testBlockCountCoversThePartialTail() {
        Assert.assertEquals(0, PartitionChecksumSidecar.blockCountFor(0, BS));
        Assert.assertEquals(1, PartitionChecksumSidecar.blockCountFor(1, BS));
        Assert.assertEquals(1, PartitionChecksumSidecar.blockCountFor(BS, BS));
        Assert.assertEquals(2, PartitionChecksumSidecar.blockCountFor(BS + 1, BS));
    }

    @Test
    public void testGrowthPastTheStoredLengthIsNotCorruption() throws Exception {
        // The partition has been appended to since the last generation. The uncovered tail is exactly
        // the "newest blocks unverified" state the ordering rules produce on purpose.
        assertMemoryLeak(() -> withFixture("grown", 2 * BS, (ff, dataPath, sidecar) -> {
            appendBytes(ff, dataPath, BS);
            Assert.assertEquals(
                    ChecksumTrailer.PRESENT_OK,
                    sidecar.verifyFile(ff, dataPath.$(), sidecar.indexOf("v.d"))
            );
        }));
    }

    @Test
    public void testTheBlockContainingTheOldEndIsDirty() {
        // The old last block was PARTIAL. Appending fills it, so its hash changes -- skipping it would
        // leave a stale hash and report corruption on correctly written data.
        Assert.assertEquals(0, PartitionChecksumSidecar.firstDirtyBlock(0, BS));
        Assert.assertEquals(0, PartitionChecksumSidecar.firstDirtyBlock(BS - 1, BS));
        Assert.assertEquals(1, PartitionChecksumSidecar.firstDirtyBlock(BS, BS));
        Assert.assertEquals(1, PartitionChecksumSidecar.firstDirtyBlock(BS + 7, BS));
    }

    @Test
    public void testUncoveredFileVerifiesAbsent() throws Exception {
        assertMemoryLeak(() -> withFixture("absent", 2 * BS, (ff, dataPath, sidecar) ->
                Assert.assertEquals(
                        ChecksumTrailer.ABSENT,
                        sidecar.verifyFile(ff, dataPath.$(), sidecar.indexOf("nothere.d"))
                )));
    }

    @Test
    public void testVerifyDetectsASingleFlippedBitAndLocalisesIt() throws Exception {
        assertMemoryLeak(() -> withFixture("flip", 3 * BS + 17, (ff, dataPath, sidecar) -> {
            flipByteAt(ff, dataPath, 2 * BS + 5);
            Assert.assertEquals(
                    ChecksumTrailer.MISMATCH,
                    sidecar.verifyFile(ff, dataPath.$(), sidecar.indexOf("v.d"))
            );
            Assert.assertEquals("a vector localises the fault to one block", 2, sidecar.lastMismatchBlock());
        }));
    }

    @Test
    public void testVerifyDetectsTruncation() throws Exception {
        assertMemoryLeak(() -> withFixture("trunc", 3 * BS + 17, (ff, dataPath, sidecar) -> {
            truncateTo(ff, dataPath, 2 * BS);
            Assert.assertEquals(
                    ChecksumTrailer.MISMATCH,
                    sidecar.verifyFile(ff, dataPath.$(), sidecar.indexOf("v.d"))
            );
        }));
    }

    @Test
    public void testVerifyPassesOnUnmodifiedFile() throws Exception {
        assertMemoryLeak(() -> withFixture("ok", 3 * BS + 17, (ff, dataPath, sidecar) ->
                Assert.assertEquals(
                        ChecksumTrailer.PRESENT_OK,
                        sidecar.verifyFile(ff, dataPath.$(), sidecar.indexOf("v.d"))
                )));
    }

    private void appendBytes(FilesFacade ff, Path dataPath, int count) {
        final long fd = ff.openRW(dataPath.$(), 0);
        final long buf = Unsafe.malloc(count, MemoryTag.NATIVE_DEFAULT);
        try {
            final long at = ff.length(dataPath.$());
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) 0x5A);
            }
            ff.write(fd, buf, count, at);
        } finally {
            ff.close(fd);
            Unsafe.free(buf, count, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void createDataFile(FilesFacade ff, Path dataPath, long length) {
        final long fd = ff.openRW(dataPath.$(), 0);
        final long buf = Unsafe.malloc(length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (long i = 0; i < length; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) (i * 31 + 7));
            }
            ff.write(fd, buf, length, 0);
        } finally {
            ff.close(fd);
            Unsafe.free(buf, length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void flipByteAt(FilesFacade ff, Path dataPath, long offset) {
        final long fd = ff.openRW(dataPath.$(), 0);
        final long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        try {
            ff.read(fd, buf, 1, offset);
            Unsafe.getUnsafe().putByte(buf, (byte) (Unsafe.getUnsafe().getByte(buf) ^ 0xFF));
            ff.write(fd, buf, 1, offset);
        } finally {
            ff.close(fd);
            Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void truncateTo(FilesFacade ff, Path dataPath, long length) {
        final long fd = ff.openRW(dataPath.$(), 0);
        try {
            Assert.assertTrue(ff.truncate(fd, length));
        } finally {
            ff.close(fd);
        }
    }

    /**
     * Builds a data file of {@code length} deterministic bytes, hashes every block of it into one
     * generation of a sidecar beside it, then runs {@code body}.
     */
    private void withFixture(String name, long length, FixtureBody body) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path dataPath = new Path(); Path chkPath = new Path();
             PartitionChecksumSidecar sidecar = new PartitionChecksumSidecar()) {
            dataPath.of(configuration.getDbRoot()).concat("pchkdata_" + name + ".d");
            chkPath.of(configuration.getDbRoot()).concat("pchkhash_" + name);
            createDataFile(ff, dataPath, length);
            sidecar.of(ff, chkPath, BS);

            final int blocks = PartitionChecksumSidecar.blockCountFor(length, BS);
            Assert.assertTrue("the fixture must actually have blocks to hash", blocks > 0);
            final long fd = ff.openRO(dataPath.$());
            final long addr = ff.mmap(fd, length, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
            try {
                sidecar.beginGeneration();
                sidecar.putFile("v.d", length, blocks);
                for (int b = 0; b < blocks; b++) {
                    sidecar.putBlockHash(PartitionChecksumSidecar.hashBlock(addr, length, b, BS));
                }
                Assert.assertTrue(sidecar.commitGeneration());
            } finally {
                ff.munmap(addr, length, MemoryTag.MMAP_DEFAULT);
                ff.close(fd);
            }
            Assert.assertEquals(ChecksumTrailer.PRESENT_OK, sidecar.coverage());

            body.run(ff, dataPath, sidecar);
        }
    }
}
