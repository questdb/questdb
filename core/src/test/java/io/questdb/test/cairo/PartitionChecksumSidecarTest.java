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
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

public class PartitionChecksumSidecarTest extends AbstractCairoTest {

    private static final int BS = 1 << 20;

    @Test
    public void testBothSlotsTornYieldsAbsentNotMismatch() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                try (PartitionChecksumSidecar s = new PartitionChecksumSidecar()) {
                    s.of(configuration.getFilesFacade(), sidecarPath(path, "both"), BS);
                    writeOneFileGeneration(s, "v.d", 100, 11L);
                }
                corruptSlot(sidecarPath(path, "both"), 0);
                corruptSlot(sidecarPath(path, "both"), 1);
                try (PartitionChecksumSidecar r = new PartitionChecksumSidecar()) {
                    r.of(configuration.getFilesFacade(), sidecarPath(path, "both"), BS);
                    Assert.assertEquals(
                            "a sidecar with no valid slot says nothing about the data",
                            ChecksumTrailer.ABSENT,
                            r.coverage()
                    );
                }
            }
        });
    }

    @Test
    public void testFreshFileHasNoCoverage() throws Exception {
        // A partition no new binary has sealed yet must read as "unverified" -- the upgrade-on-write
        // posture. Reading it as corrupt would condemn every partition that already exists.
        assertMemoryLeak(() -> {
            try (Path path = new Path(); PartitionChecksumSidecar s = new PartitionChecksumSidecar()) {
                s.of(configuration.getFilesFacade(), sidecarPath(path, "fresh"), BS);
                Assert.assertTrue(s.isOpen());
                Assert.assertEquals(ChecksumTrailer.ABSENT, s.coverage());
                Assert.assertEquals(0, s.generation());
                Assert.assertEquals(0, s.fileCount());
            }
        });
    }

    @Test
    public void testGenerationRoundTrips() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                try (PartitionChecksumSidecar s = new PartitionChecksumSidecar()) {
                    s.of(configuration.getFilesFacade(), sidecarPath(path, "rt"), BS);
                    s.beginGeneration();
                    s.putFile("v.d", 3000, 2);
                    s.putBlockHash(111L);
                    s.putBlockHash(222L);
                    s.putFile("ts.d", 8, 1);
                    s.putBlockHash(333L);
                    Assert.assertTrue(s.commitGeneration());
                }
                try (PartitionChecksumSidecar r = new PartitionChecksumSidecar()) {
                    r.of(configuration.getFilesFacade(), sidecarPath(path, "rt"), BS);
                    Assert.assertEquals(ChecksumTrailer.PRESENT_OK, r.coverage());
                    Assert.assertEquals(1L, r.generation());
                    Assert.assertEquals(2, r.fileCount());
                    final int v = r.indexOf("v.d");
                    Assert.assertTrue(v >= 0);
                    Assert.assertEquals(3000L, r.fileLength(v));
                    Assert.assertEquals(2, r.blockCount(v));
                    Assert.assertEquals(111L, r.blockHash(v, 0));
                    Assert.assertEquals(222L, r.blockHash(v, 1));
                    final int ts = r.indexOf("ts.d");
                    Assert.assertTrue(ts >= 0);
                    Assert.assertEquals(333L, r.blockHash(ts, 0));
                }
            }
        });
    }

    @Test
    public void testGenerationsAlternateSlots() throws Exception {
        // The point of A/B: publishing generation N+1 must not overwrite generation N's bytes, or a
        // torn publish leaves NEITHER readable.
        assertMemoryLeak(() -> {
            try (Path path = new Path(); PartitionChecksumSidecar s = new PartitionChecksumSidecar()) {
                s.of(configuration.getFilesFacade(), sidecarPath(path, "ab"), BS);
                for (int i = 1; i <= 4; i++) {
                    writeOneFileGeneration(s, "v.d", i * 100L, i);
                    Assert.assertEquals(i, s.generation());
                    Assert.assertEquals((long) i, s.blockHash(s.indexOf("v.d"), 0));
                }
            }
        });
    }

    @Test
    public void testImpossibleBodyLenIsRejectedWithoutReadingIt() throws Exception {
        // A corrupt bodyLen must be caught by the bounds check BEFORE it is used for address
        // arithmetic, or the trailer read walks off the mapping.
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                try (PartitionChecksumSidecar s = new PartitionChecksumSidecar()) {
                    s.of(configuration.getFilesFacade(), sidecarPath(path, "badlen"), BS);
                    writeOneFileGeneration(s, "v.d", 100, 11L);
                }
                writeLongAt(sidecarPath(path, "badlen"), PartitionChecksumSidecar.HEADER_SIZE + 8, Long.MAX_VALUE);
                try (PartitionChecksumSidecar r = new PartitionChecksumSidecar()) {
                    r.of(configuration.getFilesFacade(), sidecarPath(path, "badlen"), BS);
                    Assert.assertEquals(ChecksumTrailer.ABSENT, r.coverage());
                }
            }
        });
    }

    @Test
    public void testInvalidateDropsCoverage() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                try (PartitionChecksumSidecar s = new PartitionChecksumSidecar()) {
                    s.of(configuration.getFilesFacade(), sidecarPath(path, "inv"), BS);
                    writeOneFileGeneration(s, "v.d", 100, 11L);
                    s.invalidate();
                    Assert.assertEquals(ChecksumTrailer.ABSENT, s.coverage());
                }
                try (PartitionChecksumSidecar r = new PartitionChecksumSidecar()) {
                    r.of(configuration.getFilesFacade(), sidecarPath(path, "inv"), BS);
                    Assert.assertEquals(ChecksumTrailer.ABSENT, r.coverage());
                }
            }
        });
    }

    @Test
    public void testRecordedBlockSizeWinsOverConfigured() throws Exception {
        // Reinterpreting an existing vector at a different block size compares each block against the
        // wrong expected hash and reports corruption everywhere.
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                try (PartitionChecksumSidecar s = new PartitionChecksumSidecar()) {
                    s.of(configuration.getFilesFacade(), sidecarPath(path, "bs"), BS);
                    writeOneFileGeneration(s, "v.d", 100, 11L);
                }
                try (PartitionChecksumSidecar r = new PartitionChecksumSidecar()) {
                    r.of(configuration.getFilesFacade(), sidecarPath(path, "bs"), 1 << 22);
                    Assert.assertEquals(BS, r.blockSize());
                    Assert.assertEquals(ChecksumTrailer.PRESENT_OK, r.coverage());
                }
            }
        });
    }

    @Test
    public void testTornSlotFallsBackToPreviousGeneration() throws Exception {
        // The central safety property. A half-written generation must never be read, and must never
        // be reported as data corruption -- it is evidence about the SIDECAR, not about the data.
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                try (PartitionChecksumSidecar s = new PartitionChecksumSidecar()) {
                    s.of(configuration.getFilesFacade(), sidecarPath(path, "torn"), BS);
                    writeOneFileGeneration(s, "v.d", 100, 11L); // gen 1, slot A
                    writeOneFileGeneration(s, "v.d", 200, 22L); // gen 2, slot B
                }
                corruptSlot(sidecarPath(path, "torn"), 1);
                try (PartitionChecksumSidecar r = new PartitionChecksumSidecar()) {
                    r.of(configuration.getFilesFacade(), sidecarPath(path, "torn"), BS);
                    Assert.assertEquals(ChecksumTrailer.PRESENT_OK, r.coverage());
                    Assert.assertEquals("must fall back to the intact older generation", 1L, r.generation());
                    Assert.assertEquals(11L, r.blockHash(r.indexOf("v.d"), 0));
                }
            }
        });
    }

    @Test
    public void testUnopenableSidecarDegradesRatherThanThrows() throws Exception {
        // ENOSPC / EMFILE / read-only mount. This file carries no durability claim, so failing to
        // open it must cost DETECTION, never ingestion.
        assertMemoryLeak(() -> {
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long openRW(LPSZ name, int opts) {
                    return -1;
                }
            };
            try (Path path = new Path(); PartitionChecksumSidecar s = new PartitionChecksumSidecar()) {
                s.of(ff, sidecarPath(path, "noopen"), BS);
                Assert.assertFalse(s.isOpen());
                Assert.assertEquals(ChecksumTrailer.ABSENT, s.coverage());
                Assert.assertFalse("a closed sidecar must publish nothing", s.commitGeneration());
            }
        });
    }

    /** Flips one byte inside the given slot's body. Must not touch the other slot. */
    private void corruptSlot(Path path, int slot) {
        final long slotSize = readIntAt(path, 16);
        final long offset = PartitionChecksumSidecar.HEADER_SIZE + slot * slotSize
                + PartitionChecksumSidecar.SLOT_HEADER_SIZE + 4;
        final long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        final FilesFacade ff = configuration.getFilesFacade();
        final long fd = ff.openRW(path.$(), 0);
        try {
            ff.read(fd, buf, 1, offset);
            Unsafe.getUnsafe().putByte(buf, (byte) (Unsafe.getUnsafe().getByte(buf) ^ 0xFF));
            ff.write(fd, buf, 1, offset);
        } finally {
            ff.close(fd);
            Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private int readIntAt(Path path, long offset) {
        final long buf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        final FilesFacade ff = configuration.getFilesFacade();
        final long fd = ff.openRO(path.$());
        try {
            ff.read(fd, buf, 4, offset);
            return Unsafe.getUnsafe().getInt(buf);
        } finally {
            ff.close(fd);
            Unsafe.free(buf, 4, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private Path sidecarPath(Path path, String name) {
        return path.of(configuration.getDbRoot()).concat("pchk_" + name);
    }

    private void writeLongAt(Path path, long offset, long value) {
        final long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        final FilesFacade ff = configuration.getFilesFacade();
        final long fd = ff.openRW(path.$(), 0);
        try {
            Unsafe.getUnsafe().putLong(buf, value);
            ff.write(fd, buf, 8, offset);
        } finally {
            ff.close(fd);
            Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void writeOneFileGeneration(PartitionChecksumSidecar s, String name, long length, long hash) {
        s.beginGeneration();
        s.putFile(name, length, 1);
        s.putBlockHash(hash);
        Assert.assertTrue(s.commitGeneration());
    }
}
