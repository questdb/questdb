package io.questdb.test.cairo;

import io.questdb.cairo.ChecksumTrailer;
import io.questdb.cairo.TableUtils;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class ChecksumTrailerTest extends AbstractCairoTest {

    private static final long MAGIC = 0x4D534B4843564320L; // reuse the _cv magic shape

    @Test
    public void testAbsentBeyondWatermarkIsTorn() {
        // The whole point of the capability: past the watermark a trailer was GUARANTEED written,
        // so its absence is a torn write, not a legacy record.
        boolean covered = ChecksumTrailer.isCovered(MAGIC, MAGIC, 10L, 10L);
        Assert.assertTrue(covered);
        Assert.assertEquals(
                ChecksumTrailer.MISMATCH,
                ChecksumTrailer.applyCapability(ChecksumTrailer.ABSENT, covered)
        );
    }

    @Test
    public void testAbsentBeforeWatermarkStaysAbsent() {
        boolean covered = ChecksumTrailer.isCovered(MAGIC, MAGIC, 10L, 9L);
        Assert.assertFalse(covered);
        Assert.assertEquals(
                ChecksumTrailer.ABSENT,
                ChecksumTrailer.applyCapability(ChecksumTrailer.ABSENT, covered)
        );
    }

    @Test
    public void testNoCapabilityMagicMeansNothingIsCovered() {
        // A file written before the capability existed must never be judged as "should have had one".
        Assert.assertFalse(ChecksumTrailer.isCovered(0L, MAGIC, 0L, 1_000_000L));
    }

    @Test
    public void testWrongMagicClassifiesAbsentNotMismatch() {
        long size = 64;
        long addr = Unsafe.malloc(size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(addr, size, (byte) 7);
            long good = TableUtils.calculateCvAreaChecksum(addr, size);
            // Garbage where the magic should be = legacy trailing bytes, NOT corruption.
            Assert.assertEquals(
                    ChecksumTrailer.ABSENT,
                    ChecksumTrailer.classify(0xDEADBEEFL, good, addr, size, MAGIC)
            );
        } finally {
            Unsafe.free(addr, size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMatchingChecksumIsPresentOk() {
        long size = 64;
        long addr = Unsafe.malloc(size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(addr, size, (byte) 3);
            long good = TableUtils.calculateCvAreaChecksum(addr, size);
            Assert.assertEquals(
                    ChecksumTrailer.PRESENT_OK,
                    ChecksumTrailer.classify(MAGIC, good, addr, size, MAGIC)
            );
        } finally {
            Unsafe.free(addr, size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testFlippedByteIsMismatch() {
        long size = 64;
        long addr = Unsafe.malloc(size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(addr, size, (byte) 3);
            long good = TableUtils.calculateCvAreaChecksum(addr, size);
            Unsafe.getUnsafe().putByte(addr + 17, (byte) 4); // one bit of rot
            Assert.assertEquals(
                    ChecksumTrailer.MISMATCH,
                    ChecksumTrailer.classify(MAGIC, good, addr, size, MAGIC)
            );
        } finally {
            Unsafe.free(addr, size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        }
    }
}
