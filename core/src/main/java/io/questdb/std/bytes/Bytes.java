package io.questdb.std.bytes;

public class Bytes {

    private Bytes() {
    }

    /**
     * Aligns the given pointer to 2 bytes.
     */
    public static long align2b(long ptr) {
        return (ptr + 1) & ~0x1;
    }

    /**
     * Aligns the given pointer to 4 bytes.
     */
    public static long align4b(long ptr) {
        return (ptr + 3) & ~0x3;
    }

    /**
     * Aligns the given pointer to 8 bytes.
     */
    public static long align8b(long ptr) {
        return (ptr + 7) & ~0x7;
    }

    /**
     * Compute the size of a range, checking for overflow.
     */
    public static int checkedLoHiSize(long lo, long hi, int baseSize) {
        final long additional = hi - lo;
        if (additional < 0) {
            throw new IllegalArgumentException("lo > hi");
        }
        final long size = baseSize + additional;

        if (size > (long) Integer.MAX_VALUE) {
            throw new IllegalArgumentException("size exceeds 2GiB limit");
        }
        return (int) additional;
    }
}
