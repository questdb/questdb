package io.questdb.std.bytes;

public class Bytes {
    private Bytes() {
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
