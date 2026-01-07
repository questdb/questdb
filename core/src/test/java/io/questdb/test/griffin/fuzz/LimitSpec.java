package io.questdb.test.griffin.fuzz;

public final class LimitSpec {
    public final long lo;
    public final long hi;
    public final boolean hasHi;

    public LimitSpec(long lo, long hi, boolean hasHi) {
        this.lo = lo;
        this.hi = hi;
        this.hasHi = hasHi;
    }
}
