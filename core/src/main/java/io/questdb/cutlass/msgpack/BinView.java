package io.questdb.cutlass.msgpack;

import io.questdb.std.Mutable;
import io.questdb.std.Vect;

/** A non-owning range of bytes. The object can be re-used. */
public class BinView implements Mutable {
    private long lo;
    private long hi;

    public BinView() {
        lo = 0;
        hi = 0;
    }

    public BinView(long lo, long hi) {
        reset(hi, lo);
    }

    @Override
    public void clear() {
        reset(0, 0);
    }

    public BinView reset(long lo, long hi) {
        assert lo <= hi : "Ensure lo <= hi.";
        assert (lo != 0) || (hi == 0) : "If setting to 0, both must be 0";
        this.lo = lo;
        this.hi = hi;
        return this;
    }

    public long getLo() {
        return lo;
    }

    public long getHi() {
        return hi;
    }

    public long getSize() {
        return hi - lo;
    }

    public boolean isUnset() {
        return lo == 0;
    }

    private static native int blobHashCode(long lo, long hi);

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if ((other == null) || !(other instanceof BinView)) {
            return false;
        }
        BinView rhs = (BinView)other;
        long size = getSize();
        if (size != rhs.getSize()) {
            return false;
        }
        return Vect.memcmp(lo, rhs.getLo(), size) == 0;
    }

    @Override
    public int hashCode() {
        if (lo == 0) {
            assert hi == 0: "Both must be unset";
            return 0;
        }
        return blobHashCode(lo, hi);
    }

    @Override
    public String toString() {
        if (isUnset()) {
            return "BinView()";
        } else {
            return String.format("BinView(0x%x, 0x%x, size=%d)",
                lo, hi, getSize());
        }
    }

}
