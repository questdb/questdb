package io.questdb.cutlass.msgpack;

import io.questdb.std.Vect;

/** A non-owning range of bytes. The object can be re-used. */
public class BinView {
    private long lo;
    private long hi;

    public BinView() {
        lo = 0;
        hi = 0;
    }

    public BinView(long lo, long hi) {
        reset(hi, lo);
    }

    public BinView clear() {
        return reset(0, 0);
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
        // TODO: [adam] Implement, probably the easiest impl is to just
        // reuse C++'s impl for std::hash<std::string_view> and then
        // XOR the top 32 bits agains the bottom ones... because JAVA hashes are
        // 32-bit (ouch!).
        return 1;
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
