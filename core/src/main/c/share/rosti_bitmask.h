//
// Created by blues on 26/05/2020.
//

#ifndef ROSTI_BITMASK_H
#define ROSTI_BITMASK_H

#include "vect.h"

// An abstraction over a bitmask. It provides an easy way to iterate through the
// indexes of the set bits of a bitmask.  When Shift=0 (platforms with SSE),
// this is a true bitmask.  On non-SSE, platforms the arithematic used to
// emulate the SSE behavior works in bytes (Shift=3) and leaves each bytes as
// either 0x00 or 0x80.
//
// For example:
//   for (int i : BitMask<uint32_t, 16>(0x5)) -> yields 0, 2
//   for (int i : BitMask<uint64_t, 8, 3>(0x0000000080800000)) -> yields 2, 3
template<class T>
class BitMask {

public:
    // These are useful for unit tests (gunit).
    using value_type = int;
    using iterator = BitMask;
    using const_iterator = BitMask;

    explicit BitMask(T mask) : mask_(mask) {}

    inline BitMask &operator++() {
        mask_ &= (mask_ - 1);
        return *this;
    }

    explicit operator bool() const { return mask_ != 0; }

    int operator*() const { return TrailingZeros(); }

    BitMask begin() const { return *this; }

    BitMask end() const { return BitMask(0); }

    int TrailingZeros() const {
        return __builtin_ctzll(mask_);
    }
private:
    friend bool operator==(const BitMask &a, const BitMask &b) {
        return a.mask_ == b.mask_;
    }

    friend bool operator!=(const BitMask &a, const BitMask &b) {
        return a.mask_ != b.mask_;
    }

    T mask_;
};

#endif //ROSTI_BITMASK_H
