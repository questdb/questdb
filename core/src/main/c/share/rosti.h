/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

#ifndef ROSTI_H
#define ROSTI_H

#include <utility>
#include <cstring>
#include <cstdint>
#include "jni.h"
#include "util.h"

#include "vcl/vectorclass.h"

using ctrl_t = signed char;
using h2_t = uint8_t;

enum Ctrl : ctrl_t {
    kEmpty = -128,   // 0b10000000
    kDeleted = -2,   // 0b11111110
    kSentinel = -1,  // 0b11111111
};

static_assert(
        kEmpty & kDeleted & kSentinel & 0x80,
        "Special markers need to have the MSB to make checking for them efficient");
static_assert(kEmpty < kSentinel && kDeleted < kSentinel,
              "kEmpty and kDeleted must be smaller than kSentinel to make the "
              "SIMD test of IsEmptyOrDeleted() efficient");
static_assert(kSentinel == -1,
              "kSentinel must be -1 to elide loading it from memory into SIMD "
              "registers (pcmpeqd xmm, xmm)");
static_assert(kEmpty == -128,
              "kEmpty must be -128 to make the SIMD check for its "
              "existence efficient (psignb xmm, xmm)");
static_assert(~kEmpty & ~kDeleted & kSentinel & 0x7F,
              "kEmpty and kDeleted must share an unset bit that is not shared "
              "by kSentinel to make the scalar test for MatchEmptyOrDeleted() "
              "efficient");
static_assert(kDeleted == -2,
              "kDeleted must be -2 to make the implementation of "
              "ConvertSpecialToEmptyAndFullToDeleted efficient");

struct rosti_t {
    ctrl_t *ctrl_ = nullptr;      // [(capacity + 1) * ctrl_t]+
    unsigned char *slots_ = nullptr;   // [capacity * types]
    uint64_t size_ = 0;                  // number of full slots
    uint64_t capacity_ = 0;              // total number of slots
    uint64_t slot_size_ = 0;             // size of key in each slot
    uint64_t slot_size_shift_ = 0;
    uint64_t growth_left_ = 0;
    int32_t *value_offsets_ = nullptr;
    unsigned char *slot_initial_values_ = nullptr; // contains pointer to memory arena
};

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
    explicit BitMask(T mask) : mask_(mask) {}

    inline BitMask &operator++() {
        mask_ &= (mask_ - 1);
        return *this;
    }

    explicit operator bool() const { return mask_ != 0; }

    int operator*() const { return TrailingZeros(); }

    [[nodiscard]] BitMask begin() const { return *this; }

    [[nodiscard]] BitMask end() const { return BitMask(0); }

    [[nodiscard]] uint32_t TrailingZeros() const {
        return bit_scan_forward(mask_);
    }

private:

    friend bool operator!=(const BitMask &a, const BitMask &b) {
        return a.mask_ != b.mask_;
    }

    T mask_;
};

struct GroupSse2Impl {

    explicit GroupSse2Impl(const ctrl_t *pos) {
        ctrl.load(pos);
    }

    // Returns a bitmask representing the positions of slots that match hash.
    [[nodiscard]] inline BitMask<uint32_t> Match(h2_t hash) const {
        return BitMask<uint32_t>(to_bits(Vec16c(hash) == ctrl));
    }

    // Returns a bitmask representing the positions of empty slots.
    [[nodiscard]] inline BitMask<uint32_t> MatchEmpty() const {
        return Match(kEmpty);
    }

    // Returns a bitmask representing the positions of empty or deleted slots.
    [[nodiscard]] BitMask<uint32_t> MatchEmptyOrDeleted() const {
        return BitMask<uint32_t>(to_bits(Vec16c(kSentinel) > ctrl));
    }

    Vec16c ctrl;
};

using Group = GroupSse2Impl;

//-----------------------------------------

rosti_t *alloc_rosti(const int32_t *column_types, int32_t column_count, uint64_t map_capacity);

static void initialize_slots(rosti_t *map);

// We use 7/8th as maximum load factor.
// For 16-wide groups, that gives an average of two empty slots per group.
inline uint64_t CapacityToGrowth(uint64_t capacity) {
    return capacity - capacity / 8;
}

inline void reset_growth_left(rosti_t *map) {
    map->growth_left_ = CapacityToGrowth(map->capacity_) - map->size_;
}

// Returns a hash seed.
//
// The seed consists of the ctrl_ pointer, which adds enough entropy to ensure
// non-determinism of iteration order in most cases.
inline uint64_t HashSeed(const ctrl_t *ctrl) {
    // The low bits of the pointer have little or no entropy because of
    // alignment. We shift the pointer to try to use higher entropy bits. A
    // good number seems to be 12 bits, because that aligns with page size.
    return reinterpret_cast<uintptr_t>(ctrl) >> 12u;
}

inline uint64_t H1(uint64_t hash, const ctrl_t *ctrl) {
    return (hash >> 7u) ^ HashSeed(ctrl);
}

inline ctrl_t H2(uint64_t hash) { return hash & 0x7Fu; }

template<uint64_t Width>
class probe_seq {
public:
    probe_seq(uint64_t hash, uint64_t mask) {
        offset_ = hash & mask;
        mask_ = mask;
    }


    [[nodiscard]] inline uint64_t offset() const { return offset_; }

    [[nodiscard]] inline uint64_t offset(uint64_t i) const { return (offset_ + i) & mask_; }

    void next() {
        index_ += Width;
        offset_ += index_;
        offset_ &= mask_;
    }

    uint64_t mask() {
        return mask_;
    }

    // 0-based probe index. The i-th probe in the probe sequence.
    [[nodiscard]] uint64_t index() const { return index_; }

private:
    uint64_t mask_;
    uint64_t offset_;
    uint64_t index_ = 0;
};

inline probe_seq<sizeof(Group)> probe(const rosti_t *map, uint64_t hash) {
    return probe_seq<sizeof(Group)>(H1(hash, map->ctrl_), map->capacity_);
}


// Reset all ctrl bytes back to kEmpty, except the sentinel.
inline void reset_ctrl(rosti_t *map) {
    uint64_t l = (map->capacity_ + 1) * sizeof(Group);
    memset(map->ctrl_, kEmpty, l);
    map->ctrl_[map->capacity_] = kSentinel;
}

void initialize_slots(rosti_t *map) {
    const uint64_t ctrl_capacity = 2 * sizeof(Group) * (map->capacity_ + 1);
    auto *mem = reinterpret_cast<unsigned char *>(malloc(
            map->slot_size_ +
            ctrl_capacity +
            map->slot_size_ * (map->capacity_ + 1)));
    map->ctrl_ = reinterpret_cast<ctrl_t *>(mem) + map->slot_size_;
    map->slots_ = mem + ctrl_capacity;
    map->slot_initial_values_ = mem;
    reset_ctrl(map);
    reset_growth_left(map);
}

inline void clear(rosti_t *map) {
    map->size_ = 0;
    reset_ctrl(map);
    reset_growth_left(map);
    memset(map->slots_, 0, map->capacity_ << map->slot_size_shift_);
}

inline bool IsEmpty(ctrl_t c) { return c == kEmpty; }

inline bool IsFull(ctrl_t c) { return c >= 0; }

inline bool IsDeleted(ctrl_t c) { return c == kDeleted; }

inline bool IsEmptyOrDeleted(ctrl_t c) { return c < kSentinel; }

inline void set_ctrl(rosti_t *map, uint64_t i, ctrl_t h) {
    constexpr uint32_t group_size = sizeof(Group);
    const int32_t p = ((i - group_size) & map->capacity_) + 1 + ((group_size - 1) & map->capacity_);
    map->ctrl_[i] = h;
    map->ctrl_[p] = h;
}


// Probes the raw_hash_set with the probe sequence for hash and returns the
// pointer to the first empty or deleted slot.
// NOTE: this function must work with tables having both kEmpty and kDelete
// in one group. Such tables appears during drop_deletes_without_resize.
//
// This function is very useful when insertions happen and:
// - the input is already a set
// - there are enough slots
// - the element with the hash is not in the table
struct FindInfo {
    uint64_t offset;
    uint64_t probe_length;
};

inline FindInfo find_first_non_full(rosti_t *map, uint64_t hash) {
    auto seq = probe(map, hash);
    while (true) {
        Group g{map->ctrl_ + seq.offset()};
        auto mask = g.MatchEmptyOrDeleted();
        if (mask) {
            return {seq.offset(mask.TrailingZeros()), seq.index()};
        }
        seq.next();
    }
}

template<typename HASH_M, typename CPY>
void resize(rosti_t *map, uint64_t new_capacity, HASH_M hash_m, CPY cpy) {
    auto *old_init = map->slot_initial_values_;
    auto *old_ctrl = map->ctrl_;
    auto *old_slots = map->slots_;
    const uint64_t old_capacity = map->capacity_;
    map->capacity_ = new_capacity;
    initialize_slots(map);

    uint64_t total_probe_length = 0;
    for (uint64_t i = 0; i != old_capacity; ++i) {
        if (IsFull(old_ctrl[i])) {
            auto p = old_slots + (i << map->slot_size_shift_);
            const uint64_t hash = hash_m(p);
            auto target = find_first_non_full(map, hash);
            uint64_t new_i = target.offset;
            total_probe_length += target.probe_length;
            set_ctrl(map, new_i, H2(hash));
            cpy(map->slots_ + (new_i << map->slot_size_shift_), p, map->slot_size_);
//            *(reinterpret_cast<int32_t *>(map->slots_ + (new_i << map->slot_size_shift_))) = *p;
        }
    }
    if (old_capacity) {
        free(old_init);
    }
}

template<typename HASH_M_T, typename CPY_T>
ATTRIBUTE_NEVER_INLINE uint64_t prepare_insert(rosti_t *map, uint64_t hash, HASH_M_T hash_f, CPY_T cpy_f) {
    auto target = find_first_non_full(map, hash);
    if (PREDICT_FALSE(map->growth_left_ == 0 && !IsDeleted(map->ctrl_[target.offset]))) {
        resize(map, map->capacity_ * 2 + 1, hash_f, cpy_f);
        target = find_first_non_full(map, hash);
    }
    ++map->size_;
    map->growth_left_ -= IsEmpty(map->ctrl_[target.offset]);
    set_ctrl(map, target.offset, H2(hash));

    // initialize slot
    const uint64_t offset = target.offset << map->slot_size_shift_;
    memcpy(map->slots_ + offset, map->slot_initial_values_, map->slot_size_);
    return offset;
}

template<typename T, typename HASH_T, typename EQ_T, typename HAS_M_T, typename CPY_T>
inline std::pair<uint64_t, bool> find_or_prepare_insert(
        rosti_t *map, const T key, HASH_T hash_f, EQ_T eq_f,
        HAS_M_T hash_m_f, CPY_T cpy_f
) {
    auto hash = hash_f(key);
    auto seq = probe(map, hash);
    while (true) {
        Group g{map->ctrl_ + seq.offset()};
        for (int i : g.Match(H2(hash))) {
            const uint64_t offset = seq.offset(i) << map->slot_size_shift_;
            if (PREDICT_TRUE(eq_f(map->slots_ + offset, key))) {
                return {offset, false};
            }
        }
        if (PREDICT_TRUE(g.MatchEmpty())) {
            break;
        }
        seq.next();
    }
    return {prepare_insert(map, hash, hash_m_f, cpy_f), true};
}

inline uint64_t hashInt(uint32_t v) {
    uint64_t h = v;
    h = (h << 5u) - h + ((unsigned char) (v >> 8u));
    h = (h << 5u) - h + ((unsigned char) (v >> 16u));
    h = (h << 5u) - h + ((unsigned char) (v >> 24u));
    return h;
}

// int equivalence
inline bool eqInt(void *p, int32_t key) {
    return *reinterpret_cast<int32_t *>(p) == key;
}

// int pointer hash
inline uint64_t hashIntMem(void *p) {
    return hashInt(*reinterpret_cast<int32_t *>(p));
}

// generic slot copy
inline void cpySlot(void *to, void *from, uint64_t size) {
    memcpy(to, from, size);
}

// int key lookup
inline std::pair<uint64_t, bool> find(rosti_t *map, const int32_t key) {
    return find_or_prepare_insert<int32_t>(map, key, hashInt, eqInt, hashIntMem, cpySlot);
}

#endif //ROSTI_H
