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
#include "rosti_bitmask.h"

#if (defined(__GNUC__) && !defined(__clang__))
#define ATTRIBUTE_NEVER_INLINE __attribute__((noinline))
#elif defined(_MSC_VER)
#define ATTRIBUTE_NEVER_INLINE __declspec(noinline)
#else
#define ATTRIBUTE_NEVER_INLINE
#endif

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

struct GroupSse2Impl {

    explicit GroupSse2Impl(const ctrl_t *pos) {
        ctrl.load(pos);
    }

    // Returns a bitmask representing the positions of slots that match hash.
    inline BitMask<uint32_t> Match(h2_t hash) const {
        return BitMask<uint32_t>(vcl::to_bits(vcl::Vec16c(hash) == ctrl));
    }

    // Returns a bitmask representing the positions of empty slots.
    inline BitMask<uint32_t> MatchEmpty() const {
        return Match(kEmpty);
    }

    // Returns a bitmask representing the positions of empty or deleted slots.
    BitMask<uint32_t> MatchEmptyOrDeleted() const {
        return BitMask<uint32_t>(vcl::to_bits(vcl::Vec16c(kSentinel) > ctrl));
    }

    vcl::Vec16c ctrl;
};

using Group = GroupSse2Impl;

//-----------------------------------------

rosti_t *alloc_rosti(const int32_t *column_types, int32_t column_count, uint64_t map_capacity);

static void initialize_slots(rosti_t *map);

void clear(rosti_t *map);

static inline int32_t ceil_pow_2(int32_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1;
}

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
    return reinterpret_cast<uintptr_t>(ctrl) >> 12;
}

inline uint64_t H1(uint64_t hash, const ctrl_t *ctrl) {
    return (hash >> 7) ^ HashSeed(ctrl);
}

inline ctrl_t H2(uint64_t hash) { return hash & 0x7F; }

template<uint64_t Width>
class probe_seq {
public:
    probe_seq(uint64_t hash, uint64_t mask) {
        offset_ = hash & mask;
        mask_ = mask;
    }


    inline uint64_t offset() const { return offset_; }

    inline uint64_t offset(uint64_t i) const { return (offset_ + i) & mask_; }

    void next() {
        index_ += Width;
        offset_ += index_;
        offset_ &= mask_;
    }

    uint64_t mask() {
        return mask_;
    }

    // 0-based probe index. The i-th probe in the probe sequence.
    uint64_t index() const { return index_; }

private:
    uint64_t mask_;
    uint64_t offset_;
    uint64_t index_ = 0;
};

inline probe_seq<sizeof(Group)> probe(const rosti_t *map, uint64_t hash) {
    return probe_seq<sizeof(Group)>(H1(hash, map->ctrl_), map->capacity_);
}

inline uint64_t hash(int32_t v) {
    uint64_t h = v;
    h = (h << 5) - h + ((unsigned char) (v >> 8));
    h = (h << 5) - h + ((unsigned char) (v >> 16));
    h = (h << 5) - h + ((unsigned char) (v >> 24));
    return h;
}

uint64_t prepare_insert(rosti_t *map, uint64_t hash);

#define PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define PREDICT_TRUE(x) (__builtin_expect(false || (x), true))

inline std::pair<uint64_t, bool> find_or_prepare_insert(rosti_t *map, const int32_t key) {
    auto hh = hash(key);
    auto seq = probe(map, hh);
    while (true) {
        Group g{map->ctrl_ + seq.offset()};
        for (int i : g.Match(H2(hh))) {
            const uint64_t offset = seq.offset(i) << map->slot_size_shift_;
            int32_t p = *reinterpret_cast<int32_t *>(map->slots_ + offset);
            if (PREDICT_TRUE(p == key)) {
                return {offset, false};
            }
        }
        if (PREDICT_TRUE(g.MatchEmpty())) {
            break;
        }
        seq.next();
    }
    return {prepare_insert(map, hh), true};
}

#endif //ROSTI_H
