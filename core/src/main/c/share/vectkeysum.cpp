#include <cstdlib>
#include <utility>
#include <cstring>
#include "vect.h"
#include "vectkeysum_vanilla.h"

static inline void printVec8i(const char *name, const Vec8i &vec) {
    for (int j = 0; j < 8; j++) {
        printf("%s[%d]=%d\n", name, j, vec[j]);
    }
}

static inline void printVec4i(const char *name, const Vec4i &vec) {
    printf("%s=[%d,%d,%d,%d]\n", name, vec[0], vec[1], vec[2], vec[3]);
}

#if INSTRSET >= 10

#define MATCH_GROUPS F_AVX512(matchGroup)


#elif INSTRSET >= 8

#define MATCH_GROUPS F_AVX2(matchGroup)

#elif INSTRSET >= 5

#define MATCH_GROUPS F_SSE41(matchGroup)

#elif INSTRSET >= 2

#define MATCH_GROUPS F_SSE2(matchGroup)

#else

#endif

static inline int32_t ceil_pow_2(int32_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1;
}

static void printBits(unsigned char c) {
    char buffer[33];
    itoa(c, buffer, 2);
    printf("binary: %s\n", buffer);

//    while (c) {
//        if (c & 1) {
//            printf("1");
//        } else {
//            printf("0");
//        }
//        c >>= 1;
//    }
}

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

// A single block of empty control bytes for tables without any slots allocated.
// This enables removing a branch in the hot path of find().
inline ctrl_t *EmptyGroup() {
    alignas(16) static constexpr ctrl_t empty_group[] = {
            kSentinel, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty,
            kEmpty, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty};
    return const_cast<ctrl_t *>(empty_group);
}


template<
        int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7,
        int b0, int b1, int b2, int b3, int b4, int b5, int b6, int b7
>
static inline void gtBlend(
        Vec8i &ivec
) {
    const auto a = permute8<i0, i1, i2, i3, i4, i5, i6, i7>(ivec);
    const auto ba = a > ivec;
    const auto max = select(ba, a, ivec);
    const auto min = select(ba, ivec, a);
    ivec = blend8<b0, b1, b2, b3, b4, b5, b6, b7>(max, min);
}

// Bose-Nelson Sorting Network for iVec. dVec is reordered according to iVec values.
static inline void sort(Vec8i &iVec, Vec8d &dVec) {
    // [[0,1],[2,3],[4,5],[6,7]]
    gtBlend</* premute */1, 0, 3, 2, 5, 4, 7, 6, /* blend values */ 0, 9, 2, 11, 4, 13, 6, 15>(iVec);
    // [[0,2],[1,3],[4,6],[5,7]]
    gtBlend</* premute */2, 3, 0, 1, 6, 7, 4, 5, /* blend values */ 0, 1, 10, 11, 4, 5, 14, 15>(iVec);
    // [[1,2],[5,6],[0,4],[3,7]]
    gtBlend</* premute */4, 2, 1, 7, 0, 6, 5, 3, /* blend values */ 0, 1, 10, 3, 12, 5, 14, 15>(iVec);
    // [[1,5],[2,6]]
    gtBlend</* premute */0, 5, 6, 3, 4, 1, 2, 7, /* blend values */ 0, 1, 2, 3, 4, 13, 14, 7>(iVec);
    // [[1,4],[3,6]]
    gtBlend</* premute */0, 4, 2, 6, 1, 5, 3, 7, /* blend values */ 0, 1, 2, 3, 12, 5, 14, 7>(iVec);
    // [[2,4],[3,5]]
    gtBlend</* premute */0, 1, 4, 5, 2, 3, 6, 7, /* blend values */ 0, 1, 2, 3, 10, 13, 6, 7>(iVec);
    // [[3,4]]
    gtBlend</* premute */0, 1, 2, 4, 3, 5, 6, 7, /* blend values */ 0, 1, 2, 3, 12, 5, 6, 7>(iVec);
}

// Returns a hash seed.
//
// The seed consists of the ctrl_ pointer, which adds enough entropy to ensure
// non-determinism of iteration order in most cases.
inline size_t HashSeed(const ctrl_t *ctrl) {
    // The low bits of the pointer have little or no entropy because of
    // alignment. We shift the pointer to try to use higher entropy bits. A
    // good number seems to be 12 bits, because that aligns with page size.
    return reinterpret_cast<uintptr_t>(ctrl) >> 12;
}

inline size_t H1(size_t hash, const ctrl_t *ctrl) {
    return (hash >> 7) ^ HashSeed(ctrl);
}

inline ctrl_t H2(size_t hash) { return hash & 0x7F; }

static size_t hash(int32_t v) {
    size_t h = 0;
    h = (h << 5) - h + ((unsigned char) v);
    h = (h << 5) - h + ((unsigned char) (v >> 8));
    h = (h << 5) - h + ((unsigned char) (v >> 16));
    h = (h << 5) - h + ((unsigned char) (v >> 24));
    return h;
}

template<size_t Width>
class probe_seq {
public:
    probe_seq(size_t hash, size_t mask) {
        offset_ = hash & mask;
        mask_ = mask;
    }


    size_t offset() const { return offset_; }

    size_t offset(size_t i) const { return (offset_ + i) & mask_; }

    void next() {
        index_ += Width;
        offset_ += index_;
        offset_ &= mask_;
    }

    size_t mask() {
        return mask_;
    }

    // 0-based probe index. The i-th probe in the probe sequence.
    size_t index() const { return index_; }

private:
    size_t mask_;
    size_t offset_;
    size_t index_ = 0;
};

static inline int CountLeadingZeros64Slow(uint64_t n) {
    int zeroes = 60;
    if (n >> 32) {
        zeroes -= 32;
        n >>= 32;
    }
    if (n >> 16) {
        zeroes -= 16;
        n >>= 16;
    }
    if (n >> 8) {
        zeroes -= 8;
        n >>= 8;
    }
    if (n >> 4) {
        zeroes -= 4;
        n >>= 4;
    }
    return "\4\3\2\2\1\1\1\1\0\0\0\0\0\0\0"[n] + zeroes;
}

static inline int CountLeadingZeros64(uint64_t n) {
#if defined(_MSC_VER) && !defined(__clang__) && defined(_M_X64)
    // MSVC does not have __buitin_clzll. Use _BitScanReverse64.
  unsigned long result = 0;  // NOLINT(runtime/int)
  if (_BitScanReverse64(&result, n)) {
    return 63 - result;
  }
  return 64;
#elif defined(_MSC_VER) && !defined(__clang__)
    // MSVC does not have __buitin_clzll. Compose two calls to _BitScanReverse
  unsigned long result = 0;  // NOLINT(runtime/int)
  if ((n >> 32) && _BitScanReverse(&result, n >> 32)) {
    return 31 - result;
  }
  if (_BitScanReverse(&result, n)) {
    return 63 - result;
  }
  return 64;
#elif defined(__GNUC__) || defined(__clang__)
    // Use __builtin_clzll, which uses the following instructions:
    //  x86: bsr
    //  ARM64: clz
    //  PPC: cntlzd
    static_assert(sizeof(unsigned long long) == sizeof(n),  // NOLINT(runtime/int)
                  "__builtin_clzll does not take 64-bit arg");

    // Handle 0 as a special case because __builtin_clzll(0) is undefined.
    if (n == 0) {
        return 64;
    }
    return __builtin_clzll(n);
#else
    return CountLeadingZeros64Slow(n);
#endif
}

static inline int CountLeadingZeros32Slow(uint64_t n) {
    int zeroes = 28;
    if (n >> 16) {
        zeroes -= 16;
        n >>= 16;
    }
    if (n >> 8) {
        zeroes -= 8;
        n >>= 8;
    }
    if (n >> 4) {
        zeroes -= 4;
        n >>= 4;
    }
    return "\4\3\2\2\1\1\1\1\0\0\0\0\0\0\0"[n] + zeroes;
}

static inline int CountLeadingZeros32(uint32_t n) {
#if defined(_MSC_VER) && !defined(__clang__)
    unsigned long result = 0;  // NOLINT(runtime/int)
  if (_BitScanReverse(&result, n)) {
    return 31 - result;
  }
  return 32;
#elif defined(__GNUC__) || defined(__clang__)
    // Use __builtin_clz, which uses the following instructions:
    //  x86: bsr
    //  ARM64: clz
    //  PPC: cntlzd
    static_assert(sizeof(int) == sizeof(n),
                  "__builtin_clz does not take 32-bit arg");

    // Handle 0 as a special case because __builtin_clz(0) is undefined.
    if (n == 0) {
        return 32;
    }
    return __builtin_clz(n);
#else
    return CountLeadingZeros32Slow(n);
#endif
}

static inline int CountTrailingZerosNonZero64Slow(uint64_t n) {
    int c = 63;
    n &= ~n + 1;
    if (n & 0x00000000FFFFFFFF) c -= 32;
    if (n & 0x0000FFFF0000FFFF) c -= 16;
    if (n & 0x00FF00FF00FF00FF) c -= 8;
    if (n & 0x0F0F0F0F0F0F0F0F) c -= 4;
    if (n & 0x3333333333333333) c -= 2;
    if (n & 0x5555555555555555) c -= 1;
    return c;
}

static inline int CountTrailingZerosNonZero64(uint64_t n) {
#if defined(_MSC_VER) && !defined(__clang__) && defined(_M_X64)
    unsigned long result = 0;  // NOLINT(runtime/int)
  _BitScanForward64(&result, n);
  return result;
#elif defined(_MSC_VER) && !defined(__clang__)
    unsigned long result = 0;  // NOLINT(runtime/int)
  if (static_cast<uint32_t>(n) == 0) {
    _BitScanForward(&result, n >> 32);
    return result + 32;
  }
  _BitScanForward(&result, n);
  return result;
#elif defined(__GNUC__) || defined(__clang__)
    static_assert(sizeof(unsigned long long) == sizeof(n),  // NOLINT(runtime/int)
                  "__builtin_ctzll does not take 64-bit arg");
    return __builtin_ctzll(n);
#else
    return CountTrailingZerosNonZero64Slow(n);
#endif
}


static inline int CountTrailingZerosNonZero32Slow(uint32_t n) {
    int c = 31;
    n &= ~n + 1;
    if (n & 0x0000FFFF) c -= 16;
    if (n & 0x00FF00FF) c -= 8;
    if (n & 0x0F0F0F0F) c -= 4;
    if (n & 0x33333333) c -= 2;
    if (n & 0x55555555) c -= 1;
    return c;
}

static inline int CountTrailingZerosNonZero32(uint32_t n) {
#if defined(_MSC_VER) && !defined(__clang__)
    unsigned long result = 0;  // NOLINT(runtime/int)
  _BitScanForward(&result, n);
  return result;
#elif defined(__GNUC__) || defined(__clang__)
    static_assert(sizeof(int) == sizeof(n),
                  "__builtin_ctz does not take 32-bit arg");
    return __builtin_ctz(n);
#else
    return CountTrailingZerosNonZero32Slow(n);
#endif
}


template<typename T>
int TrailingZeros0(T x) {
    return sizeof(T) == 8 ? CountTrailingZerosNonZero64(
            static_cast<uint64_t>(x))
                          : CountTrailingZerosNonZero32(
                    static_cast<uint32_t>(x));
}

template<typename T>
int LeadingZeros0(T x) {
    return sizeof(T) == 8
           ? CountLeadingZeros64(static_cast<uint64_t>(x))
           : CountLeadingZeros32(static_cast<uint32_t>(x));
}


// An abstraction over a bitmask. It provides an easy way to iterate through the
// indexes of the set bits of a bitmask.  When Shift=0 (platforms with SSE),
// this is a true bitmask.  On non-SSE, platforms the arithematic used to
// emulate the SSE behavior works in bytes (Shift=3) and leaves each bytes as
// either 0x00 or 0x80.
//
// For example:
//   for (int i : BitMask<uint32_t, 16>(0x5)) -> yields 0, 2
//   for (int i : BitMask<uint64_t, 8, 3>(0x0000000080800000)) -> yields 2, 3
template<class T, int SignificantBits, int Shift = 0>
class BitMask {
    static_assert(Shift == 0 || Shift == 3, "");

public:
    // These are useful for unit tests (gunit).
    using value_type = int;
    using iterator = BitMask;
    using const_iterator = BitMask;

    explicit BitMask(T mask) : mask_(mask) {}

    BitMask &operator++() {
        mask_ &= (mask_ - 1);
        return *this;
    }

    explicit operator bool() const { return mask_ != 0; }

    int operator*() const { return LowestBitSet(); }

    int LowestBitSet() const {
        return TrailingZeros();
    }

    int HighestBitSet() const {
        return (sizeof(T) * CHAR_BIT - LeadingZeros0(mask_) -
                1) >>
                   Shift;
    }

    BitMask begin() const { return *this; }

    BitMask end() const { return BitMask(0); }

    int TrailingZeros() const {
        return __builtin_ctzll(mask_) >> Shift;
    }

    int LeadingZeros() const {
        constexpr int total_significant_bits = SignificantBits << Shift;
        constexpr int extra_bits = sizeof(T) * 8 - total_significant_bits;
        return LeadingZeros0(mask_ << extra_bits) >> Shift;
    }

    T mask() {
        return mask_;
    }

    int shift() {
        return Shift;
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


#if ABSL_INTERNAL_RAW_HASH_SET_HAVE_SSE2

// https://github.com/abseil/abseil-cpp/issues/209
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=87853
// _mm_cmpgt_epi8 is broken under GCC with -funsigned-char
// Work around this by using the portable implementation of Group
// when using -funsigned-char under GCC.
inline __m128i _mm_cmpgt_epi8_fixed(__m128i a, __m128i b) {
#if defined(__GNUC__) && !defined(__clang__)
  if (std::is_unsigned<char>::value) {
    const __m128i mask = _mm_set1_epi8(0x80);
    const __m128i diff = _mm_subs_epi8(b, a);
    return _mm_cmpeq_epi8(_mm_and_si128(diff, mask), mask);
  }
#endif
  return _mm_cmpgt_epi8(a, b);
}

struct GroupSse2Impl {
  static constexpr size_t kWidth = 16;  // the number of slots per group

  explicit GroupSse2Impl(const ctrl_t* pos) {
    ctrl = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pos));
  }

  // Returns a bitmask representing the positions of slots that match hash.
  BitMask<uint32_t, kWidth> Match(h2_t hash) const {
    auto match = _mm_set1_epi8(hash);
    return BitMask<uint32_t, kWidth>(
        _mm_movemask_epi8(_mm_cmpeq_epi8(match, ctrl)));
  }

  // Returns a bitmask representing the positions of empty slots.
  BitMask<uint32_t, kWidth> MatchEmpty() const {
#if ABSL_INTERNAL_RAW_HASH_SET_HAVE_SSSE3
    // This only works because kEmpty is -128.
    return BitMask<uint32_t, kWidth>(
        _mm_movemask_epi8(_mm_sign_epi8(ctrl, ctrl)));
#else
    return Match(static_cast<h2_t>(kEmpty));
#endif
  }

  // Returns a bitmask representing the positions of empty or deleted slots.
  BitMask<uint32_t, kWidth> MatchEmptyOrDeleted() const {
    auto special = _mm_set1_epi8(kSentinel);
    return BitMask<uint32_t, kWidth>(
        _mm_movemask_epi8(_mm_cmpgt_epi8_fixed(special, ctrl)));
  }

  // Returns the number of trailing empty or deleted elements in the group.
  uint32_t CountLeadingEmptyOrDeleted() const {
    auto special = _mm_set1_epi8(kSentinel);
    return TrailingZeros(
        _mm_movemask_epi8(_mm_cmpgt_epi8_fixed(special, ctrl)) + 1);
  }

  void ConvertSpecialToEmptyAndFullToDeleted(ctrl_t* dst) const {
    auto msbs = _mm_set1_epi8(static_cast<char>(-128));
    auto x126 = _mm_set1_epi8(126);
#if ABSL_INTERNAL_RAW_HASH_SET_HAVE_SSSE3
    auto res = _mm_or_si128(_mm_shuffle_epi8(x126, ctrl), msbs);
#else
    auto zero = _mm_setzero_si128();
    auto special_mask = _mm_cmpgt_epi8_fixed(zero, ctrl);
    auto res = _mm_or_si128(msbs, _mm_andnot_si128(special_mask, x126));
#endif
    _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), res);
  }

  __m128i ctrl;
};
#endif  // ABSL_INTERNAL_RAW_HASH_SET_HAVE_SSE2

struct GroupPortableImpl {
    static constexpr size_t kWidth = 8;

    explicit GroupPortableImpl(const ctrl_t *pos) {
        ctrl = *reinterpret_cast<const size_t *>(pos);
    }

    BitMask<uint64_t, kWidth, 3> Match(h2_t hash) const {
        // For the technique, see:
        // http://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
        // (Determine if a word has a byte equal to n).
        //
        // Caveat: there are false positives but:
        // - they only occur if there is a real match
        // - they never occur on kEmpty, kDeleted, kSentinel
        // - they will be handled gracefully by subsequent checks in code
        //
        // Example:
        //   v = 0x1716151413121110
        //   hash = 0x12
        //   retval = (v - lsbs) & ~v & msbs = 0x0000000080800000
        constexpr uint64_t msbs = 0x8080808080808080ULL;
        constexpr uint64_t lsbs = 0x0101010101010101ULL;
        auto x = ctrl ^(lsbs * hash);
        return BitMask<uint64_t, kWidth, 3>((x - lsbs) & ~x & msbs);
    }

    BitMask<uint64_t, kWidth, 3> MatchEmpty() const {
        constexpr uint64_t msbs = 0x8080808080808080ULL;
        return BitMask<uint64_t, kWidth, 3>((ctrl & (~ctrl << 6)) & msbs);
    }

    BitMask<uint64_t, kWidth, 3> MatchEmptyOrDeleted() const {
        constexpr uint64_t msbs = 0x8080808080808080ULL;
        return BitMask<uint64_t, kWidth, 3>((ctrl & (~ctrl << 7)) & msbs);
    }

    uint32_t CountLeadingEmptyOrDeleted() const {
        constexpr uint64_t gaps = 0x00FEFEFEFEFEFEFEULL;
        return (TrailingZeros0(((~ctrl & (ctrl >> 7)) | gaps) + 1) + 7) >> 3;
    }

    void ConvertSpecialToEmptyAndFullToDeleted(ctrl_t *dst) const {
        constexpr uint64_t msbs = 0x8080808080808080ULL;
        constexpr uint64_t lsbs = 0x0101010101010101ULL;
        auto x = ctrl & msbs;
        auto res = (~x + (x >> 7)) & ~lsbs;
        dst[0] = res;
    }

    uint64_t ctrl;
};

#if ABSL_INTERNAL_RAW_HASH_SET_HAVE_SSE2
using Group = GroupSse2Impl;
#else
using Group = GroupPortableImpl;
#endif


struct fook_map_t {
    ctrl_t *ctrl_ = EmptyGroup();      // [(capacity + 1) * ctrl_t]+
    unsigned char *slots_ = nullptr;   // [capacity * types]
    size_t size_ = 0;             // number of full slots
    size_t capacity_ = 0;         // total number of slots
    size_t slot_size_;             // size of key in each slot
    size_t slot_size_shift_;
    size_t growth_left_ = 0;
};

// Reset all ctrl bytes back to kEmpty, except the sentinel.
void reset_ctrl(fook_map_t *map) {
    uint64_t l = (map->capacity_ + 1) * sizeof(Group);
    memset(map->ctrl_, kEmpty, l);
    map->ctrl_[map->capacity_] = kSentinel;
}

// We use 7/8th as maximum load factor.
// For 16-wide groups, that gives an average of two empty slots per group.
inline size_t CapacityToGrowth(size_t capacity) {
    // `capacity*7/8`
    if (sizeof(Group) == 8 && capacity == 7) {
        // x-x/8 does not work when x==7.
        return 6;
    }
    return capacity - capacity / 8;
}

inline void reset_growth_left(fook_map_t *map) {
    map->growth_left_ = CapacityToGrowth(map->capacity_) - map->size_;
}

static void initialize_slots(fook_map_t *map) {
    const size_t ctrl_capacity = 2 * sizeof(Group) * (map->capacity_ + 1);
    auto *mem = reinterpret_cast<unsigned char *>(malloc(
            ctrl_capacity + map->slot_size_ * (map->capacity_ + 1)));
    map->ctrl_ = reinterpret_cast<ctrl_t *>(mem);
    map->slots_ = mem + ctrl_capacity;
    reset_ctrl(map);
    reset_growth_left(map);
}

static fook_map_t *allocate(const int32_t *key_types, const int32_t key_count, const size_t map_capacity) {
    size_t slot_key_size = 0;
    for (int32_t i = 0; i < key_count; i++) {
        switch (key_types[i]) {
            case 0: // BOOL
            case 1: // BYTE
                slot_key_size += 1;
                break;
            case 2: // SHORT
            case 3: // CHAR
                slot_key_size += 2;
                break;
            case 4: // INT
            case 8: // FLOAT
            case 11: // SYMBOL - store as INT
                slot_key_size += 4;
                break;
            case 5: // LONG (64 bit)
            case 6: // DATE
            case 7: // TIMESTAMP
            case 9: // DOUBLE
            case 10: // STRING - store reference only
                slot_key_size += 8;
                break;
            case 12: // LONG256
                slot_key_size += 64;
                break;
        }
    }

    auto map = reinterpret_cast<fook_map_t *>(malloc(sizeof(fook_map_t)));
    map->slot_size_ = ceil_pow_2(slot_key_size);
    map->slot_size_shift_ = 63 - __builtin_clzll(map->slot_size_);
//    printf("slot_size=%d, slot_mask=%d, key_size=%d\n", map->slot_size_, map->slot_size_shift_, slot_key_size);
    map->capacity_ = map_capacity;
    map->size_ = 0;
    initialize_slots(map);
    return map;
}

inline static probe_seq<8> probe(fook_map_t *map, size_t hash) {
    return probe_seq<sizeof(Group)>(H1(hash, map->ctrl_), map->capacity_);
}

#define PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define PREDICT_TRUE(x) (__builtin_expect(false || (x), true))


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
    size_t offset;
    size_t probe_length;
};

FindInfo find_first_non_full(fook_map_t *map, size_t hash) {
    auto seq = probe(map, hash);
    while (true) {
        Group g{map->ctrl_ + seq.offset()};
        auto mask = g.MatchEmptyOrDeleted();
        if (mask) {
#if !defined(NDEBUG)
            // We want to add entropy even when ASLR is not enabled.
        // In debug build we will randomly insert in either the front or back of
        // the group.
        // TODO(kfm,sbenza): revisit after we do unconditional mixing
        if (!is_small() && ShouldInsertBackwards(hash, ctrl_)) {
          return {seq.offset(mask.HighestBitSet()), seq.index()};
        }
#endif
            return {seq.offset(mask.LowestBitSet()), seq.index()};
        }
        seq.next();
    }
}

inline bool IsEmpty(ctrl_t c) { return c == kEmpty; }

static inline bool IsFull(ctrl_t c) { return c >= 0; }

static inline bool IsDeleted(ctrl_t c) { return c == kDeleted; }

inline bool IsEmptyOrDeleted(ctrl_t c) { return c < kSentinel; }

static void set_ctrl(fook_map_t *map, size_t i, ctrl_t h) {
    constexpr uint32_t group_size = sizeof(Group);
    const int32_t p = ((i - group_size) & map->capacity_) + 1 + ((group_size - 1) & map->capacity_);
    map->ctrl_[i] = h;
    map->ctrl_[p] = h;
}

static void resize(fook_map_t *map, size_t new_capacity) {
    auto *old_ctrl = map->ctrl_;
    auto *old_slots = map->slots_;
    const size_t old_capacity = map->capacity_;
    map->capacity_ = new_capacity;
    initialize_slots(map);

    size_t total_probe_length = 0;
    for (size_t i = 0; i != old_capacity; ++i) {
        if (IsFull(old_ctrl[i])) {
            // todo: this assumes we have int32_t key. Make hash (SIMD)
            auto p = old_slots + (i << map->slot_size_shift_);
            const size_t hh = hash(*(reinterpret_cast<int32_t *>(p)));
            auto target = find_first_non_full(map, hh);
            size_t new_i = target.offset;
            total_probe_length += target.probe_length;
            set_ctrl(map, new_i, H2(hh));

            // todo: copy assumes int32_t key
            *(reinterpret_cast<int32_t *>(map->slots_ + (new_i << map->slot_size_shift_))) = *p;
        }
    }
    if (old_capacity) {
        printf("freed previous\n//");
//        SanitizerUnpoisonMemoryRegion(old_slots, map.slot_size_ * old_capacity);
        free(old_ctrl);
    }

    printf("after rehash size=%lu, growth_left=%lu, capacity=%lu\n", map->size_, map->growth_left_, map->capacity_);
//    infoz_.RecordRehash(total_probe_length);
}

// PRECONDITION:
//   IsValidCapacity(capacity)
//   ctrl[capacity] == kSentinel
//   ctrl[i] != kSentinel for all i < capacity
// Applies mapping for every byte in ctrl:
//   DELETED -> EMPTY
//   EMPTY -> EMPTY
//   FULL -> DELETED
inline void ConvertDeletedToEmptyAndFullToDeleted(ctrl_t *ctrl, size_t capacity) {
//    assert(ctrl[capacity] == kSentinel);
//    assert(IsValidCapacity(capacity));
    for (ctrl_t *pos = ctrl; pos != ctrl + capacity + 1; pos += Group::kWidth) {
        Group{pos}.ConvertSpecialToEmptyAndFullToDeleted(pos);
    }
    // Copy the cloned ctrl bytes.
    std::memcpy(ctrl + capacity + 1, ctrl, Group::kWidth);
    ctrl[capacity] = kSentinel;
}

__declspec(noinline) void drop_deletes_without_resize(fook_map_t *map) {
//    assert(IsValidCapacity(capacity_));
//    assert(!is_small());
// Algorithm:
// - mark all DELETED slots as EMPTY
// - mark all FULL slots as DELETED
// - for each slot marked as DELETED
//     hash = Hash(element)
//     target = find_first_non_full(hash)
//     if target is in the same group
//       mark slot as FULL
//     else if target is EMPTY
//       transfer element to target
//       mark slot as EMPTY
//       mark target as FULL
//     else if target is DELETED
//       swap current element with target element
//       mark target as FULL
//       repeat procedure for current slot with moved from element (target)
    ConvertDeletedToEmptyAndFullToDeleted(map->ctrl_, map->capacity_);
//    alignas(slot_type)
    unsigned char slot[map->slot_size_];
    size_t total_probe_length = 0;

    for (size_t i = 0; i != map->capacity_; ++i) {
        if (!IsDeleted(map->ctrl_[i])) continue;
        // todo: this assumes we have int32_t key. Make hash (SIMD)
        const size_t hh = hash(*(reinterpret_cast<int32_t *>(map->slots_ + i)));
//        size_t hh = PolicyTraits::apply(HashElement{hash_ref()},
//                                        PolicyTraits::element(slots_ + i));
        auto target = find_first_non_full(map, hh);
        size_t new_i = target.offset;
        total_probe_length += target.probe_length;

// Verify if the old and new i fall within the same group wrt the hash.
// If they do, we don't need to move the object as it falls already in the
// best probe we can.
        const auto probe_index = [&](size_t pos) {
            return ((pos - probe(map, hh).offset()) & map->capacity_) / Group::kWidth;
        };

// Element doesn't move.
        if (PREDICT_TRUE(probe_index(new_i) == probe_index(i))) {
            set_ctrl(map, i, H2(hh));
            continue;
        }
        if (IsEmpty(map->ctrl_[new_i])) {
// Transfer element to the empty spot.
// set_ctrl poisons/unpoisons the slots so we have to call it at the
// right time.
            set_ctrl(map, new_i, H2(hh));
            std::memcpy(map->slots_ + new_i, map->slots_ + i, map->slot_size_);
//            PolicyTraits::transfer(&alloc_ref(), slots_ + new_i, slots_ + i);
            set_ctrl(map, i, kEmpty);
        } else {
//            assert(IsDeleted(map.ctrl_[new_i]));
            set_ctrl(map, new_i, H2(hh));
// Until we are done rehashing, DELETED marks previously FULL slots.
// Swap i and new_i elements.
            std::memcpy(slot, map->slots_ + i, map->slot_size_);
//            PolicyTraits::transfer(&alloc_ref(), slot, slots_ + i);
            std::memcpy(map->slots_ + i, map->slots_ + new_i, map->slot_size_);
//            PolicyTraits::transfer(&alloc_ref(), slots_ + i, slots_ + new_i);
            std::memcpy(map->slots_ + new_i, slot, map->slot_size_);
//            PolicyTraits::transfer(&alloc_ref(), slots_ + new_i, slot);
            --i;  // repeat
        }
    }
    reset_growth_left(map);
//    infoz_.RecordRehash(total_probe_length);
}

static void rehash_and_grow_if_necessary(fook_map_t *map) {
    if (map->capacity_ == 0) {
        printf("rehash 1 \n");
        resize(map, 1);
    } else if (map->size_ <= CapacityToGrowth(map->capacity_) / 2) {
        // Squash DELETED without growing if there is enough capacity.
        printf("rehash half arse \n");
        drop_deletes_without_resize(map);
    } else {
        // Otherwise grow the container.
        printf("rehash quad \n");
        resize(map, map->capacity_ * 2 + 1);
    }
}

/*
size_t prepare_insert(size_t hash) ABSL_ATTRIBUTE_NOINLINE {
auto target = find_first_non_full(hash);
if (ABSL_PREDICT_FALSE(growth_left() == 0 &&
!IsDeleted(ctrl_[target.offset]))) {
rehash_and_grow_if_necessary();
target = find_first_non_full(hash);
}
++size_;
growth_left() -= IsEmpty(ctrl_[target.offset]);
set_ctrl(target.offset, H2(hash));
infoz_.RecordInsert(hash, target.probe_length);
return target.offset;
}
*/

__declspec(noinline) static size_t prepare_insert(fook_map_t *map, size_t hash) {
    auto target = find_first_non_full(map, hash);
    if (PREDICT_FALSE(map->growth_left_ == 0 && !IsDeleted(map->ctrl_[target.offset]))) {
        rehash_and_grow_if_necessary(map);
        target = find_first_non_full(map, hash);
    }
    ++map->size_;
    map->growth_left_ -= IsEmpty(map->ctrl_[target.offset]);
    set_ctrl(map, target.offset, H2(hash));
//    infoz_.RecordInsert(hash, target.probe_length);
    return target.offset;
}

static inline std::pair<size_t, bool> find_or_prepare_insert(fook_map_t *map, const int32_t key) {
    auto hh = hash(key);
    auto seq = probe(map, hh);
//    printf("find key=%d, hash=%lu, offset=%lu, cap=%lu, h1=%lu, h2=%d\n", key, hh, seq.offset(), map->capacity_,
//           H1(hh, map->ctrl_), H2(hh));
    while (true) {
        Group g{map->ctrl_ + seq.offset()};
        for (int i : g.Match(H2(hh))) {
            int32_t p = *reinterpret_cast<int32_t *>(map->slots_ + (seq.offset(i) << map->slot_size_shift_));
//            printf("cmp v=%d, key=%d, offset=%lu, i=%d\n", p, key, (seq.offset(i) << map->slot_size_shift_), i);
            if (PREDICT_TRUE(p == key)) {
                return {seq.offset(i), false};
            }
        }
        if (PREDICT_TRUE(g.MatchEmpty())) {
            break;
        }
//        printf("oops!\n");
        seq.next();
    }
    return {prepare_insert(map, hh), true};
}

int64_t MATCH_GROUPS(int32_t *pl, double *pd, int64_t count) {
    const int step = 8;
    Vec8i vecia;
    Vec8i vecib;
    Vec8d vecda;
    Vec8d vecdb;

    vecia.load(pl);
    vecda.load(pd);

    int32_t key_types[] = {4};
    fook_map_t *map = allocate(key_types, 1, 2047);
    printf("pre-size: %lu, pre-growth: %lu\n", map->size_, map->growth_left_);

    _mm_prefetch(map, _MM_HINT_T0);

    // vector all the same
    // carry on looping until there is a change in data
    for (int i = 0; i < count; i++) {
//            _mm_prefetch(pl + i + 63 * step, _MM_HINT_T1);
//        vecib.load(pl + i + step);
//        vecdb.load(pd + i + step);
//        Vec8ib match = vecia == vecib;
/*
#if INSTRSET >= 10
        vecda = _mm512_mask_add_pd(vecda, match, vecda, vecdb);
#else
        vecda = if_add(extend(match), vecda, vecdb);
#endif
        if (!horizontal_and(match)) {
            sort(vecia, vecda);
//            printVec8i("sorted a", vecia);
            sort(vecib, vecdb);
//            printVec8i("sorted b", vecib);

            if (!horizontal_and(vecia == vecib)) {
*/
        // they do not match still, oh, bummer
//                printf("no-match\n");

        int32_t v = (pl + i)[0];

        auto res = find_or_prepare_insert(map, v);
        if (res.second) {
            auto *p = reinterpret_cast<int32_t *>(map->slots_ + (res.first << map->slot_size_shift_));
            *p = v;
//            printf("v=%d -> offset=%lu, read=%d\n", v, (res.first << map->slot_size_shift_), *p);
        }

//        printf("first: %ul\n", res.first);
//        if (res.second) {
//            *reinterpret_cast<int32_t *>(map->slots_ + res.first) = v;
//            printf("added: %d\n", v);
//        } else {
//            printf("dupe: %d\n", v);
//        }
    }

//            return 0;
/*
        }
    }
*/

    printf("post-size: %lu, post-growth: %lu\n", map->size_, map->growth_left_);

    return LLONG_MIN;
}

#if INSTRSET < 9

MATCH_GROUP_DISPATCHER(matchGroup)

#endif

