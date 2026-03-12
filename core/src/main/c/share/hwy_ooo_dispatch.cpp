/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "hwy_ooo_dispatch.cpp"
#include "hwy/foreach_target.h"
#include "hwy/highway.h"

#include "util.h"
#include "simd.h"
#include "ooo_dispatch.h"
#include <cstring>
#include <algorithm>

HWY_BEFORE_NAMESPACE();
namespace questdb_ooo {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// ---------------------------------------------------------------------------
// Index layout constants.
//
// Java allocates index memory as a flat int64_t[] array and passes the
// raw address via JNI. The SIMD kernels below receive int64_t* directly
// so that LoadInterleaved2 / StoreInterleaved2 operate on a genuine
// int64_t array — the type the memory was allocated as.
//
// The previous code cast index_t* to int64_t* inside each kernel.
// That violated both [basic.lval] (strict aliasing: int64_t is not
// pointer-interconvertible with index_t's uint64_t members) and
// [expr.add] (pointer arithmetic on the reinterpreted pointer crossed
// struct-member boundaries). The C-linkage wrappers at the bottom of
// this file now perform the cast at the boundary, and the SIMD code
// never sees index_t. The boundary cast itself is a bitwise address
// reinterpretation (no arithmetic, no dereference) — the underlying
// memory was allocated by Java as a flat long[] and never had index_t
// object lifetime.
// ---------------------------------------------------------------------------
static constexpr int64_t IX_TS = 0;     // offset of .ts within a pair
static constexpr int64_t IX_I = 1;      // offset of .i within a pair
static constexpr int64_t IX_STRIDE = 2; // int64 elements per index entry

// ---------------------------------------------------------------------------
// 29: copy_index_timestamp
// Copy ts fields from index[index_lo..index_hi] to dest
// ---------------------------------------------------------------------------
void CopyIndexTimestamp(const int64_t *index, int64_t index_lo, int64_t index_hi, int64_t *dest) {
    const int64_t count = index_hi - index_lo + 1;
    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);
    const int64_t *base = index + index_lo * IX_STRIDE;
    int64_t i = 0;

    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        MM_PREFETCH_T0(base + (i + 64) * IX_STRIDE);

        auto ts = hn::Zero(d);
        auto rows = hn::Zero(d);
        hn::LoadInterleaved2(d, base + i * IX_STRIDE, ts, rows);
        hn::StoreU(ts, d, dest + i);
    }

    // tail
    for (; i < count; i++) {
        dest[i] = base[i * IX_STRIDE + IX_TS];
    }
}

// ---------------------------------------------------------------------------
// 27: shift_copy
// dest[i] = src[i + src_lo] - shift
// ---------------------------------------------------------------------------
void ShiftCopy(int64_t shift, const int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest) {
    const int64_t count = src_hi - src_lo + 1;
    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);
    const auto vec_shift = hn::Set(d, shift);
    const int64_t *src_base = src + src_lo;

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        MM_PREFETCH_T0(src_base + i + 64);
        const auto vec = hn::LoadU(d, src_base + i);
        hn::StoreU(hn::Sub(vec, vec_shift), d, dest + i);
    }

    // tail
    for (; i < count; i++) {
        dest[i] = src_base[i] - shift;
    }
}

// ---------------------------------------------------------------------------
// 28: shift_copy_varchar_aux
// Copy pairs of int64, subtracting (shift << 16) from the second element
// ---------------------------------------------------------------------------
void ShiftCopyVarcharAux(int64_t shift, const int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest) {
    const int64_t count = 2 * (src_hi - src_lo + 1);
    const int64_t *src_base = src + 2 * src_lo;
    const int64_t shifted = shift << 16;

    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);

    // Build alternating shift vector: 0, shifted, 0, shifted, ...
    // N may vary by target, so build dynamically.
    if (N >= 2) {
        HWY_ALIGN int64_t shift_arr[HWY_MAX_LANES_D(hn::ScalableTag<int64_t>)];
        for (size_t k = 0; k < N; k++) {
            shift_arr[k] = (k & 1) ? shifted : 0;
        }
        const auto vec_shift = hn::Load(d, shift_arr);

        // N must be even for this to work correctly on pairs
        if ((N & 1) == 0) {
            int64_t i = 0;
            for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
                const auto vec = hn::LoadU(d, src_base + i);
                hn::StoreU(hn::Sub(vec, vec_shift), d, dest + i);
            }

            // tail
            for (; i < count; i += 2) {
                dest[i] = src_base[i];
                dest[i + 1] = src_base[i + 1] - shifted;
            }
            return;
        }
    }

    // Scalar fallback
    for (int64_t i = 0; i < count; i += 2) {
        dest[i] = src_base[i];
        dest[i + 1] = src_base[i + 1] - shifted;
    }
}

// ---------------------------------------------------------------------------
// 32: shift_copy_array_aux
// Copy pairs of int64, subtracting shift from the first element
// ---------------------------------------------------------------------------
void ShiftCopyArrayAux(int64_t shift, const int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest) {
    const int64_t count = 2 * (src_hi - src_lo + 1);
    const int64_t *src_base = src + 2 * src_lo;

    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);

    if (N >= 2) {
        HWY_ALIGN int64_t shift_arr[HWY_MAX_LANES_D(hn::ScalableTag<int64_t>)];
        for (size_t k = 0; k < N; k++) {
            shift_arr[k] = (k & 1) ? 0 : shift;
        }
        const auto vec_shift = hn::Load(d, shift_arr);

        if ((N & 1) == 0) {
            int64_t i = 0;
            for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
                const auto vec = hn::LoadU(d, src_base + i);
                hn::StoreU(hn::Sub(vec, vec_shift), d, dest + i);
            }

            // tail
            for (; i < count; i += 2) {
                dest[i] = src_base[i] - shift;
                dest[i + 1] = src_base[i + 1];
            }
            return;
        }
    }

    // Scalar fallback
    for (int64_t i = 0; i < count; i += 2) {
        dest[i] = src_base[i] - shift;
        dest[i + 1] = src_base[i + 1];
    }
}

// ---------------------------------------------------------------------------
// 26: copy_index
// Copy ts fields from index to dest
// ---------------------------------------------------------------------------
void CopyIndex(const int64_t *index, const int64_t count, int64_t *dest) {
    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        MM_PREFETCH_T0(index + (i + 64) * IX_STRIDE);

        auto ts = hn::Zero(d);
        auto rows = hn::Zero(d);
        hn::LoadInterleaved2(d, index + i * IX_STRIDE, ts, rows);
        hn::StoreU(ts, d, dest + i);
    }

    for (; i < count; i++) {
        dest[i] = index[i * IX_STRIDE + IX_TS];
    }
}

// ---------------------------------------------------------------------------
// 24: set_binary_column_null_refs
// Fill with offset + i * sizeof(int64_t)
// ---------------------------------------------------------------------------
void SetBinaryColumnNullRefs(int64_t *data, int64_t offset, int64_t count) {
    const int64_t step = static_cast<int64_t>(sizeof(int64_t));
    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);
    const auto vec_step = hn::Set(d, static_cast<int64_t>(N) * step);

    // Build initial vector: offset + 0*step, offset + 1*step, ...
    HWY_ALIGN int64_t init[HWY_MAX_LANES_D(hn::ScalableTag<int64_t>)];
    for (size_t k = 0; k < N; k++) {
        init[k] = offset + static_cast<int64_t>(k) * step;
    }
    auto vec_addr = hn::Load(d, init);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        hn::StoreU(vec_addr, d, data + i);
        vec_addr = hn::Add(vec_addr, vec_step);
    }

    // tail
    for (; i < count; i++) {
        data[i] = offset + i * step;
    }
}

// ---------------------------------------------------------------------------
// 25: set_string_column_null_refs
// Fill with offset + i * sizeof(int32_t)
// ---------------------------------------------------------------------------
void SetStringColumnNullRefs(int64_t *data, int64_t offset, int64_t count) {
    const int64_t step = static_cast<int64_t>(sizeof(int32_t));
    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);
    const auto vec_step = hn::Set(d, static_cast<int64_t>(N) * step);

    HWY_ALIGN int64_t init[HWY_MAX_LANES_D(hn::ScalableTag<int64_t>)];
    for (size_t k = 0; k < N; k++) {
        init[k] = offset + static_cast<int64_t>(k) * step;
    }
    auto vec_addr = hn::Load(d, init);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        hn::StoreU(vec_addr, d, data + i);
        vec_addr = hn::Add(vec_addr, vec_step);
    }

    // tail
    for (; i < count; i++) {
        data[i] = offset + i * step;
    }
}

// ---------------------------------------------------------------------------
// 30: set_varchar_null_refs
// Fill pairs: aux[2*i] = 4, aux[2*i+1] = offset << 16
// ---------------------------------------------------------------------------
void SetVarcharNullRefs(int64_t *aux, int64_t offset, int64_t count) {
    const int64_t o = offset << 16;
    const int64_t pair_count = count * 2;

    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);

    if (N >= 2 && (N & 1) == 0) {
        HWY_ALIGN int64_t pattern[HWY_MAX_LANES_D(hn::ScalableTag<int64_t>)];
        for (size_t k = 0; k < N; k++) {
            pattern[k] = (k & 1) ? o : 4;
        }
        const auto vec_pattern = hn::Load(d, pattern);

        int64_t i = 0;
        for (; i + static_cast<int64_t>(N) <= pair_count; i += static_cast<int64_t>(N)) {
            hn::StoreU(vec_pattern, d, aux + i);
        }

        // tail
        for (; i < pair_count; i += 2) {
            aux[i] = 4;
            aux[i + 1] = o;
        }
    } else {
        for (int64_t i = 0; i < pair_count; i += 2) {
            aux[i] = 4;
            aux[i + 1] = o;
        }
    }
}

// ---------------------------------------------------------------------------
// 31: set_array_null_refs
// Fill pairs: aux[2*i] = offset, aux[2*i+1] = 0
// ---------------------------------------------------------------------------
void SetArrayNullRefs(int64_t *aux, int64_t offset, int64_t count) {
    const int64_t pair_count = count * 2;

    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);

    if (N >= 2 && (N & 1) == 0) {
        HWY_ALIGN int64_t pattern[HWY_MAX_LANES_D(hn::ScalableTag<int64_t>)];
        for (size_t k = 0; k < N; k++) {
            pattern[k] = (k & 1) ? 0 : offset;
        }
        const auto vec_pattern = hn::Load(d, pattern);

        int64_t i = 0;
        for (; i + static_cast<int64_t>(N) <= pair_count; i += static_cast<int64_t>(N)) {
            hn::StoreU(vec_pattern, d, aux + i);
        }

        // tail
        for (; i < pair_count; i += 2) {
            aux[i] = offset;
            aux[i + 1] = 0;
        }
    } else {
        for (int64_t i = 0; i < pair_count; i += 2) {
            aux[i] = offset;
            aux[i + 1] = 0;
        }
    }
}

// ---------------------------------------------------------------------------
// 19: set_memory_vanilla_int64
// ---------------------------------------------------------------------------
void SetMemoryVanillaInt64(int64_t *data, const int64_t value, const int64_t count) {
    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);
    const auto vec = hn::Set(d, value);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        hn::StoreU(vec, d, data + i);
    }

    // tail
    for (; i < count; i++) {
        data[i] = value;
    }
}

// ---------------------------------------------------------------------------
// 20: set_memory_vanilla_int32
// ---------------------------------------------------------------------------
void SetMemoryVanillaInt32(int32_t *data, const int32_t value, const int64_t count) {
    const hn::ScalableTag<int32_t> d;
    const size_t N = hn::Lanes(d);
    const auto vec = hn::Set(d, value);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        hn::StoreU(vec, d, data + i);
    }

    for (; i < count; i++) {
        data[i] = value;
    }
}

// ---------------------------------------------------------------------------
// 21: set_memory_vanilla_double
// ---------------------------------------------------------------------------
void SetMemoryVanillaDouble(double *data, const double value, const int64_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    const auto vec = hn::Set(d, value);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        hn::StoreU(vec, d, data + i);
    }

    for (; i < count; i++) {
        data[i] = value;
    }
}

// ---------------------------------------------------------------------------
// 22: set_memory_vanilla_float
// ---------------------------------------------------------------------------
void SetMemoryVanillaFloat(float *data, const float value, const int64_t count) {
    const hn::ScalableTag<float> d;
    const size_t N = hn::Lanes(d);
    const auto vec = hn::Set(d, value);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        hn::StoreU(vec, d, data + i);
    }

    for (; i < count; i++) {
        data[i] = value;
    }
}

// ---------------------------------------------------------------------------
// 23: set_memory_vanilla_short
// ---------------------------------------------------------------------------
void SetMemoryVanillaShort(int16_t *data, const int16_t value, const int64_t count) {
    const hn::ScalableTag<int16_t> d;
    const size_t N = hn::Lanes(d);
    const auto vec = hn::Set(d, value);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        hn::StoreU(vec, d, data + i);
    }

    for (; i < count; i++) {
        data[i] = value;
    }
}

// ---------------------------------------------------------------------------
// 33: set_memory_vanilla_int128
// Fill with 128-bit value.
//
// The previous implementation reinterpret_cast'd to int64_t* and used
// Highway StoreU through that pointer. That violated [basic.lval] (strict
// aliasing: int64_t is not pointer-interconvertible with packed uint64_t
// members) and [expr.add] (pointer arithmetic crossing struct-member
// boundaries). The VCL-era code had the same structural problem plus a
// logic bug in run_vec_bulk where TVec::size() returned the int64 lane
// count instead of the struct-element count, producing wrong output for
// counts >= 5 (int128) or >= 3 (int256).
//
// Explicit member stores are standards-clean and auto-vectorize to ymm/zmm
// stores with GCC 15 and Clang 20 at -O3. Struct assignment does not
// auto-vectorize for packed structs whose value fits in GPRs.
// ---------------------------------------------------------------------------
void SetMemoryVanillaInt128(long_128bit *data, const long_128bit value, const int64_t count) {
    for (int64_t i = 0; i < count; i++) {
        data[i].long0 = value.long0;
        data[i].long1 = value.long1;
    }
}

// ---------------------------------------------------------------------------
// 34: set_memory_vanilla_int256
// Fill with 256-bit value. See int128 comment.
// ---------------------------------------------------------------------------
void SetMemoryVanillaInt256(long_256bit *data, const long_256bit value, const int64_t count) {
    for (int64_t i = 0; i < count; i++) {
        data[i].long0 = value.long0;
        data[i].long1 = value.long1;
        data[i].long2 = value.long2;
        data[i].long3 = value.long3;
    }
}

// ---------------------------------------------------------------------------
// 18: make_timestamp_index
// Create index from timestamps: dest[l-low].ts = data[l], dest[l-low].i = l | (1ULL << 63)
// ---------------------------------------------------------------------------
void MakeTimestampIndex(const int64_t *data, int64_t low, int64_t high, int64_t *dest) {
    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);
    const auto vec_inc = hn::Set(d, static_cast<int64_t>(N));

    // Build initial i-vector: low | (1<<63), (low+1) | (1<<63), ...
    HWY_ALIGN int64_t init_i[HWY_MAX_LANES_D(hn::ScalableTag<int64_t>)];
    for (size_t k = 0; k < N; k++) {
        init_i[k] = (low + static_cast<int64_t>(k)) | static_cast<int64_t>(1ULL << 63);
    }
    auto vec_i = hn::Load(d, init_i);

    int64_t l = low;
    for (; l + static_cast<int64_t>(N) - 1 <= high; l += static_cast<int64_t>(N)) {
        MM_PREFETCH_T0(data + l + 64);
        const auto vec_ts = hn::LoadU(d, data + l);
        hn::StoreInterleaved2(vec_ts, vec_i, d, dest + (l - low) * IX_STRIDE);

        vec_i = hn::Add(vec_i, vec_inc);
    }

    // tail
    for (; l <= high; l++) {
        const int64_t off = (l - low) * IX_STRIDE;
        dest[off + IX_TS] = data[l];
        dest[off + IX_I] = l | static_cast<int64_t>(1ULL << 63);
    }
}

// ---------------------------------------------------------------------------
// 31: shift_timestamp_index
// Copy ts, set i = sequential index.
// ---------------------------------------------------------------------------
void ShiftTimestampIndex(const int64_t *src, int64_t count, int64_t *dest) {
    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);
    const auto vec_inc = hn::Set(d, static_cast<int64_t>(N));

    HWY_ALIGN int64_t init[HWY_MAX_LANES_D(hn::ScalableTag<int64_t>)];
    for (size_t k = 0; k < N; k++) {
        init[k] = static_cast<int64_t>(k);
    }
    auto vec_i = hn::Load(d, init);

    int64_t l = 0;
    for (; l + static_cast<int64_t>(N) <= count; l += static_cast<int64_t>(N)) {
        MM_PREFETCH_T0(src + (l + 64) * IX_STRIDE);
        auto ts = hn::Zero(d);
        auto old_i = hn::Zero(d);
        hn::LoadInterleaved2(d, src + l * IX_STRIDE, ts, old_i);
        hn::StoreInterleaved2(ts, vec_i, d, dest + l * IX_STRIDE);
        vec_i = hn::Add(vec_i, vec_inc);
    }

    for (; l < count; l++) {
        dest[l * IX_STRIDE + IX_TS] = src[l * IX_STRIDE + IX_TS];
        dest[l * IX_STRIDE + IX_I] = l;
    }
}

// ---------------------------------------------------------------------------
// 17: flatten_index
// Set index[i].i = i for all elements
// ---------------------------------------------------------------------------
void FlattenIndex(int64_t *index, int64_t count) {
    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);
    const auto vec_inc = hn::Set(d, static_cast<int64_t>(N));

    // Build initial vector: 0, 1, 2, ...
    HWY_ALIGN int64_t init[HWY_MAX_LANES_D(hn::ScalableTag<int64_t>)];
    for (size_t k = 0; k < N; k++) {
        init[k] = static_cast<int64_t>(k);
    }
    auto vec_i = hn::Load(d, init);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        auto ts = hn::Zero(d);
        auto old_i = hn::Zero(d);
        hn::LoadInterleaved2(d, index + i * IX_STRIDE, ts, old_i);
        hn::StoreInterleaved2(ts, vec_i, d, index + i * IX_STRIDE);
        vec_i = hn::Add(vec_i, vec_inc);
    }

    // tail
    for (; i < count; i++) {
        index[i * IX_STRIDE + IX_I] = i;
    }
}

// ---------------------------------------------------------------------------
// 11: merge_shuffle_int32
// Merge from two sources based on high bit of index[i].i.
// Branchless scalar loop with stack-based source pointer table.
// The compiler generates shr 63 + table lookup for source selection.
// No per-chunk same-source detection — this maximizes memory-level
// parallelism when the working set exceeds L3 cache (the typical case).
// ---------------------------------------------------------------------------
void MergeShuffleInt32(const int32_t *src1, const int32_t *src2, int32_t *dest,
                       const index_t *index, const int64_t count) {
    const int32_t *sources[] = {src2, src1};

    for (int64_t i = 0; i < count; i++) {
        MM_PREFETCH_T0(index + i + 64);
        const auto r = static_cast<uint64_t>(index[i].i);
        dest[i] = sources[r >> 63][r & ~(1ULL << 63)];
    }
}

// ---------------------------------------------------------------------------
// 12: merge_shuffle_int64
// Merge from two sources based on high bit of index[i].i.
// Uses masked 64-bit gathers so we only touch the selected source per lane.
// ---------------------------------------------------------------------------
void MergeShuffleInt64(const int64_t *src1, const int64_t *src2, int64_t *dest,
                       const int64_t *index, const int64_t count) {
    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);
    const auto row_mask = hn::Set(d, static_cast<int64_t>(~(1ULL << 63)));

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        MM_PREFETCH_T0(index + (i + 64) * IX_STRIDE);
        auto ts = hn::Zero(d);
        auto idx = hn::Zero(d);
        hn::LoadInterleaved2(d, index + i * IX_STRIDE, ts, idx);

        // High bit set (negative) = pick from src1, clear = pick from src2
        const auto pick_src1 = hn::Lt(idx, hn::Zero(d));
        const auto row = hn::And(idx, row_mask);

        const auto from_src2 = hn::MaskedGatherIndexOr(hn::Zero(d), hn::Not(pick_src1), d, src2, row);
        const auto merged = hn::MaskedGatherIndexOr(from_src2, pick_src1, d, src1, row);
        hn::StoreU(merged, d, dest + i);
    }

    // scalar tail
    const int64_t *sources[] = {src2, src1};
    for (; i < count; i++) {
        const auto r = static_cast<uint64_t>(index[i * IX_STRIDE + IX_I]);
        const uint64_t pick = r >> 63u;
        const auto row = r & ~(1ULL << 63u);
        dest[i] = sources[pick][row];
    }
}

// ---------------------------------------------------------------------------
// 6: re_shuffle_int64
// Uses GatherIndex for SIMD gather on AVX2+ / SVE; emulated on older targets.
// ---------------------------------------------------------------------------
void ReShuffleInt64(const int64_t *src, int64_t *dest, const int64_t *index, const int64_t count) {
    const hn::ScalableTag<int64_t> d;
    const size_t N = hn::Lanes(d);

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N) <= count; i += static_cast<int64_t>(N)) {
        MM_PREFETCH_T0(index + (i + 64) * IX_STRIDE);
        auto ts = hn::Zero(d);
        auto idx = hn::Zero(d);
        hn::LoadInterleaved2(d, index + i * IX_STRIDE, ts, idx);
        hn::StoreU(hn::GatherIndex(d, src, idx), d, dest + i);
    }

    // scalar tail
    for (; i < count; i++) {
        dest[i] = src[index[i * IX_STRIDE + IX_I]];
    }
}

// ---------------------------------------------------------------------------
// 5: re_shuffle_int32
// Process 2*N64 elements per iteration by doing two LoadInterleaved2 calls,
// narrowing to int32 with OrderedDemote2To, and a full-width int32 gather.
// On AVX2 this gives 8-wide vpgatherdd, matching the VCL baseline throughput.
//
// vpgatherdd uses signed 32-bit indices, so row IDs >= INT32_MAX cannot be
// gathered. A runtime check falls back to scalar indexing for those chunks,
// matching the VCL baseline's horizontal_max guard in lookup_idx8.
// ---------------------------------------------------------------------------
void ReShuffleInt32(const int32_t *src, int32_t *dest, const int64_t *index, const int64_t count) {
    const hn::ScalableTag<int64_t> d64;
    const hn::Repartition<int32_t, decltype(d64)> d32;
    const size_t N64 = hn::Lanes(d64);
    const size_t N32 = N64 * 2;

    int64_t i = 0;
    for (; i + static_cast<int64_t>(N32) <= count; i += static_cast<int64_t>(N32)) {
        MM_PREFETCH_T0(index + (i + 64) * IX_STRIDE);

        // Load two batches of N64 interleaved {ts, idx} pairs
        auto ts_lo = hn::Zero(d64);
        auto idx64_lo = hn::Zero(d64);
        hn::LoadInterleaved2(d64, index + i * IX_STRIDE, ts_lo, idx64_lo);

        auto ts_hi = hn::Zero(d64);
        auto idx64_hi = hn::Zero(d64);
        hn::LoadInterleaved2(d64, index + (i + static_cast<int64_t>(N64)) * IX_STRIDE, ts_hi, idx64_hi);

        // vpgatherdd uses signed 32-bit indices — check all row IDs fit
        const auto max_idx = hn::GetLane(hn::MaxOfLanes(d64, hn::Max(idx64_lo, idx64_hi)));
        if (max_idx < INT32_MAX) {
            const auto idx32 = hn::OrderedDemote2To(d32, idx64_lo, idx64_hi);
            hn::StoreU(hn::GatherIndex(d32, src, idx32), d32, dest + i);
        } else {
            for (int64_t j = 0; j < static_cast<int64_t>(N32); j++) {
                dest[i + j] = src[index[(i + j) * IX_STRIDE + IX_I]];
            }
        }
    }

    // Half-width tail: process N64 remaining elements
    if (i + static_cast<int64_t>(N64) <= count) {
        const hn::Rebind<int32_t, decltype(d64)> d32h;
        auto ts = hn::Zero(d64);
        auto idx64 = hn::Zero(d64);
        hn::LoadInterleaved2(d64, index + i * IX_STRIDE, ts, idx64);
        if (hn::GetLane(hn::MaxOfLanes(d64, idx64)) < INT32_MAX) {
            hn::StoreU(hn::GatherIndex(d32h, src, hn::DemoteTo(d32h, idx64)), d32h, dest + i);
        } else {
            for (int64_t j = 0; j < static_cast<int64_t>(N64); j++) {
                dest[i + j] = src[index[(i + j) * IX_STRIDE + IX_I]];
            }
        }
        i += static_cast<int64_t>(N64);
    }

    // Scalar tail
    for (; i < count; i++) {
        dest[i] = src[index[i * IX_STRIDE + IX_I]];
    }
}

// ---------------------------------------------------------------------------
// 32: re_shuffle_128bit
// ---------------------------------------------------------------------------
void ReShuffle128Bit(const __int128 *src, __int128 *dest, const index_t *index, const int64_t count) {
    re_shuffle_vanilla(src, dest, index, count);
}

// ---------------------------------------------------------------------------
// 30: re_shuffle_256bit
// ---------------------------------------------------------------------------
void ReShuffle256Bit(const long_256bit *src, long_256bit *dest, const index_t *index, const int64_t count) {
    re_shuffle_vanilla(src, dest, index, count);
}

// ---------------------------------------------------------------------------
// 0: merge_copy_var_column_int32
// Variable-length column merge using int32 lengths. Scalar implementation.
// ---------------------------------------------------------------------------
template<typename T>
inline void MergeCopyVarColumn(
        index_t *merge_index,
        int64_t merge_index_size,
        int64_t *src_data_fix,
        char *src_data_var,
        int64_t *src_ooo_fix,
        char *src_ooo_var,
        int64_t *dst_fix,
        char *dst_var,
        int64_t dst_var_offset,
        T mult
) {
    int64_t *src_fix[] = {src_ooo_fix, src_data_fix};
    char *src_var[] = {src_ooo_var, src_data_var};

    for (int64_t l = 0; l < merge_index_size; l++) {
        MM_PREFETCH_T0(merge_index + l + 64);
        dst_fix[l] = dst_var_offset;
        const uint64_t row = merge_index[l].i;
        const uint32_t bit = (row >> 63);
        const uint64_t rr = row & ~(1ULL << 63);
        const int64_t offset = src_fix[bit][rr];
        char *src_var_ptr = src_var[bit] + offset;
        auto len = *reinterpret_cast<T *>(src_var_ptr);
        auto char_count = len > 0 ? len * mult : 0;
        reinterpret_cast<T *>(dst_var + dst_var_offset)[0] = len;
        __MEMCPY(dst_var + dst_var_offset + sizeof(T), src_var_ptr + sizeof(T), char_count);
        dst_var_offset += char_count + sizeof(T);
    }
    if (merge_index_size > 0) {
        dst_fix[merge_index_size] = dst_var_offset;
    }
}

void MergeCopyVarColumnInt32(
        index_t *merge_index,
        int64_t merge_index_size,
        int64_t *src_data_fix,
        char *src_data_var,
        int64_t *src_ooo_fix,
        char *src_ooo_var,
        int64_t *dst_fix,
        char *dst_var,
        int64_t dst_var_offset
) {
    MergeCopyVarColumn<int32_t>(merge_index, merge_index_size, src_data_fix, src_data_var,
                                src_ooo_fix, src_ooo_var, dst_fix, dst_var, dst_var_offset, 2);
}

// ---------------------------------------------------------------------------
// 3: merge_copy_var_column_int64
// ---------------------------------------------------------------------------
void MergeCopyVarColumnInt64(
        index_t *merge_index,
        int64_t merge_index_size,
        int64_t *src_data_fix,
        char *src_data_var,
        int64_t *src_ooo_fix,
        char *src_ooo_var,
        int64_t *dst_fix,
        char *dst_var,
        int64_t dst_var_offset
) {
    MergeCopyVarColumn<int64_t>(merge_index, merge_index_size, src_data_fix, src_data_var,
                                src_ooo_fix, src_ooo_var, dst_fix, dst_var, dst_var_offset, 1);
}

// ---------------------------------------------------------------------------
// 31: merge_copy_varchar_column
// VARCHAR column merge. Scalar implementation.
// ---------------------------------------------------------------------------
void MergeCopyVarcharColumn(
        index_t *merge_index,
        int64_t merge_index_size,
        int64_t *src_data_fix,
        char *src_data_var,
        int64_t *src_ooo_fix,
        char *src_ooo_var,
        int64_t *dst_fix,
        char *dst_var,
        int64_t dst_var_offset
) {
    int64_t *src_fix[] = {src_ooo_fix, src_data_fix};
    char *src_var[] = {src_ooo_var, src_data_var};

    for (int64_t l = 0; l < merge_index_size; l++) {
        const uint64_t row = merge_index[l].i;
        const uint32_t bit = (row >> 63);
        const uint64_t rr = row & ~(1ULL << 63);
        const int64_t firstWord = src_fix[bit][rr * 2];
        const int64_t secondWord = src_fix[bit][rr * 2 + 1];

        auto originalData = secondWord & 0x000000000000ffffLL;
        auto relocatedSecondWord = originalData | (dst_var_offset << 16);
        if ((firstWord & 1) == 0 && (firstWord & 4) == 0) {
            // not inlined and not null
            auto originalOffset = secondWord >> 16;
            auto len = (firstWord >> 4) & 0xffffff;
            __MEMCPY(dst_var + dst_var_offset, src_var[bit] + originalOffset, len);
            dst_var_offset += len;
        }
        dst_fix[l * 2] = firstWord;
        dst_fix[l * 2 + 1] = relocatedSecondWord;
    }
}

// ---------------------------------------------------------------------------
// 32: merge_copy_array_column
// ARRAY column merge. Scalar implementation.
// ---------------------------------------------------------------------------
void MergeCopyArrayColumn(
        index_t *merge_index,
        int64_t merge_index_size,
        int64_t *src_data_fix,
        char *src_data_var,
        int64_t *src_ooo_fix,
        char *src_ooo_var,
        int64_t *dst_fix,
        char *dst_var,
        int64_t dst_var_offset
) {
    int64_t *src_fix[] = {src_ooo_fix, src_data_fix};
    char *src_var[] = {src_ooo_var, src_data_var};

    for (int64_t l = 0; l < merge_index_size; l++) {
        const uint64_t row = merge_index[l].i;
        const uint32_t bit = (row >> 63);
        const uint64_t rr = row & ~(1ULL << 63);
        const int64_t src_var_offset = src_fix[bit][rr * 2] & OFFSET_MAX;
        auto size = static_cast<uint32_t>(src_fix[bit][rr * 2 + 1] & ARRAY_SIZE_MAX);

        const auto relocated_var_offset = dst_var_offset & OFFSET_MAX;
        if (size > 0) {
            __MEMCPY(dst_var + dst_var_offset, src_var[bit] + src_var_offset, size);
            dst_var_offset += size;
        }
        dst_fix[l * 2] = relocated_var_offset;
        dst_fix[l * 2 + 1] = size;
    }
}

// ---------------------------------------------------------------------------
// Platform memory operations
// ---------------------------------------------------------------------------
void PlatformMemcpy(void *dst, const void *src, const size_t len) {
    __MEMCPY(dst, src, len);
}

void PlatformMemcmp(const void *a, const void *b, const size_t len, int *res) {
    *res = __MEMCMP(a, b, len);
}

void PlatformMemset(void *dst, const int val, const size_t len) {
    __MEMSET(dst, val, len);
}

void PlatformMemmove(void *dst, const void *src, const size_t len) {
    __MEMMOVE(dst, src, len);
}

}  // namespace HWY_NAMESPACE
}  // namespace questdb_ooo
HWY_AFTER_NAMESPACE();

// ===========================================================================
// Dispatch tables and C-linkage wrapper functions (compiled once)
// ===========================================================================

#if HWY_ONCE

namespace questdb_ooo {

HWY_EXPORT(CopyIndexTimestamp);
HWY_EXPORT(ShiftCopy);
HWY_EXPORT(ShiftCopyVarcharAux);
HWY_EXPORT(ShiftCopyArrayAux);
HWY_EXPORT(CopyIndex);
HWY_EXPORT(SetBinaryColumnNullRefs);
HWY_EXPORT(SetStringColumnNullRefs);
HWY_EXPORT(SetVarcharNullRefs);
HWY_EXPORT(SetArrayNullRefs);
HWY_EXPORT(SetMemoryVanillaInt64);
HWY_EXPORT(SetMemoryVanillaInt32);
HWY_EXPORT(SetMemoryVanillaDouble);
HWY_EXPORT(SetMemoryVanillaFloat);
HWY_EXPORT(SetMemoryVanillaShort);
HWY_EXPORT(SetMemoryVanillaInt128);
HWY_EXPORT(SetMemoryVanillaInt256);
HWY_EXPORT(MakeTimestampIndex);
HWY_EXPORT(ShiftTimestampIndex);
HWY_EXPORT(FlattenIndex);
HWY_EXPORT(MergeShuffleInt32);
HWY_EXPORT(MergeShuffleInt64);
HWY_EXPORT(ReShuffleInt64);
HWY_EXPORT(ReShuffleInt32);
HWY_EXPORT(ReShuffle128Bit);
HWY_EXPORT(ReShuffle256Bit);
HWY_EXPORT(MergeCopyVarColumnInt32);
HWY_EXPORT(MergeCopyVarColumnInt64);
HWY_EXPORT(MergeCopyVarcharColumn);
HWY_EXPORT(MergeCopyArrayColumn);
HWY_EXPORT(PlatformMemcpy);
HWY_EXPORT(PlatformMemcmp);
HWY_EXPORT(PlatformMemset);
HWY_EXPORT(PlatformMemmove);

}  // namespace questdb_ooo

using namespace questdb_ooo;

// C-linkage wrapper functions with the original symbol names.
// These are the symbols that ooo.cpp (and the rest of the codebase) call.
extern "C" {

void copy_index_timestamp(index_t *index, int64_t index_lo, int64_t index_hi, int64_t *dest) {
    HWY_DYNAMIC_DISPATCH(CopyIndexTimestamp)(reinterpret_cast<int64_t *>(index), index_lo, index_hi, dest);
}

void shift_copy(int64_t shift, const int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest) {
    HWY_DYNAMIC_DISPATCH(ShiftCopy)(shift, src, src_lo, src_hi, dest);
}

void shift_copy_varchar_aux(int64_t shift, const int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest) {
    HWY_DYNAMIC_DISPATCH(ShiftCopyVarcharAux)(shift, src, src_lo, src_hi, dest);
}

void shift_copy_array_aux(int64_t shift, const int64_t *src, int64_t src_lo, int64_t src_hi, int64_t *dest) {
    HWY_DYNAMIC_DISPATCH(ShiftCopyArrayAux)(shift, src, src_lo, src_hi, dest);
}

void copy_index(const index_t *index, const int64_t count, int64_t *dest) {
    HWY_DYNAMIC_DISPATCH(CopyIndex)(reinterpret_cast<const int64_t *>(index), count, dest);
}

void set_binary_column_null_refs(int64_t *data, int64_t offset, int64_t count) {
    HWY_DYNAMIC_DISPATCH(SetBinaryColumnNullRefs)(data, offset, count);
}

void set_string_column_null_refs(int64_t *data, int64_t offset, int64_t count) {
    HWY_DYNAMIC_DISPATCH(SetStringColumnNullRefs)(data, offset, count);
}

void set_varchar_null_refs(int64_t *aux, int64_t offset, int64_t count) {
    HWY_DYNAMIC_DISPATCH(SetVarcharNullRefs)(aux, offset, count);
}

void set_array_null_refs(int64_t *aux, int64_t offset, int64_t count) {
    HWY_DYNAMIC_DISPATCH(SetArrayNullRefs)(aux, offset, count);
}

void set_memory_vanilla_int64(int64_t *data, const int64_t value, const int64_t count) {
    HWY_DYNAMIC_DISPATCH(SetMemoryVanillaInt64)(data, value, count);
}

void set_memory_vanilla_int32(int32_t *data, const int32_t value, const int64_t count) {
    HWY_DYNAMIC_DISPATCH(SetMemoryVanillaInt32)(data, value, count);
}

void set_memory_vanilla_double(double *data, const double value, const int64_t count) {
    HWY_DYNAMIC_DISPATCH(SetMemoryVanillaDouble)(data, value, count);
}

void set_memory_vanilla_float(float *data, const float value, const int64_t count) {
    HWY_DYNAMIC_DISPATCH(SetMemoryVanillaFloat)(data, value, count);
}

void set_memory_vanilla_short(int16_t *data, const int16_t value, const int64_t count) {
    HWY_DYNAMIC_DISPATCH(SetMemoryVanillaShort)(data, value, count);
}

void set_memory_vanilla_int128(long_128bit *data, const long_128bit value, const int64_t count) {
    HWY_DYNAMIC_DISPATCH(SetMemoryVanillaInt128)(data, value, count);
}

void set_memory_vanilla_int256(long_256bit *data, const long_256bit value, const int64_t count) {
    HWY_DYNAMIC_DISPATCH(SetMemoryVanillaInt256)(data, value, count);
}

void make_timestamp_index(const int64_t *data, int64_t low, int64_t high, index_t *dest) {
    HWY_DYNAMIC_DISPATCH(MakeTimestampIndex)(data, low, high, reinterpret_cast<int64_t *>(dest));
}

void shift_timestamp_index(const index_t *data, int64_t count, index_t *dest) {
    HWY_DYNAMIC_DISPATCH(ShiftTimestampIndex)(reinterpret_cast<const int64_t *>(data), count,
                                               reinterpret_cast<int64_t *>(dest));
}

void flatten_index(index_t *index, int64_t count) {
    HWY_DYNAMIC_DISPATCH(FlattenIndex)(reinterpret_cast<int64_t *>(index), count);
}

void merge_shuffle_int32(const int32_t *src1, const int32_t *src2, int32_t *dest,
                         const index_t *index, const int64_t count) {
    HWY_DYNAMIC_DISPATCH(MergeShuffleInt32)(src1, src2, dest, index, count);
}

void merge_shuffle_int64(const int64_t *src1, const int64_t *src2, int64_t *dest,
                         const index_t *index, const int64_t count) {
    HWY_DYNAMIC_DISPATCH(MergeShuffleInt64)(src1, src2, dest, reinterpret_cast<const int64_t *>(index), count);
}

void re_shuffle_int64(const int64_t *src, int64_t *dest, const index_t *index, const int64_t count) {
    HWY_DYNAMIC_DISPATCH(ReShuffleInt64)(src, dest, reinterpret_cast<const int64_t *>(index), count);
}

void re_shuffle_int32(const int32_t *src, int32_t *dest, const index_t *index, const int64_t count) {
    HWY_DYNAMIC_DISPATCH(ReShuffleInt32)(src, dest, reinterpret_cast<const int64_t *>(index), count);
}

void re_shuffle_128bit(const __int128 *src, __int128 *dest, const index_t *index, const int64_t count) {
    HWY_DYNAMIC_DISPATCH(ReShuffle128Bit)(src, dest, index, count);
}

void re_shuffle_256bit(const long_256bit *src, long_256bit *dest, const index_t *index, const int64_t count) {
    HWY_DYNAMIC_DISPATCH(ReShuffle256Bit)(src, dest, index, count);
}

void merge_copy_var_column_int32(index_t *merge_index, int64_t merge_index_size,
                                 int64_t *src_data_fix, char *src_data_var,
                                 int64_t *src_ooo_fix, char *src_ooo_var,
                                 int64_t *dst_fix, char *dst_var, int64_t dst_var_offset) {
    HWY_DYNAMIC_DISPATCH(MergeCopyVarColumnInt32)(merge_index, merge_index_size,
            src_data_fix, src_data_var, src_ooo_fix, src_ooo_var, dst_fix, dst_var, dst_var_offset);
}

void merge_copy_var_column_int64(index_t *merge_index, int64_t merge_index_size,
                                 int64_t *src_data_fix, char *src_data_var,
                                 int64_t *src_ooo_fix, char *src_ooo_var,
                                 int64_t *dst_fix, char *dst_var, int64_t dst_var_offset) {
    HWY_DYNAMIC_DISPATCH(MergeCopyVarColumnInt64)(merge_index, merge_index_size,
            src_data_fix, src_data_var, src_ooo_fix, src_ooo_var, dst_fix, dst_var, dst_var_offset);
}

void merge_copy_varchar_column(index_t *merge_index, int64_t merge_index_size,
                               int64_t *src_data_fix, char *src_data_var,
                               int64_t *src_ooo_fix, char *src_ooo_var,
                               int64_t *dst_fix, char *dst_var, int64_t dst_var_offset) {
    HWY_DYNAMIC_DISPATCH(MergeCopyVarcharColumn)(merge_index, merge_index_size,
            src_data_fix, src_data_var, src_ooo_fix, src_ooo_var, dst_fix, dst_var, dst_var_offset);
}

void merge_copy_array_column(index_t *merge_index, int64_t merge_index_size,
                             int64_t *src_data_fix, char *src_data_var,
                             int64_t *src_ooo_fix, char *src_ooo_var,
                             int64_t *dst_fix, char *dst_var, int64_t dst_var_offset) {
    HWY_DYNAMIC_DISPATCH(MergeCopyArrayColumn)(merge_index, merge_index_size,
            src_data_fix, src_data_var, src_ooo_fix, src_ooo_var, dst_fix, dst_var, dst_var_offset);
}

void platform_memcpy(void *dst, const void *src, const size_t len) {
    HWY_DYNAMIC_DISPATCH(PlatformMemcpy)(dst, src, len);
}

void platform_memcmp(const void *a, const void *b, const size_t len, int *res) {
    HWY_DYNAMIC_DISPATCH(PlatformMemcmp)(a, b, len, res);
}

void platform_memset(void *dst, const int val, const size_t len) {
    HWY_DYNAMIC_DISPATCH(PlatformMemset)(dst, val, len);
}

void platform_memmove(void *dst, const void *src, const size_t len) {
    HWY_DYNAMIC_DISPATCH(PlatformMemmove)(dst, src, len);
}

}  // extern "C"

#endif  // HWY_ONCE
