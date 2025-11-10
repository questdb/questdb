/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
#include "jni.h"
#include <cstring>
#include <cassert>
#include "util.h"
#include "simd.h"
#include "ooo_dispatch.h"
#include <algorithm>
#include "ooo_radix.h"

#ifdef OOO_CPP_PROFILE_TIMING
#include <atomic>
#include <time.h>
#endif

typedef struct {
    uint64_t c8[256];
    uint64_t c7[256];
    uint64_t c6[256];
    uint64_t c5[256];
    uint64_t c4[256];
    uint64_t c3[256];
    uint64_t c2[256];
    uint64_t c1[256];
} rscounts_t;

struct long_3x {
    uint64_t l1;
    uint64_t l2;
    uint64_t l3;

    bool operator<=(const long_3x &other) const {
        if (l1 > other.l1) return false;
        if (l1 == other.l1) {
            if (l2 > other.l2) return false;
            if (l2 == other.l2) {
                if (l3 > other.l3) return false;
            }
        }
        return true;
    }
};

inline void radix_shuffle_ab(uint64_t *counts, const int64_t *srcA, const uint64_t sizeA, const index_l *srcB,
                             const uint64_t sizeB, index_t *dest, int64_t minValue, uint16_t sh) {
    MM_PREFETCH_T0(counts);
    for (uint64_t x = 0; x < sizeA; x++) {
        const uint64_t value = srcA[x] - minValue;
        const auto digit = (value >> sh) & 0xffu;
        dest[counts[digit]].ts = value;
        dest[counts[digit]].i = x | (1ull << 63);
        counts[digit]++;
        MM_PREFETCH_T2(srcA + x + 64);
    }

    for (uint64_t x = 0; x < sizeB; x++) {
        const uint64_t value = srcB[x].ts - minValue;
        const auto digit = (value >> sh) & 0xffu;
        dest[counts[digit]].ts = value;
        dest[counts[digit]].i = srcB[x].i;
        counts[digit]++;
        MM_PREFETCH_T2(srcB + x + 64);
    }
}

inline void
radix_shuffle_ab_one_pass(uint64_t *counts, const int64_t *src_a, const uint64_t size_a, const index_l *src_b,
                          const uint64_t size_b, index_t *dest, int64_t min_value) {
    MM_PREFETCH_T0(counts);
    for (uint64_t x = 0; x < size_a; x++) {
        const uint64_t value = src_a[x] - min_value;
        const auto digit = value & 0xffu;
        dest[counts[digit]].ts = value + min_value;
        dest[counts[digit]].i = x | (1ull << 63);
        counts[digit]++;
        MM_PREFETCH_T2(src_a + x + 64);
    }

    for (uint64_t x = 0; x < size_b; x++) {
        const uint64_t value = src_b[x].ts - min_value;
        const auto digit = value & 0xffu;
        dest[counts[digit]].ts = value + min_value;
        dest[counts[digit]].i = src_b[x].i;
        counts[digit]++;
        MM_PREFETCH_T2(src_b + x + 64);
    }
}

template<typename T>
void radix_sort_long_index_asc_in_place(T *array, uint64_t size, T *cpy) {
    rscounts_t counts;
    memset(&counts, 0, 256 * 8 * sizeof(uint64_t));
    uint64_t o8 = 0, o7 = 0, o6 = 0, o5 = 0, o4 = 0, o3 = 0, o2 = 0, o1 = 0;
    uint64_t t8, t7, t6, t5, t4, t3, t2, t1;
    uint64_t x;

    // calculate counts
    MM_PREFETCH_NTA(counts.c8);
    for (x = 0; x < size; x++) {
        t8 = array[x] & 0xffu;
        t7 = (array[x] >> 8u) & 0xffu;
        t6 = (array[x] >> 16u) & 0xffu;
        t5 = (array[x] >> 24u) & 0xffu;
        t4 = (array[x] >> 32u) & 0xffu;
        t3 = (array[x] >> 40u) & 0xffu;
        t2 = (array[x] >> 48u) & 0xffu;
        t1 = (array[x] >> 56u) & 0xffu;
        counts.c8[t8]++;
        counts.c7[t7]++;
        counts.c6[t6]++;
        counts.c5[t5]++;
        counts.c4[t4]++;
        counts.c3[t3]++;
        counts.c2[t2]++;
        counts.c1[t1]++;
        MM_PREFETCH_T2(array + x + 64);
    }

    // convert counts to offsets
    MM_PREFETCH_T0(&counts);
    for (x = 0; x < 256; x++) {
        t8 = o8 + counts.c8[x];
        t7 = o7 + counts.c7[x];
        t6 = o6 + counts.c6[x];
        t5 = o5 + counts.c5[x];
        t4 = o4 + counts.c4[x];
        t3 = o3 + counts.c3[x];
        t2 = o2 + counts.c2[x];
        t1 = o1 + counts.c1[x];
        counts.c8[x] = o8;
        counts.c7[x] = o7;
        counts.c6[x] = o6;
        counts.c5[x] = o5;
        counts.c4[x] = o4;
        counts.c3[x] = o3;
        counts.c2[x] = o2;
        counts.c1[x] = o1;
        o8 = t8;
        o7 = t7;
        o6 = t6;
        o5 = t5;
        o4 = t4;
        o3 = t3;
        o2 = t2;
        o1 = t1;
    }

    // radix
    radix_shuffle(counts.c8, array, cpy, size, 0u);
    radix_shuffle(counts.c7, cpy, array, size, 8u);
    radix_shuffle(counts.c6, array, cpy, size, 16u);
    radix_shuffle(counts.c5, cpy, array, size, 24u);
    radix_shuffle(counts.c4, array, cpy, size, 32u);
    radix_shuffle(counts.c3, cpy, array, size, 40u);
    radix_shuffle(counts.c2, array, cpy, size, 48u);
    radix_shuffle(counts.c1, cpy, array, size, 56u);
}
template<typename T1, typename T2>
inline bool arrays_dont_overlap(const T1 *a, size_t a_count, const T2 *b, size_t b_count) {
    return ((uintptr_t) a + a_count * sizeof(T1) <= (uintptr_t) b ||
            (uintptr_t) b + b_count * sizeof(T2) <= (uintptr_t) a);
}

template<uint16_t N>
void
radix_sort_ab_long_index_asc(const int64_t *arrayA, const uint64_t sizeA, const index_l *arrayB, const uint64_t sizeB,
                             index_l *out, index_l *cpy, int64_t minValue) {
    uint64_t counts[N][256] = {{0}};
    uint64_t x;

    // calculate counts
    for (x = 0; x < sizeA; x++) {
        uint64_t value = arrayA[x] - minValue;
        // should be unrolled by compiler, n is a compile time const
        constexpr_for<0, N, 1>(
                [&](auto i) {
                    constexpr uint64_t shift = 8u * (N - i - 1);
                    const auto t0 = (value >> shift) & 0xffu;
                    counts[i][t0]++;
                }
        );
        MM_PREFETCH_T2(arrayA + x + 64);
    }

    for (x = 0; x < sizeB; x++) {
        uint64_t value = arrayB[x].ts - minValue;
        // should be unrolled by compiler, n is a compile time const
        constexpr_for<0, N, 1>(
                [&](auto i) {
                    constexpr uint64_t shift = 8u * (N - i - 1);
                    const auto t0 = (value >> shift) & 0xffu;
                    counts[i][t0]++;
                }
        );
        MM_PREFETCH_T2(arrayB + x + 64);
    }

    // convert counts to offsets
    MM_PREFETCH_T0(&counts);
    uint64_t o[N] = {0};
    for (x = 0; x < 256; x++) {
        // should be unrolled by compiler, n is a compile time const
        constexpr_for<0, N, 1>(
                [&](auto i) {
                    auto t0 = o[i] + counts[i][x];
                    counts[i][x] = o[i];
                    o[i] = t0;
                }
        );
    }

    // radix
    auto size = sizeA + sizeB;
    auto *ucpy = (index_t *) cpy;
    auto *uout = (index_t *) out;

    if constexpr (N > 1) {
        radix_shuffle_ab(counts[N - 1], arrayA, sizeA, arrayB, sizeB, ucpy, minValue, 0u);

        if constexpr (N > 2) {
            radix_shuffle(counts[N - 2], ucpy, uout, size, 8u);
            if constexpr (N > 3) {
                radix_shuffle(counts[N - 3], uout, ucpy, size, 16u);
                if constexpr (N > 4) {
                    radix_shuffle(counts[N - 4], ucpy, uout, size, 24u);
                    if constexpr (N > 5) {
                        radix_shuffle(counts[N - 5], uout, ucpy, size, 32u);
                        if constexpr (N > 6) {
                            radix_shuffle(counts[N - 6], ucpy, uout, size, 40u);
                            if constexpr (N > 7) {
                                radix_shuffle(counts[N - 7], uout, ucpy, size, 48u);
                                radix_shuffle(counts[N - 8], ucpy, out, size, minValue, 56u);
                            } else {
                                radix_shuffle(counts[N - 7], uout, cpy, size, minValue, 48u);
                            }
                        } else {
                            radix_shuffle(counts[N - 6], ucpy, out, size, minValue, 40u);
                        }
                    } else {
                        radix_shuffle(counts[N - 5], uout, cpy, size, minValue, 32u);
                    }
                } else {
                    radix_shuffle(counts[N - 4], ucpy, out, size, minValue, 24u);
                }
            } else {
                radix_shuffle(counts[N - 3], uout, cpy, size, minValue, 16u);
            }
        } else if constexpr (N > 1) {
            radix_shuffle(counts[N - 2], ucpy, out, size, minValue, 8u);
        }

        if constexpr (N % 2 == 1) {
            __MEMCPY(out, cpy, size * sizeof(index_t));
        }
    } else {
        if (arrays_dont_overlap(arrayB, sizeB, out, size) && arrays_dont_overlap(arrayA, sizeA, out, size)) {
            radix_shuffle_ab_one_pass(counts[N - 1], arrayA, sizeA, arrayB, sizeB, uout, minValue);
        } else {
            radix_shuffle_ab_one_pass(counts[N - 1], arrayA, sizeA, arrayB, sizeB, ucpy, minValue);
            __MEMCPY(out, cpy, size * sizeof(index_t));
        }
    }
}


template<typename T>
inline void radix_sort_long_index_asc_in_place(T *array, uint64_t size) {
    auto *cpy = (T *) malloc(size * sizeof(T));
    radix_sort_long_index_asc_in_place(array, size, cpy);
    free(cpy);
}

template<typename T>
inline void swap(T *a, T *b) {
    const auto t = *a;
    *a = *b;
    *b = t;
}

/**
 * This function takes last element as pivot, places
 *  the pivot element at its correct position in sorted
 *   array, and places all smaller (smaller than pivot)
 *  to left of pivot and all greater elements to right
 *  of pivot
 *
 **/
template<typename T>
inline uint64_t partition(T *index, uint64_t low, uint64_t high) {
    const auto pivot = index[high];    // pivot
    auto i = (low - 1);  // Index of smaller element

    for (uint64_t j = low; j <= high - 1; j++) {
        // If current element is smaller than or
        // equal to pivot
        if (index[j] <= pivot) {
            i++;    // increment index of smaller element
            swap(&index[i], &index[j]);
        }
    }
    swap(&index[i + 1], &index[high]);
    return (i + 1);
}

/**
 * The main function that implements QuickSort
 * arr[] --> Array to be sorted,
 * low  --> Starting index,
 * high  --> Ending index
 **/
template<typename T>
void quick_sort_long_index_asc_in_place(T *arr, int64_t low, int64_t high) {
    if (low < high) {
        /* pi is partitioning index, arr[p] is now
           at right place */
        uint64_t pi = partition(arr, low, high);

        // Separately sort elements before
        // partition and after partition
        quick_sort_long_index_asc_in_place(arr, low, pi - 1);
        quick_sort_long_index_asc_in_place(arr, pi + 1, high);
    }
}

template<typename T>
inline void sort(T *index, int64_t size) {
    if (size < 600) {
        quick_sort_long_index_asc_in_place(index, 0, size - 1);
    } else {
        radix_sort_long_index_asc_in_place(index, size);
    }
}

typedef struct {
    uint64_t value;
    uint32_t index_index;
} loser_node_t;

typedef struct {
    index_t *index;
    uint64_t pos;
    uint64_t size;
} index_entry_t;

typedef struct {
    index_t *index;
    int64_t size;
} java_index_entry_t;

void k_way_merge_long_index(
        index_entry_t *indexes,
        const uint32_t entries_count,
        uint32_t sentinels_at_start,
        index_t *dest
) {

    // calculate size of the tree
    const uint32_t tree_size = entries_count * 2;
    uint64_t merged_index_pos = 0;
    uint32_t sentinels_left = entries_count - sentinels_at_start;

    loser_node_t tree[tree_size];

    // seed the bottom of the tree with index values
    for (uint32_t i = 0; i < entries_count; i++) {
        if (indexes[i].index != nullptr) {
            tree[entries_count + i].value = indexes[i].index->ts;
        } else {
            tree[entries_count + i].value = L_MAX;
        }
        tree[entries_count + i].index_index = entries_count + i;
    }

    // seed the entire tree from bottom up
    for (uint32_t i = tree_size - 1; i > 1; i -= 2) {
        uint32_t winner;
        if (tree[i].value < tree[i - 1].value) {
            winner = i;
        } else {
            winner = i - 1;
        }
        tree[i / 2] = tree[winner];
    }

    // take the first winner
    auto winner_index = tree[1].index_index;
    index_entry_t *winner = indexes + winner_index - entries_count;
    if (winner->pos < winner->size) {
        dest[merged_index_pos++] = winner->index[winner->pos];
    } else {
        sentinels_left--;
    }

    // full run
    while (sentinels_left > 0) {

        // back fill the winning index
        if (PREDICT_TRUE(++winner->pos < winner->size)) {
            tree[winner_index].value = winner->index[winner->pos].ts;
        } else {
            tree[winner_index].value = L_MAX;
            sentinels_left--;
        }

        if (sentinels_left == 0) {
            break;
        }

        MM_PREFETCH_NTA(tree);
        while (PREDICT_TRUE(winner_index > 1)) {
            const auto right_child = winner_index % 2 == 1 ? winner_index - 1 : winner_index + 1;
            const auto target = winner_index / 2;
            if (tree[winner_index].value < tree[right_child].value) {
                tree[target] = tree[winner_index];
            } else {
                tree[target] = tree[right_child];
            }
            winner_index = target;
        }
        winner_index = tree[1].index_index;
        winner = indexes + winner_index - entries_count;
        MM_PREFETCH_NTA(winner);
        dest[merged_index_pos++] = winner->index[winner->pos];
    }
}

DECLARE_DISPATCHER(make_timestamp_index);

void binary_merge_ts_long_index(
        const int64_t *timestamps,
        const int64_t timestampLo,
        const int64_t timestamps_count,
        const index_t *index,
        const int64_t index_count,
        index_t *dest
) {
    int64_t its = timestampLo, iidx = 0, r = 0;
    int64_t timestamps_hi = timestampLo + timestamps_count;

    while (its < timestamps_hi && iidx < index_count) {
        if (timestamps[its] <= (int64_t) index[iidx].ts) {
            dest[r].ts = timestamps[its];
            dest[r++].i = (1ull << 63) | its;
            its++;
        } else {
            dest[r++] = index[iidx++];
        }
    }

    make_timestamp_index(
            timestamps,
            its,
            timestamps_hi - 1,
            &dest[r]
    );

    memcpy(&dest[r], &index[iidx], (index_count - iidx) * sizeof(index_t));
}


#ifdef OOO_CPP_PROFILE_TIMING
const int perf_counter_length = 32;
std::atomic_ulong perf_counters[perf_counter_length];

uint64_t currentTimeNanos() {
    struct timespec timespec;
    clock_gettime(CLOCK_REALTIME, &timespec);
    return timespec.tv_sec * 1000000000L + timespec.tv_nsec;
}
#endif

template<typename T>
inline void measure_time(int index, T func) {
#ifdef OOO_CPP_PROFILE_TIMING
    auto start = currentTimeNanos();
    func();
    auto end = currentTimeNanos();
    perf_counters[index].fetch_add(end - start);
#else
    func();
#endif
}

extern "C" {

DECLARE_DISPATCHER(platform_memcpy);
JNIEXPORT void JNICALL Java_io_questdb_std_Vect_memcpy0
        (JNIEnv *e, jclass cl, jlong src, jlong dst, jlong len) {
    platform_memcpy(
            reinterpret_cast<void *>(dst),
            reinterpret_cast<void *>(src),
            __JLONG_REINTERPRET_CAST__(int64_t, len)
    );
}

DECLARE_DISPATCHER(platform_memcmp);
JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_memcmp
        (JNIEnv *e, jclass cl, jlong a, jlong b, jlong len) {
    int res;
    platform_memcmp(
            reinterpret_cast<void *>(a),
            reinterpret_cast<void *>(b),
            __JLONG_REINTERPRET_CAST__(int64_t, len),
            &res
    );
    return res;
}

DECLARE_DISPATCHER(platform_memmove);
JNIEXPORT void JNICALL Java_io_questdb_std_Vect_memmove
        (JNIEnv *e, jclass cl, jlong dst, jlong src, jlong len) {
    platform_memmove(
            reinterpret_cast<void *>(dst),
            reinterpret_cast<void *>(src),
            __JLONG_REINTERPRET_CAST__(int64_t, len)
    );
}

DECLARE_DISPATCHER(platform_memset);
JNIEXPORT void JNICALL Java_io_questdb_std_Vect_memset
        (JNIEnv *e, jclass cl, jlong dst, jlong len, jint value) {
    platform_memset(
            reinterpret_cast<void *>(dst),
            value,
            __JLONG_REINTERPRET_CAST__(int64_t, len)
    );
}

DECLARE_DISPATCHER(merge_copy_var_column_int32);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_oooMergeCopyStrColumn(JNIEnv *env, jclass cl,
                                               jlong merge_index,
                                               jlong merge_index_size,
                                               jlong src_data_fix,
                                               jlong src_data_var,
                                               jlong src_ooo_fix,
                                               jlong src_ooo_var,
                                               jlong dst_fix,
                                               jlong dst_var,
                                               jlong dst_var_offset) {
    measure_time(0, [=]() {
        merge_copy_var_column_int32(
                reinterpret_cast<index_t *>(merge_index),
                __JLONG_REINTERPRET_CAST__(int64_t, merge_index_size),
                reinterpret_cast<int64_t *>(src_data_fix),
                reinterpret_cast<char *>(src_data_var),
                reinterpret_cast<int64_t *>(src_ooo_fix),
                reinterpret_cast<char *>(src_ooo_var),
                reinterpret_cast<int64_t *>(dst_fix),
                reinterpret_cast<char *>(dst_var),
                __JLONG_REINTERPRET_CAST__(int64_t, dst_var_offset)
        );
    });
}

DECLARE_DISPATCHER(merge_copy_varchar_column);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_oooMergeCopyVarcharColumn(JNIEnv *env, jclass cl,
                                                   jlong merge_index,
                                                   jlong merge_index_size,
                                                   jlong src_data_fix,
                                                   jlong src_data_var,
                                                   jlong src_ooo_fix,
                                                   jlong src_ooo_var,
                                                   jlong dst_fix,
                                                   jlong dst_var,
                                                   jlong dst_var_offset) {
    measure_time(31, [=]() {
        merge_copy_varchar_column(
                reinterpret_cast<index_t *>(merge_index),
                __JLONG_REINTERPRET_CAST__(int64_t, merge_index_size),
                reinterpret_cast<int64_t *>(src_data_fix),
                reinterpret_cast<char *>(src_data_var),
                reinterpret_cast<int64_t *>(src_ooo_fix),
                reinterpret_cast<char *>(src_ooo_var),
                reinterpret_cast<int64_t *>(dst_fix),
                reinterpret_cast<char *>(dst_var),
                __JLONG_REINTERPRET_CAST__(int64_t, dst_var_offset)
        );
    });
}

DECLARE_DISPATCHER(merge_copy_array_column);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_oooMergeCopyArrayColumn(JNIEnv *env, jclass cl,
                                                 jlong merge_index,
                                                 jlong merge_index_size,
                                                 jlong src_data_fix,
                                                 jlong src_data_var,
                                                 jlong src_ooo_fix,
                                                 jlong src_ooo_var,
                                                 jlong dst_fix,
                                                 jlong dst_var,
                                                 jlong dst_var_offset) {
    measure_time(31, [=]() {
        merge_copy_array_column(
                reinterpret_cast<index_t *>(merge_index),
                __JLONG_REINTERPRET_CAST__(int64_t, merge_index_size),
                reinterpret_cast<int64_t *>(src_data_fix),
                reinterpret_cast<char *>(src_data_var),
                reinterpret_cast<int64_t *>(src_ooo_fix),
                reinterpret_cast<char *>(src_ooo_var),
                reinterpret_cast<int64_t *>(dst_fix),
                reinterpret_cast<char *>(dst_var),
                __JLONG_REINTERPRET_CAST__(int64_t, dst_var_offset)
        );
    });
}

// 1 oooMergeCopyStrColumnWithTop removed and now executed as Merge Copy without Top
// 2 oooMergeCopyBinColumnWithTop removed and now executed as Merge Copy without Top

DECLARE_DISPATCHER(merge_copy_var_column_int64);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_oooMergeCopyBinColumn(JNIEnv *env, jclass cl,
                                               jlong merge_index,
                                               jlong merge_index_size,
                                               jlong src_data_fix,
                                               jlong src_data_var,
                                               jlong src_ooo_fix,
                                               jlong src_ooo_var,
                                               jlong dst_fix,
                                               jlong dst_var,
                                               jlong dst_var_offset) {
    measure_time(3, [=]() {
        merge_copy_var_column_int64(
                reinterpret_cast<index_t *>(merge_index),
                __JLONG_REINTERPRET_CAST__(int64_t, merge_index_size),
                reinterpret_cast<int64_t *>(src_data_fix),
                reinterpret_cast<char *>(src_data_var),
                reinterpret_cast<int64_t *>(src_ooo_fix),
                reinterpret_cast<char *>(src_ooo_var),
                reinterpret_cast<int64_t *>(dst_fix),
                reinterpret_cast<char *>(dst_var),
                __JLONG_REINTERPRET_CAST__(int64_t, dst_var_offset)
        );
    });
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_sortLongIndexAscInPlace(JNIEnv *env, jclass cl, jlong pLong, jlong len) {
    measure_time(4, [=]() {
        sort<index_t>(reinterpret_cast<index_t *>(pLong), len);
    });
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_quickSortLongIndexAscInPlace(JNIEnv *env, jclass cl, jlong pLong, jlong len) {
    quick_sort_long_index_asc_in_place<index_t>(reinterpret_cast<index_t *>(pLong), 0, len - 1);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_radixSortLongIndexAscInPlace(JNIEnv *env, jclass cl, jlong pLong, jlong len, jlong pCpy) {
    radix_sort_long_index_asc_in_place<index_t>(reinterpret_cast<index_t *>(pLong), len,
                                                reinterpret_cast<index_t *>(pCpy));
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_radixSortABLongIndexAsc(JNIEnv *env, jclass cl, jlong pDataA, jlong countA, jlong pDataB,
                                                 jlong countB, jlong pDataOut, jlong pDataCpy, jlong minTimestamp,
                                                 jlong maxTimestamp) {
    auto minTs = __JLONG_REINTERPRET_CAST__(int64_t, minTimestamp);
    auto maxTs = __JLONG_REINTERPRET_CAST__(int64_t, maxTimestamp);

    if (minTs > maxTs) {
        // invalid min/max timestamp
        return -1;
    }

    if (minTs < 0 && maxTs > INT64_MAX + minTs) {
        // difference overflows 64 bits
        return -2;
    }

    auto ts_range_bytes = range_bytes(maxTs - minTs + 1);

    auto *a = reinterpret_cast<int64_t *>(pDataA);
    auto *b = reinterpret_cast<index_l *>(pDataB);
    auto *out = reinterpret_cast<index_l *>(pDataOut);
    auto *cpy = reinterpret_cast<index_l *>(pDataCpy);

    switch (ts_range_bytes) {
        case 0:
            // All rows same timestamp
            // Copy A and B to out
            // In case B is the same as out, use memmove and copy it first
            __MEMMOVE(&out[countA], b, countB * sizeof(index_l));
            for (int64_t l = 0; l < countA; l++) {
                out[l].ts = minTs;
                out[l].i = l | (1ull << 63);
            }
            break;
        case 1:
            radix_sort_ab_long_index_asc<1>(a, countA, b, countB, out, cpy, minTs);
            break;
        case 2:
            radix_sort_ab_long_index_asc<2>(a, countA, b, countB, out, cpy, minTs);
            break;
        case 3:
            radix_sort_ab_long_index_asc<3>(a, countA, b, countB, out, cpy, minTs);
            break;
        case 4:
            radix_sort_ab_long_index_asc<4>(a, countA, b, countB, out, cpy, minTs);
            break;
        case 5:
            radix_sort_ab_long_index_asc<5>(a, countA, b, countB, out, cpy, minTs);
            break;
        case 6:
            radix_sort_ab_long_index_asc<6>(a, countA, b, countB, out, cpy, minTs);
            break;
        case 7:
            radix_sort_ab_long_index_asc<7>(a, countA, b, countB, out, cpy, minTs);
            break;
        case 8:
            radix_sort_ab_long_index_asc<8>(a, countA, b, countB, out, cpy, minTs);
            break;
        default:
            return -3;
    }

    return countA + countB;
}


JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_radixSortLongIndexAsc(JNIEnv *env, jclass cl, jlong pLong, jlong len, jlong pCpy,
                                                             jlong min, jlong max) {
    return Java_io_questdb_std_Vect_radixSortABLongIndexAsc(
            env, cl,
            0, 0,
            pLong, len,
            pLong, pCpy,
            min, max
    );
}


JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_radixSortManySegmentsIndexAsc(
        JNIEnv *env,
        jclass cl,
        jlong pDataOut,
        jlong pDataCpy,
        jlong segmentAddresses,
        jint segmentCount,
        jlong txnInfo,
        jlong txnCount,
        jlong maxSegmentRowCount,
        jlong lagTsAddr,
        jlong lagRowCount,
        jlong minTimestamp,
        jlong maxTimestamp,
        jlong totalRowCount,
        jbyte resultFormat
) {
    auto *out = reinterpret_cast<index_l *>(pDataOut);
    auto *cpy = reinterpret_cast<index_l *>(pDataCpy);
    auto **segment_map_addresses = reinterpret_cast<const index_l **>(segmentAddresses);
    auto *lag_ts_addr = reinterpret_cast<const int64_t *>(lagTsAddr);
    auto *txn_info_addr = reinterpret_cast<const txn_info *>(txnInfo);
    auto txn_count = __JLONG_REINTERPRET_CAST__(int64_t, txnCount);
    uint8_t result_format = resultFormat;

    auto segment_count = (uint32_t) segmentCount;
    auto max_segment_row_count = __JLONG_REINTERPRET_CAST__(int64_t, maxSegmentRowCount);
    auto min_ts = __JLONG_REINTERPRET_CAST__(int64_t, minTimestamp);
    auto max_ts = __JLONG_REINTERPRET_CAST__(int64_t, maxTimestamp);
    auto lag_row_count = __JLONG_REINTERPRET_CAST__(int64_t, lagRowCount);
    auto total_row_count = __JLONG_REINTERPRET_CAST__(int64_t, totalRowCount);

    // Add 1 so since 1 value is needed for deduplication to indicate row not used
    auto total_row_count_bytes = integral_type_bytes(range_bytes(total_row_count + 1));

    auto ts_range_bytes = range_bytes(max_ts - min_ts + 1);
    auto txn_bytes = range_bytes(txn_count);

    // Check that ts + seq_txn fits 64 bits
    if (ts_range_bytes + txn_bytes > 8 || ts_range_bytes < 0) {
        return merge_index_format(error_sort_timestamp_txn_range_overflow, 0, 0, 0);
    }

    auto row_count_range_bytes = range_bytes(std::max(max_segment_row_count, lag_row_count));
    auto segments_range_bytes = range_bytes(segment_count + (lag_row_count > 0));

    // Check that segment index + segment row offset fits 64 bits
    if (row_count_range_bytes + segments_range_bytes > 8 || row_count_range_bytes < 0 || segments_range_bytes < 0) {
        return merge_index_format(error_sort_segment_index_offset_range_overflow, 0, 0, 0);
    }

    // Check that total rows fits 64 bits
    if (total_row_count_bytes > 8) {
        return merge_index_format(error_sort_row_count_overflow, 0, 0, 0);
    }

    auto sorted_count = radix_sort_segments_index_asc_precompiled(
            ts_range_bytes * 8u, txn_bytes * 8u, segments_range_bytes * 8u,
            lag_ts_addr, lag_row_count,
            segment_map_addresses, txn_info_addr, txn_count, out, cpy,
            segment_count,
            min_ts,
            total_row_count_bytes,
            total_row_count,
            result_format
    );

    return merge_index_format((int64_t) sorted_count, total_row_count_bytes, segments_range_bytes, result_format);
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_mergeShuffleFixedColumnFromManyAddresses(
        JNIEnv *env,
        jclass cl,
        jint columnSizeBytes,
        jlong indexFormat,
        jlong srcAddresses,
        jlong dstAddress,
        jlong mergeIndex,
        jlong segmentAddressPtr,
        jlong segmentCount
) {
    auto column_size_bytes = (int32_t) columnSizeBytes;
    auto reverse_index_format_bytes = read_reverse_index_format_bytes(indexFormat);
    auto src_addresses = reinterpret_cast<const void **>(srcAddresses);
    auto dst_address = reinterpret_cast<void *>(dstAddress);
    auto segment_address_ptr = reinterpret_cast<const seg_info *>(segmentAddressPtr);
    auto segment_count = __JLONG_REINTERPRET_CAST__(int64_t, segmentCount);
    auto reverse_index = read_reverse_index_ptr(mergeIndex, indexFormat);
    auto merge_index_format = read_format(indexFormat);

    switch (column_size_bytes) {
        case 1:
            return merge_shuffle_fixed_columns_by_rev_index_from_many_addresses<uint8_t>(
                    reverse_index_format_bytes, src_addresses,
                    dst_address, reverse_index,
                    segment_address_ptr, segment_count,
                    merge_index_format
            );
        case 2:
            return merge_shuffle_fixed_columns_by_rev_index_from_many_addresses<uint16_t>(
                    reverse_index_format_bytes, src_addresses,
                    dst_address, reverse_index,
                    segment_address_ptr, segment_count,
                    merge_index_format
            );
        case 4:
            return merge_shuffle_fixed_columns_by_rev_index_from_many_addresses<uint32_t>(
                    reverse_index_format_bytes, src_addresses,
                    dst_address, reverse_index,
                    segment_address_ptr, segment_count,
                    merge_index_format
            );
        case 8:
            return merge_shuffle_fixed_columns_by_rev_index_from_many_addresses<uint64_t>(
                    reverse_index_format_bytes, src_addresses,
                    dst_address, reverse_index,
                    segment_address_ptr, segment_count,
                    merge_index_format
            );
        case 16:
            return merge_shuffle_fixed_columns_by_rev_index_from_many_addresses<__int128>(
                    reverse_index_format_bytes, src_addresses,
                    dst_address, reverse_index,
                    segment_address_ptr, segment_count,
                    merge_index_format
            );
        case 32:
            return merge_shuffle_fixed_columns_by_rev_index_from_many_addresses<long_256bit>(
                    reverse_index_format_bytes, src_addresses,
                    dst_address, reverse_index,
                    segment_address_ptr, segment_count,
                    merge_index_format
            );
        default:
            // Error, unsupported column size
            return -1;
    }
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_mergeShuffleStringColumnFromManyAddresses(
        JNIEnv *env,
        jclass cl,
        jlong indexFormat,
        jint dataLengthBytes,
        jlong srcPrimaryAddresses,
        jlong srcSecondaryAddresses,
        jlong dstPrimaryAddress,
        jlong dstSecondaryAddress,
        jlong mergeIndex,
        jlong dstVarOffset,
        jlong dstVarSize
) {
    auto merge_index_address = reinterpret_cast<const index_l *>(mergeIndex);
    auto src_primary = reinterpret_cast<const char **>(srcPrimaryAddresses);
    auto src_secondary = reinterpret_cast<const int64_t **>(srcSecondaryAddresses);
    auto dst_primary = reinterpret_cast<char *>(dstPrimaryAddress);
    auto dst_secondary = reinterpret_cast<int64_t *>(dstSecondaryAddress);
    auto dst_var_offset = __JLONG_REINTERPRET_CAST__(int64_t, dstVarOffset);
    auto row_count = read_row_count(indexFormat);
    auto index_segment_encoding_bytes = read_segment_bytes(indexFormat);
    auto format = read_format(indexFormat);
    auto dst_var_size = __JLONG_REINTERPRET_CAST__(int64_t, dstVarSize);

    if (format != shuffle_index_format && format != dedup_shuffle_index_format) {
        // Error, invalid format
        return -2;
    }

    int64_t dst_var_end_offset;
    switch (dataLengthBytes) {
        case 4:
            dst_var_end_offset = merge_shuffle_string_column_from_many_addresses<int32_t>(
                    index_segment_encoding_bytes * 8, src_primary,
                    src_secondary, dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset,
                    2u, dst_var_size
            );
            break;
        case 8:
            dst_var_end_offset = merge_shuffle_string_column_from_many_addresses<int64_t>(
                    index_segment_encoding_bytes * 8, src_primary,
                    src_secondary, dst_primary, dst_secondary,
                    merge_index_address, row_count,
                    dst_var_offset,
                    1u, dst_var_size
            );
            break;
        default:
            // Error, invalid data length
            return -1;
    }

    if (dst_var_end_offset < 0) {
        // Error occurred, this is error code
        return dst_var_end_offset;
    }

    return row_count;
}


JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_mergeShuffleVarcharColumnFromManyAddresses(
        JNIEnv *env,
        jclass cl,
        jlong indexFormat,
        jlong srcPrimaryAddresses,
        jlong srcSecondaryAddresses,
        jlong dstPrimaryAddress,
        jlong dstSecondaryAddress,
        jlong mergeIndex,
        jlong dstVarOffset,
        jlong dstDataSize
) {
    auto merge_index_address = reinterpret_cast<const index_l *>(mergeIndex);
    auto src_primary = reinterpret_cast<const char **>(srcPrimaryAddresses);
    auto src_secondary = reinterpret_cast<const int64_t **>(srcSecondaryAddresses);
    auto dst_primary = reinterpret_cast<char *>(dstPrimaryAddress);
    auto dst_secondary = reinterpret_cast<int64_t *>(dstSecondaryAddress);
    auto dst_var_offset = __JLONG_REINTERPRET_CAST__(int64_t, dstVarOffset);
    auto dst_data_size = __JLONG_REINTERPRET_CAST__(int64_t, dstDataSize);
    auto row_count = read_row_count(indexFormat);
    auto index_segment_encoding_bytes = read_segment_bytes(indexFormat);
    auto format = read_format(indexFormat);

    if (format != shuffle_index_format && format != dedup_shuffle_index_format) {
        // Error, invalid format
        return -2;
    }

    int64_t end_dst_var_offset = merge_shuffle_varchar_column_from_many_addresses(
            src_primary, src_secondary,
            dst_primary, dst_secondary,
            merge_index_address, row_count,
            dst_var_offset,
            index_segment_encoding_bytes * 8u,
            dst_data_size
    );
    if (end_dst_var_offset < 0) {
        // Error occurred, this is error code
        return end_dst_var_offset;
    }

    return row_count;
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_mergeShuffleArrayColumnFromManyAddresses(
        JNIEnv *env,
        jclass cl,
        jlong indexFormat,
        jlong srcPrimaryAddresses,
        jlong srcSecondaryAddresses,
        jlong dstPrimaryAddress,
        jlong dstSecondaryAddress,
        jlong mergeIndex,
        jlong dstVarOffset,
        jlong dstDataSize
) {
    auto merge_index_address = reinterpret_cast<const index_l *>(mergeIndex);
    auto src_primary = reinterpret_cast<const char **>(srcPrimaryAddresses);
    auto src_secondary = reinterpret_cast<const int64_t **>(srcSecondaryAddresses);
    auto dst_primary = reinterpret_cast<char *>(dstPrimaryAddress);
    auto dst_secondary = reinterpret_cast<int64_t *>(dstSecondaryAddress);
    auto dst_var_offset = __JLONG_REINTERPRET_CAST__(int64_t, dstVarOffset);
    auto dst_data_size = __JLONG_REINTERPRET_CAST__(int64_t, dstDataSize);
    auto row_count = read_row_count(indexFormat);
    auto index_segment_encoding_bytes = read_segment_bytes(indexFormat);
    auto format = read_format(indexFormat);

    if (format != shuffle_index_format && format != dedup_shuffle_index_format) {
        // Error, invalid format
        return -2;
    }

    int64_t end_dst_var_offset = merge_shuffle_array_column_from_many_addresses(
            src_primary, src_secondary,
            dst_primary, dst_secondary,
            merge_index_address, row_count,
            dst_var_offset,
            index_segment_encoding_bytes * 8u,
            dst_data_size
    );
    if (end_dst_var_offset < 0) {
        // Error occurred, this is error code
        return end_dst_var_offset;
    }

    return row_count;
}


JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_mergeShuffleSymbolColumnFromManyAddresses(
        JNIEnv *env,
        jclass cl,
        jlong indexFormat,
        jlong srcAddresses,
        jlong dstAddress,
        jlong mergeIndex,
        jlong txnInfo,
        jlong txnCount,
        jlong symbolMap,
        jlong symbolMapSize
) {
    auto src = reinterpret_cast<const int32_t **>(srcAddresses);
    auto dst = reinterpret_cast<int32_t *>(dstAddress);
    auto *txn_info_addr = reinterpret_cast<const txn_info *>(txnInfo);
    auto txn_count = __JLONG_REINTERPRET_CAST__(int64_t, txnCount);
    auto symbol_map = reinterpret_cast<const int32_t *>(symbolMap);
    auto row_index_bytes = read_reverse_index_format_bytes(indexFormat);
    auto reverse_index_ptr = read_reverse_index_ptr(mergeIndex, indexFormat);
    assertm(reverse_index_ptr != nullptr, "reverse index ptr is null, invalid merge index");
    auto format = read_format(indexFormat);
    int64_t reverse_index_row_count = read_reverse_index_row_count(mergeIndex, indexFormat);

    jlong rows_processed;

    switch (row_index_bytes) {
        case 1:
            rows_processed = merge_shuffle_symbol_column_from_many_addresses<uint8_t>(
                    src, dst,
                    txn_info_addr, txn_count, symbol_map,
                    reverse_index_ptr,
                    reverse_index_row_count,
                    format
            );
            break;
        case 2:
            rows_processed = merge_shuffle_symbol_column_from_many_addresses<uint16_t>(
                    src, dst,
                    txn_info_addr, txn_count, symbol_map,
                    reverse_index_ptr,
                    reverse_index_row_count,
                    format
            );
            break;
        case 4:
            rows_processed = merge_shuffle_symbol_column_from_many_addresses<uint32_t>(
                    src, dst,
                    txn_info_addr, txn_count, symbol_map,
                    reverse_index_ptr,
                    reverse_index_row_count,
                    format
            );
            break;
        case 8:
            rows_processed = merge_shuffle_symbol_column_from_many_addresses<uint64_t>(
                    src, dst,
                    txn_info_addr, txn_count, symbol_map,
                    reverse_index_ptr,
                    reverse_index_row_count,
                    format
            );
            break;
        default:
            // Error, unsupported row index bytes
            return -1;
    }


    assertm(rows_processed <= reverse_index_row_count, "rows processed does not match reverse index row count");
    return rows_processed;
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_shuffleSymbolColumnByReverseIndex(
        JNIEnv *env,
        jclass cl,
        jlong indexFormat,
        jlong srcAddresses,
        jlong dstAddress,
        jlong mergeIndex
) {
    auto src = reinterpret_cast<const int32_t *>(srcAddresses);
    auto dst = reinterpret_cast<int32_t *>(dstAddress);
    auto row_index_bytes = read_reverse_index_format_bytes(indexFormat);
    auto reverse_index_ptr = read_reverse_index_ptr(mergeIndex, indexFormat);
    auto merge_format = read_format(indexFormat);
    auto rev_index_row_count = read_reverse_index_row_count(mergeIndex, indexFormat);

    switch (row_index_bytes) {
        case 1:
            return merge_shuffle_symbol_column_by_reverse_index<uint8_t>(
                    src, dst,
                    reverse_index_ptr,
                    rev_index_row_count,
                    merge_format
            );
        case 2:
            return merge_shuffle_symbol_column_by_reverse_index<uint16_t>(
                    src, dst,
                    reverse_index_ptr,
                    rev_index_row_count,
                    merge_format
            );
        case 4:
            return merge_shuffle_symbol_column_by_reverse_index<uint32_t>(
                    src, dst,
                    reverse_index_ptr,
                    rev_index_row_count,
                    merge_format
            );
        case 8:
            return merge_shuffle_symbol_column_by_reverse_index<uint64_t>(
                    src, dst,
                    reverse_index_ptr,
                    rev_index_row_count,
                    merge_format
            );
        default:
            // Error, unsupported row index bytes
            return -1;
    }
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_remapSymbolColumnFromManyAddresses(
        JNIEnv *env,
        jclass cl,
        jlong srcAddresses,
        jlong dstAddress,
        jlong txnInfo,
        jlong txnCount,
        jlong symbolMap
) {
    auto src = reinterpret_cast<const int32_t **>(srcAddresses);
    auto dst = reinterpret_cast<int32_t *>(dstAddress);
    auto *txn_info_addr = reinterpret_cast<const txn_info *>(txnInfo);
    auto txn_count = __JLONG_REINTERPRET_CAST__(int64_t, txnCount);
    auto symbol_map = reinterpret_cast<const int32_t *>(symbolMap);

    size_t out_index = 0;
    jlong rows_processed = 0;
    for (int64_t txn_index = 0; txn_index < txn_count; txn_index++) {
        auto segment_addr = src[txn_info_addr[txn_index].seg_info_index];
        uint64_t hi = txn_info_addr[txn_index].segment_row_offset + txn_info_addr[txn_index].row_count;
        int32_t clean_symbol_count = symbol_map[2 * txn_index];
        int32_t map_offset = symbol_map[2 * txn_index + 1];

        for (uint64_t seg_row = txn_info_addr[txn_index].segment_row_offset; seg_row < hi; seg_row++, out_index++) {
            int32_t value = segment_addr[seg_row];
            if (value >= clean_symbol_count) {
                auto value2 = symbol_map[map_offset + value - clean_symbol_count];
                dst[out_index] = value2;
            } else {
                dst[out_index] = value;
            }
            rows_processed++;
        }
    }
    return rows_processed;
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_sortULongAscInPlace(JNIEnv *env, jclass cl, jlong pLong, jlong len) {
    sort<uint64_t>(reinterpret_cast<uint64_t *>(pLong), len);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_sort128BitAscInPlace(JNIEnv *env, jclass cl, jlong pLong, jlong len) {
    quick_sort_long_index_asc_in_place<__int128>(reinterpret_cast<__int128 *>(pLong), 0, len - 1);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_sort3LongAscInPlace(JNIEnv *env, jclass cl, jlong pLong, jlong count) {
    quick_sort_long_index_asc_in_place<long_3x>(reinterpret_cast<long_3x *>(pLong), 0, count - 1);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeLongIndexesAscInner(JAVA_STATIC, jlong pIndexStructArray, jint cnt,
                                                  jlong mergedIndex) {
    // prepare merge entries
    // they need to have mutable current position "pos" in index

    if (cnt < 2) {
        return;
    }

    auto count = static_cast<uint32_t>(cnt);
    const java_index_entry_t *java_entries = reinterpret_cast<java_index_entry_t *>(pIndexStructArray);
    auto *merged_index = reinterpret_cast<index_t *>(mergedIndex);

    uint32_t size = ceil_pow_2(count);
    index_entry_t entries[size];
    for (uint32_t i = 0; i < count; i++) {
        entries[i].index = java_entries[i].index;
        entries[i].pos = 0;
        entries[i].size = java_entries[i].size;
    }

    if (count < size) {
        for (uint32_t i = count; i < size; i++) {
            entries[i].index = nullptr;
            entries[i].pos = 0;
            entries[i].size = -1;
        }
    }

    k_way_merge_long_index(entries, size, size - count, merged_index);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeTwoLongIndexesAsc(
        JAVA_STATIC, jlong pTs, jlong tsIndexLo, jlong tsCount, jlong pIndex, jlong jIndexCount, jlong pIndexDest) {
    binary_merge_ts_long_index(
            reinterpret_cast<int64_t *>(pTs),
            (int64_t) tsIndexLo,
            (int64_t) tsCount,
            reinterpret_cast<index_t *>(pIndex),
            (int64_t) jIndexCount,
            reinterpret_cast<index_t *>(pIndexDest)
    );
}

DECLARE_DISPATCHER(re_shuffle_int32);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_indexReshuffle32Bit(JNIEnv *env, jclass cl, jlong pSrc, jlong pDest, jlong pIndex,
                                             jlong count) {
    measure_time(5, [=]() {
        re_shuffle_int32(
                reinterpret_cast<int32_t *>(pSrc),
                reinterpret_cast<int32_t *>(pDest),
                reinterpret_cast<index_t *>(pIndex),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

DECLARE_DISPATCHER(re_shuffle_int64);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_indexReshuffle64Bit(JNIEnv *env, jclass cl, jlong pSrc, jlong pDest, jlong pIndex,
                                             jlong count) {
    measure_time(6, [=]() {
        re_shuffle_int64(
                reinterpret_cast<int64_t *>(pSrc),
                reinterpret_cast<int64_t *>(pDest),
                reinterpret_cast<index_t *>(pIndex),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

DECLARE_DISPATCHER(re_shuffle_128bit);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_indexReshuffle128Bit(JNIEnv *env, jclass cl, jlong pSrc, jlong pDest, jlong pIndex,
                                              jlong count) {
    measure_time(32, [=]() {
        re_shuffle_128bit(
                reinterpret_cast<__int128 *>(pSrc),
                reinterpret_cast<__int128 *>(pDest),
                reinterpret_cast<index_t *>(pIndex),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

DECLARE_DISPATCHER(re_shuffle_256bit);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_indexReshuffle256Bit(JNIEnv *env, jclass cl, jlong pSrc, jlong pDest, jlong pIndex,
                                              jlong count) {
    measure_time(30, [=]() {
        re_shuffle_256bit(
                reinterpret_cast<long_256bit *>(pSrc),
                reinterpret_cast<long_256bit *>(pDest),
                reinterpret_cast<index_t *>(pIndex),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

JNIEXPORT void JNICALL
// Leave vanilla
Java_io_questdb_std_Vect_indexReshuffle16Bit(JNIEnv *env, jclass cl, jlong pSrc, jlong pDest, jlong pIndex,
                                             jlong count) {
    measure_time(7, [=]() {
        re_shuffle_vanilla<int16_t>(
                reinterpret_cast<int16_t *>(pSrc),
                reinterpret_cast<int16_t *>(pDest),
                reinterpret_cast<index_t *>(pIndex),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_indexReshuffle8Bit(JNIEnv *env, jclass cl, jlong pSrc, jlong pDest, jlong pIndex,
                                            jlong count) {
    measure_time(8, [=]() {
        re_shuffle_vanilla<int8_t>(
                reinterpret_cast<int8_t *>(pSrc),
                reinterpret_cast<int8_t *>(pDest),
                reinterpret_cast<index_t *>(pIndex),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeShuffle8Bit(JNIEnv *env, jclass cl, jlong src1, jlong src2, jlong dest, jlong index,
                                          jlong count) {
    measure_time(9, [=]() {
        merge_shuffle_vanilla<int8_t>(
                reinterpret_cast<int8_t *>(src1),
                reinterpret_cast<int8_t *>(src2),
                reinterpret_cast<int8_t *>(dest),
                reinterpret_cast<index_t *>(index),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeShuffle16Bit(JNIEnv *env, jclass cl, jlong src1, jlong src2, jlong dest, jlong index,
                                           jlong count) {
    measure_time(10, [=]() {
        merge_shuffle_vanilla<int16_t>(
                reinterpret_cast<int16_t *>(src1),
                reinterpret_cast<int16_t *>(src2),
                reinterpret_cast<int16_t *>(dest),
                reinterpret_cast<index_t *>(index),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeShuffle32Bit(JNIEnv *env, jclass cl, jlong src1, jlong src2, jlong dest, jlong index,
                                           jlong count) {
    measure_time(11, [=]() {
        merge_shuffle_vanilla<int32_t>(
                reinterpret_cast<int32_t *>(src1),
                reinterpret_cast<int32_t *>(src2),
                reinterpret_cast<int32_t *>(dest),
                reinterpret_cast<index_t *>(index),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

DECLARE_DISPATCHER(merge_shuffle_int64);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeShuffle64Bit(JNIEnv *env, jclass cl, jlong src1, jlong src2, jlong dest, jlong index,
                                           jlong count) {
    measure_time(12, [=]() {
        merge_shuffle_int64(
                reinterpret_cast<int64_t *>(src1),
                reinterpret_cast<int64_t *>(src2),
                reinterpret_cast<int64_t *>(dest),
                reinterpret_cast<index_t *>(index),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}


JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeShuffle128Bit(JNIEnv *env, jclass cl, jlong src1, jlong src2, jlong dest, jlong index,
                                            jlong count) {
    measure_time(29, [=]() {
        merge_shuffle_vanilla<__int128>(
                reinterpret_cast<__int128 *>(src1),
                reinterpret_cast<__int128 *>(src2),
                reinterpret_cast<__int128 *>(dest),
                reinterpret_cast<index_t *>(index),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeShuffle256Bit(JNIEnv *env, jclass cl, jlong src1, jlong src2, jlong dest, jlong index,
                                            jlong count) {
    measure_time(29, [=]() {
        merge_shuffle_vanilla<long_256bit>(
                reinterpret_cast<long_256bit *>(src1),
                reinterpret_cast<long_256bit *>(src2),
                reinterpret_cast<long_256bit *>(dest),
                reinterpret_cast<index_t *>(index),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

// Methods 13-16 were mergeShuffleWithTop(s) and replaced with calls to simple mergeShuffle(s)

DECLARE_DISPATCHER(flatten_index);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_flattenIndex(JNIEnv *env, jclass cl, jlong pIndex,
                                      jlong count) {
    measure_time(17, [=]() {
        flatten_index(
                reinterpret_cast<index_t *>(pIndex),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_binarySearch64Bit(JNIEnv *env, jclass cl, jlong pData, jlong value, jlong low,
                                           jlong high, jint scan_dir) {
    return binary_search<int64_t>(reinterpret_cast<int64_t *>(pData), value, low, high, scan_dir);
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_binarySearchIndexT(JNIEnv *env, jclass cl, jlong pData, jlong value, jlong low,
                                            jlong high, jint scan_dir) {
    return binary_search<index_t>(reinterpret_cast<index_t *>(pData), value, low, high, scan_dir);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_makeTimestampIndex(JNIEnv *env, jclass cl, jlong pData, jlong low,
                                            jlong high, jlong pIndex) {
    measure_time(18, [=]() {
        make_timestamp_index(
                reinterpret_cast<int64_t *>(pData),
                low,
                high,
                reinterpret_cast<index_t *>(pIndex)
        );
    });
}

DECLARE_DISPATCHER(shift_timestamp_index);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_shiftTimestampIndex(JNIEnv *env, jclass cl, jlong pSrc, jlong count, jlong pDest) {
    measure_time(31, [=]() {
        shift_timestamp_index(
                reinterpret_cast<index_t *>(pSrc),
                __JLONG_REINTERPRET_CAST__(int64_t, count),
                reinterpret_cast<index_t *>(pDest)
        );
    });
}

DECLARE_DISPATCHER(set_memory_vanilla_int64);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setMemoryLong(JNIEnv *env, jclass cl, jlong pData, jlong value,
                                       jlong count) {
    measure_time(19, [=]() {
        set_memory_vanilla_int64(
                reinterpret_cast<int64_t *>(pData),
                __JLONG_REINTERPRET_CAST__(int64_t, value),
                (int64_t) (count)
        );
    });
}

DECLARE_DISPATCHER(set_memory_vanilla_int32);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setMemoryInt(JNIEnv *env, jclass cl, jlong pData, jint value,
                                      jlong count) {
    measure_time(20, [=]() {
        set_memory_vanilla_int32(
                reinterpret_cast<int32_t *>(pData),
                value,
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

DECLARE_DISPATCHER(set_memory_vanilla_double);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setMemoryDouble(JNIEnv *env, jclass cl, jlong pData, jdouble value,
                                         jlong count) {
    measure_time(21, [=]() {
        set_memory_vanilla_double(
                reinterpret_cast<jdouble *>(pData),
                value,
                (int64_t) (count)
        );
    });
}

DECLARE_DISPATCHER(set_memory_vanilla_float);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setMemoryFloat(JNIEnv *env, jclass cl, jlong pData, jfloat value,
                                        jlong count) {
    measure_time(22, [=]() {
        set_memory_vanilla_float(
                reinterpret_cast<jfloat *>(pData),
                value,
                (int64_t) (count)
        );
    });
}

DECLARE_DISPATCHER(set_memory_vanilla_short);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setMemoryShort(JNIEnv *env, jclass cl, jlong pData, jshort value,
                                        jlong count) {
    measure_time(23, [=]() {
        set_memory_vanilla_short(
                reinterpret_cast<jshort *>(pData),
                value,
                (int64_t) (count)
        );
    });
}


DECLARE_DISPATCHER(set_string_column_null_refs);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setStringColumnNullRefs(JNIEnv *env, jclass cl, jlong pData, jlong offset,
                                               jlong count) {
    measure_time(24, [=]() {
        set_string_column_null_refs(
                reinterpret_cast<int64_t *>(pData),
                __JLONG_REINTERPRET_CAST__(int64_t, offset),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

DECLARE_DISPATCHER(set_binary_column_null_refs);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setBinaryColumnNullRefs(JNIEnv *env, jclass cl, jlong pData, jlong offset,
                                               jlong count) {
    measure_time(25, [=]() {
        set_binary_column_null_refs(
                reinterpret_cast<int64_t *>(pData),
                __JLONG_REINTERPRET_CAST__(int64_t, offset),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

DECLARE_DISPATCHER(copy_index);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_oooCopyIndex(JNIEnv *env, jclass cl, jlong pIndex, jlong index_size,
                                      jlong pDest) {
    measure_time(26, [=]() {
        copy_index(
                reinterpret_cast<index_t *>(pIndex),
                __JLONG_REINTERPRET_CAST__(int64_t, index_size),
                reinterpret_cast<int64_t *>(pDest)
        );
    });
}

DECLARE_DISPATCHER(shift_copy);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_shiftCopyFixedSizeColumnData(JNIEnv *env, jclass cl, jlong shift, jlong src, jlong srcLo,
                                                      jlong srcHi, jlong dst) {
    measure_time(27, [=]() {
        shift_copy(
                __JLONG_REINTERPRET_CAST__(int64_t, shift),
                reinterpret_cast<int64_t *>(src),
                __JLONG_REINTERPRET_CAST__(int64_t, srcLo),
                __JLONG_REINTERPRET_CAST__(int64_t, srcHi),
                reinterpret_cast<int64_t *>(dst)
        );
    });
}

DECLARE_DISPATCHER(shift_copy_varchar_aux);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_shiftCopyVarcharColumnAux(JNIEnv *env, jclass cl, jlong shift, jlong src, jlong srcLo,
                                                   jlong srcHi, jlong dst) {
    measure_time(28, [=]() {
        shift_copy_varchar_aux(
                __JLONG_REINTERPRET_CAST__(int64_t, shift),
                reinterpret_cast<int64_t *>(src),
                __JLONG_REINTERPRET_CAST__(int64_t, srcLo),
                __JLONG_REINTERPRET_CAST__(int64_t, srcHi),
                reinterpret_cast<int64_t *>(dst)
        );
    });
}

DECLARE_DISPATCHER(copy_index_timestamp);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_copyFromTimestampIndex(JNIEnv *env, jclass cl, jlong pIndex, jlong indexLo, jlong indexHi,
                                                jlong pTs) {
    measure_time(29, [=]() {
        copy_index_timestamp(
                reinterpret_cast<index_t *>(pIndex),
                __JLONG_REINTERPRET_CAST__(int64_t, indexLo),
                __JLONG_REINTERPRET_CAST__(int64_t, indexHi),
                reinterpret_cast<int64_t *>(pTs)
        );
    });
}

DECLARE_DISPATCHER(set_varchar_null_refs);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setVarcharColumnNullRefs(JNIEnv *env, jclass cl, jlong aux, jlong offset,
                                                  jlong count) {
    measure_time(30, [=]() {
        set_varchar_null_refs(
                reinterpret_cast<int64_t *>(aux),
                __JLONG_REINTERPRET_CAST__(int64_t, offset),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

DECLARE_DISPATCHER(set_array_null_refs);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setArrayColumnNullRefs(JNIEnv *env, jclass cl, jlong aux, jlong offset,
                                                  jlong count) {
    measure_time(31, [=]() {
        set_array_null_refs(
                reinterpret_cast<int64_t *>(aux),
                __JLONG_REINTERPRET_CAST__(int64_t, offset),
                __JLONG_REINTERPRET_CAST__(int64_t, count)
        );
    });
}

DECLARE_DISPATCHER(shift_copy_array_aux);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_shiftCopyArrayColumnAux(JNIEnv *env, jclass cl, jlong shift, jlong src, jlong srcLo,
                                                 jlong srcHi, jlong dst) {
    measure_time(32, [=]() {
        shift_copy_array_aux(
                __JLONG_REINTERPRET_CAST__(int64_t, shift),
                reinterpret_cast<int64_t *>(src),
                __JLONG_REINTERPRET_CAST__(int64_t, srcLo),
                __JLONG_REINTERPRET_CAST__(int64_t, srcHi),
                reinterpret_cast<int64_t *>(dst)
        );
    });
}

DECLARE_DISPATCHER(set_memory_vanilla_int128);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setMemoryLong128(JNIEnv *env, jclass cl, jlong pData, jlong long0,
                                       jlong long1, jlong count) {
    measure_time(19, [=]() {
        set_memory_vanilla_int128(
                reinterpret_cast<long_128bit *>(pData),
                long_128bit{
                    .long0 = (uint64_t)long0,
                    .long1 = (uint64_t)long1
                },
                (int64_t) (count)
        );
    });
}

DECLARE_DISPATCHER(set_memory_vanilla_int256);
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setMemoryLong256(JNIEnv *env, jclass cl, jlong pData, jlong long0,
                                       jlong long1, jlong long2, jlong long3, jlong count) {
    measure_time(19, [=]() {
        set_memory_vanilla_int256(
                reinterpret_cast<long_256bit *>(pData),
                long_256bit{
                    .long0 = (uint64_t)long0,
                    .long1 = (uint64_t)long1,
                    .long2 = (uint64_t)long2,
                    .long3 = (uint64_t)long3
                },
                (int64_t) (count)
        );
    });
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_getPerformanceCounter(JNIEnv *env, jclass cl, jint counterIndex) {
#ifdef OOO_CPP_PROFILE_TIMING
    return perf_counters[counterIndex].load();
#else
    return 0;
#endif
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_getPerformanceCountersCount(JNIEnv *env, jclass cl) {
#ifdef OOO_CPP_PROFILE_TIMING
    return perf_counter_length;
#else
    return 0;
#endif
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_resetPerformanceCounters(JNIEnv *env, jclass cl) {
#ifdef OOO_CPP_PROFILE_TIMING
    for (int i = 0; i < perf_counter_length; i++) {
        perf_counters[i].store(0);
    }
#endif
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_sortStringColumn(JNIEnv *env, jclass cl, jlong mergedTimestampsAddr, jlong valueCount,
                                       jlong srcDataAddr, jlong srcIndxAddr, jlong tgtDataAddr, jlong tgtIndxAddr) {

    const index_t *index = reinterpret_cast<index_t *> (mergedTimestampsAddr);
    const auto count = __JLONG_REINTERPRET_CAST__(int64_t, valueCount);
    const char *src_data = reinterpret_cast<const char *>(srcDataAddr);
    const auto *src_index = reinterpret_cast<const int64_t *>(srcIndxAddr);
    char *tgt_data = reinterpret_cast<char *>(tgtDataAddr);
    auto *tgt_index = reinterpret_cast<int64_t *>(tgtIndxAddr);

    int64_t offset = 0;
    for (int64_t i = 0; i < count; ++i) {
        MM_PREFETCH_T0(index + i + 64);
        const uint64_t row = index[i].i;
        const int64_t o1 = src_index[row];
        const int64_t o2 = src_index[row + 1];
        const int64_t len = o2 - o1;
        platform_memcpy(reinterpret_cast<void *>(tgt_data + offset), reinterpret_cast<const void *>(src_data + o1),
                        len);
        tgt_index[i] = offset;
        offset += len;
    }
    return __JLONG_REINTERPRET_CAST__(jlong, offset);
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_sortVarcharColumn(JNIEnv *env, jclass cl, jlong mergedTimestampsAddr, jlong valueCount,
                                           jlong srcDataAddr, jlong srcAuxAddr, jlong tgtDataAddr, jlong tgtAuxAddr) {

    const index_t *index = reinterpret_cast<index_t *> (mergedTimestampsAddr);
    const auto count = __JLONG_REINTERPRET_CAST__(int64_t, valueCount);
    const char *src_data = reinterpret_cast<const char *>(srcDataAddr);
    const auto *src_aux = reinterpret_cast<const int64_t *>(srcAuxAddr);
    char *tgt_data = reinterpret_cast<char *>(tgtDataAddr);
    auto *tgt_aux = reinterpret_cast<int64_t *>(tgtAuxAddr);

    int64_t offset = 0;
    for (int64_t i = 0; i < count; ++i) {
        MM_PREFETCH_T0(index + i + 64);
        const uint64_t row = index[i].i;
        const int64_t a1 = src_aux[2 * row];
        const int64_t a2 = src_aux[2 * row + 1];
        tgt_aux[2 * i] = a1;
        tgt_aux[2 * i + 1] = (offset << 16) | (a2 & 0xffff);
        const int64_t flags = a1 & 0x0f;
        if ((flags & 5) == 0) {
            // not inlined and not null
            const int64_t size = ((a1 >> 4) & 0xffffff);
            platform_memcpy(reinterpret_cast<void *>(tgt_data + offset),
                            reinterpret_cast<const void *>(src_data + (a2 >> 16)),
                            size);
            offset += size;
        }
    }
    return __JLONG_REINTERPRET_CAST__(jlong, offset);
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_sortArrayColumn(JNIEnv *env, jclass cl, jlong mergedTimestampsAddr, jlong valueCount,
                                         jlong srcDataAddr, jlong srcAuxAddr, jlong tgtDataAddr, jlong tgtAuxAddr) {

    const index_t *index = reinterpret_cast<index_t *> (mergedTimestampsAddr);
    const auto count = __JLONG_REINTERPRET_CAST__(int64_t, valueCount);
    const char *src_data = reinterpret_cast<const char *>(srcDataAddr);
    const auto *src_aux = reinterpret_cast<const int64_t *>(srcAuxAddr);
    char *tgt_data = reinterpret_cast<char *>(tgtDataAddr);
    auto *tgt_aux = reinterpret_cast<int64_t *>(tgtAuxAddr);

    int64_t offset = 0;
    for (int64_t i = 0; i < count; ++i) {
        MM_PREFETCH_T0(index + i + 64);
        const uint64_t row = index[i].i;
        const int64_t src_offset = src_aux[2 * row] & OFFSET_MAX;
        const int64_t size = src_aux[2 * row + 1] & ARRAY_SIZE_MAX;
        tgt_aux[2 * i] = offset;
        tgt_aux[2 * i + 1] = size;
        if (size > 0) {
            platform_memcpy(
                    reinterpret_cast<void *>(tgt_data + offset),
                    reinterpret_cast<const void *>(src_data + src_offset),
                    size
            );
            offset += size;
        }
    }
    return __JLONG_REINTERPRET_CAST__(jlong, offset);
}

}
// extern "C"
