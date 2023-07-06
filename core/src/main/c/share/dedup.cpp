//
// Created by Alex Pelagenko on 30/06/2023.
//

#include "jni.h"
#include <cstring>
#include "util.h"
#include "simd.h"
#include "ooo_dispatch.h"
#include <algorithm>


inline index_t *
merge_sort(const index_t *src, index_t *dest1, index_t *dest2, int64_t start, int64_t end, const int32_t **keys1,
           const int32_t **keys2, int32_t key_count);

inline int diff(const int32_t **keys1, const int32_t **keys2, int32_t key_count, uint64_t left, uint64_t right);

inline bool key_equals(
        const int64_t count,
        const uint64_t src_pos,
        const int32_t **src_data,
        const int64_t *src_tops,
        const uint64_t dest_pos,
        const int32_t **dest_data
) {
    for (int c = 0; c < count; c++) {
        const int64_t col_top = src_tops != nullptr ? src_tops[c] : 0;
        const int32_t src_val = src_pos >= (uint64_t) col_top ? src_data[c][src_pos - col_top] : -1;
        const int32_t dest_val = dest_data[c][dest_pos];
        if (src_val != dest_val) {
            return false;
        }
    }
    return true;
}

int64_t merge_dedup_long_index_int_keys(
        const uint64_t *src,
        const int64_t src_lo,
        const int64_t src_hi_incl,
        const index_t *index,
        const int64_t index_lo,
        const int64_t index_hi_incl,
        index_t *dest_index,
        const int64_t dedup_key_count,
        const int32_t **dedup_key_src_data,
        const int64_t *dedup_key_src_tops,
        const int32_t **dedup_index_data
) {
    int64_t src_pos = src_lo;
    int64_t index_pos = index_lo;
    index_t *dest = dest_index;

    while (src_pos <= src_hi_incl && index_pos <= index_hi_incl) {
        if (src[src_pos] < index[index_pos].ts) {
            dest[0].ts = src[src_pos];
            dest[0].i = src_pos | (1ull << 63);
            dest++;
            src_pos++;
        } else if (src[src_pos] > index[index_pos].ts) {
            *dest = index[index_pos];
            dest++;
            index_pos++;
        } else {
            // index_ts == src_ts
            const uint64_t conflict_ts = src[src_pos];
            const int64_t conflict_src_pos = src_pos;
            index_t *conflict_dest_start = dest;

            // copy all src records with the same ts
            while (src_pos <= src_hi_incl && src[src_pos] == conflict_ts) {
                dest[0].ts = conflict_ts;
                dest[0].i = src_pos | (1ull << 63);
                dest++;
                src_pos++;
            }

            // overwrite them with dest if key match
            while (index_pos <= index_hi_incl && index[index_pos].ts == conflict_ts) {
                bool matched = false;
                for (int64_t i = conflict_src_pos; i < src_pos; i++) {
                    if (key_equals(dedup_key_count, i, dedup_key_src_data,
                                   dedup_key_src_tops, index[index_pos].i, dedup_index_data)) {
                        conflict_dest_start[i - conflict_src_pos].i = index[index_pos].i;
                        matched = true;
                        // keep looking, there can be more than 1 match
                    }
                }
                if (!matched) {
                    dest[0] = index[index_pos];
                    dest++;
                }
                index_pos++;
            }
        }
    }

    if (index_pos <= index_hi_incl) {
        __MEMCPY(dest, &index[index_pos], index_hi_incl - index_pos + 1);
    }

    while (src_pos <= src_hi_incl) {
        dest[0].ts = src[src_pos];
        dest[0].i = src_pos | (1ull << 63);
        dest++;
        src_pos++;
    }

    return dest - dest_index;
}

inline int64_t dedup_sorted_timestamp_index(const index_t *pIndexIn, int64_t count, index_t *pIndexOut) {
    if (count > 0) {
        int64_t copyTo = 0;
        uint64_t lastTimestamp = pIndexIn[0].ts;
        for (int64_t i = 1; i < count; i++) {
            if (pIndexIn[i].ts > lastTimestamp) {
                pIndexOut[copyTo] = pIndexIn[i - 1];
                copyTo++;
                lastTimestamp = pIndexIn[i].ts;
            } else if (pIndexIn[i].ts < lastTimestamp) {
                return -i - 1;
            }
        }
        pIndexOut[copyTo] = pIndexIn[count - 1];
        return copyTo + 1;
    }
    return 0;
}

inline int64_t dedup_sorted_timestamp_index_with_keys(
        const index_t *index_src,
        const int64_t count,
        index_t *index_dest,
        index_t *index_tmp,
        const int32_t key_count,
        const int32_t **key_values1,
        const int32_t **key_values2
) {
    if (count < 2) {
        return count;
    }

    // find duplicate ranges
    int64_t dup_start = -1;
    int64_t dup_end = 0;
    int64_t ts_index = 0;

    for (int64_t i = 1; i < count; i++) {
        if (index_src[i].ts > index_src[ts_index].ts) {
            if (i > ts_index + 1) {
                dup_start = dup_start > -1 ? dup_start : ts_index;
                dup_end = i;
            }
            ts_index = i;
        } else if (index_src[i].ts < index_src[ts_index].ts) {
            return -i - 1;
        }
    }
    if (ts_index < count - 1 && index_src[ts_index].ts == index_src[count - 1].ts) {
        // last element is a duplicate with the previous one
        dup_start = dup_start > -1 ? dup_start : ts_index;
        dup_end = count;
    } else if (dup_start == -1 || dup_end - dup_start <= 0) {
        // no timestamp duplicates
        return count;
    }

    assert(dup_start > -1 && dup_start < count && "dup_start is incorrect");
    assert(dup_end > 0 && dup_end <= count && "dup_end is beyond count");

    // dedup range from dup_start to dup_end.
    // sort the data first by ts and keys using stable merge sort.
    const index_t *merge_result = merge_sort(index_src, index_dest, index_tmp, dup_start, dup_end, key_values1,
                                             key_values2, key_count);

    int64_t copy_to = dup_start;
    int64_t last = dup_start;

    const int32_t **keys[] = {key_values1, key_values2};
    for (int64_t i = dup_start + 1; i < dup_end; i++) {
        uint64_t l = merge_result[last].i;
        uint64_t r = merge_result[i].i;
        if (merge_result[i].ts > merge_result[last].ts ||
            diff(
                keys[l >> 63],
                keys[r >> 63],
                key_count,
                l & ~(1ull << 63),
                r & ~(1ull << 63)
            ) != 0l
        ) {
            index_dest[copy_to++] = merge_result[i - 1];
            last = i;
        } else {
            assert(merge_result[i].ts == merge_result[last].ts && "sorting failed, timestamp is not sorted");
        }
    }
    index_dest[copy_to] = merge_result[dup_end - 1];

    if (copy_to + 1 < dup_end) {
        // Duplicates found
        // copy prefix and the tail if necessary
        if (index_src != index_dest) {
            __MEMCPY(index_dest, index_src, dup_start * sizeof(index_t));
        }

        const int64_t tail = count - dup_end;
        __MEMMOVE(&index_dest[copy_to + 1], &index_src[dup_end], tail * sizeof(index_t));
        return copy_to + 1 + tail;
    } else {
        assert(copy_to + 1 == dup_end && "sorting failed");
        // Duplicates not found
        return count;

    }
}

inline void merge_sort_slice(
        const int32_t **keys[],
        const int32_t &key_count,
        const index_t *src1,
        const index_t *src2,
        index_t *dest,
        const int64_t &src1_len,
        const int64_t &src2_len,
        const index_t *end
) {

    int64_t i1 = 0, i2 = 0;

    while (i1 < src1_len && i2 < src2_len) {
        if (src1[i1] > src2[i2].ts) {
            *dest++ = src2[i2++];
        } else if (src1[i1].ts < src2[i2].ts) {
            *dest++ = src1[i1++];
        } else {
            // same timestamp
            uint64_t l = src1[i1].i;
            uint64_t r = src2[i2].i;
            if (
                    diff(
                            keys[l >> 63],
                            keys[r >> 63],
                            key_count,
                            l & ~(1ull << 63),
                            r & ~(1ull << 63)
                    ) > 0
                    ) {
                *dest++ = src2[i2++];
            } else {
                *dest++ = src1[i1++];
            }
        }
    }

    if (i1 < src1_len) {
        __MEMCPY(dest, &src1[i1], (src1_len - i1) * sizeof(index_t));
        assert(dest + src1_len - i1 <= end && "write beyond allocated boundary");
    } else {
        __MEMCPY(dest, &src2[i2], (src2_len - i2) * sizeof(index_t));
        assert(dest + src2_len - i2 <= end && "write beyond allocated boundary");
    }
}

inline int32_t
diff(const int32_t **keys1, const int32_t **keys2, const int32_t key_count, uint64_t left, uint64_t right) {
    for (int k = 0; k < key_count; k++) {
        int32_t diff = keys1[k][left] - keys2[k][right];
        if (diff != 0) {
            return diff;
        }
    }
    return 0;
}

inline index_t *merge_sort(
        const index_t *index_src,
        index_t *index_dest1,
        index_t *index_dest2,
        int64_t start,
        int64_t end,
        const int32_t **keys1,
        const int32_t **keys2,
        const int32_t key_count
) {
    index_t *const dest_arr[] = {index_dest2, index_dest1};
    const int32_t **keys[] = {keys1, keys2};
    const index_t *source = index_src;
    index_t *dest;
    const int64_t len = end - start;
    int64_t slice_len = 1;

    int cycle = 0;
    do {
        dest = dest_arr[cycle % 2]; // first write to index_dest2 then to index_dest1
        const int64_t twice_slice = 2 * slice_len;
        for (int64_t i = start; i < end; i += twice_slice) {
            merge_sort_slice(
                    keys,
                    key_count,
                    &source[i],
                    &source[i + slice_len],
                    &dest[i],
                    std::min(slice_len, end - i),
                    std::max(0ll, std::min(slice_len, end - (i + slice_len))),
                    &dest[end]
            );
        }
        source = dest_arr[cycle++ % 2]; // rotate source and destination
        slice_len = twice_slice;
    } while (slice_len < len);

    return dest;
}

extern "C" {
JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_mergeDedupTimestampWithLongIndexAsc(
        JAVA_STATIC,
        jlong pSrc,
        jlong srcLo,
        jlong srcHiInclusive,
        jlong pIndex,
        jlong indexLo,
        jlong indexHiInclusive,
        jlong pDestIndex
) {
    const uint64_t *src = reinterpret_cast<uint64_t *> (pSrc);
    const index_t *index = reinterpret_cast<index_t *> (pIndex);

    auto src_pos = __JLONG_REINTERPRET_CAST__(int64_t, srcLo);
    auto index_pos = __JLONG_REINTERPRET_CAST__(int64_t, indexLo);
    auto *dest = reinterpret_cast<index_t *> (pDestIndex);

    const auto src_hi_incl = __JLONG_REINTERPRET_CAST__(int64_t, srcHiInclusive);
    const auto index_hi_inc = __JLONG_REINTERPRET_CAST__(int64_t, indexHiInclusive);

    while (src_pos <= src_hi_incl &&
           index_pos <= index_hi_inc) {
        if (src[src_pos] < index[index_pos].ts) {
            dest[0].ts = src[src_pos];
            dest[0].i = src_pos | (1ull << 63);
            dest++;
            src_pos++;
        } else if (src[src_pos] >
                   index[index_pos].ts) {
            dest[0] = index[index_pos];
            dest++;
            index_pos++;
        } else {
            // index_ts == src_ts
            const uint64_t conflict_ts = src[src_pos];
            while (index_pos <= index_hi_inc &&
                   index[index_pos].ts == conflict_ts) {
                index_pos++;
            }

            // replace all records with same timestamp with last version from index
            while (src_pos <= src_hi_incl && src[src_pos] == conflict_ts) {
                dest[0] = index[index_pos - 1];
                dest++;
                src_pos++;
            }
        }
    }

    while (index_pos <= index_hi_inc) {
        dest[0] = index[index_pos];
        dest++;
        index_pos++;
    }

    while (src_pos <= src_hi_incl) {
        dest[0].ts = src[src_pos];
        dest[0].i = src_pos | (1ull << 63);
        dest++;
        src_pos++;
    }
    return dest - reinterpret_cast<index_t *> (pDestIndex);
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_mergeDedupTimestampWithLongIndexIntKeys(
        JAVA_STATIC,
        jlong srcTimestampAddr,
        jlong mergeDataLo,
        jlong mergeDataHi,
        jlong sortedTimestampsAddr,
        jlong mergeOOOLo,
        jlong mergeOOOHi,
        jlong tempIndexAddr,
        jint dedupKeyCount,
        jlong dedupColBuffs,
        jlong dedupColTops,
        jlong dedupO3Buffs) {
    int64_t merge_count = merge_dedup_long_index_int_keys(
            reinterpret_cast<uint64_t *> (srcTimestampAddr),
            __JLONG_REINTERPRET_CAST__(int64_t, mergeDataLo),
            __JLONG_REINTERPRET_CAST__(int64_t, mergeDataHi),
            reinterpret_cast<index_t *> (sortedTimestampsAddr),
            __JLONG_REINTERPRET_CAST__(int64_t, mergeOOOLo),
            __JLONG_REINTERPRET_CAST__(int64_t, mergeOOOHi),
            reinterpret_cast<index_t *> (tempIndexAddr),
            dedupKeyCount,
            reinterpret_cast<const int32_t **> (dedupColBuffs),
            reinterpret_cast<const int64_t *> (dedupColTops),
            reinterpret_cast<const int32_t **> (dedupO3Buffs)
    );
    return merge_count;
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_dedupSortedTimestampIndex(
        JAVA_STATIC,
        jlong pIndexIn,
        jlong count,
        jlong pIndexOut,
        jlong pIndexTemp,
        const jint dedupKeyCount,
        jlong dedupColBuffs1,
        jlong dedupColBuffs2
) {
    if (dedupKeyCount == 0) {
        return dedup_sorted_timestamp_index(
                reinterpret_cast<const index_t *> (pIndexIn),
                __JLONG_REINTERPRET_CAST__(int64_t, count),
                reinterpret_cast<index_t *> (pIndexOut)
        );
    } else {
        return dedup_sorted_timestamp_index_with_keys(
                reinterpret_cast<const index_t *> (pIndexIn),
                __JLONG_REINTERPRET_CAST__(const int64_t, count),
                reinterpret_cast<index_t *> (pIndexOut),
                reinterpret_cast<index_t *> (pIndexTemp),
                dedupKeyCount,
                reinterpret_cast<const int32_t **> (dedupColBuffs1),
                reinterpret_cast<const int32_t **> (dedupColBuffs2)
        );
    }
}

} // extern C