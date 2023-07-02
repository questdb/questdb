//
// Created by Alex Pelagenko on 30/06/2023.
//

#include "jni.h"
#include <cstring>
#include "util.h"
#include "simd.h"
#include "ooo_dispatch.h"
#include <algorithm>


void merge_sort(const index_t *pIndex, index_t *pIndex1, int64_t start, int64_t anEnd, const int32_t count,
                const int32_t **pInt);

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
        index_t *dest_start,
        const int64_t dedupKeyCount,
        const int32_t **dedupSrc,
        const int64_t *dedupSrcTops,
        const int32_t **dedupIndex
) {
    int64_t src_pos = src_lo;
    int64_t index_pos = index_lo;
    index_t *dest = dest_start;

    uint64_t src_ts;
    uint64_t index_ts;

    while (src_pos <= src_hi_incl && index_pos <= index_hi_incl) {
        src_ts = src[src_pos];
        index_ts = (int64_t) index[index_pos].ts;

        if (src_ts < index_ts) {
            dest[0].ts = src_ts;
            dest[0].i = src_pos | (1ull << 63);
            dest++;
            src_pos++;
        } else if (src_ts > index_ts) {
            dest[0].ts = index_ts;
            dest[0].i = index_pos;
            dest++;
            index_pos++;
        } else {
            // index_ts == src_ts
            const uint64_t conflict_ts = src_ts;
            const int64_t conflict_src_pos = src_pos;
            index_t *conflict_dest_start = dest;

            // copy all src records with the same ts
            while (src[src_pos] == conflict_ts) {
                dest[0].ts = conflict_ts;
                dest[0].i = src_pos | (1ull << 63);
                dest++;
                src_pos++;
            }

            // overwrite them with dest if key match
            while (index[index_pos].ts == conflict_ts) {
                bool matched = false;
                for (int64_t i = conflict_src_pos; i < src_pos; i++) {
                    if (key_equals(dedupKeyCount, i, dedupSrc, dedupSrcTops, index_pos, dedupIndex)) {
                        conflict_dest_start[i - conflict_src_pos].i = index_pos;
                        matched = true;
                        // keep looking, there can be more than 1 match
                    }
                }
                if (!matched) {
                    dest[0].ts = conflict_ts;
                    dest[0].i = index_pos;
                    dest++;
                }
                index_pos++;
            }
        }
    }

    while (index_pos <= index_hi_incl) {
        dest[0].ts = index[index_pos].ts;
        dest[0].i = (index_pos - 1);
        dest++;
        index_pos++;
    }

    while (src_pos <= src_hi_incl) {
        dest[0].ts = src[src_pos];
        dest[0].i = src_pos | (1ull << 63);
        dest++;
        src_pos++;
    }

    return dest - dest_start;
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
        const int32_t key_count,
        const int32_t **key_values
) {
    if (count < 2) {
        return count;
    }

    // find duplicate ranges
    int64_t dup_start = 0;
    int64_t dup_end = 0;
    int64_t dup_max_run_count = 0;
    int64_t dup_run_count = 0;
    uint64_t lastTimestamp = index_src[0].ts;

    for (int64_t i = 1; i < count; i++) {
        if (index_src[i].ts > lastTimestamp) {
            dup_max_run_count = std::max(dup_run_count, dup_max_run_count);
            dup_run_count = 0;
            dup_end = i;
        } else if (index_src[i].ts == lastTimestamp) {
            dup_start = std::min(dup_start, i - 1);
            dup_run_count++;
        } else if (index_src[i].ts < lastTimestamp) {
            return -i - 1;
        }
    }

    if (dup_max_run_count == 0) {
        // no dups by timestamp, all good
        return count;
    }

    // dedup range from dup_start to dup_end
    merge_sort(index_src, index_dest, dup_start, dup_end, key_count, key_values);

    int64_t copy_to = 0;
    int64_t last = 0;

    for (int64_t i = dup_start; i < dup_end; i++) {
        if (index_dest[i].ts > index_dest[last].ts ||
            !key_equals(key_count, index_dest[last].i, key_values, nullptr, index_dest[i].i, key_values)) {
            index_dest[copy_to] = index_dest[i - 1];
            copy_to++;
            last = i;
        }
    }
    if (copy_to != dup_end - 1) {
        // Duplicates found
        index_dest[copy_to] = index_dest[dup_end - 1];

        __MEMCPY(index_dest, index_src, dup_start);
        __MEMCPY(&index_dest[copy_to + 1], &index_src[dup_end], count - dup_end);

        return copy_to + 1;
    } else {
        // Duplicates not found
        return count;
    }
}

void merge_sort(
        const index_t *index_src,
        index_t *index_dest,
        int64_t start,
        int64_t end,
        const int32_t count,
        const int32_t **p_int
) {
    // TODO: merge sort here into index_dest
    __MEMCPY(&index_dest[start], &index_src[start], (end - start) * sizeof(index_t));
}

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
    auto srcPos = __JLONG_REINTERPRET_CAST__(int64_t, srcLo);
    auto indexPos = __JLONG_REINTERPRET_CAST__(int64_t, indexLo);
    auto *dest = reinterpret_cast<index_t *> (pDestIndex);

    while (srcPos <= __JLONG_REINTERPRET_CAST__(int64_t, srcHiInclusive) &&
           indexPos <= __JLONG_REINTERPRET_CAST__(int64_t, indexHiInclusive)) {
        if ((reinterpret_cast<uint64_t *> (pSrc))[srcPos] < (reinterpret_cast<index_t *> (pIndex))[indexPos].ts) {
            dest[0].ts = (reinterpret_cast<uint64_t *> (pSrc))[srcPos];
            dest[0].i = srcPos | (1ull << 63);
            dest++;
            srcPos++;
        } else if ((reinterpret_cast<uint64_t *> (pSrc))[srcPos] >
                   (reinterpret_cast<index_t *> (pIndex))[indexPos].ts) {
            dest[0] = (reinterpret_cast<index_t *> (pIndex))[indexPos];
            dest++;
            indexPos++;
        } else {
            // index_ts == src_ts
            const uint64_t conflictTs = (reinterpret_cast<uint64_t *> (pSrc))[srcPos];
            while (indexPos <= __JLONG_REINTERPRET_CAST__(int64_t, indexHiInclusive) &&
                   (reinterpret_cast<index_t *> (pIndex))[indexPos].ts == conflictTs) {
                indexPos++;
            }

            // replace all records with same timestamp with last version from index
            while ((reinterpret_cast<uint64_t *> (pSrc))[srcPos] == conflictTs) {
                dest[0] = (reinterpret_cast<index_t *> (pIndex))[indexPos - 1];
                dest++;
                srcPos++;
            }
        }
    }

    while (indexPos <= __JLONG_REINTERPRET_CAST__(int64_t, indexHiInclusive)) {
        dest[0] = (reinterpret_cast<index_t *> (pIndex))[indexPos];
        dest++;
        indexPos++;
    }

    while (srcPos <= __JLONG_REINTERPRET_CAST__(int64_t, srcHiInclusive)) {
        dest[0].ts = (reinterpret_cast<uint64_t *> (pSrc))[srcPos];
        dest[0].i = srcPos | (1ull << 63);
        dest++;
        srcPos++;
    }
    int64_t merge_count = dest - reinterpret_cast<index_t *> (pDestIndex);
    return merge_count;
}

extern "C" {
JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_mergeDedupTimestampWithLongIndexIntKeys(
        JAVA_STATIC,
        jlong pSrc,
        jlong srcLo,
        jlong srcHiInclusive,
        jlong pIndex,
        jlong indexLo,
        jlong indexHiInclusive,
        jlong pDestIndex,
        jint dedupKeyCount,
        jlong dedupColBuffs,
        jlong dedupColTops,
        jlong dedupO3Buffs) {
    int64_t merge_count = merge_dedup_long_index_int_keys(
            reinterpret_cast<uint64_t *> (pSrc),
            __JLONG_REINTERPRET_CAST__(int64_t, srcLo),
            __JLONG_REINTERPRET_CAST__(int64_t, srcHiInclusive),
            reinterpret_cast<index_t *> (pIndex),
            __JLONG_REINTERPRET_CAST__(int64_t, indexLo),
            __JLONG_REINTERPRET_CAST__(int64_t, indexHiInclusive),
            reinterpret_cast<index_t *> (pDestIndex),
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
        const jint dedupKeyCount,
        jlong dedupColBuffs
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
                dedupKeyCount,
                reinterpret_cast<const int32_t **> (dedupColBuffs)
        );
    }
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_dedupSortedTimestampIndexRebase(
        JAVA_STATIC,
        jlong pIndexIn,
        jlong jcount,
        jlong pIndexOut
) {
    auto index_in = reinterpret_cast<const index_t *> (pIndexIn);
    auto count = __JLONG_REINTERPRET_CAST__(int64_t, jcount);
    auto index_out = reinterpret_cast<index_t *> (pIndexOut);

    if (count > 0) {
        int64_t copyTo = 0;
        uint64_t lastTimestamp = index_in[0].ts;
        for (int64_t i = 1; i < count; i++) {
            if (index_in[i].ts > lastTimestamp) {
                index_out[copyTo].ts = index_in[i - 1].ts;
                index_out[copyTo].i = (i - 1) | (1ull << 63);
                copyTo++;
                lastTimestamp = index_in[i].ts;
            } else if (index_in[i].ts < lastTimestamp) {
                return -i - 1;
            }
        }
        index_out[copyTo].ts = index_in[count - 1].ts;
        index_out[copyTo].i = (count - 1) | (1ull << 63);
        return copyTo + 1;
    }
    return 0;
}

} // extern C