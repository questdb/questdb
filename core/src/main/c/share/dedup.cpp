//
// Created by Alex Pelagenko on 30/06/2023.
//

#include "jni.h"
#include <cstring>
#include "util.h"
#include "simd.h"
#include "ooo_dispatch.h"
#include <algorithm>

#pragma pack (push, 1)
struct dedup_column_t {
    int32_t column_type;
    int32_t value_size_bytes;
    int64_t column_top;
    void *column_data;
    void *o3_data;
    int64_t reserved1;
    int64_t reserved2;
    int64_t reserved3;
    char null_value[32];
};

template<typename T>
struct dedup_column {
    int32_t column_type;
    int32_t value_size_bytes;
    int64_t column_top;
    T *column_data;
    T *o3_data;
    int64_t reserved1;
    int64_t reserved2;
    int64_t reserved3;
    T null_value;
};
#pragma pack(pop)

inline int diff(const int32_t **keys1, const int32_t **keys2, int32_t key_count, uint64_t left, uint64_t right);

template<typename T>
inline int64_t
merge_dedupTimestamp_with_long_index_single_key(jlong srcTimestampAddr, jlong mergeDataLo, jlong mergeDataHi,
                                                jlong sortedTimestampsAddr, jlong mergeOOOLo, jlong mergeOOOHi,
                                                jlong tempIndexAddr, const dedup_column_t *src_keys) {
    const dedup_column<T> *col_key = (dedup_column<T> *) (src_keys);

    const auto l_row_equals_single = [&](const int64_t r, const int64_t i) {
        const T r_val = r >= col_key->column_top ? col_key->column_data[r] : col_key->null_value;
        return r_val == col_key->o3_data[i];
    };

    return merge_dedup_long_index_int_keys(
            reinterpret_cast<uint64_t *> (srcTimestampAddr),
            __JLONG_REINTERPRET_CAST__(int64_t, mergeDataLo),
            __JLONG_REINTERPRET_CAST__(int64_t, mergeDataHi),
            reinterpret_cast<index_t *> (sortedTimestampsAddr),
            __JLONG_REINTERPRET_CAST__(int64_t, mergeOOOLo),
            __JLONG_REINTERPRET_CAST__(int64_t, mergeOOOHi),
            reinterpret_cast<index_t *> (tempIndexAddr),
            l_row_equals_single
    );
}

template<typename lambda_diff>
int64_t merge_dedup_long_index_int_keys(
        const uint64_t *src,
        const int64_t src_lo,
        const int64_t src_hi_incl,
        const index_t *index,
        const int64_t index_lo,
        const int64_t index_hi_incl,
        index_t *dest_index,
        const lambda_diff l_row_equals
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
                    if (l_row_equals(i, index[index_pos].i)) {
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
        __MEMCPY(dest, &index[index_pos], (index_hi_incl - index_pos + 1) * sizeof(index_t));
        dest += index_hi_incl - index_pos + 1;
    } else {
        while (src_pos <= src_hi_incl) {
            dest[0].ts = src[src_pos];
            dest[0].i = src_pos | (1ull << 63);
            dest++;
            src_pos++;
        }
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

template<typename diff_lambda>
inline int64_t dedup_sorted_timestamp_index_with_keys(
        const index_t *index_src,
        const int64_t count,
        index_t *index_dest,
        index_t *index_tmp,
        const diff_lambda diff_l
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
    const index_t *merge_result = merge_sort(index_src, index_dest, index_tmp, dup_start, dup_end, diff_l);

    int64_t copy_to = dup_start;
    int64_t last = dup_start;
    
    for (int64_t i = dup_start + 1; i < dup_end; i++) {
        uint64_t l = merge_result[last].i;
        uint64_t r = merge_result[i].i;
        if (merge_result[i].ts > merge_result[last].ts || diff_l(l, r) != 0) {
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

template<typename diff_lambda>
inline void merge_sort_slice(
        const index_t *src1,
        const index_t *src2,
        index_t *dest,
        const int64_t &src1_len,
        const int64_t &src2_len,
        const index_t *end,
        const diff_lambda diff_l
) {

    int64_t i1 = 0, i2 = 0;

    while (i1 < src1_len && i2 < src2_len) {
        if (src1[i1] > src2[i2].ts) {
            *dest++ = src2[i2++];
        } else if (src1[i1].ts < src2[i2].ts) {
            *dest++ = src1[i1++];
        } else {
            // same timestamp
            if (diff_l(src1[i1].i, src2[i2].i) > 0) {
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

template<typename diff_lambda>
inline index_t *merge_sort(
        const index_t *index_src,
        index_t *index_dest1,
        index_t *index_dest2,
        int64_t start,
        int64_t end,
        const diff_lambda diff_l
) {
    index_t *const dest_arr[] = {index_dest2, index_dest1};
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
                    &source[i],
                    &source[i + slice_len],
                    &dest[i],
                    std::min(slice_len, end - i),
                    std::max(0ll, std::min(slice_len, end - (i + slice_len))),
                    &dest[end],
                    diff_l
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
        jlong dedupSrcBuffs1
) {
    const auto src_keys = reinterpret_cast<const dedup_column_t *>(dedupSrcBuffs1);
    if (dedupKeyCount == 1) {
        switch (src_keys[0].value_size_bytes) {
            case 1:
                return merge_dedupTimestamp_with_long_index_single_key<int8_t>(
                        srcTimestampAddr,
                        mergeDataLo,
                        mergeDataHi,
                        sortedTimestampsAddr,
                        mergeOOOLo,
                        mergeOOOHi,
                        tempIndexAddr,
                        src_keys
                );

            case 2:
                return merge_dedupTimestamp_with_long_index_single_key<int16_t>(
                        srcTimestampAddr,
                        mergeDataLo,
                        mergeDataHi,
                        sortedTimestampsAddr,
                        mergeOOOLo,
                        mergeOOOHi,
                        tempIndexAddr,
                        src_keys
                );

            case 4:
                return merge_dedupTimestamp_with_long_index_single_key<int32_t>(
                        srcTimestampAddr,
                        mergeDataLo,
                        mergeDataHi,
                        sortedTimestampsAddr,
                        mergeOOOLo,
                        mergeOOOHi,
                        tempIndexAddr,
                        src_keys
                );

            case 8:
                return merge_dedupTimestamp_with_long_index_single_key<int64_t>(
                        srcTimestampAddr,
                        mergeDataLo,
                        mergeDataHi,
                        sortedTimestampsAddr,
                        mergeOOOLo,
                        mergeOOOHi,
                        tempIndexAddr,
                        src_keys
                );

            case 16:
                return merge_dedupTimestamp_with_long_index_single_key<index_t>(
                        srcTimestampAddr,
                        mergeDataLo,
                        mergeDataHi,
                        sortedTimestampsAddr,
                        mergeOOOLo,
                        mergeOOOHi,
                        tempIndexAddr,
                        src_keys
                );

            default:
                assert(false || "column type not supported");
                return -1;
        }
    } else {
        const auto l_row_equals_multiple = [&](const int64_t r, const int64_t i) {

            const auto key_equals = [=]<class T>(const dedup_column<T> *column, const int64_t r, const int64_t i) {
                T r_val = r >= column->column_top ? column->column_data[r] : column->null_value;
                return r_val == column->o3_data[i];
            };

            for (int c = 0; c < dedupKeyCount; c++) {
                const auto col_key = &src_keys[c];
                switch (col_key->value_size_bytes) {
                    case 1:
                        if (!key_equals((dedup_column<int8_t> *) col_key, r, i)) {
                            return false;
                        }
                        break;
                    case 2:
                        if (!key_equals((dedup_column<int16_t> *) col_key, r, i)) {
                            return false;
                        }
                        break;
                    case 4:
                        if (!key_equals((dedup_column<int16_t> *) col_key, r, i)) {
                            return false;
                        }
                        break;
                    case 8:
                        if (!key_equals((dedup_column<int16_t> *) col_key, r, i)) {
                            return false;
                        }
                        break;
                    case 16:
                        if (!key_equals((dedup_column<index_t> *) col_key, r, i)) {
                            return false;
                        }
                        break;
                    default:
                        assert(false || "unsupported column type");
                        return true;
                }
            }
            return true;
        };

        return merge_dedup_long_index_int_keys(
                reinterpret_cast<uint64_t *> (srcTimestampAddr),
                __JLONG_REINTERPRET_CAST__(int64_t, mergeDataLo),
                __JLONG_REINTERPRET_CAST__(int64_t, mergeDataHi),
                reinterpret_cast<index_t *> (sortedTimestampsAddr),
                __JLONG_REINTERPRET_CAST__(int64_t, mergeOOOLo),
                __JLONG_REINTERPRET_CAST__(int64_t, mergeOOOHi),
                reinterpret_cast<index_t *> (tempIndexAddr),
                l_row_equals_multiple
        );
    }

    assert(false || "multiple keys not supported");
    return -1;
}

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_dedupSortedTimestampIndex(
        JAVA_STATIC,
        jlong pIndexIn,
        jlong count,
        jlong pIndexOut,
        jlong pIndexTemp,
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
        const auto diff_typed = [=]<class T>(const dedup_column<T> *column, const int64_t l, const int64_t r) {
            const T l_val = l > 0 ? column->column_data[l] : column->o3_data[l & ~(1ull << 63)];
            const T r_val = r > 0 ? column->column_data[r] : column->o3_data[r & ~(1ull << 63)];
            // One of the values can be MIN of the type (null value)
            // and subtraction can result in type overflow
            return l_val > r_val ? 1 : (l_val < r_val ? -1 : 0);
        };

        const auto diff_l = [&](const int64_t l, const int64_t r) {

            const auto src_keys = reinterpret_cast<const dedup_column_t *>(dedupColBuffs);
            for (int c = 0; c < dedupKeyCount; c++) {
                const dedup_column_t* col_key = &src_keys[c];
                switch (col_key->value_size_bytes) {
                    case 1: {
                        auto diff = diff_typed((dedup_column<int8_t> *) col_key, l, r);
                        if (diff != 0) {
                            return diff;
                        }
                        break;
                    }
                    case 2:{
                        auto diff = diff_typed((dedup_column<int16_t> *) col_key, l, r);
                        if (diff != 0) {
                            return diff;
                        }
                        break;
                    }
                    case 4:{
                        auto diff = diff_typed((dedup_column<int32_t> *) col_key, l, r);
                        if (diff != 0) {
                            return diff;
                        }
                        break;
                    }
                    case 8:{
                        auto diff = diff_typed((dedup_column<int64_t> *) col_key, l, r);
                        if (diff != 0) {
                            return diff;
                        }
                        break;
                    }
                    case 16:{
                        auto diff = diff_typed((dedup_column<__int128> *) col_key, l, r);
                        if (diff != 0) {
                            return diff;
                        }
                        break;
                    }
                    default:
                        assert(false || "unsupported column type");
                        return 0;
                }
            }
            return 0;
        };

        return dedup_sorted_timestamp_index_with_keys(
                reinterpret_cast<const index_t *> (pIndexIn),
                __JLONG_REINTERPRET_CAST__(const int64_t, count),
                reinterpret_cast<index_t *> (pIndexOut),
                reinterpret_cast<index_t *> (pIndexTemp),
                diff_l
        );
    }
}
}


// extern C