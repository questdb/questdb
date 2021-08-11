#include "util.h"
#include "geohash_dispatch.h"

void simd_iota(int64_t *array, const int64_t array_size, const int64_t start) {
    int64_t next = start;
    for (int64_t i = 0; i < array_size; ++i) {
        array[i] = next++;
    }
}

void filter_with_prefix(
        const int64_t hashes,
        int64_t *rows,
        const int32_t hashes_type_size,
        const int64_t rows_count,
        const int64_t *prefixes,
        const int64_t prefixes_count,
        int64_t *out_filtered_count
) {
    switch (hashes_type_size) {
        case 1:
            filter_with_prefix_generic_vanilla<int8_t>(
                    reinterpret_cast<const int8_t *>(hashes),
                    rows,
                    rows_count,
                    prefixes,
                    prefixes_count,
                    out_filtered_count
            );
            break;
        case 2:
            filter_with_prefix_generic_vanilla<int16_t>(
                    reinterpret_cast<const int16_t *>(hashes),
                    rows,
                    rows_count,
                    prefixes,
                    prefixes_count,
                    out_filtered_count
            );
            break;
        case 4:
            filter_with_prefix_generic_vanilla<int32_t>(
                    reinterpret_cast<const int32_t *>(hashes),
                    rows,
                    rows_count,
                    prefixes,
                    prefixes_count,
                    out_filtered_count
            );
            break;
        case 8:
            filter_with_prefix_generic_vanilla<int64_t>(
                    reinterpret_cast<const int64_t *>(hashes),
                    rows,
                    rows_count,
                    prefixes,
                    prefixes_count,
                    out_filtered_count
            );
            break;
        default:
            break;
    }
}
