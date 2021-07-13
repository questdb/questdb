#include "util.h"
#include "bitmap_index_utils.h"
#include "geohash_dispatch.h"

void simd_iota(int64_t *array, const int64_t array_size, const int64_t start) {
    int64_t next = start;
    for (int64_t i = 0; i < array_size; ++i) {
        array[i] = next++;
    }
}

void filter_with_prefix(
        const int64_t *hashes,
        int64_t *rows,
        const int64_t rows_count,
        const int32_t hash_length,
        const int64_t *prefixes,
        const int64_t prefixes_count
) {
    int64_t i = 0;
    for (; i < rows_count; ++i) {
        const int64_t current_hash = hashes[to_local_row_id(rows[i] - 1)];
        bool hit = false;
        for (size_t j = 0, sz = prefixes_count/2; j < sz; ++j) {
            const int64_t hash = prefixes[2*j];
            const int64_t mask = prefixes[2*j+1];
            hit |= (current_hash & mask) == hash;
        }
        const int64_t cv = rows[i];
        rows[i] = hit ? cv : 0;
    }
}
