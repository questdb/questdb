#include <cstdlib>
#include <cstdint>
#include <cstring>
#include "rosti.h"
#include <jni.h>

rosti_t *alloc_rosti(const int32_t *column_types, const int32_t column_count, const uint64_t map_capacity) {
    uint64_t slot_key_size = 0;
    auto value_offsets = reinterpret_cast<int32_t *>(malloc(sizeof(int32_t) * (column_count + 1)));
    value_offsets[0] = 0;
    for (int32_t i = 0; i < column_count; i++) {
        switch (column_types[i]) {
            case 0: // BOOL
            case 1: // BYTE
                slot_key_size += 1;
                break;
            case 2: // SHORT
            case 3: // CHAR
                slot_key_size += 2;
                break;
            case 4: // INT
            case 8: // FLOATó
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
        value_offsets[i + 1] = slot_key_size;
    }
    auto map = reinterpret_cast<rosti_t *>(malloc(sizeof(rosti_t)));
    map->slot_size_ = ceil_pow_2(slot_key_size);
    map->slot_size_shift_ = vcl::bit_scan_forward(map->slot_size_);
    map->capacity_ = map_capacity;
    map->size_ = 0;
    map->value_offsets_ = value_offsets;
    initialize_slots(map);
    return map;
}

extern "C" {

JNIEXPORT jlong JNICALL
Java_io_questdb_std_Rosti_alloc(JNIEnv *env, jclass cl, jlong pKeyTypes, jint keyTypeCount, jlong capacity) {
    return reinterpret_cast<jlong>(alloc_rosti(reinterpret_cast<int32_t *>(pKeyTypes), keyTypeCount, capacity));
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_free0(JNIEnv *env, jclass cl, jlong pRosti) {
    auto map = reinterpret_cast<rosti_t *>(pRosti);
    // initial values contains main arena pointer
    free(map->slot_initial_values_);
    free(map->value_offsets_);
    free(map);
}

JNIEXPORT void JNICALL
Java_io_questdb_std_Rosti_clear(JNIEnv *env, jclass cl, jlong pRosti) {
    clear(reinterpret_cast<rosti_t *>(pRosti));
}

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

void clear(rosti_t *map) {
    map->size_ = 0;
    reset_ctrl(map);
    reset_growth_left(map);
    memset(map->slots_, 0, map->capacity_ << map->slot_size_shift_);
}

inline bool IsEmpty(ctrl_t c) { return c == kEmpty; }

inline bool IsFull(ctrl_t c) { return c >= 0; }

inline bool IsDeleted(ctrl_t c) { return c == kDeleted; }

inline bool IsEmptyOrDeleted(ctrl_t c) { return c < kSentinel; }

void set_ctrl(rosti_t *map, uint64_t i, ctrl_t h) {
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

FindInfo find_first_non_full(rosti_t *map, uint64_t hash) {
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

void resize(rosti_t *map, uint64_t new_capacity) {
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
            const uint64_t hh = hash(*(reinterpret_cast<int32_t *>(p)));
            auto target = find_first_non_full(map, hh);
            uint64_t new_i = target.offset;
            total_probe_length += target.probe_length;
            set_ctrl(map, new_i, H2(hh));
            *(reinterpret_cast<int32_t *>(map->slots_ + (new_i << map->slot_size_shift_))) = *p;
        }
    }
    if (old_capacity) {
        free(old_init);
    }
}

void rehash_and_grow_if_necessary(rosti_t *map) {
    resize(map, map->capacity_ * 2 + 1);
}

ATTRIBUTE_NEVER_INLINE uint64_t prepare_insert(rosti_t *map, uint64_t hash) {
    auto target = find_first_non_full(map, hash);
    if (PREDICT_FALSE(map->growth_left_ == 0 && !IsDeleted(map->ctrl_[target.offset]))) {
        rehash_and_grow_if_necessary(map);
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
