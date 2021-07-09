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

#ifndef QUESTDB_BITMAP_INDEX_UTILS_H
#define QUESTDB_BITMAP_INDEX_UTILS_H

#include <utility>
#include <atomic>
#include "util.h"

inline static int64_t to_local_row_id(int64_t row_id) {
    return row_id & 0xFFFFFFFFFFFL;
}

inline static int32_t to_partition_index(int64_t row_id) {
    return static_cast<int32_t>(row_id >> 44);
}

inline static int64_t to_row_id(int32_t partition_index, int64_t local_row_id) {
    return (static_cast<int64_t>(partition_index) << 44) + local_row_id;
}

struct out_arguments {
    int64_t key_lo;
    int64_t key_hi;
    int64_t* rows;
    int64_t rows_capacity;
    int64_t rows_size;
};

struct key_header {
   int8_t  signature;
   int64_t sequence;
   int64_t value_mem_size;
   int32_t block_value_count;
   int64_t key_count;
   int64_t sequence_check;
   int8_t  padding[27];
} __attribute__((packed));

struct key_entry {
    int64_t value_count;
    int64_t first_value_block_offset;
    int64_t last_value_block_offset;
    int64_t count_check;
} __attribute__((packed));

struct value_block_link {
    int64_t prev;
    int64_t next;
} __attribute__((packed));

struct fl_record {
    int64_t first_row_id;
    int64_t last_row_id;
    int64_t timestamp_index;
private:
    int8_t padding[8];
} __attribute__((packed));

class keys_reader {
public:
    //local copy
    struct key_entry_proxy : public key_entry {
       bool is_block_consistent;
    };

    explicit keys_reader(const uint8_t *base_ptr, size_t memory_size)
            : header_ptr_(reinterpret_cast<const key_header *>(base_ptr))
            , keys_ptr_(reinterpret_cast<const key_entry *>(base_ptr + sizeof(key_header)))
            , memory_size_(memory_size) {}

    const key_entry_proxy& operator[](size_t index) const noexcept {
        auto retries_count = 10;
        proxy_.is_block_consistent = false;
        while(retries_count--) {
            proxy_.value_count = keys_ptr_[index].value_count;
            std::atomic_thread_fence(std::memory_order_acquire);
            if (keys_ptr_[index].count_check == proxy_.value_count) {
                proxy_.first_value_block_offset = keys_ptr_[index].first_value_block_offset;
                proxy_.last_value_block_offset = keys_ptr_[index].last_value_block_offset;
                std::atomic_thread_fence(std::memory_order_acquire);
                if (keys_ptr_[index].value_count == proxy_.value_count) {
                    proxy_.is_block_consistent = true;
                    break;
                }
            }
        }
        return proxy_;
    }

    [[nodiscard]] size_t memory_size() const noexcept { return memory_size_; };
    [[nodiscard]] size_t key_count() const noexcept { return header_ptr_->key_count; }
    [[nodiscard]] size_t values_total_count() const noexcept { return header_ptr_->block_value_count; }
    [[nodiscard]] size_t value_memory_size() const noexcept { return header_ptr_->value_mem_size; }
    [[nodiscard]] size_t key_count_in_memory() const noexcept {
        return (memory_size_ - sizeof(key_header)) / sizeof(int64_t);
    };

private:
    mutable key_entry_proxy proxy_;
    const key_header *header_ptr_;
    const key_entry *keys_ptr_;
    size_t memory_size_;
};

template<typename T>
class block {
public:
    explicit block(const uint8_t *base_ptr, size_t offset, size_t cap) noexcept
            : base_ptr_(base_ptr), offset_(offset), cap_(cap), msk_(cap - 1) {}

    const T& operator[](size_t index) const noexcept { return data()[index & msk_]; }
    [[nodiscard]] const T* data() const noexcept { return reinterpret_cast<const T*>(memory()); }
    [[nodiscard]] const uint8_t* memory() const noexcept { return base_ptr_ + offset_; }
    [[nodiscard]] size_t offset() const noexcept { return offset_; }
    [[nodiscard]] size_t capacity() const noexcept { return cap_; }
    [[nodiscard]] size_t memory_size() const noexcept { return cap_ * sizeof(T) + sizeof(value_block_link); }
    [[nodiscard]] const uint8_t* next() const noexcept { return base_ptr_ + next_offset(); }
    [[nodiscard]] const uint8_t* prev() const noexcept { return base_ptr_ + prev_offset(); }
    [[nodiscard]] uint64_t next_offset() const noexcept { return link()->next; }
    [[nodiscard]] uint64_t prev_offset() const noexcept { return link()->prev; }
    void move_next() noexcept { offset_ = next_offset(); }
    void move_prev() noexcept { offset_ = prev_offset(); }
private:
    [[nodiscard]] const value_block_link* link() const {
        return reinterpret_cast<const value_block_link *>(memory() + memory_size() - sizeof(value_block_link));
    }
    const uint8_t *base_ptr_;
    size_t offset_;
    size_t cap_;
    size_t msk_;
};

template<typename T>
int64_t search_in_block(const T* memory, int64_t count, T value) {
    // when block is "small", we just scan it linearly
    if (count < 64) {
        // this will definitely exit because we had checked that at least the last value is greater than value
        for (long i = 0; i < count; ++i) {
            if (memory[i] > value) {
                return i;
            }
        }
        return count;
    } else {
        // use binary search on larger block
        return branch_free_search_upper(memory, count, value);
    }
}

template<typename T>
int64_t scan_blocks_backward(block<T>& current_block, int64_t value_count, T max_value) {
    int64_t stored;
    do {
        // check block range by peeking at first and last value
        auto lo = current_block[0]; // first value in the block
        stored = (value_count - 1 & static_cast<int64_t>(current_block.capacity()) - 1) + 1;

        // can we skip this block ?
        if (lo > max_value) {
            value_count -= stored;
            // do we have previous block?
            if (value_count > 0) {
                current_block.move_prev();
                continue;
            }
        }
        break;
    } while (true);

    if (value_count > 0) {
        // do we need to search this block?
        const auto hi = current_block[stored - 1]; // last value in the block
        if (max_value < hi) {
            // yes, we do
            auto index = search_in_block(current_block.data(), stored, max_value);
            value_count -= stored - index;
        }
    }
    return value_count;
}

template<typename T>
int64_t scan_blocks_forward(block<T>& current_block, int64_t initial_count, T min_value) {
    int64_t value_count = initial_count;
    int64_t stored;
    do {
        // check block range by peeking at first and last value
        stored = value_count > current_block.capacity() - 1 ? current_block.capacity() : value_count;
        const auto hi = current_block[stored - 1]; // last value in the block
        if (hi < min_value) {
            value_count -= stored;
            // do we have previous block?
            if (value_count > 0) {
                current_block.move_next();
                continue;
            }
        }
        break;
    } while (true);

    if (value_count > 0) {
        // do we need to search this block?
        const auto lo = current_block[0]; // first value in the block
        if (min_value > lo) {
            // yes, we do
            value_count -= search_in_block(current_block.data(), stored, min_value - 1);
        }
    }
    return value_count;
}

#endif //QUESTDB_BITMAP_INDEX_UTILS_H
