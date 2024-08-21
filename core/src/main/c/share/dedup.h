//
// Created by Alex Pelagenko on 21/08/2024.
//

#ifndef QUESTDB_DEDUP_H
#define QUESTDB_DEDUP_H

#include <cstring>

#pragma pack (push, 1)
// Should match data structure described in DedupColumnCommitAddresses.java
struct dedup_column {
    int32_t column_type;
    int32_t value_size_bytes;
    int64_t column_top;
    void *column_data;
    void *column_var_data;
    int64_t column_var_data_len;
    void *o3_data;
    void *o3_var_data;
    int64_t o3_var_data_len;
    int64_t java_reserved_1;
    int64_t java_reserved_2;
    int64_t java_reserved_3;
    int64_t java_reserved_4;
    int64_t java_reserved_5;
    char null_value[32];
};
#pragma pack(pop)

#endif //QUESTDB_DEDUP_H
