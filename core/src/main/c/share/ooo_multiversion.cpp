////
//// Created by Alexander Pelagenko on 10/03/2021.
////
//
//#include "func_dispatcher.h"
//#include "ooo_multiversion.h"
//
//#include "jni.h"
//#include <cstring>
//#include <xmmintrin.h>
//#include "asmlib/asmlib.h"
//
//#ifdef __APPLE__
//#define __JLONG_REINTERPRET_CAST__(type, var)  (type)var
//#else
//#define __JLONG_REINTERPRET_CAST__(type, var)  reinterpret_cast<type>(var)
//#endif
//
//typedef struct index_t {
//    uint64_t ts;
//    uint64_t i;
//
//    bool operator<(int64_t other) const {
//        return ts < other;
//    }
//
//    bool operator>(int64_t other) const {
//        return ts > other;
//    }
//
//    bool operator==(index_t other) const {
//        return ts == other.ts;
//    }
//} index_t;
//
//
//template<class T>
//inline __attribute__((always_inline))
//void merge_copy_var_column(
//        index_t *merge_index,
//        int64_t merge_index_size,
//        int64_t *src_data_fix,
//        char *src_data_var,
//        int64_t *src_ooo_fix,
//        char *src_ooo_var,
//        int64_t *dst_fix,
//        char *dst_var,
//        int64_t dst_var_offset,
//        T mult
//) {
//    int64_t *src_fix[] = {src_ooo_fix, src_data_fix};
//    char *src_var[] = {src_ooo_var, src_data_var};
//
//    for (int64_t l = 0; l < merge_index_size; l++) {
//        _mm_prefetch(merge_index + 64, _MM_HINT_T0);
//        dst_fix[l] = dst_var_offset;
//        const uint64_t row = merge_index[l].i;
//        const uint32_t bit = (row >> 63);
//        const uint64_t rr = row & ~(1ull << 63);
//        const int64_t offset = src_fix[bit][rr];
//        char *src_var_ptr = src_var[bit] + offset;
//        auto len = *reinterpret_cast<T *>(src_var_ptr);
//        auto char_count = len > 0 ? len * mult : 0;
//        reinterpret_cast<T *>(dst_var + dst_var_offset)[0] = len;
//        man_memcpy(dst_var + dst_var_offset + sizeof(T), src_var_ptr + sizeof(T), char_count);
//        dst_var_offset += char_count + sizeof(T);
//    }
//}
//
//void MV_IMPL_NAME(mergeCopyStrColumnMvManMemcopy)(JNIEnv *env, jclass cl,
//                              jlong merge_index,
//                              jlong merge_index_size,
//                              jlong src_data_fix,
//                              jlong src_data_var,
//                              jlong src_ooo_fix,
//                              jlong src_ooo_var,
//                              jlong dst_fix,
//                              jlong dst_var,
//                              jlong dst_var_offset) {
//    merge_copy_var_column<int32_t>(
//            reinterpret_cast<index_t *>(merge_index),
//            __JLONG_REINTERPRET_CAST__(int64_t, merge_index_size),
//            reinterpret_cast<int64_t *>(src_data_fix),
//            reinterpret_cast<char *>(src_data_var),
//            reinterpret_cast<int64_t *>(src_ooo_fix),
//            reinterpret_cast<char *>(src_ooo_var),
//            reinterpret_cast<int64_t *>(dst_fix),
//            reinterpret_cast<char *>(dst_var),
//            __JLONG_REINTERPRET_CAST__(int64_t, dst_var_offset),
//            2
//    );
//}
