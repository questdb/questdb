//
// Created by Alexander Pelagenko on 10/03/2021.
//

#ifndef ZLIB_OOO_MULTIVERSION_H
#define ZLIB_OOO_MULTIVERSION_H


#ifdef ENABLE_MULTIVERSION

#if INSTRSET<=5
#define MV_IMPL_NAME(function_name) function_name
#endif

#else
#include "jni.h"
#include "vec_dispatch.h"


//#if INSTRSET<=5
//ooMergeCopyStrColumnType F_AVX2(mergeCopyStrColumnMvManMemcopy), F_AVX512(mergeCopyStrColumnMvManMemcopy), F_VANILLA(mergeCopyStrColumnMvManMemcopy);
//auto mergeCopyStrColumn_pointer_MvManMemcopy = FunctionDispatcher<ooMergeCopyStrColumnType>::dispatch(
//        F_VANILLA(mergeCopyStrColumnMvManMemcopy),
//        F_AVX512(mergeCopyStrColumnMvManMemcopy),
//        F_AVX2(mergeCopyStrColumnMvManMemcopy)
//);
//
//extern "C"
//JNIEXPORT void JNICALL
//Java_io_questdb_std_Vect_oooMergeCopyStrColumnMvManMemcpy(JNIEnv *env, jclass cl,
//                                               jlong merge_index,
//                                               jlong merge_index_size,
//                                               jlong src_data_fix,
//                                               jlong src_data_var,
//                                               jlong src_ooo_fix,
//                                               jlong src_ooo_var,
//                                               jlong dst_fix,
//                                               jlong dst_var,
//                                               jlong dst_var_offset) {
//    mergeCopyStrColumn_pointer_MvManMemcopy(env, cl, merge_index, merge_index_size, src_data_fix, src_data_var, src_ooo_fix, src_ooo_var, dst_fix, dst_var, dst_var_offset);
//}
//#endif
//#endif

#if INSTRSET>=10
#define MV_IMPL_NAME(function_name) F_AVX512(function_name)
#elif INSTRSET>=8
#define MV_IMPL_NAME(function_name) F_AVX2(function_name)
#elif INSTRSET>=5
#define MV_IMPL_NAME(function_name) F_VANILLA(function_name)
#define MV_VANILA
#endif


#endif // ifndef

#endif //ZLIB_OOO_MULTIVERSION_H
