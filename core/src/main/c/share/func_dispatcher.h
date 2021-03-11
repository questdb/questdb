//
// Created by Alexander Pelagenko on 10/03/2021.
//

#ifndef QUESTDB_FUNCTIONDISPATCHER_H
#define QUESTDB_FUNCTIONDISPATCHER_H

#include "vcl/instrset.h"
#include "vcl/vectorclass.h"



#if INSTRSET==0
#define MAN_MV_NAME(function_name) function_name##_impl_0
#define GET_NAME_BY_INSTSET_0(function_name, types) = &function_name<types>
#else
#define GET_NAME_BY_INSTSET_0(function_name, types)
#endif

#if INSTRSET==10
#define MAN_MV_NAME(function_name) function_name##_impl_10
#define GET_NAME_BY_INSTSET_10(function_name, types) = &function_name<types>
#else
#define GET_NAME_BY_INSTSET_10(function_name, types)
#endif

#if INSTRSET==8
#define MAN_MV_NAME(function_name) function_name##_impl_8
#define GET_NAME_BY_INSTSET_8(function_name, types) = &function_name<types>
#else
#define GET_NAME_BY_INSTSET_8(function_name, types)
#endif

#if INSTRSET==5
#define MAN_MV_NAME(function_name) function_name##_impl_5
#define GET_NAME_BY_INSTSET_5(function_name, types) = &function_name<types>
#else
#define GET_NAME_BY_INSTSET_5(function_name, types)
#endif

#if INSTRSET==2
#define MAN_MV_NAME(function_name) function_name##_impl_2
#define GET_NAME_BY_INSTSET_2(function_name, types) = &function_name<types>
#else
#define GET_NAME_BY_INSTSET_2(function_name, types)
#endif

template<typename F>
struct FunctionDispatcher {
    F *functype;
    static F *dispatch(F* vanilla, F* avx512, F* avx2) {
        const int iset = instrset_detect();
        if (iset >= 10) {
            return avx512;
        } else if (iset >= 8) {
            return avx2;
        } else {
            return vanilla;
        }
    }
};

#define DECLARE_DISPATCHER_T(dispatcher_name, method_name, method_types) \
\
FunctionDispatcher<decltype(method_name<method_types>)> dispatcher_name##_dispatcher;\
decltype(dispatcher_name##_dispatcher.functype) dispatcher_name##_var_0 GET_NAME_BY_INSTSET_0(method_name, method_types);\
decltype(dispatcher_name##_dispatcher.functype) dispatcher_name##_var_10 GET_NAME_BY_INSTSET_10(method_name, method_types);\
decltype(dispatcher_name##_dispatcher.functype) dispatcher_name##_var_8 GET_NAME_BY_INSTSET_8(method_name, method_types);\
decltype(dispatcher_name##_dispatcher.functype) dispatcher_name##_var_5 GET_NAME_BY_INSTSET_5(method_name, method_types);\
decltype(dispatcher_name##_dispatcher.functype) dispatcher_name##_var_2 GET_NAME_BY_INSTSET_2(method_name, method_types);\
\
auto dispatcher_name = dispatcher_name##_dispatcher.dispatch(dispatcher_name##_var_0,\
                                                       dispatcher_name##_var_10,\
                                                       dispatcher_name##_var_8,\
                                                       dispatcher_name##_var_5,\
                                                       dispatcher_name##_var_2);


#endif //QUESTDB_FUNCTIONDISPATCHER_H
