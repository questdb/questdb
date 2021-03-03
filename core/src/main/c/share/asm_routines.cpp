// Define type size_t
#ifndef _SIZE_T_DEFINED
#include "stddef.h"
#endif

void * A_memcpy (void * dest, const void * src, size_t count) {
    char* destArr =(char*) dest;
    char* srcArr =(char*) src;
    for(size_t i = 0; i < count; i++) {
        destArr[i] = srcArr[i];
    }
    return dest;
}