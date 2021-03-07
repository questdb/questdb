#include <jni.h>
#include "asmlib/asmlib.h"

extern "C" {

JNIEXPORT void JNICALL Java_io_questdb_std_Vect_memcpy0
        (JNIEnv *e, jclass cl, jlong src, jlong dst, jlong len) {
    A_memcpy(
            reinterpret_cast<void *>(dst),
            reinterpret_cast<void *>(src),
            len
    );
}

JNIEXPORT void JNICALL Java_io_questdb_std_Vect_memset
        (JNIEnv *e, jclass cl, jlong dst, jlong len, jint value) {
    A_memset(
            reinterpret_cast<void *>(dst),
            value,
            len
    );
}

}


