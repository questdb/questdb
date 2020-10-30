#include <jni.h>
#include <cstdint>

#define _GNU_SOURCE

extern "C" {

JNIEXPORT jlong JNICALL Java_io_questdb_std_Os_cas
        (JNIEnv *e, jclass cl, jlong ptr, jlong oldVal, jlong newVal) {
    return __sync_val_compare_and_swap(
            reinterpret_cast<int64_t *>(ptr),
            reinterpret_cast<int64_t>(oldVal),
            reinterpret_cast<int64_t>(newVal)
    );
}

}