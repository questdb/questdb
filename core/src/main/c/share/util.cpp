#include <jni.h>
#include <cstdint>

extern "C" {
JNIEXPORT jlong JNICALL Java_io_questdb_std_Os_compareAndSwap
        (JNIEnv *e, jclass cl, jlong volatile ptr, jlong oldVal, jlong newVal) {
    return __sync_val_compare_and_swap(
            reinterpret_cast<int64_t *>(ptr),
            reinterpret_cast<int64_t>(oldVal),
            reinterpret_cast<int64_t>(newVal)
    );
}
}