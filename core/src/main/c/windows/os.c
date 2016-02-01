
#include <processthreadsapi.h>

typedef HANDLE HWND;

#include <stdbool.h>
#include <intrin.h>
#include "../share/os.h"

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Os_getPid
        (JNIEnv *e, jclass cl) {
    return GetCurrentProcessId();
}


