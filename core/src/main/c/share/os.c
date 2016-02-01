#include <unistd.h>
#include "../share/os.h"

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Os_getPid
        (JNIEnv *e, jclass cp) {
    return getpid();
}

