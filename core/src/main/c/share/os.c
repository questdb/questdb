#include <unistd.h>
#include <sys/errno.h>
#include "../share/os.h"

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Os_getPid
        (JNIEnv *e, jclass cp) {
    return getpid();
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Os_errno
        (JNIEnv *e, jclass cl) {
    return errno;
}


