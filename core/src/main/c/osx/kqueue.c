#include <sys/event.h>
#include <sys/time.h>
#include <stddef.h>
#include <sys/errno.h>
#include "kqueue.h"

JNIEXPORT jshort JNICALL Java_com_nfsdb_net_Kqueue_getEvfiltRead
        (JNIEnv *e, jclass cl) {
    return EVFILT_READ;
}

JNIEXPORT jshort JNICALL Java_com_nfsdb_net_Kqueue_getEvfiltWrite
        (JNIEnv *e, jclass cl) {
    return EVFILT_WRITE;
}

JNIEXPORT jshort JNICALL Java_com_nfsdb_net_Kqueue_getSizeofKevent
        (JNIEnv *e, jclass cl) {
    return (short) sizeof(struct kevent);
}

JNIEXPORT jshort JNICALL Java_com_nfsdb_net_Kqueue_getFdOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, ident);
}

JNIEXPORT jshort JNICALL Java_com_nfsdb_net_Kqueue_getFilterOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, filter);
}

JNIEXPORT jshort JNICALL Java_com_nfsdb_net_Kqueue_getFlagsOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, flags);
}

JNIEXPORT jshort JNICALL Java_com_nfsdb_net_Kqueue_getEvAdd
        (JNIEnv *e, jclass cl) {
    return EV_ADD;
}

JNIEXPORT jshort JNICALL Java_com_nfsdb_net_Kqueue_getEvOneshot
        (JNIEnv *e, jclass cl) {
    return EV_ONESHOT;
}


JNIEXPORT jint JNICALL Java_com_nfsdb_net_Kqueue_kqueue
        (JNIEnv *e, jclass cl) {
    return kqueue();
}

JNIEXPORT jint JNICALL Java_com_nfsdb_net_Kqueue_kevent
        (JNIEnv *e, jclass cl, jint kq, jlong changelist, jint nChanges, jlong eventlist, jint nEvents) {
    struct timespec dontBlock = {0, 0};
    return (jint) kevent(kq, (const struct kevent *) changelist, nChanges, (struct kevent *) eventlist, nEvents,
                         &dontBlock);
}



