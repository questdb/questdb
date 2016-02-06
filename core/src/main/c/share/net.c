#include <darwin/jni_md.h>
#include <jni.h>
#include <sys/socket.h>
#include <sys/fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "net.h"

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_socketTcp
        (JNIEnv *e, jobject cl, jboolean blocking) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd > 0 && !blocking) {
        if (fcntl(fd, F_SETFL, O_NONBLOCK) < 0) {
            close(fd);
            return -1;
        }

        int oni = 1;
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *) &oni, sizeof(oni)) < 0) {
            close(fd);
            return -1;
        }
    }
    return fd;
}

JNIEXPORT jboolean JNICALL Java_com_nfsdb_misc_Net_bind
        (JNIEnv *e, jobject cl, jint fd, jint address, jint port) {
    struct sockaddr_in addr;

    addr.sin_family = AF_INET;

    addr.sin_addr.s_addr = htonl(address);
    addr.sin_port = htons(port);

    return (jboolean) (bind(fd, (struct sockaddr *) &addr, sizeof(addr)) == 0);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_accept
        (JNIEnv *e, jobject cl, jint fd) {
    return accept(fd, NULL, NULL);
}

JNIEXPORT void JNICALL Java_com_nfsdb_misc_Net_listen
        (JNIEnv *e, jclass cl, jint fd, jint backlog) {
    listen(fd, backlog);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_send
        (JNIEnv *e, jclass cl, jlong fd, jlong ptr, jint len) {
    return (jint) send((int) fd, (const void *) ptr, (size_t) len, 0);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_recv
        (JNIEnv *e, jclass cl, jlong fd, jlong ptr, jint len) {
    return (jint) recv((int) fd, (void *) ptr, (size_t) len, 0);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_configureNonBlocking
        (JNIEnv *e, jclass cl, jlong fd) {
    int flags;


    if ((flags = fcntl((int) fd, F_GETFL, 0)) < 0) {
        return flags;
    }


    if ((flags = fcntl((int) fd, F_SETFL, flags | O_NONBLOCK)) < 0) {
        return flags;
    }

    return 0;
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_setSndBuf
        (JNIEnv *e, jclass cl, jlong fd, jint size) {
    jint sz = size;
    return setsockopt((int) fd, SOL_SOCKET, SO_SNDBUF, (char *) &sz, sizeof(sz));
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_setRcvBuf
        (JNIEnv *e, jclass cl, jlong fd, jint size) {
    jint sz = size;
    return setsockopt((int) fd, SOL_SOCKET, SO_RCVBUF, (char *) &sz, sizeof(sz));
}
