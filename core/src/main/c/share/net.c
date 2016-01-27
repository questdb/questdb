#include <darwin/jni_md.h>
#include <jni.h>
#include <sys/socket.h>
#include <sys/fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/errno.h>
#include "net.h"

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_socketTcp
        (JNIEnv *e, jobject cl, jboolean blocking) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd > 0 && !blocking) {
        fcntl(fd, F_SETFL, O_NONBLOCK);
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



