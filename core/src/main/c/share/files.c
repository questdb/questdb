#include <unistd.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include "files.h"

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Files_write
        (JNIEnv *e, jclass cl,
         jint fd,
         jlong address,
         jint len,
         jlong offset) {

    return pwrite(fd, (void *) (address), (size_t) len, (off_t) offset);
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Files_read
        (JNIEnv *e, jclass cl,
         jint fd,
         jlong address,
         jint len,
         jlong offset) {

    return pread(fd, (void *) address, (size_t) len, (off_t) offset);
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Files_lastModified
        (JNIEnv *e, jclass cl, jlong pchar) {

    struct stat st;

    int r = stat((const char *) pchar, &st);
    return r == 0 ? 1000 * (jlong) st.st_mtime : r;
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Files_openRO
        (JNIEnv *e, jclass cl, jlong lpszName) {
    return open((const char *) lpszName, O_RDONLY);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Files_close
        (JNIEnv *e, jclass cl, jint fd) {
    return close(fd);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Files_openRW
        (JNIEnv *e, jclass cl, jlong lpszName) {
    return open((const char *) lpszName, O_CREAT | O_RDWR);
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Files_length
        (JNIEnv *e, jclass cl, jlong pchar) {
    struct stat st;

    int r = stat((const char *) pchar, &st);
    return r == 0 ? st.st_size : r;
}
