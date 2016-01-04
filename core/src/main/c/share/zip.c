#include <stdlib.h>
#include <src/main/c/share/zlib-1.2.8/zutil.h>
#include <src/main/c/share/zip.h>

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Zip_deflateInit
        (JNIEnv *e, jclass cl, jint level, jboolean nowrap) {
    z_streamp strm = calloc(1, sizeof(z_stream));

    if (strm == 0) {
        return -1;
    }

    int ret;
    switch (ret = deflateInit2(strm, level, Z_DEFLATED, nowrap ? -MAX_WBITS : MAX_WBITS, DEF_MEM_LEVEL,
                               Z_DEFAULT_STRATEGY)) {
        case Z_OK:
            return (jlong) strm;
        default:
            free(strm);
            return ret;
    }
}

JNIEXPORT void JNICALL Java_com_nfsdb_misc_Zip_setInput
        (JNIEnv *e, jclass cl, jlong ptr, jlong address, jint available) {
    z_streamp strm = (z_streamp) ptr;
    strm->next_in = (Bytef *) address;
    strm->avail_in = (uInt) available;
}


JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Zip_deflate
        (JNIEnv *e, jclass cl, jlong ptr, jlong address, jint available, jboolean flush) {
    z_streamp strm = (z_streamp) ptr;
    strm->next_out = (Bytef *) address;
    strm->avail_out = (uInt) available;

    int ret;
    if ((ret = deflate(strm, flush ? Z_FINISH : Z_NO_FLUSH)) < 0) {
        return ret;
    }
    return (jint) (available - strm->avail_out);
}

JNIEXPORT void JNICALL Java_com_nfsdb_misc_Zip_deflateEnd
        (JNIEnv *e, jclass cl, jlong ptr) {
    z_streamp strm = (z_streamp) ptr;
    deflateEnd(strm);
    free(strm);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Zip_crc32
        (JNIEnv *e, jclass cl, jint crc, jlong address, jint available) {
    return (jint) crc32((uLong) crc, (const Bytef *) address, (uInt) available);
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Zip_inflateInit
        (JNIEnv *e, jclass cl, jboolean nowrap) {

    z_streamp strm = calloc(1, sizeof(z_stream));

    if (strm == 0) {
        return -1;
    }

    int ret;
    switch (ret = inflateInit2(strm, nowrap ? -MAX_WBITS : MAX_WBITS)) {
        case Z_OK:
            return (jlong) strm;
        default:
            free(strm);
            return ret;
    }
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Zip_inflate
        (JNIEnv *e, jclass cl, jlong ptr, jlong address, jint available, jboolean flush) {
    z_streamp strm = (z_streamp) ptr;
    strm->next_out = (Bytef *) address;
    strm->avail_out = (uInt) available;

    int ret;
    if ((ret = inflate(strm, flush ? Z_FINISH : Z_NO_FLUSH)) < 0) {
        return ret;
    }
    return (jint) (available - strm->avail_out);
}

JNIEXPORT void JNICALL Java_com_nfsdb_misc_Zip_inflateEnd
        (JNIEnv *e, jclass cl, jlong ptr) {
    z_streamp strm = (z_streamp) ptr;
    inflateEnd(strm);
    free(strm);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Zip_inflateReset
        (JNIEnv *e, jclass cl, jlong ptr) {
    return (jint) inflateReset((z_streamp) ptr);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Zip_deflateReset
        (JNIEnv *e, jclass cl, jlong ptr) {
    return (jint) deflateReset((z_streamp) ptr);
}
