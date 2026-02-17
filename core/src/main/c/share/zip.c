/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#include <stdlib.h>
#include <src/main/c/share/zlib/zutil.h>
#include <src/main/c/share/zip.h>

JNIEXPORT jlong JNICALL Java_io_questdb_std_Zip_deflateInit
        (JNIEnv *e, jclass cl) {
    z_streamp strm = calloc(1, sizeof(z_stream));

    if (strm == 0) {
        return -1;
    }

    int ret;
    switch (ret = deflateInit2(strm, -1, Z_DEFLATED, -MAX_WBITS, DEF_MEM_LEVEL, Z_DEFAULT_STRATEGY)) {
        case Z_OK:
            return (jlong) strm;
        default:
            free(strm);
            return ret;
    }
}

JNIEXPORT void JNICALL Java_io_questdb_std_Zip_setInput
        (JNIEnv *e, jclass cl, jlong ptr, jlong address, jint available) {
    z_streamp strm = (z_streamp) ptr;
    strm->next_in = (Bytef *) address;
    strm->avail_in = (uInt) available;
}


JNIEXPORT jint JNICALL Java_io_questdb_std_Zip_deflate
        (JNIEnv *e, jclass cl, jlong ptr, jlong address, jint available, jboolean flush) {
    z_streamp strm = (z_streamp) ptr;
    strm->next_out = (Bytef *) address;
    strm->avail_out = (uInt) available;
    return deflate(strm, flush ? Z_FINISH : Z_NO_FLUSH);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Zip_availIn
        (JNIEnv *e, jclass cl, jlong ptr) {
    return (jint) ((z_streamp) ptr)->avail_in;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Zip_availOut
        (JNIEnv *e, jclass cl, jlong ptr) {
    return (jint) ((z_streamp) ptr)->avail_out;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Zip_totalOut
        (JNIEnv *e, jclass cl, jlong ptr) {
    return (jint) ((z_streamp) ptr)->total_out;
}

JNIEXPORT void JNICALL Java_io_questdb_std_Zip_deflateEnd
        (JNIEnv *e, jclass cl, jlong ptr) {
    z_streamp strm = (z_streamp) ptr;
    deflateEnd(strm);
    free(strm);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Zip_crc32
        (JNIEnv *e, jclass cl, jint crc, jlong address, jint available) {
    return (jint) crc32((uLong) crc, (const Bytef *) address, (uInt) available);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Zip_inflateInit
        (JNIEnv *e, jclass cl, jboolean nowrap) {

    z_streamp strm = calloc(1, sizeof(z_stream));

    if (strm == 0) {
        return -42; // avoid clashes with Z_*_ERROR consts
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

JNIEXPORT jlong JNICALL Java_io_questdb_std_Zip_inflateInitGzip
        (JNIEnv *e, jclass cl) {

    z_streamp strm = calloc(1, sizeof(z_stream));

    if (strm == 0) {
        return -1;
    }

    int ret;
    // 16+MAX_WBITS => gzip-only (per zlib docs)
    switch (ret = inflateInit2(strm, 16+MAX_WBITS)) {
        case Z_OK:
            return (jlong) strm;
        default:
            free(strm);
            return ret;
    }
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Zip_inflate
        (JNIEnv *e, jclass cl, jlong ptr, jlong address, jint available, jboolean flush) {
    z_streamp strm = (z_streamp) ptr;
    strm->next_out = (Bytef *) address;
    strm->avail_out = (uInt) available;
    return inflate(strm, flush ? Z_FINISH : Z_NO_FLUSH);
}

JNIEXPORT void JNICALL Java_io_questdb_std_Zip_inflateEnd
        (JNIEnv *e, jclass cl, jlong ptr) {
    z_streamp strm = (z_streamp) ptr;
    inflateEnd(strm);
    free(strm);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Zip_inflateReset
        (JNIEnv *e, jclass cl, jlong ptr) {
    return (jint) inflateReset((z_streamp) ptr);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Zip_deflateReset
        (JNIEnv *e, jclass cl, jlong ptr) {
    return (jint) deflateReset((z_streamp) ptr);
}
