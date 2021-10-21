/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

#define _GNU_SOURCE
#include "../share/files.h"
#include <sys/mman.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <sys/sendfile.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/fcntl.h>


static inline jlong _io_questdb_std_Files_mremap0
        (jlong fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    void *orgAddr = (void *) address;
    void *newAddr = mremap(orgAddr, (size_t) previousLen, (size_t) newLen, MREMAP_MAYMOVE);
    if (newAddr == MAP_FAILED) {
        return -1;
    }
    return (jlong) newAddr;
}
 
JNIEXPORT jlong JNICALL JavaCritical_io_questdb_std_Files_mremap0
        (jlong fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    return _io_questdb_std_Files_mremap0(fd, address, previousLen, newLen, offset, flags);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_mremap0
        (JNIEnv *e, jclass cl, jlong fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    return _io_questdb_std_Files_mremap0(fd, address, previousLen, newLen, offset, flags);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_copy
        (JNIEnv *e, jclass cls, jlong lpszFrom, jlong lpszTo) {
    const char* from = (const char *) lpszFrom;
    const char* to = (const char *) lpszTo;
    const int input = open(from, O_RDONLY);
    if (-1 ==  (input)) {
        return -1;
    }

    const int output = creat(to, 0644);
    if (-1 == (output)) {
        close(input);
        return -1;
    }

    // On linux sendfile can accept file as well as sockets
    off_t offset = 0;
    struct stat fileStat = {0};
    fstat(input, &fileStat);
    int result = sendfile(output, input, &offset, fileStat.st_size);

    close(input);
    close(output);

    return result;
}
