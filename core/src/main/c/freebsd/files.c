/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

#include "../share/files.h"
#include <sys/mman.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
 
JNIEXPORT jlong JNICALL JavaCritical_io_questdb_std_Files_mremap0
        (jlong fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    int prot = 0;

    if (flags == com_questdb_std_Files_MAP_RO) {
        prot = PROT_READ;
    } else if (flags == com_questdb_std_Files_MAP_RW) {
        prot = PROT_READ | PROT_WRITE;
    }

    void *orgAddr = (void *) address;
    void *newAddr = mmap(orgAddr, (size_t) newLen, prot, MAP_SHARED, (int) fd, offset);
    if (orgAddr != newAddr) {
        munmap(orgAddr, (size_t) previousLen);
    }
    if (newAddr == MAP_FAILED) {
	return -1;
    }
    return (jlong) newAddr;
}
 
JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_mremap0
        (JNIEnv *e, jclass cl, jlong fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    return JavaCritical_io_questdb_std_Files_mremap0(fd, address, previousLen, newLen, offset, flags);
}

