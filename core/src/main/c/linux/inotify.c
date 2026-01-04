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
 *  See the License for the specific languageext install ms-vscode.cpptools governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#include <errno.h>
#include <jni.h>
#include <stddef.h>
#include <sys/inotify.h>
#include <sys/types.h>
#include <unistd.h>

JNIEXPORT jint JNICALL Java_io_questdb_std_filewatch_LinuxAccessor_inotifyInit(JNIEnv *e, jclass cl) {
    return (jint) inotify_init();
}

JNIEXPORT jint JNICALL
Java_io_questdb_std_filewatch_LinuxAccessor_inotifyAddWatch(JNIEnv *e, jclass cl, jint fd, jlong pathPtr, jint flags) {
    return (jint) inotify_add_watch(fd, (char *) pathPtr, flags);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_filewatch_LinuxAccessor_inotifyRmWatch(JNIEnv *e, jclass cl, jint fd, jint wd) {
    return (jshort) inotify_rm_watch(fd, wd);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_filewatch_LinuxAccessor_getINMODIFY(JNIEnv *e, jclass cl) {
    return IN_MODIFY;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_filewatch_LinuxAccessor_getINCLOSEWRITE(JNIEnv *e, jclass cl) {
    return IN_CLOSE_WRITE;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_filewatch_LinuxAccessor_getINCREATE(JNIEnv *e, jclass cl) {
    return IN_CREATE;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_filewatch_LinuxAccessor_getINMOVEDTO(JNIEnv *e, jclass cl) {
    return IN_MOVED_TO;
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_filewatch_LinuxAccessor_getEventFilenameOffset(JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct inotify_event, name);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_filewatch_LinuxAccessor_getEventFilenameSizeOffset(JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct inotify_event, len);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_filewatch_LinuxAccessor_getSizeofEvent(JNIEnv *e, jclass cl) {
    return (jshort) sizeof(struct inotify_event);
}

JNIEXPORT jint JNICALL
Java_io_questdb_std_filewatch_LinuxAccessor_readEvent(JNIEnv *e, jclass cl, jint fd, jlong buf, jint bufSize) {
    return (jint) read(fd, (void *) buf, bufSize);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_filewatch_LinuxAccessor_pipe
        (JNIEnv *e, jclass cl) {
    int fds[2];
    int res = pipe(fds);
    if (res < 0) {
        return res;
    }
    return (jlong) fds[0] << 32 | (jlong) fds[1];
}


JNIEXPORT jint JNICALL Java_io_questdb_std_filewatch_LinuxAccessor_readPipe
        (JNIEnv *e, jclass cl, jint fd) {
    char buf[1];
    ssize_t s;
    s = read((int) fd, &buf[0], 1);
    if (s != 1) {
        return -1;
    }
    return (jint) buf[0];
}

JNIEXPORT jint JNICALL Java_io_questdb_std_filewatch_LinuxAccessor_writePipe
        (JNIEnv *e, jclass cl, jint fd) {
    char buf[1];
    ssize_t s;
    buf[0] = 1;
    s = write((int) fd, &buf[0], 1);
    if (s != 1) {
        return -1;
    }
    return 0;
}
