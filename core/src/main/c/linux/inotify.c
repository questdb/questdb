/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

#include <jni.h>
#include <sys/inotify.h>
#include <unistd.h>

JNIEXPORT jlong JNICALL Java_io_questdb_InotifyAccessor_inotifyInit(JNIEnv *e, jclass cl)
{
    return (jlong)inotify_init();
}

JNIEXPORT jlong JNICALL Java_io_questdb_InotifyAccessor_inotifyAddWatch(JNIEnv *e, jclass cl, jlong fd, jlong pathPtr, jint flags)
{
    return (jlong)inotify_add_watch(
        fd,
        (char *)pathPtr,
        flags);
}

JNIEXPORT jshort JNICALL Java_io_questdb_InotifyAccessor_inotifyRmWatch(JNIEnv *e, jclass cl, jlong fd, jlong wd)
{
    return (jshort)inotify_rm_watch(
        fd,
        wd);
}

JNIEXPORT jshort JNICALL Java_io_questdb_InotifyAccessor_closeFd(JNIEnv *e, jclass cl, jlong fd)
{
    return (jshort)close(fd);
}

JNIEXPORT jint JNICALL Java_io_questdb_InotifyAccessor_getINMODIFY(JNIEnv *e, jclass cl)
{
    return IN_MODIFY;
}
JNIEXPORT jint JNICALL Java_io_questdb_InotifyAccessor_getINCLOSEWRITE(JNIEnv *e, jclass cl)
{
    return IN_CLOSE_WRITE;
}
JNIEXPORT jint JNICALL Java_io_questdb_InotifyAccessor_getINCREATE(JNIEnv *e, jclass cl)
{
    return IN_CREATE;
}
JNIEXPORT jint JNICALL Java_io_questdb_InotifyAccessor_getINMOVEDTO(JNIEnv *e, jclass cl)
{
    return IN_MOVED_TO;
}
