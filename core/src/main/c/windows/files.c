/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

#include <unistd.h>
#include <sys/stat.h>
#include <minwindef.h>
#include <fileapi.h>

typedef HANDLE HWND;

#include <winbase.h>
#include "../share/files.h"

int set_file_pos(HANDLE fd, jlong offset) {
    if (offset < 0) {
        return 1;
    }
    long highPos = (long) (offset >> 32);
    return SetFilePointer(fd, (DWORD) offset, &highPos, FILE_BEGIN) != INVALID_SET_FILE_POINTER;
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Files_write
        (JNIEnv *e, jclass cl,
         jlong fd,
         jlong address,
         jint len,
         jlong offset) {
    DWORD count;
    return set_file_pos((HANDLE) fd, offset) &&
           WriteFile((HANDLE) fd, (LPCVOID) address, (DWORD) len, &count, NULL) ? count : 0;
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Files_read
        (JNIEnv *e, jclass cl,
         jlong fd,
         jlong address,
         jint len,
         jlong offset) {
    DWORD count;
    return set_file_pos((HANDLE) fd, offset) &&
           ReadFile((HANDLE) fd, (LPVOID) address, (DWORD) len, &count, NULL) ? count : 0;
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Files_getLastModified
        (JNIEnv *e, jclass cl, jlong pchar) {

    struct stat st;

    int r = stat((const char *) pchar, &st);
    return r == 0 ? 1000 * (jlong) st.st_mtime : r;
}

JNIEXPORT jboolean JNICALL Java_com_nfsdb_misc_Files_setLastModified
        (JNIEnv *e, jclass cl, jlong lpszName, jlong millis){

    HANDLE handle = CreateFile(
            (LPCSTR) lpszName,
            FILE_WRITE_ATTRIBUTES,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            NULL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            NULL
    );
    if (handle == INVALID_HANDLE_VALUE) {
        return 0;
    }
    millis += 11644477200000;
    millis *= 10000;
    FILETIME t;
    t.dwHighDateTime = (DWORD) ((millis >> 32) & 0xFFFFFFFF);
    t.dwLowDateTime = (DWORD) millis;
    int r = SetFileTime(handle, NULL, NULL, &t);
    CloseHandle(handle);
    return (jboolean) r;
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Files_openRO
        (JNIEnv *e, jclass cl, jlong lpszName) {
    OFSTRUCT st;
    return OpenFile((LPCSTR) lpszName, &st, OF_READ);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Files_close
        (JNIEnv *e, jclass cl, jlong fd) {
    return CloseHandle((HANDLE) fd) ? 0 : GetLastError();
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Files_openRW
        (JNIEnv *e, jclass cl, jlong lpszName) {
    return (jlong) CreateFile(
            (LPCSTR) lpszName,
            GENERIC_WRITE,
            FILE_SHARE_READ ,
            NULL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            NULL
    );
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Files_openAppend
        (JNIEnv *e, jclass cl, jlong lpszName) {
    HANDLE h = CreateFile(
            (LPCSTR) lpszName,
            FILE_APPEND_DATA,
            FILE_SHARE_READ ,
            NULL,
            OPEN_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            NULL
    );

    if (h != INVALID_HANDLE_VALUE) {
        SetFilePointer(h, 0, NULL, FILE_END);
    }

    return (jlong) h;
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Files_append
        (JNIEnv *e, jclass cl, jlong fd, jlong address, jint length) {
    DWORD count;
    return WriteFile((HANDLE) fd, (LPCVOID) address, (DWORD) length, &count, NULL) ? count : 0;
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Files_length
        (JNIEnv *e, jclass cl, jlong pchar) {
    struct stat st;

    int r = stat((const char *) pchar, &st);
    return r == 0 ? st.st_size : r;
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Files_getStdOutFd
        (JNIEnv *e, jclass cl) {
    return (jlong) GetStdHandle(STD_OUTPUT_HANDLE);
}

