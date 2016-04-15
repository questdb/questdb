/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
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

