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

#include <shlwapi.h>
#include <minwindef.h>
#include <fileapi.h>
#include <winbase.h>
#include <stdint.h>
#include "../share/files.h"
#include "errno.h"
#include "files.h"

typedef struct {
    HANDLE hDir;
    char utf8Name[UTF8_MAX_PATH];
    int utf8NameSize;
    FILE_NOTIFY_INFORMATION *pfni;
    int pfniSize;
} FILEWATCH;

void _stopWatch(FILEWATCH *pwatch);

JNIEXPORT jlong JNICALL
Java_io_questdb_std_filewatch_WindowsAccessorImpl_openDirectory0(JNIEnv *e, jclass cls, jlong lpszDirName) {

    int dirNameLen = MultiByteToWideChar(
            CP_UTF8,
            MB_ERR_INVALID_CHARS,
            (LPCCH) lpszDirName,
            -1,
            NULL,
            0
    );

    if (dirNameLen > -1) {
        wchar_t buf[dirNameLen];
        MultiByteToWideChar(
                CP_UTF8,
                0,
                (LPCCH) lpszDirName,
                -1,
                buf,
                dirNameLen
        );

        HANDLE hDir = CreateFileW(
                buf,
                FILE_LIST_DIRECTORY,
                FILE_SHARE_READ
                | FILE_SHARE_WRITE
                | FILE_SHARE_DELETE,
                0,
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS
                | FILE_FLAG_OVERLAPPED,
                0
        );

        if (hDir == INVALID_HANDLE_VALUE) {
            SaveLastError();
            return -1;
        }

        FILEWATCH *pfilewatch = malloc(sizeof(FILEWATCH));
        pfilewatch->pfniSize = sizeof(FILE_NOTIFY_INFORMATION) + UTF8_MAX_PATH - 1;
        pfilewatch->pfni = malloc(pfilewatch->pfniSize);
        pfilewatch->hDir = hDir;
        memset(pfilewatch->utf8Name, 0, UTF8_MAX_PATH);

        return (jlong) pfilewatch;
    } else {
        SaveLastError();
        return -1;
    }
}


JNIEXPORT jboolean JNICALL Java_io_questdb_std_filewatch_WindowsAccessorImpl_readDirectoryChanges0
        (JNIEnv *e, jclass cls, jlong pfilewatch) {

    FILEWATCH *pwatch = (FILEWATCH *) pfilewatch;

    DWORD bytesReturned;

    memset(pwatch->pfni, 0, pwatch->pfniSize);
    boolean b = ReadDirectoryChangesW(
            pwatch->hDir,
            pwatch->pfni,
            pwatch->pfniSize,
            FALSE,
            FILE_NOTIFY_CHANGE_LAST_WRITE | FILE_NOTIFY_CHANGE_CREATION | FILE_NOTIFY_CHANGE_FILE_NAME,
            &bytesReturned,
            NULL,
            NULL
    );

    if (b) {
        memset(pwatch->utf8Name, 0, UTF8_MAX_PATH);
        int len = WideCharToMultiByte(
                CP_UTF8,
                0,
                &pwatch->pfni->FileName[0],
                -1,
                pwatch->utf8Name,
                UTF8_MAX_PATH,
                NULL,
                NULL
        );
        if (len >= 0) {
            // len includes 0 terminator
            pwatch->utf8NameSize = len - 1;
        } else {
            SaveLastError();
            return FALSE;
        }
    } else {
        SaveLastError();
    }
    return b;
}

void _stopWatch(FILEWATCH *pwatch) {
    if (pwatch->hDir != 0) {
        CloseHandle(pwatch->hDir);
        pwatch->hDir = 0;
    }
}

JNIEXPORT void JNICALL
Java_io_questdb_std_filewatch_WindowsAccessorImpl_stopWatch0(JNIEnv *e, jclass cls, jlong pfilewatch) {
    _stopWatch((FILEWATCH *) pfilewatch);
}


JNIEXPORT jlong JNICALL
Java_io_questdb_std_filewatch_WindowsAccessorImpl_getFileName0(JNIEnv *e, jclass cls, jlong pfilewatch) {
    FILEWATCH *pwatch = (FILEWATCH *) pfilewatch;
    return (jlong) pwatch->utf8Name;
}

JNIEXPORT jint JNICALL
Java_io_questdb_std_filewatch_WindowsAccessorImpl_getFileNameSize0(JNIEnv *e, jclass cls, jlong pfilewatch) {
    FILEWATCH *pwatch = (FILEWATCH *) pfilewatch;
    return (jint) pwatch->utf8NameSize;
}

JNIEXPORT void JNICALL
Java_io_questdb_std_filewatch_WindowsAccessorImpl_closeDirectory0(JNIEnv *e, jclass cls, jlong pfilewatch) {
    FILEWATCH *pwatch = (FILEWATCH *) pfilewatch;
    _stopWatch(pwatch);
    free(pwatch->pfni);
    free(pwatch);
}
