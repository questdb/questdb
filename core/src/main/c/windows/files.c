/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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
 ******************************************************************************/

#include <unistd.h>
#include <sys/stat.h>
#include <minwindef.h>
#include <fileapi.h>

typedef HANDLE HWND;

#include <winbase.h>
#include <direct.h>
#include "../share/files.h"

int set_file_pos(HANDLE fd, jlong offset) {
    if (offset < 0) {
        return 1;
    }
    long highPos = (long) (offset >> 32);
    return SetFilePointer(fd, (DWORD) offset, &highPos, FILE_BEGIN) != INVALID_SET_FILE_POINTER;
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_write
        (JNIEnv *e, jclass cl,
         jlong fd,
         jlong address,
         jlong len,
         jlong offset) {
    DWORD count;
    return set_file_pos((HANDLE) fd, offset) &&
           WriteFile((HANDLE) fd, (LPCVOID) address, (DWORD) len, &count, NULL) ? count : 0;
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_read
        (JNIEnv *e, jclass cl,
         jlong fd,
         jlong address,
         jlong len,
         jlong offset) {
    DWORD count;
    return set_file_pos((HANDLE) fd, offset) &&
           ReadFile((HANDLE) fd, (LPVOID) address, (DWORD) len, &count, NULL) ? count : 0;
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_sequentialRead
        (JNIEnv *e, jclass cl, jlong fd, jlong address, jint len) {
    DWORD count;
    return ReadFile((HANDLE) fd, (LPVOID) address, (DWORD) len, &count, NULL) ? count : 0;

}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_getLastModified
        (JNIEnv *e, jclass cl, jlong pchar) {

    TIME_ZONE_INFORMATION tz;
    LONG bias;

    switch (GetTimeZoneInformation(&tz)) {
        case TIME_ZONE_ID_STANDARD:
            bias = tz.StandardBias;
            break;
        case TIME_ZONE_ID_DAYLIGHT:
            bias = tz.DaylightBias;
            break;
        default:
            bias = 0;
    }
    if (bias != 0) {
        bias *= 60000L;
    }
    struct stat st;
    int r = stat((const char *) pchar, &st);
    return r == 0 ? (1000 * (jlong) st.st_mtime) + bias : r;
}

JNIEXPORT jboolean JNICALL Java_com_questdb_misc_Files_setLastModified
        (JNIEnv *e, jclass cl, jlong lpszName, jlong millis) {

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

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_length0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    struct stat st;
    int r = stat((const char *) lpszName, &st);
    return r == 0 ? st.st_size : r;
}

JNIEXPORT jint JNICALL Java_com_questdb_misc_Files_mkdir
        (JNIEnv *e, jclass cl, jlong lpszName, jint mode) {
    return _mkdir((const char *) lpszName);
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_openRO
        (JNIEnv *e, jclass cl, jlong lpszName) {
    return (jlong) CreateFile(
            (LPCSTR) lpszName,
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            NULL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            NULL
    );
}

JNIEXPORT jint JNICALL Java_com_questdb_misc_Files_close
        (JNIEnv *e, jclass cl, jlong fd) {
    return CloseHandle((HANDLE) fd) ? 0 : GetLastError();
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_dup
        (JNIEnv *e, jclass cl, jlong fd) {
    return _dup((int) fd);
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_openRW
        (JNIEnv *e, jclass cl, jlong lpszName) {
    return (jlong) CreateFile(
            (LPCSTR) lpszName,
            GENERIC_WRITE | GENERIC_READ,
            FILE_SHARE_READ,
            NULL,
            OPEN_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            NULL
    );
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_openAppend
        (JNIEnv *e, jclass cl, jlong lpszName) {
    HANDLE h = CreateFile(
            (LPCSTR) lpszName,
            FILE_APPEND_DATA,
            FILE_SHARE_READ,
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

JNIEXPORT void JNICALL Java_com_questdb_misc_Files_append
        (JNIEnv *e, jclass cl, jlong fd, jlong address, jint length) {
    DWORD count;
    WriteFile((HANDLE) fd, (LPCVOID) address, (DWORD) length, &count, NULL) ? count : 0;
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_length
        (JNIEnv *e, jclass cl, jlong fd) {
    DWORD len;
    GetFileSize((HANDLE) fd, &len);
    return len;
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_getStdOutFd
        (JNIEnv *e, jclass cl) {
    return (jlong) GetStdHandle(STD_OUTPUT_HANDLE);
}

JNIEXPORT jboolean JNICALL Java_com_questdb_misc_Files_truncate
        (JNIEnv *e, jclass cl, jlong handle, jlong size) {
    if (set_file_pos((HANDLE) handle, size)) {
        return (jboolean) SetEndOfFile((HANDLE) handle);
    }
    return FALSE;
}

JNIEXPORT jint JNICALL Java_com_questdb_misc_Files_munmap0
        (JNIEnv *e, jclass cl, jlong address, jlong len) {
    if (UnmapViewOfFile((LPCVOID) address) == 0) {
        return -1;
    } else {
        return 0;
    }
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_mmap0
        (JNIEnv *e, jclass cl, jlong fd, jlong len, jlong offset, jint flags) {
    jlong maxsize = offset + len;
    DWORD flProtect;
    DWORD dwDesiredAccess;
    LPCVOID address;

    if (flags == com_questdb_misc_Files_MAP_RW) {
        flProtect = PAGE_READWRITE;
        dwDesiredAccess = FILE_MAP_WRITE;
    } else {
        flProtect = PAGE_READONLY;
        dwDesiredAccess = FILE_MAP_READ;
    }

    HANDLE hMapping = CreateFileMapping((HANDLE) fd, NULL, flProtect, (DWORD) (maxsize >> 32), (DWORD) maxsize, NULL);
    if (hMapping == NULL) {
        printf("no mapping: %lu", GetLastError());
        return -1;
    }

    address = MapViewOfFile(hMapping, dwDesiredAccess, (DWORD) (offset >> 32), (DWORD) offset, (SIZE_T) len);

    unsigned long err = GetLastError();
    if (CloseHandle(hMapping) == 0) {
        printf("cant close: %lu", GetLastError());
        if (address != NULL) {
            UnmapViewOfFile(address);
        }
        return -1;
    }

    if (address == NULL) {
        printf("no view: %lu", err);
        return -1;
    }

    return (jlong) address;
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_getPageSize
        (JNIEnv *e, jclass cl) {
    SYSTEM_INFO siSysInfo;
    GetSystemInfo(&siSysInfo);
    return siSysInfo.dwAllocationGranularity;
}

JNIEXPORT jboolean JNICALL Java_com_questdb_misc_Files_remove
        (JNIEnv *e, jclass cl, jlong lpsz) {
    return (jboolean) DeleteFile((LPCSTR) lpsz);
}

JNIEXPORT jboolean JNICALL Java_com_questdb_misc_Files_rmdir
        (JNIEnv *e, jclass cl, jlong lpsz) {
    return (jboolean) RemoveDirectoryA((LPCSTR) lpsz);
}

typedef struct {
    WIN32_FIND_DATAA *find_dataa;
    HANDLE hFind;
} FIND;

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_findFirst
        (JNIEnv *e, jclass cl, jlong lpszName) {

    FIND *find = malloc(sizeof(FIND));
    find->find_dataa = malloc(sizeof(WIN32_FIND_DATAA));

    char path[2048];
    sprintf(path, "%s\\*.*", (char *) lpszName);
    find->hFind = FindFirstFile(path, find->find_dataa);
    if (find->hFind == INVALID_HANDLE_VALUE) {
        free(find->find_dataa);
        free(find);
        return 0;
    }
    return (jlong) find;
}

JNIEXPORT jboolean JNICALL Java_com_questdb_misc_Files_findNext
        (JNIEnv *e, jclass cl, jlong findPtr) {
    FIND *find = (FIND *) findPtr;
    return (jboolean) FindNextFile(find->hFind, find->find_dataa);
}

JNIEXPORT void JNICALL Java_com_questdb_misc_Files_findClose
        (JNIEnv *e, jclass cl, jlong findPtr) {
    FIND *find = (FIND *) findPtr;
    FindClose(find->hFind);
    free(find->find_dataa);
    free(find);
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Files_findName
        (JNIEnv *e, jclass cl, jlong findPtr) {
    return (jlong) ((FIND *) findPtr)->find_dataa->cFileName;
}


JNIEXPORT jint JNICALL Java_com_questdb_misc_Files_findType
        (JNIEnv *e, jclass cl, jlong findPtr) {
    return ((FIND *) findPtr)->find_dataa->dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY ?
           com_questdb_misc_Files_DT_DIR : com_questdb_misc_Files_DT_REG;
}

JNIEXPORT jboolean JNICALL Java_com_questdb_misc_Files_rename
        (JNIEnv *e, jclass cl, jlong lpszOld, jlong lpszNew) {
    BOOL r = MoveFile((LPCSTR) lpszOld, (LPCSTR) lpszNew);
    if (!r) {
        printf("error: %lu\n\n", GetLastError());
    }
    return (jboolean) r;
}
