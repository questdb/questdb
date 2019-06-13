/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

#define _WIN32_WINNT 0x600 /* GetFileInformationByHandleEx is Vista+ */

#include <shlwapi.h>
#include <unistd.h>
#include <sys/stat.h>
#include <minwindef.h>
#include <fileapi.h>

//typedef HANDLE HWND;

#include <winbase.h>
#include <direct.h>
#include "../share/files.h"
#include "errno.h"

int set_file_pos(HANDLE fd, jlong offset) {
    if (offset < 0) {
        return 1;
    }
    long highPos = (long) (offset >> 32);
    int r = SetFilePointer(fd, (DWORD) offset, &highPos, FILE_BEGIN) != INVALID_SET_FILE_POINTER;
    if (r == INVALID_SET_FILE_POINTER) {
        SaveLastError();
    }
    return r;
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_write
        (JNIEnv *e, jclass cl,
         jlong fd,
         jlong address,
         jlong len,
         jlong offset) {
    DWORD count;
    if (set_file_pos((HANDLE) fd, offset) && WriteFile((HANDLE) fd, (LPCVOID) address, (DWORD) len, &count, NULL)) {
        return count;
    }
    SaveLastError();
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_read
        (JNIEnv *e, jclass cl,
         jlong fd,
         jlong address,
         jlong len,
         jlong offset) {
    DWORD count;
    if (set_file_pos((HANDLE) fd, offset) &&
        ReadFile((HANDLE) fd, (LPVOID) address, (DWORD) len, &count, NULL)) {
        return count;
    }
    SaveLastError();
    return 0;
}

#define MILLIS_SINCE_1970 11644473600000

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_getLastModified
        (JNIEnv *e, jclass cl, jlong lpszName) {

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
        SaveLastError();
        return -1;
    }

    FILETIME creation;
    FILETIME access;
    FILETIME write;

    if (GetFileTime(handle, &creation, &access, &write)) {
        CloseHandle(handle);
        return  ((((jlong)write.dwHighDateTime) << 32) | write.dwLowDateTime) / 10000 - MILLIS_SINCE_1970;
    }
    CloseHandle(handle);
    SaveLastError();
    return -1;
}

JNIEXPORT jboolean JNICALL
Java_com_questdb_std_Files_exists0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    return PathFileExistsA((LPCSTR) lpszName);
}

JNIEXPORT jboolean JNICALL Java_com_questdb_std_Files_setLastModified
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
        SaveLastError();
        return 0;
    }

    millis += MILLIS_SINCE_1970; // millis between 1601-01-01 and 1970-01-01
    millis *= 10000;
    FILETIME t;
    t.dwHighDateTime = (DWORD) ((millis >> 32) & 0xFFFFFFFF);
    t.dwLowDateTime = (DWORD) millis;
    int r = SetFileTime(handle, NULL, NULL, &t);
    CloseHandle(handle);
    return (jboolean) r;
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_length0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    struct stat st;
    int r = stat((const char *) lpszName, &st);
    if (r == 0) {
        return st.st_size;
    }
    SaveLastError();
    return r;
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Files_mkdir
        (JNIEnv *e, jclass cl, jlong lpszName, jint mode) {
    jint r = _mkdir((const char *) lpszName);
    if (r != 0) {
        SaveLastError();
    }
    return r;
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_openRO
        (JNIEnv *e, jclass cl, jlong lpszName) {
    HANDLE fd = CreateFile(
            (LPCSTR) lpszName,
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            NULL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            NULL
    );

    if (fd == INVALID_HANDLE_VALUE) {
        SaveLastError();
    }
    return (jlong) fd;
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Files_close0
        (JNIEnv *e, jclass cl, jlong fd) {
    jint r = CloseHandle((HANDLE) fd);
    if (!r) {
        SaveLastError();
        return -1;
    }
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_dup
        (JNIEnv *e, jclass cl, jlong fd) {
    return _dup((int) fd);
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_openRW
        (JNIEnv *e, jclass cl, jlong lpszName) {
    //NtOpenFile()
    HANDLE r = CreateFile(
            (LPCSTR) lpszName,
            GENERIC_WRITE | GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_DELETE,
            NULL,
            OPEN_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            NULL
    );

    if (r == INVALID_HANDLE_VALUE) {
        SaveLastError();
    }
    return (jlong) r;
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_openAppend
        (JNIEnv *e, jclass cl, jlong lpszName) {
    HANDLE h = CreateFile(
            (LPCSTR) lpszName,
            FILE_APPEND_DATA,
            FILE_SHARE_READ | FILE_SHARE_DELETE,
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

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_append
        (JNIEnv *e, jclass cl,
         jlong fd,
         jlong address,
         jlong len) {
    DWORD count;
    if (WriteFile((HANDLE) fd, (LPCVOID) address, (DWORD) len, &count, NULL)) {
        return count;
    }

    SaveLastError();
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_length
        (JNIEnv *e, jclass cl, jlong fd) {
    DWORD high;
    DWORD low = GetFileSize((HANDLE) fd, &high);
    if (low != INVALID_FILE_SIZE) {
        return low | (__int64) high << 32;
    } else {
        SaveLastError();
        return INVALID_FILE_SIZE;
    }
}

JNIEXPORT jboolean JNICALL Java_com_questdb_std_Files_exists
        (JNIEnv *e, jclass cl, jlong fd) {

    FILE_STANDARD_INFO info;
    if (GetFileInformationByHandleEx((HANDLE) fd, FileStandardInfo, &info, sizeof(FILE_STANDARD_INFO))) {
        return (jboolean) !info.DeletePending;
    }
    return FALSE;
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_getStdOutFd
        (JNIEnv *e, jclass cl) {
    return (jlong) GetStdHandle(STD_OUTPUT_HANDLE);
}

JNIEXPORT jboolean JNICALL Java_com_questdb_std_Files_truncate
        (JNIEnv *e, jclass cl, jlong handle, jlong size) {
    if (set_file_pos((HANDLE) handle, size) && SetEndOfFile((HANDLE) handle)) {
        return TRUE;
    }
    SaveLastError();
    return FALSE;
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Files_munmap0
        (JNIEnv *e, jclass cl, jlong address, jlong len) {
    if (UnmapViewOfFile((LPCVOID) address) == 0) {
        SaveLastError();
        return -1;
    } else {
        return 0;
    }
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_mmap0
        (JNIEnv *e, jclass cl, jlong fd, jlong len, jlong offset, jint flags) {
    jlong maxsize = offset + len;
    DWORD flProtect;
    DWORD dwDesiredAccess;
    LPCVOID address;

    if (flags == com_questdb_std_Files_MAP_RW) {
        flProtect = PAGE_READWRITE;
        dwDesiredAccess = FILE_MAP_WRITE;
    } else {
        flProtect = PAGE_READONLY;
        dwDesiredAccess = FILE_MAP_READ;
    }

    HANDLE hMapping = CreateFileMapping((HANDLE) fd, NULL, flProtect | SEC_RESERVE, (DWORD) (maxsize >> 32),
                                        (DWORD) maxsize, NULL);
    if (hMapping == NULL) {
        SaveLastError();
        return -1;
    }

    address = MapViewOfFile(hMapping, dwDesiredAccess, (DWORD) (offset >> 32), (DWORD) offset, (SIZE_T) len);

    SaveLastError();

    if (CloseHandle(hMapping) == 0) {
        SaveLastError();
        if (address != NULL) {
            UnmapViewOfFile(address);
        }
        return -1;
    }

    if (address == NULL) {
        return -1;
    }

    return (jlong) address;
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_getPageSize
        (JNIEnv *e, jclass cl) {
    SYSTEM_INFO siSysInfo;
    GetSystemInfo(&siSysInfo);
    return siSysInfo.dwAllocationGranularity;
}

JNIEXPORT jboolean JNICALL Java_com_questdb_std_Files_remove
        (JNIEnv *e, jclass cl, jlong lpsz) {
    if (DeleteFile((LPCSTR) lpsz)) {
        return TRUE;
    }
    SaveLastError();
    return FALSE;
}

JNIEXPORT jboolean JNICALL Java_com_questdb_std_Files_rmdir
        (JNIEnv *e, jclass cl, jlong lpsz) {
    jboolean b = (jboolean) RemoveDirectoryA((LPCSTR) lpsz);
    if (!b) {
        SaveLastError();
    }

    return b;
}

typedef struct {
    WIN32_FIND_DATAA *find_dataa;
    HANDLE hFind;
} FIND;

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_findFirst
        (JNIEnv *e, jclass cl, jlong lpszName) {

    FIND *find = malloc(sizeof(FIND));
    find->find_dataa = malloc(sizeof(WIN32_FIND_DATAA));

    char path[2048];
    sprintf(path, "%s\\*.*", (char *) lpszName);
    find->hFind = FindFirstFile(path, find->find_dataa);
    if (find->hFind == INVALID_HANDLE_VALUE) {
        SaveLastError();
        DWORD err = GetLastError();
        free(find->find_dataa);
        free(find);
        return err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND ? 0 : -1;
    }
    return (jlong) find;
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Files_findNext
        (JNIEnv *e, jclass cl, jlong findPtr) {
    FIND *find = (FIND *) findPtr;
    if (FindNextFile(find->hFind, find->find_dataa)) {
        return 1;
    }

    if (GetLastError() == ERROR_NO_MORE_FILES) {
        return 0;
    }

    SaveLastError();
    return -1;
}

JNIEXPORT void JNICALL Java_com_questdb_std_Files_findClose
        (JNIEnv *e, jclass cl, jlong findPtr) {
    FIND *find = (FIND *) findPtr;
    FindClose(find->hFind);
    free(find->find_dataa);
    free(find);
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Files_findName
        (JNIEnv *e, jclass cl, jlong findPtr) {
    return (jlong) ((FIND *) findPtr)->find_dataa->cFileName;
}


JNIEXPORT jint JNICALL Java_com_questdb_std_Files_findType
        (JNIEnv *e, jclass cl, jlong findPtr) {
    return ((FIND *) findPtr)->find_dataa->dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY ?
           com_questdb_std_Files_DT_DIR : com_questdb_std_Files_DT_REG;
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Files_lock
        (JNIEnv *e, jclass cl, jlong fd) {
    DWORD high;
    DWORD low = GetFileSize((HANDLE) fd, &high);
    if (LockFile((HANDLE) fd, 0, 0, low, high)) {
        return 0;
    }

    SaveLastError();
    return 1;
}


JNIEXPORT jboolean JNICALL Java_com_questdb_std_Files_rename
        (JNIEnv *e, jclass cl, jlong lpszOld, jlong lpszNew) {
    if (MoveFile((LPCSTR) lpszOld, (LPCSTR) lpszNew)) {
        return TRUE;
    }
    SaveLastError();
    return FALSE;
}
