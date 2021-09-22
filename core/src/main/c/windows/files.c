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

#define _WIN32_WINNT 0x600 /* GetFileInformationByHandleEx is Vista+ */

#include <shlwapi.h>
#include <minwindef.h>
#include <fileapi.h>
#include <winbase.h>
#include <direct.h>
#include <stdint.h>
#include "../share/files.h"
#include "errno.h"

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_copy
        (JNIEnv *e, jclass cls, jlong lpszFrom, jlong lpszTo) {
    const char *from = (const char *) lpszFrom;
    const char *to = (const char *) lpszTo;
    if (CopyFile(from, to, TRUE) == FALSE) {
        SaveLastError();
        return -1;
    }
    return 1;
}

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

HANDLE openUtf8(jlong lpszName, DWORD dwDesiredAccess, DWORD dwShareMode, DWORD dwCreationDisposition) {
    size_t len = MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszName, -1, NULL, 0);
    if (len > 0) {
        wchar_t buf[len];
        MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszName, -1, buf, len);

        HANDLE fd = CreateFileW(
                buf,
                dwDesiredAccess,
                dwShareMode,
                NULL,
                dwCreationDisposition,
                FILE_ATTRIBUTE_NORMAL,
                NULL
        );

        if (fd == INVALID_HANDLE_VALUE) {
            SaveLastError();
        }

        return fd;
    }
    return INVALID_HANDLE_VALUE;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_write
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

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_read
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

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getLastModified
        (JNIEnv *e, jclass cl, jlong lpszName) {

    HANDLE handle = openUtf8(
            lpszName,
            FILE_WRITE_ATTRIBUTES,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            OPEN_EXISTING
    );

    if (handle > INVALID_HANDLE_VALUE) {
        SaveLastError();
        return -1;
    }

    FILETIME creation;
    FILETIME access;
    FILETIME write;

    if (GetFileTime(handle, &creation, &access, &write)) {
        CloseHandle(handle);
        return ((((jlong) write.dwHighDateTime) << 32) | write.dwLowDateTime) / 10000 - MILLIS_SINCE_1970;
    }
    CloseHandle(handle);
    SaveLastError();
    return -1;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_exists0(JNIEnv *e, jclass cl, jlong lpszName) {
    size_t len = MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszName, -1, NULL, 0);
    if (len > 0) {
        wchar_t buf[len];
        MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszName, -1, buf, len);
        return PathFileExistsW(buf);
    }
    return FALSE;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_msync(JNIEnv *e, jclass cl, jlong addr, jlong len, jboolean async) {
    if (FlushViewOfFile((LPCVOID) addr, len) == 0) {
        SaveLastError();
        return -1;
    }
    return 0;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_fsync(JNIEnv *e, jclass cl, jlong fd) {
    // Windows does not seem to have fsync or cannot fsync directory.
    // To be fair we never saw our destructive test fail on windows,
    // which leads to an assumption that all directory changes on windows are synchronous.
    return -1;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_setLastModified
        (JNIEnv *e, jclass cl, jlong lpszName, jlong millis) {

    HANDLE handle = openUtf8(
            lpszName,
            FILE_WRITE_ATTRIBUTES,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            OPEN_EXISTING
    );


    if (handle != INVALID_HANDLE_VALUE) {
        millis += MILLIS_SINCE_1970; // millis between 1601-01-01 and 1970-01-01
        millis *= 10000;
        FILETIME t;
        t.dwHighDateTime = (DWORD) ((millis >> 32) & 0xFFFFFFFF);
        t.dwLowDateTime = (DWORD) millis;
        int r = SetFileTime(handle, NULL, NULL, &t);
        CloseHandle(handle);
        return (jboolean) r;
    }
    return FALSE;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_length0(JNIEnv *e, jclass cl, jlong lpszName) {

    HANDLE h = openUtf8(
            lpszName,
            FILE_READ_ONLY,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            OPEN_EXISTING
    );
    jlong len = Java_io_questdb_std_Files_length(e, cl, (jlong) h);
    CloseHandle(h);
    return len;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_mkdir(JNIEnv *e, jclass cl, jlong lpszName, jint mode) {

    size_t len = MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszName, -1, NULL, 0);
    if (len > 0) {
        wchar_t buf[len];
        MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszName, -1, buf, len);

        if (CreateDirectoryW(buf, NULL)) {
            return 0;
        }
        SaveLastError();
        return -1;
    }

    return -2;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_openRO(JNIEnv *e, jclass cl, jlong lpszName) {
    return (jlong) openUtf8(
            lpszName,
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            OPEN_EXISTING
    );
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_close0
        (JNIEnv *e, jclass cl, jlong fd) {
    jint r = CloseHandle((HANDLE) fd);
    if (!r) {
        SaveLastError();
        return -1;
    }
    return 0;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_dup
        (JNIEnv *e, jclass cl, jlong fd) {
    return _dup((int) fd);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_openRW
        (JNIEnv *e, jclass cl, jlong lpszName) {
    return (jlong) openUtf8(
            lpszName,
            GENERIC_WRITE | GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            OPEN_ALWAYS
    );
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_openAppend
        (JNIEnv *e, jclass cl, jlong lpszName) {
    HANDLE h = openUtf8(
            lpszName,
            FILE_APPEND_DATA,
            FILE_SHARE_READ | FILE_SHARE_DELETE,
            OPEN_ALWAYS
    );

    if (h > INVALID_HANDLE_VALUE) {
        SetFilePointer(h, 0, NULL, FILE_END);
    }

    return (jlong) h;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_append
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

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_length
        (JNIEnv *e, jclass cl, jlong fd) {
    DWORD high;
    DWORD low = GetFileSize((HANDLE) fd, &high);
    if (low != INVALID_FILE_SIZE) {
        return low | (__int64) high << 32;
    } else {
        SaveLastError();
        return -1;
    }
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_exists
        (JNIEnv *e, jclass cl, jlong fd) {

    FILE_STANDARD_INFO info;
    if (GetFileInformationByHandleEx((HANDLE) fd, FileStandardInfo, &info, sizeof(FILE_STANDARD_INFO))) {
        return (jboolean) !info.DeletePending;
    }
    return FALSE;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getStdOutFd
        (JNIEnv *e, jclass cl) {
    return (jlong) GetStdHandle(STD_OUTPUT_HANDLE);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_truncate
        (JNIEnv *e, jclass cl, jlong handle, jlong size) {
    if (set_file_pos((HANDLE) handle, size) && SetEndOfFile((HANDLE) handle)) {
        return TRUE;
    }
    SaveLastError();
    return FALSE;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_allocate
        (JNIEnv *e, jclass cl, jlong handle, jlong size) {
    /* On Windows truncate does the allocation */
    return Java_io_questdb_std_Files_truncate(e, cl, handle, size);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_munmap0
        (JNIEnv *e, jclass cl, jlong address, jlong len) {
    if (UnmapViewOfFile((LPCVOID) address) == 0) {
        SaveLastError();
        return -1;
    } else {
        return 0;
    }
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_mmap0
        (JNIEnv *e, jclass cl, jlong fd, jlong len, jlong offset, jint flags, jlong baseAddress) {
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

    address = MapViewOfFileEx(
            hMapping, dwDesiredAccess,
            (DWORD) (offset >> 32),
            (DWORD) offset,
            (SIZE_T) len,
            (LPVOID) baseAddress
    );

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

static inline jlong _io_questdb_std_Files_mremap0
        (jlong fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    jlong newAddress = Java_io_questdb_std_Files_mmap0((JNIEnv *) NULL, (jclass) NULL, fd, newLen, offset, flags, 0);
    // Note that unmapping will not flush dirty pages because the mapping to address is shared with newAddress
    Java_io_questdb_std_Files_munmap0((JNIEnv *) NULL, (jclass) NULL, address, previousLen);
    return newAddress;
}

JNIEXPORT jlong JNICALL JavaCritical_io_questdb_std_Files_mremap0
        (jlong fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    return _io_questdb_std_Files_mremap0(fd, address, previousLen, newLen, offset, flags);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_mremap0
        (JNIEnv *e, jclass cl, jlong fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    return _io_questdb_std_Files_mremap0(fd, address, previousLen, newLen, offset, flags);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getPageSize
        (JNIEnv *e, jclass cl) {
    SYSTEM_INFO siSysInfo;
    GetSystemInfo(&siSysInfo);
    return siSysInfo.dwAllocationGranularity;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_remove
        (JNIEnv *e, jclass cl, jlong lpszName) {

    size_t len = MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszName, -1, NULL, 0);
    if (len > 0) {
        wchar_t buf[len];
        MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszName, -1, buf, len);

        if (DeleteFileW(buf)) {
            return TRUE;
        }
    }

    SaveLastError();
    return FALSE;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_rmdir
        (JNIEnv *e, jclass cl, jlong lpszName) {
    size_t len = MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszName, -1, NULL, 0);
    if (len > 0) {
        wchar_t buf[len];
        MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszName, -1, buf, len);

        if (RemoveDirectoryW(buf)) {
            return TRUE;
        }
    }
    SaveLastError();
    return FALSE;
}

#define UTF8_MAX_PATH (MAX_PATH * 4)

typedef struct {
    WIN32_FIND_DATAW *find_data;
    HANDLE hFind;
    char utf8Name[UTF8_MAX_PATH];
} FIND;

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_findFirst
        (JNIEnv *e, jclass cl, jlong lpszName) {

    char path[strlen((const char *) lpszName) + 32];
    sprintf(path, "%s\\*.*", (char *) lpszName);

    int len = MultiByteToWideChar(CP_UTF8, 0, path, -1, NULL, 0);
    if (len > 0) {
        wchar_t buf[len];
        MultiByteToWideChar(CP_UTF8, 0, path, -1, buf, len);

        FIND *find = malloc(sizeof(FIND));
        find->find_data = malloc(sizeof(WIN32_FIND_DATAW));

        find->hFind = FindFirstFileW(buf, find->find_data);
        if (find->hFind == INVALID_HANDLE_VALUE) {
            SaveLastError();
            DWORD err = GetLastError();
            free(find->find_data);
            free(find);
            return err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND ? 0 : -1;
        }
        return (jlong) find;
    }
    return -2;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_findNext
        (JNIEnv *e, jclass cl, jlong findPtr) {
    FIND *find = (FIND *) findPtr;
    if (FindNextFileW(find->hFind, find->find_data)) {
        return 1;
    }

    if (GetLastError() == ERROR_NO_MORE_FILES) {
        return 0;
    }

    SaveLastError();
    return -1;
}

JNIEXPORT void JNICALL Java_io_questdb_std_Files_findClose
        (JNIEnv *e, jclass cl, jlong findPtr) {
    FIND *find = (FIND *) findPtr;
    FindClose(find->hFind);
    free(find->find_data);
    free(find);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_findName
        (JNIEnv *e, jclass cl, jlong findPtr) {
    WideCharToMultiByte(CP_UTF8, 0, ((FIND *) findPtr)->find_data->cFileName, -1, ((FIND *) findPtr)->utf8Name,
                        UTF8_MAX_PATH, NULL, NULL);
    return (jlong) ((FIND *) findPtr)->utf8Name;
}


JNIEXPORT jint JNICALL Java_io_questdb_std_Files_findType
        (JNIEnv *e, jclass cl, jlong findPtr) {
    return ((FIND *) findPtr)->find_data->dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY ?
           com_questdb_std_Files_DT_DIR : com_questdb_std_Files_DT_REG;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_lock
        (JNIEnv *e, jclass cl, jlong fd) {
    if (LockFile((HANDLE) fd, 0, 0, 1, 0)) {
        return 0;
    }

    SaveLastError();
    return 1;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_openCleanRW
        (JNIEnv *e, jclass cl, jlong lpszName, jlong size) {
    OVERLAPPED sOverlapped;
    sOverlapped.Offset = 0;
    sOverlapped.OffsetHigh = 0;
    jlong fd = Java_io_questdb_std_Files_openRW(e, cl, lpszName);

    if (fd < -1) {
        // error opening / creating file
        // save last error already called
        return fd;
    }

    HANDLE handle = (HANDLE) fd;

    jlong fileSize = Java_io_questdb_std_Files_length(e, cl, fd);
    if (fileSize > 0) {
        if (LockFileEx(handle, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 0, 1, &sOverlapped)) {
            // truncate file to 1 byte
            if (Java_io_questdb_std_Files_truncate(e, cl, fd, 1) == JNI_TRUE) {
                // write first byte to 0
                DWORD writtenCount = 0;
                byte buff[1] = {0};
                if (set_file_pos(handle, 0) && WriteFile(handle, (LPCVOID) &buff, (DWORD) 1, &writtenCount, NULL) &&
                    writtenCount == 1) {

                    // extend file to `size`
                    if (Java_io_questdb_std_Files_allocate(e, cl, fd, size) == JNI_TRUE) {
                        // downgrade to shared lock
                        if (UnlockFileEx(handle, 0, 0, 1, &sOverlapped)) {
                            if (LockFileEx(handle, 0, 0, 0, 1, &sOverlapped)) {
                                // success
                                return fd;
                            }
                        }
                    }
                }
            }
        } else {
            // extend file to `size`
            if (fileSize >= size || Java_io_questdb_std_Files_allocate(e, cl, fd, size) == JNI_TRUE) {
                // put a shared lock
                if (LockFileEx(handle, 0, 0, 0, 1, &sOverlapped)) {
                    return fd;
                }
            }
        }
    } else {
        // file size is already 0, no cleanup but allocate the file.
        if (Java_io_questdb_std_Files_truncate(e, cl, fd, size)
                && LockFileEx(handle, 0, 0, 0, 1, &sOverlapped)) {
            return fd;
        }
    }

    // Any non-happy path comes here.
    SaveLastError();
    CloseHandle(handle);
    return -1;

}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_rename(JNIEnv *e, jclass cl, jlong lpszOld, jlong lpszNew) {

    size_t len = MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszOld, -1, NULL, 0);
    if (len > 0) {
        wchar_t buf1[len];
        MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszOld, -1, buf1, len);

        len = MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszNew, -1, NULL, 0);

        if (len > 0) {
            wchar_t buf2[len];
            MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszNew, -1, buf2, len);

            if (MoveFileW(buf1, buf2)) {
                return TRUE;
            }
        }
    }
    SaveLastError();
    return FALSE;
}
