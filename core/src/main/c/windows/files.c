/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
#include <windows.h>
#include "../share/files.h"
#include "errno.h"
#include "files.h"

#include <stdio.h>
#include <ntdef.h>

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_copy
        (JNIEnv *e, jclass cls, jlong lpszFrom, jlong lpszTo) {

    int fromLen = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszFrom, -1, NULL, 0);
    int toLen = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszTo, -1, NULL, 0);
    if (fromLen > 0 && toLen) {
        wchar_t fromBuf[fromLen];
        wchar_t toBuf[toLen];

        MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszFrom, -1, fromBuf, fromLen);
        MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszTo, -1, toBuf, toLen);

        if (CopyFileW(fromBuf, toBuf, TRUE) == FALSE) {
            SaveLastError();
            return -1;
        }
        return 1;
    }
    // invalid UTF8 paths
    return -1;
}


jboolean set_file_pos(HANDLE fd, jlong offset) {
    if (offset < 0) {
        return 1;
    }
    LONG highPos = (LONG) (offset >> 32);
    DWORD r = SetFilePointer(fd, (LONG) offset, &highPos, FILE_BEGIN) != INVALID_SET_FILE_POINTER;
    if (r == INVALID_SET_FILE_POINTER) {
        SaveLastError();
        return FALSE;
    }
    return TRUE;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_copyDataToOffset
        (JNIEnv *e, jclass cls, jint srcFd, jint destFd, jlong fromOffset, jlong destOffset, jlong length) {

    char buf[16 * 4096];
    DWORD read_sz;
    LONG64 rd_off = fromOffset;
    LONG64 wrt_off = destOffset;
    LONG64 hi;

    if (length < 0) {
        hi = _I64_MAX;
    } else {
        hi = fromOffset + length;
    }

    HANDLE srcHandle = FD_TO_HANDLE(srcFd);
    HANDLE destHandle = FD_TO_HANDLE(destFd);

    while (rd_off < hi) {
        // Create OVERLAPPED structure for read operation
        OVERLAPPED readOverlapped = {0};
        readOverlapped.Offset = (DWORD)(rd_off & 0xFFFFFFFF);
        readOverlapped.OffsetHigh = (DWORD)(rd_off >> 32);

        // Calculate how much to read (don't exceed the limit)
        DWORD toRead = sizeof(buf);
        if (rd_off + toRead > hi) {
            toRead = (DWORD)(hi - rd_off);
        }

        if (!ReadFile(srcHandle, buf, toRead, &read_sz, &readOverlapped) || read_sz == 0) {
            break;
        }

        // Write the data we just read
        char *out_ptr = buf;
        DWORD remaining = read_sz;

        while (remaining > 0) {
            // Create OVERLAPPED structure for write operation
            OVERLAPPED writeOverlapped = {0};
            writeOverlapped.Offset = (DWORD)(wrt_off & 0xFFFFFFFF);
            writeOverlapped.OffsetHigh = (DWORD)(wrt_off >> 32);

            DWORD write_sz;
            if (!WriteFile(destHandle, out_ptr, remaining, &write_sz, &writeOverlapped)) {
                SaveLastError();
                return rd_off - fromOffset;
            }

            if (write_sz > 0) {
                remaining -= write_sz;
                out_ptr += write_sz;
                wrt_off += write_sz;
            } else {
                // Write returned 0, which shouldn't happen with overlapped I/O
                SaveLastError();
                return -1;
            }
        }

        rd_off += read_sz;
    }

    return rd_off - fromOffset;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_copyData
        (JNIEnv *e, jclass cls, jint srcFd, jint destFd, jlong fromOffset, jlong length) {
    return Java_io_questdb_std_Files_copyDataToOffset(e, cls, srcFd, destFd, fromOffset, 0, length);
}

HANDLE openUtf8(jlong lpszName, DWORD dwDesiredAccess, DWORD dwShareMode, DWORD dwCreationDisposition) {
    int len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszName, -1, NULL, 0);
    if (len > 0) {
        wchar_t buf[len];
        MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszName, -1, buf, len);

        HANDLE handle = CreateFileW(
                buf,
                dwDesiredAccess,
                dwShareMode,
                NULL,
                dwCreationDisposition,
                FILE_ATTRIBUTE_NORMAL,
                NULL
        );

        if (handle == INVALID_HANDLE_VALUE) {
            SaveLastError();
        }

        return handle;
    }
    return INVALID_HANDLE_VALUE;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_write
        (JNIEnv *e, jclass cl, jint fd, jlong address, jlong len, jlong offset) {
    DWORD count;
    if (len < 0 || offset < 0) {
        // To align with other platforms
        return -1;
    }

    HANDLE handle = FD_TO_HANDLE(fd);

    // Create OVERLAPPED structure with the specified offset
    OVERLAPPED overlapped = {0};
    overlapped.Offset = (DWORD)(offset & 0xFFFFFFFF);
    overlapped.OffsetHigh = (DWORD)(offset >> 32);

    if (WriteFile(handle, (LPCVOID) address, (DWORD) len, &count, &overlapped)) {
        return count;
    }

    SaveLastError();
    return -1;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_read
        (JNIEnv *e, jclass cl, jint fd, jlong address, jlong len, jlong offset) {

    if (len < 0 || offset < 0) {
        // To align with other platforms
        return -1;
    }

    DWORD count;
    HANDLE handle = FD_TO_HANDLE(fd);

    // Create OVERLAPPED structure with the specified offset
    OVERLAPPED overlapped = {0};
    overlapped.Offset = (DWORD)(offset & 0xFFFFFFFF);
    overlapped.OffsetHigh = (DWORD)(offset >> 32);

    if (!ReadFile(handle, (LPVOID) address, (DWORD) len, &count, &overlapped)) {
        SaveLastError();
    }
    return count;
}

JNIEXPORT jbyte JNICALL Java_io_questdb_std_Files_readNonNegativeByte
        (JNIEnv *e, jclass cl, jint fd, jlong offset) {
    DWORD count;
    jbyte result;
    HANDLE handle = FD_TO_HANDLE(fd);

    // Create OVERLAPPED structure with the specified offset
    OVERLAPPED overlapped = {0};
    overlapped.Offset = (DWORD)(offset & 0xFFFFFFFF);
    overlapped.OffsetHigh = (DWORD)(offset >> 32);

    if (ReadFile(handle, (LPVOID) &result, (DWORD) 1, &count, &overlapped)) {
        if (count == 1) {
            return result;
        }
    }
    SaveLastError();
    return -1;
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_Files_readNonNegativeShort
        (JNIEnv *e, jclass cl, jint fd, jlong offset) {
    DWORD count;
    jshort result;
    HANDLE handle = FD_TO_HANDLE(fd);

    // Create OVERLAPPED structure with the specified offset
    OVERLAPPED overlapped = {0};
    overlapped.Offset = (DWORD)(offset & 0xFFFFFFFF);
    overlapped.OffsetHigh = (DWORD)(offset >> 32);

    if (ReadFile(handle, (LPVOID) &result, (DWORD) 2, &count, &overlapped)) {
        if (count == 2) {
            return result;
        }
    }
    SaveLastError();
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_readNonNegativeInt
        (JNIEnv *e, jclass cl, jint fd, jlong offset) {
    DWORD count;
    jint result;
    HANDLE handle = FD_TO_HANDLE(fd);

    // Create OVERLAPPED structure with the specified offset
    OVERLAPPED overlapped = {0};
    overlapped.Offset = (DWORD)(offset & 0xFFFFFFFF);
    overlapped.OffsetHigh = (DWORD)(offset >> 32);

    if (ReadFile(handle, (LPVOID) &result, (DWORD) 4, &count, &overlapped)) {
        if (count == 4) {
            return result;
        }
    }
    SaveLastError();
    return -1;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_readIntAsUnsignedLong
        (JNIEnv *e, jclass cl, jint fd, jlong offset) {
    DWORD count;
    uint32_t result;
    HANDLE handle = FD_TO_HANDLE(fd);

    // Create OVERLAPPED structure with the specified offset
    OVERLAPPED overlapped = {0};
    overlapped.Offset = (DWORD)(offset & 0xFFFFFFFF);
    overlapped.OffsetHigh = (DWORD)(offset >> 32);

    if (ReadFile(handle, (LPVOID) &result, (DWORD) 4, &count, &overlapped)) {
        if (count == 4) {
            return (jlong) result;
        }
    }
    SaveLastError();
    return -1;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_readNonNegativeLong
        (JNIEnv *e, jclass cl, jint fd, jlong offset) {
    jlong result;
    OVERLAPPED overlapped = {0};
    overlapped.Offset = (DWORD)(offset & 0xFFFFFFFF);
    overlapped.OffsetHigh = (DWORD)(offset >> 32);


    HANDLE handle = FD_TO_HANDLE(fd);
    DWORD bytesRead;
    if (!ReadFile(handle, (LPVOID) &result, sizeof(uint64_t), &bytesRead, &overlapped) || bytesRead != sizeof(uint64_t)) {
        SaveLastError();
        return -1;
    }

    return result;

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

    if (handle == INVALID_HANDLE_VALUE) {
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
    int len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszName, -1, NULL, 0);
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

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_fsync(JNIEnv *e, jclass cl, jint fd) {
    if (FlushFileBuffers(FD_TO_HANDLE(fd))) {
        return 0;
    }
    SaveLastError();
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_sync(JNIEnv *e, jclass cl) {
    // Windows does not seem to have sync.
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

    if (h == INVALID_HANDLE_VALUE) {
        return -1;
    }

    jlong len = Java_io_questdb_std_Files_length(e, cl, HANDLE_TO_FD(h));
    CloseHandle(h);
    return len;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_hardLink(JNIEnv *e, jclass cl, jlong lpszSrc, jlong lpszHardLink) {

    size_t lenSrc = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszSrc, -1, NULL, 0);
    if (lenSrc < 1) {
        SaveLastError();
        return -1;
    }
    wchar_t bufSrc[lenSrc];
    MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszSrc, -1, bufSrc, (int) lenSrc);

    size_t lenHardLink = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszHardLink, -1, NULL, 0);
    if (lenHardLink < 1) {
        SaveLastError();
        return -1;
    }
    wchar_t bufHardLink[lenHardLink];
    MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszHardLink, -1, bufHardLink, (int) lenHardLink);

    if (CreateHardLinkW(bufHardLink, bufSrc, NULL)) {
        return 0;
    }
    SaveLastError();
    return -1;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_isSoftLink(JNIEnv *e, jclass cl, jlong lpszSoftLink) {

    int len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszSoftLink, -1, NULL, 0);
    if (len > 0) {
        wchar_t buf[len];
        MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszSoftLink, -1, buf, len);

        HANDLE handle = CreateFileW(
                buf,
                GENERIC_READ,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                NULL,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_READONLY | FILE_FLAG_OPEN_REPARSE_POINT,
                NULL
        );

        if (handle == INVALID_HANDLE_VALUE) {
            SaveLastError();
            return FALSE;
        }

        FILE_BASIC_INFO info;
        jboolean result = GetFileInformationByHandleEx(handle, FileBasicInfo, &info, sizeof(FILE_BASIC_INFO));
        if (!result) {
            SaveLastError();
        } else {
            result = info.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT;
        }

        int tmpErr = errno;
        CloseHandle(handle);
        errno = tmpErr;
        return result;
    }
    return FALSE;
}


JNIEXPORT jint JNICALL Java_io_questdb_std_Files_softLink(JNIEnv *e, jclass cl, jlong lpszSrc, jlong lpszSoftLink) {

    size_t lenSrc = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszSrc, -1, NULL, 0);
    if (lenSrc < 1) {
        SaveLastError();
        return -1;
    }
    wchar_t bufSrc[lenSrc];
    MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszSrc, -1, bufSrc, (int) lenSrc);

    size_t lenSoftLink = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszSoftLink, -1, NULL, 0);
    if (lenSoftLink < 1) {
        SaveLastError();
        return -1;
    }

    wchar_t bufSoftLink[lenSoftLink];
    MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszSoftLink, -1, bufSoftLink, (int) lenSoftLink);

    if (CreateSymbolicLinkW(bufSoftLink, bufSrc, SYMBOLIC_LINK_FLAG_ALLOW_UNPRIVILEGED_CREATE)) {
        return 0;
    }

    SaveLastError();
    return -1;
}

// JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_isDir(JNIEnv *e, jclass cl, jlong lpszName) {
//     See Rust implementation in `files.rs`.
// }

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_unlink(JNIEnv *e, jclass cl, jlong lpszSoftLink) {
    // https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-deletefile
    // If the path points to a symbolic link, the symbolic link is deleted, not the target.
    return Java_io_questdb_std_Files_remove(e, cl, lpszSoftLink) ? 0 : -1;
}


JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_noop
        (JNIEnv *e, jclass cl) {
    return 0;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_mkdir(JNIEnv *e, jclass cl, jlong lpszName, jint mode) {

    size_t len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszName, -1, NULL, 0);
    if (len > 0) {
        wchar_t buf[len];
        MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszName, -1, buf, (int) len);

        if (CreateDirectoryW(buf, NULL)) {
            return 0;
        }
        SaveLastError();
        return -1;
    }

    return -2;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getFileSystemStatus(JNIEnv *e, jclass cl, jlong lpszName) {
    DWORD fileSystemFlags;
    char drive[MAX_PATH];
    char fileSystemName[MAX_PATH];
    char volumeName[MAX_PATH];
    DWORD VolumeSerialNo;
    unsigned long MaxComponentLength;

    if (GetVolumePathName((LPCSTR) lpszName, drive, MAX_PATH)) {
        if (GetVolumeInformation(
                drive,
                volumeName,
                MAX_PATH,
                &VolumeSerialNo,
                &MaxComponentLength,
                &fileSystemFlags,
                fileSystemName,
                MAX_PATH
        )) {
            if ((fileSystemFlags & FILE_SUPPORTS_TRANSACTIONS) != 0) {
                // windows share (CIFS) reports filesystem as NTFS
                // local disks support transactions, but CIFS does not
                strcpy((char *) lpszName, fileSystemName);
                return FLAG_FS_SUPPORTED * 0x2b;
            }
            // unsupported file system
            strcpy((char *) lpszName, "SMB");
            return 0x3b;
        }
    }

    SaveLastError();
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openRO(JNIEnv *e, jclass cl, jlong lpszName) {
    return HANDLE_TO_FD(openUtf8(
            lpszName,
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            OPEN_EXISTING
    ));
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_close0
        (JNIEnv *e, jclass cl, jint fd) {
    jint r = CloseHandle(FD_TO_HANDLE(fd));
    if (!r) {
        SaveLastError();
        return -1;
    }
    return 0;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openRW
        (JNIEnv *e, jclass cl, jlong lpszName) {
    return HANDLE_TO_FD(openUtf8(
            lpszName,
            GENERIC_WRITE | GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            OPEN_ALWAYS
    ));
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openRWOpts
        (JNIEnv *e, jclass cl, jlong lpszName, jlong opts) {
    // consider using FILE_FLAG_WRITE_THROUGH
    return HANDLE_TO_FD(openUtf8(
            lpszName,
            GENERIC_WRITE | GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            OPEN_ALWAYS
    ));
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openAppend
        (JNIEnv *e, jclass cl, jlong lpszName) {
    HANDLE handle = openUtf8(
            lpszName,
            FILE_APPEND_DATA,
            FILE_SHARE_READ | FILE_SHARE_DELETE,
            OPEN_ALWAYS
    );

    if (handle != INVALID_HANDLE_VALUE) {
        SetFilePointer(handle, 0, NULL, FILE_END);
    }

    return HANDLE_TO_FD(handle);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_append
        (JNIEnv *e, jclass cl, jint fd, jlong address, jlong len) {
    DWORD count;
    if (WriteFile(FD_TO_HANDLE(fd), (LPCVOID) address, (DWORD) len, &count, NULL)) {
        return count;
    }

    SaveLastError();
    return -1;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_length
        (JNIEnv *e, jclass cl, jint fd) {
    DWORD high;
    DWORD low = GetFileSize(FD_TO_HANDLE(fd), &high);
    if (low != INVALID_FILE_SIZE) {
        return low | (__int64) high << 32;
    } else {
        SaveLastError();
        return -1;
    }
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_exists
        (JNIEnv *e, jclass cl, jint fd) {

    FILE_STANDARD_INFO info;
    if (GetFileInformationByHandleEx(FD_TO_HANDLE(fd), FileStandardInfo, &info, sizeof(FILE_STANDARD_INFO))) {
        return (jboolean) !info.DeletePending;
    }
    return FALSE;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_getStdOutFd
        (JNIEnv *e, jclass cl) {
    return (jlong) GetStdHandle(STD_OUTPUT_HANDLE);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_truncate
        (JNIEnv *e, jclass cl, jint fd, jlong size) {
    HANDLE handle = FD_TO_HANDLE(fd);

    FILE_END_OF_FILE_INFO fileInfo;
    fileInfo.EndOfFile.QuadPart = size;

    if (SetFileInformationByHandle(handle, FileEndOfFileInfo, &fileInfo, sizeof(fileInfo))) {
        return TRUE;
    }
    SaveLastError();
    return FALSE;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_allocate
        (JNIEnv *e, jclass cl, jint fd, jlong size) {
    /* On Windows truncate does the allocation */
    return Java_io_questdb_std_Files_truncate(e, cl, fd, size);
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
        (JNIEnv *e, jclass cl, jint fd, jlong len, jlong offset, jint flags, jlong baseAddress) {
    if (len == 0) {
        // With len == 0, Windows will mmap the whole file. To be POSIX-compatible, return MAP_FAILED instead.
        // docs: https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-mapviewoffileex
        // dwNumberOfBytesToMap: "If this parameter is 0 (zero), the mapping extends from the
        // specified offset to the end of the file mapping."
        SetLastError(ERROR_INVALID_PARAMETER);
        SaveLastError();
        return -1;
    }

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

    HANDLE hMapping = CreateFileMapping(
            FD_TO_HANDLE(fd),
            NULL,
            flProtect | SEC_RESERVE,
            (DWORD) (maxsize >> 32),
            (DWORD) maxsize,
            NULL
    );
    if (hMapping == NULL) {
        SaveLastError();
        return -1;
    }

    address = MapViewOfFileEx(
            hMapping,
            dwDesiredAccess,
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

static inline jlong internal_mremap0
        (jint fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    jlong newAddress = Java_io_questdb_std_Files_mmap0((JNIEnv *) NULL, (jclass) NULL, fd, newLen, offset, flags, 0);
    // Note that unmapping will not flush dirty pages because the mapping to address is shared with newAddress
    Java_io_questdb_std_Files_munmap0((JNIEnv *) NULL, (jclass) NULL, address, previousLen);
    return newAddress;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_mremap0
        (JNIEnv *e, jclass cl, jint fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    return internal_mremap0(fd, address, previousLen, newLen, offset, flags);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getPageSize
        (JNIEnv *e, jclass cl) {
    SYSTEM_INFO siSysInfo;
    GetSystemInfo(&siSysInfo);
    return siSysInfo.dwAllocationGranularity;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_remove
        (JNIEnv *e, jclass cl, jlong lpszName) {

    int len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszName, -1, NULL, 0);
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
    int len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszName, -1, NULL, 0);
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

typedef struct {
    WIN32_FIND_DATAW *find_data;
    HANDLE hFind;
    char utf8Name[UTF8_MAX_PATH];
} FIND;

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_findFirst
        (JNIEnv *e, jclass cl, jlong lpszName) {

    char path[strlen((const char *) lpszName) + 32];
    sprintf(path, "%s\\*.*", (char *) lpszName);

    int len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, NULL, 0);
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
    WideCharToMultiByte(
            CP_UTF8,
            0,
            ((FIND *) findPtr)->find_data->cFileName,
            -1,
            ((FIND *) findPtr)->utf8Name,
            UTF8_MAX_PATH,
            NULL,
            NULL
    );
    return (jlong) ((FIND *) findPtr)->utf8Name;
}


JNIEXPORT jint JNICALL Java_io_questdb_std_Files_findType
        (JNIEnv *e, jclass cl, jlong findPtr) {
    return ((FIND *) findPtr)->find_data->dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY ?
           com_questdb_std_Files_DT_DIR : com_questdb_std_Files_DT_REG;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_lock
        (JNIEnv *e, jclass cl, jint fd) {
    if (LockFile(FD_TO_HANDLE(fd), 0, 0, 1, 0)) {
        return 0;
    }

    SaveLastError();
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openCleanRW
        (JNIEnv *e, jclass cl, jlong lpszName, jlong size) {
    OVERLAPPED sOverlapped;
    sOverlapped.Offset = 0;
    sOverlapped.OffsetHigh = 0;
    jint fd = Java_io_questdb_std_Files_openRW(e, cl, lpszName);

    if (fd < 0) {
        // error opening / creating file
        // save last error already called
        return fd;
    }

    HANDLE handle = FD_TO_HANDLE(fd);

    jlong fileSize = Java_io_questdb_std_Files_length(e, cl, fd);
    if (fileSize > 0) {
        if (LockFileEx(handle, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 0, 1, &sOverlapped)) {
            // truncate file to 1 byte
            if (Java_io_questdb_std_Files_truncate(e, cl, fd, 1) == JNI_TRUE) {
                // write first byte to 0
                DWORD writtenCount = 0;
                byte buff[1] = {0};
                if (set_file_pos(handle, 0)
                    && WriteFile(handle, (LPCVOID) &buff, (DWORD) 1, &writtenCount, NULL)
                    && writtenCount == 1) {

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
    } else if (fileSize == 0) {
        // file size is already 0, no cleanup but allocate the file
        if (Java_io_questdb_std_Files_truncate(e, cl, fd, size) == JNI_TRUE
            && LockFileEx(handle, 0, 0, 0, 1, &sOverlapped)) {
            return fd;
        }
    }

    // Any non-happy path comes here.
    SaveLastError();
    CloseHandle(handle);
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_rename(JNIEnv *e, jclass cl, jlong lpszOld, jlong lpszNew) {
    int len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszOld, -1, NULL, 0);
    if (len > 0) {
        wchar_t buf1[len];
        MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszOld, -1, buf1, len);

        len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszNew, -1, NULL, 0);

        if (len > 0) {
            wchar_t buf2[len];
            MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszNew, -1, buf2, len);

            if (MoveFileW(buf1, buf2)) {
                return FILES_RENAME_ERR_OK;
            }
        }
    }
    SaveLastError();
    return ERROR_NOT_SAME_DEVICE == GetLastError() ? FILES_RENAME_ERR_EXDEV : FILES_RENAME_ERR_OTHER;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getDiskSize(JNIEnv *e, jclass cl, jlong lpszPath) {
    ULARGE_INTEGER lpFreeBytesAvailableToCaller;
    if ((void *) lpszPath != NULL) {
        int len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, (LPCCH) lpszPath, -1, NULL, 0);
        if (len > 0) {
            wchar_t buf[len];
            MultiByteToWideChar(CP_UTF8, 0, (LPCCH) lpszPath, -1, buf, len);
            if (GetDiskFreeSpaceExW(buf, &lpFreeBytesAvailableToCaller, NULL, NULL)) {
                return (jlong) lpFreeBytesAvailableToCaller.QuadPart;
            }
        }
    } else {
        if (GetDiskFreeSpaceExW(NULL, &lpFreeBytesAvailableToCaller, NULL, NULL)) {
            return (jlong) lpFreeBytesAvailableToCaller.QuadPart;
        }
    }
    SaveLastError();
    return -1;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getFileLimit
        (JNIEnv *e, jclass cl) {
    return 0; // no-op
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getMapCountLimit
        (JNIEnv *e, jclass cl) {
    return 0; // no-op
}
