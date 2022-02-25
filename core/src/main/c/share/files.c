/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>

#ifdef __APPLE__

#include <sys/time.h>

#else

#include <utime.h>

#endif

#include <stdlib.h>
#include <dirent.h>
#include <sys/errno.h>
#include <sys/time.h>
#include "files.h"

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_write
        (JNIEnv *e, jclass cl,
         jlong fd,
         jlong address,
         jlong len,
         jlong offset) {
    return pwrite((int) fd, (void *) (address), (size_t) len, (off_t) offset);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_mmap0
        (JNIEnv *e, jclass cl, jlong fd, jlong len, jlong offset, jint flags, jlong baseAddress) {
    int prot = 0;

    if (flags == com_questdb_std_Files_MAP_RO) {
        prot = PROT_READ;
    } else if (flags == com_questdb_std_Files_MAP_RW) {
        prot = PROT_READ | PROT_WRITE;
    }
    return (jlong) mmap((void *) baseAddress, (size_t) len, prot, MAP_SHARED, (int) fd, offset);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_munmap0
        (JNIEnv *cl, jclass e, jlong address, jlong len) {
    return munmap((void *) address, (size_t) len);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_append
        (JNIEnv *e, jclass cl,
         jlong fd,
         jlong address,
         jlong len) {
    return write((int) fd, (void *) (address), (size_t) len);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_read
        (JNIEnv *e, jclass cl,
         jlong fd,
         jlong address,
         jlong len,
         jlong offset) {

    return pread((int) fd, (void *) address, (size_t) len, (off_t) offset);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_readULong
        (JNIEnv *e, jclass cl,
         jlong fd,
         jlong offset) {
    jlong result;
    ssize_t readLen = pread((int) fd, (void *) &result, (size_t) 8, (off_t) offset);
    if (readLen != 8) {
        return -1;
    }
    return result;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getLastModified
        (JNIEnv *e, jclass cl, jlong pchar) {
    struct stat st;
    int r = stat((const char *) pchar, &st);
#ifdef __APPLE__
    return r == 0 ? ((1000 * st.st_mtimespec.tv_sec) + (st.st_mtimespec.tv_nsec / 1000000)) : r;
#else
    return r == 0 ? ((1000 * st.st_mtim.tv_sec) + (st.st_mtim.tv_nsec / 1000000)) : r;
#endif
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_openRO
        (JNIEnv *e, jclass cl, jlong lpszName) {
    return open((const char *) lpszName, O_RDONLY);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_close0
        (JNIEnv *e, jclass cl, jlong fd) {
    return close((int) fd);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_openRW
        (JNIEnv *e, jclass cl, jlong lpszName) {
    umask(0);
    return open((const char *) lpszName, O_CREAT | O_RDWR, 0644);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_openRWOpts
        (JNIEnv *e, jclass cl, jlong lpszName, jlong opts) {
    umask(0);
    return open((const char *) lpszName, O_CREAT | O_RDWR | opts, 0644);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_openAppend
        (JNIEnv *e, jclass cl, jlong lpszName) {
    umask(0);
    return open((const char *) lpszName, O_CREAT | O_WRONLY | O_APPEND, 0644);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_length0
        (JNIEnv *e, jclass cl, jlong pchar) {
    struct stat st;

    int r = stat((const char *) pchar, &st);
    return r == 0 ? st.st_size : r;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_mkdir
        (JNIEnv *e, jclass cl, jlong pchar, jint mode) {
    return mkdir((const char *) pchar, (mode_t) mode);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_length
        (JNIEnv *e, jclass cl, jlong fd) {
    struct stat st;
    int r = fstat((int) fd, &st);
    return r == 0 ? st.st_size : r;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_exists
        (JNIEnv *e, jclass cl, jlong fd) {
    struct stat st;
    int r = fstat((int) fd, &st);
    return (jboolean) (r == 0 ? st.st_nlink > 0 : 0);
}

#ifdef __APPLE__

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_setLastModified
        (JNIEnv *e, jclass cl, jlong lpszName, jlong millis) {
    struct timeval t[2];
    gettimeofday(t, NULL);
    t[1].tv_sec = millis / 1000;
    t[1].tv_usec = (__darwin_suseconds_t) ((millis % 1000) * 1000);
    return (jboolean) (utimes((const char *) lpszName, t) == 0);
}

#else

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_setLastModified
        (JNIEnv *e, jclass cl, jlong lpszName, jlong millis) {
    struct timeval t[2];
    gettimeofday(t, NULL);
    t[1].tv_sec = millis / 1000;
    t[1].tv_usec = ((millis % 1000) * 1000);
    return (jboolean) (utimes((const char *) lpszName, t) == 0);
}

#endif

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getStdOutFd
        (JNIEnv *e, jclass cl) {
    return (jlong) 1;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_truncate
        (JNIEnv *e, jclass cl, jlong fd, jlong len) {
    if (ftruncate((int) fd, len) == 0) {
        return JNI_TRUE;
    }
    return JNI_FALSE;
}

#ifdef __APPLE__

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_allocate
        (JNIEnv *e, jclass cl, jlong fd, jlong len) {
    // MACOS allocates additional space. Check what size the file currently is
    struct stat st;
    if (fstat((int) fd, &st) != 0) {
        return JNI_FALSE;
    }
    const jlong  fileLen = st.st_blksize * st.st_blocks;
    jlong deltaLen = len - fileLen;
    if (deltaLen > 0) {
        // F_ALLOCATECONTIG - try to allocate continuous space.
        fstore_t flags = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, deltaLen, 0};
        int result = fcntl(fd, F_PREALLOCATE, &flags);
        if (result == -1) {
            // F_ALLOCATEALL - try to allocate non-continuous space.
            flags.fst_flags = F_ALLOCATEALL;
            result = fcntl((int)fd, F_PREALLOCATE, &flags);
            if (result == -1) {
                return JNI_FALSE;
            }
        }
    }
    return ftruncate((int)fd, len) == 0;
}

#else

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_allocate
        (JNIEnv *e, jclass cl, jlong fd, jlong len) {
    int rc = posix_fallocate(fd, 0, len);
    if (rc == 0) {
        return JNI_TRUE;
    }
    if (rc == EINVAL) {
        // Some file systems (such as ZFS) do not support posix_fallocate
        struct stat st;
        rc = fstat((int) fd, &st);
        if (rc != 0) {
            return JNI_FALSE;
        }
        if (st.st_size < len) {
            rc = ftruncate(fd, len);
            if (rc != 0) {
                return JNI_FALSE;
            }
        }
        return JNI_TRUE;
    }
    return JNI_FALSE;
}

#endif

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_msync(JNIEnv *e, jclass cl, jlong addr, jlong len, jboolean async) {
    return msync((void *) addr, len, async ? MS_ASYNC : MS_SYNC);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_fsync(JNIEnv *e, jclass cl, jlong fd) {
    return fsync((int) fd);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_remove
        (JNIEnv *e, jclass cl, jlong lpsz) {
    return (jboolean) (remove((const char *) lpsz) == 0);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_rmdir
        (JNIEnv *e, jclass cl, jlong lpsz) {
    return (jboolean) (rmdir((const char *) lpsz) == 0);
}

typedef struct {
    DIR *dir;
    struct dirent *entry;
} FIND;

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_findFirst
        (JNIEnv *e, jclass cl, jlong lpszName) {

    DIR *dir;
    struct dirent *entry;

    dir = opendir((const char *) lpszName);
    if (!dir) {
        if (errno == ENOENT) {
            return 0;
        }
        return -1;
    }

    errno = 0;
    entry = readdir(dir);
    if (!entry) {
        if (errno == 0) {
            closedir(dir);
            return 0;
        }
        closedir(dir);
        return -1;

    }

    FIND *find = malloc(sizeof(FIND));
    find->dir = dir;
    find->entry = entry;
    return (jlong) find;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getPageSize
        (JNIEnv *e, jclass cl) {
    return sysconf(_SC_PAGESIZE);
}


JNIEXPORT jint JNICALL Java_io_questdb_std_Files_findNext
        (JNIEnv *e, jclass cl, jlong findPtr) {
    FIND *find = (FIND *) findPtr;
    errno = 0;
    find->entry = readdir(find->dir);
    if (find->entry != NULL) {
        return 1;
    }
    return errno == 0 ? 0 : -1;
}

JNIEXPORT void JNICALL Java_io_questdb_std_Files_findClose
        (JNIEnv *e, jclass cl, jlong findPtr) {
    FIND *find = (FIND *) findPtr;
    closedir(find->dir);
    free(find);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_findName
        (JNIEnv *e, jclass cl, jlong findPtr) {
    return (jlong) ((FIND *) findPtr)->entry->d_name;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_findType
        (JNIEnv *e, jclass cl, jlong findPtr) {
    return ((FIND *) findPtr)->entry->d_type;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_lock
        (JNIEnv *e, jclass cl, jlong fd) {
    return flock((int) fd, LOCK_EX | LOCK_NB);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_openCleanRW
        (JNIEnv *e, jclass cl, jlong lpszName, jlong size) {

    jlong fd = open((const char *) lpszName, O_CREAT | O_RDWR, 0644);

    if (fd < -1) {
        // error opening / creating file
        return fd;
    }

    jlong fileSize = Java_io_questdb_std_Files_length(e, cl, fd);
    if (fileSize > 0) {
        if (flock((int)fd, LOCK_EX | LOCK_NB) == 0) {
            // truncate file to 0 byte
            if (ftruncate(fd, 0) == 0) {
                // allocate file to `size`
                if (Java_io_questdb_std_Files_allocate(e, cl, fd, size) == JNI_TRUE) {
                    // downgrade to shared lock
                    if (flock((int) fd, LOCK_SH) == 0) {
                        // success
                        return fd;
                    }
                }
            }
        } else {
            if (fileSize >= size || Java_io_questdb_std_Files_allocate(e, cl, fd, size) == JNI_TRUE) {
                // put a shared lock
                if (flock((int) fd, LOCK_SH) == 0) {
                    // success
                    return fd;
                }
            }
        }
    } else {
        // file size is already 0, no cleanup but allocate the file.
        if (Java_io_questdb_std_Files_allocate(e, cl, fd, size) == JNI_TRUE && flock((int) fd, LOCK_SH) == 0) {
            // success
            return fd;
        }
    }

    // Any non-happy path comes here.
    // Save errno before close.
    int errnoTmp = errno;
    close(fd);
    // Restore real errno
    errno = errnoTmp;
    return -1;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_rename
        (JNIEnv *e, jclass cls, jlong lpszOld, jlong lpszNew) {
    return (jboolean) (rename((const char *) lpszOld, (const char *) lpszNew) == 0);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_exists0
        (JNIEnv *e, jclass cls, jlong lpsz) {
    return access((const char *) lpsz, F_OK) == 0;
}
