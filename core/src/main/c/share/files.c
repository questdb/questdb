/*+*****************************************************************************
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

#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include "sysutil.h"

#include <stdlib.h>
#include <stdint.h>
#include <dirent.h>
#include <string.h>
#include <sys/errno.h>
#include <sys/time.h>
#include <sys/mount.h>
#include "files.h"

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_write
        (JNIEnv *e, jclass cl, jint fd, jlong address, jlong len, jlong offset) {
    off_t writeOffset = offset;
    ssize_t written;

    do {
        size_t count = len > MAX_RW_COUNT ? MAX_RW_COUNT : len;
        RESTARTABLE(pwrite((int) fd, (void *) (address), count, writeOffset), written);
        if (written < 0) {
            return written;
        }
        len -= written;
        writeOffset += written;
        address += written;
        // Exit if written == 0 or there is nothing to write
    } while (len > 0 && written > 0);

    return writeOffset - offset;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_mmap0
        (JNIEnv *e, jclass cl, jint fd, jlong len, jlong offset, jint flags, jlong baseAddress) {
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
        (JNIEnv *e, jclass cl, jint fd, jlong address, jlong len) {
    ssize_t res;
    RESTARTABLE(write((int) fd, (void *) (address), (size_t) len), res);
    return res;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_read
        (JNIEnv *e, jclass cl, jint fd, jlong address, jlong len, jlong offset) {

    off_t readOffset = offset;
    ssize_t read;

    do {
        size_t count = len > MAX_RW_COUNT ? MAX_RW_COUNT : len;
        RESTARTABLE(pread((int) fd, (void *) (address), count, readOffset), read);
        if (read < 0) {
            return read;
        }
        len -= read;
        readOffset += read;
        address += read;

        // Exit if read the given length or EOL (read == 0)
    } while (len > 0 && read > 0);

    return readOffset - offset;
}

JNIEXPORT jbyte JNICALL Java_io_questdb_std_Files_readNonNegativeByte
        (JNIEnv *e, jclass cl,
         jint fd,
         jlong offset) {
    jbyte result;
    ssize_t readLen = pread((int) fd, (void *) &result, sizeof(jbyte), (off_t) offset);
    if (readLen != sizeof(jbyte)) {
        return -1;
    }
    return result;
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_Files_readNonNegativeShort
        (JNIEnv *e, jclass cl,
         jint fd,
         jlong offset) {
    jshort result;
    ssize_t readLen = pread((int) fd, (void *) &result, sizeof(jshort), (off_t) offset);
    if (readLen != sizeof(jshort)) {
        return -1;
    }
    return result;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_readNonNegativeInt
        (JNIEnv *e, jclass cl, jint fd, jlong offset) {
    jint result;
    ssize_t readLen = pread((int) fd, (void *) &result, sizeof(jint), (off_t) offset);
    if (readLen != sizeof(jint)) {
        return -1;
    }
    return result;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_readIntAsUnsignedLong
        (JNIEnv *e, jclass cl, jint fd, jlong offset) {
    uint32_t result;
    ssize_t readLen = pread((int) fd, (void *) &result, sizeof(jint), (off_t) offset);
    if (readLen != sizeof(uint32_t)) {
        return -1;
    }
    return (jlong) result;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_readNonNegativeLong
        (JNIEnv *e, jclass cl, jint fd, jlong offset) {
    jlong result;
    ssize_t readLen;
    RESTARTABLE(pread((int) fd, (void *) &result, sizeof(jlong), (off_t) offset), readLen);
    if (readLen != sizeof(jlong)) {
        return -1;
    }
    return result;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openRO
        (JNIEnv *e, jclass cl, jlong lpszName) {
    return open((const char *) lpszName, O_RDONLY);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_close0
        (JNIEnv *e, jclass cl, jint fd) {
    return close((int) fd);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openRW
        (JNIEnv *e, jclass cl, jlong lpszName) {
    umask(2);
    return open((const char *) lpszName, O_CREAT | O_RDWR, 0644);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openRWOpts
        (JNIEnv *e, jclass cl, jlong lpszName, jint opts) {
    umask(2);
    return open((const char *) lpszName, O_CREAT | O_RDWR | opts, 0644);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openAppend
        (JNIEnv *e, jclass cl, jlong lpszName) {
    umask(2);
    return open((const char *) lpszName, O_CREAT | O_WRONLY | O_APPEND, 0644);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_length0
        (JNIEnv *e, jclass cl, jlong pchar) {
    struct stat st;

    int r = stat((const char *) pchar, &st);
    return r == 0 ? st.st_size : r;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_hardLink
        (JNIEnv *e, jclass cl, jlong pcharSrc, jlong pcharHardLink) {
    return link((const char *) pcharSrc, (const char *) pcharHardLink);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_readLink0
        (JNIEnv *e, jclass cl, jlong path, jlong buf, jint len) {
    return (jint) readlink((const char *) path, (char *) buf, len);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_isSoftLink
        (JNIEnv *e, jclass cl, jlong pcharSoftLink) {

    struct stat st;
    if (lstat((const char *) pcharSoftLink, &st) == 0) {
        return S_ISLNK(st.st_mode);
    }
    return JNI_FALSE;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_isDir
        (JNIEnv *e, jclass cl, jlong pchar) {

    struct stat st;
    if (stat((const char *) pchar, &st) == 0) {
        return S_ISDIR(st.st_mode);
    }
    return JNI_FALSE;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_softLink
        (JNIEnv *e, jclass cl, jlong pcharSrc, jlong pcharSoftLink) {
    return symlink((const char *) pcharSrc, (const char *) pcharSoftLink);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_unlink
        (JNIEnv *e, jclass cl, jlong pcharSoftLink) {
    return unlink((const char *) pcharSoftLink);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_mkdir
        (JNIEnv *e, jclass cl, jlong pchar, jint mode) {
    return mkdir((const char *) pchar, (mode_t) mode);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_length
        (JNIEnv *e, jclass cl, jint fd) {
    struct stat st;
    int r = fstat((int) fd, &st);
    return r == 0 ? st.st_size : r;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_exists
        (JNIEnv *e, jclass cl, jint fd) {
    struct stat st;
    int r = fstat((int) fd, &st);
    return (jboolean) (r == 0 ? st.st_nlink > 0 : 0);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_getStdOutFd
        (JNIEnv *e, jclass cl) {
    return (jlong) 1;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_truncate
        (JNIEnv *e, jclass cl, jint fd, jlong len) {
    if (ftruncate((int) fd, len) == 0) {
        return JNI_TRUE;
    }
    return JNI_FALSE;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_msync(JNIEnv *e, jclass cl, jlong addr, jlong len, jboolean async) {
    return msync((void *) addr, len, async ? MS_ASYNC : MS_SYNC);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_fsync(JNIEnv *e, jclass cl, jint fd) {
    return fsync((int) fd);
}

/* Darwin only: the F_FULLFSYNC and F_BARRIERFSYNC code below compiles only on Apple platforms.
 *
 * fsync(2) on Darwin pushes dirty pages to the drive but does not flush the drive's write cache; Apple's man
 * page directs callers to F_FULLFSYNC when data must be physically written. macOS has no fdatasync either.
 *
 * Some filesystems (network mounts, some FUSE) reject F_FULLFSYNC with ENOTSUP or EINVAL; there fsync is the
 * strongest barrier available, so the call falls back to it rather than failing the commit. Any other errno
 * is an I/O failure and propagates to the caller. */
#if defined(__APPLE__)
static jint darwin_full_fsync(int fd) {
    if (fcntl(fd, F_FULLFSYNC) == 0) {
        return 0;
    }
    if (errno == ENOTSUP || errno == EINVAL || errno == ENOTTY) {
        return fsync(fd);
    }
    return -1;
}
#endif

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_fdatasync0(JNIEnv *e, jclass cl, jint fd) {
#if defined(__APPLE__)
    /* macOS has no fdatasync. F_FULLFSYNC is the only real barrier, and it is stronger than this function's
     * contract. */
    return darwin_full_fsync((int) fd);
#elif defined(__linux__) || defined(__FreeBSD__)
    return fdatasync((int) fd);
#else
    return fsync((int) fd);
#endif
}

/* Ordering barrier, not a durability barrier: writes issued before it reach the medium before writes issued
 * after it, but it may return with the data still in the drive's volatile cache.
 *
 * The WAL commit path writes column data, then _event, then the sequencer record, and a sequencer record must
 * never reach media ahead of the data it references: an ordering property. The F_FULLFSYNC at the commit
 * point (the sequencer flush that advances localDurableSeqTxn) makes the commit durable, and because it
 * flushes the device cache, everything ordered before it lands with it.
 *
 * The ordering holds per device: it composes only while the WAL segments, _event and the sequencer share the
 * database root's filesystem; a deployment that splits them across volumes would need a full flush of each.
 *
 * F_BARRIERFSYNC on Darwin. Elsewhere there is no cheaper barrier, so this maps to the durable one. */
JNIEXPORT jint JNICALL Java_io_questdb_std_Files_barrierFsync0(JNIEnv *e, jclass cl, jint fd) {
#if defined(__APPLE__)
    if (fcntl(fd, F_BARRIERFSYNC) == 0) {
        return 0;
    }
    if (errno == ENOTSUP || errno == EINVAL || errno == ENOTTY) {
        return fsync((int) fd);
    }
    return -1;
#elif defined(__linux__) || defined(__FreeBSD__)
    /* No cheaper barrier exists here, and fdatasync both orders and persists. */
    return fdatasync((int) fd);
#else
    return fsync((int) fd);
#endif
}

/* fsync's contract, data and all metadata, with the device cache flushed.
 *
 * Not fdatasync0: fdatasync may skip metadata that reading the data back does not need. Only Darwin differs
 * from fsync, where F_FULLFSYNC adds the missing device-cache flush and keeps the contract. */
JNIEXPORT jint JNICALL Java_io_questdb_std_Files_fsyncDurable0(JNIEnv *e, jclass cl, jint fd) {
#if defined(__APPLE__)
    return darwin_full_fsync((int) fd);
#else
    /* fsync flushes the device cache here, so this is fsync. */
    return fsync((int) fd);
#endif
}

#if defined(__linux__)
/* <fcntl.h> declares sync_file_range(2) only under _GNU_SOURCE (glibc), which this file does not define, so
 * declare it here, like syncfs below. Linux-only, since 2.6.17; off_t is off64_t on our LP64 targets
 * (x86-64, aarch64). Newer GCC rejects an implicit declaration as an error. */
extern int sync_file_range(int fd, off_t offset, off_t nbytes, unsigned int flags);
#endif

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_syncFileRange0(JNIEnv *e, jclass cl, jint fd, jlong offset, jlong nbytes, jint flags) {
#if defined(__linux__)
    /* Starts writeback of the page cache over [offset, offset+nbytes) to the device's cache, without a
     * device flush; durability needs a following device flush. It acts only on pages the kernel knows are
     * dirty, so mmap-dirtied pages must be msynced (or written with write()) first. */
    return sync_file_range((int) fd, offset, nbytes, (unsigned) flags);
#else
    /* No equivalent primitive: a no-op returning 0. Callers must not rely on it for durability. */
    (void) fd; (void) offset; (void) nbytes; (void) flags;
    return 0;
#endif
}

#if defined(__linux__)
/* syncfs(2) is exposed by <unistd.h> only under _GNU_SOURCE (glibc); this file does not define it
 * globally, so forward-declare the prototype here. syncfs has existed in glibc since 2.14 and the
 * syscall since Linux 2.6.39, so it is present on every supported target. */
extern int syncfs(int fd);
#endif

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_syncfs0(JNIEnv *e, jclass cl, jint fd) {
#if defined(__linux__)
    /* Writes back all dirty data of the filesystem containing fd, journals its pending metadata changes
     * (including ext4 extent conversions), and issues one device cache flush, in place of one fdatasync
     * per file. */
    return syncfs((int) fd);
#else
    /* No filesystem-wide sync bound to an fd: flush this file only. Callers check isSyncfsFileSystemWide()
     * and flush their write set per file instead. Darwin needs F_FULLFSYNC for the flush to reach the
     * device. */
#if defined(__APPLE__)
    return darwin_full_fsync((int) fd);
#else
    return fsync((int) fd);
#endif
#endif
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_sync(JNIEnv *e, jclass cl) {
    sync();
    return 0;
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
    int type;
    struct dirent *entry;
} FIND;

void setFind(FIND *find, struct dirent *entry) {
    find->entry = entry;
    find->type = entry->d_type;
}

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
    setFind(find, entry);
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
    struct dirent *entry = readdir(find->dir);
    if (entry) {
        setFind(find, entry);
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
    return ((FIND *) findPtr)->type;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_lock
        (JNIEnv *e, jclass cl, jint fd) {
    return flock((int) fd, LOCK_EX | LOCK_NB);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_openCleanRW
        (JNIEnv *e, jclass cl, jlong lpszName, jlong size) {
    jint fd = open((const char *) lpszName, O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        // error opening / creating file
        return -1;
    }

    jlong fileSize = Java_io_questdb_std_Files_length(e, cl, fd);
    if (fileSize > 0) {
        ssize_t res;
        RESTARTABLE(flock((int) fd, LOCK_EX | LOCK_NB), res);
        if (res == 0) {
            // truncate file to 0 byte
            if (ftruncate(fd, 0) == 0) {
                // allocate file to `size`
                if (Java_io_questdb_std_Files_allocate(e, cl, fd, size) == JNI_TRUE) {
                    // Zero the file and msync, so that we have no unpleasant side effects like non-zero bytes read on ZFS.
                    // See https://github.com/questdb/questdb/issues/4756
                    void *addr = mmap(NULL, (size_t) size, PROT_READ | PROT_WRITE, MAP_SHARED, (int) fd, 0);
                    if (addr != MAP_FAILED) {
                        memset(addr, 0, size);
                        if (msync(addr, size, MS_SYNC) == 0) {
                            munmap(addr, (size_t) size);
                            // finally, downgrade to shared lock
                            if (flock((int) fd, LOCK_SH) == 0) {
                                // success
                                return fd;
                            }
                        }
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

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_rename
        (JNIEnv *e, jclass cls, jlong lpszOld, jlong lpszNew) {
    int err = rename((const char *) lpszOld, (const char *) lpszNew);
    if (err != 0) {
        return errno == EXDEV ? FILES_RENAME_ERR_EXDEV : FILES_RENAME_ERR_OTHER;
    }
    return FILES_RENAME_ERR_OK;
}

/* POSIX: rename(2) cannot be made durable by itself; the caller fsyncs the parent directory afterwards, so
 * this is rename(). The Windows build adds MOVEFILE_WRITE_THROUGH, since Windows has no directory handle to
 * fsync. */
JNIEXPORT jint JNICALL Java_io_questdb_std_Files_renameDurable0
        (JNIEnv *e, jclass cls, jlong lpszOld, jlong lpszNew) {
    return Java_io_questdb_std_Files_rename(e, cls, lpszOld, lpszNew);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_exists0
        (JNIEnv *e, jclass cls, jlong lpsz) {
    return access((const char *) lpsz, F_OK) == 0;
}
