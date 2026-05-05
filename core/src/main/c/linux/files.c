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

#define _GNU_SOURCE

#include "../share/files.h"
#include "../share/sysutil.h"
#include <sys/mman.h>
#include <errno.h>

// MADV_POPULATE_WRITE was added in Linux 5.14
#ifndef MADV_POPULATE_WRITE
#define MADV_POPULATE_WRITE 23
#endif
#include <limits.h>
#include <string.h>
#include <stdio.h>
#include <sys/sendfile.h>
#include <unistd.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <sys/vfs.h>
#include <fcntl.h>
#include <stdint.h>

static inline jlong _io_questdb_std_Files_mremap0
        (jint fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    void *orgAddr = (void *) address;
    void *newAddr = mremap(orgAddr, (size_t) previousLen, (size_t) newLen, MREMAP_MAYMOVE);
    if (newAddr == MAP_FAILED) {
        return -1;
    }
    return (jlong) newAddr;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_mremap0
        (JNIEnv *e, jclass cl, jint fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    return _io_questdb_std_Files_mremap0(fd, address, previousLen, newLen, offset, flags);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_copy
        (JNIEnv *e, jclass cls, jlong lpszFrom, jlong lpszTo) {
    const char *from = (const char *) lpszFrom;
    const char *to = (const char *) lpszTo;
    const int input = open(from, O_RDONLY);
    if (-1 == (input)) {
        return -1;
    }

    const int output = creat(to, 0644);
    if (-1 == (output)) {
        close(input);
        return -1;
    }

    // On linux sendfile can accept file as well as sockets
    struct stat fileStat = {0};
    fstat(input, &fileStat);

    size_t len = fileStat.st_size;
    while (len > 0) {
        ssize_t writtenLen;
        RESTARTABLE(sendfile(output, input, NULL, len > MAX_RW_COUNT ? MAX_RW_COUNT : len), writtenLen);

        if (writtenLen <= 0) {
            break;
        }
        len -= writtenLen;
    }

    close(input);
    close(output);

    return len == 0 ? 0 : -1;
}

size_t copyData0(int srcFd, long dstFd, off_t srcOffset, off_t dstOffset, int64_t length) {
    lseek64(dstFd, dstOffset, SEEK_SET);

    size_t len = length > 0 ? (size_t)length : SIZE_MAX;
    off_t offset = srcOffset;

    while (len > 0) {
        ssize_t writtenLen = sendfile64((int) dstFd, (int) srcFd, &offset, len > MAX_RW_COUNT ? MAX_RW_COUNT : len);
        if (writtenLen <= 0
            // Signals should not interrupt sendfile on Linux but just to align with POSIX standards
            && errno != EINTR) {
            break;
        }
        len -= writtenLen;
        // offset is already increased
    }

    return offset - srcOffset;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_copyData
        (JNIEnv *e, jclass cls, jint srcFd, jint destFd, jlong srcOffset, jlong length) {
    return (jlong) copyData0((int) srcFd, (int) destFd, (off_t) srcOffset, 0, (int64_t) length);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_copyDataToOffset
        (JNIEnv *e, jclass cls, jint srcFd, jint destFd, jlong srcOffset, jlong dstOffset, jlong length) {
    return (jlong) copyData0((int) srcFd, (int) destFd, (off_t) srcOffset, (off_t) dstOffset, (int64_t) length);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_fadvise0
        (JNIEnv *e, jclass cls, jint fd, jlong offset, jlong len, jint advise) {
    return posix_fadvise((int) fd, (off_t) offset, (off_t) len, advise);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_getPosixFadvRandom(JNIEnv *e, jclass cls) {
    return POSIX_FADV_RANDOM;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_getPosixFadvSequential(JNIEnv *e, jclass cls) {
    return POSIX_FADV_SEQUENTIAL;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_madvise0
        (JNIEnv *e, jclass cls, jlong address, jlong len, jint advise) {
    return madvise((void *) address, (size_t) len, advise);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_getMadvPopulateWrite(JNIEnv *e, jclass cls) {
    return MADV_POPULATE_WRITE;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_getPosixMadvRandom(JNIEnv *e, jclass cls) {
    return MADV_RANDOM;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_getPosixMadvSequential(JNIEnv *e, jclass cls) {
    return MADV_SEQUENTIAL;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_getPosixMadvDontneed(JNIEnv *e, jclass cls) {
    return MADV_DONTNEED;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getFileSystemStatus
        (JNIEnv *e, jclass cl, jlong lpszName) {
    struct statfs sb;
    if (statfs((const char *) lpszName, &sb) != 0) {
        return 0;
    }
    const jlong magic = (jlong) sb.f_type;
    const char *name;
    // Start from FLAG_FS_SUPPORTED: any case below that names the filesystem is, by
    // definition, recognised by name. mmap-safe and hard-fail cases override with
    // their full flag combos. The truly-unknown default branch clears this back to 0.
    jlong flags = FLAG_FS_SUPPORTED;
    switch (sb.f_type) {
        // mmap-safe whitelist for Linux
        case 0xEF53: // ext2, ext3, ext4
            name = "ext4";
            flags = FLAG_FS_SUPPORTED | FLAG_FS_MMAP_SAFE;
            break;
        case 0x58465342:
            name = "XFS";
            flags = FLAG_FS_SUPPORTED | FLAG_FS_MMAP_SAFE;
            break;
        case 0x2Fc12fc1:
            name = "ZFS";
            flags = FLAG_FS_SUPPORTED | FLAG_FS_MMAP_SAFE;
            break;
        case 0x794c7630:
            name = "OVERLAYFS";
            flags = FLAG_FS_SUPPORTED | FLAG_FS_MMAP_SAFE;
            break;
        case 0x01021994:
            name = "TMPFS";
            flags = FLAG_FS_SUPPORTED | FLAG_FS_MMAP_SAFE;
            break;
        // hard-fail: msync alone cannot make these durable enough
        case 0x6969:
            name = "NFS";
            flags = FLAG_FS_SUPPORTED | FLAG_FS_HARD_FAIL;
            break;
        // recognised but not mmap-safe: requires commit.mode=sync (or !-override)
        case 0x01021997:
            // 9p / Docker Desktop bind mount on WSL2.
            name = "V9FS";
            flags = FLAG_FS_SUPPORTED;
            break;
        case 0x65735546:
            // Generic FUSE — durability is driver-dependent (sshfs is unsafe).
            name = "FUSE";
            flags = FLAG_FS_SUPPORTED;
            break;
        case 0x6a656a63:
            // VirtIO-FS (Docker Desktop on macOS, lima, podman): mmap'd writes
            // are buffered in the client page cache and require msync/sync to
            // reach the underlying file. Same shape as 9p on WSL2.
            name = "virtiofs";
            flags = FLAG_FS_SUPPORTED;
            break;
        case 0xff534d42:
            name = "CIFS";
            flags = FLAG_FS_SUPPORTED;
            break;
        case 0x517b:
            name = "SMB";
            flags = FLAG_FS_SUPPORTED;
            break;
        case 0xfe534d42:
            name = "SMB2";
            flags = FLAG_FS_SUPPORTED;
            break;
        // recognised but unclassified: the gate falls back to "requires sync"
        case 0xadf5:    name = "ADFS";          break;
        case 0xadff:    name = "AFFS";          break;
        case 0x0187:    name = "AUTOFS";        break;
        case 0x62646576: name = "BDEVFS";       break;
        case 0x42465331: name = "BEFS";         break;
        case 0x1badface: name = "BFS";          break;
        case 0x42494e4d: name = "BINFMTFS";     break;
        case 0xcafe4a11: name = "BPF_FS";       break;
        case 0x9123683e: name = "BTRFS";        break;
        case 0x27e0eb:  name = "CGROUP";        break;
        case 0x63677270: name = "CGROUP2";      break;
        case 0x73757245: name = "CODA";         break;
        case 0x012ff7b7: name = "COH";          break;
        case 0x28cd3d45: name = "CRAMFS";       break;
        case 0x64626720: name = "DEBUGFS";      break;
        case 0x1373:    name = "DEVFS";         break;
        case 0x1cd1:    name = "DEVPTS";        break;
        case 0xf15f:    name = "ECRYPTFS";      break;
        case 0xde5e81e4: name = "EFIVARFS";     break;
        case 0x00414a53: name = "EFS";          break;
        case 0x137d:    name = "EXT";           break;
        case 0xef51:    name = "EXT2_OLD";      break;
        case 0xf2f52010: name = "F2FS";         break;
        case 0x4244:    name = "HFS";           break;
        case 0x00c0ffee: name = "HOSTFS";       break;
        case 0xf995e849: name = "HPFS";         break;
        case 0x958458f6: name = "HUGETLBFS";    break;
        case 0x9660:    name = "ISOFS";         break;
        case 0x72b6:    name = "JFFS2";         break;
        case 0x3153464a: name = "JFS";          break;
        case 0x19800202: name = "MQUEUE";       break;
        case 0x4d44:    name = "MSDOS";         break;
        case 0x11307854: name = "MTD_INODE_FS"; break;
        case 0x564c:    name = "NCP";           break;
        case 0x3434:    name = "NILFS";         break;
        case 0x6e736673: name = "NSFS";         break;
        case 0x5346544e: name = "NTFS_SB";      break;
        case 0x7461636f: name = "OCFS2";        break;
        case 0x9fa1:    name = "OPENPROM";      break;
        case 0x50495045: name = "PIPEFS";       break;
        case 0x9fa0:    name = "PROC";          break;
        case 0x6165676c: name = "PSTOREFS";     break;
        case 0x002f:    name = "QNX4";          break;
        case 0x68191122: name = "QNX6";         break;
        case 0x858458f6: name = "RAMFS";        break;
        case 0x52654973: name = "REISERFS";     break;
        case 0x7275:    name = "ROMFS";         break;
        case 0x73636673: name = "SECURITYFS";   break;
        case 0xf97cff8c: name = "SELINUX";      break;
        case 0x43415d53: name = "SMACK";        break;
        case 0x534f434b: name = "SOCKFS";       break;
        case 0x73717368: name = "SQUASHFS";     break;
        case 0x62656572: name = "SYSFS";        break;
        case 0x012ff7b6: name = "SYSV2";        break;
        case 0x012ff7b5: name = "SYSV4";        break;
        case 0x74726163: name = "TRACEFS";      break;
        case 0x15013346: name = "UDF";          break;
        case 0x00011954: name = "UFS";          break;
        case 0x9fa2:    name = "USBDEVICE";     break;
        case 0xa501fcf5: name = "VXFS";         break;
        case 0xabba1974: name = "XENFS";        break;
        case 0x012ff7b4: name = "XENIX";        break;
        default:
            // Magic not in our table: we genuinely cannot name this filesystem.
            // Strip FLAG_FS_SUPPORTED so the startup log and /warnings surface
            // it as UNSUPPORTED rather than implying we know what it is.
            name = "unknown";
            flags = 0;
            break;
    }
    strcpy((char *) lpszName, name);
    return magic | flags;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_setLastModified
        (JNIEnv *e, jclass cl, jlong lpszName, jlong millis) {
    struct timeval t[2];
    gettimeofday(&t[0], NULL);
    t[1].tv_sec = millis / 1000;
    t[1].tv_usec = ((millis % 1000) * 1000);
    return (jboolean) (utimes((const char *) lpszName, t) == 0);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getDiskSize(JNIEnv *e, jclass cl, jlong lpszPath) {
    struct statvfs buf;
    if (statvfs((const char *) lpszPath, &buf) == 0) {
        return (jlong) buf.f_bavail * (jlong )buf.f_bsize;
    }
    return -1;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_allocate
        (JNIEnv *e, jclass cl, jint fd, jlong len) {
    int rc = posix_fallocate(fd, 0, len);
    if (rc == 0) {
        return JNI_TRUE;
    }
    if (rc == EINVAL && len > 0) {
        // Some file systems (such as ZFS) do not support posix_fallocate
        struct stat st;
        int rc = fstat((int) fd, &st);
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

    errno = rc; // communicate errno to caller
    return JNI_FALSE;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getLastModified
        (JNIEnv *e, jclass cl, jlong pchar) {
    struct stat st;
    int r = stat((const char *) pchar, &st);
    return r == 0 ? ((1000 * st.st_mtim.tv_sec) + (st.st_mtim.tv_nsec / 1000000)) : r;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getFileLimit
        (JNIEnv *e, jclass cl) {
    struct rlimit limit;
    if (getrlimit(RLIMIT_NOFILE, &limit) == 0) {
        if (limit.rlim_cur == RLIM_INFINITY) {
            // Remap RLIM_INFINITY (~0UL, unsigned) to LONG_MAX (signed).
            return LONG_MAX;
        }
        return (jlong) limit.rlim_cur;
    }
    return -1;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getMapCountLimit
        (JNIEnv *e, jclass cl) {
    FILE* fd = fopen("/proc/sys/vm/max_map_count", "r");
    if (fd == NULL) {
        return 0;
    }
    long limit = 0L;
    int res = fscanf(fd, "%ld", &limit);
    fclose(fd);

    if (res == 1) {
        return limit;
    }
    return 0;
}
