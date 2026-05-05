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

#include "../share/files.h"
#include <sys/mman.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include "../share/sysutil.h"

#ifdef __APPLE__

#include <copyfile.h>
#include <unistd.h>
#include <sys/fcntl.h>
#include <sys/mount.h>
#include <sys/stat.h>

#else

#include <unistd.h>
#include <sys/fcntl.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <sys/time.h>
#include <sys/stat.h>

#endif

static inline jlong _io_questdb_std_Files_mremap0
        (jint fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    int prot = 0;

    if (flags == com_questdb_std_Files_MAP_RO) {
        prot = PROT_READ;
    } else if (flags == com_questdb_std_Files_MAP_RW) {
        prot = PROT_READ | PROT_WRITE;
    }

    void *orgAddr = (void *) address;
    void *newAddr = mmap(orgAddr, (size_t) newLen, prot, MAP_SHARED, (int) fd, offset);
    if (orgAddr != newAddr) {
        munmap(orgAddr, (size_t) previousLen);
    }
    if (newAddr == MAP_FAILED) {
	    return -1;
    }
    return (jlong) newAddr;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_mremap0
        (JNIEnv *e, jclass cl, jint fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    return _io_questdb_std_Files_mremap0(fd, address, previousLen, newLen, offset, flags);
}

size_t copyData0(int srcFd, long dstFd, off_t srcOffset, off_t dstOffset, int64_t length) {
    char buf[4096 * 4]; // 16K
    off_t read_sz;
    off_t rd_off = srcOffset;
    off_t wrt_off = dstOffset;
    off_t len;

    if (length < 0) {
        len = LONG_MAX - srcOffset;
    } else {
        len = length;
    }
    off_t hi = srcOffset + len;

    for (;;) {
        RESTARTABLE(pread(srcFd, buf, sizeof buf, rd_off), read_sz);
        if (read_sz <= 0) {
            break;
        }
        char *out_ptr = buf;

        if (rd_off + read_sz > hi) {
            read_sz = hi - rd_off;
        }

        long wrtn;
        do {
            RESTARTABLE(pwrite(dstFd, out_ptr, read_sz, wrt_off), wrtn);
            if (wrtn >= 0) {
                read_sz -= wrtn;
                out_ptr += wrtn;
                wrt_off += wrtn;
            } else {
                break;
            }
        } while (read_sz > 0);

        if (read_sz > 0) {
            // error
            return -1;
        }

        rd_off += wrtn;
        if (rd_off >= hi) {
            /* Success! */
            break;
        }
    }

    return rd_off - srcOffset;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_copyData
        (JNIEnv *e, jclass cls, jint srcFd, jint dstFd, jlong srcOffset, jlong length) {
    return (jlong) copyData0((int) srcFd, (int) dstFd, srcOffset, 0, (int64_t) length);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_copyDataToOffset
        (JNIEnv *e, jclass cls, jint srcFd, jint dstFd, jlong srcOffset, jlong dstOffset, jlong length) {
    return (jlong) copyData0((int) srcFd, (int) dstFd, srcOffset, dstOffset, (int64_t) length);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getDiskSize(JNIEnv *e, jclass cl, jlong lpszPath) {
    struct statfs buf;
    if (statfs((const char *) lpszPath, &buf) == 0) {
        return (jlong)buf.f_bavail * (jlong)buf.f_bsize;
    }
    return -1;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_setLastModified
        (JNIEnv *e, jclass cl, jlong lpszName, jlong millis) {
    struct timeval t[2];
    gettimeofday(&t[0], NULL);
    t[1].tv_sec = millis / 1000;
#ifdef __APPLE__
    t[1].tv_usec = (__darwin_suseconds_t) ((millis % 1000) * 1000);
#else
    t[1].tv_usec = ((millis % 1000) * 1000);
#endif
    return (jboolean) (utimes((const char *) lpszName, t) == 0);
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

#ifdef __APPLE__

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_allocate
        (JNIEnv *e, jclass cl, jint fd, jlong len) {
    // MACOS allocates additional space. Check what size the file currently is
    struct stat st;
    int _fd = (int) fd;
    if (fstat(_fd, &st) != 0) {
        return JNI_FALSE;
    }
    const jlong fileLen = st.st_blksize * st.st_blocks;
    jlong deltaLen = len - fileLen;
    if (deltaLen > 0) {
        // F_ALLOCATECONTIG - try to allocate continuous space.
        fstore_t flags = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, deltaLen, 0};
        int result = fcntl(_fd, F_PREALLOCATE, &flags);
        if (result == -1) {
            // F_ALLOCATEALL - try to allocate non-continuous space.
            flags.fst_flags = F_ALLOCATEALL;
            result = fcntl((int) fd, F_PREALLOCATE, &flags);
            if (result == -1) {
                return JNI_FALSE;
            }
        }
    }
    return ftruncate((int) fd, len) == 0;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_copy
        (JNIEnv *e, jclass cls, jlong lpszFrom, jlong lpszTo) {
    const char *from = (const char *) lpszFrom;
    const char *to = (const char *) lpszTo;
    const int input = open(from, O_RDONLY);
    if (input == -1) {
        return -1;
    }

    const int output = creat(to, 0644);
    if (output == -1) {
        close(input);
        return -1;
    }

    // On Apple there is fcopyfile
    int result = fcopyfile(input, output, 0, COPYFILE_ALL);

    close(input);
    close(output);

    return result;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getFileSystemStatus
        (JNIEnv *e, jclass cl, jlong lpszName) {
    struct statfs sb;
    if (statfs((const char *) lpszName, &sb) != 0) {
        return 0;
    }
    // the path passed in is usually 256 chars; f_fstypename is at most 16 and
    // zero-padded after the name, so checking the byte after the expected length
    // for '\0' yields an exact match without a libc dependency.
    strcpy((char *) lpszName, sb.f_fstypename);
    const jlong magic = (jlong) sb.f_type;
    const char *n = (const char *) lpszName;

    // APFS - the only mmap-safe filesystem on macOS. Apple bumps f_type across
    // releases (0x1C, 0x1a, 0x19 seen so far), so match by name as well to
    // future-proof against another magic change.
    if ((n[0]=='a' && n[1]=='p' && n[2]=='f' && n[3]=='s' && n[4]==0)
            || sb.f_type == 0x1C || sb.f_type == 0x1a || sb.f_type == 0x19) {
        return magic | FLAG_FS_SUPPORTED | FLAG_FS_MMAP_SAFE;
    }
    // VirtIO-FS (Docker Desktop on macOS): mmap'd writes are buffered in the
    // client page cache; without msync they silently never reach the underlying
    // file, same as 9p on WSL2.
    if (n[0]=='v' && n[1]=='i' && n[2]=='r' && n[3]=='t'
            && n[4]=='i' && n[5]=='o' && n[6]=='f' && n[7]=='s' && n[8]==0) {
        return magic | FLAG_FS_SUPPORTED;
    }
    // SMB share - not mmap-safe but commit.mode=sync makes it usable.
    if (n[0]=='s' && n[1]=='m' && n[2]=='b' && n[3]=='f' && n[4]=='s' && n[5]==0) {
        return magic | FLAG_FS_SUPPORTED;
    }
    // NFS - hard fail; msync alone cannot make it durable enough.
    if (n[0]=='n' && n[1]=='f' && n[2]=='s' && n[3]==0) {
        return magic | FLAG_FS_SUPPORTED | FLAG_FS_HARD_FAIL;
    }
    // FUSE-based filesystems (macFUSE, sshfs, etc.) - durability is driver-dependent.
    if ((n[0]=='f' && n[1]=='u' && n[2]=='s' && n[3]=='e' && n[4]==0)
            || (n[0]=='m' && n[1]=='a' && n[2]=='c' && n[3]=='f' && n[4]=='u' && n[5]=='s' && n[6]=='e' && n[7]==0)
            || (n[0]=='o' && n[1]=='s' && n[2]=='x' && n[3]=='f' && n[4]=='u' && n[5]=='s' && n[6]=='e' && n[7]==0)) {
        return magic | FLAG_FS_SUPPORTED;
    }
    return magic;
}

#else

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

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_copy
        (JNIEnv *e, jclass cls, jlong lpszFrom, jlong lpszTo) {
    const char* from = (const char *) lpszFrom;
    const char* to = (const char *) lpszTo;

    const int input = open(from, O_RDONLY);
    if (-1 ==  (input)) {
        return -1;
    }

    const int output = creat(to, 0644);
    if (-1 == (output)) {
        close(input);
        return -1;
    }

    int result = copyData0(input, output, 0, 0, -1);
    close(input);
    close(output);

    return result;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getFileSystemStatus
        (JNIEnv *e, jclass cl, jlong lpszName) {
    struct statfs sb;
    if (statfs((const char *) lpszName, &sb) != 0) {
        return 0;
    }
    const jlong magic = (jlong) sb.f_type;
    const char *name;
    jlong flags = 0;
    switch (sb.f_type) {
        // mmap-safe whitelist for FreeBSD
        case 0x35:
            // tested with FreeBSD 13.2, partition type 'freebsd-ufs'
            name = "UFS";
            flags = FLAG_FS_SUPPORTED | FLAG_FS_MMAP_SAFE;
            break;
        case 0x2Fc12fc1:
            name = "ZFS";
            flags = FLAG_FS_SUPPORTED | FLAG_FS_MMAP_SAFE;
            break;
        case 0x01021994:
            name = "TMPFS";
            flags = FLAG_FS_SUPPORTED | FLAG_FS_MMAP_SAFE;
            break;
        // hard-fail
        case 0x6969:
            name = "NFS";
            flags = FLAG_FS_SUPPORTED | FLAG_FS_HARD_FAIL;
            break;
        // recognised but not mmap-safe: requires commit.mode=sync (or !-override)
        case 0x01021997:
            name = "V9FS";
            flags = FLAG_FS_SUPPORTED;
            break;
        case 0x65735546:
            name = "FUSE";
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
        // recognised but unclassified: gate falls back to "requires sync"
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
        case 0x794c7630: name = "OVERLAYFS";    break;
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
        case 0x58465342: name = "XFS";          break;
        case 0xEF53:    name = "ext4";          break;
        default:
            name = "unknown";
            break;
    }
    strcpy((char *) lpszName, name);
    return magic | flags;
}

#endif

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getFileLimit
        (JNIEnv *e, jclass cl) {
    return 0; // no-op
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_getMapCountLimit
        (JNIEnv *e, jclass cl) {
    return 0; // no-op
}
