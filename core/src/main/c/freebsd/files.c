/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

JNIEXPORT jlong JNICALL JavaCritical_io_questdb_std_Files_mremap0
        (jint fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    return _io_questdb_std_Files_mremap0(fd, address, previousLen, newLen, offset, flags);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Files_mremap0
        (JNIEnv *e, jclass cl, jint fd, jlong address, jlong previousLen, jlong newLen, jlong offset, jint flags) {
    return _io_questdb_std_Files_mremap0(fd, address, previousLen, newLen, offset, flags);
}

size_t copyData0(int srcFd, int dstFd, off_t srcOffset, off_t dstOffset, int64_t length) {
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
    if (statfs((const char *) lpszName, &sb) == 0) {
        // the path, which is usually passed here is 256 chars
        // the FS name is 16
        strcpy((char *) lpszName, sb.f_fstypename);
        switch (sb.f_type) {
            case 0x1C: // apfs
            case 0x1a:
                return FLAG_FS_SUPPORTED * ((jlong) sb.f_type);
            default:
                return sb.f_type;
        }
    }
    return 0;
}

#else

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Files_allocate
        (JNIEnv *e, jclass cl, jint fd, jlong len) {
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
    if (statfs((const char *) lpszName, &sb) == 0) {
        switch (sb.f_type) {
            case 0xadf5:
                strcpy((char *) lpszName, "ADFS");
                return sb.f_type;
            case 0xadff:
                strcpy((char *) lpszName, "AFFS");
                return sb.f_type;
            case 0x0187:
                strcpy((char *) lpszName, "AUTOFS");
                return sb.f_type;
            case 0x62646576:
                strcpy((char *) lpszName, "BDEVFS");
                return sb.f_type;
            case 0x42465331:
                strcpy((char *) lpszName, "BEFS");
                return sb.f_type;
            case 0x1badface:
                strcpy((char *) lpszName, "BFS");
                return sb.f_type;
            case 0x42494e4d:
                strcpy((char *) lpszName, "BINFMTFS");
                return sb.f_type;
            case 0xcafe4a11:
                strcpy((char *) lpszName, "BPF_FS");
                return sb.f_type;
            case 0x9123683e:
                strcpy((char *) lpszName, "BTRFS");
                return sb.f_type;
            case 0x27e0eb:
                strcpy((char *) lpszName, "CGROUP");
                return sb.f_type;
            case 0x63677270:
                strcpy((char *) lpszName, "CGROUP2");
                return sb.f_type;
            case 0xff534d42:
                strcpy((char *) lpszName, "CIFS");
                return sb.f_type;
            case 0x73757245:
                strcpy((char *) lpszName, "CODA");
                return sb.f_type;
            case 0x012ff7b7:
                strcpy((char *) lpszName, "COH");
                return sb.f_type;
            case 0x28cd3d45:
                strcpy((char *) lpszName, "CRAMFS");
                return sb.f_type;
            case 0x64626720:
                strcpy((char *) lpszName, "DEBUGFS");
                return sb.f_type;
            case 0x1373:
                strcpy((char *) lpszName, "DEVFS");
                return sb.f_type;
            case 0x1cd1:
                strcpy((char *) lpszName, "DEVPTS");
                return sb.f_type;
            case 0xf15f:
                strcpy((char *) lpszName, "ECRYPTFS");
                return sb.f_type;
            case 0xde5e81e4:
                strcpy((char *) lpszName, "EFIVARFS");
                return sb.f_type;
            case 0x00414a53:
                strcpy((char *) lpszName, "EFS");
                return sb.f_type;
            case 0x137d:
                strcpy((char *) lpszName, "EXT");
                return sb.f_type;
            case 0xef51:
                strcpy((char *) lpszName, "EXT2_OLD");
                return sb.f_type;
            case 0xf2f52010:
                strcpy((char *) lpszName, "F2FS");
                return sb.f_type;
            case 0x65735546:
                strcpy((char *) lpszName, "FUSE");
                return sb.f_type;
            case 0x4244:
                strcpy((char *) lpszName, "HFS");
                return sb.f_type;
            case 0x00c0ffee:
                strcpy((char *) lpszName, "HOSTFS");
                return sb.f_type;
            case 0xf995e849:
                strcpy((char *) lpszName, "HPFS");
                return sb.f_type;
            case 0x958458f6:
                strcpy((char *) lpszName, "HUGETLBFS");
                return sb.f_type;
            case 0x9660:
                strcpy((char *) lpszName, "ISOFS");
                return sb.f_type;
            case 0x72b6:
                strcpy((char *) lpszName, "JFFS2");
                return sb.f_type;
            case 0x3153464a:
                strcpy((char *) lpszName, "JFS");
                return sb.f_type;
            case 0x19800202:
                strcpy((char *) lpszName, "MQUEUE");
                return sb.f_type;
            case 0x4d44:
                strcpy((char *) lpszName, "MSDOS");
                return sb.f_type;
            case 0x11307854:
                strcpy((char *) lpszName, "MTD_INODE_FS");
                return sb.f_type;
            case 0x564c:
                strcpy((char *) lpszName, "NCP");
                return sb.f_type;
            case 0x6969:
                strcpy((char *) lpszName, "NFS");
                return sb.f_type;
            case 0x3434:
                strcpy((char *) lpszName, "NILFS");
                return sb.f_type;
            case 0x6e736673:
                strcpy((char *) lpszName, "NSFS");
                return sb.f_type;
            case 0x5346544e:
                strcpy((char *) lpszName, "NTFS_SB");
                return sb.f_type;
            case 0x7461636f:
                strcpy((char *) lpszName, "OCFS2");
                return sb.f_type;
            case 0x9fa1:
                strcpy((char *) lpszName, "OPENPROM");
                return sb.f_type;
            case 0x794c7630:
                strcpy((char *) lpszName, "OVERLAYFS");
                return FLAG_FS_SUPPORTED * ((jlong) sb.f_type);
            case 0x50495045:
                strcpy((char *) lpszName, "PIPEFS");
                return sb.f_type;
            case 0x9fa0:
                strcpy((char *) lpszName, "PROC");
                return sb.f_type;
            case 0x6165676c:
                strcpy((char *) lpszName, "PSTOREFS");
                return sb.f_type;
            case 0x002f:
                strcpy((char *) lpszName, "QNX4");
                return sb.f_type;
            case 0x68191122:
                strcpy((char *) lpszName, "QNX6");
                return sb.f_type;
            case 0x858458f6:
                strcpy((char *) lpszName, "RAMFS");
                return sb.f_type;
            case 0x52654973:
                strcpy((char *) lpszName, "REISERFS");
                return sb.f_type;
            case 0x7275:
                strcpy((char *) lpszName, "ROMFS");
                return sb.f_type;
            case 0x73636673:
                strcpy((char *) lpszName, "SECURITYFS");
                return sb.f_type;
            case 0xf97cff8c:
                strcpy((char *) lpszName, "SELINUX");
                return sb.f_type;
            case 0x43415d53:
                strcpy((char *) lpszName, "SMACK");
                return sb.f_type;
            case 0x517b:
                strcpy((char *) lpszName, "SMB");
                return sb.f_type;
            case 0xfe534d42:
                strcpy((char *) lpszName, "SMB2");
                return sb.f_type;
            case 0x534f434b:
                strcpy((char *) lpszName, "SOCKFS");
                return sb.f_type;
            case 0x73717368:
                strcpy((char *) lpszName, "SQUASHFS");
                return sb.f_type;
            case 0x62656572:
                strcpy((char *) lpszName, "SYSFS");
                return sb.f_type;
            case 0x012ff7b6:
                strcpy((char *) lpszName, "SYSV2");
                return sb.f_type;
            case 0x012ff7b5:
                strcpy((char *) lpszName, "SYSV4");
                return sb.f_type;
            case 0x01021994:
                strcpy((char *) lpszName, "TMPFS");
                return sb.f_type;
            case 0x74726163:
                strcpy((char *) lpszName, "TRACEFS");
                return sb.f_type;
            case 0x15013346:
                strcpy((char *) lpszName, "UDF");
                return sb.f_type;
            case 0x00011954:
                strcpy((char *) lpszName, "UFS");
                return sb.f_type;
            case 0x35:
                strcpy((char *) lpszName, "UFS");
                // tested with FreeBSD 13.2, partition type 'freebsd-ufs'
                return FLAG_FS_SUPPORTED * sb.f_type;
            case 0x9fa2:
                strcpy((char *) lpszName, "USBDEVICE");
                return sb.f_type;
            case 0x01021997:
                strcpy((char *) lpszName, "V9FS");
                return FLAG_FS_SUPPORTED * ((jlong) sb.f_type);
            case 0xa501fcf5:
                strcpy((char *) lpszName, "VXFS");
                return sb.f_type;
            case 0xabba1974:
                strcpy((char *) lpszName, "XENFS");
                return sb.f_type;
            case 0x012ff7b4:
                strcpy((char *) lpszName, "XENIX");
                return sb.f_type;
            case 0x58465342:
                strcpy((char *) lpszName, "XFS");
                return sb.f_type;
            case 0xEF53: // ext2, ext3, ext4
                strcpy((char *) lpszName, "ext4");
                return FLAG_FS_SUPPORTED * ((jlong) sb.f_type);
            default:
                strcpy((char *) lpszName, "unknown");
                return sb.f_type;
        }
    }
    return 0;
}

#endif


