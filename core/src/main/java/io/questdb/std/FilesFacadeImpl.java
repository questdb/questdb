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

package io.questdb.std;

import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.MutableUtf8Sink;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

public class FilesFacadeImpl implements FilesFacade {

    public static final FilesFacade INSTANCE = new FilesFacadeImpl();
    public static final int _16M = 16 * 1024 * 1024;
    private final FsOperation copyFsOperation = this::copy;
    private final FsOperation hardLinkFsOperation = this::hardLink;
    private long mapPageSize = 0;

    @Override
    public boolean allocate(int fd, long size) {
        return Files.allocate(fd, size);
    }

    @Override
    public boolean allowMixedIO(CharSequence root) {
        return !Os.isWindows();
    }

    @Override
    public long append(int fd, long buf, int len) {
        return Files.append(fd, buf, len);
    }

    @Override
    public boolean close(int fd) {
        return Files.close(fd) == 0;
    }

    @Override
    public boolean closeRemove(int fd, LPSZ path) {
        // On Windows we cannot remove file that is open, close it first
        if (isRestrictedFileSystem() && fd > -1) {
            Files.close(fd);
        }

        // On other file systems we can remove file that is open, and sometimes we want to close the file descriptor
        // after the removal, in case when file descriptor is the lock FD.
        boolean ok = remove(path);
        if (!isRestrictedFileSystem() && fd > -1) {
            Files.close(fd);
        }
        return ok;
    }

    @Override
    public int copy(LPSZ from, LPSZ to) {
        return Files.copy(from, to);
    }

    @Override
    public long copyData(int srcFd, int destFd, long offsetSrc, long length) {
        return Files.copyData(srcFd, destFd, offsetSrc, length);
    }

    @Override
    public long copyData(int srcFd, int destFd, long offsetSrc, long destOffset, long length) {
        return Files.copyDataToOffset(srcFd, destFd, offsetSrc, destOffset, length);
    }

    @Override
    public int copyRecursive(Path src, Path dst, int dirMode) {
        return runRecursive(src, dst, dirMode, copyFsOperation);
    }

    @Override
    public int errno() {
        return Os.errno();
    }

    @Override
    public boolean exists(LPSZ path) {
        return Files.exists(path);
    }

    @Override
    public boolean exists(int fd) {
        return Files.exists(fd);
    }

    @Override
    public void fadvise(int fd, long offset, long len, int advise) {
        if (advise > -1) {
            Files.fadvise(fd, offset, len, advise);
        }
    }

    @Override
    public long findClose(long findPtr) {
        if (findPtr != 0) {
            Files.findClose(findPtr);
        }
        return 0;
    }

    @Override
    public long findFirst(LPSZ path) {
        long ptr = Files.findFirst(path);
        if (ptr == -1) {
            throw CairoException.critical(Os.errno()).put("findFirst failed on ").put(path);
        }
        return ptr;
    }

    @Override
    public long findName(long findPtr) {
        return Files.findName(findPtr);
    }

    @Override
    public int findNext(long findPtr) {
        int r = Files.findNext(findPtr);
        if (r == -1) {
            throw CairoException.critical(Os.errno()).put("findNext failed");
        }
        return r;
    }

    @Override
    public int findType(long findPtr) {
        return Files.findType(findPtr);
    }

    @Override
    public void fsync(int fd) {
        int res = Files.fsync(fd);
        if (res == 0) {
            return;
        }
        throw CairoException.critical(errno()).put("could not fsync [fd=").put(fd).put(']');
    }

    @Override
    public void fsyncAndClose(int fd) {
        int res = Files.fsync(fd);
        if (res == 0) {
            close(fd);
            return;
        }
        close(fd);
        throw CairoException.critical(errno()).put("could not fsync [fd=").put(fd).put(']');
    }

    @Override
    public long getDirSize(Path path) {
        return Files.getDirSize(path);
    }

    @Override
    public long getDiskFreeSpace(LPSZ path) {
        return Files.getDiskFreeSpace(path);
    }

    @Override
    public long getLastModified(LPSZ path) {
        return Files.getLastModified(path);
    }

    @Override
    public long getMapPageSize() {
        if (mapPageSize == 0) {
            mapPageSize = computeMapPageSize();
        }
        return mapPageSize;
    }

    @Override
    public long getOpenFileCount() {
        return Files.getOpenFileCount();
    }

    @Override
    public long getPageSize() {
        return Files.PAGE_SIZE;
    }

    @Override
    public int hardLink(LPSZ src, LPSZ hardLink) {
        return Files.hardLink(src, hardLink);
    }

    @Override
    public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
        return runRecursive(src, dst, dirMode, hardLinkFsOperation);
    }

    @Override
    public boolean isCrossDeviceCopyError(int errno) {
        return Os.isPosix() && errno == 18;
    }

    @Override
    public boolean isDirOrSoftLinkDir(LPSZ path) {
        return Files.isDirOrSoftLinkDir(path);
    }

    @Override
    public boolean isDirOrSoftLinkDirNoDots(Path path, int rootLen, long pUtf8NameZ, int type) {
        return Files.isDirOrSoftLinkDirNoDots(path, rootLen, pUtf8NameZ, type);
    }

    @Override
    public boolean isDirOrSoftLinkDirNoDots(Path path, int rootLen, long pUtf8NameZ, int type, MutableUtf8Sink nameSink) {
        return Files.isDirOrSoftLinkDirNoDots(path, rootLen, pUtf8NameZ, type, nameSink);
    }

    @Override
    public boolean isRestrictedFileSystem() {
        return Os.isWindows();
    }

    @Override
    public boolean isSoftLink(LPSZ softLink) {
        return Files.isSoftLink(softLink);
    }

    @Override
    public void iterateDir(LPSZ path, FindVisitor func) {
        long p = findFirst(path);
        if (p > 0) {
            try {
                do {
                    func.onFind(findName(p), findType(p));
                } while (findNext(p) > 0);
            } finally {
                findClose(p);
            }
        }
    }

    @Override
    public long length(int fd) {
        long r = Files.length(fd);
        if (r < 0) {
            throw CairoException.critical(Os.errno()).put("Checking file size failed");
        }
        return r;
    }

    @Override
    public long length(LPSZ name) {
        return Files.length(name);
    }

    @Override
    public int lock(int fd) {
        return Files.lock(fd);
    }

    @Override
    public void madvise(long address, long len, int advise) {
        if (advise > -1) {
            Files.madvise(address, len, advise);
        }
    }

    @Override
    public int mkdir(Path path, int mode) {
        return Files.mkdir(path, mode);
    }

    @Override
    public int mkdirs(Path path, int mode) {
        return Files.mkdirs(path, mode);
    }

    @Override
    public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
        return Files.mmap(fd, len, offset, flags, memoryTag);
    }

    @Override
    public long mremap(int fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        return Files.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
    }

    @Override
    public void msync(long addr, long len, boolean async) {
        int res = Files.msync(addr, len, async);
        if (res == 0) {
            return;
        }
        throw CairoException.critical(errno()).put("could not msync");
    }

    @Override
    public void munmap(long address, long size, int memoryTag) {
        Files.munmap(address, size, memoryTag);
    }

    @Override
    public int openAppend(LPSZ name) {
        return Files.openAppend(name);
    }

    @Override
    public int openCleanRW(LPSZ name, long size) {
        // Open files and if file exists, try exclusively lock it
        // If exclusive lock worked the file will be cleaned and allocated to the given size
        // Shared lock will be left on the file which will be removed when file descriptor is closed
        // If file did not exist, it will be allocated to the size and shared lock set
        return Files.openCleanRW(name, size);
    }

    @Override
    public int openRO(LPSZ name) {
        return Files.openRO(name);
    }

    @Override
    public int openRW(LPSZ name, long opts) {
        return Files.openRW(name, opts);
    }

    @Override
    public long read(int fd, long buf, long len, long offset) {
        return Files.read(fd, buf, len, offset);
    }

    @Override
    public boolean readLink(Path softLink, Path readTo) {
        return Files.readLink(softLink, readTo);
    }

    @Override
    public byte readNonNegativeByte(int fd, long offset) {
        return Files.readNonNegativeByte(fd, offset);
    }

    @Override
    public int readNonNegativeInt(int fd, long offset) {
        return Files.readNonNegativeInt(fd, offset);
    }

    @Override
    public long readNonNegativeLong(int fd, long offset) {
        return Files.readNonNegativeLong(fd, offset);
    }

    @Override
    public boolean remove(LPSZ name) {
        return Files.remove(name);
    }

    @Override
    public int rename(LPSZ from, LPSZ to) {
        return Files.rename(from, to);
    }

    @Override
    final public boolean rmdir(Path name) {
        return rmdir(name, true);
    }

    @Override
    public boolean rmdir(Path name, boolean lazy) {
        return Files.rmdir(name, lazy);
    }

    @Override
    public int softLink(LPSZ src, LPSZ softLink) {
        return Files.softLink(src, softLink);
    }

    @Override
    public int sync() {
        return Files.sync();
    }

    @Override
    public boolean touch(LPSZ path) {
        return Files.touch(path);
    }

    @Override
    public boolean truncate(int fd, long size) {
        return Files.truncate(fd, size);
    }

    @Override
    public int typeDirOrSoftLinkDirNoDots(Path path, int rootLen, long pUtf8NameZ, int type, @Nullable MutableUtf8Sink nameSink) {
        return Files.typeDirOrSoftLinkDirNoDots(path, rootLen, pUtf8NameZ, type, nameSink);
    }

    @Override
    public int unlink(LPSZ softLink) {
        return Files.unlink(softLink);
    }

    @Override
    public boolean unlinkOrRemove(Path path, Log LOG) {
        int checkedType = isSoftLink(path) ? Files.DT_LNK : Files.DT_UNKNOWN;
        return unlinkOrRemove(path, checkedType, LOG);
    }

    @Override
    public boolean unlinkOrRemove(Path path, int checkedType, Log LOG) {
        if (checkedType == Files.DT_LNK) {
            // in Windows ^ ^ will return DT_DIR, but that is ok as the behaviour
            // is to delete the link, not the contents of the target. in *nix
            // systems we can simply unlink, which deletes the link and leaves
            // the contents of the target intact
            if (unlink(path) == 0) {
                LOG.debug().$("removed by unlink [path=").$(path).I$();
                return true;
            } else {
                LOG.debug().$("failed to unlink, will remove [path=").$(path).I$();
            }
        }

        if (rmdir(path)) {
            LOG.debug().$("removed [path=").$(path).I$();
            return true;
        }
        LOG.debug().$("cannot remove [path=").$(path).$(", errno=").$(errno()).I$();
        return false;
    }

    public void walk(Path path, FindVisitor func) {
        Files.walk(path, func);
    }

    @Override
    public long write(int fd, long address, long len, long offset) {
        return Files.write(fd, address, len, offset);
    }

    private long computeMapPageSize() {
        long pageSize = getPageSize();
        long mapPageSize = pageSize * pageSize;
        if (mapPageSize < pageSize || mapPageSize > _16M) {
            if (_16M % pageSize == 0) {
                return _16M;
            }
            return pageSize;
        } else {
            return mapPageSize;
        }
    }

    private int runRecursive(Path src, Path dst, int dirMode, FsOperation operation) {
        int dstLen = dst.size();
        int srcLen = src.size();
        int len = src.size();
        long p = findFirst(src.$());

        if (!exists(dst.$()) && -1 == mkdir(dst, dirMode)) {
            return -1;
        }

        if (p > 0) {
            try {
                int res;
                do {
                    long name = findName(p);
                    if (Files.notDots(name)) {
                        int type = findType(p);
                        src.trimTo(len);
                        src.concat(name);
                        dst.concat(name);
                        if (type == Files.DT_FILE) {
                            if ((res = operation.invoke(src.$(), dst.$())) < 0) {
                                return res;
                            }
                        } else {
                            // Ignore if subfolder already exists
                            mkdir(dst.$(), dirMode);

                            if ((res = runRecursive(src, dst, dirMode, operation)) < 0) {
                                return res;
                            }
                        }
                        src.trimTo(srcLen);
                        dst.trimTo(dstLen);
                    }
                } while (findNext(p) > 0);
            } finally {
                findClose(p);
                src.trimTo(srcLen);
                dst.trimTo(dstLen);
            }
        }

        return 0;
    }

    @FunctionalInterface
    private interface FsOperation {
        int invoke(LPSZ src, LPSZ dst);
    }
}
