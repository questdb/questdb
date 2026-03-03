/*******************************************************************************
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

package io.questdb.test.cairo.fuzz;

import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.std.FilesFacade;
import io.questdb.std.FindVisitor;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.MutableUtf8Sink;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

public class FailureFileFacade implements FilesFacade {
    private final AtomicInteger failureGenerated = new AtomicInteger();
    private final FilesFacade ff;
    private final AtomicInteger osCallsCount = new AtomicInteger(0);

    public FailureFileFacade(@NotNull FilesFacade filesFacade) {
        this.ff = filesFacade;
    }

    @Override
    public boolean allocate(long fd, long size) {
        if (checkForFailure()) {
            return false;
        }
        return ff.allocate(fd, size);
    }

    @Override
    public boolean allowMixedIO(CharSequence root) {
        return ff.allowMixedIO(root);
    }

    @Override
    public long append(long fd, long buf, long len) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.append(fd, buf, len);
    }

    public void clearFailures() {
        osCallsCount.set(0);
        failureGenerated.set(0);
    }

    @Override
    public boolean close(long fd) {
        return ff.close(fd);
    }

    @Override
    public boolean closeRemove(long fd, LPSZ path) {
        return ff.closeRemove(fd, path);
    }

    @Override
    public int copy(LPSZ from, LPSZ to) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.copy(from, to);
    }

    @Override
    public long copyData(long srcFd, long destFd, long offsetSrc, long length) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.copyData(srcFd, destFd, offsetSrc, length);
    }

    @Override
    public long copyData(long srcFd, long destFd, long offsetSrc, long destOffset, long length) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.copyData(srcFd, destFd, offsetSrc, destOffset, length);
    }

    @Override
    public int copyRecursive(Path src, Path dst, int dirMode) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.copyRecursive(src, dst, dirMode);
    }

    @Override
    public int errno() {
        return ff.errno();
    }

    @Override
    public boolean exists(LPSZ path) {
        return ff.exists(path);
    }

    @Override
    public boolean exists(long fd) {
        return ff.exists(fd);
    }

    @Override
    public void fadvise(long fd, long offset, long len, int advise) {
        ff.fadvise(fd, offset, len, advise);
    }

    public int failureGenerated() {
        return failureGenerated.get();
    }

    @Override
    public long findClose(long findPtr) {
        return ff.findClose(findPtr);
    }

    @Override
    public long findFirst(LPSZ path) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.findFirst(path);
    }

    @Override
    public long findName(long findPtr) {
        return ff.findName(findPtr);
    }

    @Override
    public int findNext(long findPtr) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.findNext(findPtr);
    }

    @Override
    public int findType(long findPtr) {
        return ff.findType(findPtr);
    }

    @Override
    public void fsync(long fd) {
        ff.fsync(fd);
    }

    @Override
    public void fsyncAndClose(long fd) {
        ff.fsyncAndClose(fd);
    }

    @Override
    public long getDirSize(Path path) {
        return ff.getDirSize(path);
    }

    @Override
    public long getDiskFreeSpace(LPSZ path) {
        return ff.getDiskFreeSpace(path);
    }

    @Override
    public long getFileLimit() {
        return ff.getFileLimit();
    }

    @Override
    public int getFileSystemStatus(LPSZ lpszName) {
        return ff.getFileSystemStatus(lpszName);
    }

    @Override
    public long getLastModified(LPSZ path) {
        return ff.getLastModified(path);
    }

    @Override
    public long getMapCountLimit() {
        return ff.getMapCountLimit();
    }

    @Override
    public long getMapPageSize() {
        return ff.getMapPageSize();
    }

    @Override
    public long getOpenFileCount() {
        return ff.getOpenFileCount();
    }

    @Override
    public long getPageSize() {
        return ff.getPageSize();
    }

    @Override
    public int hardLink(LPSZ src, LPSZ hardLink) {
        return ff.hardLink(src, hardLink);
    }

    @Override
    public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
        return ff.hardLinkDirRecursive(src, dst, dirMode);
    }

    @Override
    public boolean isCrossDeviceCopyError(int errno) {
        return ff.isCrossDeviceCopyError(errno);
    }

    @Override
    public boolean isDirOrSoftLinkDir(LPSZ path) {
        return ff.isDirOrSoftLinkDir(path);
    }

    @Override
    public boolean isDirOrSoftLinkDirNoDots(Path path, int rootLen, long pUtf8NameZ, int type) {
        return ff.isDirOrSoftLinkDirNoDots(path, rootLen, pUtf8NameZ, type);
    }

    @Override
    public boolean isDirOrSoftLinkDirNoDots(Path path, int rootLen, long pUtf8NameZ, int type, MutableUtf8Sink nameSink) {
        return ff.isDirOrSoftLinkDirNoDots(path, rootLen, pUtf8NameZ, type, nameSink);
    }

    @Override
    public boolean isRestrictedFileSystem() {
        return ff.isRestrictedFileSystem();
    }

    @Override
    public boolean isSoftLink(LPSZ softLink) {
        return ff.isSoftLink(softLink);
    }

    @Override
    public void iterateDir(LPSZ path, FindVisitor func) {
        ff.iterateDir(path, func);
    }

    @Override
    public long length(long fd) {
        if (checkForFailure()) {
            throw CairoException.critical(Os.errno()).put("[failure-facade] checking file size failed");
        }
        return ff.length(fd);
    }

    @Override
    public long length(LPSZ name) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.length(name);
    }

    @Override
    public int lock(long fd) {
        return ff.lock(fd);
    }

    @Override
    public void madvise(long address, long len, int advise) {
        ff.madvise(address, len, advise);
    }

    @Override
    public int mkdir(LPSZ path, int mode) {
        checkForFailure();
        return ff.mkdir(path, mode);
    }

    @Override
    public int mkdirs(Path path, int mode) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.mkdirs(path, mode);
    }

    @Override
    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.mmap(fd, len, offset, flags, memoryTag);
    }

    @Override
    public long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.mmapNoCache(fd, len, offset, flags, memoryTag);
    }

    @Override
    public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        if (newSize != previousSize) {
            if (checkForFailure()) {
                return -1;
            }
            return ff.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        }
        return addr;
    }

    @Override
    public long mremapNoCache(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.mremapNoCache(fd, addr, previousSize, newSize, offset, mode, memoryTag);
    }

    @Override
    public void msync(long addr, long len, boolean async) {
        ff.msync(addr, len, async);
    }

    @Override
    public void munmap(long address, long size, int memoryTag) {
        ff.munmap(address, size, memoryTag);
    }

    @Override
    public long openAppend(LPSZ name) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.openAppend(name);
    }

    @Override
    public long openCleanRW(LPSZ name, long size) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.openCleanRW(name, size);
    }

    @Override
    public long openRO(LPSZ name) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.openRO(name);
    }

    @Override
    public long openRONoCache(LPSZ path) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.openRONoCache(path);
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.openRW(name, opts);
    }

    @Override
    public long openRWNoCache(LPSZ name, int opts) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.openRWNoCache(name, opts);
    }

    @Override
    public long read(long fd, long buf, long size, long offset) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.read(fd, buf, size, offset);
    }

    @Override
    public long readIntAsUnsignedLong(long fd, long offset) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.readIntAsUnsignedLong(fd, offset);
    }

    @Override
    public boolean readLink(Path softLink, Path readTo) {
        return ff.readLink(softLink, readTo);
    }

    @Override
    public byte readNonNegativeByte(long fd, long offset) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.readNonNegativeByte(fd, offset);
    }

    @Override
    public int readNonNegativeInt(long fd, long offset) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.readNonNegativeInt(fd, offset);
    }

    @Override
    public long readNonNegativeLong(long fd, long offset) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.readNonNegativeLong(fd, offset);
    }

    @Override
    public void remove(LPSZ name) {
        if (checkForFailure()) {
            throw CairoException.critical(errno()).put("[failure-facade] could not remove [file=").put(name).put(']');
        }
        ff.remove(name);
    }

    @Override
    public boolean removeQuiet(LPSZ name) {
        if (checkForFailure()) {
            return false;
        }
        return ff.removeQuiet(name);
    }

    @Override
    public int rename(LPSZ from, LPSZ to) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.rename(from, to);
    }

    @Override
    public boolean rmdir(Path name) {
        if (checkForFailure()) {
            return false;
        }
        return ff.rmdir(name);
    }

    @Override
    public boolean rmdir(Path name, boolean haltOnError) {
        if (checkForFailure()) {
            return false;
        }
        return ff.rmdir(name, haltOnError);
    }

    public void setToFailAfter(int ioFailureCallCount) {
        int osCalls;

        while (true) {
            osCalls = osCallsCount.get();
            if (osCalls > 0 || osCallsCount.compareAndSet(osCalls, ioFailureCallCount)) {
                return;
            }
        }
    }

    @Override
    public int softLink(LPSZ src, LPSZ softLink) {
        return ff.softLink(src, softLink);
    }

    @Override
    public int sync() {
        return ff.sync();
    }

    @Override
    public boolean touch(LPSZ path) {
        if (checkForFailure()) {
            return false;
        }
        return ff.touch(path);
    }

    @Override
    public boolean truncate(long fd, long size) {
        if (checkForFailure()) {
            return false;
        }
        return ff.truncate(fd, size);
    }

    @Override
    public int typeDirOrSoftLinkDirNoDots(Path path, int rootLen, long pUtf8NameZ, int type, MutableUtf8Sink nameSink) {
        return ff.typeDirOrSoftLinkDirNoDots(path, rootLen, pUtf8NameZ, type, nameSink);
    }

    @Override
    public int unlink(LPSZ softLink) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.unlink(softLink);
    }

    @Override
    public boolean unlinkOrRemove(Path path, Log LOG) {
        if (checkForFailure()) {
            return false;
        }
        return ff.unlinkOrRemove(path, LOG);
    }

    @Override
    public boolean unlinkOrRemove(Path path, int checkedType, Log LOG) {
        if (checkForFailure()) {
            return false;
        }
        return ff.unlinkOrRemove(path, checkedType, LOG);
    }

    @Override
    public void walk(Path src, FindVisitor func) {
        ff.walk(src, func);
    }

    @Override
    public long write(long fd, long address, long len, long offset) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.write(fd, address, len, offset);
    }

    private boolean checkForFailure() {
        boolean fail = osCallsCount.decrementAndGet() == 0;
        if (fail) {
            failureGenerated.incrementAndGet();
        }
        return fail;
    }
}
