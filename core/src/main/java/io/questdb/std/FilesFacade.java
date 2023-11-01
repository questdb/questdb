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

import io.questdb.log.Log;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.MutableUtf8Sink;
import io.questdb.std.str.Path;

public interface FilesFacade {
    long MAP_FAILED = -1;

    boolean allocate(int fd, long size);

    boolean allowMixedIO(CharSequence root);

    long append(int fd, long buf, int len);

    boolean close(int fd);

    boolean closeRemove(int fd, LPSZ path);

    int copy(LPSZ from, LPSZ to);

    long copyData(int srcFd, int destFd, long offsetSrc, long length);

    long copyData(int srcFd, int destFd, long offsetSrc, long destOffset, long length);

    int copyRecursive(Path src, Path dst, int dirMode);

    int errno();

    boolean exists(LPSZ path);

    boolean exists(int fd);

    void fadvise(int fd, long offset, long len, int advise);

    long findClose(long findPtr);

    long findFirst(LPSZ path);

    long findName(long findPtr);

    int findNext(long findPtr);

    int findType(long findPtr);

    void fsync(int fd);

    void fsyncAndClose(int fd);

    long getDirSize(Path path);

    long getDiskFreeSpace(LPSZ path);

    long getLastModified(LPSZ path);

    long getMapPageSize();

    long getOpenFileCount();

    long getPageSize();

    int hardLink(LPSZ src, LPSZ hardLink);

    int hardLinkDirRecursive(Path src, Path dst, int dirMode);

    boolean isCrossDeviceCopyError(int errno);

    boolean isDirOrSoftLinkDir(LPSZ path);

    boolean isDirOrSoftLinkDirNoDots(Path path, int rootLen, long pUtf8NameZ, int type);

    boolean isDirOrSoftLinkDirNoDots(Path path, int rootLen, long pUtf8NameZ, int type, MutableUtf8Sink nameSink);

    boolean isRestrictedFileSystem();

    boolean isSoftLink(LPSZ softLink);

    void iterateDir(LPSZ path, FindVisitor func);

    long length(int fd);

    long length(LPSZ name);

    int lock(int fd);

    void madvise(long address, long len, int advise);

    int mkdir(Path path, int mode);

    int mkdirs(Path path, int mode);

    long mmap(int fd, long len, long offset, int flags, int memoryTag);

    long mremap(int fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag);

    void msync(long addr, long len, boolean async);

    void munmap(long address, long size, int memoryTag);

    int openAppend(LPSZ name);

    int openCleanRW(LPSZ name, long size);

    int openRO(LPSZ name);

    int openRW(LPSZ name, long opts);

    long read(int fd, long buf, long size, long offset);

    boolean readLink(Path softLink, Path readTo);

    byte readNonNegativeByte(int fd, long offset);

    int readNonNegativeInt(int fd, long offset);

    long readNonNegativeLong(int fd, long offset);

    boolean remove(LPSZ name);

    int rename(LPSZ from, LPSZ to);

    boolean rmdir(Path name);  // Implementation-specific laziness.

    boolean rmdir(Path name, boolean lazy);

    int softLink(LPSZ src, LPSZ softLink);

    int sync();

    boolean touch(LPSZ path);

    boolean truncate(int fd, long size);

    int typeDirOrSoftLinkDirNoDots(Path path, int rootLen, long pUtf8NameZ, int type, MutableUtf8Sink nameSink);

    int unlink(LPSZ softLink);

    boolean unlinkOrRemove(Path path, Log LOG);

    boolean unlinkOrRemove(Path path, int checkedType, Log LOG);

    void walk(Path src, FindVisitor func);

    long write(int fd, long address, long len, long offset);
}
