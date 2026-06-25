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

package io.questdb.std;

import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.MutableUtf8Sink;
import io.questdb.std.str.Path;

public interface FilesFacade {
    long MAP_FAILED = -1;

    boolean allocate(long fd, long size);

    boolean allowMixedIO(CharSequence root);

    long append(long fd, long buf, long len);

    boolean close(long fd);

    boolean closeRemove(long fd, LPSZ path);

    int copy(LPSZ from, LPSZ to);

    long copyData(long srcFd, long destFd, long offsetSrc, long length);

    long copyData(long srcFd, long destFd, long offsetSrc, long destOffset, long length);

    int copyRecursive(Path src, Path dst, int dirMode);

    int errno();

    boolean exists(LPSZ path);

    boolean exists(long fd);

    void fadvise(long fd, long offset, long len, int advise);

    long findClose(long findPtr);

    long findFirst(LPSZ path);

    long findName(long findPtr);

    int findNext(long findPtr);

    int findType(long findPtr);

    void fdatasync(long fd);

    void fsync(long fd);

    void fsyncAndClose(long fd);

    /**
     * Linux sync_file_range(2): initiate writeback of the file's page-cache pages over
     * {@code [offset, offset+nbytes)} to the device cache without a device flush (see the
     * {@code Files.SYNC_FILE_RANGE_*} flags). A no-op returning 0 on non-Linux platforms.
     * Durability still requires a following {@link #fdatasync(long)}/{@link #fsync(long)}.
     * <p>
     * Provided as a {@code default} so existing implementors keep compiling; fault-injection
     * facades may override it to model writeback/device-cache semantics.
     */
    default int syncFileRange(long fd, long offset, long nbytes, int flags) {
        return Files.syncFileRange(fd, offset, nbytes, flags);
    }

    /**
     * Linux syncfs(2): make the WHOLE filesystem containing {@code fd} durable in one device flush —
     * writes back all dirty data and journals every pending metadata change (including ext4
     * unwritten-&gt;written extent conversions for every inode), then flushes the device cache. Used by the
     * batched SYNC commit to make all just-drained column extents truly durable with a single flush
     * instead of one {@link #fdatasync(long)} per column. Falls back to an fsync of {@code fd} on
     * non-Linux platforms. See {@link Files#syncfs(long)}.
     * <p>
     * Provided as a {@code default} so existing implementors keep compiling; fault-injection facades
     * (e.g. the crash model) override it to model the whole-filesystem journal-commit + flush.
     */
    default void syncfs(long fd) {
        int res = Files.syncfs(fd);
        if (res == 0) {
            return;
        }
        throw CairoException.critical(Os.errno()).put("could not syncfs [fd=").put(fd).put(']');
    }

    long getDirSize(Path path);

    long getDiskFreeSpace(LPSZ path);

    long getFileLimit();

    int getFileSystemStatus(LPSZ lpszName);

    long getLastModified(LPSZ path);

    long getMapCountLimit();

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

    long length(long fd);

    long length(LPSZ name);

    int lock(long fd);

    void madvise(long address, long len, int advise);

    int mkdir(LPSZ path, int mode);

    int mkdirs(Path path, int mode);

    long mmap(long fd, long len, long offset, int flags, int memoryTag);

    /**
     * Memory map without using the MmapCache. Useful for streaming reads where
     * we want each mapping to be independent and release page cache via madvise.
     */
    long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag);

    long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag);

    /**
     * Remap memory without using the MmapCache. Useful for streaming reads.
     */
    long mremapNoCache(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag);

    void msync(long addr, long len, boolean async);

    void munmap(long address, long size, int memoryTag);

    long openAppend(LPSZ name);

    long openCleanRW(LPSZ name, long size);

    long openRO(LPSZ name);

    long openRONoCache(LPSZ path);

    long openRW(LPSZ name, int opts);

    long openRWNoCache(LPSZ name, int opts);

    long read(long fd, long buf, long size, long offset);

    long readIntAsUnsignedLong(long fd, long offset);

    boolean readLink(Path softLink, Path readTo);

    byte readNonNegativeByte(long fd, long offset);

    int readNonNegativeInt(long fd, long offset);

    long readNonNegativeLong(long fd, long offset);

    void remove(LPSZ name);

    boolean removeQuiet(LPSZ name);

    int rename(LPSZ from, LPSZ to);

    boolean rmdir(Path name);  // Implementation-specific laziness.

    boolean rmdir(Path name, boolean haltOnError);

    int softLink(LPSZ src, LPSZ softLink);

    int sync();

    boolean touch(LPSZ path);

    boolean truncate(long fd, long size);

    int typeDirOrSoftLinkDirNoDots(Path path, int rootLen, long pUtf8NameZ, int type, MutableUtf8Sink nameSink);

    int unlink(LPSZ softLink);

    boolean unlinkOrRemove(Path path, Log LOG);

    boolean unlinkOrRemove(Path path, int checkedType, Log LOG);

    void walk(Path src, FindVisitor func);

    long write(long fd, long address, long len, long offset);
}
