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
import io.questdb.std.str.Utf8Sequence;

public interface FilesFacade {
    long MAP_FAILED = -1;

    boolean allocate(long fd, long size);

    boolean allocateRange(long fd, long offset, long size);

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

    /**
     * Ordering barrier only, see {@link Files#barrierFsync(long)}. A commit is not durable on this alone;
     * pair it with {@link #fdatasync(long)} at the commit point. The default calls {@link #fdatasync(long)},
     * the stronger barrier.
     */
    default void barrierFsync(long fd) {
        fdatasync(fd);
    }

    void fsync(long fd);

    /**
     * {@link #fsync(long)} semantics (data and metadata) with the device write cache flushed. Not
     * interchangeable with {@link #fdatasync(long)}, which may skip metadata not needed to read the data
     * back. The default calls {@link #fsync(long)}.
     */
    default void fsyncDurable(long fd) {
        fsync(fd);
    }

    void fsyncAndClose(long fd);

    /**
     * Linux sync_file_range(2): starts writeback of the file's page-cache pages over
     * {@code [offset, offset+nbytes)} to the device cache without a device flush (see the
     * {@code Files.SYNC_FILE_RANGE_*} flags). A no-op returning 0 off Linux. Durability still needs a
     * following device flush. Fault-injection facades may override it to model writeback.
     */
    default int syncFileRange(long fd, long offset, long nbytes, int flags) {
        return Files.syncFileRange(fd, offset, nbytes, flags);
    }

    /**
     * Linux syncfs(2): makes the filesystem containing {@code fd} durable in one device flush. It writes back
     * all dirty data and journals every pending metadata change (including ext4 extent conversions), then
     * flushes the device cache. The batched flush uses it in place of one {@link #fdatasync(long)} per column.
     * Off Linux it flushes {@code fd} only (see {@link #isSyncfsFileSystemWide()} and
     * {@link Files#syncfs(long)}). Fault-injection facades override it to model the filesystem-wide flush.
     */
    default void syncfs(long fd) {
        int res = Files.syncfs(fd);
        if (res == 0) {
            return;
        }
        throw CairoException.dataSyncFailureOfFd(this, fd, Os.errno(), "syncfs", "could not syncfs");
    }

    /**
     * Returns whether {@link #syncfs(long)} flushes the entire filesystem containing {@code fd}.
     * Linux provides that guarantee; the macOS and Windows implementations flush only the supplied file.
     */
    default boolean isSyncfsFileSystemWide() {
        return Os.isLinux();
    }

    /**
     * Whether {@link #syncFileRange(long, long, long, int)} is worth issuing against {@code root}.
     * <p>
     * {@code sync_file_range} is a Linux page-cache optimisation: it starts, and optionally waits for,
     * writeback without journalling metadata, so callers must still take a real barrier afterwards. On ZFS
     * the page-cache model does not match (durability is a txg commit), so the call buys nothing and is
     * skipped rather than risking the known mmap interactions.
     *
     * @param root an existing path on the filesystem in question
     * @return {@code true} if a drain pass is likely to help
     */
    default boolean isSyncFileRangeEffective(Utf8Sequence root) {
        return Os.isLinux();
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

    /**
     * The publish-by-rename barrier: the rename is durable when this returns.
     * <p>
     * On POSIX this is {@link #rename(LPSZ, LPSZ)}: the caller makes the new name durable by fsyncing the
     * parent directory afterwards. Windows has no directory handle to fsync, so there the implementation asks
     * the move itself to write through. The default calls {@link #rename(LPSZ, LPSZ)}, so a facade that
     * intercepts {@code rename} intercepts this too.
     */
    default int renameDurable(LPSZ from, LPSZ to) {
        return rename(from, to);
    }

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
