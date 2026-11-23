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

package io.questdb.test.cairo.fuzz;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.log.Log;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FindVisitor;
import io.questdb.std.LongList;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.MutableUtf8Sink;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class FailureFileFacade implements FilesFacade {
    // Three arming modes, selected by armedThread / queryWorkerNamePrefix:
    //   1. Global (both null): any thread counts and fires (WAL/recovery harness).
    //   2. Single-thread (armedThread != null): only the arming thread (serial
    //      query fuzzer, so a background job cannot trip the fault).
    //   3. Query-execution scope (queryWorkerNamePrefix != null): the arming
    //      thread and the query pool's workers count and fire; any other thread is
    //      ignored and recorded in contaminatingThreadName (parallel query fuzzer:
    //      a FILE fault may fire on a reduce worker, and a background op leaking
    //      into the quiesced query phase is reported, not silently absorbed).
    private volatile Thread armedThread;
    // Mode 3: the arming (test) thread, always admitted alongside the query pool.
    private volatile Thread armingThread;
    // Mode 3: name of an unexpected thread that touched the armed facade, or null.
    private volatile String contaminatingThreadName;
    private final AtomicInteger failureGenerated = new AtomicInteger();
    private final FilesFacade ff;
    private final AtomicInteger osCallsCount = new AtomicInteger(0);
    // The tables whose partition and _txn files the facade follows, for armTableFailure(), by table name
    private final ConcurrentHashMap<String, WatchedTable> watchedTables = new ConcurrentHashMap<>();
    // Mode 3: name prefix of the query pool's worker threads, or null otherwise.
    private volatile String queryWorkerNamePrefix;

    public FailureFileFacade(@NotNull FilesFacade filesFacade) {
        this.ff = filesFacade;
    }

    @Override
    public boolean allocate(long fd, long size) {
        if (checkForFailure() || isTableAllocateFailing(fd)) {
            return false;
        }
        return ff.allocate(fd, size);
    }

    @Override
    public boolean allocate(long fd, long allocatedSize, long size) {
        if (checkForFailure() || isTableAllocateFailing(fd)) {
            return false;
        }
        return ff.allocate(fd, allocatedSize, size);
    }

    /**
     * Arms a one-shot failure of the next operation of {@code kind} on a file of a table that {@link #watchTable}
     * follows, apart from the call-count arming of {@link #setToFailAfter}. When {@code scopeClassName} is not null, the
     * failure fires only on a call stack that runs that class, and not {@code excludedMethodName} of it. A new arming
     * replaces the one the table has, and {@link #clearFailures} leaves it.
     */
    public void armTableFailure(
            String tableName,
            TableFailureKind kind,
            @Nullable String scopeClassName,
            @Nullable String excludedMethodName
    ) {
        final WatchedTable table = watchedTables.get(tableName);
        assert table != null : "the facade does not follow " + tableName;
        table.armed.set(new TableFailure(kind, scopeClassName, excludedMethodName));
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

    // clears the call-count arming of setToFailAfter(); armTableFailure() keeps its arming, see disarmTableFailure()
    public void clearFailures() {
        osCallsCount.set(0);
        failureGenerated.set(0);
        armedThread = null;
        armingThread = null;
        queryWorkerNamePrefix = null;
        contaminatingThreadName = null;
    }

    @Override
    public boolean close(long fd) {
        if (!watchedTables.isEmpty()) {
            watchedTables.forEach((name, table) -> table.forget(fd));
        }
        return ff.close(fd);
    }

    // disarms the failure that armTableFailure() armed on the table, when it did not fire
    public void disarmTableFailure(String tableName) {
        final WatchedTable table = watchedTables.get(tableName);
        if (table != null) {
            table.armed.set(null);
        }
    }

    @Override
    public boolean closeRemove(long fd, LPSZ path) {
        return ff.closeRemove(fd, path);
    }

    /**
     * In query-execution scope (mode 3), the name of an unexpected thread that
     * touched the armed facade -- i.e. a background job that leaked into the
     * quiesced query phase -- or null if none did. The parallel query fuzzer
     * asserts this stays null after a FILE fault.
     */
    public String contaminatingThreadName() {
        return contaminatingThreadName;
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

    // the failures that armTableFailure() armed on the table and that fired
    public int getTableFailuresFired(String tableName) {
        final WatchedTable table = watchedTables.get(tableName);
        return table != null ? table.firedCount.get() : 0;
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

    /**
     * Forwarded like every other syscall. Without this the default implementation runs against the wrapped
     * facade's fd but bypasses this wrapper, so on Darwin, where barrierFsync is a distinct syscall rather
     * than a routed fdatasync, the fault injection never sees a WAL commit barrier.
     */
    @Override
    public void barrierFsync(long fd) {
        try {
            ff.barrierFsync(fd);
        } catch (CairoException e) {
            throw raiseAsOwnFlushFailure(e);
        }
    }

    @Override
    public void fdatasync(long fd) {
        try {
            ff.fdatasync(fd);
        } catch (CairoException e) {
            throw raiseAsOwnFlushFailure(e);
        }
    }

    @Override
    public void syncfs(long fd) {
        try {
            ff.syncfs(fd);
        } catch (CairoException e) {
            throw raiseAsOwnFlushFailure(e);
        }
    }

    @Override
    public void fsync(long fd) {
        try {
            ff.fsync(fd);
        } catch (CairoException e) {
            throw raiseAsOwnFlushFailure(e);
        }
    }

    @Override
    public void fsyncAndClose(long fd) {
        try {
            ff.fsyncAndClose(fd);
        } catch (CairoException e) {
            throw raiseAsOwnFlushFailure(e);
        }
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

    /**
     * Whether a failure planted by {@link #setToFailAfter} has not fired yet.
     */
    public boolean isArmed() {
        return osCallsCount.get() > 0;
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
        if (!watchedTables.isEmpty()) {
            for (WatchedTable table : watchedTables.values()) {
                // while a TXN_GROWTH failure is armed, the _txn file reads as empty to the extension of its mapping,
                // which then allocates disk space and fails
                final TableFailure failure = table.armed.get();
                if (failure != null && failure.kind == TableFailureKind.TXN_GROWTH && table.isTxnFd(fd) && isTxnMappingExtending() && failure.isInScope()) {
                    return 0;
                }
            }
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
        try {
            ff.msync(addr, len, async);
        } catch (CairoException e) {
            throw raiseAsOwnFlushFailure(e);
        }
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
        if (watchedTables.isEmpty()) {
            return ff.openRW(name, opts);
        }
        final WatchedTable table = watchedTableOf(name);
        if (table == null) {
            return ff.openRW(name, opts);
        }
        final boolean isPartitionFile = isPartitionFile(name);
        final TableFailure failure = table.armed.get();
        if (failure != null && failure.kind == TableFailureKind.PARTITION_OPEN && isPartitionFile && failure.isInScope() && table.fire(failure)) {
            return -1;
        }
        final long fd = ff.openRW(name, opts);
        if (fd > -1) {
            if (isPartitionFile) {
                table.followPartitionFd(fd);
            } else if (Utf8s.endsWithAscii(name, Files.SEPARATOR + TableUtils.TXN_FILE_NAME)) {
                table.followTxnFd(fd);
            }
        }
        return fd;
    }

    @Override
    public long openRWExisting(LPSZ name) {
        if (checkForFailure()) {
            return -1;
        }
        return ff.openRWExisting(name);
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

    /**
     * Makes the facade follow the partition and the _txn files of a table, so that {@link #armTableFailure} can fail an
     * operation on one that opened before the arming. Call before the table's files open.
     */
    public void watchTable(String tableName) {
        watchedTables.putIfAbsent(tableName, new WatchedTable(tableName));
    }

    public void setToFailAfter(int ioFailureCallCount) {
        // Default (unscoped): any thread's op counts and fires. Arm only when not
        // already armed so a pending fault is not reset by a concurrent caller.
        armedThread = null;
        int osCalls;
        while (true) {
            osCalls = osCallsCount.get();
            if (osCalls > 0 || osCallsCount.compareAndSet(osCalls, ioFailureCallCount)) {
                return;
            }
        }
    }

    /**
     * Arms the fault scoped to the calling thread: only file ops issued by this
     * thread count down and fire. See {@link #armedThread} for why the query
     * fuzzer needs this over the unscoped {@link #setToFailAfter(int)}.
     */
    public void setToFailAfterOnCurrentThread(int ioFailureCallCount) {
        // Only the arming thread counts down and fires (see checkForFailure), so
        // the same thread both writes and reads osCallsCount and no CAS guard is
        // needed against concurrent decrements.
        osCallsCount.set(ioFailureCallCount);
        armedThread = Thread.currentThread();
    }

    /**
     * Arms the fault scoped to the query execution (mode 3): ops from the arming
     * thread and from query-pool workers (name starts with {@code queryWorkerNamePrefix})
     * count down and fire; any other thread is ignored and recorded in
     * {@link #contaminatingThreadName()}.
     */
    public void setToFailAfterOnQueryThreads(int ioFailureCallCount, String queryWorkerNamePrefix) {
        armedThread = null;
        armingThread = Thread.currentThread();
        contaminatingThreadName = null;
        this.queryWorkerNamePrefix = queryWorkerNamePrefix;
        osCallsCount.set(ioFailureCallCount);
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
        final Thread current = Thread.currentThread();
        // Mode 2: single-thread scope. Ignore ops on every other thread so the
        // fault stays on the query under test and off engine background jobs.
        final Thread armed = armedThread;
        if (armed != null && current != armed) {
            return false;
        }
        // Mode 3: query-execution scope. Admit the arming thread and the query
        // pool's workers; ignore any other thread and record it as contamination
        // so it neither consumes a count nor fires. Mode 1 (both null) admits all.
        final String prefix = queryWorkerNamePrefix;
        if (prefix != null && current != armingThread && !current.getName().startsWith(prefix)) {
            contaminatingThreadName = current.getName();
            return false;
        }
        boolean fail = osCallsCount.decrementAndGet() == 0;
        if (fail) {
            failureGenerated.incrementAndGet();
        }
        return fail;
    }

    // whether a file is in a partition directory, whose name starts with the partition timestamp
    private static boolean isPartitionFile(LPSZ name) {
        final int fileStart = Utf8s.lastIndexOfAscii(name, Files.SEPARATOR);
        if (fileStart < 1) {
            return false;
        }
        int dirStart = fileStart - 1;
        while (dirStart > -1 && name.byteAt(dirStart) != Files.SEPARATOR) {
            dirStart--;
        }
        return dirStart > -1 && dirStart + 1 < fileStart && Character.isDigit((char) name.byteAt(dirStart + 1));
    }

    // whether a commit extends the mapping of a _txn file, rather than a writer that opens the file and maps it
    private static boolean isTxnMappingExtending() {
        return StackWalker.getInstance().walk(
                frames -> frames.anyMatch(frame -> "io.questdb.cairo.vm.MemoryCMARWImpl".equals(frame.getClassName()) && "extend0".equals(frame.getMethodName()))
        );
    }

    private boolean isTableAllocateFailing(long fd) {
        if (watchedTables.isEmpty()) {
            return false;
        }
        for (WatchedTable table : watchedTables.values()) {
            final TableFailure failure = table.armed.get();
            if (failure != null) {
                final boolean isTargetFd = switch (failure.kind) {
                    case PAGE_ALLOCATE -> table.isPartitionFd(fd);
                    case TXN_GROWTH -> table.isTxnFd(fd) && isTxnMappingExtending();
                    default -> false;
                };
                if (isTargetFd && failure.isInScope() && table.fire(failure)) {
                    return true;
                }
            }
        }
        return false;
    }

    // the table that the facade follows whose directory holds the file, or null
    private WatchedTable watchedTableOf(LPSZ name) {
        for (WatchedTable table : watchedTables.values()) {
            if (Utf8s.containsAscii(name, table.dirPrefix)) {
                return table;
            }
        }
        return null;
    }

    /**
     * The wrapped facade records a failed flush against itself, so it would not reach the engine configured with
     * this wrapper. Raise it again through the factory against this facade.
     */
    private CairoException raiseAsOwnFlushFailure(CairoException e) {
        if (!e.isDataSyncFailure()) {
            return e;
        }
        return CairoException.dataSyncFailure(this, e.getErrno(), e.getDataSyncOperation(), e.getFlyweightMessage());
    }

    // the operation that armTableFailure() fails
    public enum TableFailureKind {
        // the next allocate() of a file in a partition directory of the table, as when a column file grows by a page on
        // a full disk
        PAGE_ALLOCATE,
        // the next open of a file in a partition directory of the table, as when the table starts a partition on a full
        // disk
        PARTITION_OPEN,
        // the next growth of the _txn file of the table, at a commit
        TXN_GROWTH
    }

    private static final class TableFailure {
        private final String excludedMethodName;
        private final TableFailureKind kind;
        private final String scopeClassName;

        private TableFailure(TableFailureKind kind, @Nullable String scopeClassName, @Nullable String excludedMethodName) {
            this.kind = kind;
            this.scopeClassName = scopeClassName;
            this.excludedMethodName = excludedMethodName;
        }

        private boolean isInScope() {
            if (scopeClassName == null) {
                return true;
            }
            return StackWalker.getInstance().walk(frames -> {
                final boolean[] isInClass = {false};
                final boolean[] isExcluded = {false};
                frames.forEach(frame -> {
                    if (scopeClassName.equals(frame.getClassName())) {
                        isInClass[0] = true;
                        if (frame.getMethodName().equals(excludedMethodName)) {
                            isExcluded[0] = true;
                        }
                    }
                });
                return isInClass[0] && !isExcluded[0];
            });
        }
    }

    // A table the facade follows: the fds of its partition and _txn files, and the failure armed on it
    private static final class WatchedTable {
        private final AtomicReference<TableFailure> armed = new AtomicReference<>();
        // the separator and the directory name prefix of the table
        private final String dirPrefix;
        private final AtomicInteger firedCount = new AtomicInteger();
        private final LongList partitionFds = new LongList();
        private final LongList txnFds = new LongList();

        private WatchedTable(String tableName) {
            this.dirPrefix = Files.SEPARATOR + tableName + TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
        }

        private boolean fire(TableFailure failure) {
            if (armed.compareAndSet(failure, null)) {
                firedCount.incrementAndGet();
                return true;
            }
            return false;
        }

        private synchronized void followPartitionFd(long fd) {
            partitionFds.add(fd);
        }

        private synchronized void followTxnFd(long fd) {
            txnFds.add(fd);
        }

        private synchronized void forget(long fd) {
            int index = partitionFds.indexOf(fd);
            if (index > -1) {
                partitionFds.removeIndex(index);
            }
            index = txnFds.indexOf(fd);
            if (index > -1) {
                txnFds.removeIndex(index);
            }
        }

        private synchronized boolean isPartitionFd(long fd) {
            return partitionFds.indexOf(fd) > -1;
        }

        private synchronized boolean isTxnFd(long fd) {
            return txnFds.indexOf(fd) > -1;
        }
    }
}
