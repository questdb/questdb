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

package io.questdb.std;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.MutableUtf8Sink;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;

/**
 * FilesFacade that routes operations per-table to either {@link MemFdFilesFacade}
 * (for memory tables) or a disk {@link FilesFacade} (for regular tables).
 * <p>
 * Path-based operations check whether the path falls under a registered
 * memory-table directory prefix. Fd-based operations track which fds were
 * opened for memory tables and route accordingly.
 * <p>
 * Directory listings (findFirst/findNext) for the database root merge entries
 * from both facades so that all tables are visible.
 */
public class RoutingFilesFacade implements FilesFacade {

    private static final Log LOG = LogFactory.getLog(RoutingFilesFacade.class);

    private final FilesFacade diskFf;
    private final MemFdFilesFacade memFf;

    // Memory-table directory prefixes (full paths including dbRoot).
    private final ObjHashSet<String> memoryTablePrefixes = new ObjHashSet<>();

    // Fds opened via MemFdFilesFacade, so close/fsync route correctly.
    private final LongHashSet memoryFds = new LongHashSet();

    // Combined find iterators for merged directory listings.
    private final LongObjHashMap<MergedFindState> mergedFinders = new LongObjHashMap<>();
    private long nextMergedFindPtr = Long.MIN_VALUE + 1; // negative range to avoid collisions

    // Find pointers opened via MemFdFilesFacade, so findName/findNext route correctly.
    private final LongHashSet memoryFindPtrs = new LongHashSet();

    // Database root path, for detecting root-level directory listings.
    private String dbRoot;

    public RoutingFilesFacade(FilesFacade diskFf, MemFdFilesFacade memFf) {
        this.diskFf = diskFf;
        this.memFf = memFf;
    }

    public void setDbRoot(CharSequence dbRoot) {
        this.dbRoot = Chars.toString(dbRoot);
    }

    /**
     * Registers a memory-table directory path prefix. All path-based
     * operations whose path starts with this prefix will be routed to
     * MemFdFilesFacade.
     */
    public synchronized void registerMemoryTable(CharSequence dirPath) {
        String p = Chars.toString(dirPath);
        memoryTablePrefixes.add(p);
        LOG.info().$("registered memory table [path=").$(p).I$();
    }

    /**
     * Unregisters a memory-table directory path prefix.
     */
    public synchronized void unregisterMemoryTable(CharSequence dirPath) {
        String p = Chars.toString(dirPath);
        memoryTablePrefixes.remove(p);
        LOG.info().$("unregistered memory table [path=").$(p).I$();
    }

    public synchronized boolean isMemoryTable(CharSequence dirPath) {
        return memoryTablePrefixes.contains(Chars.toString(dirPath));
    }

    // -------------------------------------------------------------------------
    // Routing helpers
    // -------------------------------------------------------------------------

    private synchronized boolean isMemoryPath(LPSZ path) {
        String p = lpszToString(path);
        for (int i = 0, n = memoryTablePrefixes.size(); i < n; i++) {
            String prefix = memoryTablePrefixes.get(i);
            if (p.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private synchronized boolean isMemoryPathStr(String p) {
        for (int i = 0, n = memoryTablePrefixes.size(); i < n; i++) {
            String prefix = memoryTablePrefixes.get(i);
            if (p.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private FilesFacade choose(LPSZ path) {
        return isMemoryPath(path) ? memFf : diskFf;
    }

    private synchronized void trackMemoryFd(long fd) {
        memoryFds.add(fd);
    }

    private synchronized boolean isMemoryFd(long fd) {
        return memoryFds.contains(fd);
    }

    private synchronized void untrackMemoryFd(long fd) {
        memoryFds.remove(fd);
    }

    // -------------------------------------------------------------------------
    // Path-based open operations (route + track fd)
    // -------------------------------------------------------------------------

    @Override
    public long openAppend(LPSZ name) {
        if (isMemoryPath(name)) {
            long fd = memFf.openAppend(name);
            if (fd >= 0) trackMemoryFd(fd);
            return fd;
        }
        return diskFf.openAppend(name);
    }

    @Override
    public long openCleanRW(LPSZ name, long size) {
        if (isMemoryPath(name)) {
            long fd = memFf.openCleanRW(name, size);
            if (fd >= 0) trackMemoryFd(fd);
            return fd;
        }
        return diskFf.openCleanRW(name, size);
    }

    @Override
    public long openRO(LPSZ name) {
        if (isMemoryPath(name)) {
            long fd = memFf.openRO(name);
            if (fd >= 0) trackMemoryFd(fd);
            return fd;
        }
        return diskFf.openRO(name);
    }

    @Override
    public long openRONoCache(LPSZ path) {
        if (isMemoryPath(path)) {
            long fd = memFf.openRONoCache(path);
            if (fd >= 0) trackMemoryFd(fd);
            return fd;
        }
        return diskFf.openRONoCache(path);
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        if (isMemoryPath(name)) {
            long fd = memFf.openRW(name, opts);
            if (fd >= 0) trackMemoryFd(fd);
            return fd;
        }
        return diskFf.openRW(name, opts);
    }

    @Override
    public long openRWNoCache(LPSZ name, int opts) {
        if (isMemoryPath(name)) {
            long fd = memFf.openRWNoCache(name, opts);
            if (fd >= 0) trackMemoryFd(fd);
            return fd;
        }
        return diskFf.openRWNoCache(name, opts);
    }

    // -------------------------------------------------------------------------
    // Fd-based operations that need routing
    // -------------------------------------------------------------------------

    @Override
    public boolean close(long fd) {
        if (isMemoryFd(fd)) {
            untrackMemoryFd(fd);
            return memFf.close(fd);
        }
        return diskFf.close(fd);
    }

    @Override
    public void fsync(long fd) {
        if (isMemoryFd(fd)) {
            memFf.fsync(fd);
        } else {
            diskFf.fsync(fd);
        }
    }

    @Override
    public void fsyncAndClose(long fd) {
        if (isMemoryFd(fd)) {
            untrackMemoryFd(fd);
            memFf.fsyncAndClose(fd);
        } else {
            diskFf.fsyncAndClose(fd);
        }
    }

    @Override
    public int lock(long fd) {
        return isMemoryFd(fd) ? memFf.lock(fd) : diskFf.lock(fd);
    }

    // -------------------------------------------------------------------------
    // Path-based operations (simple routing)
    // -------------------------------------------------------------------------

    @Override
    public boolean allocate(long fd, long size) {
        return Files.allocate(fd, size);
    }

    @Override
    public boolean allowMixedIO(CharSequence root) {
        return diskFf.allowMixedIO(root);
    }

    @Override
    public long append(long fd, long buf, long len) {
        return Files.append(fd, buf, len);
    }

    @Override
    public boolean closeRemove(long fd, LPSZ path) {
        close(fd);
        return removeQuiet(path);
    }

    @Override
    public int copy(LPSZ from, LPSZ to) {
        // Both paths should belong to the same facade.
        return choose(from).copy(from, to);
    }

    @Override
    public long copyData(long srcFd, long destFd, long offsetSrc, long length) {
        return Files.copyData(srcFd, destFd, offsetSrc, length);
    }

    @Override
    public long copyData(long srcFd, long destFd, long offsetSrc, long destOffset, long length) {
        return Files.copyDataToOffset(srcFd, destFd, offsetSrc, destOffset, length);
    }

    @Override
    public int copyRecursive(Path src, Path dst, int dirMode) {
        return choose(src.$()).copyRecursive(src, dst, dirMode);
    }

    @Override
    public int errno() {
        return Os.errno();
    }

    @Override
    public boolean exists(LPSZ path) {
        if (isMemoryPath(path)) {
            return memFf.exists(path);
        }
        return diskFf.exists(path);
    }

    @Override
    public boolean exists(long fd) {
        return Files.exists(fd);
    }

    @Override
    public void fadvise(long fd, long offset, long len, int advise) {
        if (isMemoryFd(fd)) {
            memFf.fadvise(fd, offset, len, advise);
        } else {
            diskFf.fadvise(fd, offset, len, advise);
        }
    }

    // -------------------------------------------------------------------------
    // Find operations — merge disk and memory for dbRoot listings
    // -------------------------------------------------------------------------

    @Override
    public long findFirst(LPSZ path) {
        String p = lpszToString(path);
        // For dbRoot listings, merge results from both facades.
        if (dbRoot != null && normDir(p).equals(normDir(dbRoot))) {
            return mergedFindFirst(path);
        }
        if (isMemoryPathStr(p)) {
            long ptr = memFf.findFirst(path);
            if (ptr > 0) {
                synchronized (this) {
                    memoryFindPtrs.add(ptr);
                }
            }
            return ptr;
        }
        return diskFf.findFirst(path);
    }

    private boolean isMemoryFindPtr(long findPtr) {
        synchronized (this) {
            return memoryFindPtrs.contains(findPtr);
        }
    }

    @Override
    public long findName(long findPtr) {
        synchronized (this) {
            int idx = mergedFinders.keyIndex(findPtr);
            if (idx < 0) {
                return mergedFinders.valueAt(idx).nameBuf;
            }
        }
        if (isMemoryFindPtr(findPtr)) {
            return memFf.findName(findPtr);
        }
        return diskFf.findName(findPtr);
    }

    @Override
    public int findNext(long findPtr) {
        synchronized (this) {
            int idx = mergedFinders.keyIndex(findPtr);
            if (idx < 0) {
                MergedFindState state = mergedFinders.valueAt(idx);
                state.freeBuf();
                state.index++;
                if (state.index >= state.names.size()) {
                    return 0;
                }
                state.loadCurrent();
                return 1;
            }
        }
        if (isMemoryFindPtr(findPtr)) {
            return memFf.findNext(findPtr);
        }
        return diskFf.findNext(findPtr);
    }

    @Override
    public int findType(long findPtr) {
        synchronized (this) {
            int idx = mergedFinders.keyIndex(findPtr);
            if (idx < 0) {
                MergedFindState state = mergedFinders.valueAt(idx);
                return state.types.get(state.index);
            }
        }
        if (isMemoryFindPtr(findPtr)) {
            return memFf.findType(findPtr);
        }
        return diskFf.findType(findPtr);
    }

    @Override
    public long findClose(long findPtr) {
        synchronized (this) {
            int idx = mergedFinders.keyIndex(findPtr);
            if (idx < 0) {
                mergedFinders.valueAt(idx).freeBuf();
                mergedFinders.removeAt(idx);
                return 0;
            }
        }
        if (isMemoryFindPtr(findPtr)) {
            synchronized (this) {
                memoryFindPtrs.remove(findPtr);
            }
            return memFf.findClose(findPtr);
        }
        return diskFf.findClose(findPtr);
    }

    private long mergedFindFirst(LPSZ path) {
        // Collect entries from memFf (memory tables under dbRoot).
        ObjList<String> names = new ObjList<>();
        IntList types = new IntList();
        ObjHashSet<String> seen = new ObjHashSet<>();

        // Memory entries first.
        long mPtr = memFf.findFirst(path);
        if (mPtr > 0) {
            try {
                do {
                    String name = nativeToString(memFf.findName(mPtr));
                    if (name != null && !name.equals(".") && !name.equals("..")) {
                        names.add(name);
                        types.add(memFf.findType(mPtr));
                        seen.add(name);
                    }
                } while (memFf.findNext(mPtr) > 0);
            } finally {
                memFf.findClose(mPtr);
            }
        }

        // Disk entries, skipping duplicates.
        long dPtr = diskFf.findFirst(path);
        if (dPtr > 0) {
            try {
                do {
                    String name = nativeToString(diskFf.findName(dPtr));
                    if (name != null && !seen.contains(name)) {
                        names.add(name);
                        types.add(diskFf.findType(dPtr));
                    }
                } while (diskFf.findNext(dPtr) > 0);
            } finally {
                diskFf.findClose(dPtr);
            }
        }

        if (names.size() == 0) {
            return 0;
        }

        MergedFindState state = new MergedFindState(names, types);
        state.loadCurrent();
        long ptr;
        synchronized (this) {
            ptr = nextMergedFindPtr++;
            mergedFinders.put(ptr, state);
        }
        return ptr;
    }

    // -------------------------------------------------------------------------
    // Remaining path-based operations
    // -------------------------------------------------------------------------

    @Override
    public long getDirSize(Path path) {
        return choose(path.$()).getDirSize(path);
    }

    @Override
    public long getDiskFreeSpace(LPSZ path) {
        return diskFf.getDiskFreeSpace(path);
    }

    @Override
    public long getFileLimit() {
        return diskFf.getFileLimit();
    }

    @Override
    public int getFileSystemStatus(LPSZ lpszName) {
        return diskFf.getFileSystemStatus(lpszName);
    }

    @Override
    public long getLastModified(LPSZ path) {
        return choose(path).getLastModified(path);
    }

    @Override
    public long getMapCountLimit() {
        return diskFf.getMapCountLimit();
    }

    @Override
    public long getMapPageSize() {
        return diskFf.getMapPageSize();
    }

    @Override
    public long getOpenFileCount() {
        return diskFf.getOpenFileCount() + memFf.getOpenFileCount();
    }

    @Override
    public long getPageSize() {
        return diskFf.getPageSize();
    }

    @Override
    public int hardLink(LPSZ src, LPSZ hardLink) {
        return choose(src).hardLink(src, hardLink);
    }

    @Override
    public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
        return choose(src.$()).hardLinkDirRecursive(src, dst, dirMode);
    }

    @Override
    public boolean isCrossDeviceCopyError(int errno) {
        return diskFf.isCrossDeviceCopyError(errno);
    }

    @Override
    public boolean isDirOrSoftLinkDir(LPSZ path) {
        if (isMemoryPath(path)) {
            return memFf.isDirOrSoftLinkDir(path);
        }
        return diskFf.isDirOrSoftLinkDir(path);
    }

    @Override
    public boolean isDirOrSoftLinkDirNoDots(Path path, int rootLen, long pUtf8NameZ, int type) {
        return isDirOrSoftLinkDirNoDots(path, rootLen, pUtf8NameZ, type, null);
    }

    @Override
    public boolean isDirOrSoftLinkDirNoDots(Path path, int rootLen, long pUtf8NameZ, int type, @Nullable MutableUtf8Sink nameSink) {
        if (!Files.notDots(pUtf8NameZ)) {
            return false;
        }
        int savedLen = path.size();
        path.trimTo(rootLen).concat(pUtf8NameZ).$();
        boolean isDir = isDirOrSoftLinkDir(path.$());
        if (isDir && nameSink != null) {
            nameSink.clear();
            nameSink.put(path, rootLen, path.size());
        }
        path.trimTo(savedLen);
        return isDir;
    }

    @Override
    public boolean isRestrictedFileSystem() {
        return diskFf.isRestrictedFileSystem();
    }

    @Override
    public boolean isSoftLink(LPSZ softLink) {
        return choose(softLink).isSoftLink(softLink);
    }

    @Override
    public void iterateDir(LPSZ path, FindVisitor func) {
        long findPtr = findFirst(path);
        if (findPtr != 0) {
            try {
                do {
                    func.onFind(findName(findPtr), findType(findPtr));
                } while (findNext(findPtr) > 0);
            } finally {
                findClose(findPtr);
            }
        }
    }

    @Override
    public long length(long fd) {
        return Files.length(fd);
    }

    @Override
    public long length(LPSZ name) {
        return choose(name).length(name);
    }

    @Override
    public void madvise(long address, long len, int advise) {
        Files.madvise(address, len, advise);
    }

    @Override
    public int mkdir(LPSZ path, int mode) {
        return choose(path).mkdir(path, mode);
    }

    @Override
    public int mkdirs(Path path, int mode) {
        return choose(path.$()).mkdirs(path, mode);
    }

    @Override
    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
        return Files.mmap(fd, len, offset, flags, memoryTag);
    }

    @Override
    public long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag) {
        return Files.mmapNoCache(fd, len, offset, flags, memoryTag);
    }

    @Override
    public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        return Files.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
    }

    @Override
    public long mremapNoCache(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        return Files.mremapNoCache(fd, addr, previousSize, newSize, offset, mode, memoryTag);
    }

    @Override
    public void msync(long addr, long len, boolean async) {
        // Could be from either; msync on memfd is a no-op anyway.
        diskFf.msync(addr, len, async);
    }

    @Override
    public void munmap(long address, long size, int memoryTag) {
        Files.munmap(address, size, memoryTag);
    }

    @Override
    public long read(long fd, long buf, long size, long offset) {
        return Files.read(fd, buf, size, offset);
    }

    @Override
    public long readIntAsUnsignedLong(long fd, long offset) {
        return Files.readIntAsUnsignedLong(fd, offset);
    }

    @Override
    public boolean readLink(Path softLink, Path readTo) {
        return choose(softLink.$()).readLink(softLink, readTo);
    }

    @Override
    public byte readNonNegativeByte(long fd, long offset) {
        return Files.readNonNegativeByte(fd, offset);
    }

    @Override
    public int readNonNegativeInt(long fd, long offset) {
        return Files.readNonNegativeInt(fd, offset);
    }

    @Override
    public long readNonNegativeLong(long fd, long offset) {
        return Files.readNonNegativeLong(fd, offset);
    }

    @Override
    public void remove(LPSZ name) {
        choose(name).remove(name);
    }

    @Override
    public boolean removeQuiet(LPSZ name) {
        return choose(name).removeQuiet(name);
    }

    @Override
    public int rename(LPSZ from, LPSZ to) {
        return choose(from).rename(from, to);
    }

    @Override
    public boolean rmdir(Path name) {
        return choose(name.$()).rmdir(name);
    }

    @Override
    public boolean rmdir(Path name, boolean haltOnError) {
        return choose(name.$()).rmdir(name, haltOnError);
    }

    @Override
    public int softLink(LPSZ src, LPSZ softLink) {
        return choose(src).softLink(src, softLink);
    }

    @Override
    public int sync() {
        return diskFf.sync();
    }

    @Override
    public boolean touch(LPSZ path) {
        return choose(path).touch(path);
    }

    @Override
    public boolean truncate(long fd, long size) {
        return Files.truncate(fd, size);
    }

    @Override
    public int typeDirOrSoftLinkDirNoDots(Path path, int rootLen, long pUtf8NameZ, int type, @Nullable MutableUtf8Sink nameSink) {
        if (!Files.notDots(pUtf8NameZ)) {
            return Files.DT_UNKNOWN;
        }
        int savedLen = path.size();
        path.trimTo(rootLen).concat(pUtf8NameZ).$();
        boolean isDir = isDirOrSoftLinkDir(path.$());
        int result;
        if (isDir) {
            result = Files.DT_DIR;
            if (nameSink != null) {
                nameSink.clear();
                nameSink.put(path, rootLen, path.size());
            }
        } else {
            result = Files.DT_UNKNOWN;
        }
        path.trimTo(savedLen);
        return result;
    }

    @Override
    public int unlink(LPSZ softLink) {
        return choose(softLink).unlink(softLink);
    }

    @Override
    public boolean unlinkOrRemove(Path path, Log LOG) {
        return choose(path.$()).unlinkOrRemove(path, LOG);
    }

    @Override
    public boolean unlinkOrRemove(Path path, int checkedType, Log LOG) {
        return choose(path.$()).unlinkOrRemove(path, checkedType, LOG);
    }

    @Override
    public void walk(Path src, FindVisitor func) {
        choose(src.$()).walk(src, func);
    }

    @Override
    public long write(long fd, long address, long len, long offset) {
        return Files.write(fd, address, len, offset);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private static String normDir(String path) {
        if (path != null && path.length() > 1 && path.charAt(path.length() - 1) == Files.SEPARATOR) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }

    private static String lpszToString(LPSZ lpsz) {
        if (lpsz == null) {
            return null;
        }
        int len = lpsz.size();
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = lpsz.byteAt(i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String nativeToString(long ptr) {
        if (ptr == 0) return null;
        int len = 0;
        while (Unsafe.getUnsafe().getByte(ptr + len) != 0) len++;
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(ptr + i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    // -------------------------------------------------------------------------
    // Merged find state for dbRoot directory listings
    // -------------------------------------------------------------------------

    private static class MergedFindState {
        final ObjList<String> names;
        final IntList types;
        int index;
        long nameBuf = 0;

        MergedFindState(ObjList<String> names, IntList types) {
            this.names = names;
            this.types = types;
            this.index = 0;
        }

        void freeBuf() {
            if (nameBuf != 0) {
                Unsafe.getUnsafe().freeMemory(nameBuf);
                nameBuf = 0;
            }
        }

        void loadCurrent() {
            freeBuf();
            if (index < names.size()) {
                byte[] bytes = names.get(index).getBytes(StandardCharsets.UTF_8);
                nameBuf = Unsafe.getUnsafe().allocateMemory(bytes.length + 1);
                for (int i = 0; i < bytes.length; i++) {
                    Unsafe.getUnsafe().putByte(nameBuf + i, bytes[i]);
                }
                Unsafe.getUnsafe().putByte(nameBuf + bytes.length, (byte) 0);
            }
        }
    }
}
