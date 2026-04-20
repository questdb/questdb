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

import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.MutableUtf8Sink;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FilesFacade backed entirely by anonymous memory-mapped file descriptors
 * (memfd_create on Linux). No files are created on the real filesystem.
 * <p>
 * Each logical path maps to a primary memfd kept alive in an internal registry.
 * Every open() call duplicates the primary fd so callers hold independent
 * descriptors with independent lifetimes; kernel reclaims the backing memory
 * only when all duplicates and the primary are closed.
 * <p>
 * All registry operations are synchronized on this instance. This is adequate
 * for single-engine embedded use.
 */
public class MemFdFilesFacade implements FilesFacade {

    public static final MemFdFilesFacade INSTANCE = new MemFdFilesFacade();

    // Sentinel stored in pathRegistry for virtual directories (no memfd).
    private static final long DIRECTORY_MARKER = Long.MIN_VALUE;
    private static final Log LOG = LogFactory.getLog(MemFdFilesFacade.class);

    // Capacity limit in bytes (-1 = unlimited). Acts as a virtual "disk size"
    // bounding the total RSS consumed by memfd-backed files.
    private final long capacityBytes;

    // Running total of bytes allocated across all primary memfds.
    private final AtomicLong usedBytes = new AtomicLong(0);

    // Logical path → primary raw OS fd (or DIRECTORY_MARKER for directories).
    // Raw OS fds are NOT wrapped in Files.createUniqueFd() and are therefore
    // invisible to Files.getOpenCachedFileCount(). This mirrors the real
    // filesystem where files on disk do not count as open file descriptors.
    // noEntryValue for CharSequenceLongHashMap is -1, which never collides with
    // DIRECTORY_MARKER (Long.MIN_VALUE) or valid OS fds (always > 0).
    private final CharSequenceLongHashMap pathRegistry = new CharSequenceLongHashMap();

    // Child unique fd → logical path, populated by every open() call.
    private final LongObjHashMap<String> childFdToPath = new LongObjHashMap<>();

    // Active directory iterators keyed by an auto-incrementing find pointer.
    private final LongObjHashMap<FindState> activeFinders = new LongObjHashMap<>();

    private long nextFindPtr = 1;

    public MemFdFilesFacade() {
        this(-1);
    }

    public MemFdFilesFacade(long capacityBytes) {
        this.capacityBytes = capacityBytes;
    }

    public long getCapacityBytes() {
        return capacityBytes;
    }

    public long getUsedBytes() {
        return usedBytes.get();
    }

    /**
     * Closes all primary fds and clears all internal state. After this call
     * the facade behaves as if freshly constructed. Intended for use between
     * test methods to prevent state leaking across tests.
     */
    public synchronized void clear() {
        ObjList<CharSequence> keys = pathRegistry.keys();
        long creditBytes = 0;
        for (int i = 0, n = keys.size(); i < n; i++) {
            long val = pathRegistry.get(keys.get(i));
            if (val != DIRECTORY_MARKER) {
                long sz = Files.length((int) val);
                if (sz > 0) {
                    creditBytes += sz;
                }
                Files.close0((int) val);
            }
        }
        if (creditBytes > 0) {
            Unsafe.recordMemAlloc(-creditBytes, MemoryTag.NATIVE_MEMFD_STORAGE);
        }
        pathRegistry.clear();
        childFdToPath.clear();
        activeFinders.clear();
        usedBytes.set(0);
        nextFindPtr = 1;
    }

    // -------------------------------------------------------------------------
    // File open / create
    // -------------------------------------------------------------------------

    @Override
    public long openAppend(LPSZ name) {
        return openOrCreate(name, true);
    }

    @Override
    public long openCleanRW(LPSZ name, long size) {
        String path = lpszToString(name);
        synchronized (this) {
            // Remove any existing entry so we start fresh.
            closePrimary(path);
            if (size > 0 && capacityBytes > 0 && usedBytes.get() + size > capacityBytes) {
                return -1;
            }
            // Charge the requested backing storage against the global RSS limit
            // before touching the kernel. Throws cleanly with an OOM CairoException
            // if the limit would be exceeded, leaving no state behind.
            if (size > 0) {
                Unsafe.chargeExternalRss(size, MemoryTag.NATIVE_MEMFD_STORAGE);
            }
            registerAncestors(path);
            int osFd = Files.memfdCreate(name.ptr());
            if (osFd < 0) {
                LOG.error().$("memfdCreate failed [path=").$(name).$(", errno=").$(Os.errno()).I$();
                if (size > 0) {
                    Unsafe.recordMemAlloc(-size, MemoryTag.NATIVE_MEMFD_STORAGE);
                }
                return -1;
            }
            pathRegistry.put(path, (long) osFd);
            if (size > 0) {
                if (!Files.truncate(osFd, size)) {
                    closePrimary(path);
                    return -1;
                }
                if (capacityBytes > 0) {
                    usedBytes.addAndGet(size);
                }
            }
            return dupChild(path, osFd);
        }
    }

    @Override
    public long openRO(LPSZ name) {
        String path = lpszToString(name);
        synchronized (this) {
            int idx = pathRegistry.keyIndex(path);
            if (idx < 0 && pathRegistry.valueAt(idx) == DIRECTORY_MARKER) {
                // On a real filesystem, open(dir, O_RDONLY) succeeds and returns
                // an fd usable for fsync. Create a dummy memfd for the same purpose.
                int osFd = Files.memfdCreate(name.ptr());
                if (osFd < 0) {
                    return -1;
                }
                return Files.createUniqueFd(osFd);
            }
        }
        return openOrCreate(name, false);
    }

    @Override
    public long openRONoCache(LPSZ path) {
        return openRO(path);
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        return openOrCreate(name, true);
    }

    @Override
    public long openRWNoCache(LPSZ name, int opts) {
        return openRW(name, opts);
    }

    // -------------------------------------------------------------------------
    // Close / remove
    // -------------------------------------------------------------------------

    @Override
    public boolean allocate(long fd, long size) {
        long currentSize = Files.length(fd);
        if (currentSize < 0) {
            currentSize = 0;
        }
        long delta = size - currentSize;
        if (delta > 0 && capacityBytes > 0 && usedBytes.get() + delta > capacityBytes) {
            return false;
        }
        if (delta > 0) {
            Unsafe.chargeExternalRss(delta, MemoryTag.NATIVE_MEMFD_STORAGE);
        }
        boolean result = Files.allocate(fd, size);
        if (!result) {
            if (delta > 0) {
                Unsafe.recordMemAlloc(-delta, MemoryTag.NATIVE_MEMFD_STORAGE);
            }
            return false;
        }
        if (delta < 0) {
            Unsafe.recordMemAlloc(delta, MemoryTag.NATIVE_MEMFD_STORAGE);
        }
        if (capacityBytes > 0 && delta != 0) {
            usedBytes.addAndGet(delta);
        }
        return true;
    }

    @Override
    public boolean allowMixedIO(CharSequence root) {
        return true;
    }

    @Override
    public long append(long fd, long buf, long len) {
        return Files.append(fd, buf, len);
    }

    @Override
    public boolean close(long fd) {
        int res = Files.close(fd);
        if (res == 0) {
            synchronized (this) {
                childFdToPath.remove(fd);
            }
        }
        return res == 0;
    }

    @Override
    public boolean closeRemove(long fd, LPSZ path) {
        close(fd);
        return removeQuiet(path);
    }

    // -------------------------------------------------------------------------
    // Copy / data transfer
    // -------------------------------------------------------------------------

    @Override
    public int copy(LPSZ from, LPSZ to) {
        // Copy via data transfer between the two memfds if both exist.
        long srcFd = openRO(from);
        if (srcFd < 0) {
            return -1;
        }
        long dstFd = openRW(to, 0);
        if (dstFd < 0) {
            close(srcFd);
            return -1;
        }
        long len = length(srcFd);
        truncate(dstFd, len);
        long copied = Files.copyData(srcFd, dstFd, 0, len);
        close(srcFd);
        close(dstFd);
        return copied == len ? 0 : -1;
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
        return -1;
    }

    // -------------------------------------------------------------------------
    // errno / misc
    // -------------------------------------------------------------------------

    @Override
    public int errno() {
        return Os.errno();
    }

    // -------------------------------------------------------------------------
    // exists
    // -------------------------------------------------------------------------

    @Override
    public boolean exists(LPSZ path) {
        if (path == null) {
            return false;
        }
        String p = lpszToString(path);
        synchronized (this) {
            return pathRegistry.keyIndex(p) < 0;
        }
    }

    @Override
    public boolean exists(long fd) {
        return Files.exists(fd);
    }

    // -------------------------------------------------------------------------
    // fadvise / find / fsync
    // -------------------------------------------------------------------------

    @Override
    public void fadvise(long fd, long offset, long len, int advise) {
        // memfd does not benefit from fadvise; treat as no-op.
    }

    @Override
    public long findClose(long findPtr) {
        if (findPtr == 0) {
            return 0;
        }
        synchronized (this) {
            int idx = activeFinders.keyIndex(findPtr);
            if (idx < 0) {
                activeFinders.valueAt(idx).freeBuf();
                activeFinders.removeAt(idx);
            }
        }
        return 0;
    }

    @Override
    public long findFirst(LPSZ path) {
        String dirPath = normDir(lpszToString(path));
        String prefix = dirPath + Files.SEPARATOR;
        ObjList<String> names = new ObjList<>();
        IntList types = new IntList();

        synchronized (this) {
            boolean dirExists = pathRegistry.keyIndex(dirPath) < 0;

            ObjList<CharSequence> allKeys = pathRegistry.keys();
            for (int i = 0, n = allKeys.size(); i < n; i++) {
                String key = allKeys.get(i).toString();
                if (!key.startsWith(prefix)) {
                    continue;
                }
                String suffix = key.substring(prefix.length());
                if (suffix.isEmpty() || suffix.indexOf(Files.SEPARATOR) >= 0) {
                    continue;
                }
                long val = pathRegistry.get(key);
                names.add(suffix);
                types.add(val == DIRECTORY_MARKER ? Files.DT_DIR : Files.DT_FILE);
            }

            if (!dirExists && names.size() == 0) {
                return 0;
            }

            // For empty existing directories, add a single "." entry so that
            // findFirst returns a valid (non-zero) ptr.  Callers that iterate
            // with a do-while pattern rely on findFirst succeeding for any
            // directory that exists.  The "." is filtered by notDots() /
            // typeDirOrSoftLinkDirNoDots() before it reaches application logic.
            if (names.size() == 0) {
                names.add(".");
                types.add(Files.DT_DIR);
            }

            FindState state = new FindState(names, types);
            state.loadCurrent();
            long ptr = nextFindPtr++;
            activeFinders.put(ptr, state);
            return ptr;
        }
    }

    @Override
    public long findName(long findPtr) {
        synchronized (this) {
            int idx = activeFinders.keyIndex(findPtr);
            if (idx >= 0) {
                return 0;
            }
            return activeFinders.valueAt(idx).nameBuf;
        }
    }

    @Override
    public int findNext(long findPtr) {
        synchronized (this) {
            int idx = activeFinders.keyIndex(findPtr);
            if (idx >= 0) {
                return -1;
            }
            FindState state = activeFinders.valueAt(idx);
            state.freeBuf();
            state.index++;
            if (state.index >= state.names.size()) {
                return 0;
            }
            state.loadCurrent();
            return 1;
        }
    }

    @Override
    public int findType(long findPtr) {
        synchronized (this) {
            int idx = activeFinders.keyIndex(findPtr);
            if (idx >= 0) {
                return Files.DT_UNKNOWN;
            }
            FindState state = activeFinders.valueAt(idx);
            return state.types.get(state.index);
        }
    }

    @Override
    public void fsync(long fd) {
        // memfd is RAM-backed; fsync is a no-op.
    }

    @Override
    public void fsyncAndClose(long fd) {
        close(fd);
    }

    // -------------------------------------------------------------------------
    // getDirSize / getDiskFreeSpace / getFileLimit / getFileSystemStatus
    // -------------------------------------------------------------------------

    @Override
    public long getDirSize(Path path) {
        String prefix = normDir(lpszToString(path.$()));
        long total = 0;
        synchronized (this) {
            ObjList<CharSequence> keys = pathRegistry.keys();
            for (int i = 0, n = keys.size(); i < n; i++) {
                String key = keys.get(i).toString();
                long val = pathRegistry.get(key);
                if (key.startsWith(prefix) && val != DIRECTORY_MARKER) {
                    total += Files.length((int) val);
                }
            }
        }
        return total;
    }

    @Override
    public long getDiskFreeSpace(LPSZ path) {
        if (capacityBytes > 0) {
            return Math.max(0, capacityBytes - usedBytes.get());
        }
        return Long.MAX_VALUE;
    }

    @Override
    public long getFileLimit() {
        return Files.getFileLimit();
    }

    @Override
    public int getFileSystemStatus(LPSZ lpszName) {
        return 0;
    }

    @Override
    public long getLastModified(LPSZ path) {
        return 0;
    }

    @Override
    public long getMapCountLimit() {
        return Files.getMapCountLimit();
    }

    @Override
    public long getMapPageSize() {
        long pageSize = Files.PAGE_SIZE;
        long mapPageSize = pageSize * pageSize;
        if (mapPageSize < pageSize || mapPageSize > FilesFacadeImpl._16M) {
            if (FilesFacadeImpl._16M % pageSize == 0) {
                return FilesFacadeImpl._16M;
            }
            return pageSize;
        }
        return mapPageSize;
    }

    @Override
    public long getOpenFileCount() {
        synchronized (this) {
            return childFdToPath.size();
        }
    }

    @Override
    public long getPageSize() {
        return Files.PAGE_SIZE;
    }

    // -------------------------------------------------------------------------
    // hardLink / softLink / etc.  (unsupported for in-memory FS)
    // -------------------------------------------------------------------------

    @Override
    public int hardLink(LPSZ src, LPSZ hardLink) {
        String srcPath = lpszToString(src);
        String dstPath = lpszToString(hardLink);
        synchronized (this) {
            int srcIdx = pathRegistry.keyIndex(srcPath);
            if (srcIdx >= 0) {
                return -1; // source does not exist
            }
            long srcOsFd = pathRegistry.valueAt(srcIdx);
            if (srcOsFd == DIRECTORY_MARKER) {
                return -1;
            }
            // Dup the source fd so both paths share the same backing memory.
            int dupOsFd = Files.dup((int) srcOsFd);
            if (dupOsFd < 0) {
                return -1;
            }
            registerAncestors(dstPath);
            int dstIdx = pathRegistry.keyIndex(dstPath);
            pathRegistry.putAt(dstIdx, dstPath, (long) dupOsFd);
            return 0;
        }
    }

    @Override
    public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
        return -1;
    }

    @Override
    public boolean isCrossDeviceCopyError(int errno) {
        return false;
    }

    @Override
    public boolean isDirOrSoftLinkDir(LPSZ path) {
        String p = normDir(lpszToString(path));
        synchronized (this) {
            int idx = pathRegistry.keyIndex(p);
            return idx < 0 && pathRegistry.valueAt(idx) == DIRECTORY_MARKER;
        }
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
        // Return true to skip directory-fd fsync operations. The engine opens
        // directories read-only to fsync them, but memfds have no directory fds.
        return true;
    }

    @Override
    public boolean isSoftLink(LPSZ softLink) {
        return false;
    }

    @Override
    public void iterateDir(LPSZ path, FindVisitor func) {
        long findPtr = findFirst(path);
        if (findPtr > 0) {
            try {
                do {
                    func.onFind(findName(findPtr), findType(findPtr));
                } while (findNext(findPtr) > 0);
            } finally {
                findClose(findPtr);
            }
        }
    }

    // -------------------------------------------------------------------------
    // length / lock
    // -------------------------------------------------------------------------

    @Override
    public long length(long fd) {
        long r = Files.length(fd);
        if (r < 0) {
            throw CairoException.critical(Os.errno()).put("Checking file size failed");
        }
        return r;
    }

    @Override
    public long length(LPSZ name) {
        String p = lpszToString(name);
        synchronized (this) {
            int idx = pathRegistry.keyIndex(p);
            if (idx >= 0) {
                return -1;
            }
            long primaryOsFd = pathRegistry.valueAt(idx);
            if (primaryOsFd == DIRECTORY_MARKER) {
                return -1;
            }
            return Files.length((int) primaryOsFd);
        }
    }

    @Override
    public int lock(long fd) {
        return 0;
    }

    // -------------------------------------------------------------------------
    // madvise / mkdir / mmap family
    // -------------------------------------------------------------------------

    @Override
    public void madvise(long address, long len, int advise) {
        if (address != 0 && advise > -1) {
            Files.madvise(address, len, advise);
        }
    }

    @Override
    public int mkdir(LPSZ path, int mode) {
        String p = normDir(lpszToString(path));
        synchronized (this) {
            int idx = pathRegistry.keyIndex(p);
            if (idx < 0) {
                return 0; // already exists
            }
            pathRegistry.putAt(idx, p, DIRECTORY_MARKER);
        }
        return 0;
    }

    @Override
    public int mkdirs(Path path, int mode) {
        // Use lpszToString to get the correct Java string from the native
        // UTF-8 bytes. Then iterate over the string's characters (not bytes).
        String fullPath = lpszToString(path.$());
        for (int i = 1; i < fullPath.length(); i++) {
            if (fullPath.charAt(i) == Files.SEPARATOR) {
                String component = normDir(fullPath.substring(0, i));
                synchronized (this) {
                    int idx = pathRegistry.keyIndex(component);
                    if (idx >= 0) {
                        pathRegistry.putAt(idx, component, DIRECTORY_MARKER);
                    }
                }
            }
        }
        // Register the full path itself.
        String full = normDir(fullPath);
        synchronized (this) {
            int idx = pathRegistry.keyIndex(full);
            if (idx >= 0) {
                pathRegistry.putAt(idx, full, DIRECTORY_MARKER);
            }
        }
        return 0;
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
        // memfd is RAM-backed; msync is a no-op.
    }

    @Override
    public void munmap(long address, long size, int memoryTag) {
        Files.munmap(address, size, memoryTag);
    }

    // -------------------------------------------------------------------------
    // read
    // -------------------------------------------------------------------------

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
        return false;
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

    // -------------------------------------------------------------------------
    // remove / rename / rmdir
    // -------------------------------------------------------------------------

    @Override
    public void remove(LPSZ name) {
        if (!removeQuiet(name)) {
            throw CairoException.critical(errno()).put("could not remove [file=").put(name).put(']');
        }
    }

    @Override
    public boolean removeQuiet(LPSZ name) {
        String p = lpszToString(name);
        synchronized (this) {
            closePrimary(p);
            // Always return true: removing a non-existent file is not an error,
            // matching FilesFacadeImpl behavior (ENOENT is treated as success).
            return true;
        }
    }

    @Override
    public int rename(LPSZ from, LPSZ to) {
        String fromStr = lpszToString(from);
        String toStr = lpszToString(to);
        synchronized (this) {
            int fromIdx = pathRegistry.keyIndex(fromStr);
            if (fromIdx >= 0) {
                return -1; // source does not exist
            }

            // Collect all entries to rename: the entry itself plus all children
            // (e.g., renaming a directory renames all files under it).
            String fromPrefix = fromStr + Files.SEPARATOR;
            ObjList<CharSequence> allKeys = pathRegistry.keys();
            ObjList<String> keysToRename = new ObjList<>();
            keysToRename.add(fromStr);
            for (int i = 0, n = allKeys.size(); i < n; i++) {
                String key = allKeys.get(i).toString();
                if (key.startsWith(fromPrefix)) {
                    keysToRename.add(key);
                }
            }

            for (int i = 0, n = keysToRename.size(); i < n; i++) {
                String oldKey = keysToRename.get(i);
                String newKey = oldKey.equals(fromStr)
                        ? toStr
                        : toStr + oldKey.substring(fromStr.length());
                int idx = pathRegistry.keyIndex(oldKey);
                if (idx < 0) {
                    long fd = pathRegistry.valueAt(idx);
                    pathRegistry.removeAt(idx);
                    pathRegistry.put(newKey, fd);
                }
            }

            // Update childFdToPath entries that referred to renamed paths.
            LongList childKeysToUpdate = new LongList();
            ObjList<String> childNewValues = new ObjList<>();
            childFdToPath.forEach((k, v) -> {
                if (v.equals(fromStr) || v.startsWith(fromPrefix)) {
                    childKeysToUpdate.add(k);
                    childNewValues.add(v.equals(fromStr) ? toStr : toStr + v.substring(fromStr.length()));
                }
            });
            for (int i = 0, n = childKeysToUpdate.size(); i < n; i++) {
                childFdToPath.put(childKeysToUpdate.get(i), childNewValues.get(i));
            }
        }
        return 0;
    }

    @Override
    public boolean rmdir(Path name) {
        return rmdir(name, true);
    }

    @Override
    public boolean rmdir(Path name, boolean haltOnError) {
        String prefix = normDir(lpszToString(name.$()));
        synchronized (this) {
            ObjList<CharSequence> keys = pathRegistry.keys();
            ObjList<String> toRemove = new ObjList<>();
            for (int i = 0, n = keys.size(); i < n; i++) {
                String key = keys.get(i).toString();
                if (key.equals(prefix) || key.startsWith(prefix + Files.SEPARATOR)) {
                    toRemove.add(key);
                }
            }
            for (int i = 0, n = toRemove.size(); i < n; i++) {
                closePrimary(toRemove.get(i));
            }
        }
        return true;
    }

    // -------------------------------------------------------------------------
    // softLink / sync / touch / truncate
    // -------------------------------------------------------------------------

    @Override
    public int softLink(LPSZ src, LPSZ softLink) {
        return -1;
    }

    @Override
    public int sync() {
        return 0;
    }

    @Override
    public boolean touch(LPSZ path) {
        long fd = openOrCreate(path, true);
        if (fd < 0) {
            return false;
        }
        close(fd);
        return true;
    }

    @Override
    public boolean truncate(long fd, long size) {
        long currentSize = Files.length(fd);
        if (currentSize < 0) {
            currentSize = 0;
        }
        long delta = size - currentSize;
        if (delta > 0 && capacityBytes > 0 && usedBytes.get() + delta > capacityBytes) {
            return false;
        }
        if (delta > 0) {
            Unsafe.chargeExternalRss(delta, MemoryTag.NATIVE_MEMFD_STORAGE);
        }
        boolean result = Files.truncate(fd, size);
        if (!result) {
            if (delta > 0) {
                Unsafe.recordMemAlloc(-delta, MemoryTag.NATIVE_MEMFD_STORAGE);
            }
            return false;
        }
        if (delta < 0) {
            Unsafe.recordMemAlloc(delta, MemoryTag.NATIVE_MEMFD_STORAGE);
        }
        if (capacityBytes > 0 && delta != 0) {
            usedBytes.addAndGet(delta);
        }
        return true;
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
        return removeQuiet(softLink) ? 0 : -1;
    }

    @Override
    public boolean unlinkOrRemove(Path path, Log LOG) {
        return rmdir(path);
    }

    @Override
    public boolean unlinkOrRemove(Path path, int checkedType, Log LOG) {
        return rmdir(path);
    }

    // -------------------------------------------------------------------------
    // walk / write
    // -------------------------------------------------------------------------

    @Override
    public void walk(Path src, FindVisitor func) {
        walkRecursive(src, func);
    }

    @Override
    public long write(long fd, long address, long len, long offset) {
        return Files.write(fd, address, len, offset);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    // Closes the primary fd registered for the given path and removes it from
    // the registry. Returns true if an entry was found and removed.
    private boolean closePrimary(String path) {
        int idx = pathRegistry.keyIndex(path);
        if (idx >= 0) {
            return false; // not found
        }
        long primaryOsFd = pathRegistry.valueAt(idx);
        pathRegistry.removeAt(idx);
        if (primaryOsFd != DIRECTORY_MARKER) {
            long fileSize = Files.length((int) primaryOsFd);
            if (fileSize > 0) {
                Unsafe.recordMemAlloc(-fileSize, MemoryTag.NATIVE_MEMFD_STORAGE);
                if (capacityBytes > 0) {
                    usedBytes.addAndGet(-fileSize);
                }
            }
            Files.close0((int) primaryOsFd);
        }
        return true;
    }

    // Duplicates primaryOsFd, wraps it in a unique fd, registers it in
    // childFdToPath, and returns it. Returns -1 on dup failure.
    private long dupChild(String path, int primaryOsFd) {
        int dupOsFd = Files.dup(primaryOsFd);
        if (dupOsFd < 0) {
            LOG.error().$("dup failed [path=").$(path).$(", errno=").$(Os.errno()).I$();
            return -1;
        }
        long childFd = Files.createUniqueFd(dupOsFd);
        childFdToPath.put(childFd, path);
        return childFd;
    }

    // Registers all ancestor directories of the given path as DIRECTORY_MARKER
    // if they are not already present. Must be called under synchronization.
    private void registerAncestors(String path) {
        for (int i = path.length() - 1; i > 0; i--) {
            if (path.charAt(i) == Files.SEPARATOR) {
                String ancestor = path.substring(0, i);
                int idx = pathRegistry.keyIndex(ancestor);
                if (idx >= 0) {
                    pathRegistry.putAt(idx, ancestor, DIRECTORY_MARKER);
                }
            }
        }
    }

    // Strips a trailing path separator from a directory path so that the
    // registry uses consistent keys.
    private static String normDir(String path) {
        if (path != null && path.length() > 1 && path.charAt(path.length() - 1) == Files.SEPARATOR) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }

    // Reads the null-terminated UTF-8 content from an LPSZ native pointer
    // into a Java String. Unlike Utf8s.toString() which delegates to
    // Object.toString() (broken for PathLPSZ), this reads the actual bytes.
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

    // Opens an existing path (create=false) or creates it if absent (create=true).
    // Returns a child unique fd on success, -1 on failure.
    private long openOrCreate(LPSZ name, boolean create) {
        String path = lpszToString(name);
        synchronized (this) {
            int idx = pathRegistry.keyIndex(path);
            if (idx < 0) {
                long primaryOsFd = pathRegistry.valueAt(idx);
                if (primaryOsFd == DIRECTORY_MARKER) {
                    return -1; // cannot open a directory as a file
                }
                return dupChild(path, (int) primaryOsFd);
            }
            if (!create) {
                return -1; // file not found
            }
            // Auto-register ancestor directories so isDirOrSoftLinkDir works.
            registerAncestors(path);
            // Create a new memfd and register it as the primary.
            int osFd = Files.memfdCreate(name.ptr());
            if (osFd < 0) {
                LOG.error().$("memfdCreate failed [path=").$(path).$(", errno=").$(Os.errno()).I$();
                return -1;
            }
            // Re-query index since registerAncestors may have rehashed the map.
            idx = pathRegistry.keyIndex(path);
            pathRegistry.putAt(idx, path, (long) osFd);
            return dupChild(path, osFd);
        }
    }

    private void walkRecursive(Path src, FindVisitor func) {
        long findPtr = findFirst(src.$());
        if (findPtr > 0) {
            int len = src.size();
            try {
                do {
                    long namePtr = findName(findPtr);
                    int type = findType(findPtr);
                    func.onFind(namePtr, type);
                    if (type == Files.DT_DIR && Files.notDots(namePtr)) {
                        src.trimTo(len).concat(namePtr).$();
                        walkRecursive(src, func);
                    }
                } while (findNext(findPtr) > 0);
            } finally {
                src.trimTo(len);
                findClose(findPtr);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Directory iterator state
    // -------------------------------------------------------------------------

    private static class FindState {

        final ObjList<String> names;
        final IntList types;
        int index;
        long nameBuf = 0;

        FindState(ObjList<String> names, IntList types) {
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
