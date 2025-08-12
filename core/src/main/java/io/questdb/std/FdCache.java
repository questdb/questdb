/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.ParanoiaState.FD_PARANOIA_MODE;

/**
 * Thread-safe cache for file descriptors with reference counting and path-based lookup.
 * Prevents duplicate file opens and manages both read-only and read-write access modes.
 */
public class FdCache {
    static final AtomicInteger OPEN_OS_FILE_COUNT = new AtomicInteger();
    private static final int MAX_RECORD_POOL_CAPACITY = 16 * 1024;
    private static final int NON_CACHED = (2 << 30);
    private static final int RO_MASK = 0;
    private final AtomicInteger fdCounter = new AtomicInteger(1);
    private final LongObjHashMap<FdCacheRecord> openFdMapByFd = new LongObjHashMap<>();
    private final Utf8SequenceObjHashMap<FdCacheRecord> openFdMapByPath = new Utf8SequenceObjHashMap<>();
    private final ObjStack<FdCacheRecord> recordPool = new ObjStack<>();
    private long fdReuseCount = 0;

    /**
     * Closes file descriptor, decrements reference count, and removes from cache if last reference.
     */
    public synchronized int close(long fd) {
        int keyIndex = openFdMapByFd.keyIndex(fd);
        if (keyIndex > -1) {
            // ALl long FDs are unique and tracked in the map, unless detached.
            // If not found in openFdMapByFd map it means double close.
            throw new IllegalStateException("fd " + fd + " is already closed!");
        }

        int fdKind = (Numbers.decodeLowInt(fd) >>> 30) & 3; // Extract bits 30-31
        if (fdKind > 1) {
            // NON_CACHED. Simply close the underlying fd.
            int osFd = Numbers.decodeHighInt(fd);
            int res = Files.close0(osFd);
            if (res != 0) {
                return res;
            }
            OPEN_OS_FILE_COUNT.decrementAndGet();
            openFdMapByFd.removeAt(keyIndex);
            return 0;
        }

        FdCacheRecord fdCacheRecord = openFdMapByFd.valueAt(keyIndex);
        // Remove unique FD tracking.
        openFdMapByFd.removeAt(keyIndex);

        fdCacheRecord.count--;
        if (fdCacheRecord.count == 0) {
            openFdMapByPath.remove(fdCacheRecord.path);
            int res = Files.close0(fdCacheRecord.osFd);

            if (res != 0) {
                // If closing fails, we don't want to decrement the open file count
                // and pool the record.
                return res;
            }

            if (recordPool.size() < MAX_RECORD_POOL_CAPACITY) {
                fdCacheRecord.osFd = -1;
                recordPool.push(fdCacheRecord);
            }
            OPEN_OS_FILE_COUNT.decrementAndGet();
        }

        return 0;
    }

    /**
     * Creates unique file descriptor wrapper for non-cached OS file descriptor.
     */
    public synchronized long createUniqueFdNonCached(int fd) {
        if (fd > -1) {
            int index = fdCounter.getAndIncrement();
            long markedFd = Numbers.encodeLowHighInts(index | NON_CACHED, fd);
            openFdMapByFd.put(markedFd, FdCacheRecord.EMPTY);
            OPEN_OS_FILE_COUNT.incrementAndGet();
            return markedFd;
        }
        return fd;
    }

    /**
     * Creates unique file descriptor wrapper for stdout without validation checks.
     */
    public synchronized long createUniqueFdNonCachedStdOut(int fd) {
        int index = fdCounter.getAndIncrement();
        long markedFd = Numbers.encodeLowHighInts(index | NON_CACHED, fd);
        openFdMapByFd.put(markedFd, FdCacheRecord.EMPTY);
        return markedFd;
    }

    /**
     * Removes file descriptor from cache without closing underlying OS descriptor.
     */
    public synchronized void detach(long fd) {
        int keyIndex = openFdMapByFd.keyIndex(fd);
        if (keyIndex < 0) {
            FdCacheRecord cacheRecord = openFdMapByFd.valueAt(keyIndex);
            if (cacheRecord != FdCacheRecord.EMPTY) {
                throw new IllegalStateException("Cannot detach file cached file descriptor");
            }

            openFdMapByFd.removeAt(keyIndex);
            OPEN_OS_FILE_COUNT.decrementAndGet();
        }
    }

    public synchronized long getOpenCachedFileCount() {
        return openFdMapByFd.size();
    }

    /**
     * Returns comma-separated list of open file descriptor IDs for debugging.
     */
    public synchronized String getOpenFdDebugInfo() {
        final StringSink sink = Misc.getThreadLocalSink();
        openFdMapByFd.forEach((key, value) -> {
            if (sink.length() > 0) {
                sink.put(',');
            }
            sink.put(key);
        });
        return sink.toString();
    }

    public long getOpenOsFileCount() {
        return OPEN_OS_FILE_COUNT.get();
    }

    /**
     * Returns number of times cached file descriptors were reused.
     */
    public long getReuseCount() {
        return fdReuseCount;
    }

    /**
     * Opens file in read-only mode with caching support.
     */
    public synchronized long openROCached(LPSZ lpsz) {
        final FdCacheRecord holder = getFdCacheRecord(lpsz);
        if (holder == null) {
            // Failed to open
            return -1;
        }

        holder.count++;
        long uniqROFd = createUniqueFdRO(holder.osFd);
        openFdMapByFd.put(uniqROFd, holder);

        return uniqROFd;
    }

    /**
     * Removes file path from cache when file is deleted.
     */
    public synchronized boolean remove(LPSZ lpsz) {
        // Even if we cannot remove, remove the fd from cache anyway
        openFdMapByPath.remove(lpsz);
        return Files.remove(lpsz.ptr());
    }

    /**
     * Renames file in the filesystem and updates cache.
     *
     * @param oldName Old file name
     * @param newName New file name
     * @return 0 on success, -1 on failure
     */
    public synchronized int rename(LPSZ oldName, LPSZ newName) {
        int keyIndex = openFdMapByPath.keyIndex(oldName);
        int result = Files.rename(oldName.ptr(), newName.ptr());
        if (result == 0 && keyIndex < 0) {
            FdCacheRecord record = openFdMapByPath.valueAt(keyIndex);
            openFdMapByPath.removeAt(keyIndex);
            Utf8String path = Utf8String.newInstance(newName);
            openFdMapByPath.put(path, record);
            record.path = path;
        }
        return result;
    }

    /**
     * Retrieves memory map cache file descriptor for given file descriptor.
     */
    public synchronized long toMmapCacheFd(long fd) {
        var cacheRecord = openFdMapByFd.get(fd);
        if (cacheRecord == null) {
            return 0;
        }
        return cacheRecord.mmapCacheFd;
    }

    /**
     * Extracts underlying OS file descriptor from cached file descriptor.
     */
    public int toOsFd(long fd) {
        if (FD_PARANOIA_MODE && fd != -1) {
            synchronized (this) {
                int keyIndex = openFdMapByFd.keyIndex(fd);
                assert keyIndex < 0 : "Invalid fd=" + fd + ", not found in cache";
            }
        }
        int osFd = Numbers.decodeHighInt(fd);
        assert fd == -1 || osFd > 0;
        return osFd;
    }

    /**
     * Extracts OS file descriptor with write permission validation.
     */
    public int toOsFd(long fd, boolean write) {
        assert !write || (Numbers.decodeLowInt(fd) >>> 30) != 0 : "RO fd cannot be used for writing: " + fd;
        return toOsFd(fd);
    }

    private FdCacheRecord createFdCacheRecord(Utf8String path, long mmapCacheFd) {
        FdCacheRecord holder = recordPool.pop();
        if (holder == null) {
            holder = new FdCacheRecord(path, mmapCacheFd);
        } else {
            holder.path = path;
            holder.mmapCacheFd = mmapCacheFd;
        }
        return holder;
    }

    private long createUniqueFdRO(int fd) {
        int index = fdCounter.getAndIncrement();
        return Numbers.encodeLowHighInts(index | RO_MASK, fd);
    }

    @Nullable
    private FdCacheRecord getFdCacheRecord(LPSZ lpsz) {
        int keyIndex = openFdMapByPath.keyIndex(lpsz);
        final FdCacheRecord holder;
        if (keyIndex > -1) {
            int osFd = Files.openRO(lpsz.ptr());
            if (osFd < 0) {
                // Failed to open
                holder = null;
            } else {
                OPEN_OS_FILE_COUNT.incrementAndGet();
                Utf8String path = Utf8String.newInstance(lpsz);
                holder = createFdCacheRecord(path, Numbers.encodeLowHighInts(fdCounter.incrementAndGet(), osFd));
                holder.osFd = osFd;
                openFdMapByPath.putAt(keyIndex, lpsz, holder);
            }
        } else {
            holder = openFdMapByPath.valueAtQuick(keyIndex);
            fdReuseCount++;
        }
        return holder;
    }

    /**
     * Cache record holding file path, OS file descriptor, reference count, and mmap cache link.
     */
    private static class FdCacheRecord {
        private static final FdCacheRecord EMPTY = new FdCacheRecord(null, 0);
        long mmapCacheFd;
        private int count;
        private int osFd;
        private Utf8String path;

        public FdCacheRecord(Utf8String path, long mmapCacheFd) {
            this.path = path;
            this.mmapCacheFd = mmapCacheFd;
        }
    }
}
