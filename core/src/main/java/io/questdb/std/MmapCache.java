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
import io.questdb.log.LogFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;

/**
 * Thread-safe cache for memory-mapped file regions with reference counting.
 * Reuses existing mappings for the same file when possible to reduce system calls.
 * <p>
 * With async munmap enabled, releasing the last reference to the current mapping of a file does not unmap it.
 * The record stays in both caches with a zero reference count, and the async munmap queue gets an eviction hint
 * pointing at it. A lookup that finds the record before the hint is consumed revives it without another mmap.
 * The consumer retires the record only if it is still mapped and unused when the hint comes up; otherwise the
 * hint is dropped. Every zero-count mapped record has at least one hint queued behind it, so each one is
 * eventually reclaimed. Hints may outlive the mapping they were queued for, and the record may by then be pooled
 * and reused for another mapping: the consumer always acts on the record's current state, and evicting any
 * unused mapping is safe.
 */
public final class MmapCache {
    public static final MmapCache INSTANCE = new MmapCache();

    private static final Log LOG = LogFactory.getLog(MmapCache.class);
    private static final int MAX_RECORD_POOL_CAPACITY = 16 * 1024;
    private static final int MUNMAP_QUEUE_CAPACITY = 8 * 1024;
    private final LongObjHashMap<MmapCacheRecord> mmapAddrCache = new LongObjHashMap<>();
    private final LongObjHashMap<MmapCacheRecord> mmapFileCache = new LongObjHashMap<>();
    private final MCSequence munmapConsumerSequence;
    private final MPSequence munmapProducesSequence;
    private final RingQueue<MunmapTask> munmapTaskRingQueue;
    private final ObjStack<MmapCacheRecord> recordPool = new ObjStack<>();
    private long mmapReuseCount = 0;

    private MmapCache() {
        munmapTaskRingQueue = new RingQueue<>(MunmapTask::new, MUNMAP_QUEUE_CAPACITY);
        munmapProducesSequence = new MPSequence(munmapTaskRingQueue.getCycle());
        munmapConsumerSequence = new MCSequence(munmapTaskRingQueue.getCycle());
        munmapProducesSequence.then(munmapConsumerSequence).then(munmapProducesSequence);
    }

    /**
     * Process accumulated unmap requests.
     * This method is not thread safe! It's meant to be called from a synchronized job - one per Server.
     *
     * @return true if at least one mapping was unmapped, false otherwise.
     */
    public boolean asyncMunmap() {
        boolean useful = false;
        long cursor;
        do {
            cursor = munmapConsumerSequence.next();
            if (cursor > -1) {
                useful = true;
                consumeMunmapTask(munmapTaskRingQueue.get(cursor));
                munmapConsumerSequence.done(cursor);
            } else if (cursor == -2) {
                Os.pause();
            }
        } while (cursor != -1);
        return useful;
    }

    /**
     * Maps file region into memory, reusing existing mapping if available.
     *
     * @param fd           file descriptor to map
     * @param mmapCacheKey unique value that is safe to use as a key for caching the map. FD is not good enough
     *                     since a FD can be closed after the mapping is created and OS can re-use for a different file.
     * @param len          length of the mapping
     * @param offset       offset in the file to start mapping from
     * @param flags        memory mapping flags, e.g., Files.MAP_RO for read-only
     */
    public long cacheMmap(int fd, long mmapCacheKey, long len, long offset, int flags, int memoryTag) {
        if (len < 1) {
            throw CairoException.critical(0)
                    .put("could not mmap file, invalid len [len=").put(len)
                    .put(", offset=").put(offset)
                    .put(", fd=").put(fd)
                    .put(", memoryTag=").put(memoryTag)
                    .put(']');
        }
        if (offset != 0 || mmapCacheKey == 0 || !Files.FS_CACHE_ENABLED || flags == Files.MAP_RW) {
            return mmap0(fd, len, offset, flags, memoryTag);
        }

        if (Files.ASYNC_MUNMAP_ENABLED) {
            return cacheMmapOptimistic(fd, mmapCacheKey, len, memoryTag);
        } else {
            return cacheMmapPessimistic(fd, mmapCacheKey, len, memoryTag);
        }
    }

    /**
     * Returns number of times cached memory mappings were reused.
     */
    public long getReuseCount() {
        return mmapReuseCount;
    }

    /**
     * Checks if memory mapping has only one active reference.
     */
    public synchronized boolean isSingleUse(long address) {
        var cacheRecord = mmapAddrCache.get(address);
        return cacheRecord == null || cacheRecord.count <= 1;
    }

    /**
     * Resizes existing memory mapping, reusing or creating new mapping as needed.
     */
    public long mremap(int fd, long mmapCacheKey, long address, long previousSize, long newSize, long offset, int flags, int memoryTag) {
        if (newSize < 1) {
            throw CairoException.critical(0)
                    .put("could not remap file, invalid newSize [previousSize=").put(previousSize)
                    .put(", newSize=").put(newSize)
                    .put(", offset=").put(offset)
                    .put(", fd=").put(fd)
                    .put(", memoryTag=").put(memoryTag)
                    .put(']');
        }
        if (offset != 0 || mmapCacheKey == 0 || !Files.FS_CACHE_ENABLED || flags == Files.MAP_RW) {
            return mremap0(fd, address, previousSize, newSize, offset, flags, memoryTag, memoryTag);
        }
        if (previousSize == 0) {
            // If previous size is 0, we cannot remap, just mmap a new region
            return cacheMmap(fd, mmapCacheKey, newSize, offset, flags, memoryTag);
        }

        long unmapPtr = 0, unmapLen = 0;
        int unmapTag = 0;
        long newAddress = 0;

        synchronized (this) {

            int addrMapIndex = mmapAddrCache.keyIndex(address);
            assert addrMapIndex < 0 : "old address is not found in mmap cache";

            MmapCacheRecord record = mmapAddrCache.valueAt(addrMapIndex);

            int fdIndex = Integer.MAX_VALUE;
            if (newSize >= previousSize) {
                if (record.length >= newSize) {
                    // Address is already long enough, just return it
                    return address;
                }

                // Check if someone else remapped this to a larger size
                fdIndex = mmapFileCache.keyIndex(mmapCacheKey);
                if (fdIndex < 0) {
                    MmapCacheRecord updatedCacheRecord = mmapFileCache.valueAt(fdIndex);
                    if (updatedCacheRecord.length >= newSize) {
                        // Cache for the FD is updated by someone else
                        // The fd cache record is already long enough, just return the address
                        updatedCacheRecord.count++;
                        newAddress = updatedCacheRecord.address;
                        // We should not store zero addresses in the cache. We do not cache if someone maps 0 length,
                        // and we do not allow to remap to 0 length.
                        assert newAddress != 0;
                        mmapReuseCount++;

                        record.count--;
                        if (record.count == 0) {
                            // The old cache record is not used anymore. The file cache points at the longer
                            // record, so no lookup can revive this one: retire it right away.
                            unmapPtr = record.address;
                            unmapLen = record.length;
                            unmapTag = record.memoryTag;
                            retire(record, addrMapIndex);
                        }
                    }
                }
            }

            if (newAddress == 0) {
                // We need to extend the mmap
                if (record.count == 1) {
                    // No one else uses the record, we can use mremap.
                    // it mremap0() throws then we change nothing
                    newAddress = mremap0(fd, record.address, record.length, newSize, offset, Files.MAP_RO, record.memoryTag, memoryTag);
                    if (newAddress != FilesFacade.MAP_FAILED) {
                        record.address = newAddress;
                        record.length = newSize;
                        record.memoryTag = memoryTag;
                        mmapAddrCache.removeAt(addrMapIndex);
                        mmapAddrCache.put(newAddress, record);
                    }
                } else {
                    // Someone else is using the record, we need to create a new one
                    assert record.count > 1 : "invalid reference count in mmap cache";
                    // if mmap0() throws then we change nothing
                    newAddress = mmap0(fd, newSize, 0, Files.MAP_RO, memoryTag);

                    // yay, mmap0() did not throw! it could still return -1 though
                    if (newAddress != FilesFacade.MAP_FAILED) {
                        // we decrease reference count of the old record iff mmap0() succeeded.
                        // Q: Why we don't decrease the reference count even in the presence of failures?
                        // A: Because the semantic of mremap() failure is that the old mapping is still valid
                        //    and callers are still expected to eventually close the old mapping
                        record.count--;
                        // Cache the new mmap record
                        MmapCacheRecord newRecord = createMmapCacheRecord(fd, mmapCacheKey, newSize, newAddress, memoryTag);
                        if (fdIndex != Integer.MAX_VALUE) {
                            mmapFileCache.putAt(fdIndex, mmapCacheKey, newRecord);
                        } else {
                            mmapFileCache.put(mmapCacheKey, newRecord);
                        }
                        mmapAddrCache.put(newAddress, newRecord);
                    }
                }
            }
        }

        // unmap is usually a slow OS call, to not block everyone, move it out of the synchronized section
        if (unmapPtr != 0) {
            // Unmap the old address if it was not used anymore
            unmap0(unmapPtr, unmapLen, unmapTag);
        }

        // Return the new address
        return newAddress;
    }

    /**
     * Unmaps memory region, decrements reference count, and removes from cache if last reference.
     */
    public void unmap(long address, long len, int memoryTag) {
        if (address <= 0 || len <= 0) {
            throw CairoException.critical(0)
                    .put("unmap: invalid address or length [address=" + address + ", len=" + len + ']');
        }

        if (!Files.FS_CACHE_ENABLED) {
            unmap0(address, len, memoryTag);
            return;
        }

        long unmapPtr = address, unmapLen = len;
        int unmapTag = memoryTag;

        synchronized (this) {
            int addrMapIndex = mmapAddrCache.keyIndex(address);
            if (addrMapIndex < 0) {
                var record = mmapAddrCache.valueAt(addrMapIndex);
                assert record.count > 0 : "unmap of a released mapping [address=" + address + ']';
                record.count--;

                if (record.count != 0) {
                    return;
                }

                // The last usage of the mapping is released. If it is still the file's current mapping, keep it
                // cached for revival and leave the unmap to the async consumer. A superseded mapping cannot be
                // revived, and a full queue cannot take the hint: retire those right away.
                if (Files.ASYNC_MUNMAP_ENABLED && isCurrentFileMapping(record) && publishEvictionHint(record)) {
                    return;
                }

                unmapPtr = record.address;
                unmapLen = record.length;
                unmapTag = record.memoryTag;
                retire(record, addrMapIndex);
            }
            // else: not cached, unmap the caller's mapping as is
        }

        // Unmap after exiting the lock. With async munmap enabled, this offloads the unmap to the consumer,
        // unless the queue is full.
        unmap0(unmapPtr, unmapLen, unmapTag);
    }

    private static long mmap0(int fd, long len, long offset, int flags, int memoryTag) {
        long address = Files.mmap0(fd, len, offset, flags, 0);
        if (address != FilesFacade.MAP_FAILED) {
            Unsafe.recordMemAlloc(len, memoryTag);
        }
        return address;
    }

    private static long mremap0(int fd, long address, long previousSize, long newSize, long offset, int flags, int oldMemoryTag, int memoryTag) {
        address = Files.mremap0(fd, address, previousSize, newSize, offset, flags);
        if (address != -1) {
            if (oldMemoryTag == memoryTag) {
                Unsafe.recordMemAlloc(newSize - previousSize, memoryTag);
            } else {
                Unsafe.recordMemAlloc(newSize, memoryTag);
                Unsafe.recordMemAlloc(-previousSize, oldMemoryTag);
            }
        }
        return address;
    }

    private static void munmapOrLog(long address, long len, int memoryTag) {
        int result = Files.munmap0(address, len);
        if (result != -1) {
            Unsafe.recordMemAlloc(-len, memoryTag);
        } else {
            int errno = Os.errno();
            LOG.critical().$("munmap failed [address=").$(address)
                    .$(", size=").$(len)
                    .$(", tag=").$(MemoryTag.nameOf(memoryTag))
                    .$(", errno=").$(errno)
                    .I$();
        }
    }

    private long cacheMmapOptimistic(int fd, long mmapCacheKey, long len, int memoryTag) {
        // Fast path: check cache under lock
        synchronized (this) {
            int fdMapIndex = mmapFileCache.keyIndex(mmapCacheKey);
            if (fdMapIndex < 0) {
                MmapCacheRecord record = mmapFileCache.valueAt(fdMapIndex);
                if (record.length >= len) {
                    // a zero reference count is a released mapping awaiting eviction: this revives it
                    assert record.count >= 0 : "found a record with negative reference count in mmap cache [fd=" + fd + "]";
                    record.count++;
                    mmapReuseCount++;
                    return record.address;
                }
            }
        }
        // Cache miss, need to create new mapping. Perform actual mmap outside the lock.
        long address = mmap0(fd, len, 0, Files.MAP_RO, memoryTag);
        if (address == FilesFacade.MAP_FAILED) {
            return address;
        }

        // We'll need these if we make a redundant mapping and need to unmap it:
        long redundantAddress;
        long redundantLen;
        int redundantTag;
        long returnAddress;

        // Re-acquire lock and update cache
        synchronized (this) {
            // Re-check: someone else might have added a mapping while we were mapping
            int fdMapIndex = mmapFileCache.keyIndex(mmapCacheKey);
            if (fdMapIndex >= 0) {
                // We're alone -- use our mapping and return right away
                MmapCacheRecord record = createMmapCacheRecord(fd, mmapCacheKey, len, address, memoryTag);
                mmapFileCache.putAt(fdMapIndex, mmapCacheKey, record);
                mmapAddrCache.put(address, record);
                return address;
            }

            // Race condition -- both we and another thread created a mapping. Decide which one
            // to keep. We can't keep the existing one if it's too small.
            MmapCacheRecord existingRecord = mmapFileCache.valueAt(fdMapIndex);
            if (existingRecord.length < len) {
                // Existing mapping is too small - replace it with ours.
                // There are two caches: file cache and address cache. We'll put the entry
                // with our larger mapping into the file cache, so it gets used from now on.
                // However, some threads may already have grabbed the smaller mapping, and
                // are using it. Once all its users are done with it and unmap it, that will
                // remove it from the address cache. Therefore, we add our address to the
                // address cache, and leave the other one there as well.
                MmapCacheRecord record = createMmapCacheRecord(fd, mmapCacheKey, len, address, memoryTag);
                mmapFileCache.putAt(fdMapIndex, mmapCacheKey, record);
                mmapAddrCache.put(address, record);
                return address;
            }

            // Existing mapping is fine - use it, discard ours
            existingRecord.count++;
            mmapReuseCount++;
            redundantAddress = address;
            redundantLen = len;
            redundantTag = memoryTag;
            returnAddress = existingRecord.address;
        }

        // We lost the race, clean up redundant mapping outside the lock
        try {
            // This submits an async unmap operation, and only in an extreme (unmap queue full) case
            // will do it sync with the potential to throw CairoException
            unmap0(redundantAddress, redundantLen, redundantTag);
        } catch (CairoException e) {
            LOG.critical().$("failed to unmap redundant mapping after losing race [message=").$(e.getMessage()).I$();
        }
        return returnAddress;
    }

    private long cacheMmapPessimistic(int fd, long mmapCacheKey, long len, int memoryTag) {
        synchronized (this) {
            int fdMapIndex = mmapFileCache.keyIndex(mmapCacheKey);
            if (fdMapIndex < 0) {
                MmapCacheRecord record = mmapFileCache.valueAt(fdMapIndex);
                if (record.length >= len) {
                    // a zero reference count is a released mapping awaiting eviction: this revives it
                    assert record.count >= 0 : "found a record with negative reference count in mmap cache [fd=" + fd + "]";
                    record.count++;
                    mmapReuseCount++;
                    return record.address;
                }
            }

            // Cache RO maps only.
            long address = mmap0(fd, len, 0, Files.MAP_RO, memoryTag);

            if (address == FilesFacade.MAP_FAILED) {
                return address;
            }
            // Cache the mmap record
            MmapCacheRecord record = createMmapCacheRecord(fd, mmapCacheKey, len, address, memoryTag);
            mmapFileCache.putAt(fdMapIndex, mmapCacheKey, record);

            // Point the returned address to the correct offset
            mmapAddrCache.put(address, record);

            return address;
        }

    }

    private void consumeMunmapTask(MunmapTask task) {
        final MmapCacheRecord record = task.record;
        if (record == null) {
            munmapOrLog(task.address, task.size, task.memoryTag);
            return;
        }
        task.record = null;

        long address, len;
        int memoryTag;
        synchronized (this) {
            if (record.address == 0 || record.count != 0) {
                // Already retired, or revived since the hint was queued. A revived mapping gets a new hint
                // when its last user releases it again.
                return;
            }
            // Snapshot under the lock: once retired, the record can be pooled and reused by another thread.
            address = record.address;
            len = record.length;
            memoryTag = record.memoryTag;
            int addrMapIndex = mmapAddrCache.keyIndex(address);
            assert addrMapIndex < 0 && mmapAddrCache.valueAt(addrMapIndex) == record : "mapped record is not in mmap address cache";
            retire(record, addrMapIndex);
        }
        munmapOrLog(address, len, memoryTag);
    }

    private MmapCacheRecord createMmapCacheRecord(int fd, long fileCacheKey, long len, long address, int memoryTag) {
        MmapCacheRecord rec = recordPool.pop();
        if (rec != null) {
            rec.of(fd, fileCacheKey, len, address, 1, memoryTag);
            return rec;
        }
        return new MmapCacheRecord(fd, fileCacheKey, len, address, 1, memoryTag);
    }

    /**
     * Must be called under the lock.
     */
    private boolean isCurrentFileMapping(MmapCacheRecord record) {
        int fdIndex = mmapFileCache.keyIndex(record.fileCacheKey);
        return fdIndex < 0 && mmapFileCache.valueAt(fdIndex) == record;
    }

    /**
     * Queues a hint to evict the released, still cached record. Must be called under the lock, so that the record
     * cannot be revived and released again before the hint is in the queue.
     *
     * @return false if the queue is full
     */
    private boolean publishEvictionHint(MmapCacheRecord record) {
        long seq;
        while ((seq = munmapProducesSequence.next()) == -2) {
            Os.pause();
        }
        if (seq < 0) {
            return false;
        }
        MunmapTask task = munmapTaskRingQueue.get(seq);
        task.record = record;
        task.address = 0;
        task.size = 0;
        task.memoryTag = 0;
        munmapProducesSequence.done(seq);
        return true;
    }

    /**
     * Removes the record from both caches and pools it. Must be called under the lock, with the record's
     * mapping details already copied out: the caller unmaps after releasing the lock.
     */
    private void retire(MmapCacheRecord record, int addrMapIndex) {
        mmapAddrCache.removeAt(addrMapIndex);
        // The file cache may already point at a longer mapping of the same file
        int fdIndex = mmapFileCache.keyIndex(record.fileCacheKey);
        if (fdIndex < 0 && mmapFileCache.valueAt(fdIndex) == record) {
            mmapFileCache.removeAt(fdIndex);
        }
        record.address = 0;
        if (recordPool.size() < MAX_RECORD_POOL_CAPACITY) {
            recordPool.push(record);
        }
    }

    private void unmap0(long address, long len, int memoryTag) {
        if (Files.ASYNC_MUNMAP_ENABLED) {
            // sequence returning -2 -> we lost a CAS race. we do a cheap retry
            // sequence returning -1 -> the queue is full. then it's cheaper to do the munmap ourserlves
            long seq;
            while ((seq = munmapProducesSequence.next()) == -2) {
                Os.pause();
            }

            if (seq > -1) {
                MunmapTask task = munmapTaskRingQueue.get(seq);
                task.record = null;
                task.address = address;
                task.size = len;
                task.memoryTag = memoryTag;
                munmapProducesSequence.done(seq);
                return;
            } else {
                LOG.info().$("async munmap queue is full").$();
            }
        }
        int result = Files.munmap0(address, len);
        if (result != -1) {
            Unsafe.recordMemAlloc(-len, memoryTag);
        } else {
            throw CairoException.critical(Os.errno())
                    .put("munmap failed [address=").put(address)
                    .put(", len=").put(len)
                    .put(", memoryTag=").put(memoryTag).put(']');
        }
    }

    /**
     * Cache record holding memory mapping details and reference count.
     */
    private static class MmapCacheRecord {
        long address;
        int count;
        int fd;
        long fileCacheKey;
        long length;
        int memoryTag;

        public MmapCacheRecord(int fd, long fileCacheKey, long length, long address, int count, int memoryTag) {
            this.fd = fd;
            this.fileCacheKey = fileCacheKey;
            this.length = length;
            this.address = address;
            this.count = count;
            this.memoryTag = memoryTag;
        }

        public void of(int fd, long fileCacheKey, long len, long address, int count, int memoryTag) {
            this.fd = fd;
            this.fileCacheKey = fileCacheKey;
            this.length = len;
            this.address = address;
            this.count = count;
            this.memoryTag = memoryTag;
        }
    }

    /**
     * Either a raw mapping to unmap (address, size, memoryTag), or, when the record is set, a hint to evict
     * the record if it is still mapped and unused when the hint is consumed.
     */
    private static class MunmapTask {
        private long address;
        private int memoryTag;
        private MmapCacheRecord record;
        private long size;
    }
}
