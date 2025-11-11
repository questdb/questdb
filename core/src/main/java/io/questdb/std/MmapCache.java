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

import io.questdb.cairo.CairoException;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.YieldingWaitStrategy;

/**
 * Thread-safe cache for memory-mapped file regions with reference counting.
 * Reuses existing mappings for the same file when possible to reduce system calls.
 */
public class MmapCache {
    private static final int MAX_RECORD_POOL_CAPACITY = 16 * 1024;
    private final LongObjHashMap<MmapCacheRecord> mmapAddrCache = new LongObjHashMap<>();
    private final LongObjHashMap<MmapCacheRecord> mmapFileCache = new LongObjHashMap<>();
    private final SCSequence munmapConsumerSequence;
    private final MPSequence munmapProducesSequence;
    private final RingQueue<MunmapTask> munmapTaskRingQueue;
    private final ObjStack<MmapCacheRecord> recordPool = new ObjStack<>();
    private long mmapReuseCount = 0;

    public MmapCache() {
        // todo: configurable queue size
        munmapTaskRingQueue = new RingQueue<>(MunmapTask::new, Numbers.ceilPow2(10000));
        munmapProducesSequence = new MPSequence(munmapTaskRingQueue.getCycle());
        munmapConsumerSequence = new SCSequence(new YieldingWaitStrategy());
        munmapProducesSequence.then(munmapConsumerSequence).then(munmapProducesSequence);
    }

    public boolean asyncMunmap() {
        // todo: handle errors
        return munmapConsumerSequence.consumeAll(munmapTaskRingQueue, task -> {
            int result = Files.munmap0(task.address, task.size);
            if (result != -1) {
                Unsafe.recordMemAlloc(-task.size, task.memoryTag);
            }
        });
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

        synchronized (this) {

            int fdMapIndex = mmapFileCache.keyIndex(mmapCacheKey);
            if (fdMapIndex < 0) {
                MmapCacheRecord record = mmapFileCache.valueAt(fdMapIndex);
                if (record.length >= len) {
                    assert record.count > 0 : "found a record with zero reference count in mmap cache [fd=" + fd + "]";
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
        return cacheRecord != null && cacheRecord.count == 1;
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
                            // The old cache record is not used anymore
                            mmapAddrCache.removeAt(addrMapIndex);
                            unmapPtr = record.address;
                            unmapLen = record.length;
                            unmapTag = record.memoryTag;
                            record.address = 0;

                            if (recordPool.size() < MAX_RECORD_POOL_CAPACITY) {
                                recordPool.push(record);
                            }
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

        long unmapPtr, unmapLen;
        int unmapTag;

        synchronized (this) {
            int addrMapIndex = mmapAddrCache.keyIndex(address);
            if (addrMapIndex > -1) {
                // Not cached
                unmap0(address, len, memoryTag);
                return;
            }

            var record = mmapAddrCache.valueAt(addrMapIndex);
            record.count--;

            if (record.count != 0) {
                assert record.count > -1;
                return;
            }

            // Remove the record from the cache, the last usage of the address is unmapped
            mmapAddrCache.removeAt(addrMapIndex);

            // Check if the same map record is used for the FD,
            // it can be already overwritten by a longer map over the same file
            int fdIndex = mmapFileCache.keyIndex(record.fileCacheKey);
            if (fdIndex < 0 && mmapFileCache.valueAt(fdIndex) == record) {
                mmapFileCache.removeAt(fdIndex);
            }

            // Unmap after exiting the lock.
            unmapPtr = record.address;
            unmapLen = record.length;
            unmapTag = record.memoryTag;
            record.address = 0;
            if (recordPool.size() < MAX_RECORD_POOL_CAPACITY) {
                recordPool.push(record);
            }
        }

        // offload the unmap to a single thread to not block everyone under synchronized section
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

    private MmapCacheRecord createMmapCacheRecord(int fd, long fileCacheKey, long len, long address, int memoryTag) {
        MmapCacheRecord rec = recordPool.pop();
        if (rec != null) {
            rec.of(fd, fileCacheKey, len, address, 1, memoryTag);
            return rec;
        }
        return new MmapCacheRecord(fd, fileCacheKey, len, address, 1, memoryTag);
    }

    private void unmap0(long address, long len, int memoryTag) {
        if (Files.ASYNC_MUNMAP_ENABLED) {
            long seq = munmapProducesSequence.next();
            if (seq > -1) {
                MunmapTask task = munmapTaskRingQueue.get(seq);
                task.address = address;
                task.size = len;
                task.memoryTag = memoryTag;
                munmapProducesSequence.done(seq);
                return;
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

    private static class MunmapTask {
        long address;
        int memoryTag;
        long size;
    }
}
