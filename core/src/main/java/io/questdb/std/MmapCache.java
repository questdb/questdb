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

/**
 * Thread-safe cache for memory-mapped file regions with reference counting.
 * Reuses existing mappings for the same file when possible to reduce system calls.
 */
public class MmapCache {
    private final LongObjHashMap<MmapCacheRecord> mmapAddrCache = new LongObjHashMap<>();
    private final LongObjHashMap<MmapCacheRecord> mmapFileCache = new LongObjHashMap<>();
    private long mmapReuseCount = 0;

    /**
     * Maps file region into memory, reusing existing mapping if available.
     */
    public long cacheMmap(int fd, long fileCacheKey, long len, long offset, int flags, int memoryTag) {
        if (offset != 0 || fileCacheKey == 0 || !Files.FS_CACHE_ENABLED) {
            return mmap0(fd, len, offset, flags, memoryTag);
        }

        synchronized (this) {

            int fdMapIndex = mmapFileCache.keyIndex(fileCacheKey);
            if (fdMapIndex < 0) {
                MmapCacheRecord record = mmapFileCache.valueAt(fdMapIndex);
                if (record.length >= len) {
                    record.count++;
                    mmapReuseCount++;
                    return record.address;
                }
            }

            // Always map as RW, even if the request is RO.
            long address = mmap0(fd, len, 0, Files.MAP_RW, memoryTag);

            if (address == -1) {
                // mmap failed, return 0
                return address;
            }
            // Cache the mmap record
            var record = new MmapCacheRecord(fd, fileCacheKey, len, address, 1, memoryTag);
            mmapFileCache.putAt(fdMapIndex, fileCacheKey, record);

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
    public boolean isSingleUse(long address) {
        var cacheRecord = mmapAddrCache.get(address);
        return cacheRecord != null && cacheRecord.count == 1;
    }

    /**
     * Resizes existing memory mapping, reusing or creating new mapping as needed.
     */
    public long mremap(int fd, long fileCacheKey, long address, long previousSize, long newSize, long offset, int flags, int memoryTag) {
        // TODO: handle the offset
        if (offset != 0 || fileCacheKey == 0 || !Files.FS_CACHE_ENABLED) {
            return mremap0(fd, address, previousSize, newSize, offset, flags, memoryTag, memoryTag);
        }

        long unmapPtr = 0, unmapLen = 0;
        int unmapTag = 0;
        long newAddress = 0;

        synchronized (this) {

            int addrMapIndex = mmapAddrCache.keyIndex(address);
            assert addrMapIndex < 0 : "old address is not found in mmap cache";

            MmapCacheRecord record = mmapAddrCache.valueAt(addrMapIndex);

            if (newSize >= previousSize) {
                if (record.length >= newSize) {
                    // Address is already long enough, just return it
                    return address;
                }

                // Check if someone else remapped this to a larger size
                int fdIndex = mmapFileCache.keyIndex(fileCacheKey);
                if (fdIndex < 0) {
                    MmapCacheRecord updatedCacheRecord = mmapFileCache.valueAt(fdIndex);
                    if (updatedCacheRecord.length >= newSize) {
                        // Cache for the FD is updated by someone else
                        // The fd cache record is already long enough, just return the address
                        updatedCacheRecord.count++;
                        newAddress = updatedCacheRecord.address;
                        mmapReuseCount++;

                        record.count--;
                        if (record.count == 0) {
                            // The old cache record is not used anymore
                            mmapAddrCache.removeAt(addrMapIndex);
                            unmapPtr = record.address;
                            unmapLen = record.length;
                            unmapTag = record.memoryTag;
                            record.address = 0;
                        }
                    }
                }
            }

            if (newAddress == 0) {
                // We need to extend the mmap
                record.count--;
                if (record.count == 0) {
                    // No one else uses the record, we can use mremap
                    newAddress = mremap0(fd, record.address, record.length, newSize, offset, Files.MAP_RW, record.memoryTag, memoryTag);
                    if (newAddress != -1) {
                        record.address = newAddress;
                        record.length = newSize;
                        record.memoryTag = memoryTag;
                        mmapAddrCache.removeAt(addrMapIndex);
                        mmapAddrCache.put(newAddress, record);
                    }
                    record.count = 1;
                } else {
                    // Someone else is using the record, we need to create a new one
                    newAddress = mmap0(fd, newSize, 0, Files.MAP_RW, memoryTag);
                    if (newAddress != -1) {
                        // Cache the new mmap record
                        var newRecord = new MmapCacheRecord(fd, fileCacheKey, newSize, newAddress, 1, memoryTag);
                        mmapFileCache.put(fileCacheKey, newRecord);
                        mmapAddrCache.put(newAddress, newRecord);
                    }
                }
            }
        }

        // offload the unmap to a single thread to not block everyone under synchronized section
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
        if (address == 0 || len <= 0) {
            return;
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
            assert record.count > -1;

            if (record.count != 0) {
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
        }

        // offload the unmap to a single thread to not block everyone under synchronized section
        unmap0(unmapPtr, unmapLen, unmapTag);
    }

    private static long mmap0(int fd, long len, long offset, int flags, int memoryTag) {
        long address = Files.mmap0(fd, len, offset, flags, 0);
        if (address != -1) {
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

    private static void unmap0(long address, long len, int memoryTag) {
        if (address != 0 && Files.munmap0(address, len) != -1) {
            Unsafe.recordMemAlloc(-len, memoryTag);
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
    }
}
