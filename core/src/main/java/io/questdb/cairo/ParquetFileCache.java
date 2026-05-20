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

package io.questdb.cairo;

import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Utf8SequenceObjHashMap;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;

import java.io.Closeable;

/**
 * Engine-scoped shared cache of opened parquet files. Each entry holds the
 * file descriptor, mmap address, and a parsed {@link ParquetFileDecoder}.
 * The underlying fd and mmap are themselves refcounted by {@code FdCache}
 * and {@code MmapCache.INSTANCE}; this cache adds entry-level refcounts so
 * the decoder native state (which is NOT refcounted at a lower layer) can
 * be shared across cursors against the same path.
 * <p>
 * Eviction is LRU and bounded by total mapped bytes and entry count, both
 * configured globally. Entries currently in use (refcount &gt; 0) are pinned
 * and skipped during eviction. Once a pressured eviction pass can free no
 * more refcount=0 entries, the cache is full of live holders and stays
 * over the budget until a holder releases.
 * <p>
 * Thread model: every public method locks the cache. Acquire/release are
 * the hot paths and do at most one path lookup, one LRU touch, and (on
 * miss) one file open + mmap + footer parse. Decoder construction happens
 * under the lock; that is fine while parquet files are infrequent and
 * footers are small. If contention becomes a problem, the lock can be
 * split per-shard later.
 */
public class ParquetFileCache implements Closeable {

    private static final Log LOG = LogFactory.getLog(ParquetFileCache.class);
    private final Utf8SequenceObjHashMap<Entry> entries = new Utf8SequenceObjHashMap<>();
    private final Object lock = new Object();
    // Oldest at index 0, newest at the tail. Holds every Entry alive in `entries`,
    // including refcount=0 ones eligible for eviction.
    private final ObjList<Entry> lru = new ObjList<>();
    private final long maxBytes;
    private final int maxEntries;
    private boolean closed;
    private long currentBytes;

    public ParquetFileCache(CairoConfiguration configuration) {
        this.maxBytes = Math.max(1L, configuration.getParquetCacheMaxBytes());
        this.maxEntries = Math.max(1, configuration.getParquetCacheMaxEntries());
    }

    /**
     * Acquire an entry for {@code path}, opening + mmaping + parsing the
     * footer on miss. Callers MUST call {@link #release(Entry)} exactly
     * once for every successful acquire. The returned entry's fd, mmap
     * address, and decoder are valid until the matching release; under
     * the entry-level refcount the cache may keep them alive longer for
     * reuse by other acquires.
     */
    public Entry acquire(Utf8Sequence path, FilesFacade ff) {
        synchronized (lock) {
            if (closed) {
                throw CairoException.nonCritical().put("parquet file cache is closed");
            }
            final int idx = entries.keyIndex(path);
            if (idx < 0) {
                // Stat-validate before serving a hit. Catches the case where a
                // caller rewrote the file on disk while the previous holder
                // released it. Path-keyed lookup alone would silently serve
                // stale bytes - the same path against new contents - and a
                // downstream schema check would happily accept the old footer.
                // One syscall per hit; cheap relative to the saved open+mmap+
                // parse cost on every cursor reuse.
                final Entry hit = entries.valueAt(idx);
                try (Path callPath = new Path(MemoryTag.NATIVE_PATH)) {
                    callPath.of(path);
                    if (ff.length(callPath.$()) == hit.mapSize) {
                        hit.refcount++;
                        touchLru(hit);
                        return hit;
                    }
                }
                // Size mismatch: invalidate the cached entry so subsequent
                // acquires re-open. If no holder is using it, free now. If
                // holders are still reading the old bytes (snapshot view),
                // mark it orphaned via inCache=false and let the final
                // release() free it - currentBytes was already adjusted here
                // so release() must not adjust again.
                invalidate(hit);
                // Fall through to open fresh.
            }
            // Miss: open before evicting. If open fails we never alter
            // cache state - safer than evicting first and finding out we
            // could not open the new file anyway. Allocate a per-call
            // Path - acquire is called at most once per file-open, so the
            // alloc churn is negligible and avoids a cache-resident
            // native-path buffer that would grow with the longest path
            // ever seen.
            final long fd;
            long addr = 0;
            long size = 0;
            ParquetFileDecoder decoder = null;
            try (Path callPath = new Path(MemoryTag.NATIVE_PATH)) {
                callPath.of(path);
                fd = TableUtils.openRO(ff, callPath.$(), LOG);
            }
            try {
                size = ff.length(fd);
                addr = TableUtils.mapRO(ff, fd, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                decoder = new ParquetFileDecoder();
                decoder.of(addr, size, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            } catch (Throwable th) {
                if (addr != 0) {
                    ff.munmap(addr, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                }
                ff.close(fd);
                Misc.free(decoder);
                throw th;
            }
            final Entry fresh = new Entry(ff, Utf8String.newInstance(path), fd, addr, size, decoder);
            fresh.refcount = 1;
            fresh.inCache = true;
            // Make room: walk LRU and free refcount=0 entries until the
            // budget would cover this new entry. We always insert even if
            // we cannot free enough - the alternative is to fail the
            // acquire, and a temporarily-over-budget cache is preferable
            // to a hard failure. Pressure resolves when a holder releases.
            evictRefcountZeroWhileOver(size);
            // Re-fetch keyIndex - the invalidate-on-size-mismatch path above
            // may have removed the previous entry from the map, in which case
            // the `idx` captured at the top is no longer valid for putAt.
            entries.put(fresh.path, fresh);
            lru.add(fresh);
            currentBytes += size;
            return fresh;
        }
    }

    /**
     * Evict every entry whose refcount is zero. Intended for test teardown
     * and operator-triggered cache reclaim; refcount &gt; 0 entries stay because
     * a holder is still using them. Returns the number of entries freed.
     */
    public int evictAllReleased() {
        synchronized (lock) {
            int freed = 0;
            for (int i = 0; i < lru.size(); ) {
                final Entry e = lru.getQuick(i);
                if (e.refcount == 0) {
                    lru.remove(i);
                    entries.remove(e.path);
                    currentBytes -= e.mapSize;
                    e.inCache = false;
                    e.freeResources();
                    freed++;
                } else {
                    i++;
                }
            }
            return freed;
        }
    }

    public long getCurrentBytes() {
        synchronized (lock) {
            return currentBytes;
        }
    }

    public int getEntryCount() {
        synchronized (lock) {
            return lru.size();
        }
    }

    /**
     * Release a previously-acquired entry. After this call the caller
     * must not touch the entry's fd, mmap address, or decoder - the cache
     * may evict it.
     */
    public void release(Entry entry) {
        if (entry == null) {
            return;
        }
        synchronized (lock) {
            if (entry.refcount <= 0) {
                throw CairoException.critical(0)
                        .put("parquet file cache: release on unacquired entry [path=")
                        .put(entry.path).put(']');
            }
            entry.refcount--;
            if (entry.refcount == 0) {
                if (!entry.inCache) {
                    // Orphaned by invalidate(); the cache's bookkeeping was
                    // already updated when the entry left the map. Just free
                    // the resources.
                    entry.freeResources();
                } else if (currentBytes > maxBytes || lru.size() > maxEntries) {
                    evictOne(entry);
                }
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            // Free every entry regardless of refcount: at engine shutdown
            // any lingering holders are a bug that we cannot fix from here,
            // and leaking fd / mmap on shutdown is worse than a UAF on a
            // holder that should have closed first. Log if we find any.
            for (int i = 0, n = lru.size(); i < n; i++) {
                Entry e = lru.getQuick(i);
                if (e.refcount > 0) {
                    LOG.error().$("parquet file cache: entry held at shutdown [path=").$(e.path)
                            .$(", refcount=").$(e.refcount).I$();
                }
                e.freeResources();
            }
            lru.clear();
            entries.clear();
            currentBytes = 0;
        }
    }

    private void evictOne(Entry e) {
        // Caller holds lock and has verified refcount == 0. Find the entry's
        // LRU position by linear scan; the list is bounded by maxEntries so
        // this is cheap.
        for (int i = 0, n = lru.size(); i < n; i++) {
            if (lru.getQuick(i) == e) {
                lru.remove(i);
                break;
            }
        }
        entries.remove(e.path);
        currentBytes -= e.mapSize;
        e.inCache = false;
        e.freeResources();
    }

    /**
     * Drop {@code e} from the indexed cache state. If no holder remains
     * (refcount == 0) the resources are freed immediately; otherwise the
     * entry is marked {@code inCache=false} so the final {@link #release}
     * frees it. Either way the cache's byte counter is debited here.
     */
    private void invalidate(Entry e) {
        entries.remove(e.path);
        for (int i = 0, n = lru.size(); i < n; i++) {
            if (lru.getQuick(i) == e) {
                lru.remove(i);
                break;
            }
        }
        currentBytes -= e.mapSize;
        e.inCache = false;
        if (e.refcount == 0) {
            e.freeResources();
        }
    }

    private void evictRefcountZeroWhileOver(long incomingSize) {
        // Walk LRU from oldest forward. Stop when either the new entry
        // would fit AND we have entry-count headroom, or we cannot find
        // another refcount=0 candidate. Refcount > 0 entries are pinned
        // and we step past them without touching position in LRU.
        int i = 0;
        while (i < lru.size()
                && (currentBytes + incomingSize > maxBytes || lru.size() >= maxEntries)) {
            final Entry victim = lru.getQuick(i);
            if (victim.refcount == 0) {
                lru.remove(i);
                entries.remove(victim.path);
                currentBytes -= victim.mapSize;
                victim.inCache = false;
                victim.freeResources();
                // Don't advance i: removing shifted the list left.
            } else {
                i++;
            }
        }
    }

    private void touchLru(Entry e) {
        // Move to tail. The list is bounded by maxEntries (defaults to ~4096)
        // so a linear scan is fine; the alternative LinkedHashSet brings GC
        // pressure and a node per entry.
        final int n = lru.size();
        for (int i = 0; i < n; i++) {
            if (lru.getQuick(i) == e) {
                if (i != n - 1) {
                    lru.remove(i);
                    lru.add(e);
                }
                return;
            }
        }
    }

    public static final class Entry {
        public final ParquetFileDecoder decoder;
        public final long fd;
        public final FilesFacade ff;
        public final long mapAddr;
        public final long mapSize;
        public final Utf8String path;
        // True while this entry is still indexed by the cache. Cleared when
        // the entry is removed from the path map (either evicted under
        // pressure or invalidated because the underlying file changed) -
        // release() then knows to free immediately instead of asking the
        // cache to evict, which would double-decrement currentBytes and
        // touch lists the entry is no longer in.
        boolean inCache;
        int refcount; // guarded by ParquetFileCache.lock

        Entry(FilesFacade ff, Utf8String path, long fd, long mapAddr, long mapSize, ParquetFileDecoder decoder) {
            this.ff = ff;
            this.path = path;
            this.fd = fd;
            this.mapAddr = mapAddr;
            this.mapSize = mapSize;
            this.decoder = decoder;
        }

        void freeResources() {
            Misc.free(decoder);
            if (mapAddr != 0) {
                ff.munmap(mapAddr, mapSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            if (fd != -1) {
                ff.close(fd);
            }
        }
    }
}
