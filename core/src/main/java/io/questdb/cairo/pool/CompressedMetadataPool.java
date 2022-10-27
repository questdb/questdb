/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo.pool;

import io.questdb.cairo.*;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Map;

public class CompressedMetadataPool extends AbstractPool implements ResourcePool<TableRecordMetadata> {

    public static final int ENTRY_SIZE = 32;
    private static final Log LOG = LogFactory.getLog(CompressedMetadataPool.class);
    private static final long UNLOCKED = -1L;
    private static final long NEXT_STATUS = Unsafe.getFieldOffset(Entry.class, "nextStatus");
    private static final long LOCK_OWNER = Unsafe.getFieldOffset(Entry.class, "lockOwner");
    private static final int NEXT_OPEN = 0;
    private static final int NEXT_ALLOCATED = 1;
    private static final int NEXT_LOCKED = 2;
    private final ConcurrentHashMap<Entry> entries = new ConcurrentHashMap<>();
    private final int maxSegments;
    private final int maxEntries;
    private final TableSequencerAPI tableSequencerAPI;

    public CompressedMetadataPool(CairoConfiguration configuration, TableSequencerAPI tableSequencerAPI) {
        super(configuration, configuration.getInactiveReaderTTL());
        this.maxSegments = configuration.getReaderPoolMaxSegments();
        this.maxEntries = maxSegments * ENTRY_SIZE;
        this.tableSequencerAPI = tableSequencerAPI;
    }

    @Override
    public TableRecordMetadata get(CharSequence tableName) {

        Entry e = getEntry(tableName);

        long lockOwner = e.lockOwner;
        long thread = Thread.currentThread().getId();

        if (lockOwner != UNLOCKED) {
            LOG.info().$('\'').utf8(tableName).$("' is locked [owner=").$(lockOwner).$(']').$();
            throw EntryLockedException.instance("unknown");
        }

        do {
            for (int i = 0; i < ENTRY_SIZE; i++) {
                if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                    Unsafe.arrayPutOrdered(e.releaseOrAcquireTimes, i, clock.getTicks());
                    // got lock, allocate if needed
                    R1 r = e.getReader(i);
                    if (r == null) {
                        try {
                            LOG.info()
                                    .$("open '").utf8(tableName)
                                    .$("' [at=").$(e.index).$(':').$(i)
                                    .$(']').$();
                            r = newMetadataInstance(tableName, e, i);
                        } catch (CairoException ex) {
                            Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                            throw ex;
                        }

                        e.setReader(i, r);
                        notifyListener(thread, tableName, PoolListener.EV_CREATE, e.index, i);
                    } else {
                        notifyListener(thread, tableName, PoolListener.EV_GET, e.index, i);
                    }

                    r.tryReload();

                    if (isClosed()) {
                        e.setReader(i, null);
                        LOG.info().$('\'').utf8(tableName).$("' born free").$();
                        return r;
                    }
                    LOG.debug().$('\'').utf8(tableName).$("' is assigned [at=").$(e.index).$(':').$(i).$(", thread=").$(thread).$(']').$();
                    return r;
                }
            }

            LOG.debug().$("Thread ").$(thread).$(" is moving to entry ").$(e.index + 1).$();

            // all allocated, create next entry if possible
            if (Unsafe.getUnsafe().compareAndSwapInt(e, NEXT_STATUS, NEXT_OPEN, NEXT_ALLOCATED)) {
                LOG.debug().$("Thread ").$(thread).$(" allocated entry ").$(e.index + 1).$();
                e.next = new Entry(e.index + 1, clock.getTicks());
            }
            e = e.next;
        } while (e != null && e.index < maxSegments);

        // max entries exceeded
        notifyListener(thread, tableName, PoolListener.EV_FULL, -1, -1);
        LOG.info().$("could not get, busy [table=`").utf8(tableName).$("`, thread=").$(thread).$(", retries=").$(this.maxSegments).$(']').$();
        throw EntryUnavailableException.instance("unknown");
    }

    public int getBusyCount() {
        int count = 0;
        for (Map.Entry<CharSequence, Entry> me : entries.entrySet()) {
            Entry e = me.getValue();
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (Unsafe.arrayGetVolatile(e.allocations, i) != UNALLOCATED && e.readers[i] != null) {
                        count++;
                    }
                }
                e = e.next;
            } while (e != null);
        }
        return count;
    }

    public int getMaxEntries() {
        return maxEntries;
    }

    public boolean lock(CharSequence name) {
        Entry e = getEntry(name);
        final long thread = Thread.currentThread().getId();
        if (Unsafe.cas(e, LOCK_OWNER, UNLOCKED, thread) || Unsafe.cas(e, LOCK_OWNER, thread, thread)) {
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                        closeReader(thread, e, i, PoolListener.EV_LOCK_CLOSE, PoolConstants.CR_NAME_LOCK);
                    } else if (Unsafe.cas(e.allocations, i, thread, thread)) {
                        // same thread, don't need to order reads
                        if (e.readers[i] != null) {
                            // this thread has busy reader, it should close first
                            e.lockOwner = -1L;
                            return false;
                        }
                    } else {
                        LOG.info().$("could not lock, busy [table=`").utf8(name).$("`, at=").$(e.index).$(':').$(i).$(", owner=").$(e.allocations[i]).$(", thread=").$(thread).$(']').$();
                        e.lockOwner = -1L;
                        return false;
                    }
                }

                // prevent new entries from being created
                if (e.next == null && Unsafe.getUnsafe().compareAndSwapInt(e, NEXT_STATUS, NEXT_OPEN, NEXT_LOCKED)) {
                    break;
                }

                e = e.next;
            } while (e != null);
        } else {
            LOG.error().$("' already locked [table=`").utf8(name).$("`, owner=").$(e.lockOwner).$(']').$();
            notifyListener(thread, name, PoolListener.EV_LOCK_BUSY, -1, -1);
            return false;
        }
        notifyListener(thread, name, PoolListener.EV_LOCK_SUCCESS, -1, -1);
        LOG.debug().$("locked [table=`").utf8(name).$("`, thread=").$(thread).$(']').$();
        return true;
    }

    public void unlock(CharSequence name) {
        Entry e = entries.get(name);
        long thread = Thread.currentThread().getId();
        if (e == null) {
            LOG.info().$("not found, cannot unlock [table=`").$(name).$("`]").$();
            notifyListener(thread, name, PoolListener.EV_NOT_LOCKED, -1, -1);
            return;
        }

        if (e.lockOwner == thread) {
            entries.remove(name);
            while (e != null) {
                e = e.next;
            }
        } else {
            notifyListener(thread, name, PoolListener.EV_NOT_LOCK_OWNER);
            throw CairoException.nonCritical().put("Not the lock owner of ").put(name);
        }

        notifyListener(thread, name, PoolListener.EV_UNLOCKED, -1, -1);
        LOG.debug().$("unlocked [table=`").utf8(name).$("`]").$();
    }

    private void checkClosed() {
        if (isClosed()) {
            LOG.info().$("is closed").$();
            throw PoolClosedException.INSTANCE;
        }
    }

    @Override
    protected void closePool() {
        super.closePool();
        LOG.info().$("closed").$();
    }

    @Override
    protected boolean releaseAll(long deadline) {
        long thread = Thread.currentThread().getId();
        boolean removed = false;
        int casFailures = 0;
        int closeReason = deadline < Long.MAX_VALUE ? PoolConstants.CR_IDLE : PoolConstants.CR_POOL_CLOSE;

        for (Entry e : entries.values()) {
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    R1 r;
                    if (deadline > Unsafe.arrayGetVolatile(e.releaseOrAcquireTimes, i) && (r = e.getReader(i)) != null) {
                        if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                            // check if deadline violation still holds
                            if (deadline > e.releaseOrAcquireTimes[i]) {
                                removed = true;
                                closeReader(thread, e, i, PoolListener.EV_EXPIRE, closeReason);
                            }
                            Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                        } else {
                            casFailures++;
                            if (deadline == Long.MAX_VALUE) {
                                r.goodbye();
                                LOG.info().$("shutting down. '").$(r.getTableName()).$("' is left behind").$();
                            }
                        }
                    }
                }
                // this does not release the next
                e = e.next;
            } while (e != null);
        }

        // when we are timing out entries the result is "true" if there was any work done
        // when we're closing pool, the result is true when pool is empty
        if (closeReason == PoolConstants.CR_IDLE) {
            return removed;
        } else {
            return casFailures == 0;
        }
    }

    private void closeReader(long thread, Entry entry, int index, short ev, int reason) {
        R1 r = entry.getReader(index);
        if (r != null) {
            r.goodbye();
            r.close();
            LOG.info().$("closed '").utf8(r.getTableName())
                    .$("' [at=").$(entry.index).$(':').$(index)
                    .$(", reason=").$(PoolConstants.closeReasonText(reason))
                    .I$();
            notifyListener(thread, r.getTableName(), ev, entry.index, index);
            entry.setReader(index, null);
        }
    }

    Map<CharSequence, Entry> entries() {
        return entries;
    }

    private Entry getEntry(CharSequence name) {
        checkClosed();

        Entry e = entries.get(name);
        if (e == null) {
            e = new Entry(0, clock.getTicks());
            Entry other = entries.putIfAbsent(name, e);
            if (other != null) {
                e = other;
            }
        }
        return e;
    }

    @NotNull
    private R1 newMetadataInstance(CharSequence tableName, Entry e, int entryIndex) {
        if (tableSequencerAPI.hasSequencer(tableName)) {
            R r = new R(this, e, entryIndex);
            tableSequencerAPI.getTableMetadata(tableName, r, true);
            return r;
        }
        return new LegacyR(this, e, entryIndex, tableName);
    }

    private void notifyListener(long thread, CharSequence name, short event, int segment, int position) {
        PoolListener listener = getPoolListener();
        if (listener != null) {
            listener.onEvent(PoolListener.SRC_READER, thread, name, event, (short) segment, (short) position);
        }
    }

    private boolean returnToPool(R1 r) {
        CharSequence name = r.getTableName();

        long thread = Thread.currentThread().getId();

        int index = r.getIndex();
        final CompressedMetadataPool.Entry e = r.getEntry();
        if (e == null) {
            return false;
        }

        final long owner = Unsafe.arrayGetVolatile(e.allocations, index);
        if (owner != UNALLOCATED) {

            LOG.debug().$('\'').$(name).$("' is back [at=").$(e.index).$(':').$(index).$(", thread=").$(thread).$(']').$();
            notifyListener(thread, name, PoolListener.EV_RETURN, e.index, index);

            // release the entry for anyone to pick up
            e.releaseOrAcquireTimes[index] = clock.getTicks();
            Unsafe.arrayPutOrdered(e.allocations, index, UNALLOCATED);
            final boolean closed = isClosed();

            // When pool is closed we will race against release thread
            // to release our entry. No need to bother releasing map entry, pool is going down.
            return !closed || !Unsafe.cas(e.allocations, index, UNALLOCATED, owner);
        }

        throw CairoException.critical(0).put("double close [table=").put(name).put(", index=").put(index).put(']');
    }

    private interface R1 extends TableRecordMetadata {

        Entry getEntry();

        int getIndex();

        CompressedMetadataPool getPool();

        void goodbye();

        void tryReload();
    }

    private static final class Entry {
        final long[] allocations = new long[ENTRY_SIZE];
        final long[] releaseOrAcquireTimes = new long[ENTRY_SIZE];
        final Object[] readers = new Object[ENTRY_SIZE];
        final int index;
        volatile long lockOwner = -1L;
        @SuppressWarnings("unused")
        int nextStatus = 0;
        volatile Entry next;

        public Entry(int index, long currentMicros) {
            this.index = index;
            Arrays.fill(allocations, UNALLOCATED);
            Arrays.fill(releaseOrAcquireTimes, currentMicros);
        }

        Entry getNext() {
            return next;
        }

        long getOwnerVolatile(int pos) {
            return Unsafe.arrayGetVolatile(allocations, pos);
        }

        R1 getReader(int pos) {
            return (R1) readers[pos];
        }

        long getReleaseOrAcquireTime(int pos) {
            return releaseOrAcquireTimes[pos];
        }

        void setReader(int pos, R1 reader) {
            readers[pos] = reader;
        }
    }

    private static class LegacyR extends DynamicTableReaderMetadata implements R1 {
        private final int index;
        private CompressedMetadataPool pool;
        private Entry entry;

        public LegacyR(CompressedMetadataPool pool, Entry entry, int index, CharSequence name) {
            super(pool.getConfiguration(), Chars.toString(name));
            load();
            this.pool = pool;
            this.entry = entry;
            this.index = index;
        }

        @Override
        public void close() {
            final CompressedMetadataPool pool = getPool();
            if (pool != null && getEntry() != null) {
                if (pool.returnToPool(this)) {
                    return;
                }
            }
            super.close();
        }

        @Override
        public Entry getEntry() {
            return entry;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public CompressedMetadataPool getPool() {
            return pool;
        }

        public void goodbye() {
            entry = null;
            pool = null;
        }

        @Override
        public void tryReload() {
            reload();
        }
    }

    private static class R extends GenericTableRecordMetadata implements R1 {
        private final int index;
        private CompressedMetadataPool pool;
        private Entry entry;

        public R(CompressedMetadataPool pool, Entry entry, int index) {
            this.pool = pool;
            this.entry = entry;
            this.index = index;
        }

        @Override
        public void close() {
            final CompressedMetadataPool pool = getPool();
            if (pool != null && getEntry() != null) {
                if (pool.returnToPool(this)) {
                    return;
                }
            }
            super.close();
        }

        @Override
        public Entry getEntry() {
            return entry;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public CompressedMetadataPool getPool() {
            return pool;
        }

        public void goodbye() {
            entry = null;
            pool = null;
        }

        @Override
        public void tryReload() {
            pool.tableSequencerAPI.reloadMetadataConditionally(getTableName(), getStructureVersion(), this, true);
        }
    }
}