/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Unsafe;

import java.util.Arrays;
import java.util.Map;

public class ReaderPool extends AbstractPool implements ResourcePool<TableReader> {

    private static final Log LOG = LogFactory.getLog(ReaderPool.class);
    private static final long UNLOCKED = -1L;
    private static final long NEXT_STATUS = Unsafe.getFieldOffset(Entry.class, "nextStatus");
    private static final int ENTRY_SIZE = 32;
    private static final long LOCK_OWNER = Unsafe.getFieldOffset(Entry.class, "lockOwner");
    private static final int NEXT_OPEN = 0;
    private static final int NEXT_ALLOCATED = 1;
    private static final int NEXT_LOCKED = 2;
    private final ConcurrentHashMap<Entry> entries = new ConcurrentHashMap<>();
    private final int maxSegments;
    private final int maxEntries;

    public ReaderPool(CairoConfiguration configuration) {
        super(configuration, configuration.getInactiveReaderTTL());
        this.maxSegments = configuration.getReaderPoolMaxSegments();
        this.maxEntries = maxSegments * ENTRY_SIZE;
    }

    @Override
    public TableReader get(CharSequence name) {

        Entry e = getEntry(name);

        long lockOwner = e.lockOwner;
        long thread = Thread.currentThread().getId();

        if (lockOwner != UNLOCKED) {
            LOG.info().$('\'').utf8(name).$("' is locked [owner=").$(lockOwner).$(']').$();
            throw EntryLockedException.instance("unknown");
        }

        do {
            for (int i = 0; i < ENTRY_SIZE; i++) {
                if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                    // got lock, allocate if needed
                    R r = e.readers[i];
                    if (r == null) {
                        try {
                            LOG.info()
                                    .$("open '").utf8(name)
                                    .$("' [at=").$(e.index).$(':').$(i)
                                    .$(']').$();
                            r = new R(this, e, i, name);
                        } catch (CairoException ex) {
                            Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                            throw ex;
                        }

                        e.readers[i] = r;
                        notifyListener(thread, name, PoolListener.EV_CREATE, e.index, i);
                    } else {
                        r.goActive();
                        notifyListener(thread, name, PoolListener.EV_GET, e.index, i);
                    }

                    if (isClosed()) {
                        e.readers[i] = null;
                        r.goodby();
                        LOG.info().$('\'').utf8(name).$("' born free").$();
                        return r;
                    }

                    LOG.debug().$('\'').utf8(name).$("' is assigned [at=").$(e.index).$(':').$(i).$(", thread=").$(thread).$(']').$();
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
        notifyListener(thread, name, PoolListener.EV_FULL, -1, -1);
        LOG.info().$("could not get, busy [table=`").utf8(name).$("`, thread=").$(thread).$(", retries=").$(this.maxSegments).$(']').$();
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
            throw CairoException.instance(0).put("Not the lock owner of ").put(name);
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

        for (Map.Entry<CharSequence, Entry> me : entries.entrySet()) {

            Entry e = me.getValue();

            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    R r;
                    if (deadline > Unsafe.arrayGetVolatile(e.releaseTimes, i) && (r = e.readers[i]) != null) {
                        if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                            // check if deadline violation still holds
                            if (deadline > e.releaseTimes[i]) {
                                removed = true;
                                closeReader(thread, e, i, PoolListener.EV_EXPIRE, closeReason);
                            }
                            Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                        } else {
                            casFailures++;

                            if (deadline == Long.MAX_VALUE) {
                                r.goodby();
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
        // when we closing pool, the result is true when pool is empty
        if (closeReason == PoolConstants.CR_IDLE) {
            return removed;
        } else {
            return casFailures == 0;
        }
    }

    private void closeReader(long thread, Entry entry, int index, short ev, int reason) {
        R r = entry.readers[index];
        if (r != null) {
            r.goodby();
            r.close();
            LOG.info().$("closed '").$(r.getTableName()).$("' [at=").$(entry.index).$(':').$(index).$(", reason=").$(PoolConstants.closeReasonText(reason)).$(']').$();
            notifyListener(thread, r.getTableName(), ev, entry.index, index);
            entry.readers[index] = null;
        }
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

    private void notifyListener(long thread, CharSequence name, short event, int segment, int position) {
        PoolListener listener = getPoolListener();
        if (listener != null) {
            listener.onEvent(PoolListener.SRC_READER, thread, name, event, (short) segment, (short) position);
        }
    }

    private boolean returnToPool(R reader) {
        CharSequence name = reader.getTableName();

        long thread = Thread.currentThread().getId();

        int index = reader.index;
        final ReaderPool.Entry e = reader.entry;
        if (e == null) {
            return false;
        }

        if (Unsafe.arrayGetVolatile(e.allocations, index) != UNALLOCATED) {

            LOG.debug().$('\'').$(name).$("' is back [at=").$(e.index).$(':').$(index).$(", thread=").$(thread).$(']').$();
            notifyListener(thread, name, PoolListener.EV_RETURN, e.index, index);

            e.releaseTimes[index] = clock.getTicks();
            Unsafe.arrayPutOrdered(e.allocations, index, UNALLOCATED);

            // todo: there is a race condition between this method and
            //   releaseAll() when the latter shuts down the pool. I thought of adding a version counter
            //   that each method will attempt to increment thus recognising race condition and
            //   taking action
            return true;
        }

        LOG.error().$('\'').$(name).$("' is available [at=").$(e.index).$(':').$(index).$(']').$();
        return true;

    }

    public static class Entry {
        final long[] allocations = new long[ENTRY_SIZE];
        final long[] releaseTimes = new long[ENTRY_SIZE];
        final R[] readers = new R[ENTRY_SIZE];
        final int index;
        volatile long lockOwner = -1L;
        @SuppressWarnings("unused")
        int nextStatus = 0;
        volatile Entry next;

        public Entry(int index, long currentMicros) {
            this.index = index;
            Arrays.fill(allocations, UNALLOCATED);
            Arrays.fill(releaseTimes, currentMicros);
        }
    }

    public static class R extends TableReader {
        private final int index;
        private ReaderPool pool;
        private Entry entry;

        public R(ReaderPool pool, Entry entry, int index, CharSequence name) {
            super(pool.getConfiguration(), name);
            this.pool = pool;
            this.entry = entry;
            this.index = index;
        }

        @Override
        public void close() {
            if (isOpen()) {
                goPassive();
                if (pool != null && entry != null && pool.returnToPool(this)) {
                    return;
                }
                super.close();
            }
        }

        private void goodby() {
            entry = null;
            pool = null;
        }
    }
}