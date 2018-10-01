/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.store.factory;

import com.questdb.ex.*;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.Unsafe;
import com.questdb.std.ex.JournalException;
import com.questdb.store.Journal;
import com.questdb.store.PoolConstants;
import com.questdb.store.factory.configuration.JournalConfiguration;
import com.questdb.store.factory.configuration.JournalMetadata;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CachingReaderFactory extends AbstractFactory implements JournalCloseInterceptor {

    private static final long CLOSED = Unsafe.getFieldOffset(CachingReaderFactory.class, "closed");
    private static final Log LOG = LogFactory.getLog(CachingReaderFactory.class);
    private static final long UNLOCKED = -1L;
    private static final long NEXT_STATUS = Unsafe.getFieldOffset(Entry.class, "nextStatus");
    private static final int ENTRY_SIZE = 32;
    private static final int TRUE = 1;
    private static final int FALSE = 0;
    private static final long LOCK_OWNER = Unsafe.getFieldOffset(Entry.class, "lockOwner");
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final int maxSegments;
    private final int maxEntries;
    private volatile int closed = FALSE;

    public CachingReaderFactory(String databaseHome, long inactiveTtl, int maxSegments) {
        super(databaseHome, inactiveTtl);
        this.maxSegments = maxSegments;
        this.maxEntries = maxSegments * ENTRY_SIZE;
    }

    public CachingReaderFactory(JournalConfiguration configuration, long inactiveTtl, int maxSegments) {
        super(configuration, inactiveTtl);
        this.maxSegments = maxSegments;
        this.maxEntries = maxSegments * ENTRY_SIZE;
    }

    @Override
    public boolean canClose(Journal journal) {
        String name = journal.getName();

        if (journal instanceof R) {
            long thread = Thread.currentThread().getId();

            Entry e = entries.get(name);
            if (e == null) {
                LOG.error().$("Reader '").$(name).$("' is not managed by this pool").$();
                notifyListener(thread, name, FactoryEventListener.EV_NOT_IN_POOL, (short) -1, (short) -1);
                return true;
            }

            R r = (R) journal;

            if (Unsafe.arrayGetVolatile(r.entry.allocations, r.index) != PoolConstants.UNALLOCATED) {

                if (closed == TRUE) {
                    // keep locked and close
                    Unsafe.arrayPut(r.entry.readers, r.index, null);
                    notifyListener(thread, name, FactoryEventListener.EV_OUT_OF_POOL_CLOSE, r.entry.index, r.index);
                    return true;
                }

                Unsafe.arrayPut(r.entry.releaseTimes, r.index, System.currentTimeMillis());
                Unsafe.arrayPutOrdered(r.entry.allocations, r.index, PoolConstants.UNALLOCATED);

                LOG.info().$("Thread ").$(thread).$(" released reader '").$(name).$("' (").$(r.entry.index).$(',').$(r.index).$(')').$();
                notifyListener(thread, name, FactoryEventListener.EV_RETURN, r.entry.index, r.index);
                return false;
            }
            LOG.error().$("Thread ").$(thread).$(" attempts to release unallocated reader '").$(name).$("' ").$(r.entry.index).$(',').$(r.index).$();
        } else {
            LOG.error().$("Internal error. Closing foreign reader: ").$(name).$();
        }
        return true;
    }

    @Override
    public void close() {
        if (Unsafe.getUnsafe().compareAndSwapInt(this, CLOSED, FALSE, TRUE)) {
            releaseAll(Long.MAX_VALUE);
        }
    }

    public int getMaxEntries() {
        return maxEntries;
    }

    public int getBusyCount() {
        int count = 0;
        for (Map.Entry<String, Entry> me : entries.entrySet()) {
            Entry e = me.getValue();
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (Unsafe.arrayGetVolatile(e.allocations, i) != PoolConstants.UNALLOCATED && Unsafe.arrayGet(e.readers, i) != null) {
                        count++;
                    }
                }
                e = e.next;
            } while (e != null);
        }
        return count;
    }

    public void unlock(String name) {
        Entry e = entries.get(name);
        long thread = Thread.currentThread().getId();
        if (e == null) {
            LOG.info().$("Reader '").$(name).$("' does not exist. Nothing to unlock.").$();
            notifyListener(thread, name, FactoryEventListener.EV_NOT_LOCKED, -1, -1);
            return;
        }

        if (e.lockOwner == thread) {
            entries.remove(name);
        }
        notifyListener(thread, name, FactoryEventListener.EV_UNLOCKED, -1, -1);
        LOG.info().$("Reader '").$(name).$("' is unlocked").$();
    }

    public void lock(String name) throws JournalException {

        Entry e = entries.get(name);
        if (e == null) {
            LOG.info().$("Reader '").$(name).$("' doesn't exist. Nothing to lock.").$();
            return;
        }

        long thread = Thread.currentThread().getId();

        if (Unsafe.cas(e, LOCK_OWNER, UNLOCKED, thread) || Unsafe.cas(e, LOCK_OWNER, thread, thread)) {
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (Unsafe.cas(e.allocations, i, PoolConstants.UNALLOCATED, thread)) {
                        closeReader(thread, e, i, FactoryEventListener.EV_LOCK_CLOSE, FactoryEventListener.EV_LOCK_CLOSE_EX, PoolConstants.CR_NAME_LOCK);
                    } else if (Unsafe.arrayGet(e.allocations, i) != thread || Unsafe.arrayGet(e.readers, i) != null) {
                        LOG.info().$("Reader '").$(name).$("' is partially locked by ").$(e.lockOwner).$();
                        throw RetryLockException.INSTANCE;
                    }
                }
                e = e.next;
            } while (e != null);
        } else {
            LOG.error().$("Reader '").$(name).$("' is already locked by ").$(e.lockOwner).$();
            notifyListener(thread, name, FactoryEventListener.EV_LOCK_BUSY, -1, -1);
            throw JournalLockedException.INSTANCE;
        }
        notifyListener(thread, name, FactoryEventListener.EV_LOCK_SUCCESS, -1, -1);
        LOG.info().$("Reader '").$(name).$("' is locked").$();
    }

    private void notifyListener(long thread, String name, short event, int segment, int position) {
        FactoryEventListener listener = getEventListener();
        if (listener != null) {
            listener.onEvent(FactoryEventListener.SRC_READER, thread, name, event, (short) segment, (short) position);
        }
    }

    private void closeReader(long thread, Entry e, int index, short ev, short evex, int reason) {
        R r = Unsafe.arrayGet(e.readers, index);
        if (r != null) {
            try {
                r.setCloseInterceptor(null);
                r.close();
                LOG.info().$("Closed reader '").$(r.getName()).$("' (").$(e.index).$(',').$(index).$(") ").$(PoolConstants.closeReasonText(reason)).$();
                notifyListener(thread, r.getName(), ev, e.index, index);
            } catch (Throwable e1) {
                LOG.error().$("Cannot close reader '").$(r.getName()).$("' (").$(e.index).$(',').$(index).$(") ").$(PoolConstants.closeReasonText(reason)).$(e1.getMessage()).$();
                notifyListener(thread, r.getName(), evex, e.index, index);
            }
            Unsafe.arrayPut(e.readers, index, null);
        }
    }

    @Override
    protected boolean releaseAll(long deadline) {
        long thread = Thread.currentThread().getId();
        boolean removed = false;
        int closeReason = deadline < Long.MAX_VALUE ? PoolConstants.CR_IDLE : PoolConstants.CR_POOL_CLOSE;

        for (Map.Entry<String, Entry> me : entries.entrySet()) {

            Entry e = me.getValue();

            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (deadline > Unsafe.arrayGetVolatile(e.releaseTimes, i) && Unsafe.arrayGet(e.readers, i) != null) {
                        if (Unsafe.cas(e.allocations, i, PoolConstants.UNALLOCATED, thread)) {
                            // check if deadline violation still holds
                            if (deadline > Unsafe.arrayGet(e.releaseTimes, i)) {
                                removed = true;
                                closeReader(thread, e, i, FactoryEventListener.EV_EXPIRE, FactoryEventListener.EV_EXPIRE_EX, closeReason);
                            }
                            Unsafe.arrayPutOrdered(e.allocations, i, PoolConstants.UNALLOCATED);
                        }
                    }
                }
                e = e.next;
            } while (e != null);
        }
        return removed;
    }

    @SuppressWarnings("unchecked")
    <T> Journal<T> reader(JournalMetadata<T> metadata) throws JournalException {
        if (closed == TRUE) {
            LOG.info().$("Pool is closed");
            throw FactoryClosedException.INSTANCE;
        }

        String name = metadata.getName();
        Entry e = entries.get(name);

        long thread = Thread.currentThread().getId();

        if (e == null) {
            LOG.info().$("Thread ").$(thread).$(" is racing to create first entry for '").$(name).$('\'').$();
            e = new Entry(0);
            Entry other = entries.putIfAbsent(name, e);
            if (other != null) {
                e = other;
                LOG.info().$("Thread ").$(thread).$(" LOST the race to create first entry for '").$(name).$('\'').$();
            } else {
                // existence check
                if (getConfiguration().exists(name) != JournalConfiguration.EXISTS) {
                    LOG.info().$("Reader '").$(name).$("' does not exist").$();
                    throw JournalDoesNotExistException.INSTANCE;
                }
                LOG.info().$("Thread ").$(thread).$(" WON the race to create first entry for '").$(name).$('\'').$();
            }
        }

        long lockOwner = e.lockOwner;

        if (lockOwner != UNLOCKED) {
            LOG.info().$("Reader '").$(name).$("' is locked by ").$(lockOwner).$();
            throw JournalLockedException.INSTANCE;
        }

        do {
            for (int i = 0; i < ENTRY_SIZE; i++) {
                if (Unsafe.cas(e.allocations, i, PoolConstants.UNALLOCATED, thread)) {
                    // got lock, allocate if needed
                    R r = Unsafe.arrayGet(e.readers, i);
                    if (r == null) {
                        LOG.info().$("Thread ").$(thread).$(" created new reader '").$(name).$("' (").$(e.index).$(',').$(i).$(')').$();
                        r = new R(e, i, metadata, new File(getConfiguration().getJournalBase(), metadata.getName()));
                        notifyListener(thread, name, FactoryEventListener.EV_CREATE, e.index, i);
                        if (closed == TRUE) {
                            // don't assign interceptor or keep reference
                            return r;
                        }

                        Unsafe.arrayPut(e.readers, i, r);
                        r.setCloseInterceptor(this);
                    } else {
                        LOG.info().$("Thread ").$(thread).$(" allocated reader '").$(name).$("' (").$(e.index).$(',').$(i).$(')').$();
                        r.refresh();
                        notifyListener(thread, name, FactoryEventListener.EV_GET, e.index, i);
                    }

                    if (closed == TRUE) {
                        Unsafe.arrayPut(e.readers, i, null);
                        r.setCloseInterceptor(null);
                    }

                    return r;
                }
            }

            LOG.info().$("Thread ").$(thread).$(" is moving to entry ").$(e.index + 1).$();

            // all allocated, create next entry if possible

            if (Unsafe.getUnsafe().compareAndSwapInt(e, NEXT_STATUS, 0, 1)) {
                LOG.info().$("Thread ").$(thread).$(" allocated entry ").$(e.index + 1).$();
                e.next = new Entry(e.index + 1);
            }

            e = e.next;

        } while (e != null && e.index < maxSegments);

        // max entries exceeded
        notifyListener(thread, name, FactoryEventListener.EV_FULL, -1, -1);
        LOG.info().$("Thread ").$(thread).$(" cannot allocate reader. Max entries exceeded (").$(this.maxSegments).$(')').$();
        throw FactoryFullException.INSTANCE;
    }

    private static class Entry {
        final long[] allocations = new long[ENTRY_SIZE];
        final long[] releaseTimes = new long[ENTRY_SIZE];
        final R[] readers = new R[ENTRY_SIZE];
        final int index;
        volatile long lockOwner = -1L;
        @SuppressWarnings("unused")
        long nextStatus = 0;
        volatile Entry next;

        public Entry(int index) {
            this.index = index;
            Arrays.fill(allocations, PoolConstants.UNALLOCATED);
            Arrays.fill(releaseTimes, System.currentTimeMillis());
        }
    }

    public static class R<T> extends Journal<T> {
        private Entry entry;
        private int index;

        public R(Entry entry, int index, JournalMetadata<T> metadata, File location) throws JournalException {
            super(metadata, location);
            this.entry = entry;
            this.index = index;
        }
    }
}
