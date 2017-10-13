/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.cairo.pool;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.CairoException;
import com.questdb.cairo.FilesFacade;
import com.questdb.cairo.TableReader;
import com.questdb.cairo.pool.ex.EntryLockedException;
import com.questdb.cairo.pool.ex.EntryUnavailableException;
import com.questdb.cairo.pool.ex.PoolClosedException;
import com.questdb.factory.FactoryConstants;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Unsafe;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReaderPool extends AbstractPool {

    public static final long CLOSED = Unsafe.getFieldOffset(ReaderPool.class, "closed");
    private static final Log LOG = LogFactory.getLog(ReaderPool.class);
    private static final long UNLOCKED = -1L;
    private static final long NEXT_STATUS = Unsafe.getFieldOffset(Entry.class, "nextStatus");
    private static final int ENTRY_SIZE = 32;
    private static final int TRUE = 1;
    private static final int FALSE = 0;
    private static final long LOCK_OWNER = Unsafe.getFieldOffset(Entry.class, "lockOwner");
    private final ConcurrentHashMap<CharSequence, Entry> entries = new ConcurrentHashMap<>();
    private final int maxSegments;
    private final int maxEntries;
    private volatile int closed = FALSE;

    public ReaderPool(CairoConfiguration configuration) {
        super(configuration.getFilesFacade(), configuration.getRoot(), configuration.getInactiveReaderTTL());
        this.maxSegments = configuration.getReaderPoolSegments();
        this.maxEntries = maxSegments * ENTRY_SIZE;
    }

    @Override
    public void close() {
        if (Unsafe.getUnsafe().compareAndSwapInt(this, CLOSED, FALSE, TRUE)) {
            releaseAll(Long.MAX_VALUE);
        }
    }

    public int getBusyCount() {
        int count = 0;
        for (Map.Entry<CharSequence, Entry> me : entries.entrySet()) {
            Entry e = me.getValue();
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (Unsafe.arrayGetVolatile(e.allocations, i) != UNALLOCATED && Unsafe.arrayGet(e.readers, i) != null) {
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

    public void lock(CharSequence name) {

        checkClosed();

        Entry e = entries.get(name);
        if (e == null) {
            LOG.info().$('\'').$(name).$("' not found, cannot lock").$();
            return;
        }

        long thread = Thread.currentThread().getId();

        if (Unsafe.getUnsafe().compareAndSwapLong(e, LOCK_OWNER, UNLOCKED, thread) ||
                Unsafe.getUnsafe().compareAndSwapLong(e, LOCK_OWNER, thread, thread)) {
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                        closeReader(thread, e, i, PoolListener.EV_LOCK_CLOSE, FactoryConstants.CR_NAME_LOCK);
                    } else if (Unsafe.arrayGet(e.allocations, i) != thread || Unsafe.arrayGet(e.readers, i) != null) {
                        LOG.info().$("'").$(name).$("' is busy [owner=").$(e.lockOwner).$(']').$();
                        throw EntryLockedException.INSTANCE;
                    }
                }
                e = e.next;
            } while (e != null);
        } else {
            LOG.error().$('\'').$(name).$("' is already locked [owner=").$(e.lockOwner).$(']').$();
            notifyListener(thread, name, PoolListener.EV_LOCK_BUSY, -1, -1);
            throw EntryUnavailableException.INSTANCE;
        }
        notifyListener(thread, name, PoolListener.EV_LOCK_SUCCESS, -1, -1);
        LOG.info().$('\'').$(name).$("' locked").$();
    }

    public TableReader reader(CharSequence name) {

        checkClosed();

        Entry e = entries.get(name);

        long thread = Thread.currentThread().getId();

        if (e == null) {
            e = new Entry(0);
            Entry other = entries.putIfAbsent(name, e);
            if (other != null) {
                e = other;
                LOG.info().$("Thread ").$(thread).$(" lost race to allocate '").$(name).$('\'').$();
            }
        }

        long lockOwner = e.lockOwner;

        if (lockOwner != UNLOCKED) {
            LOG.info().$('\'').$(name).$("' is locked [owner=").$(lockOwner).$(']').$();
            throw EntryLockedException.INSTANCE;
        }

        do {
            for (int i = 0; i < ENTRY_SIZE; i++) {
                if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                    // got lock, allocate if needed
                    R r = Unsafe.arrayGet(e.readers, i);
                    if (r == null) {

                        try {
                            r = new R(ff, this, e, i, root, name);
                        } catch (CairoException e1) {
                            Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                            throw e1;
                        }

                        Unsafe.arrayPut(e.readers, i, r);
                        notifyListener(thread, name, PoolListener.EV_CREATE, e.index, i);
                    } else {
                        r.reload();
                        notifyListener(thread, name, PoolListener.EV_GET, e.index, i);
                    }

                    if (closed == TRUE) {
                        Unsafe.arrayPut(e.readers, i, null);
                        r.goodby();
                    }

                    LOG.info().$('\'').$(name).$("' is assigned [at=").$(e.index).$(':').$(i).$(", thread=").$(thread).$(']').$();
                    return r;
                }
            }

            LOG.debug().$("Thread ").$(thread).$(" is moving to entry ").$(e.index + 1).$();

            // all allocated, create next entry if possible
            if (Unsafe.getUnsafe().compareAndSwapInt(e, NEXT_STATUS, 0, 1)) {
                LOG.debug().$("Thread ").$(thread).$(" allocated entry ").$(e.index + 1).$();
                e.next = new Entry(e.index + 1);
            }

            e = e.next;

        } while (e != null && e.index < maxSegments);

        // max entries exceeded
        notifyListener(thread, name, PoolListener.EV_FULL, -1, -1);
        LOG.info().$('\'').$(name).$("' is busy [thread=").$(thread).$(", retries=").$(this.maxSegments).$(']').$();
        throw EntryUnavailableException.INSTANCE;
    }

    public void unlock(CharSequence name) {
        Entry e = entries.get(name);
        long thread = Thread.currentThread().getId();
        if (e == null) {
            LOG.info().$('\'').$(name).$("' not found, cannot unlock").$();
            notifyListener(thread, name, PoolListener.EV_NOT_LOCKED, -1, -1);
            return;
        }

        if (e.lockOwner == thread) {
            entries.remove(name);
        }
        notifyListener(thread, name, PoolListener.EV_UNLOCKED, -1, -1);
        LOG.info().$('\'').$(name).$("' unlocked").$();
    }

    private void checkClosed() {
        if (closed == TRUE) {
            LOG.info().$("is down");
            throw PoolClosedException.INSTANCE;
        }
    }

    private void closeReader(long thread, Entry entry, int index, short ev, int reason) {
        R r = Unsafe.arrayGet(entry.readers, index);
        if (r != null) {
            r.goodby();
            r.close();
            LOG.info().$("'").$(r.getName()).$("' closed [at=").$(entry.index).$(':').$(index).$(", reason=").$(FactoryConstants.closeReasonText(reason)).$(']').$();
            notifyListener(thread, r.getName(), ev, entry.index, index);
            Unsafe.arrayPut(entry.readers, index, null);
        }
    }

    private void notifyListener(long thread, CharSequence name, short event, int segment, int position) {
        PoolListener listener = getPoolListener();
        if (listener != null) {
            listener.onEvent(PoolListener.SRC_READER, thread, name, event, (short) segment, (short) position);
        }
    }

    @Override
    protected boolean releaseAll(long deadline) {
        long thread = Thread.currentThread().getId();
        boolean removed = false;
        int closeReason = deadline < Long.MAX_VALUE ? FactoryConstants.CR_IDLE : FactoryConstants.CR_POOL_CLOSE;

        for (Map.Entry<CharSequence, Entry> me : entries.entrySet()) {

            Entry e = me.getValue();

            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (deadline > Unsafe.arrayGetVolatile(e.releaseTimes, i) && Unsafe.arrayGet(e.readers, i) != null) {
                        if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                            // check if deadline violation still holds
                            if (deadline > Unsafe.arrayGet(e.releaseTimes, i)) {
                                removed = true;
                                closeReader(thread, e, i, PoolListener.EV_EXPIRE, closeReason);
                            }
                            Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                        }
                    }
                }
                e = e.next;
            } while (e != null);
        }
        return removed;
    }

    private boolean returnToPool(R reader) {
        CharSequence name = reader.getName();

        long thread = Thread.currentThread().getId();

        int index = reader.index;

        if (Unsafe.arrayGetVolatile(reader.entry.allocations, index) != UNALLOCATED) {

            if (closed == TRUE) {
                // keep locked and close
                Unsafe.arrayPut(reader.entry.readers, index, null);
                notifyListener(thread, name, PoolListener.EV_OUT_OF_POOL_CLOSE, reader.entry.index, index);
                LOG.info().$("allowing '").$(name).$("' to close [thread=").$(thread).$(']').$();
                return false;
            }

            LOG.info().$('\'').$(name).$("' is back [thread=").$(thread).$(", at=").$(reader.entry.index).$(':').$(index).$(']').$();
            notifyListener(thread, name, PoolListener.EV_RETURN, reader.entry.index, index);

            Unsafe.arrayPut(reader.entry.releaseTimes, index, System.currentTimeMillis());
            Unsafe.arrayPutOrdered(reader.entry.allocations, index, UNALLOCATED);

            return true;
        }
        LOG.error().$('\'').$(name).$("' is available [at=").$(reader.entry.index).$(':').$(index).$(']');
        return false;
    }

    private static class Entry {
        final long[] allocations = new long[ENTRY_SIZE];
        final long[] releaseTimes = new long[ENTRY_SIZE];
        final R[] readers = new R[ENTRY_SIZE];
        volatile long lockOwner = -1L;
        @SuppressWarnings("unused")
        long nextStatus = 0;
        volatile Entry next;
        int index = 0;

        public Entry(int index) {
            this.index = index;
            Arrays.fill(allocations, UNALLOCATED);
            Arrays.fill(releaseTimes, System.currentTimeMillis());
        }
    }

    public static class R extends TableReader {
        private ReaderPool pool;
        private Entry entry;
        private int index;

        public R(FilesFacade ff, ReaderPool pool, Entry entry, int index, CharSequence root, CharSequence name) {
            super(ff, root, name);
            this.pool = pool;
            this.entry = entry;
            this.index = index;
        }

        @Override
        public void close() {
            if (pool != null && entry != null && pool.returnToPool(this)) {
                return;
            }
            super.close();
        }

        private void goodby() {
            entry = null;
            pool = null;
        }
    }
}