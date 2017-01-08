/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.factory;

import com.questdb.Journal;
import com.questdb.ex.JournalException;
import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CachingReaderFactory2 extends ReaderFactoryImpl implements JournalCloseInterceptor {

    public static final long CLOSED;
    public static final int E_POOL_CLOSED = 1;
    public static final int E_NAME_LOCKED = 2;
    public static final int E_POOL_FULL = 3;
    public static final int E_NOT_AN_OWNER = 4;
    public static final int E_AGAIN = 5;
    public static final int E_OK = 0;

    private static final Log LOG = LogFactory.getLog(CachingReaderFactory2.class);
    private static final long UNALLOCATED = -1L;
    private static final long UNLOCKED = -1L;
    private static final long NEXT_STATUS;
    private static final int ENTRY_SIZE = 32;
    private static final int TRUE = 1;
    private static final int FALSE = 0;
    private static final long LOCK_OWNER;
    private static final ThreadLocal<Error> error = new ThreadLocal<Error>() {
        @Override
        protected Error initialValue() {
            return new Error();
        }
    };
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final int maxSegments;
    private final int maxEntries;
    private volatile int closed = FALSE;

    public CachingReaderFactory2(String databaseHome, int maxSegments) {
        super(databaseHome);
        this.maxSegments = maxSegments;
        this.maxEntries = maxSegments * ENTRY_SIZE;
    }

    public CachingReaderFactory2(JournalConfiguration configuration, int maxSegments) {
        super(configuration);
        this.maxSegments = maxSegments;
        this.maxEntries = maxSegments * ENTRY_SIZE;
    }

    @Override
    public boolean canClose(Journal journal) {
        String name = journal.getName();

        if (journal instanceof R) {
            Entry e = entries.get(name);
            if (e == null) {
                LOG.error().$("Reader '").$(name).$("' is not managed by this pool").$();
                return true;
            }

            long thread = Thread.currentThread().getId();
            R r = (R) journal;

            if (Unsafe.arrayGet(r.entry.allocations, r.index) == thread) {

                if (closed == TRUE) {
                    // keep locked and close
                    Unsafe.arrayPut(r.entry.readers, r.index, null);
                    return true;
                }

                Unsafe.arrayPut(r.entry.releaseTimes, r.index, System.currentTimeMillis());
                Unsafe.arrayPutOrdered(r.entry.allocations, r.index, UNALLOCATED);

                LOG.info().$("Thread ").$(thread).$(" released reader '").$(name).$('\'').$();
                return false;
            }

            LOG.error().$("Thread ").$(thread).$(" does not own reader '").$(name).$("' at pos ").$(r.entry.index).$(',').$(r.index).$();
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

    public int getError() {
        return error.get().code;
    }

    public int getMaxEntries() {
        return maxEntries;
    }

    public boolean lock(String name) {

        error(E_OK);

        Entry e = entries.get(name);
        if (e == null) {
            return true;
        }

        boolean result = true;

        long thread = Thread.currentThread().getId();

        if (Unsafe.getUnsafe().compareAndSwapLong(e, LOCK_OWNER, UNLOCKED, thread) ||
                Unsafe.getUnsafe().compareAndSwapLong(e, LOCK_OWNER, thread, thread)) {
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                        R r = Unsafe.arrayGet(e.readers, i);
                        if (r != null) {
                            r.setCloseInterceptor(null);
                            r.close();
                            Unsafe.arrayPut(e.readers, i, null);
                        }
                    } else if (Unsafe.arrayGet(e.readers, i) != null) {
                        result = false;
                        error(E_AGAIN);
                    }
                }
                e = e.next;
            } while (e != null);

            return result;

        } else {
            LOG.error().$("Reader '").$(name).$("' is already locked by ").$(e.lockOwner).$();
            error(E_NOT_AN_OWNER);
            return false;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Journal<T> reader(JournalMetadata<T> metadata) throws JournalException {
        if (closed == TRUE) {
            LOG.info().$("Pool is closed");
            error(E_POOL_CLOSED);
            return null;
        }

        String name = metadata.getKey().getName();
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
                LOG.info().$("Thread ").$(thread).$(" WON the race to create first entry for '").$(name).$('\'').$();
            }
        }

        long lockOwner = e.lockOwner;

        if (lockOwner != UNLOCKED) {
            LOG.info().$("Reader '").$(name).$("' is locked by ").$(lockOwner).$();
            error(E_NAME_LOCKED);
            return null;
        }

        do {
            for (int i = 0; i < ENTRY_SIZE; i++) {
                if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                    error(E_OK);
                    LOG.info().$("Thread ").$(thread).$(" allocated reader '").$(name).$("' at pos: ").$(e.index).$(',').$(i).$();
                    // got lock, allocate if needed
                    R r = Unsafe.arrayGet(e.readers, i);
                    if (r == null) {
                        r = new R(e, i, metadata);
                        if (closed == TRUE) {
                            // don't assign interceptor or keep reference
                            return r;
                        }

                        Unsafe.arrayPut(e.readers, i, r);
                        r.setCloseInterceptor(this);
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
        LOG.info().$("Thread ").$(thread).$(" cannot allocate reader. Max entries exceeded (").$(this.maxSegments).$(')').$();
        error(E_POOL_FULL);
        return null;
    }

    public void unlock(String name) {
        error(E_OK);

        Entry e = entries.get(name);
        if (e == null) {
            return;
        }

        long thread = Thread.currentThread().getId();

        if (Unsafe.getUnsafe().compareAndSwapLong(e, LOCK_OWNER, thread, UNLOCKED)) {
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    Unsafe.cas(e.allocations, i, thread, UNALLOCATED);
                }
                e = e.next;
            } while (e != null);
        }
    }

    private static void error(int code) {
        error.get().code = code;
    }

    private void releaseAll(long deadline) {
        long thread = Thread.currentThread().getId();

        R r;
        for (Map.Entry<String, Entry> me : entries.entrySet()) {

            Entry e = me.getValue();

            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (deadline > Unsafe.arrayGetVolatile(e.releaseTimes, i) && (r = Unsafe.arrayGet(e.readers, i)) != null) {
                        if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                            // check if deadline violation still holds
                            if (deadline > Unsafe.arrayGet(e.releaseTimes, i)) {
                                r.setCloseInterceptor(null);
                                try {
                                    r.close();
                                } catch (Throwable e1) {
                                    LOG.error().$("Cannot close reader '").$(r.getName()).$("': ").$(e1.getMessage()).$();
                                }
                                Unsafe.arrayPut(e.readers, i, null);
                            }
                            Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                        }
                    }
                }
                e = e.next;
            } while (e != null);
        }
    }

    private static class Entry {
        final long[] allocations = new long[ENTRY_SIZE];
        final long[] releaseTimes = new long[ENTRY_SIZE];
        final R[] readers = new R[ENTRY_SIZE];
        long nextStatus = 0;
        volatile Entry next;
        volatile long lockOwner = -1L;
        int index = 0;

        public Entry(int index) {
            this.index = index;
            Arrays.fill(allocations, UNALLOCATED);
            Arrays.fill(releaseTimes, System.currentTimeMillis());
        }
    }

    public static class R<T> extends Journal<T> {
        private Entry entry;
        private int index;

        public R(Entry entry, int index, JournalMetadata<T> metadata) throws JournalException {
            super(metadata);
            this.entry = entry;
            this.index = index;
        }
    }

    private static class Error {
        int code = E_OK;
    }

    static {
        try {
            Field f = Entry.class.getDeclaredField("nextStatus");
            NEXT_STATUS = Unsafe.getUnsafe().objectFieldOffset(f);

            Field f2 = CachingReaderFactory2.class.getDeclaredField("closed");
            CLOSED = Unsafe.getUnsafe().objectFieldOffset(f2);

            Field f3 = Entry.class.getDeclaredField("lockOwner");
            LOCK_OWNER = Unsafe.getUnsafe().objectFieldOffset(f3);

        } catch (NoSuchFieldException e) {
            throw new JournalRuntimeException("Cannot initialize class", e);
        }
    }
}
