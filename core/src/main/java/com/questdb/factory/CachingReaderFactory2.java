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

    private static final Log LOG = LogFactory.getLog(CachingReaderFactory2.class);

    private static final long NEXT_STATUS;
    private static final int ENTRY_SIZE = 32;
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final int maxEntries;
    private final boolean closed = false;

    public CachingReaderFactory2(String databaseHome, int maxEntries) {
        super(databaseHome);
        this.maxEntries = maxEntries;
    }

    public CachingReaderFactory2(JournalConfiguration configuration, int maxEntries) {
        super(configuration);
        this.maxEntries = maxEntries;
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

                if (closed) {
                    // keep locked and close
                    Unsafe.arrayPut(r.entry.readers, r.index, null);
                    return true;
                }

                Unsafe.arrayPut(r.entry.releaseTimes, r.index, System.currentTimeMillis());
                Unsafe.arrayPutOrdered(r.entry.allocations, r.index, -1L);

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
    @SuppressWarnings("unchecked")
    public <T> Journal<T> reader(JournalMetadata<T> metadata) throws JournalException {
        if (closed) {
            LOG.info().$("Pool is closed");
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

        do {
            for (int i = 0, n = e.allocations.length; i < n; i++) {
                if (Unsafe.cas(e.allocations, i, -1L, thread)) {
                    LOG.info().$("Thread ").$(thread).$(" allocated reader '").$(name).$("' at pos: ").$(e.index).$(',').$(i).$();
                    // got lock, allocate if needed
                    R r = Unsafe.arrayGet(e.readers, i);
                    if (r == null) {
                        Unsafe.arrayPut(e.readers, i, r = new R<>(e, i, metadata));
                        if (!closed) {
                            r.setCloseInterceptor(this);
                        }
                    }

                    if (closed) {
                        r.setCloseInterceptor(null);
                    }

                    return r;
                }
            }

            LOG.info().$("Thread ").$(thread).$(" is moving to entry ").$(e.index + 1).$();

            // all allocated, create next entry if possible

            if (e.nextStatus == 0) {
                if (Unsafe.getUnsafe().compareAndSwapInt(e, NEXT_STATUS, 0, 1)) {
                    LOG.info().$("Thread ").$(thread).$(" allocated entry ").$(e.index + 1).$();
                    e.next = new Entry(e.index + 1);
                }
            }

            // cannot allocate, disallowed
            if (e.nextStatus == 2) {
                LOG.info().$("Thread ").$(thread).$(" is not allowed to allocate ").$(e.index + 1).$();
                return null;
            }

            e = e.next;
        } while (e.index < maxEntries);

        // max entries exceeded
        LOG.info().$("Thread ").$(thread).$(" cannot allocate reader. Max entries exceeded (").$(this.maxEntries).$(')').$();
        return null;
    }

    private void releaseAll(long deadline) {
        long thread = Thread.currentThread().getId();

        R r;
        for (Map.Entry<String, Entry> me : entries.entrySet()) {

            Entry e = me.getValue();

            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (deadline > Unsafe.arrayGet(e.releaseTimes, i) && (r = Unsafe.arrayGet(e.readers, i)) != null) {
                        if (Unsafe.cas(e.allocations, i, -1L, thread)) {
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
        final long nextStatus = 0;
        Entry next;
        int index = 0;

        public Entry(int index) {
            this.index = index;
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

    static {
        try {
            Field f = Entry.class.getDeclaredField("nextStatus");
            NEXT_STATUS = Unsafe.getUnsafe().objectFieldOffset(f);
        } catch (NoSuchFieldException e) {
            throw new JournalRuntimeException("Cannot initialize class", e);
        }
    }
}
