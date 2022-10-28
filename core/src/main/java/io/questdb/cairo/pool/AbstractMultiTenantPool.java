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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Unsafe;

import java.util.Arrays;
import java.util.Map;

public abstract class AbstractMultiTenantPool<T extends PoolTenant> extends AbstractPool implements ResourcePool<T> {
    public static final int ENTRY_SIZE = 32;
    private static final Log LOG = LogFactory.getLog(AbstractMultiTenantPool.class);
    private static final long UNLOCKED = -1L;
    private static final long NEXT_STATUS = Unsafe.getFieldOffset(Entry.class, "nextStatus");
    private static final long LOCK_OWNER = Unsafe.getFieldOffset(Entry.class, "lockOwner");
    private static final int NEXT_OPEN = 0;
    private static final int NEXT_ALLOCATED = 1;
    private static final int NEXT_LOCKED = 2;
    private final ConcurrentHashMap<Entry<T>> entries = new ConcurrentHashMap<>();
    private final int maxSegments;
    private final int maxEntries;

    public AbstractMultiTenantPool(CairoConfiguration configuration) {
        super(configuration, configuration.getInactiveReaderTTL());
        this.maxSegments = configuration.getReaderPoolMaxSegments();
        this.maxEntries = maxSegments * ENTRY_SIZE;
    }

    public Map<CharSequence, Entry<T>> entries() {
        return entries;
    }

    protected abstract T newTenant(String tableName, Entry<T> entry, int index);

    @Override
    public T get(CharSequence tableName) {

        Entry<T> e = getEntry(tableName);

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
                    T tenant = e.getTenant(i);
                    if (tenant == null) {
                        try {
                            LOG.info()
                                    .$("open '").utf8(tableName)
                                    .$("' [at=").$(e.index).$(':').$(i)
                                    .$(']').$();
                            tenant = newTenant(Chars.toString(tableName), e, i);
                        } catch (CairoException ex) {
                            Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                            throw ex;
                        }

                        e.assignTenant(i, tenant);
                        notifyListener(thread, tableName, PoolListener.EV_CREATE, e.index, i);
                    } else {
                        tenant.refresh();
                        notifyListener(thread, tableName, PoolListener.EV_GET, e.index, i);
                    }

                    if (isClosed()) {
                        e.assignTenant(i, null);
                        tenant.goodbye();
                        LOG.info().$('\'').utf8(tableName).$("' born free").$();
                        return tenant;
                    }
                    LOG.debug().$('\'').utf8(tableName).$("' is assigned [at=").$(e.index).$(':').$(i).$(", thread=").$(thread).$(']').$();
                    return tenant;
                }
            }

            LOG.debug().$("Thread ").$(thread).$(" is moving to entry ").$(e.index + 1).$();

            // all allocated, create next entry if possible
            if (Unsafe.getUnsafe().compareAndSwapInt(e, NEXT_STATUS, NEXT_OPEN, NEXT_ALLOCATED)) {
                LOG.debug().$("Thread ").$(thread).$(" allocated entry ").$(e.index + 1).$();
                e.next = new Entry<T>(e.index + 1, clock.getTicks());
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
        for (Map.Entry<CharSequence, Entry<T>> me : entries.entrySet()) {
            Entry<T> e = me.getValue();
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (Unsafe.arrayGetVolatile(e.allocations, i) != UNALLOCATED && e.getTenant(i) != null) {
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
        Entry<T> e = getEntry(name);
        final long thread = Thread.currentThread().getId();
        if (Unsafe.cas(e, LOCK_OWNER, UNLOCKED, thread) || Unsafe.cas(e, LOCK_OWNER, thread, thread)) {
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                        closeTenant(thread, e, i, PoolListener.EV_LOCK_CLOSE, PoolConstants.CR_NAME_LOCK);
                    } else if (Unsafe.cas(e.allocations, i, thread, thread)) {
                        // same thread, don't need to order reads
                        if (e.getTenant(i) != null) {
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
        Entry<T> e = entries.get(name);
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

        for (Entry<T> e : entries.values()) {
            do {
                for (int i = 0; i < ENTRY_SIZE; i++) {
                    T r;
                    if (deadline > Unsafe.arrayGetVolatile(e.releaseOrAcquireTimes, i) && (r = e.getTenant(i)) != null) {
                        if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                            // check if deadline violation still holds
                            if (deadline > e.releaseOrAcquireTimes[i]) {
                                removed = true;
                                closeTenant(thread, e, i, PoolListener.EV_EXPIRE, closeReason);
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

    private void closeTenant(long thread, Entry<T> entry, int index, short ev, int reason) {
        T tenant = entry.getTenant(index);
        if (tenant != null) {
            tenant.goodbye();
            tenant.close();
            LOG.info().$("closed '").utf8(tenant.getTableName())
                    .$("' [at=").$(entry.index).$(':').$(index)
                    .$(", reason=").$(PoolConstants.closeReasonText(reason))
                    .I$();
            notifyListener(thread, tenant.getTableName(), ev, entry.index, index);
            entry.assignTenant(index, null);
        }
    }

    private Entry<T> getEntry(CharSequence name) {
        checkClosed();

        Entry<T> e = entries.get(name);
        if (e == null) {
            e = new Entry<T>(0, clock.getTicks());
            Entry<T> other = entries.putIfAbsent(name, e);
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

    protected boolean returnToPool(T tenant) {
        final Entry<T> e = tenant.getEntry();
        if (e == null) {
            return false;
        }

        final CharSequence tableName = tenant.getTableName();
        final long thread = Thread.currentThread().getId();
        final int index = tenant.getIndex();
        final long owner = Unsafe.arrayGetVolatile(e.allocations, index);

        if (owner != UNALLOCATED) {
            LOG.debug().$('\'').$(tableName).$("' is back [at=").$(e.index).$(':').$(index).$(", thread=").$(thread).$(']').$();
            notifyListener(thread, tableName, PoolListener.EV_RETURN, e.index, index);

            // release the entry for anyone to pick up
            e.releaseOrAcquireTimes[index] = clock.getTicks();
            Unsafe.arrayPutOrdered(e.allocations, index, UNALLOCATED);
            final boolean closed = isClosed();

            // When pool is closed we will race against release thread
            // to release our entry. No need to bother releasing map entry, pool is going down.
            return !closed || !Unsafe.cas(e.allocations, index, UNALLOCATED, owner);
        }

        throw CairoException.critical(0).put("double close [table=").put(tableName).put(", index=").put(index).put(']');
    }

    public static final class Entry<T> {
        private final long[] allocations = new long[ENTRY_SIZE];
        private final long[] releaseOrAcquireTimes = new long[ENTRY_SIZE];
        @SuppressWarnings("unchecked")
        private final T[] tenants = (T[]) new Object[ENTRY_SIZE];
        private final int index;
        private volatile long lockOwner = -1L;
        @SuppressWarnings("unused")
        int nextStatus = 0;
        private volatile Entry<T> next;

        public Entry(int index, long currentMicros) {
            this.index = index;
            Arrays.fill(allocations, UNALLOCATED);
            Arrays.fill(releaseOrAcquireTimes, currentMicros);
        }

        public long getOwnerVolatile(int pos) {
            return Unsafe.arrayGetVolatile(allocations, pos);
        }

        public long getReleaseOrAcquireTime(int pos) {
            return releaseOrAcquireTimes[pos];
        }

        public T getTenant(int pos) {
            return tenants[pos];
        }

        public void assignTenant(int pos, T tenant) {
            tenants[pos] = tenant;
        }

        public Entry<T> getNext() {
            return next;
        }
    }
}