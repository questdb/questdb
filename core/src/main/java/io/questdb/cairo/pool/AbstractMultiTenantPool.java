/*******************************************************************************
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

package io.questdb.cairo.pool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Map;

public abstract class AbstractMultiTenantPool<T extends PoolTenant<T>> extends AbstractPool implements ResourcePool<T> {
    public final static String LOCKED = "pool is locked";
    public final static String NO_LOCK_REASON = "unknown";
    public final static String POOL_SIZE = "pool size exceeded";
    private static final long LOCK_OWNER = Unsafe.getFieldOffset(Entry.class, "lockOwner");
    private static final int NEXT_ALLOCATED = 1;
    private static final int NEXT_LOCKED = 2;
    private static final int NEXT_OPEN = 0;
    private static final long NEXT_STATUS = Unsafe.getFieldOffset(Entry.class, "nextStatus");
    private static final long UNLOCKED = -1L;
    private final Log LOG = LogFactory.getLog(this.getClass());
    private final ConcurrentHashMap<Entry<T>> entries = new ConcurrentHashMap<>();
    private final int maxEntries;
    private final int maxSegments;
    private final int segmentSize;
    private final ThreadLocal<ResourcePoolSupervisor<T>> threadLocalPoolSupervisor;

    public AbstractMultiTenantPool(CairoConfiguration configuration, int maxSegments, long inactiveTtlMillis) {
        super(configuration, inactiveTtlMillis);
        this.maxSegments = maxSegments;
        this.segmentSize = configuration.getPoolSegmentSize();
        this.maxEntries = maxSegments * configuration.getPoolSegmentSize();
        if (configuration.cairoResourcePoolTracingEnabled()) {
            threadLocalPoolSupervisor = new io.questdb.std.ThreadLocal<>(TracingResourcePoolSupervisor::new);
        } else {
            threadLocalPoolSupervisor = new ThreadLocal<>();
        }
    }

    public void configureThreadLocalPoolSupervisor(@NotNull ResourcePoolSupervisor<T> poolSupervisor) {
        this.threadLocalPoolSupervisor.set(poolSupervisor);
    }

    public Map<CharSequence, Entry<T>> entries() {
        return entries;
    }

    @Override
    public T get(TableToken tableToken) {
        return get0(tableToken, null);
    }

    public int getBusyCount() {
        int count = 0;
        for (Map.Entry<CharSequence, Entry<T>> me : entries.entrySet()) {
            Entry<T> e = me.getValue();
            do {
                for (int i = 0; i < segmentSize; i++) {
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

    public int getSegmentSize() {
        return segmentSize;
    }

    public boolean isCopyOfSupported() {
        return false;
    }

    public boolean lock(TableToken tableToken) {
        Entry<T> e = getEntry(tableToken);
        final long thread = Thread.currentThread().getId();
        if (Unsafe.cas(e, LOCK_OWNER, UNLOCKED, thread) || e.lockOwner == thread) {
            do {
                for (int i = 0; i < segmentSize; i++) {
                    if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                        closeTenant(thread, e, i, PoolListener.EV_LOCK_CLOSE, PoolConstants.CR_NAME_LOCK);
                    } else if (Unsafe.arrayGetVolatile(e.allocations, i) == thread) {
                        // same thread, don't need to order reads
                        if (e.getTenant(i) != null) {
                            // this thread has busy reader, it should close first
                            e.lockOwner = UNLOCKED;
                            return false;
                        }
                    } else {
                        LOG.info().$("could not lock, busy [table=").$(tableToken)
                                .$(", at=").$(e.index).$(':').$(i)
                                .$(", owner=").$(e.allocations[i])
                                .$(", thread=").$(thread)
                                .I$();
                        e.lockOwner = UNLOCKED;
                        return false;
                    }
                }

                // try to prevent new entries from being created
                if (e.next == null) {
                    if (Unsafe.getUnsafe().compareAndSwapInt(e, NEXT_STATUS, NEXT_OPEN, NEXT_LOCKED)) {
                        break;
                    } else if (e.nextStatus == NEXT_ALLOCATED) {
                        // now we must wait until another thread that executes a get() call
                        // assigns the newly created next entry
                        while (e.next == null) {
                            Os.pause();
                        }
                    }
                }

                e = e.next;
            } while (e != null);
        } else {
            LOG.error().$("already locked [table=").$(tableToken)
                    .$(", owner=").$(e.lockOwner)
                    .I$();
            notifyListener(thread, tableToken, PoolListener.EV_LOCK_BUSY, -1, -1);
            return false;
        }
        notifyListener(thread, tableToken, PoolListener.EV_LOCK_SUCCESS, -1, -1);
        LOG.debug().$("locked [table=").$(tableToken)
                .$(", thread=").$(thread)
                .I$();
        return true;
    }

    public void notifyDropped(TableToken token, boolean fullDropped) {
        Entry<T> firstEntry = entries.get(token.getDirName());
        long thread = Thread.currentThread().getId();

        if (firstEntry != null) {
            // Mark the entry as dropped. Any attempt to return to pool after this point
            // will simply close the tenant. The get0() method also checks this flag after
            // CAS to catch concurrent drops.
            firstEntry.dropped = true;
            if (fullDropped) {
                entries.remove(token.getDirName());
            }

            Entry<T> e = firstEntry;
            while (e != null) {
                for (int i = 0; i < segmentSize; i++) {
                    if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                        try {
                            closeTenant(thread, e, i, PoolListener.EV_DROPPED, PoolConstants.CR_DROPPED);
                        } catch (Throwable th) {
                            LOG.critical().$("error on closing pool item [table=").$(token)
                                    .$(", error=").$(th).I$();
                        }
                        Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                    }
                }

                if (e.next == null && e.nextStatus == NEXT_ALLOCATED) {
                    // now we must wait until another thread that executes a get() call
                    // assigns the newly created next entry
                    while (e.next == null) {
                        Os.pause();
                    }
                }

                e = e.next;
            }
        }
    }

    public void removeThreadLocalPoolSupervisor() {
        this.threadLocalPoolSupervisor.remove();
    }

    public void unlock(TableToken tableToken) {
        unlock(tableToken, false);
    }

    public void unlock(TableToken tableToken, boolean quiet) {
        Entry<T> e = entries.get(tableToken.getDirName());
        long thread = Thread.currentThread().getId();
        if (e == null) {
            if (!quiet) {
                // This is OK, the table deletion holds lock and deletes the entry
                LOG.info().$("not found, cannot unlock [table=").$(tableToken).I$();
                notifyListener(thread, tableToken, PoolListener.EV_NOT_LOCKED, -1, -1);
            }
            return;
        }

        if (e.lockOwner == thread) {
            entries.remove(tableToken.getDirName());
            while (e != null) {
                e = e.next;
            }
        } else {
            notifyListener(thread, tableToken, PoolListener.EV_NOT_LOCK_OWNER);
            throw CairoException.nonCritical().put("Not the lock owner of ").put(tableToken.getDirName());
        }

        notifyListener(thread, tableToken, PoolListener.EV_UNLOCKED, -1, -1);
        LOG.debug().$("unlocked [table=").$(tableToken).I$();
    }

    private void checkClosed() {
        if (isClosed()) {
            LOG.debug().$("is closed").$();
            throw PoolClosedException.INSTANCE;
        }
    }

    private void closeTenant(long thread, Entry<T> entry, int index, short ev, int reason) {
        T tenant = entry.getTenant(index);
        if (tenant != null) {
            tenant.goodbye();
            tenant.close();
            LOG.debug().$("closed [table=").$(tenant.getTableToken())
                    .$(", at=").$(entry.index).$(':').$(index)
                    .$(", reason=").$(PoolConstants.closeReasonText(reason))
                    .I$();
            notifyListener(thread, tenant.getTableToken(), ev, entry.index, index);
            entry.assignTenant(index, null);
        }
    }

    private T get0(TableToken tableToken, @Nullable T copyOfTenant) {
        Entry<T> rootEntry = getEntry(tableToken);
        Entry<T> e = rootEntry;

        long lockOwner = e.lockOwner;
        long thread = Thread.currentThread().getId();

        if (lockOwner != UNLOCKED) {
            LOG.info().$("table is locked [table=").$(tableToken)
                    .$(", owner=").$(lockOwner)
                    .I$();
            throw EntryLockedException.instance(NO_LOCK_REASON);
        }

        do {
            for (int i = 0; i < segmentSize; i++) {
                if (Unsafe.cas(e.allocations, i, UNALLOCATED, thread)) {
                    // Check if table was fully dropped concurrently. This narrows the race window
                    // between verifyTableToken and pool allocation. Even if this check passes,
                    // the sequencer WRITE lock provides the ultimate correctness guarantee.
                    // Note: For TRUNCATE (fullDropped=false), dropped is set but entry stays in map.
                    // We only throw for full drops where the entry was removed from the map.
                    if (rootEntry.dropped && entries.get(tableToken.getDirName()) != rootEntry) {
                        Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                        throw CairoException.tableDropped(tableToken);
                    }

                    Unsafe.arrayPutOrdered(e.releaseOrAcquireTimes, i, clock.getTicks());
                    // got lock, allocate if needed
                    T tenant = e.getTenant(i);
                    ResourcePoolSupervisor<T> supervisor = threadLocalPoolSupervisor.get();
                    if (tenant == null) {
                        try {
                            LOG.debug()
                                    .$("open [table=").$(tableToken)
                                    .$(", at=").$(e.index).$(':').$(i)
                                    .I$();
                            tenant = copyOfTenant != null
                                    ? newCopyOfTenant(copyOfTenant, rootEntry, e, i, supervisor)
                                    : newTenant(tableToken, rootEntry, e, i, supervisor);
                        } catch (CairoException ex) {
                            Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                            throw ex;
                        }

                        e.assignTenant(i, tenant);
                        notifyListener(thread, tableToken, PoolListener.EV_CREATE, e.index, i);
                    } else {
                        try {
                            if (copyOfTenant != null) {
                                tenant.refreshAt(supervisor, copyOfTenant);
                            } else {
                                tenant.refresh(supervisor);
                            }
                        } catch (Throwable th) {
                            tenant.goodbye();
                            tenant.close();
                            e.assignTenant(i, null);
                            Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                            throw th;
                        }
                        notifyListener(thread, tableToken, PoolListener.EV_GET, e.index, i);
                    }

                    if (isClosed()) {
                        e.assignTenant(i, null);
                        tenant.goodbye();
                        LOG.info().$("born free [table=").$(tableToken).I$();
                        tenant.updateTableToken(tableToken);
                        supervisor = tenant.getSupervisor();
                        if (supervisor != null) {
                            supervisor.onResourceBorrowed(tenant);
                        }
                        return tenant;
                    }
                    LOG.debug().$("assigned [table=").$(tableToken)
                            .$(", at=").$(e.index).$(':').$(i)
                            .$(", thread=").$(thread)
                            .I$();
                    tenant.updateTableToken(tableToken);
                    supervisor = tenant.getSupervisor();
                    if (supervisor != null) {
                        supervisor.onResourceBorrowed(tenant);
                    }
                    return tenant;
                }
            }

            LOG.debug().$("Thread ").$(thread).$(" is moving to entry ").$(e.index + 1).$();

            // all allocated, create next entry if possible
            if (Unsafe.getUnsafe().compareAndSwapInt(e, NEXT_STATUS, NEXT_OPEN, NEXT_ALLOCATED)) {
                LOG.debug().$("Thread ").$(thread).$(" allocated entry ").$(e.index + 1).$();
                e.next = new Entry<>(e.index + 1, clock.getTicks(), segmentSize);
            } else {
                // if the race is lost we need to wait until e.next is set by the winning thread
                while (e.next == null && e.nextStatus == NEXT_ALLOCATED) {
                    Os.pause();
                }
                if (e.nextStatus == NEXT_LOCKED) {
                    LOG.info().$("table is locked [table=").$(tableToken)
                            .$(", owner=").$(lockOwner)
                            .I$();
                    throw EntryLockedException.instance(LOCKED);
                }
            }
            e = e.next;
        } while (e != null && e.index < maxSegments);

        // max entries exceeded
        notifyListener(thread, tableToken, PoolListener.EV_FULL, -1, -1);
        LOG.info().$("could not get, busy [table=").$(tableToken)
                .$(", thread=").$(thread)
                .$(", retries=").$(this.maxSegments)
                .I$();
        throw EntryUnavailableException.instance(POOL_SIZE);
    }

    private Entry<T> getEntry(TableToken token) {
        checkClosed();

        Entry<T> e = entries.get(token.getDirName());
        if (e == null) {
            e = new Entry<>(0, clock.getTicks(), segmentSize);
            Entry<T> other = entries.putIfAbsent(token.getDirName(), e);
            if (other != null) {
                e = other;
            }
        }
        return e;
    }

    private void notifyListener(long thread, TableToken token, short event, int segment, int position) {
        PoolListener listener = getPoolListener();
        if (listener != null) {
            listener.onEvent(getListenerSrc(), thread, token, event, (short) segment, (short) position);
        }
    }

    @Override
    protected void closePool() {
        super.closePool();
        LOG.debug().$("closed").$();
    }

    protected void expelFromPool(T tenant) {
        final Entry<T> e = tenant.getEntry();
        if (e == null) {
            return;
        }

        final TableToken tableToken = tenant.getTableToken();
        final long thread = Thread.currentThread().getId();
        final int index = tenant.getIndex();
        final long owner = Unsafe.arrayGetVolatile(e.allocations, index);

        if (owner != UNALLOCATED) {
            LOG.debug().$("table is expelled [table=").$(tableToken)
                    .$(", at=").$(e.index).$(':').$(index)
                    .$(", thread=").$(thread)
                    .I$();
            notifyListener(thread, tableToken, PoolListener.EV_OUT_OF_POOL_CLOSE, e.index, index);
            e.assignTenant(index, null);
            Unsafe.cas(e.allocations, index, owner, UNALLOCATED);
        }
    }

    protected T getCopyOf(@NotNull T srcTenant) {
        if (!isCopyOfSupported()) {
            throw new UnsupportedOperationException("getCopyOf is not supported by this pool");
        }
        return get0(srcTenant.getTableToken(), srcTenant);
    }

    protected abstract byte getListenerSrc();

    protected T newCopyOfTenant(
            T srcTenant,
            Entry<T> rootEntry,
            Entry<T> entry,
            int index,
            @Nullable ResourcePoolSupervisor<T> supervisor
    ) {
        throw new UnsupportedOperationException();
    }

    protected abstract T newTenant(
            TableToken tableToken,
            Entry<T> rootEntry,
            Entry<T> entry,
            int index,
            @Nullable ResourcePoolSupervisor<T> supervisor
    );

    @Override
    protected boolean releaseAll(long deadline) {
        long thread = Thread.currentThread().getId();
        boolean removed = false;
        int casFailures = 0;
        int closeReason = deadline < Long.MAX_VALUE ? PoolConstants.CR_IDLE : PoolConstants.CR_POOL_CLOSE;

        TableToken leftBehind = null;
        for (Entry<T> e : entries.values()) {
            do {
                for (int i = 0; i < segmentSize; i++) {
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
                                Unsafe.arrayPutOrdered(e.allocations, i, UNALLOCATED);
                                var rec = LOG.infoW().$("shutting down, table is left behind [table=").$(r.getTableToken()).$(']');
                                try {
                                    var supervisor = r.getSupervisor();
                                    if (supervisor instanceof TracingResourcePoolSupervisor<T>) {
                                        ((TracingResourcePoolSupervisor<T>) supervisor).printResourceInfo(rec.$(": "), r);
                                    }
                                } finally {
                                    rec.$();
                                }
                                leftBehind = r.getTableToken();
                            }
                        }
                    }
                }
                // this does not release the next
                e = e.next;
            } while (e != null);
        }

        if (leftBehind != null) {
            // This code branch should be in tests only.
            // Release the item, to not block the pool, but throw an exception to fail the test
            throw CairoException.nonCritical()
                    .put("table is left behind on pool shutdown [table=").put(leftBehind).put(']');
        }

        // when we are timing out entries the result is "true" if there was any work done
        // when we're closing pool, the result is true when pool is empty
        if (closeReason == PoolConstants.CR_IDLE) {
            return removed;
        } else {
            return casFailures == 0;
        }
    }

    protected boolean returnToPool(T tenant) {
        final Entry<T> e = tenant.getEntry();
        if (e == null) {
            return false;
        }

        final TableToken tableToken = tenant.getTableToken();
        final long thread = Thread.currentThread().getId();
        final int index = tenant.getIndex();
        final long owner = Unsafe.arrayGetVolatile(e.allocations, index);

        if (owner != UNALLOCATED) {
            // Check if entry is being locked or dropped by checking the first entry's lockOwner
            LOG.debug().$("table is back [table=").$(tableToken)
                    .$(", at=").$(e.index).$(':').$(index)
                    .$(", thread=").$(thread)
                    .I$();
            notifyListener(thread, tableToken, PoolListener.EV_RETURN, e.index, index);

            // release the entry for anyone to pick up
            e.releaseOrAcquireTimes[index] = clock.getTicks();
            Unsafe.arrayPutOrdered(e.allocations, index, UNALLOCATED);
            if (tenant.getRootEntry().dropped) {
                // Table is dropped (entry removed or replaced) - dispose instead of returning
                // Make sure no one else has taken it in the meantime
                if (Unsafe.cas(e.allocations, index, UNALLOCATED, owner)) {
                    // Check if our tenant is still in the entry slot.
                    // If notifyDropped() already closed it, the slot will be null.
                    // This prevents double-close when notifyDropped() races with returnToPool().
                    if (e.getTenant(index) != tenant) {
                        // Tenant was already closed by notifyDropped(), just release the slot
                        Unsafe.arrayPutOrdered(e.allocations, index, UNALLOCATED);
                        return true; // Tell caller not to close (already closed)
                    }
                    LOG.info().$("close on return, table is dropped [table=").$(tableToken)
                            .$(", at=").$(e.index).$(':').$(index)
                            .I$();

                    tenant.goodbye();
                    e.assignTenant(index, null);
                    Unsafe.arrayPutOrdered(e.allocations, index, UNALLOCATED);

                    return false;
                }
            }
            final boolean closed = isClosed();

            // When pool is closed we will race against release thread
            // to release our entry. No need to bother releasing map entry, pool is going down.
            return !closed || !Unsafe.cas(e.allocations, index, UNALLOCATED, owner);
        }

        if (isClosed()) {
            // Returning to closed pool is ok under race condition
            // We may end up here because our "allocation" has been erased while we
            // still see the reference to the pool. The allocation "erasure"
            // occurs when pool is being closed. The memory writes should be ordered
            // in such a way that when we see "UNALLOCATED" the pool closed flag is already set.
            return false;
        }
        throw CairoException.critical(0).put("double close [table=").put(tableToken)
                .put(", index=").put(index).put(']');
    }

    public static final class Entry<T> {
        private final long[] allocations;
        private final int index;
        private final long[] releaseOrAcquireTimes;
        private final T[] tenants;
        public volatile boolean dropped;
        private volatile long lockOwner = -1L;
        private volatile Entry<T> next;
        // This is modified using CAS operations, it cannot be final.
        // Must be volatile for visibility of plain reads after CAS races.
        @SuppressWarnings("FieldMayBeFinal")
        private volatile int nextStatus = NEXT_OPEN;

        @SuppressWarnings("unchecked")
        public Entry(int index, long currentMicros, int entrySize) {
            allocations = new long[entrySize];
            releaseOrAcquireTimes = new long[entrySize];
            tenants = (T[]) new Object[entrySize];
            this.index = index;
            Arrays.fill(allocations, UNALLOCATED);
            Arrays.fill(releaseOrAcquireTimes, currentMicros);
        }

        public void assignTenant(int pos, T tenant) {
            tenants[pos] = tenant;
        }

        public int getIndex() {
            return index;
        }

        public Entry<T> getNext() {
            return next;
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
    }
}
