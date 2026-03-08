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

package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Worker;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentLongHashMap;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.ThreadLocal;
import io.questdb.std.WeakMutableObjectPool;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A concurrent registry of running sql commands.
 */
public class QueryRegistry {

    private static final Log LOG = LogFactory.getLog(QueryRegistry.class);
    private final Clock clock;
    private final AtomicLong idSeq = new AtomicLong();
    private final ConcurrentLongHashMap<Entry> registry = new ConcurrentLongHashMap<>();
    private final ThreadLocal<WeakMutableObjectPool<Entry>> tlQueryPool;

    private volatile Listener listener;

    public QueryRegistry(CairoConfiguration configuration) {
        this.clock = configuration.getMicrosecondClock();
        tlQueryPool = new ThreadLocal<>(() -> new WeakMutableObjectPool<>(Entry::new, configuration.getQueryRegistryPoolSize()));
    }

    /**
     * Cancels command with given id.
     * Cancellation is not immediate and depends on how often the running command checks circuit breaker.
     * Cancelling commands issued by other users is allowed for admin user only.
     *
     * @param queryId          id of query to cancel, must be non-negative
     * @param executionContext execution context
     * @return true if query was found in registry and cancelled, otherwise false
     * @throws CairoException when user doesn't have permission to cancel the query or query is executed in WAL Apply job.
     */
    public boolean cancel(long queryId, SqlExecutionContext executionContext) throws CairoException {
        SecurityContext securityContext = executionContext.getSecurityContext();
        if (!securityContext.isQueryCancellationAllowed()) {
            throw CairoException.nonCritical().put("Query cancellation is disabled");
        }

        Entry entry = registry.get(queryId);
        if (entry != null) {
            if (!Chars.equals(entry.principal, securityContext.getPrincipal())) {
                // only a SQL Engine admin can cancel other user's queries
                securityContext.authorizeSqlEngineAdmin();
            }

            if (entry.isWAL) {
                throw CairoException.nonCritical().put("query applied in WAL job can't be cancelled [id=").put(queryId).put(']');
            }
            entry.cancel();
            entry.changedAtNs = clock.getTicks();
            entry.state = Entry.State.CANCELLED;
            LOG.info().$("cancelling query [user=").$(securityContext.getPrincipal()).$(",queryId=").$(queryId).$(",sql=").$(entry.query).I$();
            return true;
        }

        LOG.info().$("query not found in registry [id=").$(queryId).I$();
        return false;
    }

    public Entry getEntry(long id) {
        return registry.get(id);
    }

    /**
     * Copy ids of currently running sql commands to target list.
     * List is cleared before adding ids.
     *
     * @param target - list to copy ids to
     */
    public void getEntryIds(@NotNull LongList target) {
        target.clear();

        ConcurrentLongHashMap.KeyIterator<Entry> iterator = registry.keySet().iterator();

        while (iterator.hasNext()) {
            target.add(iterator.next());
        }
    }

    /**
     * Add given command to registry.
     *
     * @param query            - query text
     * @param executionContext - execution context
     * @return non-negative id assigned to given query. It may be used to look query up in registry.
     */
    public long register(CharSequence query, SqlExecutionContext executionContext) {
        final long queryId = idSeq.getAndIncrement();
        final Entry e = tlQueryPool.get().pop();
        // Just in case something messed the cached Entry
        // while it was in the pool, like late query cancel()
        // clean the object before using.
        e.clear();

        e.registeredAtNs = clock.getTicks();
        e.changedAtNs = e.registeredAtNs;
        e.state = Entry.State.ACTIVE;

        if (executionContext.containsSecret()) {
            e.query.put("<SECRET>");
        } else {
            // we shouldn't copy text in case of sensitive queries
            e.query.put(query);
        }

        final Thread thread = Thread.currentThread();
        if (thread instanceof Worker worker) {
            e.workerId = worker.getWorkerId();
            e.poolName = worker.getPoolName();
        }
        e.isWAL = executionContext.isWalApplication();
        e.principal = executionContext.getSecurityContext().getPrincipal();
        registry.put(queryId, e);

        Listener listener = this.listener;
        if (listener != null) {
            listener.onRegister(query, queryId, executionContext);
        }

        executionContext.setCancelledFlag(e.cancelled);
        return queryId;
    }

    @TestOnly
    public void setListener(Listener listener) {
        this.listener = listener;
    }

    /**
     * Remove query with given id from registry.
     *
     * @param queryId          - id of query to remove
     * @param executionContext - execution context
     */
    public void unregister(long queryId, SqlExecutionContext executionContext) {
        if (queryId < 0) {
            // likely because query was already unregistered
            return;
        }

        // Remove shared AtomicBoolean from execution context CircuitBreaker
        // before returning Entry to the pool
        executionContext.setCancelledFlag(null);
        final Entry e = registry.remove(queryId);
        if (e != null) {
            tlQueryPool.get().push(e);
        } else {
            // this might happen if query was cancelled
            LOG.error().$("query to unregister not found [id=").$(queryId).I$();
        }
    }

    public interface Listener {
        void onRegister(CharSequence query, long queryId, SqlExecutionContext executionContext);
    }

    public static class Entry implements Mutable {
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final StringSink query = new StringSink();
        private long changedAtNs;
        private boolean isWAL;
        private CharSequence poolName;
        private CharSequence principal;
        private long registeredAtNs;
        private byte state;
        private long workerId;

        public void cancel() {
            cancelled.set(true);
        }

        @Override
        public void clear() {
            query.clear();
            registeredAtNs = 0;
            changedAtNs = 0;
            cancelled.set(false);
            poolName = null;
            workerId = -1;
            principal = null;
            state = State.IDLE;
            isWAL = false;
        }

        public AtomicBoolean getCancelled() {
            return cancelled;
        }

        public long getChangedAtNs() {
            return changedAtNs;
        }

        public CharSequence getPoolName() {
            return poolName;
        }

        public CharSequence getPrincipal() {
            return principal;
        }

        public StringSink getQuery() {
            return query;
        }

        public long getRegisteredAtNs() {
            return registeredAtNs;
        }

        public byte getState() {
            return state;
        }

        public String getStateText() {
            return State.getText(state);
        }

        public long getWorkerId() {
            return workerId;
        }

        public boolean isWAL() {
            return isWAL;
        }

        public static class State {
            public static final byte ACTIVE = 2;
            public static final byte CANCELLED = (byte) (ACTIVE + 1);
            public static final byte IDLE = 1;

            private State() {
            }

            public static String getText(byte state) {
                return switch (state) {
                    case IDLE -> "idle";
                    case ACTIVE -> "active";
                    case CANCELLED -> "cancelled";
                    default -> "unknown state";
                };
            }
        }
    }
}
