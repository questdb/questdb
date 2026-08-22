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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Mutable;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public class NetworkSqlExecutionCircuitBreaker implements SqlExecutionCircuitBreaker, Closeable, Mutable {
    private final MillisecondClock clock;
    private final SqlExecutionCircuitBreakerConfiguration configuration;
    private final long connectionCheckThrottle;
    private final long defaultMaxTime;
    private final CairoEngine engine;
    private final NetworkFacade nf;
    private final int throttle;
    private volatile AtomicBoolean cancelledFlag;
    private long fd = -1;
    private boolean isClosed;
    // Wall-clock time (millis) of the last heavy connection probe; gates the throttled probes in
    // statefulThrowExceptionIfTrippedTimeThrottled(), checkIfTripped(long, long) and getState(long, long).
    // Written without synchronization: consults of a shared instance must never be concurrent - the
    // thread currently driving the connection owns it (dispatcher handoffs between requests are
    // sequential); worker threads operate on per-worker wrapper copies.
    private long lastConnectionCheckTime;
    private volatile long powerUpTime = Long.MAX_VALUE;
    private int secret;
    private int testCount;
    private long timeout;

    public NetworkSqlExecutionCircuitBreaker(CairoEngine engine, @NotNull SqlExecutionCircuitBreakerConfiguration configuration) {
        this.configuration = configuration;
        this.nf = configuration.getNetworkFacade();
        this.throttle = configuration.getCircuitBreakerThrottle();
        this.connectionCheckThrottle = configuration.getCircuitBreakerConnectionCheckThrottle();
        this.clock = configuration.getClock();
        long timeout = configuration.getQueryTimeout();
        if (timeout > 0) {
            this.timeout = timeout;
        } else if (timeout == TIMEOUT_FAIL_ON_FIRST_CHECK) {
            this.timeout = -100;
        } else {
            this.timeout = Long.MAX_VALUE;
        }
        this.defaultMaxTime = this.timeout;
        this.engine = engine;
    }

    @Override
    public void cancel() {
        powerUpTime = Long.MIN_VALUE;
        // This call can be concurrent with the call to setCancelledFlag
        AtomicBoolean cf = cancelledFlag;
        if (cf != null) {
            cf.set(true);
        }
    }

    @Override
    public boolean checkIfTripped() {
        return checkIfTripped(powerUpTime, fd);
    }

    @Override
    public boolean checkIfTripped(long millis, long fd) {
        final long now = clock.getTicks();
        if (now - timeout > millis) {
            return true;
        }
        if ((cancelledFlag != null && cancelledFlag.get()) || engine.isClosing()) {
            return true;
        }
        return testConnectionTimeThrottled(now, fd);
    }

    @Override
    public boolean checkIfTrippedNoThrottle() {
        final long now = clock.getTicks();
        if (now - timeout > powerUpTime) {
            return true;
        }
        if ((cancelledFlag != null && cancelledFlag.get()) || engine.isClosing()) {
            return true;
        }
        lastConnectionCheckTime = now;
        return testConnection(fd);
    }

    public void clear() {
        secret = -1;
        powerUpTime = Long.MAX_VALUE;
        testCount = 0;
        lastConnectionCheckTime = 0;
        fd = -1;
        timeout = defaultMaxTime;
    }

    public void clearCancelSentinel() {
        // Drop a cancel left by a prior, finished query so it cannot trip the next one on this reused
        // breaker. Guarded per-query resets cannot clear it (isTimerSet() reports MIN_VALUE as "set").
        if (isCancelled()) {
            unsetTimer();
        }
    }

    @Override
    public void close() {
        isClosed = true;
        fd = -1;
    }

    @Override
    public AtomicBoolean getCancelledFlag() {
        return cancelledFlag;
    }

    @Override
    public SqlExecutionCircuitBreakerConfiguration getConfiguration() {
        return configuration;
    }

    public long getDefaultMaxTime() {
        return defaultMaxTime;
    }

    @Override
    public long getFd() {
        return fd;
    }

    public int getSecret() {
        return secret;
    }

    @Override
    public int getState() {
        return getState(powerUpTime, fd);
    }

    @Override
    public int getState(long millis, long fd) {
        // A cancelled breaker carries the powerUpTime == MIN_VALUE sentinel, which the overflow-safe
        // timeout predicate below would otherwise report as STATE_TIMEOUT. Classify it as cancelled so
        // callers reading getState() see an honest reason.
        if (isCancelled()) {
            return STATE_CANCELLED;
        }
        final long now = clock.getTicks();
        if (now - timeout > millis) {
            return STATE_TIMEOUT;
        }
        if ((cancelledFlag != null && cancelledFlag.get()) || engine.isClosing()) {
            return STATE_CANCELLED;
        }
        if (testConnectionTimeThrottled(now, fd)) {
            return STATE_BROKEN_CONNECTION;
        }
        return STATE_OK;
    }

    @Override
    public long getTimeout() {
        return timeout;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public boolean isTimerSet() {
        return powerUpTime < Long.MAX_VALUE;
    }

    public NetworkSqlExecutionCircuitBreaker of(long fd) {
        assert !isClosed;
        testCount = 0;
        if (this.fd != fd) {
            lastConnectionCheckTime = 0;
        }
        this.fd = fd;
        return this;
    }

    // Re-arms the timeout timer without zeroing the connection-check window. Used by
    // SqlExecutionCircuitBreakerWrapper.init(), which runs once per reduce task: the full
    // resetTimer() would let the first probe after every task bypass the throttle.
    public void rearmTimer() {
        powerUpTime = clock.getTicks();
        testCount = 0;
    }

    public void resetMaxTimeToDefault() {
        this.timeout = defaultMaxTime;
    }

    @Override
    public void resetTimer() {
        powerUpTime = clock.getTicks();
        // Start a fresh throttle window for the new query, so the next breaker consultation performs a
        // real check. Without this, a single-shot check (open/build/pre-dispatch over an empty base)
        // could fall in the middle of a throttle window and never test cancellation/timeout.
        testCount = 0;
        // Force a prompt connection probe at the start of the new query (lastConnectionCheckTime=0 makes
        // the first time-throttled check fall outside any window for a real wall-clock).
        lastConnectionCheckTime = 0;
    }

    @Override
    public void setCancelledFlag(AtomicBoolean cancelledFlag) {
        this.cancelledFlag = cancelledFlag;
    }

    @Override
    public void setFd(long fd) {
        this.fd = fd;
    }

    public void setSecret(int secret) {
        this.secret = secret;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public void statefulThrowExceptionIfTripped() {
        // Always perform a real check on the first call after a reset (testCount == 0), so empty/instant
        // queries that consult the breaker only a handful of times (single-shot open/build/pre-dispatch
        // checks over an empty base) still observe a tripped breaker. Otherwise test once per throttle
        // window to keep hot per-row/per-frame loops cheap.
        if (testCount == 0 || testCount >= throttle) {
            statefulThrowExceptionIfTrippedNoThrottle(); // performs the real test and resets testCount to 0
        }
        testCount++;
    }

    @Override
    public void statefulThrowExceptionIfTrippedNoThrottle() {
        final long now = clock.getTicks();
        testCount = 0;
        testTimeout(now);
        testCancelled();
        lastConnectionCheckTime = now;
        if (testConnection(fd)) {
            throw CairoException.nonCritical().put("remote disconnected, query aborted [fd=").put(fd).put(']').setInterruption(true);
        }
    }

    @Override
    public void statefulThrowExceptionIfTrippedTimeThrottled() {
        final long now = clock.getTicks();
        // Cancellation and timeout are cheap (no syscall) and must run every call so the query stays
        // promptly cancellable even at a per-frame, nested-loop-re-scanned call site.
        testTimeout(now);
        testCancelled();
        // Throttle only the heavy connection probe (a hangup-poll syscall) by elapsed wall-clock time.
        // The state lives on this breaker, which the execution context shares across every cursor in the
        // query, so the probe fires at most once per window for the whole query - a big CROSS JOIN small
        // that re-scans the slave once per master row can no longer turn into one syscall per master row.
        if (testConnectionTimeThrottled(now, fd)) {
            throw CairoException.nonCritical().put("remote disconnected, query aborted [fd=").put(fd).put(']').setInterruption(true);
        }
    }

    @Override
    public void unsetTimer() {
        powerUpTime = Long.MAX_VALUE;
    }

    private boolean isCancelled() {
        return powerUpTime == Long.MIN_VALUE;
    }

    private void testCancelled() {
        // isCancelled() (the powerUpTime == MIN_VALUE sentinel) must come first: cancel() sets the
        // sentinel unconditionally but only flips cancelledFlag when it is already attached. A cancel
        // that races QueryRegistry.register() before it binds the per-query flag leaves only the
        // sentinel, and testTimeout()'s now - MIN_VALUE arithmetic overflows without tripping, so this
        // is the single place that reliably turns such a cancel into a thrown queryCancelled().
        if (isCancelled() || (cancelledFlag != null && cancelledFlag.get()) || engine.isClosing()) {
            throw CairoException.queryCancelled(fd);
        }
    }

    private boolean testConnectionTimeThrottled(long now, long fd) {
        if (now - lastConnectionCheckTime >= connectionCheckThrottle) {
            lastConnectionCheckTime = now;
            return testConnection(fd);
        }
        return false;
    }

    private void testTimeout(long now) {
        long runtime = now - powerUpTime;
        if (runtime > timeout) {
            if (isCancelled()) {
                throw CairoException.queryCancelled(fd);
            } else {
                throw CairoException.queryTimedOut(fd, runtime, timeout);
            }
        }
    }

    protected boolean testConnection(long fd) {
        if (fd == -1 || !configuration.checkConnection()) {
            return false;
        }
        return nf.testConnection(fd, 0, 0);
    }
}
