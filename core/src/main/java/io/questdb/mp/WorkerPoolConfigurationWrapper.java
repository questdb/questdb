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

package io.questdb.mp;

import io.questdb.Metrics;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class WorkerPoolConfigurationWrapper implements DynamicFiberWorkerPoolConfiguration {
    private final AtomicBoolean isNotifying = new AtomicBoolean();
    private final AtomicReference<State> state = new AtomicReference<>(new State(null, null, 0));

    @Override
    public FiberConfiguration getFiberConfiguration() {
        final WorkerPoolConfiguration delegate = getDelegate();
        return new FiberConfiguration(
                delegate.getFiberMaxLiveCount(),
                delegate.getFiberRetainedCount(),
                delegate.getFiberMountBudget()
        );
    }

    public WorkerPoolConfiguration getDelegate() {
        return state.get().delegate;
    }

    @Override
    public int getFiberMaxLiveCount() {
        return getDelegate().getFiberMaxLiveCount();
    }

    @Override
    public int getFiberMountBudget() {
        return getDelegate().getFiberMountBudget();
    }

    @Override
    public int getFiberRetainedCount() {
        return getDelegate().getFiberRetainedCount();
    }

    @Override
    public Metrics getMetrics() {
        return getDelegate().getMetrics();
    }

    @Override
    public long getNapThreshold() {
        return getDelegate().getNapThreshold();
    }

    @Override
    public String getPoolName() {
        return getDelegate().getPoolName();
    }

    @Override
    public long getSleepThreshold() {
        return getDelegate().getSleepThreshold();
    }

    @Override
    public long getSleepTimeout() {
        return getDelegate().getSleepTimeout();
    }

    @Override
    public int[] getWorkerAffinity() {
        return getDelegate().getWorkerAffinity();
    }

    @Override
    public int getWorkerCount() {
        return getDelegate().getWorkerCount();
    }

    @Override
    public WorkerPoolMode getWorkerPoolMode() {
        return getDelegate().getWorkerPoolMode();
    }

    @Override
    public long getYieldThreshold() {
        return getDelegate().getYieldThreshold();
    }

    @Override
    public boolean haltOnError() {
        return getDelegate().haltOnError();
    }

    @Override
    public boolean isDaemonPool() {
        return getDelegate().isDaemonPool();
    }

    @Override
    public boolean isEnabled() {
        return getDelegate().isEnabled();
    }

    public void setDelegate(WorkerPoolConfiguration delegate) {
        if (delegate == null) {
            throw new IllegalArgumentException("worker pool configuration delegate must not be null");
        }
        while (true) {
            final State current = state.get();
            final State next = new State(delegate, current.listener, current.version + 1);
            if (state.compareAndSet(current, next)) {
                break;
            }
        }
        notifyFiberConfigurationListener();
    }

    @Override
    public void setFiberConfigurationListener(FiberConfigurationListener listener) {
        while (true) {
            final State current = state.get();
            final State next = new State(current.delegate, listener, current.version + 1);
            if (state.compareAndSet(current, next)) {
                break;
            }
        }
        notifyFiberConfigurationListener();
    }

    @Override
    public int workerPoolPriority() {
        return getDelegate().workerPoolPriority();
    }

    private void notifyFiberConfigurationListener() {
        if (!isNotifying.compareAndSet(false, true)) {
            return;
        }
        Error error = null;
        RuntimeException runtimeException = null;
        long notifiedVersion = Long.MIN_VALUE;
        while (true) {
            final State current = state.get();
            if (current.version != notifiedVersion) {
                notifiedVersion = current.version;
                if (current.listener != null && current.delegate != null) {
                    try {
                        final int maxLiveCount = current.delegate.getFiberMaxLiveCount();
                        final int retainedCount = current.delegate.getFiberRetainedCount();
                        final int mountBudget = current.delegate.getFiberMountBudget();
                        current.listener.onConfigurationChanged(maxLiveCount, retainedCount, mountBudget);
                    } catch (Error e) {
                        if (error == null) {
                            error = e;
                        }
                    } catch (RuntimeException e) {
                        if (runtimeException == null) {
                            runtimeException = e;
                        }
                    }
                }
                continue;
            }

            isNotifying.set(false);
            if (state.get().version == notifiedVersion || !isNotifying.compareAndSet(false, true)) {
                break;
            }
        }
        if (error != null) {
            throw error;
        }
        if (runtimeException != null) {
            throw runtimeException;
        }
    }

    private static final class State {
        private final WorkerPoolConfiguration delegate;
        private final FiberConfigurationListener listener;
        private final long version;

        private State(
                WorkerPoolConfiguration delegate,
                FiberConfigurationListener listener,
                long version
        ) {
            this.delegate = delegate;
            this.listener = listener;
            this.version = version;
        }
    }
}
