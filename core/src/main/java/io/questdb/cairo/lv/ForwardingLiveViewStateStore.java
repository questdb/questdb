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

package io.questdb.cairo.lv;

import io.questdb.cairo.TableToken;
import org.jetbrains.annotations.NotNull;

/**
 * A stable {@link LiveViewStateStore} that forwards every call to a swappable inner delegate.
 * <p>
 * Because {@link LiveViewRefreshJob} caches the result of {@code engine.getLiveViewStateStore()} at
 * construction, swapping the engine field directly would strand that cached reference on the stale
 * store; forwarding through a stable wrapper would make a single delegate write visible to every
 * cached holder at once.
 * <p>
 * That is the wrapper's purpose, not a contract the engine currently honours: {@code CairoEngine.load()}
 * reassigns the {@code liveViewStateStore} field itself, on live views and mat views alike. It is
 * harmless today because every {@code LiveViewRefreshJob} is constructed after {@code load()} - the
 * worker pool depends on the engine component, and the components start in one serial topological
 * pass - and because a job stranded on a stale real store still refreshes: its
 * {@code isRefreshEnabled()} is unconditionally true and the lagging-view scan reads the registry,
 * a final engine field {@code load()} never touches. The cost of a second {@code load()} is
 * notification latency, not stopped refresh.
 * <p>
 * <b>Nothing swaps it today.</b> Under symmetric local refresh a live view is node-local derived
 * data that every node refreshes for itself, so a role switch is continuation rather than
 * reconstruction and both roles run the same store. The delegate is therefore chosen once, at
 * construction, by {@code CairoEngine.createLiveViewStateStore}: a real {@link LiveViewStateStoreImpl}
 * when live views are enabled, {@link NoOpLiveViewStateStore} when the feature is off. {@link
 * #swapDelegate} has no production caller — it is kept because it is what makes the wrapper worth
 * having if a future role-dependent store returns, and because it mirrors the mat-view side, which
 * does swap. Treat a monomorphic delegate as the current invariant, not as an accident.
 * <p>
 * The delegate reference is volatile so a swap on the lifecycle thread would be visible to refresh
 * worker threads without external synchronization. A swap returns the previous delegate so the
 * caller can close it once it is unreachable.
 */
public class ForwardingLiveViewStateStore implements LiveViewStateStore {
    private volatile LiveViewStateStore delegate;

    public ForwardingLiveViewStateStore(@NotNull LiveViewStateStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public void close() {
        delegate.close();
    }

    public @NotNull LiveViewStateStore getDelegate() {
        return delegate;
    }

    @Override
    public boolean isRefreshEnabled() {
        return delegate.isRefreshEnabled();
    }

    @Override
    public void notifyBaseRefreshed(LiveViewRefreshTask task, long seqTxn) {
        delegate.notifyBaseRefreshed(task, seqTxn);
    }

    @Override
    public void notifyBaseTableCommit(TableToken baseTableToken, long seqTxn) {
        delegate.notifyBaseTableCommit(baseTableToken, seqTxn);
    }

    @Override
    public void registerBaseTable(CharSequence baseTableName) {
        delegate.registerBaseTable(baseTableName);
    }

    /**
     * Swaps the inner delegate and returns the previous one. The caller closes the returned delegate
     * after the swap, once it is unreachable. A no-op when the new delegate is identical to the
     * current one.
     */
    public LiveViewStateStore swapDelegate(@NotNull LiveViewStateStore newDelegate) {
        final LiveViewStateStore previous = delegate;
        if (previous == newDelegate) {
            return null;
        }
        delegate = newDelegate;
        return previous;
    }

    @Override
    public boolean tryDequeueRefreshTask(LiveViewRefreshTask target) {
        return delegate.tryDequeueRefreshTask(target);
    }
}
