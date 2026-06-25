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

package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A stable {@link MatViewStateStore} that forwards every call to a swappable inner delegate.
 * <p>
 * The engine installs one instance of this wrapper for its whole life and never replaces the field;
 * a role switch swaps the inner delegate instead (NoOp on a read-only replica, a real
 * {@link MatViewStateStoreImpl} on a primary). Because consumers such as
 * {@link MatViewRefreshJob} and {@link MatViewTimerJob} cache the result of
 * {@code engine.getMatViewStateStore()} at construction, swapping the field directly would strand
 * those cached references on the stale store. Forwarding through a stable wrapper makes a single
 * delegate write visible to every cached holder at once.
 * <p>
 * The delegate reference is volatile so a swap on the lifecycle thread is visible to refresh and
 * apply worker threads without external synchronization. A swap returns the previous delegate so the
 * caller can close it after it is no longer reachable.
 */
public class ForwardingMatViewStateStore implements MatViewStateStore {
    private volatile MatViewStateStore delegate;

    public ForwardingMatViewStateStore(@NotNull MatViewStateStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public MatViewState addViewState(MatViewDefinition viewDefinition) {
        return delegate.addViewState(viewDefinition);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void createViewState(MatViewDefinition viewDefinition) {
        delegate.createViewState(viewDefinition);
    }

    @Override
    public void enqueueFullRefresh(TableToken matViewToken) {
        delegate.enqueueFullRefresh(matViewToken);
    }

    @Override
    public void enqueueIncrementalRefresh(TableToken matViewToken) {
        delegate.enqueueIncrementalRefresh(matViewToken);
    }

    @Override
    public void enqueueInvalidate(TableToken matViewToken, String invalidationReason) {
        delegate.enqueueInvalidate(matViewToken, invalidationReason);
    }

    @Override
    public void enqueueInvalidateDependentViews(TableToken baseTableToken, String invalidationReason) {
        delegate.enqueueInvalidateDependentViews(baseTableToken, invalidationReason);
    }

    @Override
    public void enqueueRangeRefresh(TableToken matViewToken, long rangeFrom, long rangeTo) {
        delegate.enqueueRangeRefresh(matViewToken, rangeFrom, rangeTo);
    }

    @Override
    public void enqueueUpdateRefreshIntervals(TableToken matViewToken) {
        delegate.enqueueUpdateRefreshIntervals(matViewToken);
    }

    public @NotNull MatViewStateStore getDelegate() {
        return delegate;
    }

    @Override
    public @Nullable MatViewState getViewState(TableToken matViewToken) {
        return delegate.getViewState(matViewToken);
    }

    @Override
    public void notifyBaseInvalidated(TableToken baseTableToken) {
        delegate.notifyBaseInvalidated(baseTableToken);
    }

    @Override
    public void notifyBaseRefreshed(MatViewRefreshTask task, long seqTxn) {
        delegate.notifyBaseRefreshed(task, seqTxn);
    }

    @Override
    public void notifyBaseTableCommit(MatViewRefreshTask task, long seqTxn) {
        delegate.notifyBaseTableCommit(task, seqTxn);
    }

    @Override
    public void reenqueueRefreshTask(MatViewRefreshTask task) {
        delegate.reenqueueRefreshTask(task);
    }

    @Override
    public void removeViewState(TableToken matViewToken) {
        delegate.removeViewState(matViewToken);
    }

    /**
     * Swaps the inner delegate and returns the previous one. The caller closes the returned delegate
     * after the swap, once it is unreachable. A no-op when the new delegate is identical to the
     * current one.
     */
    public MatViewStateStore swapDelegate(@NotNull MatViewStateStore newDelegate) {
        final MatViewStateStore previous = delegate;
        if (previous == newDelegate) {
            return null;
        }
        delegate = newDelegate;
        return previous;
    }

    @Override
    public boolean tryDequeueRefreshTask(MatViewRefreshTask task) {
        return delegate.tryDequeueRefreshTask(task);
    }

    @Override
    public void updateViewDefinition(@NotNull TableToken matViewToken, @NotNull MatViewDefinition newDefinition) {
        delegate.updateViewDefinition(matViewToken, newDefinition);
    }
}
