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
 * Forwards cached store references through a volatile delegate. A swap returns the
 * old delegate for the caller to close after it becomes unreachable.
 * <p>
 * Current engine startup chooses the delegate once, before constructing refresh jobs.
 * CairoEngine.load() replaces the wrapper itself; jobs must therefore be constructed
 * after load to receive notifications through the current store.
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
