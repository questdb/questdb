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

/**
 * A {@link LiveViewStateStore} that drops every call. Installed as the delegate of
 * {@link ForwardingLiveViewStateStore} on a read-only replica so refresh workers stay idle:
 * {@link #notifyBaseTableCommit} ignores base-table commits (no task ever queues) and
 * {@link #isRefreshEnabled()} returns false, so {@link LiveViewRefreshJob} skips its whole pass --
 * both the notification drain and the registry fallback scan. A promote swaps the delegate to a real
 * {@link LiveViewStateStoreImpl}.
 */
public class NoOpLiveViewStateStore implements LiveViewStateStore {
    public static final NoOpLiveViewStateStore INSTANCE = new NoOpLiveViewStateStore();

    private NoOpLiveViewStateStore() {
    }

    @Override
    public void clear() {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isRefreshEnabled() {
        return false;
    }

    @Override
    public void notifyBaseRefreshed(LiveViewRefreshTask task, long seqTxn) {
    }

    @Override
    public void notifyBaseTableCommit(TableToken baseTableToken, long seqTxn) {
    }

    @Override
    public void registerBaseTable(CharSequence baseTableName) {
    }

    @Override
    public boolean tryDequeueRefreshTask(LiveViewRefreshTask target) {
        return false;
    }
}
