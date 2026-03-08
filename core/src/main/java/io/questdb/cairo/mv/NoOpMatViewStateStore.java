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

package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

public class NoOpMatViewStateStore implements MatViewStateStore {
    public static final NoOpMatViewStateStore INSTANCE = new NoOpMatViewStateStore();

    @Override
    public MatViewState addViewState(MatViewDefinition viewDefinition) {
        return null;
    }

    @TestOnly
    @Override
    public void clear() {
    }

    @Override
    public void close() {
    }

    @Override
    public void createViewState(MatViewDefinition viewDefinition) {
    }

    @Override
    public void enqueueFullRefresh(TableToken matViewToken) {
    }

    @Override
    public void enqueueIncrementalRefresh(TableToken matViewToken) {
    }

    @Override
    public void enqueueInvalidate(TableToken matViewToken, String invalidationReason) {
    }

    @Override
    public void enqueueInvalidateDependentViews(TableToken baseTableToken, String invalidationReason) {
    }

    @Override
    public void enqueueRangeRefresh(TableToken matViewToken, long rangeFrom, long rangeTo) {
    }

    @Override
    public void enqueueUpdateRefreshIntervals(TableToken matViewToken) {
    }

    @Override
    public MatViewState getViewState(TableToken matViewToken) {
        return null;
    }

    @Override
    public void notifyBaseInvalidated(TableToken baseTableToken) {
    }

    @Override
    public void notifyBaseRefreshed(MatViewRefreshTask task, long seqTxn) {
    }

    @Override
    public void notifyBaseTableCommit(MatViewRefreshTask task, long seqTxn) {
    }

    @Override
    public void removeViewState(TableToken matViewToken) {
    }

    @Override
    public boolean tryDequeueRefreshTask(MatViewRefreshTask task) {
        return false;
    }

    @Override
    public void updateViewDefinition(@NotNull TableToken matViewToken, @NotNull MatViewDefinition newDefinition) {
    }
}
