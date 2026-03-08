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
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Mat view state store serves {@link MatViewRefreshJob} synchronization and coordination needs.
 * It holds per-view states and a task queue.
 * <p>
 * The actual implementation may be no-op in case of disabled mat views or read-only replica.
 * Due to this, outside the refresh job the state should be accessed by reading the _mv.s file
 * instead of accessing the store - see {@link MatViewStateReader}.
 */
public interface MatViewStateStore extends QuietCloseable, Mutable {

    // Only creates the view state, no refresh initiated, no telemetry event logged.
    MatViewState addViewState(MatViewDefinition viewDefinition);

    @TestOnly
    @Override
    void clear();

    // Creates the view state and logs telemetry event.
    void createViewState(MatViewDefinition viewDefinition);

    void enqueueFullRefresh(TableToken matViewToken);

    void enqueueIncrementalRefresh(TableToken matViewToken);

    void enqueueInvalidate(TableToken matViewToken, String invalidationReason);

    void enqueueInvalidateDependentViews(TableToken baseTableToken, String invalidationReason);

    void enqueueRangeRefresh(TableToken matViewToken, long rangeFrom, long rangeTo);

    // Used to cache WAL txn intervals for manual and timer mat views.
    // That's to let WalPurgeJob make progress.
    void enqueueUpdateRefreshIntervals(TableToken matViewToken);

    @Nullable
    MatViewState getViewState(TableToken matViewToken);

    // Called by refresh job once it invalidated dependent mat views.
    void notifyBaseInvalidated(TableToken baseTableToken);

    // Called by refresh job once it incrementally refreshed dependent mat views.
    void notifyBaseRefreshed(MatViewRefreshTask task, long seqTxn);

    // Called by WAL apply job once it applied transaction(s) to the base table
    // and wants to send refresh job an incremental refresh message.
    void notifyBaseTableCommit(MatViewRefreshTask task, long seqTxn);

    void removeViewState(TableToken matViewToken);

    boolean tryDequeueRefreshTask(MatViewRefreshTask task);

    void updateViewDefinition(@NotNull TableToken matViewToken, @NotNull MatViewDefinition newDefinition);
}
