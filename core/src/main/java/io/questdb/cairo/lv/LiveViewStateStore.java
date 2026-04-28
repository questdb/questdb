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
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;

/**
 * Per-engine coordination for {@link LiveViewRefreshJob} instances: a refresh task
 * queue plus a per-base-table "last notified seqTxn" counter used to collapse bursts
 * of WAL commits into a single queued task while a refresh is in flight.
 * <p>
 * Live views read from WAL segments directly (see {@link WalSegmentPageFrameCursor}),
 * so notifications can fan out from the sequencer at commit time rather than from
 * the WAL apply job; refresh may legitimately race ahead of the base-table reader
 * and there is no base-table-txn-vs-notified-txn reconciliation to perform.
 */
public interface LiveViewStateStore extends QuietCloseable {

    /**
     * Enqueues a force-drain refresh task for the given base table. Used by the
     * {@code LiveViewTimerJob} to flush rows held back in the LAG window after
     * the base table has gone idle. Unlike {@link #notifyBaseTableCommit}, this
     * does not advance the dedup gate, so it coexists with a concurrent WAL
     * commit for the same base table.
     */
    void enqueueForceDrain(TableToken baseTableToken);

    /**
     * Registers a base table so that {@link #notifyBaseTableCommit} stops treating it
     * as uninteresting. Called from the live view registry on view create/reload; the
     * per-table entry is never removed (grow-only, bounded by distinct base tables
     * that ever had a live view).
     */
    void registerBaseTable(CharSequence baseTableName);

    /**
     * Called by the refresh job once incremental refresh completes at {@code seqTxn}.
     * If more commits have landed in the meantime, re-enqueues the task so the job
     * picks the view up again.
     */
    void notifyBaseRefreshed(LiveViewRefreshTask task, long seqTxn);

    /**
     * Called by the sequencer on every WAL commit. Cheaply skips tables that no live
     * view depends on, and deduplicates bursts of commits into a single queued task
     * while a refresh is in flight.
     */
    void notifyBaseTableCommit(TableToken baseTableToken, long seqTxn);

    /**
     * Polls the refresh task queue. Returns true when a task has been copied into
     * {@code target}; false when the queue is empty.
     */
    boolean tryDequeueRefreshTask(LiveViewRefreshTask target);

    @TestOnly
    void clear();
}
