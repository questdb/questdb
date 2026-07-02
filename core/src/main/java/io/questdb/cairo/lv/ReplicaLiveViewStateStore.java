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
 * A {@link LiveViewStateStore} for a read-only replica that wants live-view freshness parity with
 * the primary. Like {@link NoOpLiveViewStateStore} it drops every queue/notification call and keeps
 * {@link #isRefreshEnabled()} false -- so the refresh workers never flush, apply, or advance a
 * durable watermark (the on-disk tier is fed by the global apply job from replicated WAL). Unlike
 * NoOp it returns {@link #isLeadReconstructionEnabled()} true, so {@link LiveViewRefreshJob} runs the
 * compute-lead-only pass (the registry fallback scan, then a lead refresh with no LV WAL write) that
 * rebuilds each view's un-flushed lead in RAM. That makes a replica's live-view reads current with
 * the primary instead of trailing by up to one FLUSH EVERY interval.
 * <p>
 * Installed as the {@link ForwardingLiveViewStateStore} delegate on a read-only replica when live
 * views are enabled; a promote swaps in a real {@link LiveViewStateStoreImpl}, a demote swaps this
 * one back in (see the enterprise {@code EntCairoEngine.switchLiveViewMachinery}).
 */
public class ReplicaLiveViewStateStore implements LiveViewStateStore {
    public static final ReplicaLiveViewStateStore INSTANCE = new ReplicaLiveViewStateStore();

    private ReplicaLiveViewStateStore() {
    }

    @Override
    public void clear() {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isLeadReconstructionEnabled() {
        return true;
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
