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

package io.questdb.cairo.wal;

import io.questdb.cairo.CommitMode;

/**
 * Decides whether this node makes materialized WAL-apply state locally durable under
 * {@link io.questdb.cairo.CommitMode#ADAPTIVE}. {@link io.questdb.cairo.CairoEngine} holds it, and
 * {@code ApplyWal2TableJob.maybeAdvanceDurableEpoch} consults it once per apply batch, as do the WAL purge floor
 * and every place that publishes or reconciles a table's save point.
 *
 * <p>Fail-safe polarity: the default is {@link #ALWAYS_ON}. Only a live Enterprise replica installs
 * {@link #REPLICA_SKIP}; every other state (single node, primary, transitional) is always on, so a node is
 * never under-durable by accident.
 */
@FunctionalInterface
public interface LocalDurabilityPolicy {

    /**
     * The default, for a primary or a single node: take the adaptive save point. The local disk may hold the
     * only copy of the data, so it must be made durable.
     */
    LocalDurabilityPolicy ALWAYS_ON = () -> true;

    /**
     * Installed by Enterprise while a node is a replica: skip the adaptive save point and the per-batch
     * {@code fsyncMaterializedState}. A replica keeps its applied columns at {@link CommitMode#NOSYNC} grade;
     * the object store holds the durable copy.
     * <p>
     * Nothing downloads WAL again after a crash: the downloader resumes from the local {@code _txnlog}. A
     * replica therefore keeps no save point that startup would rewind the table to, because the WAL purge job
     * keeps no floor here and the replay would need WAL it has deleted; startup leaves each table at its live
     * cut. A table is made durable before its save point is removed, since nothing restores its lazily applied
     * state afterwards. See {@code TableWriter.reconcileDurableEpochAnchor}.
     */
    LocalDurabilityPolicy REPLICA_SKIP = () -> false;

    /**
     * @return true if this node takes adaptive save points when it applies WAL
     */
    boolean isLocalDurabilityEnabled();

    /**
     * Resolves the effective commit mode for a durability decision under {@code policy}:
     * {@link CommitMode#ADAPTIVE} becomes {@link CommitMode#NOSYNC} when local durability is disabled (a
     * replica, whose object store holds the durable copy). Every other mode is returned unchanged, matching
     * the save point gate's {@code getEffectiveCommitMode() != ADAPTIVE} precondition.
     */
    static int resolveCommitMode(int commitMode, LocalDurabilityPolicy policy) {
        return commitMode == CommitMode.ADAPTIVE && !policy.isLocalDurabilityEnabled()
                ? CommitMode.NOSYNC
                : commitMode;
    }
}
