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
 * Governs whether this node forces materialized WAL-apply state locally durable under
 * {@link io.questdb.cairo.CommitMode#ADAPTIVE}. Installed on {@link io.questdb.cairo.CairoEngine}
 * and consulted once per apply batch in
 * {@code ApplyWal2TableJob.maybeAdvanceDurableEpoch}.
 *
 * <p>Fail-safe polarity: the OSS default is {@link #ALWAYS_ON}. Only a definitively-live Enterprise
 * replica installs {@link #REPLICA_SKIP}; every other state (single-node, primary, transitional) is
 * always-on, so a node is never accidentally under-durable.
 */
@FunctionalInterface
public interface LocalDurabilityPolicy {

    /**
     * OSS default and primary / single-node behavior: always fire the adaptive durable epoch. The
     * local disk holds not-yet-uploaded truth, so it must be forced durable.
     */
    LocalDurabilityPolicy ALWAYS_ON = () -> true;

    /**
     * Installed by Enterprise while a node is a replica: skip the adaptive durable epoch. A
     * replica's applied columns are a rebuildable cache of object-store truth (recovery =
     * re-download + re-apply via the WalDownloader), so the per-batch {@code fsyncMaterializedState}
     * + durable epoch copies are redundant I/O.
     */
    LocalDurabilityPolicy REPLICA_SKIP = () -> false;

    /**
     * @return true iff this node should fire the adaptive apply-side durable epoch.
     */
    boolean isLocalDurabilityEnabled();

    /**
     * Resolve the effective commit mode for a durability decision under {@code policy}. Under
     * {@link CommitMode#ADAPTIVE}, when local durability is disabled (a replica), downgrade to
     * {@link CommitMode#NOSYNC} — the written state is a rebuildable cache of object-store truth, so
     * the sync is redundant. An explicitly-declared {@code SYNC}/{@code ASYNC} mode is preserved
     * unchanged (only {@code ADAPTIVE} is policy-sensitive), mirroring the epoch gate's
     * {@code getEffectiveCommitMode() != ADAPTIVE} precondition.
     */
    static int resolveCommitMode(int commitMode, LocalDurabilityPolicy policy) {
        return commitMode == CommitMode.ADAPTIVE && !policy.isLocalDurabilityEnabled()
                ? CommitMode.NOSYNC
                : commitMode;
    }
}
