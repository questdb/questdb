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

import io.questdb.cairo.TableToken;

/**
 * Tracks the highest sequencer txn that has been durably persisted (uploaded to
 * the configured object store) for each WAL table. Used by QWP to emit a second
 * "durable" acknowledgment frame to clients that opt in.
 * <p>
 * The OSS server ships {@link LocalDurableAckRegistry} as the default, which tracks the
 * local-fsync tier (ADAPTIVE commit mode). Enterprise installations with primary replication
 * enabled override this via {@link io.questdb.cairo.CairoEngine#setDurableAckRegistry} with an
 * implementation backed by the upload pipeline. {@link DefaultDurableAckRegistry} is a
 * legacy no-op kept for tests and Enterprise composition.
 */
public interface DurableAckRegistry {

    /**
     * Returns the highest seqTxn durably REPLICATED (uploaded to the configured object store) for
     * the given table, or -1 if nothing has been replicated yet, the table is unknown to the
     * registry, or durable-ack tracking is not enabled on this server. This is the
     * {@link DurabilityTier#REPLICATED} (failover-safe) frontier.
     *
     * @param tableDirName the directory name of the table (matches
     *                     {@code TableToken.getDirName()})
     * @return the highest durably-replicated seqTxn, or -1
     */
    long getReplicatedDurableSeqTxn(CharSequence tableDirName);

    /**
     * Returns the highest seqTxn whose WAL commit was fdatasync'd locally for the given table,
     * or -1 if no local-fsync guarantee has been established (e.g. NOSYNC tables, unknown dir,
     * or a registry that does not track local durability).
     *
     * <p>The local-durable frontier is a weaker tier than the replicated frontier:
     * {@code applied >= localDurable >= replicated} in the durability ordering.
     *
     * <p>Default implementation returns -1 (no local-fsync tracking). Override in
     * {@code LocalDurableAckRegistry} (OSS default) which reads from the table's
     * {@link io.questdb.cairo.wal.seq.SeqTxnTracker#getLocalDurableSeqTxn()}.
     *
     * @param tableDirName the directory name of the table (matches
     *                     {@code TableToken.getDirName()})
     * @return the highest locally-fdatasync'd seqTxn, or -1
     */
    default long getLocalDurableSeqTxn(CharSequence tableDirName) {
        return -1L;
    }

    /**
     * Returns true when durable-ack tracking is wired up on this server (i.e.
     * primary replication to an object store is enabled). When false, QWP
     * silently ignores the {@code X-QWP-Request-Durable-Ack} opt-in header.
     */
    boolean isEnabled();

    /**
     * Invoked after a table, view or materialized view has been dropped so the
     * registry can release any state keyed by that table's directory name.
     * Late uploads arriving after the drop may re-create a short-lived entry;
     * that orphan is bounded by the uploader's in-flight queue depth at drop
     * time and is acceptable.
     */
    default void onTableDropped(TableToken tableToken) {
    }

    /**
     * Whether this server can offer the given {@link DurabilityTier}. Availability is server-level;
     * an offered tier may still report -1 for a table that cannot satisfy it (e.g. a NOSYNC table
     * under the LOCAL tier). Default: no tier available; concrete registries override.
     */
    default boolean isTierAvailable(int tier) {
        return false;
    }

    /**
     * The strongest tier this server can offer, or {@link DurabilityTier#NONE}.
     */
    default int strongestAvailableTier() {
        if (isTierAvailable(DurabilityTier.REPLICATED)) {
            return DurabilityTier.REPLICATED;
        }
        if (isTierAvailable(DurabilityTier.LOCAL)) {
            return DurabilityTier.LOCAL;
        }
        return DurabilityTier.NONE;
    }
}
