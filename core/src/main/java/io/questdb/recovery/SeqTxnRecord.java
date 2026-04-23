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

package io.questdb.recovery;

import io.questdb.cairo.wal.WalUtils;

/**
 * Record from the sequencer transaction log ({@code txn_seq/_txnlog}).
 * Each record represents one committed WAL transaction: either a data commit
 * (positive walId), a DDL change (walId={@link WalUtils#METADATA_WALID}), or
 * a table drop (walId={@link WalUtils#DROP_TABLE_WAL_ID}).
 *
 * <p>V2 records carry min/max timestamps and row counts from the txnlog.
 * V1 records leave those at {@link TxnState#UNSET_LONG}, but they can be
 * enriched later from WAL event files via {@link #enrichFromEvent}.
 */
public final class SeqTxnRecord {
    private final long commitTimestamp;
    private final int segmentId;
    private final int segmentTxn;
    private final long structureVersion;
    private final long txn;
    private final int walId;
    // mutable: set from V2 txnlog or enriched from WAL events
    private long maxTimestamp;
    private long minTimestamp;
    private long rowCount;

    SeqTxnRecord(
            long txn,
            long structureVersion,
            int walId,
            int segmentId,
            int segmentTxn,
            long commitTimestamp,
            long minTimestamp,
            long maxTimestamp,
            long rowCount
    ) {
        this.txn = txn;
        this.structureVersion = structureVersion;
        this.walId = walId;
        this.segmentId = segmentId;
        this.segmentTxn = segmentTxn;
        this.commitTimestamp = commitTimestamp;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.rowCount = rowCount;
    }

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getRowCount() {
        return rowCount;
    }

    public int getSegmentId() {
        return segmentId;
    }

    public int getSegmentTxn() {
        return segmentTxn;
    }

    public long getStructureVersion() {
        return structureVersion;
    }

    public long getTxn() {
        return txn;
    }

    public int getWalId() {
        return walId;
    }

    public boolean isDdlChange() {
        return walId == WalUtils.METADATA_WALID;
    }

    public boolean isTableDrop() {
        return walId == WalUtils.DROP_TABLE_WAL_ID;
    }

    /**
     * Enriches this record with data from a WAL event when the txnlog is V1
     * (which does not store row counts or timestamps). Only overwrites fields
     * that are still at {@link TxnState#UNSET_LONG}.
     */
    void enrichFromEvent(long eventRowCount, long eventMinTimestamp, long eventMaxTimestamp) {
        if (rowCount == TxnState.UNSET_LONG && eventRowCount != TxnState.UNSET_LONG) {
            rowCount = eventRowCount;
        }
        if (minTimestamp == TxnState.UNSET_LONG && eventMinTimestamp != TxnState.UNSET_LONG) {
            minTimestamp = eventMinTimestamp;
        }
        if (maxTimestamp == TxnState.UNSET_LONG && eventMaxTimestamp != TxnState.UNSET_LONG) {
            maxTimestamp = eventMaxTimestamp;
        }
    }
}
