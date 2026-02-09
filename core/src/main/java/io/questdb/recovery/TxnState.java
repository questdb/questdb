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

import io.questdb.std.ObjList;

/**
 * Parsed state from a {@code _txn} file. Contains the transaction header fields
 * (txn number, row counts, timestamps, versions), the symbol segment (per-symbol
 * committed/transient counts), and the partition segment (per-partition row counts,
 * name txns, flags).
 *
 * <p>Fields that could not be read are left at sentinel values ({@link #UNSET_INT},
 * {@link #UNSET_LONG}). Any issues encountered during parsing are accumulated in
 * the {@link ReadIssue} list.
 *
 * <p>Populated by {@link BoundedTxnReader}; setters are package-private.
 */
public final class TxnState {
    public static final int UNSET_INT = Integer.MIN_VALUE;
    public static final long UNSET_LONG = Long.MIN_VALUE;
    private final ObjList<ReadIssue> issues = new ObjList<>();
    private final ObjList<TxnPartitionState> partitions = new ObjList<>();
    private final ObjList<TxnSymbolState> symbols = new ObjList<>();
    private int lagRowCount = UNSET_INT;
    private int lagTxnCount = UNSET_INT;
    private int mapWriterCount = UNSET_INT;
    private int recordBaseOffset = UNSET_INT;
    private long baseVersion = UNSET_LONG;
    private long columnVersion = UNSET_LONG;
    private long dataVersion = UNSET_LONG;
    private long fileSize = -1;
    private long fixedRowCount = UNSET_LONG;
    private long lagMaxTimestamp = UNSET_LONG;
    private long lagMinTimestamp = UNSET_LONG;
    private long maxTimestamp = UNSET_LONG;
    private long minTimestamp = UNSET_LONG;
    private long partitionTableVersion = UNSET_LONG;
    private long seqTxn = UNSET_LONG;
    private long structureVersion = UNSET_LONG;
    private long transientRowCount = UNSET_LONG;
    private long truncateVersion = UNSET_LONG;
    private long txn = UNSET_LONG;
    private String txnPath;

    public void addIssue(RecoveryIssueSeverity severity, RecoveryIssueCode code, String message) {
        issues.add(new ReadIssue(severity, code, message));
    }

    public long getBaseVersion() {
        return baseVersion;
    }

    public long getColumnVersion() {
        return columnVersion;
    }

    public long getDataVersion() {
        return dataVersion;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getFixedRowCount() {
        return fixedRowCount;
    }

    public ObjList<ReadIssue> getIssues() {
        return issues;
    }

    public long getLagMaxTimestamp() {
        return lagMaxTimestamp;
    }

    public long getLagMinTimestamp() {
        return lagMinTimestamp;
    }

    public int getLagRowCount() {
        return lagRowCount;
    }

    public int getLagTxnCount() {
        return lagTxnCount;
    }

    public int getMapWriterCount() {
        return mapWriterCount;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public ObjList<TxnPartitionState> getPartitions() {
        return partitions;
    }

    public long getPartitionTableVersion() {
        return partitionTableVersion;
    }

    public int getRecordBaseOffset() {
        return recordBaseOffset;
    }

    public long getRowCount() {
        if (fixedRowCount == UNSET_LONG || transientRowCount == UNSET_LONG) {
            return UNSET_LONG;
        }
        return fixedRowCount + transientRowCount;
    }

    public long getSeqTxn() {
        return seqTxn;
    }

    public long getStructureVersion() {
        return structureVersion;
    }

    public ObjList<TxnSymbolState> getSymbols() {
        return symbols;
    }

    public long getTransientRowCount() {
        return transientRowCount;
    }

    public long getTruncateVersion() {
        return truncateVersion;
    }

    public long getTxn() {
        return txn;
    }

    public String getTxnPath() {
        return txnPath;
    }

    void setBaseVersion(long baseVersion) {
        this.baseVersion = baseVersion;
    }

    void setColumnVersion(long columnVersion) {
        this.columnVersion = columnVersion;
    }

    void setDataVersion(long dataVersion) {
        this.dataVersion = dataVersion;
    }

    void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    void setFixedRowCount(long fixedRowCount) {
        this.fixedRowCount = fixedRowCount;
    }

    void setLagMaxTimestamp(long lagMaxTimestamp) {
        this.lagMaxTimestamp = lagMaxTimestamp;
    }

    void setLagMinTimestamp(long lagMinTimestamp) {
        this.lagMinTimestamp = lagMinTimestamp;
    }

    void setLagRowCount(int lagRowCount) {
        this.lagRowCount = lagRowCount;
    }

    void setLagTxnCount(int lagTxnCount) {
        this.lagTxnCount = lagTxnCount;
    }

    void setMapWriterCount(int mapWriterCount) {
        this.mapWriterCount = mapWriterCount;
    }

    void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    void setMinTimestamp(long minTimestamp) {
        this.minTimestamp = minTimestamp;
    }

    void setPartitionTableVersion(long partitionTableVersion) {
        this.partitionTableVersion = partitionTableVersion;
    }

    void setRecordBaseOffset(int recordBaseOffset) {
        this.recordBaseOffset = recordBaseOffset;
    }

    void setSeqTxn(long seqTxn) {
        this.seqTxn = seqTxn;
    }

    void setStructureVersion(long structureVersion) {
        this.structureVersion = structureVersion;
    }

    void setTransientRowCount(long transientRowCount) {
        this.transientRowCount = transientRowCount;
    }

    void setTruncateVersion(long truncateVersion) {
        this.truncateVersion = truncateVersion;
    }

    void setTxn(long txn) {
        this.txn = txn;
    }

    void setTxnPath(String txnPath) {
        this.txnPath = txnPath;
    }
}

