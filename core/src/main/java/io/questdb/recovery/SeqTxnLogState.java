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
 * Parsed state from a sequencer {@code _txnlog} file. Contains the header
 * fields (version, maxTxn, createTimestamp, partSize) and the list of
 * {@link SeqTxnRecord} entries.
 *
 * <p>Fields that could not be read are left at sentinel values
 * ({@link TxnState#UNSET_INT}, {@link TxnState#UNSET_LONG}). Any issues
 * encountered during parsing are accumulated in the {@link ReadIssue} list.
 *
 * <p>Populated by {@link BoundedSeqTxnLogReader}; setters are package-private.
 */
public final class SeqTxnLogState {
    private final ObjList<ReadIssue> issues = new ObjList<>();
    private final ObjList<SeqTxnRecord> records = new ObjList<>();
    private long createTimestamp = TxnState.UNSET_LONG;
    private long maxTxn = TxnState.UNSET_LONG;
    private int partSize = TxnState.UNSET_INT;
    private String txnlogPath;
    private int version = TxnState.UNSET_INT;

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public ObjList<ReadIssue> getIssues() {
        return issues;
    }

    public long getMaxTxn() {
        return maxTxn;
    }

    public int getPartSize() {
        return partSize;
    }

    public ObjList<SeqTxnRecord> getRecords() {
        return records;
    }

    public String getTxnlogPath() {
        return txnlogPath;
    }

    public int getVersion() {
        return version;
    }

    void setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }

    void setMaxTxn(long maxTxn) {
        this.maxTxn = maxTxn;
    }

    void setPartSize(int partSize) {
        this.partSize = partSize;
    }

    void setTxnlogPath(String txnlogPath) {
        this.txnlogPath = txnlogPath;
    }

    void setVersion(int version) {
        this.version = version;
    }
}
