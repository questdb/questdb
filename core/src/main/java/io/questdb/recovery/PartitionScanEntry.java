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

/**
 * One entry in a partition scan result. Links a partition directory to its
 * {@link PartitionScanStatus} and, when matched, to the corresponding
 * {@link TxnPartitionState} and resolved row count.
 */
public final class PartitionScanEntry {
    private final String dirName;
    private final String partitionName;
    private final long rowCount;
    private final PartitionScanStatus status;
    private final int txnIndex;
    private final TxnPartitionState txnPartition;

    public PartitionScanEntry(
            String dirName,
            String partitionName,
            PartitionScanStatus status,
            TxnPartitionState txnPartition,
            int txnIndex,
            long rowCount
    ) {
        this.dirName = dirName;
        this.partitionName = partitionName;
        this.status = status;
        this.txnPartition = txnPartition;
        this.txnIndex = txnIndex;
        this.rowCount = rowCount;
    }

    public String getDirName() {
        return dirName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public long getRowCount() {
        return rowCount;
    }

    public PartitionScanStatus getStatus() {
        return status;
    }

    public int getTxnIndex() {
        return txnIndex;
    }

    public TxnPartitionState getTxnPartition() {
        return txnPartition;
    }
}
