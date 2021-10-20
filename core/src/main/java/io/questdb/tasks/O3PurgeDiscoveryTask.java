/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.tasks;

import io.questdb.cairo.TxnScoreboard;

public class O3PurgeDiscoveryTask {
    private CharSequence tableName;
    private int partitionBy;
    private TxnScoreboard txnScoreboard;
    private long timestamp;
    private long mostRecentTxn;

    public long getMostRecentTxn() {
        return mostRecentTxn;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public CharSequence getTableName() {
        return tableName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public TxnScoreboard getTxnScoreboard() {
        return txnScoreboard;
    }

    public void of(CharSequence tableName, int partitionBy, TxnScoreboard txnScoreboard, long timestamp, long mostRecentTxn) {
        this.tableName = tableName;
        this.partitionBy = partitionBy;
        this.txnScoreboard = txnScoreboard;
        this.timestamp = timestamp;
        this.mostRecentTxn = mostRecentTxn;
    }
}
