/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.LongList;
import io.questdb.std.Mutable;

public class PartitionPurgeTask implements Mutable {
    private final LongList activePartitionRemoveCandidates = new LongList();
    private String tableName;
    private long timestamp;
    private int partitionBy;
    private long partitionNameTxn;

    @Override
    public void clear() {
        activePartitionRemoveCandidates.clear();
    }

    public void copyFrom(PartitionPurgeTask inTask) {
        tableName = inTask.tableName;
        timestamp = inTask.timestamp;
        partitionBy = inTask.partitionBy;
        partitionNameTxn = inTask.partitionNameTxn;
        activePartitionRemoveCandidates.clear();
        activePartitionRemoveCandidates.add(inTask.activePartitionRemoveCandidates);
    }

    public String getTableName() {
        return tableName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public long getPartitionNameTxn() {
        return partitionNameTxn;
    }

    public void of(String tableName, long timestamp, int partitionBy, long partitionNameTxn) {
        this.tableName = tableName;
        this.timestamp = timestamp;
        this.partitionBy = partitionBy;
        this.partitionNameTxn = partitionNameTxn;
        this.activePartitionRemoveCandidates.clear();
    }
}
