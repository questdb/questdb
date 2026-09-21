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

package io.questdb.tasks;

import io.questdb.cairo.TableToken;
import io.questdb.std.Mutable;
import io.questdb.std.str.StringSink;

public class PostingSealPurgeTask implements Mutable {
    private final StringSink indexColumnName = new StringSink();
    private long fromTableTxn;
    private int partitionBy;
    private long partitionNameTxn;
    private long partitionTimestamp;
    private long postingColumnNameTxn;
    private long sealTxn;
    private TableToken tableToken;
    private int timestampType;
    private long toTableTxn;

    @Override
    public void clear() {
        this.tableToken = null;
        this.indexColumnName.clear();
    }

    public long getFromTableTxn() {
        return fromTableTxn;
    }

    public CharSequence getIndexColumnName() {
        return indexColumnName;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public long getPartitionNameTxn() {
        return partitionNameTxn;
    }

    public long getPartitionTimestamp() {
        return partitionTimestamp;
    }

    public long getPostingColumnNameTxn() {
        return postingColumnNameTxn;
    }

    public long getSealTxn() {
        return sealTxn;
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    public int getTimestampType() {
        return timestampType;
    }

    public long getToTableTxn() {
        return toTableTxn;
    }

    public boolean isEmpty() {
        return tableToken == null;
    }

    public void of(
            TableToken tableToken,
            CharSequence indexColumnName,
            long postingColumnNameTxn,
            long sealTxn,
            long partitionTimestamp,
            long partitionNameTxn,
            int partitionBy,
            int timestampType,
            long fromTableTxn,
            long toTableTxn
    ) {
        this.tableToken = tableToken;
        this.indexColumnName.clear();
        this.indexColumnName.put(indexColumnName);
        this.postingColumnNameTxn = postingColumnNameTxn;
        this.sealTxn = sealTxn;
        this.partitionTimestamp = partitionTimestamp;
        this.partitionNameTxn = partitionNameTxn;
        this.partitionBy = partitionBy;
        this.timestampType = timestampType;
        this.fromTableTxn = fromTableTxn;
        this.toTableTxn = toTableTxn;
    }
}
