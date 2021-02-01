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

import io.questdb.cairo.AppendMemory;
import io.questdb.cairo.ContiguousVirtualMemory;
import io.questdb.cairo.TableWriterMetadata;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.AbstractLockable;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;

public class OutOfOrderPartitionTask extends AbstractLockable {
    private CharSequence pathToTable;
    private long txn;
    private long srcOooLo;
    private long srcOooHi;
    private long oooTimestampMax;
    private long lastPartitionSize;
    private long partitionTimestampHi;
    private long sortedTimestampsAddr;
    private long tableMaxTimestamp;
    private long tableCeilOfMaxTimestamp;
    private long tableFloorOfMinTimestamp;
    private long tableFloorOfMaxTimestamp;
    private FilesFacade ff;
    private int partitionBy;
    private int timestampIndex;
    private ObjList<AppendMemory> columns;
    private ObjList<ContiguousVirtualMemory> oooColumns;
    private TableWriterMetadata metadata;
    private SOUnboundedCountDownLatch doneLatch;

    public ObjList<AppendMemory> getColumns() {
        return columns;
    }

    public SOUnboundedCountDownLatch getDoneLatch() {
        return doneLatch;
    }

    public FilesFacade getFf() {
        return ff;
    }

    public long getLastPartitionSize() {
        return lastPartitionSize;
    }

    public TableWriterMetadata getMetadata() {
        return metadata;
    }

    public ObjList<ContiguousVirtualMemory> getOooColumns() {
        return oooColumns;
    }

    public long getOooTimestampMax() {
        return oooTimestampMax;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public long getPartitionTimestampHi() {
        return partitionTimestampHi;
    }

    public CharSequence getPathToTable() {
        return pathToTable;
    }

    public long getSortedTimestampsAddr() {
        return sortedTimestampsAddr;
    }

    public long getSrcOooHi() {
        return srcOooHi;
    }

    public long getSrcOooLo() {
        return srcOooLo;
    }

    public long getTableCeilOfMaxTimestamp() {
        return tableCeilOfMaxTimestamp;
    }

    public long getTableFloorOfMaxTimestamp() {
        return tableFloorOfMaxTimestamp;
    }

    public long getTableFloorOfMinTimestamp() {
        return tableFloorOfMinTimestamp;
    }

    public long getTableMaxTimestamp() {
        return tableMaxTimestamp;
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    public long getTxn() {
        return txn;
    }

    public void of(
            CharSequence path,
            long txn,
            long srcOooLo,
            long srcOooHi,
            long oooTimestampMax,
            long lastPartitionSize,
            long partitionTimestampHi,
            long sortedTimestampsAddr,
            long tableMaxTimestamp,
            long tableCeilOfMaxTimestamp,
            long tableFloorOfMinTimestamp,
            long tableFloorOfMaxTimestamp,
            FilesFacade ff,
            int partitionBy,
            int timestampIndex,
            ObjList<AppendMemory> columns,
            ObjList<ContiguousVirtualMemory> oooColumns,
            TableWriterMetadata metadata,
            SOUnboundedCountDownLatch doneLatch
    ) {
        this.pathToTable = path;
        this.txn = txn;
        this.srcOooLo = srcOooLo;
        this.srcOooHi = srcOooHi;
        this.oooTimestampMax = oooTimestampMax;
        this.lastPartitionSize = lastPartitionSize;
        this.partitionTimestampHi = partitionTimestampHi;
        this.sortedTimestampsAddr = sortedTimestampsAddr;
        this.tableMaxTimestamp = tableMaxTimestamp;
        this.tableCeilOfMaxTimestamp = tableCeilOfMaxTimestamp;
        this.tableFloorOfMinTimestamp = tableFloorOfMinTimestamp;
        this.tableFloorOfMaxTimestamp = tableFloorOfMaxTimestamp;
        this.ff = ff;
        this.partitionBy = partitionBy;
        this.timestampIndex = timestampIndex;
        this.columns = columns;
        this.oooColumns = oooColumns;
        this.metadata = metadata;
        this.doneLatch = doneLatch;
    }
}
