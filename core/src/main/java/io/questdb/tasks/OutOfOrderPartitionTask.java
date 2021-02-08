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
import io.questdb.cairo.TableWriter;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;

public class OutOfOrderPartitionTask {
    private FilesFacade ff;
    private CharSequence pathToTable;
    private int partitionBy;
    private ObjList<AppendMemory> columns;
    private ObjList<ContiguousVirtualMemory> oooColumns;
    private long srcOooLo;
    private long srcOooHi;
    private long oooTimestampMax;
    private long oooTimestampHi;
    private long txn;
    private long sortedTimestampsAddr;
    private long lastPartitionSize;
    private long tableCeilOfMaxTimestamp;
    private long tableFloorOfMinTimestamp;
    private long tableFloorOfMaxTimestamp;
    private long tableMaxTimestamp;
    private TableWriter tableWriter;
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

    public ObjList<ContiguousVirtualMemory> getOooColumns() {
        return oooColumns;
    }

    public long getOooTimestampHi() {
        return oooTimestampHi;
    }

    public long getOooTimestampMax() {
        return oooTimestampMax;
    }

    public int getPartitionBy() {
        return partitionBy;
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

    public TableWriter getTableWriter() {
        return tableWriter;
    }

    public long getTxn() {
        return txn;
    }

    public void of(
            FilesFacade ff,
            CharSequence path,
            int partitionBy,
            ObjList<AppendMemory> columns,
            ObjList<ContiguousVirtualMemory> oooColumns,
            long srcOooLo,
            long srcOooHi,
            long oooTimestampMax,
            long oooTimestampHi,
            long txn,
            long sortedTimestampsAddr,
            long lastPartitionSize,
            long tableCeilOfMaxTimestamp,
            long tableFloorOfMinTimestamp,
            long tableFloorOfMaxTimestamp,
            long tableMaxTimestamp,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        this.pathToTable = path;
        this.txn = txn;
        this.srcOooLo = srcOooLo;
        this.srcOooHi = srcOooHi;
        this.oooTimestampMax = oooTimestampMax;
        this.oooTimestampHi = oooTimestampHi;
        this.lastPartitionSize = lastPartitionSize;
        this.sortedTimestampsAddr = sortedTimestampsAddr;
        this.tableMaxTimestamp = tableMaxTimestamp;
        this.tableCeilOfMaxTimestamp = tableCeilOfMaxTimestamp;
        this.tableFloorOfMinTimestamp = tableFloorOfMinTimestamp;
        this.tableFloorOfMaxTimestamp = tableFloorOfMaxTimestamp;
        this.ff = ff;
        this.partitionBy = partitionBy;
        this.columns = columns;
        this.oooColumns = oooColumns;
        this.tableWriter = tableWriter;
        this.doneLatch = doneLatch;
    }
}
