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

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.vm.AppendOnlyVirtualMemory;
import io.questdb.cairo.vm.ContiguousVirtualMemory;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;

public class OutOfOrderPartitionTask {
    private FilesFacade ff;
    private CharSequence pathToTable;
    private int partitionBy;
    private ObjList<AppendOnlyVirtualMemory> columns;
    private ObjList<ContiguousVirtualMemory> oooColumns;
    private long srcOooLo;
    private long srcOooHi;
    private long srcOooMax;
    private long oooTimestampMin;
    private long oooTimestampMax;
    private long partitionTimestamp;
    private long txn;
    private long sortedTimestampsAddr;
    private long lastPartitionSize;
    private long tableFloorOfMaxTimestamp;
    private TableWriter tableWriter;
    private SOUnboundedCountDownLatch doneLatch;

    public ObjList<AppendOnlyVirtualMemory> getColumns() {
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

    public long getPartitionTimestamp() {
        return partitionTimestamp;
    }

    public long getOooTimestampMax() {
        return oooTimestampMax;
    }

    public long getOooTimestampMin() {
        return oooTimestampMin;
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

    public long getSrcOooMax() {
        return srcOooMax;
    }

    public long getTableFloorOfMaxTimestamp() {
        return tableFloorOfMaxTimestamp;
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
            ObjList<AppendOnlyVirtualMemory> columns,
            ObjList<ContiguousVirtualMemory> oooColumns,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long txn,
            long sortedTimestampsAddr,
            long lastPartitionSize,
            long tableFloorOfMaxTimestamp,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        this.pathToTable = path;
        this.txn = txn;
        this.srcOooLo = srcOooLo;
        this.srcOooHi = srcOooHi;
        this.srcOooMax = srcOooMax;
        this.oooTimestampMin = oooTimestampMin;
        this.oooTimestampMax = oooTimestampMax;
        this.partitionTimestamp = oooTimestampHi;
        this.lastPartitionSize = lastPartitionSize;
        this.sortedTimestampsAddr = sortedTimestampsAddr;
        this.tableFloorOfMaxTimestamp = tableFloorOfMaxTimestamp;
        this.ff = ff;
        this.partitionBy = partitionBy;
        this.columns = columns;
        this.oooColumns = oooColumns;
        this.tableWriter = tableWriter;
        this.doneLatch = doneLatch;
    }
}
