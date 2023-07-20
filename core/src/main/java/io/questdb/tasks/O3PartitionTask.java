/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.O3Basket;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.ObjList;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.str.Path;

import java.util.concurrent.atomic.AtomicInteger;

public class O3PartitionTask {
    private AtomicInteger columnCounter;
    private ObjList<MemoryMA> columns;
    private long dedupColSinkAddr;
    private boolean last;
    private long maxTimestamp; // table's max timestamp
    private O3Basket o3Basket;
    private ReadOnlyObjList<? extends MemoryCR> o3Columns;
    private long oooTimestampMax;
    private long oooTimestampMin;
    private int partitionBy;
    private long partitionTimestamp;
    private long partitionUpdateSinkAddr;
    private Path pathToTable;
    private long sortedTimestampsAddr;
    private long srcDataMax;
    private long srcNameTxn;
    private long srcOooHi;
    private long srcOooLo;
    private long srcOooMax;
    private TableWriter tableWriter;
    private long txn;

    public AtomicInteger getColumnCounter() {
        return columnCounter;
    }

    public ObjList<MemoryMA> getColumns() {
        return columns;
    }

    public long getDedupColSinkAddr() {
        return dedupColSinkAddr;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public O3Basket getO3Basket() {
        return o3Basket;
    }

    public ReadOnlyObjList<? extends MemoryCR> getO3Columns() {
        return o3Columns;
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

    public long getPartitionTimestamp() {
        return partitionTimestamp;
    }

    public long getPartitionUpdateSinkAddr() {
        return partitionUpdateSinkAddr;
    }

    public Path getPathToTable() {
        return pathToTable;
    }

    public long getSortedTimestampsAddr() {
        return sortedTimestampsAddr;
    }

    public long getSrcDataMax() {
        return srcDataMax;
    }

    public long getSrcNameTxn() {
        return srcNameTxn;
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

    public TableWriter getTableWriter() {
        return tableWriter;
    }

    public long getTxn() {
        return txn;
    }

    public boolean isLast() {
        return last;
    }

    public void of(
            Path path,
            int partitionBy,
            ObjList<MemoryMA> columns,
            ReadOnlyObjList<? extends MemoryCR> o3Columns,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long partitionTimestamp,
            long maxTimestamp,
            long srcDataMax,
            long srcNameTxn,
            boolean last,
            long txn,
            long sortedTimestampsAddr,
            TableWriter tableWriter,
            AtomicInteger columnCounter,
            O3Basket o3Basket,
            long partitionUpdateSinkAddr,
            long dedupColSinkAddr
    ) {
        this.pathToTable = path;
        this.txn = txn;
        this.srcOooLo = srcOooLo;
        this.srcOooHi = srcOooHi;
        this.srcOooMax = srcOooMax;
        this.oooTimestampMin = oooTimestampMin;
        this.oooTimestampMax = oooTimestampMax;
        this.partitionTimestamp = partitionTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.srcDataMax = srcDataMax;
        this.srcNameTxn = srcNameTxn;
        this.last = last;
        this.sortedTimestampsAddr = sortedTimestampsAddr;
        this.partitionBy = partitionBy;
        this.columns = columns;
        this.o3Columns = o3Columns;
        this.tableWriter = tableWriter;
        this.columnCounter = columnCounter;
        this.o3Basket = o3Basket;
        this.partitionUpdateSinkAddr = partitionUpdateSinkAddr;
        this.dedupColSinkAddr = dedupColSinkAddr;
    }
}
