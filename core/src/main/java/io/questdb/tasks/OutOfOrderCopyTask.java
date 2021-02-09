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
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.AbstractLockable;
import io.questdb.std.FilesFacade;

import java.util.concurrent.atomic.AtomicInteger;

public class OutOfOrderCopyTask extends AbstractLockable {
    private AtomicInteger columnCounter;
    private AtomicInteger partCounter;
    private FilesFacade ff;
    private CharSequence pathToTable;
    private int columnType;
    private int blockType;
    private long timestampMergeIndexAddr;
    private long srcDataTop;
    private long srcDataFixFd;
    private long srcDataFixAddr;
    private long srcDataFixSize;
    private long srcDataVarFd;
    private long srcDataVarAddr;
    private long srcDataVarSize;
    private long srcDataLo;
    private long srcDataHi;
    private long srcDataMax;
    private long tableFloorOfMaxTimestamp;
    private long dataTimestampHi;
    private long srcOooFixAddr;
    private long srcOooFixSize;
    private long srcOooVarAddr;
    private long srcOooVarSize;
    private long srcOooLo;
    private long srcOooHi;
    private long srcOooMax;
    private long srcOooPartitionLo;
    private long srcOooPartitionHi;
    private long oooTimestampMin;
    private long oooTimestampMax;
    private long oooTimestampHi;
    private long dstFixFd;
    private long dstFixAddr;
    private long dstFixOffset;
    private long dstFixSize;
    private long dstVarFd;
    private long dstVarAddr;
    private long dstVarOffset;
    private long dstVarSize;
    private long dstKFd;
    private long dstVFd;
    private long dstIndexOffset;
    private boolean isIndexed;
    private long srcTimestampFd;
    private long srcTimestampAddr;
    private long srcTimestampSize;
    private boolean partitionMutates;
    private TableWriter tableWriter;
    private SOUnboundedCountDownLatch doneLatch;

    public int getBlockType() {
        return blockType;
    }

    public AtomicInteger getColumnCounter() {
        return columnCounter;
    }

    public int getColumnType() {
        return columnType;
    }

    public long getDataTimestampHi() {
        return dataTimestampHi;
    }

    public SOUnboundedCountDownLatch getDoneLatch() {
        return doneLatch;
    }

    public long getDstFixAddr() {
        return dstFixAddr;
    }

    public long getDstFixFd() {
        return dstFixFd;
    }

    public long getDstFixOffset() {
        return dstFixOffset;
    }

    public long getDstFixSize() {
        return dstFixSize;
    }

    public long getDstIndexOffset() {
        return dstIndexOffset;
    }

    public long getDstKFd() {
        return dstKFd;
    }

    public long getDstVFd() {
        return dstVFd;
    }

    public long getDstVarAddr() {
        return dstVarAddr;
    }

    public long getDstVarFd() {
        return dstVarFd;
    }

    public long getDstVarOffset() {
        return dstVarOffset;
    }

    public long getDstVarSize() {
        return dstVarSize;
    }

    public FilesFacade getFf() {
        return ff;
    }

    public long getOooTimestampHi() {
        return oooTimestampHi;
    }

    public long getOooTimestampMax() {
        return oooTimestampMax;
    }

    public long getOooTimestampMin() {
        return oooTimestampMin;
    }

    public AtomicInteger getPartCounter() {
        return partCounter;
    }

    public CharSequence getPathToTable() {
        return pathToTable;
    }

    public long getSrcDataFixAddr() {
        return srcDataFixAddr;
    }

    public long getSrcDataFixFd() {
        return srcDataFixFd;
    }

    public long getSrcDataFixSize() {
        return srcDataFixSize;
    }

    public long getSrcDataHi() {
        return srcDataHi;
    }

    public long getSrcDataLo() {
        return srcDataLo;
    }

    public long getSrcDataMax() {
        return srcDataMax;
    }

    public long getSrcDataTop() {
        return srcDataTop;
    }

    public long getSrcDataVarAddr() {
        return srcDataVarAddr;
    }

    public long getSrcDataVarFd() {
        return srcDataVarFd;
    }

    public long getSrcDataVarSize() {
        return srcDataVarSize;
    }

    public long getSrcOooFixAddr() {
        return srcOooFixAddr;
    }

    public long getSrcOooFixSize() {
        return srcOooFixSize;
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

    public long getSrcOooPartitionHi() {
        return srcOooPartitionHi;
    }

    public long getSrcOooPartitionLo() {
        return srcOooPartitionLo;
    }

    public long getSrcOooVarAddr() {
        return srcOooVarAddr;
    }

    public long getSrcOooVarSize() {
        return srcOooVarSize;
    }

    public long getSrcTimestampAddr() {
        return srcTimestampAddr;
    }

    public long getSrcTimestampFd() {
        return srcTimestampFd;
    }

    public long getSrcTimestampSize() {
        return srcTimestampSize;
    }

    public long getTableFloorOfMaxTimestamp() {
        return tableFloorOfMaxTimestamp;
    }

    public TableWriter getTableWriter() {
        return tableWriter;
    }

    public long getTimestampMergeIndexAddr() {
        return timestampMergeIndexAddr;
    }

    public boolean isIndexed() {
        return isIndexed;
    }

    public boolean isPartitionMutates() {
        return partitionMutates;
    }

    public void of(
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            FilesFacade ff,
            CharSequence pathToTable,
            int columnType,
            int blockType,
            long timestampMergeIndexAddr,
            long srcDataTop,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            long srcDataLo,
            long srcDataHi,
            long srcDataMax,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarSize,
            long dstKFd,
            long dstVFd,
            long dstIndexOffset,
            boolean isIndexed,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            TableWriter tableWriter,
            SOUnboundedCountDownLatch doneLatch
    ) {
        this.columnCounter = columnCounter;
        this.partCounter = partCounter;
        this.ff = ff;
        this.pathToTable = pathToTable;
        this.columnType = columnType;
        this.blockType = blockType;
        this.timestampMergeIndexAddr = timestampMergeIndexAddr;
        this.srcDataTop = srcDataTop;
        this.srcDataFixFd = srcDataFixFd;
        this.srcDataFixAddr = srcDataFixAddr;
        this.srcDataFixSize = srcDataFixSize;
        this.srcDataVarFd = srcDataVarFd;
        this.srcDataVarAddr = srcDataVarAddr;
        this.srcDataVarSize = srcDataVarSize;
        this.srcDataLo = srcDataLo;
        this.srcDataHi = srcDataHi;
        this.srcDataMax = srcDataMax;
        this.tableFloorOfMaxTimestamp = tableFloorOfMaxTimestamp;
        this.dataTimestampHi = dataTimestampHi;
        this.srcOooFixAddr = srcOooFixAddr;
        this.srcOooFixSize = srcOooFixSize;
        this.srcOooVarAddr = srcOooVarAddr;
        this.srcOooVarSize = srcOooVarSize;
        this.srcOooLo = srcOooLo;
        this.srcOooHi = srcOooHi;
        this.srcOooMax = srcOooMax;
        this.srcOooPartitionLo = srcOooPartitionLo;
        this.srcOooPartitionHi = srcOooPartitionHi;
        this.oooTimestampMin = oooTimestampMin;
        this.oooTimestampMax = oooTimestampMax;
        this.oooTimestampHi = oooTimestampHi;
        this.dstFixFd = dstFixFd;
        this.dstFixAddr = dstFixAddr;
        this.dstFixOffset = dstFixOffset;
        this.dstFixSize = dstFixSize;
        this.dstVarFd = dstVarFd;
        this.dstVarAddr = dstVarAddr;
        this.dstVarOffset = dstVarOffset;
        this.dstVarSize = dstVarSize;
        this.dstKFd = dstKFd;
        this.dstVFd = dstVFd;
        this.dstIndexOffset = dstIndexOffset;
        this.isIndexed = isIndexed;
        this.srcTimestampFd = srcTimestampFd;
        this.srcTimestampAddr = srcTimestampAddr;
        this.srcTimestampSize = srcTimestampSize;
        this.partitionMutates = partitionMutates;
        this.tableWriter = tableWriter;
        this.doneLatch = doneLatch;
    }
}
