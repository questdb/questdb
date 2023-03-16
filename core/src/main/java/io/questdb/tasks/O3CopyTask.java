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

import io.questdb.cairo.BitmapIndexWriter;
import io.questdb.cairo.TableWriter;

import java.util.concurrent.atomic.AtomicInteger;

public class O3CopyTask {
    private int blockType;
    private AtomicInteger columnCounter;
    private int columnType;
    private long dstFixAddr;
    private int dstFixFd;
    private long dstFixFileOffset;
    private long dstFixOffset;
    private long dstFixSize;
    private long dstIndexAdjust;
    private long dstIndexOffset;
    private int dstKFd;
    private int dstVFd;
    private long dstVarAddr;
    private long dstVarAdjust;
    private int dstVarFd;
    private long dstVarOffset;
    private long dstVarOffsetEnd;
    private long dstVarSize;
    private int indexBlockCapacity;
    private BitmapIndexWriter indexWriter;
    private AtomicInteger partCounter;
    private boolean partitionMutates;
    private long partitionTimestamp;
    private long srcDataFixAddr;
    private int srcDataFixFd;
    private long srcDataFixOffset;
    private long srcDataFixSize;
    private long srcDataHi;
    private long srcDataLo;
    private long srcDataMax;
    private long srcDataTop;
    private long srcDataVarAddr;
    private int srcDataVarFd;
    private long srcDataVarOffset;
    private long srcDataVarSize;
    private long srcOooFixAddr;
    private long srcOooHi;
    private long srcOooLo;
    private long srcOooMax;
    private long srcOooPartitionHi;
    private long srcOooPartitionLo;
    private long srcOooVarAddr;
    private long srcTimestampAddr;
    private int srcTimestampFd;
    private long srcTimestampSize;
    private TableWriter tableWriter;
    private long timestampMax;
    private long timestampMergeIndexAddr;
    private long timestampMergeIndexSize;
    private long timestampMin;

    public int getBlockType() {
        return blockType;
    }

    public AtomicInteger getColumnCounter() {
        return columnCounter;
    }

    public int getColumnType() {
        return columnType;
    }

    public long getDstFixAddr() {
        return dstFixAddr;
    }

    public int getDstFixFd() {
        return dstFixFd;
    }

    public long getDstFixFileOffset() {
        return dstFixFileOffset;
    }

    public long getDstFixOffset() {
        return dstFixOffset;
    }

    public long getDstFixSize() {
        return dstFixSize;
    }

    public long getDstIndexAdjust() {
        return dstIndexAdjust;
    }

    public long getDstIndexOffset() {
        return dstIndexOffset;
    }

    public int getDstKFd() {
        return dstKFd;
    }

    public int getDstVFd() {
        return dstVFd;
    }

    public long getDstVarAddr() {
        return dstVarAddr;
    }

    public long getDstVarAdjust() {
        return dstVarAdjust;
    }

    public int getDstVarFd() {
        return dstVarFd;
    }

    public long getDstVarOffset() {
        return dstVarOffset;
    }

    public long getDstVarOffsetEnd() {
        return dstVarOffsetEnd;
    }

    public long getDstVarSize() {
        return dstVarSize;
    }

    public int getIndexBlockCapacity() {
        return indexBlockCapacity;
    }

    public BitmapIndexWriter getIndexWriter() {
        return indexWriter;
    }

    public AtomicInteger getPartCounter() {
        return partCounter;
    }

    public long getPartitionTimestamp() {
        return partitionTimestamp;
    }

    public long getSrcDataFixAddr() {
        return srcDataFixAddr;
    }

    public int getSrcDataFixFd() {
        return srcDataFixFd;
    }

    public long getSrcDataFixOffset() {
        return srcDataFixOffset;
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

    public int getSrcDataVarFd() {
        return srcDataVarFd;
    }

    public long getSrcDataVarOffset() {
        return srcDataVarOffset;
    }

    public long getSrcDataVarSize() {
        return srcDataVarSize;
    }

    public long getSrcOooFixAddr() {
        return srcOooFixAddr;
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

    public long getSrcTimestampAddr() {
        return srcTimestampAddr;
    }

    public int getSrcTimestampFd() {
        return srcTimestampFd;
    }

    public long getSrcTimestampSize() {
        return srcTimestampSize;
    }

    public TableWriter getTableWriter() {
        return tableWriter;
    }

    public long getTimestampMax() {
        return timestampMax;
    }

    public long getTimestampMergeIndexAddr() {
        return timestampMergeIndexAddr;
    }

    public long getTimestampMergeIndexSize() {
        return timestampMergeIndexSize;
    }

    public long getTimestampMin() {
        return timestampMin;
    }

    public boolean isPartitionMutates() {
        return partitionMutates;
    }

    public void of(
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            int blockType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            int srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixOffset,
            long srcDataFixSize,
            int srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarOffset,
            long srcDataVarSize,
            long srcDataLo,
            long srcDataHi,
            long srcDataTop,
            long srcDataMax,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long timestampMin,
            long timestampMax,
            long oooTimestampHi,
            int dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
            long dstFixFileOffset,
            long dstFixSize,
            int dstVarFd,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarOffsetEnd,
            long dstVarAdjust,
            long dstVarSize,
            int dstKFd,
            int dstVFd,
            long dstIndexOffset,
            long dstIndexAdjust,
            int indexBlockCapacity,
            int srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            boolean partitionMutates,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter
    ) {
        this.columnCounter = columnCounter;
        this.partCounter = partCounter;
        this.columnType = columnType;
        this.blockType = blockType;
        this.timestampMergeIndexAddr = timestampMergeIndexAddr;
        this.timestampMergeIndexSize = timestampMergeIndexSize;
        this.srcDataFixFd = srcDataFixFd;
        this.srcDataFixAddr = srcDataFixAddr;
        this.srcDataFixOffset = srcDataFixOffset;
        this.srcDataFixSize = srcDataFixSize;
        this.srcDataVarFd = srcDataVarFd;
        this.srcDataVarAddr = srcDataVarAddr;
        this.srcDataVarOffset = srcDataVarOffset;
        this.srcDataVarSize = srcDataVarSize;
        this.srcDataTop = srcDataTop;
        this.srcDataLo = srcDataLo;
        this.srcDataHi = srcDataHi;
        this.srcDataMax = srcDataMax;
        this.srcOooFixAddr = srcOooFixAddr;
        this.srcOooVarAddr = srcOooVarAddr;
        this.srcOooLo = srcOooLo;
        this.srcOooHi = srcOooHi;
        this.srcOooMax = srcOooMax;
        this.srcOooPartitionLo = srcOooPartitionLo;
        this.srcOooPartitionHi = srcOooPartitionHi;
        this.timestampMin = timestampMin;
        this.timestampMax = timestampMax;
        this.partitionTimestamp = oooTimestampHi;
        this.dstFixFd = dstFixFd;
        this.dstFixAddr = dstFixAddr;
        this.dstFixOffset = dstFixOffset;
        this.dstFixFileOffset = dstFixFileOffset;
        this.dstFixSize = dstFixSize;
        this.dstVarFd = dstVarFd;
        this.dstVarAddr = dstVarAddr;
        this.dstVarOffset = dstVarOffset;
        this.dstVarOffsetEnd = dstVarOffsetEnd;
        this.dstVarAdjust = dstVarAdjust;
        this.dstVarSize = dstVarSize;
        this.dstKFd = dstKFd;
        this.dstVFd = dstVFd;
        this.dstIndexOffset = dstIndexOffset;
        this.dstIndexAdjust = dstIndexAdjust;
        this.indexBlockCapacity = indexBlockCapacity;
        this.srcTimestampFd = srcTimestampFd;
        this.srcTimestampAddr = srcTimestampAddr;
        this.srcTimestampSize = srcTimestampSize;
        this.partitionMutates = partitionMutates;
        this.tableWriter = tableWriter;
        this.indexWriter = indexWriter;
    }
}
