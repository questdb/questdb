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

import io.questdb.cairo.BitmapIndexWriter;
import io.questdb.cairo.TableWriter;
import io.questdb.std.AbstractLockable;

import java.util.concurrent.atomic.AtomicInteger;

public class O3CopyTask extends AbstractLockable {
    private AtomicInteger columnCounter;
    private AtomicInteger partCounter;
    private int columnType;
    private int blockType;
    private long timestampMergeIndexAddr;
    private long srcDataFixFd;
    private long srcDataFixAddr;
    private long srcDataFixOffset;
    private long srcDataFixSize;
    private long srcDataVarFd;
    private long srcDataVarAddr;
    private long srcDataVarOffset;
    private long srcDataVarSize;
    private long srcDataLo;
    private long srcDataHi;
    private long srcDataTop;
    private long srcDataMax;
    private long srcOooFixAddr;
    private long srcOooVarAddr;
    private long srcOooLo;
    private long srcOooHi;
    private long srcOooMax;
    private long srcOooPartitionLo;
    private long srcOooPartitionHi;
    private long timestampMin;
    private long timestampMax;
    private long partitionTimestamp;
    private long dstFixFd;
    private long dstFixAddr;
    private long dstFixOffset;
    private long dstFixSize;
    private long dstVarFd;
    private long dstVarAddr;
    private long dstVarOffset;
    private long dstVarOffsetEnd;
    private long dstVarAdjust;
    private long dstVarSize;
    private long dstKFd;
    private long dstVFd;
    private long dstIndexOffset;
    private long dstIndexAdjust;
    private boolean isIndexed;
    private long srcTimestampFd;
    private long srcTimestampAddr;
    private long srcTimestampSize;
    private boolean partitionMutates;
    private TableWriter tableWriter;
    private BitmapIndexWriter indexWriter;

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

    public long getDstFixFd() {
        return dstFixFd;
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

    public long getDstKFd() {
        return dstKFd;
    }

    public long getDstVFd() {
        return dstVFd;
    }

    public long getDstVarAddr() {
        return dstVarAddr;
    }

    public long getDstVarAdjust() {
        return dstVarAdjust;
    }

    public long getDstVarFd() {
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

    public long getSrcDataFixFd() {
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

    public long getSrcDataVarFd() {
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

    public long getSrcTimestampFd() {
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

    public long getTimestampMin() {
        return timestampMin;
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
            int columnType,
            int blockType,
            long timestampMergeIndexAddr,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixOffset,
            long srcDataFixSize,
            long srcDataVarFd,
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
            long dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarOffsetEnd,
            long dstVarAdjust,
            long dstVarSize,
            long dstKFd,
            long dstVFd,
            long dstIndexOffset,
            long dstIndexAdjust,
            boolean isIndexed,
            long srcTimestampFd,
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
        this.isIndexed = isIndexed;
        this.srcTimestampFd = srcTimestampFd;
        this.srcTimestampAddr = srcTimestampAddr;
        this.srcTimestampSize = srcTimestampSize;
        this.partitionMutates = partitionMutates;
        this.tableWriter = tableWriter;
        this.indexWriter = indexWriter;
    }
}
