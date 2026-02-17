/*******************************************************************************
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

import io.questdb.cairo.BitmapIndexWriter;
import io.questdb.cairo.TableWriter;
import io.questdb.std.str.Path;

import java.util.concurrent.atomic.AtomicInteger;

public class O3OpenColumnTask {
    private long activeFixFd;
    private long activeVarFd;
    private AtomicInteger columnCounter;
    private int columnIndex;
    private CharSequence columnName;
    private long columnNameTxn;
    private int columnType;
    private int indexBlockCapacity;
    private BitmapIndexWriter indexWriter;
    private long mergeDataHi;
    private long mergeDataLo;
    private long mergeOOOHi;
    private long mergeOOOLo;
    private int mergeType;
    private long o3SplitPartitionSize;
    private long oldPartitionTimestamp;
    private int openColumnMode;
    private AtomicInteger partCounter;
    private long partitionTimestamp;
    private long partitionUpdateSinkAddr;
    private Path pathToTable;
    private long prefixHi;
    private long prefixLo;
    private int prefixType;
    private long srcDataMax;
    private long srcDataNewPartitionSize;
    private long srcDataOldPartitionSize;
    private long srcDataTop;
    private long srcNameTxn;
    private long srcOooFixAddr;
    private long srcOooHi;
    private long srcOooLo;
    private long srcOooMax;
    private long srcOooVarAddr;
    private long srcTimestampAddr;
    private long srcTimestampFd;
    private long srcTimestampSize;
    private long suffixHi;
    private long suffixLo;
    private int suffixType;
    private TableWriter tableWriter;
    private long timestampMergeIndexAddr;
    private long timestampMergeIndexSize;
    private long timestampMin;
    private long txn;

    public long getActiveFixFd() {
        return activeFixFd;
    }

    public long getActiveVarFd() {
        return activeVarFd;
    }

    public AtomicInteger getColumnCounter() {
        return columnCounter;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public CharSequence getColumnName() {
        return columnName;
    }

    public long getColumnNameTxn() {
        return columnNameTxn;
    }

    public int getColumnType() {
        return columnType;
    }

    public int getIndexBlockCapacity() {
        return indexBlockCapacity;
    }

    public BitmapIndexWriter getIndexWriter() {
        return indexWriter;
    }

    public long getMergeDataHi() {
        return mergeDataHi;
    }

    public long getMergeDataLo() {
        return mergeDataLo;
    }

    public long getMergeOOOHi() {
        return mergeOOOHi;
    }

    public long getMergeOOOLo() {
        return mergeOOOLo;
    }

    public int getMergeType() {
        return mergeType;
    }

    public long getO3SplitPartitionSize() {
        return o3SplitPartitionSize;
    }

    public long getOldPartitionTimestamp() {
        return oldPartitionTimestamp;
    }

    public int getOpenColumnMode() {
        return openColumnMode;
    }

    public AtomicInteger getPartCounter() {
        return partCounter;
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

    public long getPrefixHi() {
        return prefixHi;
    }

    public long getPrefixLo() {
        return prefixLo;
    }

    public int getPrefixType() {
        return prefixType;
    }

    public long getSrcDataMax() {
        return srcDataMax;
    }

    public long getSrcDataNewPartitionSize() {
        return srcDataNewPartitionSize;
    }

    public long getSrcDataOldPartitionSize() {
        return srcDataOldPartitionSize;
    }

    public long getSrcDataTop() {
        return srcDataTop;
    }

    public long getSrcNameTxn() {
        return srcNameTxn;
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

    public long getSuffixHi() {
        return suffixHi;
    }

    public long getSuffixLo() {
        return suffixLo;
    }

    public int getSuffixType() {
        return suffixType;
    }

    public TableWriter getTableWriter() {
        return tableWriter;
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

    public long getTxn() {
        return txn;
    }

    public void of(
            int openColumnMode,
            Path pathToTable,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long partitionTimestamp,
            long oldPartitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            long srcNameTxn,
            long txn,
            int prefixType,
            long prefixLo,
            long prefixHi,
            int mergeType,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            int suffixType,
            long suffixLo,
            long suffixHi,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int indexBlockCapacity,
            long activeFixFd,
            long activeVarFd,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3NewPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long partitionUpdateSinkAddr,
            int columnIndex,
            long columnNameTxn
    ) {
        this.openColumnMode = openColumnMode;
        this.pathToTable = pathToTable;
        this.columnCounter = columnCounter;
        this.partCounter = partCounter;
        this.columnName = columnName;
        this.columnType = columnType;
        this.timestampMergeIndexAddr = timestampMergeIndexAddr;
        this.timestampMergeIndexSize = timestampMergeIndexSize;
        this.srcOooFixAddr = srcOooFixAddr;
        this.srcOooVarAddr = srcOooVarAddr;
        this.srcOooLo = srcOooLo;
        this.srcOooHi = srcOooHi;
        this.srcOooMax = srcOooMax;
        this.timestampMin = timestampMin;
        this.partitionTimestamp = partitionTimestamp;
        this.oldPartitionTimestamp = oldPartitionTimestamp;
        this.srcDataTop = srcDataTop;
        this.srcDataMax = srcDataMax;
        this.srcNameTxn = srcNameTxn;
        this.txn = txn;
        this.prefixType = prefixType;
        this.prefixLo = prefixLo;
        this.prefixHi = prefixHi;
        this.mergeType = mergeType;
        this.mergeDataLo = mergeDataLo;
        this.mergeDataHi = mergeDataHi;
        this.mergeOOOLo = mergeOOOLo;
        this.mergeOOOHi = mergeOOOHi;
        this.suffixType = suffixType;
        this.suffixLo = suffixLo;
        this.suffixHi = suffixHi;
        this.srcTimestampFd = srcTimestampFd;
        this.srcTimestampAddr = srcTimestampAddr;
        this.srcTimestampSize = srcTimestampSize;
        this.indexBlockCapacity = indexBlockCapacity;
        this.activeFixFd = activeFixFd;
        this.activeVarFd = activeVarFd;
        this.tableWriter = tableWriter;
        this.indexWriter = indexWriter;
        this.partitionUpdateSinkAddr = partitionUpdateSinkAddr;
        this.columnIndex = columnIndex;
        this.columnNameTxn = columnNameTxn;
        this.srcDataNewPartitionSize = srcDataNewPartitionSize;
        this.srcDataOldPartitionSize = srcDataOldPartitionSize;
        this.o3SplitPartitionSize = o3NewPartitionSize;
    }
}
