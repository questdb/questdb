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

import java.util.concurrent.atomic.AtomicInteger;

public class O3OpenColumnTask {
    private CharSequence pathToTable;
    private AtomicInteger columnCounter;
    private AtomicInteger partCounter;
    private long txn;
    private int openColumnMode;
    private CharSequence columnName;
    private int columnType;
    private boolean isIndexed;
    private long srcTimestampFd;
    private long srcTimestampAddr;
    private long srcTimestampSize;
    private long timestampMergeIndexAddr;
    private long srcOooFixAddr;
    private long srcOooVarAddr;
    private long srcDataTop;
    private long srcDataMax;
    private long srcDataTxn;
    private long srcOooLo;
    private long srcOooHi;
    private long srcOooMax;
    private long timestampMin;
    private long timestampMax;
    private long oooTimestampLo;
    private long partitionTimestamp;
    private int prefixType;
    private long prefixLo;
    private long prefixHi;
    private int mergeType;
    private long mergeDataLo;
    private long mergeDataHi;
    private long mergeOOOLo;
    private long mergeOOOHi;
    private int suffixType;
    private long suffixLo;
    private long suffixHi;
    private long activeFixFd;
    private long activeVarFd;
    private TableWriter tableWriter;
    private BitmapIndexWriter indexWriter;

    public long getActiveFixFd() {
        return activeFixFd;
    }

    public long getActiveVarFd() {
        return activeVarFd;
    }

    public AtomicInteger getColumnCounter() {
        return columnCounter;
    }

    public CharSequence getColumnName() {
        return columnName;
    }

    public int getColumnType() {
        return columnType;
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

    public long getOooTimestampLo() {
        return oooTimestampLo;
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

    public CharSequence getPathToTable() {
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

    public long getSrcDataTop() {
        return srcDataTop;
    }

    public long getSrcDataTxn() {
        return srcDataTxn;
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

    public long getTimestampMax() {
        return timestampMax;
    }

    public long getTimestampMergeIndexAddr() {
        return timestampMergeIndexAddr;
    }

    public long getTimestampMin() {
        return timestampMin;
    }

    public long getTxn() {
        return txn;
    }

    public boolean isIndexed() {
        return isIndexed;
    }

    public void of(
            int openColumnMode,
            CharSequence pathToTable,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long timestampMin,
            long timestampMax,
            long oooTimestampLo,
            long oooTimestampHi,
            long srcDataTop,
            long srcDataMax,
            long srcDataTxn,
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
            boolean isIndexed,
            long activeFixFd,
            long activeVarFd,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter
    ) {
        this.openColumnMode = openColumnMode;
        this.pathToTable = pathToTable;
        this.columnCounter = columnCounter;
        this.partCounter = partCounter;
        this.columnName = columnName;
        this.columnType = columnType;
        this.timestampMergeIndexAddr = timestampMergeIndexAddr;
        this.srcOooFixAddr = srcOooFixAddr;
        this.srcOooVarAddr = srcOooVarAddr;
        this.srcOooLo = srcOooLo;
        this.srcOooHi = srcOooHi;
        this.srcOooMax = srcOooMax;
        this.timestampMin = timestampMin;
        this.timestampMax = timestampMax;
        this.oooTimestampLo = oooTimestampLo;
        this.partitionTimestamp = oooTimestampHi;
        this.srcDataTop = srcDataTop;
        this.srcDataMax = srcDataMax;
        this.srcDataTxn = srcDataTxn;
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
        this.isIndexed = isIndexed;
        this.activeFixFd = activeFixFd;
        this.activeVarFd = activeVarFd;
        this.tableWriter = tableWriter;
        this.indexWriter = indexWriter;
    }
}
