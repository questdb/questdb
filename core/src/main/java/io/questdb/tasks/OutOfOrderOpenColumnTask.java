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
import io.questdb.std.AbstractLockable;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

public class OutOfOrderOpenColumnTask extends AbstractLockable implements Closeable {
    private final Path path = new Path();
    private FilesFacade ff;
    private AtomicInteger columnCounter;
    private long txn;
    private int openColumnMode;
    private CharSequence columnName;
    private int columnType;
    private boolean isIndexed;
    private long timestampFd;
    private long timestampMergeIndexAddr;
    private AppendMemory srcDataFixColumn;
    private AppendMemory srcDataVarColumn;
    private long srcOooFixAddr;
    private long srcOooFixSize;
    private long srcOooVarAddr;
    private long srcOooVarSize;
    private long srcDataMax;
    private long srcOooLo;
    private long srcOooHi;
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

    @Override
    public void close() {
        Misc.free(path);
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

    public FilesFacade getFf() {
        return ff;
    }

    public AppendMemory getSrcDataFixColumn() {
        return srcDataFixColumn;
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

    public int getOpenColumnMode() {
        return openColumnMode;
    }

    public Path getPath() {
        return path;
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

    public long getSrcOooVarAddr() {
        return srcOooVarAddr;
    }

    public long getSrcOooVarSize() {
        return srcOooVarSize;
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

    public long getTimestampFd() {
        return timestampFd;
    }

    public long getTimestampMergeIndexAddr() {
        return timestampMergeIndexAddr;
    }

    public long getTxn() {
        return txn;
    }

    public AppendMemory getSrcDataVarColumn() {
        return srcDataVarColumn;
    }

    public boolean isIndexed() {
        return isIndexed;
    }

    public void of(
            FilesFacade ff,
            AtomicInteger columnCounter,
            long txn,
            int openColumnMode,
            CharSequence columnName,
            int columnType,
            boolean isIndexed,
            long timestampFd,
            long timestampMergeIndexAddr,
            AppendMemory srcDataFixColumn,
            AppendMemory srcDataVarColumn,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            CharSequence path,
            long srcOooLo,
            long srcOooHi,
            long srcDataMax,
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
            long suffixHi
    ) {
        // todo: copy path
        this.ff = ff;
        this.columnCounter = columnCounter;
        this.txn = txn;
        this.openColumnMode = openColumnMode;
        this.columnName = columnName;
        this.columnType = columnType;
        this.isIndexed = isIndexed;
        this.timestampFd = timestampFd;
        this.timestampMergeIndexAddr = timestampMergeIndexAddr;
        this.srcDataFixColumn = srcDataFixColumn;
        this.srcDataVarColumn = srcDataVarColumn;
        this.srcOooFixAddr = srcOooFixAddr;
        this.srcOooFixSize = srcOooFixSize;
        this.srcOooVarAddr = srcOooVarAddr;
        this.srcOooVarSize = srcOooVarSize;
        this.srcOooLo = srcOooLo;
        this.srcOooHi = srcOooHi;
        this.srcDataMax = srcDataMax;
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
    }
}
