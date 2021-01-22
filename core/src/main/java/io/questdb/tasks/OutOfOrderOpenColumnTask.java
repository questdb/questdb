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
import io.questdb.std.AbstractLockable;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class OutOfOrderOpenColumnTask extends AbstractLockable implements Closeable {
    private final Path path = new Path();
    private FilesFacade ff;
    private long txn;
    private int openColumnMode;
    private CharSequence columnName;
    private int columnType;
    private boolean isColumnIndexed;
    private long oooIndexLo;
    private long oooIndexHi;
    private long oooIndexMax;
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
    private long dataIndexMax;
    private AppendMemory fixColumn;
    private AppendMemory varColumn;
    private ContiguousVirtualMemory oooFixColumn;
    private ContiguousVirtualMemory oooVarColumn;

    @Override
    public void close() {
        Misc.free(path);
    }

    public CharSequence getColumnName() {
        return columnName;
    }

    public int getColumnType() {
        return columnType;
    }

    public long getDataIndexMax() {
        return dataIndexMax;
    }

    public FilesFacade getFf() {
        return ff;
    }

    public AppendMemory getFixColumn() {
        return fixColumn;
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

    public ContiguousVirtualMemory getOooFixColumn() {
        return oooFixColumn;
    }

    public long getOooIndexHi() {
        return oooIndexHi;
    }

    public long getOooIndexLo() {
        return oooIndexLo;
    }

    public long getOooIndexMax() {
        return oooIndexMax;
    }

    public ContiguousVirtualMemory getOooVarColumn() {
        return oooVarColumn;
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

    public long getSuffixHi() {
        return suffixHi;
    }

    public long getSuffixLo() {
        return suffixLo;
    }

    public int getSuffixType() {
        return suffixType;
    }

    public long getTxn() {
        return txn;
    }

    public AppendMemory getVarColumn() {
        return varColumn;
    }

    public boolean isColumnIndexed() {
        return isColumnIndexed;
    }

    public void of(
            FilesFacade ff,
            long txn,
            int openColumnMode,
            CharSequence columnName,
            int columnType,
            boolean isColumnIndexed,
            AppendMemory fixColumn,
            AppendMemory varColumn,
            ContiguousVirtualMemory oooFixColumn,
            ContiguousVirtualMemory oooVarColumn,
            CharSequence path,
            long oooIndexLo,
            long oooIndexHi,
            long oooIndexMax,
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
            long dataIndexMax
    ) {
        // todo: copy path
        this.ff = ff;
        this.txn = txn;
        this.columnName = columnName;
        this.columnType = columnType;
        this.isColumnIndexed = isColumnIndexed;
        this.fixColumn = fixColumn;
        this.varColumn = varColumn;
        this.oooFixColumn = oooFixColumn;
        this.oooVarColumn = oooVarColumn;
        this.oooIndexLo = oooIndexLo;
        this.oooIndexHi = oooIndexHi;
        this.oooIndexMax = oooIndexMax;
        this.openColumnMode = openColumnMode;
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
        this.dataIndexMax = dataIndexMax;
    }
}
