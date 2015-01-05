/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.lang.cst.impl.ref;

import com.nfsdb.Partition;
import com.nfsdb.column.SymbolTable;
import com.nfsdb.column.VariableColumn;
import com.nfsdb.lang.cst.IntVariable;
import com.nfsdb.lang.cst.IntVariableSource;
import com.nfsdb.lang.cst.PartitionSlice;
import com.nfsdb.lang.cst.StatefulJournalSource;

/**
 * Reads partition/rowid from current position of JoinedSource, reads symbol column value based on current position of said JoinedSource
 */
public class StringXTabVariableSource implements IntVariableSource, IntVariable {
    private final StatefulJournalSource masterSource;
    private final String slaveSymbol;
    private final int masterColumnIndex;
    private SymbolTable slaveTab;
    private VariableColumn column;
    private Partition partition;
    private int slaveKey;
    private long rowid = -1;

    public StringXTabVariableSource(StatefulJournalSource masterSource, String masterSymbol, String slaveSymbol) {
        this.masterSource = masterSource;
        this.slaveSymbol = slaveSymbol;
        this.masterColumnIndex = masterSource.getJournal().getMetadata().getColumnIndex(masterSymbol);
    }

    @Override
    public IntVariable getVariable(PartitionSlice slice) {
        if (slaveTab == null) {
            slaveTab = slice.partition.getJournal().getSymbolTable(slaveSymbol);
        }
        return this;
    }

    @Override
    public int getValue() {
        if (switchPartition() || masterSource.current().rowid != rowid) {
            rowid = masterSource.current().rowid;
            slaveKey = slaveTab.getQuick(column.getStr(rowid));
        }
        return slaveKey;
    }

    private boolean switchPartition() {
        if (masterSource.current().partition == partition) {
            return false;
        }
        partition = masterSource.current().partition;
        column = (VariableColumn) partition.getAbstractColumn(masterColumnIndex);
        return true;
    }
}
