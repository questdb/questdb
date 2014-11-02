/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.lang.cst.impl.ref;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.column.FixedColumn;
import com.nfsdb.journal.column.SymbolTable;
import com.nfsdb.journal.lang.cst.IntVariable;
import com.nfsdb.journal.lang.cst.IntVariableSource;
import com.nfsdb.journal.lang.cst.PartitionSlice;
import com.nfsdb.journal.lang.cst.StatefulJournalSource;

import java.util.Arrays;

/**
 * Reads partition/rowid from current position of JoinedSource, reads symbol column value based on current position of said JoinedSource
 */
public class SymbolXTabVariableSource implements IntVariableSource, IntVariable {
    private final StatefulJournalSource masterSource;
    private final String slaveSymbol;
    private final int masterColumnIndex;
    private final int map[];
    private final SymbolTable masterTab;
    private SymbolTable slaveTab;
    private FixedColumn column;
    private Partition partition;
    private long rowid = -1;
    private int slaveKey;

    public SymbolXTabVariableSource(StatefulJournalSource masterSource, String masterSymbol, String slaveSymbol) {
        this.masterSource = masterSource;
        this.slaveSymbol = slaveSymbol;
        this.masterColumnIndex = masterSource.getJournal().getMetadata().getColumnIndex(masterSymbol);
        this.masterTab = masterSource.getJournal().getSymbolTable(masterSymbol);
        map = new int[masterTab.size()];
        Arrays.fill(map, -1);
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
            int masterKey = column.getInt(rowid);
            if (map[masterKey] == -1) {
                map[masterKey] = slaveTab.getQuick(masterTab.value(masterKey));
            }
            slaveKey = map[masterKey];
        }
        return slaveKey;
    }

    private boolean switchPartition() {
        if (masterSource.current().partition == partition) {
            return false;
        }
        partition = masterSource.current().partition;
        column = (FixedColumn) partition.getAbstractColumn(masterColumnIndex);
        return true;
    }
}
