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

package com.nfsdb.lang.cst.impl.join;

import com.nfsdb.Journal;
import com.nfsdb.Partition;
import com.nfsdb.column.FixedColumn;
import com.nfsdb.column.SymbolTable;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.lang.cst.RowCursor;
import com.nfsdb.lang.cst.impl.dfrm.JournalRowSourceHash;
import com.nfsdb.lang.cst.impl.qry.AbstractJournalSource;
import com.nfsdb.lang.cst.impl.qry.JournalRecord;
import com.nfsdb.lang.cst.impl.qry.JournalRecordSource;
import com.nfsdb.lang.cst.impl.qry.RecordMetadata;
import com.nfsdb.lang.cst.impl.ref.StringRef;
import com.nfsdb.utils.Rows;

import java.util.Arrays;

public class SymbolOuterHashJoin extends AbstractJournalSource implements JournalRecordSource {
    private final JournalRecordSource masterSource;
    private final JournalRowSourceHash hash;
    private final StringRef masterSymbol;
    private final StringRef slaveSymbol;
    private final JournalRecord journalRecord = new JournalRecord(this);
    private JournalRecord joinedData;
    private int columnIndex;
    private SymbolTable masterTab;
    private SymbolTable slaveTab;
    private Partition lastMasterPartition;
    private int lastSlavePartIndex = -1;
    private FixedColumn column;
    private boolean nextSlave = false;
    private int[] map;
    private RowCursor slaveCursor;

    public SymbolOuterHashJoin(JournalRecordSource masterSource, StringRef masterSymbol, JournalRowSourceHash hash, StringRef slaveSymbol) {
        this.masterSource = masterSource;
        this.hash = hash;
        this.masterSymbol = masterSymbol;
        this.slaveSymbol = slaveSymbol;
        init();
    }

    @Override
    public void reset() {
        masterSource.reset();
        hash.reset();
        nextSlave = false;
        init();
    }

    private void init() {
        this.columnIndex = masterSource.getJournal().getMetadata().getColumnIndex(masterSymbol.value);
        this.masterTab = masterSource.getJournal().getSymbolTable(masterSymbol.value);
        this.slaveTab = hash.getJournal().getSymbolTable(slaveSymbol.value);
        int sz = masterTab.size();
        if (map == null || map.length < sz) {
            map = new int[sz];
        }
        Arrays.fill(map, -1);
        lastSlavePartIndex = -1;
        lastMasterPartition = null;
    }

    @Override
    public Journal getJournal() {
        return masterSource.getJournal();
    }

    @Override
    public boolean hasNext() {
        return nextSlave || masterSource.hasNext();
    }

    @Override
    @SuppressWarnings("unchecked")
    public JournalRecord next() {

        if (!nextSlave) {
            nextMaster();
        }

        if (nextSlave || slaveCursor.hasNext()) {
            long rowid = slaveCursor.next();
            int pind = Rows.toPartitionIndex(rowid);
            if (lastSlavePartIndex != pind) {
                try {
                    journalRecord.partition = hash.getJournal().getPartition(pind, false);
                    lastSlavePartIndex = pind;
                } catch (JournalException e) {
                    throw new JournalRuntimeException(e);
                }
            }
            journalRecord.rowid = Rows.toLocalRowID(rowid);
            joinedData.setSlave(journalRecord);
            nextSlave = slaveCursor.hasNext();
        } else {
            joinedData.setSlave(null);
            nextSlave = false;
        }

        return joinedData;
    }

    private void nextMaster() {

        JournalRecord m = masterSource.next();
        if (lastMasterPartition != m.partition) {
            lastMasterPartition = m.partition;
            column = (FixedColumn) m.partition.getAbstractColumn(columnIndex);
        }
        int masterKey = column.getInt(m.rowid);

        if (map[masterKey] == -1) {
            map[masterKey] = slaveTab.getQuick(masterTab.value(masterKey));
        }

        slaveCursor = hash.cursor(map[masterKey]);
        joinedData = m;
    }

    @Override
    public RecordMetadata nextMetadata() {
        return hash.getMetadata();
    }
}
