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

package com.nfsdb.journal.lang.cst.impl.join;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.column.FixedColumn;
import com.nfsdb.journal.column.SymbolTable;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.lang.cst.DataItem;
import com.nfsdb.journal.lang.cst.JoinedSource;
import com.nfsdb.journal.lang.cst.JournalSource;
import com.nfsdb.journal.lang.cst.RowCursor;
import com.nfsdb.journal.lang.cst.impl.dfrn.DataFrame;
import com.nfsdb.journal.lang.cst.impl.dfrn.DataFrameSource;
import com.nfsdb.journal.lang.cst.impl.ref.StringRef;
import com.nfsdb.journal.utils.Rows;

import java.util.Arrays;

public class SymbolToFrameOuterJoin extends AbstractImmutableIterator<DataItem> implements JoinedSource {
    private final JournalSource masterSource;
    private final DataFrameSource slaveSource;
    private final StringRef masterSymbol;
    private final StringRef slaveSymbol;
    private DataItem joinedData;
    private DataFrame frame;
    private int columnIndex;
    private SymbolTable masterTab;
    private SymbolTable slaveTab;
    private Partition lastPartition;
    private FixedColumn column;
    private boolean nextSlave = false;
    private boolean initMap = true;
    private int[] map;
    private RowCursor slaveCursor;
    private DataItem dataItem = new DataItem();

    public SymbolToFrameOuterJoin(JournalSource masterSource, StringRef masterSymbol, DataFrameSource frameSource, StringRef slaveSymbol) {
        this.masterSource = masterSource;
        this.slaveSource = frameSource;
        this.masterSymbol = masterSymbol;
        this.slaveSymbol = slaveSymbol;
        this.columnIndex = masterSource.getJournal().getMetadata().getColumnIndex(masterSymbol.value);
        this.masterTab = masterSource.getJournal().getSymbolTable(masterSymbol.value);
        this.slaveTab = slaveSource.getJournal().getSymbolTable(slaveSymbol.value);
    }

    @Override
    public void reset() {
        masterSource.reset();
        slaveSource.reset();
        nextSlave = false;
        initMap = true;
        this.columnIndex = masterSource.getJournal().getMetadata().getColumnIndex(masterSymbol.value);
        this.masterTab = masterSource.getJournal().getSymbolTable(masterSymbol.value);
        this.slaveTab = slaveSource.getJournal().getSymbolTable(slaveSymbol.value);
    }

    @Override
    public boolean hasNext() {
        return nextSlave || masterSource.hasNext();
    }

    @Override
    @SuppressWarnings("unchecked")
    public DataItem next() {

        if (!nextSlave) {
            nextMaster();
        }

        if (nextSlave || slaveCursor.hasNext()) {
            long rowid = slaveCursor.next();
            try {
                dataItem.partition = slaveSource.getJournal().getPartition(Rows.toPartitionIndex(rowid), false);
                dataItem.rowid = Rows.toLocalRowID(rowid);
            } catch (JournalException e) {
                throw new JournalRuntimeException(e);
            }
            joinedData.slave = dataItem;
            nextSlave = slaveCursor.hasNext();
        } else {
            joinedData.slave = null;
            nextSlave = false;
        }

        return joinedData;
    }

    private void nextMaster() {

        if (frame == null) {
            frame = slaveSource.getFrame();
        }

        DataItem m = masterSource.next();
        if (lastPartition != m.partition) {
            lastPartition = m.partition;
            column = (FixedColumn) m.partition.getAbstractColumn(columnIndex);
        }

        joinedData = m;

        if (initMap) {
            int sz = masterTab.size();
            if (map == null || map.length < sz) {
                map = new int[sz];
            }
            Arrays.fill(map, -1);
            initMap = false;
        }

        int masterKey = column.getInt(joinedData.rowid);
        int slaveKey = map[masterKey];

        if (slaveKey == -1) {
            map[masterKey] = slaveKey = slaveTab.getQuick(masterTab.value(masterKey));
        }

        slaveCursor = frame.cursor(slaveKey);
    }
}
