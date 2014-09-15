package com.nfsdb.journal.lang.cst.impl.join;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.column.FixedColumn;
import com.nfsdb.journal.column.SymbolTable;
import com.nfsdb.journal.lang.cst.DataItem;
import com.nfsdb.journal.lang.cst.JoinedData;
import com.nfsdb.journal.lang.cst.JoinedSource;
import com.nfsdb.journal.lang.cst.JournalSource;
import com.nfsdb.journal.lang.cst.impl.ref.IntRef;
import com.nfsdb.journal.lang.cst.impl.ref.StringRef;

import java.util.Arrays;

public class SymbolJoin extends AbstractImmutableIterator<JoinedData> implements JoinedSource {
    private final JournalSource masterSource;
    private final JournalSource slaveSource;
    private final IntRef keyRef;
    private final JoinedData joinedData = new JoinedData();
    private final StringRef masterSymbol;
    private final StringRef slaveSymbol;
    private int columnIndex;
    private SymbolTable masterTab;
    private SymbolTable slaveTab;
    private Partition lastPartition;
    private FixedColumn column;
    private boolean nextSlave = false;
    private boolean initMap = true;
    private int[] map;

    public SymbolJoin(JournalSource masterSource, StringRef masterSymbol, JournalSource slaveSource, StringRef slaveSymbol, IntRef keyRef) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;
        this.keyRef = keyRef;
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
    public JoinedData next() {

        if (!nextSlave) {
            nextMaster();
        }

        if (nextSlave = slaveSource.hasNext()) {
            joinedData.s = slaveSource.next();
        } else {
            joinedData.s = null;
        }

        return joinedData;
    }

    private void nextMaster() {
        DataItem m = masterSource.next();
        if (lastPartition != m.partition) {
            lastPartition = m.partition;
            column = (FixedColumn) m.partition.getAbstractColumn(columnIndex);
        }
        joinedData.m = m;

        if (initMap) {
            int sz = masterTab.size();
            if (map == null || map.length < sz) {
                map = new int[sz];
            }
            Arrays.fill(map, -1);
            initMap = false;
        }

        int masterKey = column.getInt(joinedData.m.rowid);
        int slaveKey = map[masterKey];

        if (slaveKey == -1) {
            slaveKey = slaveTab.getQuick(masterTab.value(masterKey));
            map[masterKey] = slaveKey;
        }

        keyRef.value = slaveKey;
        slaveSource.reset();
    }
}
