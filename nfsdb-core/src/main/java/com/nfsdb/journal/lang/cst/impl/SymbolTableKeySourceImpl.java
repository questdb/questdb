package com.nfsdb.journal.lang.cst.impl;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.column.SymbolTable;
import com.nfsdb.journal.lang.cst.KeyCursor;
import com.nfsdb.journal.lang.cst.KeySource;

public class SymbolTableKeySourceImpl implements KeySource, KeyCursor {

    private final String symbol;
    private final String[] values;
    private final int[] keys;
    private SymbolTable symbolTable;
    private int keyIndex;
    private int keyCount;

    public SymbolTableKeySourceImpl(String symbol, String[] values) {
        this.symbol = symbol;
        this.values = values;
        this.keys = new int[values.length];
    }

    @Override
    public KeyCursor cursor(Partition partition) {
        if (this.symbolTable == null) {
            this.symbolTable = partition.getJournal().getSymbolTable(symbol);
            int keyCount = 0;
            for (int i = 0; i < values.length; i++) {
                int key = symbolTable.getQuick(values[i]);
                if (key >= 0) {
                    keys[keyCount++] = key;
                }
            }
            this.keyCount = keyCount;
        }
        this.keyIndex = 0;
        return this;
    }

    @Override
    public boolean hasNext() {
        return keyIndex < keyCount;
    }

    @Override
    public int next() {
        return keys[keyIndex++];
    }
}
