package com.questdb.cairo;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.store.SymbolTable;

class TableColumnMetadata implements RecordColumnMetadata {
    private final int type;
    private String name;

    public TableColumnMetadata(String name, int type) {
        this.name = name;
        this.type = type;
    }

    @Override
    public int getBucketCount() {
        return 0;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public SymbolTable getSymbolTable() {
        return null;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public boolean isIndexed() {
        return false;
    }
}
