package com.nfsdb.lang.cst.impl.qry;


import com.nfsdb.storage.ColumnType;
import com.nfsdb.storage.SymbolTable;

public final class RecordColumn implements com.nfsdb.factory.configuration.RecordColumnMetadata {

    private final String name;
    private final ColumnType type;
    private final SymbolTable sTable;

    public RecordColumn(String name, ColumnType type) {
        this(name, type, null);
    }

    public RecordColumn(String name, ColumnType type, SymbolTable sTable) {
        this.name = name;
        this.type = type;
        this.sTable = sTable;
    }

    @Override
    public ColumnType getType() {
        return type;
    }

    @Override
    public SymbolTable getSymbolTable() {
        return sTable;
    }

    @Override
    public String getName() {
        return name;
    }
}
