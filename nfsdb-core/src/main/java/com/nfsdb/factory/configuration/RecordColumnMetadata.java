package com.nfsdb.factory.configuration;

import com.nfsdb.storage.ColumnType;
import com.nfsdb.storage.SymbolTable;

public interface RecordColumnMetadata {
    ColumnType getType();

    SymbolTable getSymbolTable();

    String getName();
}
