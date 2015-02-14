package com.nfsdb.factory.configuration;

import com.nfsdb.column.ColumnType;
import com.nfsdb.column.SymbolTable;

public interface RecordColumnMetadata {
    ColumnType getType();

    SymbolTable getSymbolTable();

    String getName();
}
