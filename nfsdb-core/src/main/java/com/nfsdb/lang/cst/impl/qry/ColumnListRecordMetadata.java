package com.nfsdb.lang.cst.impl.qry;

import com.nfsdb.collections.ObjIntHashMap;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.lang.cst.RecordMetadata;
import java.util.Arrays;
import java.util.List;

public class ColumnListRecordMetadata implements RecordMetadata {
    private final List<RecordColumnMetadata> columns;
    private final ObjIntHashMap<CharSequence> columnIndices = new ObjIntHashMap<CharSequence>();

    public ColumnListRecordMetadata(RecordColumnMetadata ... records) {
        this.columns = Arrays.asList(records);
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public int getColumnIndex(CharSequence name) {
        return columnIndices.get(name);
    }

    @Override
    public RecordColumnMetadata getColumn(int index) {
        return columns.get(index);
    }

    @Override
    public RecordColumnMetadata getColumn(CharSequence name) {
        return columns.get(columnIndices.get(name));
    }
}
