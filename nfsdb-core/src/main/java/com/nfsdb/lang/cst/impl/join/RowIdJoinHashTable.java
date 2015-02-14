package com.nfsdb.lang.cst.impl.join;

import com.nfsdb.collections.mmap.MultiMap;
import com.nfsdb.column.ColumnType;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.lang.cst.impl.qry.RandomAccessRecordSource;
import com.nfsdb.lang.cst.impl.qry.Record;
import com.nfsdb.lang.cst.impl.qry.RecordColumn;
import com.nfsdb.lang.cst.impl.qry.RecordSource;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.swing.plaf.multi.MultiInternalFrameUI;
import java.io.Closeable;
import java.io.IOException;

public class RowIdJoinHashTable implements JoinHashTable, Closeable {
    private final static String keyColumnName = "KY_0C81_=";
    private final static String valueColumnName = "VAL_0C81_=";

    public final MultiMap hashTable;

    public RowIdJoinHashTable(RandomAccessRecordSource<? extends Record> source, String hashColumn) {
        hashTable = buildHashTable(source, hashColumn);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public RecordSource<? extends Record> getRows(Object key) {
        throw new NotImplementedException();

    }

    private MultiMap buildHashTable(RandomAccessRecordSource<? extends Record> source, String hashColumn) {
        int colIndex = source.getMetadata().getColumnIndex(hashColumn);
        RecordColumnMetadata key = source.getMetadata().getColumn(colIndex);
        ColumnType colType = key.getType();

        MultiMap.Builder builder = new MultiMap.Builder();
        builder.keyColumn(key);
        builder.valueColumn(new RecordColumn(valueColumnName, ColumnType.LONG, null));
        MultiMap map = builder.build();

        for(Record r : source) {
            MultiMap.Key mapKey = map.claimKey();
            MultiMap.putKey(mapKey, r, colIndex, colType);
        }
        return map;
    }

}
