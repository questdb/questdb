package com.nfsdb.lang.cst.impl.join;

import com.nfsdb.collections.mmap.MapValues;
import com.nfsdb.collections.mmap.MultiMap;
import com.nfsdb.collections.mmap.MultiRecordMap;
import com.nfsdb.column.ColumnType;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.lang.cst.impl.qry.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.swing.plaf.multi.MultiInternalFrameUI;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class RowIdJoinHashTable implements JoinHashTable, Closeable {
    private final static String keyColumnName = "KY_0C81_=";
    // Some hardly used in real column life name.
    private final static String valueColumnName = "VAL_0C81_=";

    public final MultiRecordMap hashTable;
    private final RandomAccessRecordSource<? extends Record> source;

    public RowIdJoinHashTable(RandomAccessRecordSource<? extends Record> source, String hashColumn) {
        this.source = source;
        hashTable = buildHashTable(source, hashColumn);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public RecordSource<? extends Record> getRows(Record master, int columnIndex, ColumnType type) {
        throw new NotImplementedException();
//        MultiMap.Key mapKey = hashTable.claimKey();
//        MultiMap.putKey(mapKey, master, columnIndex, type);
//        final MapValues values = hashTable.claimSlot(mapKey);
//        final int count = values.isNew() ? 0 : values.getInt(0);
//
//        return new RecordSource<Record>() {
//            int i = 0;
//
//            @Override
//            public RecordMetadata getMetadata() {
//                return source.getMetadata();
//            }
//
//            @Override
//            public void reset() {
//                i = -1;
//            }
//
//            @Override
//            public Iterator<Record> iterator() {
//                return this;
//            }
//
//            @Override
//            public boolean hasNext() {
//                return i++ < count;
//            }
//
//            @Override
//            public Record next() {
//                return source.getByRowId(values.getLong(i));
//            }
//
//            @Override
//            public void remove() {
//            }
//        };
    }

    private MultiRecordMap buildHashTable(RandomAccessRecordSource<? extends Record> source, String hashColumn) {
        int colIndex = source.getMetadata().getColumnIndex(hashColumn);
        RecordColumnMetadata key = source.getMetadata().getColumn(colIndex);
        ColumnType colType = key.getType();

        MultiRecordMap.Builder builder = new MultiRecordMap.Builder();
        builder.keyColumn(key);
//        List<>
//        RecordMetadata valueMetadata = new ColumnListRecordMetadata();
//
//        for(Record r : source) {
//            MultiMap.Key mapKey = map.claimKey();
//            MultiMap.putKey(mapKey, r, colIndex, colType);
//            mapKey.commit();
//
//            MapValues values = map.claimSlot(mapKey);
//            int nextIndex = values.isNew() ? 1 : values.getInt(0) + 1;
//            values.putInt(0, nextIndex);
//            values.putLong(nextIndex, r.getRowId());
//        }

        return builder.build();
    }
}
