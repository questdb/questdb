package com.nfsdb.lang.cst.impl.join;

import com.nfsdb.collections.mmap.MultiMap;
import com.nfsdb.collections.mmap.MultiRecordMap;
import com.nfsdb.column.DirectInputStream;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordMetadata;
import com.nfsdb.lang.cst.RecordSource;
import com.nfsdb.lang.cst.impl.qry.*;
import com.nfsdb.storage.ColumnType;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

public class RowIdJoinHashTable implements JoinHashTable, Closeable {
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
        MultiMap.KeyWriter mapKey = hashTable.claimKey();
        MultiMap.putKey(mapKey, master, columnIndex, type);
        mapKey.commit();
        final RecordSource<Record> ids = hashTable.get(mapKey);

        return new RecordSource<Record>() {
            @Override
            public RecordMetadata getMetadata() {
                return source.getMetadata();
            }

            @Override
            public void reset() {
                ids.reset();
            }

            @Override
            public Iterator<Record> iterator() {
                return this;
            }

            @Override
            public boolean hasNext() {
                return ids.hasNext();
            }

            @Override
            public Record next() {
                return source.getByRowId(ids.next().getLong(0));
            }

            @Override
            public void remove() {
            }
        };
    }

    private MultiRecordMap buildHashTable(RandomAccessRecordSource<? extends Record> source, String hashColumn) {
        int colIndex = source.getMetadata().getColumnIndex(hashColumn);
        RecordColumnMetadata key = source.getMetadata().getColumn(colIndex);
        ColumnType colType = key.getType();

        IdRecordWrapper wrapper = new IdRecordWrapper();
        MultiRecordMap.Builder builder = new MultiRecordMap.Builder();
        builder.keyColumn(key);
        builder.setRecordMetadata(wrapper.getMetadata());
        MultiRecordMap map = builder.build();

        for(Record r : source) {
            MultiMap.KeyWriter mapKey = map.claimKey();
            MultiMap.putKey(mapKey, r, colIndex, colType);
            mapKey.commit();
            wrapper.setId(r.getRowId());
            map.add(mapKey, wrapper);
        }

        return map;
    }

    private static class IdRecordWrapper implements Record {
        private final static String keyColumnName = "KY_0C81_=";
        private final static RecordMetadata idColumnMetadata;
        private long id;

        static {
            ColumnMetadata idColumn = new ColumnMetadata();
            idColumn.setType(ColumnType.LONG);
            idColumn.setName(keyColumnName);
            idColumnMetadata = new ColumnListRecordMetadata(idColumn);
        }

        public void setId (long id) {
            this.id = id;
        }

        @Override
        public long getRowId() {
            return id;
        }

        @Override
        public RecordMetadata getMetadata() {
            return idColumnMetadata;
        }

        @Override
        public byte get(String column) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte get(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getBin(int col, OutputStream s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getBin(String column, OutputStream s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getBool(String column) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getBool(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getDate(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble(String column) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getFloat(String column) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getFloat(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getInt(String column) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getInt(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(String column) {
            return id;
        }

        @Override
        public long getLong(int col) {
            return id;
        }

        @Override
        public short getShort(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getStr(String column) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getStr(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getStr(int col, CharSink sink) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getSym(String column) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getSym(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DirectInputStream getBin(String column) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DirectInputStream getBin(int col) {
            throw new UnsupportedOperationException();
        }
    }
}
