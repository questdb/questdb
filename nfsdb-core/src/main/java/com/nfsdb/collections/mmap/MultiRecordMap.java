package com.nfsdb.collections.mmap;

import com.nfsdb.collections.DirectRecordLinkedList;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordMetadata;
import com.nfsdb.lang.cst.impl.qry.RecordColumn;
import com.nfsdb.storage.ColumnType;
import com.sun.javaws.exceptions.InvalidArgumentException;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiRecordMap implements Closeable {
    private final MultiMap map;
    private final DirectRecordLinkedList records;

    private MultiRecordMap(int capacity, long dataSize, float loadFactor, int avgRecSize, List<RecordColumnMetadata> keyColumns, RecordMetadata valueMetadata) {
        MultiMap.Builder builder = new MultiMap.Builder();
        for(RecordColumnMetadata key : keyColumns) {
            builder.keyColumn(key);
        }
        builder.valueColumn(new RecordColumn("offset", ColumnType.LONG, null));
        builder.setLoadFactor(loadFactor);
        builder.setDataSize(dataSize);
        builder.setCapacity(capacity);
        map = builder.build();
        records = new DirectRecordLinkedList(valueMetadata, capacity, avgRecSize);
    }

    public MultiMap.KeyWriter claimKey() {
        return map.keyWriter();
    }

    public void add(MultiMap.KeyWriter key, Record value) {
        MapValues values = map.values(key);
        long prevVal = values.isNew() ? -1 : values.getLong(0);
        long newVal = records.append(value, prevVal);
        values.putLong(0, newVal);
    }

    public RecordSource<Record> get(MultiMap.Key key) {
        MapValues values = map.claimSlot(key);
        long offset = values.isNew() ? -1 : values.getLong(0);
        records.init(offset);
        return records;
    }

    @Override
    public void close() throws IOException {
        free();
    }

    @Override
    protected void finalize() throws Throwable {
        free();
        super.finalize();
    }

    private void free() throws IOException {
        map.close();
        records.close();
    }

    public static class Builder {
        private final List<RecordColumnMetadata> keyColumns = new ArrayList<>();
        private RecordMetadata metadata;
        private int capacity = 67;
        private long dataSize = 4096;
        private int avgRecordSize = 0;
        private float loadFactor = 0.5f;

        public Builder setRecordMetadata(RecordMetadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder keyColumn(RecordColumnMetadata metadata) {
            keyColumns.add(metadata);
            return this;
        }

        public Builder setCapacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

        public Builder setDataSize(long dataSize) {
            this.dataSize = dataSize;
            return this;
        }

        public Builder setLoadFactor(float loadFactor) {
            this.loadFactor = loadFactor;
            return this;
        }

        public Builder setAvgRecordSize(int avgRecordSize) throws InvalidArgumentException {
            if (avgRecordSize < 0) {
                throw new InvalidArgumentException(new String[] { "avgRecordSize must be positive" } );
            }
            this.avgRecordSize = avgRecordSize;
            return this;
        }

        public MultiRecordMap build() {
            assert(metadata != null);

            if (avgRecordSize == 0) {
                avgRecordSize = getAvgRecordSize(metadata);
            }
            return new MultiRecordMap(capacity, dataSize, loadFactor, avgRecordSize, keyColumns, metadata);
        }

        public int getAvgRecordSize(RecordMetadata metadata) {
            int size = 0;
            for(int i = 0; i <  metadata.getColumnCount(); i++) {
                ColumnType type = metadata.getColumnType(i);
                int colSize = type.size();
                if (colSize == 0) {
                    colSize = 64 * 1024;
                }
                size += colSize;
            }
            return size;
        }
    }
}
