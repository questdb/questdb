/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.collections.mmap;

import com.nfsdb.collections.DirectRecordLinkedList;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.storage.ColumnType;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiRecordMap implements Closeable {
    public final MultiMap map;
    private final DirectRecordLinkedList records;

    private MultiRecordMap(int capacity, long dataSize, float loadFactor, int avgRecSize, List<RecordColumnMetadata> keyColumns, RecordMetadata valueMetadata) {
        MultiMap.Builder builder = new MultiMap.Builder();
        for(RecordColumnMetadata key : keyColumns) {
            builder.keyColumn(key);
        }
        builder.valueColumn(new ColumnMetadata() {{
            setName("offset");
            setType(ColumnType.LONG);
        }});
        builder.setLoadFactor(loadFactor);
        builder.setDataSize(dataSize);
        builder.setCapacity(capacity);
        map = builder.build();
        records = new DirectRecordLinkedList(valueMetadata, capacity, avgRecSize);
    }

    public void add(MultiMap.KeyWriter key, Record value) {
        MapValues values = map.getOrCreateValues(key);
        long prevVal = values.isNew() ? -1 : values.getLong(0);
        long newVal = records.append(value, prevVal);
        values.putLong(0, newVal);
    }

    public MultiMap.KeyWriter claimKey() {
        return map.keyWriter();
    }

    public void clear() {
        map.clear();
    }

    @Override
    public void close() throws IOException {
        map.free();
        records.close();
    }

    public RecordCursor<Record> get(MultiMap.KeyWriter key) {
        MapValues values = map.getValues(key);
        records.init(values == null ? -1 : values.getLong(0));
        return records;
    }

    public static class Builder {
        private final List<RecordColumnMetadata> keyColumns = new ArrayList<>();
        private RecordMetadata metadata;
        private int capacity = 10000;
        private long dataSize = 4096;
        private int avgRecordSize = 0;
        private float loadFactor = 0.5f;

        public MultiRecordMap build() {
            assert (metadata != null);

            if (avgRecordSize == 0) {
                avgRecordSize = getAvgRecordSize(metadata);
            }
            return new MultiRecordMap(capacity, dataSize, loadFactor, avgRecordSize, keyColumns, metadata);
        }

        public int getAvgRecordSize(RecordMetadata metadata) {
            int size = 0;
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                ColumnType type = metadata.getColumn(i).getType();
                int colSize = type.size();
                if (colSize == 0) {
                    colSize = 64 * 1024;
                }
                size += colSize;
            }
            return size;
        }

        public Builder keyColumn(RecordColumnMetadata metadata) {
            keyColumns.add(metadata);
            return this;
        }

        public Builder setAvgRecordSize(int avgRecordSize) {
            if (avgRecordSize < 0) {
                throw new IllegalArgumentException("avgRecordSize must be positive");
            }
            this.avgRecordSize = avgRecordSize;
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

        public Builder setRecordMetadata(RecordMetadata metadata) {
            this.metadata = metadata;
            return this;
        }
    }
}
