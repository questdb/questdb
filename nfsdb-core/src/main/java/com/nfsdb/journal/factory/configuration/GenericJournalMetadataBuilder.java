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

package com.nfsdb.journal.factory.configuration;

import com.nfsdb.journal.PartitionType;
import com.nfsdb.journal.column.ColumnType;
import com.nfsdb.journal.exceptions.JournalConfigurationException;
import com.nfsdb.journal.utils.ByteBuffers;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GenericJournalMetadataBuilder implements JMetadataBuilder {
    private final String location;
    private final List<ColumnMetadata> metadata = new ArrayList<>();
    private final TObjectIntMap<String> nameToIndexMap = new TObjectIntHashMap<>(10, 0.2f, -1);
    private int tsColumnIndex = -1;
    private PartitionType partitionBy = PartitionType.NONE;
    private int recordCountHint = 100000;
    private int txCountHint = -1;
    private String key;
    private long openFileTTL = TimeUnit.MINUTES.toMillis(3);
    private int lag = -1;

    public GenericJournalMetadataBuilder(String location) {
        this.location = location;
    }

    public GenericJournalMetadataBuilder(JournalMetadata model) {
        this.location = model.getLocation();
        this.tsColumnIndex = model.getTimestampColumnIndex();
        this.partitionBy = model.getPartitionType();
        this.recordCountHint = model.getRecordHint();
        this.txCountHint = model.getTxCountHint();
        this.key = model.getKeyQuiet();
        this.openFileTTL = model.getOpenFileTTL();
        this.lag = model.getLag();
        for (int i = 0; i < model.getColumnCount(); i++) {
            ColumnMetadata from = model.getColumnMetadata(i);
            ColumnMetadata to = new ColumnMetadata();
            to.copy(from);
            metadata.add(to);
            nameToIndexMap.put(to.name, i);
        }

    }

    public GenericSymbolBuilder $sym(String name) {
        return new GenericSymbolBuilder(this, newMeta(name));
    }

    public GenericStringBuilder $str(String name) {
        return new GenericStringBuilder(this, newMeta(name));
    }

    public GenericBinaryBuilder $bin(String name) {
        return new GenericBinaryBuilder(this, newMeta(name));
    }

    public GenericIntBuilder $int(String name) {
        return new GenericIntBuilder(this, newMeta(name));
    }

    public GenericJournalMetadataBuilder $ts() {
        return $ts("timestamp");
    }

    public GenericJournalMetadataBuilder $ts(String name) {
        ColumnMetadata meta = newMeta(name);
        meta.type = ColumnType.DATE;
        meta.size = 8;
        return this;
    }

    public GenericJournalMetadataBuilder $date(String name) {
        ColumnMetadata meta = newMeta(name);
        meta.type = ColumnType.DATE;
        meta.size = 8;
        return this;
    }

    public GenericJournalMetadataBuilder partitionBy(PartitionType type) {
        this.partitionBy = type;
        return this;
    }

    public GenericJournalMetadataBuilder recordCountHint(int count) {
        this.recordCountHint = count;
        return this;
    }

    public GenericJournalMetadataBuilder txCountHint(int count) {
        this.txCountHint = count;
        return this;
    }

    public GenericJournalMetadataBuilder key(String key) {
        this.key = key;
        return this;
    }

    public GenericJournalMetadataBuilder openFileTTL(long time, TimeUnit unit) {
        this.openFileTTL = unit.toMillis(time);
        return this;
    }

    public GenericJournalMetadataBuilder lag(long time, TimeUnit unit) {
        this.lag = (int) unit.toHours(time);
        return this;
    }

    public String getLocation() {
        return location;
    }

    @SuppressWarnings("unchecked")
    public JournalMetadata build() {

        // default tx count hint
        if (txCountHint == -1) {
            txCountHint = (int) (recordCountHint * 0.1);
        }


        ColumnMetadata m[] = new ColumnMetadata[metadata.size()];

        for (int i = 0, sz = metadata.size(); i < sz; i++) {
            ColumnMetadata meta = metadata.get(i);
            if (meta.indexed && meta.distinctCountHint <= 1) {
                meta.distinctCountHint = Math.max(2, (int) (recordCountHint * 0.01));
            }

            if (meta.size == 0 && meta.avgSize == 0) {
                throw new JournalConfigurationException("Invalid size for column %s.%s", location, meta.name);
            }

            // distinctCount
            if (meta.distinctCountHint <= 0) {
                meta.distinctCountHint = (int) (recordCountHint * 0.2); //20%
            }

            switch (meta.type) {
                case STRING:
                    meta.size = meta.avgSize + 4;
                    meta.bitHint = ByteBuffers.getBitHint(meta.avgSize * 2, recordCountHint);
                    meta.indexBitHint = ByteBuffers.getBitHint(8, recordCountHint);
                    break;
                case BINARY:
                    meta.size = meta.avgSize;
                    meta.bitHint = ByteBuffers.getBitHint(meta.avgSize, recordCountHint);
                    meta.indexBitHint = ByteBuffers.getBitHint(8, recordCountHint);
                    break;
                default:
                    meta.bitHint = ByteBuffers.getBitHint(meta.size, recordCountHint);
            }

            m[i] = meta;
        }

        return new JournalMetadataImpl(
                location
                , null
                , null
                , key
                , location
                , partitionBy
                , m
                , tsColumnIndex
                , openFileTTL
                , recordCountHint
                , txCountHint
                , lag
                , null
        );
    }

    private ColumnMetadata newMeta(String name) {
        int index = nameToIndexMap.get(name);
        if (index == -1) {
            ColumnMetadata meta = new ColumnMetadata();
            meta.name = name;
            metadata.add(meta);
            nameToIndexMap.put(name, metadata.size() - 1);
            return meta;
        } else {
            throw new JournalConfigurationException("Duplicate column: " + name);
        }
    }

    @Override
    public JMetadataBuilder location(String absolutePath) {
        return this;
    }
}
