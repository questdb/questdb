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
import com.nfsdb.journal.factory.NullsAdaptorFactory;
import com.nfsdb.journal.utils.ByteBuffers;
import com.nfsdb.journal.utils.Unsafe;
import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class JournalMetadataBuilder<T> {
    private final Map<String, ColumnMetadata> columnMetadata = new HashMap<>();
    private final Class<T> modelClass;
    private Constructor<T> constructor;
    private TObjectIntMap<String> nameToIndexMap;
    private String location;
    private int tsColumnIndex = -1;
    private PartitionType partitionBy = PartitionType.NONE;
    private int recordCountHint = 100000;
    private int txCountHint = -1;
    private String key;
    private long openFileTTL = TimeUnit.MINUTES.toMillis(3);
    private int lag = -1;
    private NullsAdaptorFactory<T> nullsFactory;

    public JournalMetadataBuilder(Class<T> modelClass) {
        this.modelClass = modelClass;
        parseClass();
    }

    public JournalMetadataBuilder(JournalMetadata<T> model) {
        this.modelClass = model.getModelClass();
        parseClass();
        this.location = model.getLocation();
        this.tsColumnIndex = model.getTimestampColumnIndex();
        this.partitionBy = model.getPartitionType();
        this.recordCountHint = model.getRecordHint();
        this.txCountHint = model.getTxCountHint();
        this.key = model.getKeyQuiet();
        this.openFileTTL = model.getOpenFileTTL();
        this.lag = model.getLag();
        this.nullsFactory = model.getNullsAdaptorFactory();

        for (int i = 0; i < model.getColumnCount(); i++) {
            ColumnMetadata from = model.getColumnMetadata(i);
            ColumnMetadata to = columnMetadata.get(from.name);
            to.copy(from);
        }
    }

    public SymbolBuilder<T> $sym(String name) {
        return new SymbolBuilder<>(this, getMeta(name));
    }

    public StringBuilder<T> $str(String name) {
        return new StringBuilder<>(this, getMeta(name));
    }

    public BinaryBuilder<T> $bin(String name) {
        return new BinaryBuilder<>(this, getMeta(name));
    }

    public IntBuilder<T> $int(String name) {
        return new IntBuilder<>(this, getMeta(name));
    }

    public JournalMetadataBuilder<T> $ts() {
        return $ts("timestamp");
    }

    public JournalMetadataBuilder<T> $ts(String name) {
        tsColumnIndex = nameToIndexMap.get(name);
        if (tsColumnIndex == -1) {
            throw new JournalConfigurationException("Invalid column name: %s", name);
        }
        return this;
    }

    public JournalMetadataBuilder<T> location(String location) {
        this.location = location;
        return this;
    }

    public JournalMetadataBuilder<T> partitionBy(PartitionType type) {
        this.partitionBy = type;
        return this;
    }

    public JournalMetadataBuilder<T> recordCountHint(int count) {
        this.recordCountHint = count;
        return this;
    }

    public JournalMetadataBuilder<T> txCountHint(int count) {
        this.txCountHint = count;
        return this;
    }

    public JournalMetadataBuilder<T> key(String key) {
        this.key = key;
        return this;
    }

    public JournalMetadataBuilder<T> openFileTTL(long time, TimeUnit unit) {
        this.openFileTTL = unit.toMillis(time);
        return this;
    }

    public JournalMetadataBuilder<T> lag(long time, TimeUnit unit) {
        this.lag = (int) unit.toHours(time);
        return this;
    }

    public JournalMetadataBuilder<T> nullsFactory(NullsAdaptorFactory<T> factory) {
        this.nullsFactory = factory;
        return this;
    }

    public String getLocation() {
        return location;
    }

    public JournalMetadata<T> build() {

        // default tx count hint
        if (txCountHint == -1) {
            txCountHint = (int) (recordCountHint * 0.1);
        }


        ColumnMetadata metadata[] = new ColumnMetadata[nameToIndexMap.size()];

        for (String name : columnMetadata.keySet()) {
            int index = nameToIndexMap.get(name);
            ColumnMetadata meta = columnMetadata.get(name);


            if (meta.indexed && meta.distinctCountHint <= 1) {
                meta.distinctCountHint = Math.max(2, (int) (recordCountHint * 0.01));
            }

            if (meta.size == 0 && meta.avgSize == 0) {
                throw new JournalConfigurationException("Invalid size for column %s.%s", modelClass.getName(), meta.name);
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

            metadata[index] = meta;
        }

        return new JournalMetadataImpl<>(
                modelClass
                , constructor
                , key
                , location
                , partitionBy
                , metadata
                , tsColumnIndex
                , openFileTTL
                , recordCountHint
                , txCountHint
                , lag
                , nullsFactory
        );
    }

    private ColumnMetadata getMeta(String name) {
        ColumnMetadata meta = columnMetadata.get(name);
        if (meta == null) {
            throw new JournalConfigurationException("No such column: %s", name);
        }
        return meta;
    }

    private void parseClass() throws JournalConfigurationException {
        try {
            this.constructor = modelClass.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new JournalConfigurationException("No default constructor declared on %s", modelClass.getName());
        }

        List<Field> classFields = getAllFields(new ArrayList<Field>(), modelClass);

        this.nameToIndexMap = new TObjectIntHashMap<>(classFields.size(), Constants.DEFAULT_LOAD_FACTOR, -1);
        this.location = modelClass.getCanonicalName();

        for (int i = 0; i < classFields.size(); i++) {
            Field f = classFields.get(i);

            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            ColumnMetadata meta = new ColumnMetadata();
            Class type = f.getType();
            for (ColumnType t : ColumnType.values()) {
                if (t.matches(type)) {
                    meta.type = t;
                    meta.size = t.size();
                    break;
                }
            }

            if (meta.type == null) {
                continue;
            }

            meta.offset = Unsafe.getUnsafe().objectFieldOffset(f);
            meta.name = f.getName();
            columnMetadata.put(meta.name, meta);
            nameToIndexMap.put(meta.name, nameToIndexMap.size());
        }
    }

    private List<Field> getAllFields(List<Field> fields, Class<?> type) {
        Collections.addAll(fields, type.getDeclaredFields());
        if (type.getSuperclass() != null) {
            fields = getAllFields(fields, type.getSuperclass());
        }
        return fields;
    }
}
