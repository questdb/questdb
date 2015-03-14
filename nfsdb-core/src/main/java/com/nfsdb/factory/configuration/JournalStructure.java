/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.factory.configuration;

import com.nfsdb.PartitionType;
import com.nfsdb.collections.ObjIntHashMap;
import com.nfsdb.exceptions.JournalConfigurationException;
import com.nfsdb.logging.Logger;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.ByteBuffers;
import com.nfsdb.utils.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class JournalStructure implements MetadataBuilder<Object> {
    private static final Logger LOGGER = Logger.getLogger(JournalStructure.class);
    private final List<ColumnMetadata> metadata = new ArrayList<>();
    private final ObjIntHashMap<String> nameToIndexMap = new ObjIntHashMap<>();
    private String location;
    private int tsColumnIndex = -1;
    private PartitionType partitionBy = PartitionType.NONE;
    private int recordCountHint = 100000;
    private int txCountHint = -1;
    private String key;
    private long openFileTTL = TimeUnit.MINUTES.toMillis(3);
    private int lag = -1;
    private Class<Object> modelClass;
    private Constructor<Object> constructor;
    private boolean partialMapping = false;

    public JournalStructure(String location) {
        this.location = location;
    }

    public JournalStructure(String location, ColumnMetadata columnMetadata[]) {
        this.location = location;
        for (int i = 0, sz = columnMetadata.length; i < sz; i++) {
            ColumnMetadata to = new ColumnMetadata();
            metadata.add(to.copy(columnMetadata[i]));
            nameToIndexMap.put(to.name, i);
        }
    }

    public JournalStructure(JournalMetadata model) {
        this.location = model.getLocation();
        this.tsColumnIndex = model.getTimestampIndex();
        this.partitionBy = model.getPartitionType();
        this.recordCountHint = model.getRecordHint();
        this.txCountHint = model.getTxCountHint();
        this.key = model.getKeyQuiet();
        this.openFileTTL = model.getOpenFileTTL();
        this.lag = model.getLag();
        for (int i = 0; i < model.getColumnCount(); i++) {
            ColumnMetadata to = new ColumnMetadata();
            metadata.add(to.copy(model.getColumnMetadata(i)));
            nameToIndexMap.put(to.name, i);
        }
    }

    public GenericBinaryBuilder $bin(String name) {
        return new GenericBinaryBuilder(this, newMeta(name));
    }

    public JournalStructure $bool(String name) {
        return $meta(name, ColumnType.BOOLEAN);
    }

    public JournalStructure $date(String name) {
        return $meta(name, ColumnType.DATE);
    }

    public JournalStructure $double(String name) {
        return $meta(name, ColumnType.DOUBLE);
    }

    public JournalStructure $float(String name) {
        return $meta(name, ColumnType.FLOAT);
    }

    public GenericIntBuilder $int(String name) {
        return new GenericIntBuilder(this, newMeta(name));
    }

    public JournalStructure $long(String name) {
        return $meta(name, ColumnType.LONG);
    }

    public JournalStructure $short(String name) {
        return $meta(name, ColumnType.SHORT);
    }

    public GenericStringBuilder $str(String name) {
        return new GenericStringBuilder(this, newMeta(name));
    }

    public GenericSymbolBuilder $sym(String name) {
        return new GenericSymbolBuilder(this, newMeta(name));
    }

    public JournalStructure $ts() {
        return $ts("timestamp");
    }

    public JournalStructure $ts(String name) {
        $meta(name, ColumnType.DATE);
        tsColumnIndex = nameToIndexMap.get(name);
        return this;
    }

    public JournalMetadata<Object> build() {

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
            if (meta.distinctCountHint <= 0 && meta.type == ColumnType.SYMBOL) {
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

        return new JournalMetadata<>(
                location
                , modelClass
                , constructor
                , key
                , location
                , partitionBy
                , m
                , tsColumnIndex
                , openFileTTL
                , recordCountHint
                , txCountHint
                , lag
                , partialMapping
        );
    }

    public String getLocation() {
        return location;
    }

    public JournalStructure key(String key) {
        this.key = key;
        return this;
    }

    public JournalStructure lag(long time, TimeUnit unit) {
        this.lag = (int) unit.toHours(time);
        return this;
    }

    @Override
    public JournalStructure location(String location) {
        this.location = location;
        return this;
    }

    @Override
    public JournalStructure location(File path) {
        this.location = path.getAbsolutePath();
        return this;
    }

    @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS", "LEST_LOST_EXCEPTION_STACK_TRACE"})
    @SuppressWarnings("unchecked")
    public JournalMetadata map(Class clazz) {
        List<Field> classFields = getAllFields(new ArrayList<Field>(), clazz);

        for (int i = 0; i < classFields.size(); i++) {
            Field f = classFields.get(i);

            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }

            int index = nameToIndexMap.get(f.getName());
            if (index == -1) {
                LOGGER.warn("Unusable member field: " + clazz.getName() + "." + f.getName());
                continue;
            }

            Class type = f.getType();
            ColumnMetadata meta = metadata.get(index);

            checkTypes(meta.type, toColumnType(type));

            meta.offset = Unsafe.getUnsafe().objectFieldOffset(f);
        }

        this.partialMapping = missingMappings();

        this.modelClass = clazz;
        try {
            this.constructor = modelClass.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new JournalConfigurationException("No default constructor declared on %s", modelClass.getName());
        }

        return this.build();
    }

    public JournalStructure openFileTTL(long time, TimeUnit unit) {
        this.openFileTTL = unit.toMillis(time);
        return this;
    }

    public JournalStructure partitionBy(PartitionType type) {
        if (type != PartitionType.DEFAULT) {
            this.partitionBy = type;
        }
        return this;
    }

    public JournalStructure recordCountHint(int count) {
        if (count > 0) {
            this.recordCountHint = count;
        }
        return this;
    }

    public JournalStructure txCountHint(int count) {
        this.txCountHint = count;
        return this;
    }

    private JournalStructure $meta(String name, ColumnType type) {
        ColumnMetadata m = newMeta(name);
        m.type = type;
        m.size = type.size();
        return this;
    }

    private void checkTypes(ColumnType expected, ColumnType actual) {
        if (expected == actual) {
            return;
        }

        if (expected == ColumnType.DATE && actual == ColumnType.LONG) {
            return;
        }

        if (expected == ColumnType.SYMBOL && actual == ColumnType.STRING) {
            return;
        }

        throw new JournalConfigurationException("Type mismatch: expected=" + expected + ", actual=" + actual);
    }

    private List<Field> getAllFields(List<Field> fields, Class<?> type) {
        Collections.addAll(fields, type.getDeclaredFields());
        if (type.getSuperclass() != null) {
            fields = getAllFields(fields, type.getSuperclass());
        }
        return fields;
    }

    private boolean missingMappings() {
        boolean mappingMissing = false;
        for (int i = 0, metadataSize = metadata.size(); i < metadataSize; i++) {
            ColumnMetadata m = metadata.get(i);
            if (m.offset == 0) {
                mappingMissing = true;
                LOGGER.warn("Unmapped data column: %s", m.name);
            }
        }

        return mappingMissing;
    }

    private ColumnMetadata newMeta(String name) {
        int index = nameToIndexMap.get(name);
        if (index == -1) {
            ColumnMetadata meta = new ColumnMetadata().setName(name);
            metadata.add(meta);
            nameToIndexMap.put(name, metadata.size() - 1);
            return meta;
        } else {
            throw new JournalConfigurationException("Duplicate column: " + name);
        }
    }

    private ColumnType toColumnType(Class type) {
        for (ColumnType t : ColumnType.values()) {
            if (t.matches(type)) {
                return t;
            }
        }
        return null;
    }
}
