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

package com.nfsdb.journal.factory;

import com.nfsdb.journal.PartitionType;
import com.nfsdb.journal.column.ColumnType;
import com.nfsdb.journal.exceptions.JournalConfigurationException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.utils.ByteBuffers;
import com.nfsdb.journal.utils.Checksum;
import com.nfsdb.journal.utils.Unsafe;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

public class JournalMetadata<T> {
    public static final int BYTE_LIMIT = 0xff;
    public static final int TWO_BYTE_LIMIT = 0xffff;
    private static final int INVALID_INDEX = -1;
    private int timestampColumnIndex = INVALID_INDEX;
    private final TObjectIntMap<String> columnMetadataMap;
    private final List<ColumnMetadata> columnMetadataList;
    private final Class modelClass;
    private final String timestampColumn;
    private final String key;
    private final int openPartitionTTL;
    private final NullsAdaptor<T> nullsAdaptor;
    private String location;
    private PartitionType partitionType;
    private int recordHint;
    private Constructor constructor;
    private ColumnMetadata timestampColumnMetadata;
    private int lagHours = 0;
    private int columnCount = -1;

    public JournalMetadata(Class<T> modelClass
            , String location
            , String timestampColumn
            , PartitionType partitionType
            , int recordHint
            , int openPartitionTTL
            , int lagHours
            , String key
            , NullsAdaptor<T> nullsAdaptor) throws JournalConfigurationException {
        this.modelClass = modelClass;
        this.location = location;
        this.timestampColumn = timestampColumn;
        this.partitionType = partitionType;
        this.recordHint = recordHint;
        this.openPartitionTTL = openPartitionTTL;
        this.lagHours = lagHours;
        this.key = key;
        this.columnMetadataMap = new TObjectIntHashMap<>(10, 0.5f, INVALID_INDEX);
        this.columnMetadataList = new ArrayList<>();
        this.nullsAdaptor = nullsAdaptor;
        parseClass();
    }

    public JournalMetadata(JournalMetadata<T> that) {
        this.modelClass = that.modelClass;
        this.location = that.location;
        this.partitionType = that.partitionType;
        this.recordHint = that.recordHint;
        this.columnMetadataMap = that.columnMetadataMap;
        this.columnMetadataList = that.columnMetadataList;
        this.constructor = that.constructor;
        this.timestampColumn = that.timestampColumn;
        this.timestampColumnIndex = that.timestampColumnIndex;
        this.openPartitionTTL = that.openPartitionTTL;
        this.lagHours = that.lagHours;
        this.key = that.key;
        this.timestampColumnMetadata = that.timestampColumnMetadata;
        this.nullsAdaptor = that.nullsAdaptor;
    }

    public NullsAdaptor<T> getNullsAdaptor() {
        return nullsAdaptor;
    }

    public ColumnMetadata getColumnMetadata(String name) {
        return columnMetadataList.get(getColumnIndex(name));
    }

    public ColumnMetadata getColumnMetadata(int columnIndex) {
        return columnMetadataList.get(columnIndex);
    }

    public int getColumnIndex(String columnName) {
        int result = columnMetadataMap.get(columnName);
        if (result == INVALID_INDEX) {
            throw new JournalRuntimeException("Invalid column name: %s", columnName);
        }
        return result;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public PartitionType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(PartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public int getColumnCount() {
        if (columnCount == -1) {
            columnCount = columnMetadataList.size();
        }
        return columnCount;
    }

    public ColumnMetadata getTimestampColumnMetadata() {
        return timestampColumnMetadata;
    }

    public int getTimestampColumnIndex() {
        if (timestampColumnIndex == INVALID_INDEX) {
            if (timestampColumnMetadata == null) {
                throw new JournalRuntimeException("There is no timestamp column in %s", this);
            }
            timestampColumnIndex = getColumnIndex(timestampColumnMetadata.name);
        }
        return timestampColumnIndex;
    }

    public Object newObject() throws JournalRuntimeException {
        try {
            return constructor.newInstance();
        } catch (Exception e) {
            throw new JournalRuntimeException("Could not create instance of class: " + modelClass.getName(), e);
        }
    }

    public Class getModelClass() {
        return modelClass;
    }

    public File getColumnIndexBase(File partitionDir, int columnIndex) {
        ColumnMetadata meta = getColumnMetadata(columnIndex);
        if (!meta.indexed) {
            throw new JournalRuntimeException("There is no index for column: %s", meta.name);
        }
        return new File(partitionDir, meta.name);
    }

    @Override
    public String toString() {
        return "JournalMetadata{" +
                "SHA='" + DatatypeConverter.printBase64Binary(Checksum.getChecksum(this)) + '\'' +
                ", columnMetadataList*=" + columnMetadataList +
                ", modelClass*=" + modelClass +
                ", timestampColumn='" + timestampColumn + '\'' +
                ", key='" + key + '\'' +
                ", openPartitionTTL=" + openPartitionTTL +
                ", nullsAdaptor=" + nullsAdaptor +
                ", location='" + location + '\'' +
                ", partitionType*=" + partitionType +
                ", recordHint=" + recordHint +
                ", lagHours=" + lagHours +
                '}';
    }

    public int getOpenPartitionTTL() {
        return openPartitionTTL;
    }

    public int getLagHours() {
        return lagHours;
    }

    public int getRecordHint() {
        return recordHint;
    }

    public void setRecordHint(int recordHint) throws JournalConfigurationException {
        this.recordHint = recordHint;
        adjustHintsForComplexTypes();
    }

    public String getKey() {
        return key;
    }

    void updateVariableSizes() throws JournalConfigurationException {

        for (int i = 0, columnMetadataListSize = columnMetadataList.size(); i < columnMetadataListSize; i++) {
            ColumnMetadata m = columnMetadataList.get(i);
            switch (m.type) {
                case STRING:
                    if (m.maxSize <= BYTE_LIMIT) {
                        m.size = m.maxSize + 1;
                    } else if (m.size <= TWO_BYTE_LIMIT) {
                        m.size = m.maxSize + 2;
                    } else {
                        m.size = m.maxSize + 4;
                    }
                    m.size += 1;
                    if (m.avgSize == ColumnMetadata.AUTO_AVG_SIZE) {
                        m.avgSize = m.maxSize;
                    }
                    break;
                case SYMBOL:
                    m.size = 4;
                    break;
                default:
            }
        }

        adjustHintsForComplexTypes();
    }

    @SuppressWarnings("unchecked")
    private void parseClass() throws JournalConfigurationException {
        try {
            this.constructor = modelClass.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new JournalConfigurationException("No default constructor declared on %s", modelClass.getName());
        }

        Field[] classFields = modelClass.getDeclaredFields();
        for (Field f : classFields) {

            if (Modifier.isStatic(f.getModifiers()) || "__isset_bitfield".equals(f.getName())) {
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

            if (meta.name.equals(timestampColumn)) {
                this.timestampColumnMetadata = meta;
            }

            columnMetadataMap.put(meta.name, columnMetadataList.size());
            columnMetadataList.add(meta);
        }

        if (timestampColumn != null && !columnMetadataMap.containsKey(timestampColumn)) {
            throw new JournalConfigurationException("Invalid timestampColumn value on journal: %s", this);
        }

        if (partitionType != PartitionType.NONE && timestampColumn == null) {
            throw new JournalConfigurationException(
                    "Either use partitionType=NONE or specify 'timestampColumn' on journal: %s", this);
        }
    }

    private void adjustHintsForComplexTypes() throws JournalConfigurationException {
        for (int i = 0, columnMetadataListSize = columnMetadataList.size(); i < columnMetadataListSize; i++) {
            ColumnMetadata meta = columnMetadataList.get(i);
            if (meta.size == 0 || meta.avgSize == 0) {
                throw new JournalConfigurationException("Invalid size for column %s.%s", this.getModelClass().getName(), meta.name);
            }
            switch (meta.type) {
                case STRING:
                    meta.bitHint = ByteBuffers.getBitHint(meta.avgSize * 2, recordHint);
                    meta.indexBitHint = ByteBuffers.getBitHint(8, recordHint);
                    break;
                case BINARY:
                    meta.bitHint = ByteBuffers.getBitHint(meta.avgSize, recordHint);
                    meta.indexBitHint = ByteBuffers.getBitHint(8, recordHint);
                    break;
                default:
                    meta.bitHint = ByteBuffers.getBitHint(meta.size, recordHint);
            }
        }
    }

    public static class ColumnMetadata {
        public static final int AUTO_AVG_SIZE = -1;
        public String name;
        public ColumnType type;
        public long offset;
        public int size;
        public int avgSize = AUTO_AVG_SIZE;
        public boolean indexed;
        public int bitHint;
        public int indexBitHint;
        public int distinctCountHint;
        public String sameAs;

        @Override
        public String toString() {
            return "ColumnMetadata{" +
                    "name*='" + name + '\'' +
                    ", type*=" + type +
                    ", offset=" + offset +
                    ", size*=" + size +
                    ", avgSize=" + avgSize +
                    ", indexed*=" + indexed +
                    ", bitHint=" + bitHint +
                    ", indexBitHint=" + indexBitHint +
                    ", distinctCountHint*=" + distinctCountHint +
                    ", sameAs*='" + sameAs + '\'' +
                    ", maxSize=" + maxSize +
                    '}';
        }

        public int maxSize = JournalMetadata.BYTE_LIMIT;


    }
}
