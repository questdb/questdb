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
import com.nfsdb.journal.exceptions.JournalConfigurationException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.factory.NullsAdaptor;
import com.nfsdb.journal.factory.NullsAdaptorFactory;
import com.nfsdb.journal.utils.Base64;
import com.nfsdb.journal.utils.Checksum;
import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Arrays;

public class JournalMetadataImpl<T> implements JournalMetadata<T> {

    private final String id;
    private final Class<T> modelClass;
    private final NullsAdaptor<T> nullsAdaptor;
    private final NullsAdaptorFactory<T> nullsAdaptorFactory;
    private final String location;
    private final PartitionType partitionBy;
    private final int columnCount;
    private final ColumnMetadata timestampMetadata;
    private final Constructor<T> constructor;
    private final long openFileTTL;
    private final int ioBlockRecordCount;
    private final int ioBlockTxCount;
    private final String key;
    private final ColumnMetadata[] columnMetadata;
    private final TObjectIntMap<String> columnIndexLookup;
    private final int timestampColumnIndex;
    private final int lag;

    public JournalMetadataImpl(
            String id
            , Class<T> modelClass
            , Constructor<T> constructor
            , String key
            , String location
            , PartitionType partitionBy
            , ColumnMetadata[] columnMetadata
            , int timestampColumnIndex
            , long openFileTTL
            , int ioBlockRecordCount
            , int ioBlockTxCount
            , int lag
            , NullsAdaptorFactory<T> nullsAdaptorFactory
    ) {
        this.id = id;
        this.modelClass = modelClass;
        this.nullsAdaptorFactory = nullsAdaptorFactory;
        this.nullsAdaptor = nullsAdaptorFactory != null ? nullsAdaptorFactory.getInstance(modelClass) : null;
        this.location = location;
        this.partitionBy = partitionBy;
        this.columnMetadata = new ColumnMetadata[columnMetadata.length];
        System.arraycopy(columnMetadata, 0, this.columnMetadata, 0, columnMetadata.length);
        this.columnCount = columnMetadata.length;
        this.timestampMetadata = timestampColumnIndex >= 0 ? columnMetadata[timestampColumnIndex] : null;
        this.timestampColumnIndex = timestampColumnIndex;
        this.constructor = constructor;
        this.openFileTTL = openFileTTL;
        this.ioBlockRecordCount = ioBlockRecordCount;
        this.ioBlockTxCount = ioBlockTxCount;
        this.key = key;
        this.columnIndexLookup = new TObjectIntHashMap<>(columnCount, Constants.DEFAULT_LOAD_FACTOR, -1);
        for (int i = 0; i < columnMetadata.length; i++) {
            columnIndexLookup.put(columnMetadata[i].name, i);
        }
        this.lag = lag;
    }

    @Override
    public NullsAdaptor<T> getNullsAdaptor() {
        return nullsAdaptor;
    }

    @Override
    public ColumnMetadata getColumnMetadata(String name) {
        return getColumnMetadata(getColumnIndex(name));
    }

    @Override
    public ColumnMetadata getColumnMetadata(int columnIndex) {
        return columnMetadata[columnIndex];
    }

    @Override
    public int getColumnIndex(String columnName) {
        int result = columnIndexLookup.get(columnName);
        if (result == -1) {
            throw new JournalRuntimeException("Invalid column name: %s", columnName);
        }
        return result;
    }

    @Override
    public String getLocation() {
        return location;
    }

    @Override
    public PartitionType getPartitionType() {
        return partitionBy;
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public ColumnMetadata getTimestampColumnMetadata() {
        return timestampMetadata;
    }

    @Override
    public int getTimestampColumnIndex() {
        return timestampColumnIndex;
    }

    @Override
    public Object newObject() throws JournalRuntimeException {
        try {
            return constructor.newInstance();
        } catch (Exception e) {
            throw new JournalRuntimeException("Could not create instance of class: " + modelClass.getName(), e);
        }
    }

    @Override
    public Class<T> getModelClass() {
        return modelClass;
    }

    @Override
    public File getColumnIndexBase(File partitionDir, int columnIndex) {
        ColumnMetadata meta = getColumnMetadata(columnIndex);
        if (!meta.indexed) {
            throw new JournalRuntimeException("There is no index for column: %s", meta.name);
        }
        return new File(partitionDir, meta.name);
    }

    @Override
    public long getOpenFileTTL() {
        return openFileTTL;
    }

    @Override
    public int getLag() {
        return this.lag;
    }

    @Override
    public int getRecordHint() {
        return ioBlockRecordCount;
    }

    @Override
    public int getTxCountHint() {
        return ioBlockTxCount;
    }

    @Override
    public String getKey() {
        if (key == null) {
            throw new JournalConfigurationException(modelClass.getName() + " does not have a key");
        }
        return key;
    }

    @Override
    public String getKeyQuiet() {
        return key;
    }

    @Override
    public NullsAdaptorFactory<T> getNullsAdaptorFactory() {
        return nullsAdaptorFactory;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "JournalMetaImpl{" +
                "SHA='" + Base64._printBase64Binary(Checksum.getChecksum(this)) + '\'' +
                ", id=" + id +
                ", modelClass=" + modelClass +
                ", nullsAdaptor=" + nullsAdaptor +
                ", nullsAdaptorFactory=" + nullsAdaptorFactory +
                ", location='" + location + '\'' +
                ", partitionBy=" + partitionBy +
                ", columnCount=" + columnCount +
                ", timestampMetadata=" + timestampMetadata +
                ", constructor=" + constructor +
                ", openFileTTL=" + openFileTTL +
                ", ioBlockRecordCount=" + ioBlockRecordCount +
                ", ioBlockTxCount=" + ioBlockTxCount +
                ", key='" + key + '\'' +
                ", columnMetadata=" + Arrays.toString(columnMetadata) +
                ", columnIndexLookup=" + columnIndexLookup +
                ", timestampColumnIndex=" + timestampColumnIndex +
                ", lag=" + lag +
                '}';
    }
}
