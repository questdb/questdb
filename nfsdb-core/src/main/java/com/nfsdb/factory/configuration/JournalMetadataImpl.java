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

package com.nfsdb.factory.configuration;

import com.nfsdb.JournalKey;
import com.nfsdb.PartitionType;
import com.nfsdb.collections.ObjIntHashMap;
import com.nfsdb.exceptions.JournalConfigurationException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.storage.HugeBuffer;
import com.nfsdb.utils.Base64;
import com.nfsdb.utils.Checksum;
import org.jetbrains.annotations.NotNull;

import java.lang.StringBuilder;
import java.lang.reflect.Constructor;
import java.util.Arrays;

public class JournalMetadataImpl<T> implements JournalMetadata<T> {

    private static final int TO_STRING_COL1_PAD = 20;
    private static final int TO_STRING_COL2_PAD = 55;
    private final String id;
    private final Class<T> modelClass;
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
    private final ObjIntHashMap<CharSequence> columnIndexLookup;
    private final int timestampColumnIndex;
    private final int lag;
    private final boolean partialMapping;

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
            , boolean partialMapping
    ) {
        this.id = id;
        this.modelClass = modelClass;
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
        this.columnIndexLookup = new ObjIntHashMap<>(columnCount);
        for (int i = 0; i < columnMetadata.length; i++) {
            columnIndexLookup.put(columnMetadata[i].name, i);
        }
        this.lag = lag;
        this.partialMapping = partialMapping;
    }

    public JournalMetadataImpl(HugeBuffer buf) {
        buf.setPos(0);
        id = buf.getStr();
        modelClass = null;
        location = buf.getStr();
        partitionBy = PartitionType.valueOf(buf.getStr());
        columnCount = buf.getInt();
        columnMetadata = new ColumnMetadata[columnCount];
        columnIndexLookup = new ObjIntHashMap<>();
        for (int i = 0; i < columnCount; i++) {
            columnMetadata[i] = new ColumnMetadata();
            columnMetadata[i].read(buf);
            columnIndexLookup.put(columnMetadata[i].name, i);
        }
        timestampColumnIndex = buf.getInt();
        if (timestampColumnIndex >= 0) {
            timestampMetadata = columnMetadata[timestampColumnIndex];
        } else {
            timestampMetadata = null;
        }
        openFileTTL = buf.getLong();
        ioBlockRecordCount = buf.getInt();
        ioBlockTxCount = buf.getInt();
        key = buf.getStr();
        lag = buf.getInt();
        constructor = null;
        partialMapping = false;
    }

    @Override
    public void copyColumnMetadata(ColumnMetadata[] meta) {

        if (meta.length != columnCount) {
            throw new JournalRuntimeException("Invalid length for column metadata array");
        }
        System.arraycopy(columnMetadata, 0, meta, 0, meta.length);
    }

    @Override
    public JournalKey<T> deriveKey() {
        if (modelClass != null) {
            return new JournalKey<>(modelClass, location);
        } else {
            return new JournalKey<>(location);
        }
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getColumnIndex(CharSequence columnName) {
        int result = columnIndexLookup.get(columnName);
        if (result == -1) {
            throw new JournalRuntimeException("Invalid column name: %s", columnName);
        }
        return result;
    }

    @Override
    @NotNull
    public ColumnMetadata getColumnMetadata(CharSequence name) {
        return getColumnMetadata(getColumnIndex(name));
    }

    @Override
    public ColumnMetadata getColumnMetadata(int columnIndex) {
        return columnMetadata[columnIndex];
    }

    public String getId() {
        return id;
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
    public int getLag() {
        return this.lag;
    }

    @Override
    public String getLocation() {
        return location;
    }

    @Override
    public Class<T> getModelClass() {
        return modelClass;
    }

    @Override
    public long getOpenFileTTL() {
        return openFileTTL;
    }

    @Override
    public PartitionType getPartitionType() {
        return partitionBy;
    }

    @Override
    public int getRecordHint() {
        return ioBlockRecordCount;
    }

    @Override
    public int getTimestampIndex() {
        return timestampColumnIndex;
    }

    @Override
    public ColumnMetadata getTimestampMetadata() {
        return timestampMetadata;
    }

    @Override
    public int getTxCountHint() {
        return ioBlockTxCount;
    }

    @Override
    public boolean isPartialMapped() {
        return partialMapping;
    }

    @Override
    public Object newObject() throws JournalRuntimeException {
        try {
            return constructor.newInstance();
        } catch (Exception e) {
            throw new JournalRuntimeException("Could not create instance of class: " + modelClass.getName(), e);
        }
    }

    public void write(HugeBuffer buf) {
        buf.setPos(0);
        buf.put(id);
        buf.put(location);
        buf.put(partitionBy.name());
        buf.put(columnCount);
        for (int i = 0; i < columnMetadata.length; i++) {
            columnMetadata[i].write(buf);
        }
        buf.put(timestampColumnIndex);
        buf.put(openFileTTL);
        buf.put(ioBlockRecordCount);
        buf.put(ioBlockTxCount);
        buf.put(key);
        buf.put(lag);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + (location != null ? location.hashCode() : 0);
        result = 31 * result + partitionBy.hashCode();
        result = 31 * result + columnCount;
        result = 31 * result + (timestampMetadata != null ? timestampMetadata.hashCode() : 0);
        result = 31 * result + (int) (openFileTTL ^ (openFileTTL >>> 32));
        result = 31 * result + ioBlockRecordCount;
        result = 31 * result + ioBlockTxCount;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(columnMetadata);
        result = 31 * result + timestampColumnIndex;
        result = 31 * result + lag;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JournalMetadataImpl that = (JournalMetadataImpl) o;

        return columnCount == that.columnCount
                && ioBlockRecordCount == that.ioBlockRecordCount
                && ioBlockTxCount == that.ioBlockTxCount
                && lag == that.lag
                && openFileTTL == that.openFileTTL
                && timestampColumnIndex == that.timestampColumnIndex
                && Arrays.equals(columnMetadata, that.columnMetadata)
                && id.equals(that.id)
                && !(key != null ? !key.equals(that.key) : that.key != null)
                && !(location != null ? !location.equals(that.location) : that.location != null)
                && partitionBy == that.partitionBy
                && !(timestampMetadata != null ? !timestampMetadata.equals(that.timestampMetadata) : that.timestampMetadata != null);

    }

    @Override
    public String toString() {

        StringBuilder b = new StringBuilder();
        sep(b);
        b.append("|");
        pad(b, TO_STRING_COL1_PAD, "Location:");
        pad(b, TO_STRING_COL2_PAD, location).append('\n');


        b.append("|");
        pad(b, TO_STRING_COL1_PAD, "SHA:");
        pad(b, TO_STRING_COL2_PAD, Base64._printBase64Binary(Checksum.getChecksum(this))).append('\n');

        b.append("|");
        pad(b, TO_STRING_COL1_PAD, "Partition by");
        pad(b, TO_STRING_COL2_PAD, partitionBy.name()).append('\n');
        sep(b);


        for (int i = 0; i < columnCount; i++) {
            b.append("|");
            pad(b, TO_STRING_COL1_PAD, Integer.toString(i));
            col(b, columnMetadata[i]);
            b.append('\n');
        }

        sep(b);

        return b.toString();
    }

    private void col(StringBuilder b, ColumnMetadata m) {
        pad(b, TO_STRING_COL2_PAD, (m.distinctCountHint > 0 ? m.distinctCountHint + " ~ " : "") + (m.indexed ? "#" : "") + m.name + (m.sameAs != null ? " -> " + m.sameAs : "") + " " + m.type.name() + "(" + m.size + ")");
    }

    private StringBuilder pad(StringBuilder b, int w, String value) {
        int pad = value == null ? w : w - value.length();
        for (int i = 0; i < pad; i++) {
            b.append(' ');
        }

        if (value != null) {
            if (pad < 0) {
                b.append("...").append(value.substring(-pad + 3));
            } else {
                b.append(value);
            }
        }

        b.append("  |");

        return b;
    }

    private void sep(StringBuilder b) {
        b.append("+");
        for (int i = 0; i < TO_STRING_COL1_PAD + TO_STRING_COL2_PAD + 5; i++) {
            b.append("-");
        }
        b.append("+\n");
    }

}
