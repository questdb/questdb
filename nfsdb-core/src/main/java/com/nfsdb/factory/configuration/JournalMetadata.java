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

import com.nfsdb.JournalKey;
import com.nfsdb.PartitionType;
import com.nfsdb.collections.ObjIntHashMap;
import com.nfsdb.exceptions.JournalConfigurationException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.exceptions.NoSuchColumnException;
import com.nfsdb.lang.cst.RecordMetadata;
import com.nfsdb.storage.HugeBuffer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.NotNull;

import java.lang.StringBuilder;
import java.lang.reflect.Constructor;

public class JournalMetadata<T> implements RecordMetadata {

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

    public JournalMetadata(
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

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public JournalMetadata(HugeBuffer buf) {
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

    public void copyColumnMetadata(ColumnMetadata[] meta) {

        if (meta.length != columnCount) {
            throw new JournalRuntimeException("Invalid length for column metadata array");
        }
        System.arraycopy(columnMetadata, 0, meta, 0, meta.length);
    }

    public JournalKey<T> deriveKey(String location) {
        if (modelClass != null) {
            return new JournalKey<>(modelClass, location);
        } else {
            return new JournalKey<>(location);
        }
    }

    @Override
    @NotNull
    public ColumnMetadata getColumn(CharSequence name) {
        return getColumn(getColumnIndex(name));
    }

    @Override
    public ColumnMetadata getColumn(int columnIndex) {
        return columnMetadata[columnIndex];
    }

    public int getColumnCount() {
        return columnCount;
    }

    public int getColumnIndex(CharSequence columnName) {
        int result = columnIndexLookup.get(columnName);
        if (result == -1) {
            throw new NoSuchColumnException("Invalid column name: %s", columnName);
        }
        return result;
    }

    public String getId() {
        return id;
    }

    public String getKey() {
        if (key == null) {
            throw new JournalConfigurationException(modelClass.getName() + " does not have a key");
        }
        return key;
    }

    public String getKeyQuiet() {
        return key;
    }

    public int getLag() {
        return this.lag;
    }

    public String getLocation() {
        return location;
    }

    public Class<T> getModelClass() {
        return modelClass;
    }

    public long getOpenFileTTL() {
        return openFileTTL;
    }

    public PartitionType getPartitionType() {
        return partitionBy;
    }

    public int getRecordHint() {
        return ioBlockRecordCount;
    }

    public int getTimestampIndex() {
        return timestampColumnIndex;
    }

    public ColumnMetadata getTimestampMetadata() {
        return timestampMetadata;
    }

    public int getTxCountHint() {
        return ioBlockTxCount;
    }

    public boolean isCompatible(JournalMetadata that) {
        if (that == null
                || this.getPartitionType() != that.getPartitionType()
                || this.getColumnCount() != that.getColumnCount()
                ) {
            return false;
        }

        for (int i = 0, k = getColumnCount(); i < k; i++) {
            ColumnMetadata thisM = this.getColumn(i);
            ColumnMetadata thatM = that.getColumn(i);

            if (!thisM.name.equals(thatM.name)
                    || thisM.type != thatM.type
                    || thisM.size != thatM.size
                    || thisM.distinctCountHint != thatM.distinctCountHint
                    || thisM.indexed != thatM.indexed
                    || (thisM.sameAs == null && thatM.sameAs != null)
                    || (thisM.sameAs != null && !thisM.sameAs.equals(thatM.sameAs))
                    ) {
                return false;
            }
        }

        return true;
    }

    public boolean isPartialMapped() {
        return partialMapping;
    }

    @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
    public Object newObject() {
        try {
            return constructor.newInstance();
        } catch (Exception e) {
            throw new JournalRuntimeException("Could not create instance of class: " + modelClass.getName(), e);
        }
    }

    @Override
    public String toString() {

        StringBuilder b = new StringBuilder();
        sep(b);
        b.append("|");
        pad(b, TO_STRING_COL1_PAD, "Location:");
        pad(b, TO_STRING_COL2_PAD, location).append('\n');


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
        buf.setAppendOffset(buf.getPos());
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
