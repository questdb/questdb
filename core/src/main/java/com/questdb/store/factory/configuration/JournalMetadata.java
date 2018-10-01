/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.store.factory.configuration;

import com.questdb.ex.JournalConfigurationException;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.Chars;
import com.questdb.std.Misc;
import com.questdb.std.Unsafe;
import com.questdb.std.str.CharSink;
import com.questdb.store.*;

import java.lang.reflect.Constructor;

public class JournalMetadata<T> extends AbstractRecordMetadata {

    private static final int TO_STRING_COL1_PAD = 20;
    private static final int TO_STRING_COL2_PAD = 55;
    private final String name;
    private final Class<T> modelClass;
    private final int partitionBy;
    private final int columnCount;
    private final ColumnMetadata timestampMetadata;
    private final Constructor<T> constructor;
    private final long openFileTTL;
    private final int ioBlockRecordCount;
    private final int ioBlockTxCount;
    private final String keyColumn;
    private final ColumnMetadata[] columnMetadata;
    private final CharSequenceIntHashMap columnIndexLookup;
    private final int timestampColumnIndex;
    private final int lag;
    private final boolean partialMapping;
    private final JournalKey<T> key;
    private final boolean ordered;

    public JournalMetadata(
            String name
            , Class<T> modelClass
            , Constructor<T> constructor
            , String keyColumn
            , int partitionBy
            , ColumnMetadata[] columnMetadata
            , int timestampColumnIndex
            , long openFileTTL
            , int ioBlockRecordCount
            , int ioBlockTxCount
            , int lag
            , boolean partialMapping
            , boolean ordered
    ) {
        this.name = name;
        this.modelClass = modelClass;
        this.partitionBy = partitionBy;
        this.columnMetadata = new ColumnMetadata[columnMetadata.length];
        System.arraycopy(columnMetadata, 0, this.columnMetadata, 0, columnMetadata.length);
        this.columnCount = columnMetadata.length;
        this.timestampMetadata = timestampColumnIndex > -1 ? columnMetadata[timestampColumnIndex] : null;
        this.timestampColumnIndex = timestampColumnIndex;
        this.constructor = constructor;
        this.openFileTTL = openFileTTL;
        this.ioBlockRecordCount = ioBlockRecordCount;
        this.ioBlockTxCount = ioBlockTxCount;
        this.keyColumn = keyColumn;
        this.columnIndexLookup = new CharSequenceIntHashMap(columnCount);
        for (int i = 0; i < columnMetadata.length; i++) {
            columnIndexLookup.put(columnMetadata[i].name, i);
        }
        this.lag = lag;
        this.partialMapping = partialMapping;
        this.key = new JournalKey<>(modelClass, name, partitionBy, ioBlockRecordCount, ordered);
//        if (modelClass != null) {
//            this.key = new JournalKey<>(modelClass, Chars.getFileName(path), partitionBy, ordered);
//        } else {
//            this.key = new JournalKey<>(name, null, partitionBy, ioBlockRecordCount, ordered);
//        }
        this.ordered = ordered;
    }

    public JournalMetadata(UnstructuredFile buf, String name) {
        buf.setPos(0);
        String oldName = buf.getStr();
        this.name = name == null ? oldName : name;
        modelClass = null;
        partitionBy = buf.getInt();
        columnCount = buf.getInt();
        timestampColumnIndex = buf.getInt();
        columnMetadata = new ColumnMetadata[columnCount];
        columnIndexLookup = new CharSequenceIntHashMap();
        for (int i = 0; i < columnCount; i++) {
            columnMetadata[i] = new ColumnMetadata();
            columnMetadata[i].read(buf);
            columnIndexLookup.put(columnMetadata[i].name, i);
        }
        if (timestampColumnIndex > -1) {
            timestampMetadata = columnMetadata[timestampColumnIndex];
        } else {
            timestampMetadata = null;
        }
        openFileTTL = buf.getLong();
        ioBlockRecordCount = buf.getInt();
        ioBlockTxCount = buf.getInt();
        keyColumn = buf.getStr();
        lag = buf.getInt();
        ordered = buf.getBool();
        constructor = null;
        partialMapping = false;
        this.key = new JournalKey<>(null, this.name, partitionBy, ioBlockRecordCount, ordered);
    }

    public void copyColumnMetadata(ColumnMetadata[] meta) {

        if (meta.length != columnCount) {
            throw new JournalRuntimeException("Invalid length for column metadata array");
        }
        System.arraycopy(columnMetadata, 0, meta, 0, meta.length);
    }

    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getColumnIndexQuiet(CharSequence name) {
        int index = columnIndexLookup.get(name);
        if (index > -1) {
            return index;
        }

        if (getAlias() == null) {
            return -1;
        }

        ColumnName columnName = ColumnName.singleton(name);

        if (columnName.alias().length() == 0) {
            return -1;
        }

        if (Chars.equals(columnName.alias(), getAlias())) {
            return columnIndexLookup.get(columnName.name());
        }
        return -1;
    }

    @Override
    public ColumnMetadata getColumnQuick(int index) {
        return Unsafe.arrayGet(columnMetadata, index);
    }

    public int getTimestampIndex() {
        return timestampColumnIndex;
    }

    public JournalKey<T> getKey() {
        return key;
    }

    public String getKeyColumn() {
        if (keyColumn == null) {
            throw new JournalConfigurationException(modelClass.getName() + " does not have a keyColumn");
        }
        return keyColumn;
    }

    public String getKeyQuiet() {
        return keyColumn;
    }

    public int getLag() {
        return this.lag;
    }

    public Class<T> getModelClass() {
        return modelClass;
    }

    public String getModelClassName() {
        return modelClass == null ? null : modelClass.getName();
    }

    public String getName() {
        return name;
    }

    public long getOpenFileTTL() {
        return openFileTTL;
    }

    public int getPartitionBy() {
        return partitionBy;
    }


    public int getRecordHint() {
        return ioBlockRecordCount;
    }

    public ColumnMetadata getTimestampMetadata() {
        return timestampMetadata;
    }

    public int getTxCountHint() {
        return ioBlockTxCount;
    }

    public boolean isCompatible(JournalMetadata that, boolean ignorePartitionType) {
        if (that == null
                || (this.getPartitionBy() != that.getPartitionBy() && !ignorePartitionType)
                || this.getColumnCount() != that.getColumnCount()
        ) {
            return false;
        }

        for (int i = 0, k = getColumnCount(); i < k; i++) {
            ColumnMetadata thisM = this.getColumnQuick(i);
            ColumnMetadata thatM = that.getColumnQuick(i);

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

    public boolean isOrdered() {
        return ordered;
    }

    public boolean isPartialMapped() {
        return partialMapping;
    }

    public Object newObject() {
        if (constructor == null) {
            throw new JournalRuntimeException("There is no object class associated with this journal. Please use generic access methods");
        }
        try {
            return constructor.newInstance();
        } catch (Exception e) {
            throw new JournalRuntimeException("Could not create instance of class: " + modelClass.getName(), e);
        }
    }

    @Override
    public String toString() {

        CharSink b = Misc.getThreadLocalBuilder();
        sep(b);
        b.put('|');
        pad(b, TO_STRING_COL1_PAD, "Name:");
        pad(b, TO_STRING_COL2_PAD, name).put('\n');


        b.put('|');
        pad(b, TO_STRING_COL1_PAD, "Partition by");
        pad(b, TO_STRING_COL2_PAD, PartitionBy.toString(partitionBy)).put('\n');
        sep(b);


        for (int i = 0; i < columnCount; i++) {
            b.put('|');
            pad(b, TO_STRING_COL1_PAD, Integer.toString(i));
            col(b, columnMetadata[i]);
            b.put('\n');
        }

        sep(b);

        return b.toString();
    }

    public void write(UnstructuredFile buf) {
        buf.setPos(0);
        buf.put(name);
        buf.put(partitionBy);
        buf.put(columnCount);
        buf.put(timestampColumnIndex);
        for (int i = 0; i < columnMetadata.length; i++) {
            columnMetadata[i].write(buf);
        }
        buf.put(openFileTTL);
        buf.put(ioBlockRecordCount);
        buf.put(ioBlockTxCount);
        buf.put(keyColumn);
        buf.put(lag);
        buf.put(ordered);
        buf.setAppendOffset(buf.getPos());
    }

    private static CharSink pad(CharSink b, int w, String value) {
        int pad = value == null ? w : w - value.length();
        for (int i = 0; i < pad; i++) {
            b.put(' ');
        }

        if (value != null) {
            if (pad < 0) {
                b.put("...").put(value.substring(-pad + 3));
            } else {
                b.put(value);
            }
        }

        b.put("  |");

        return b;
    }

    private static void sep(CharSink b) {
        b.put('+');
        for (int i = 0; i < TO_STRING_COL1_PAD + TO_STRING_COL2_PAD + 5; i++) {
            b.put('-');
        }
        b.put("+\n");
    }

    private void col(CharSink b, ColumnMetadata m) {
        pad(b, TO_STRING_COL2_PAD, (m.distinctCountHint > 0 ? m.distinctCountHint + " ~ " : "") + (m.indexed ? '#' : "") + m.name + (m.sameAs != null ? " -> " + m.sameAs : "") + ' ' + ColumnType.nameOf(m.type) + '(' + m.size + ')');
    }
}
