/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.factory.configuration;

import com.questdb.JournalKey;
import com.questdb.PartitionType;
import com.questdb.ex.JournalConfigurationException;
import com.questdb.ex.JournalRuntimeException;
import com.questdb.misc.Chars;
import com.questdb.misc.Unsafe;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.store.UnstructuredFile;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.lang.StringBuilder;
import java.lang.reflect.Constructor;

public class JournalMetadata<T> extends AbstractRecordMetadata {

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
    private final String keyColumn;
    private final ColumnMetadata[] columnMetadata;
    private final CharSequenceIntHashMap columnIndexLookup;
    private final int timestampColumnIndex;
    private final int lag;
    private final boolean partialMapping;
    private final JournalKey<T> key;
    private final ColumnName columnName = new ColumnName();

    public JournalMetadata(
            String id
            , Class<T> modelClass
            , Constructor<T> constructor
            , String keyColumn
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
        if (modelClass != null) {
            this.key = new JournalKey<>(modelClass, location);
        } else {
            this.key = new JournalKey<>(id);
        }
    }

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public JournalMetadata(UnstructuredFile buf) {
        buf.setPos(0);
        id = buf.getStr();
        modelClass = null;
        location = buf.getStr();
        partitionBy = PartitionType.valueOf(buf.getStr());
        columnCount = buf.getInt();
        columnMetadata = new ColumnMetadata[columnCount];
        columnIndexLookup = new CharSequenceIntHashMap();
        for (int i = 0; i < columnCount; i++) {
            columnMetadata[i] = new ColumnMetadata();
            columnMetadata[i].read(buf);
            columnIndexLookup.put(columnMetadata[i].name, i);
        }
        timestampColumnIndex = buf.getInt();
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
        constructor = null;
        partialMapping = false;
        this.key = new JournalKey<>(id);
    }

    public void copyColumnMetadata(ColumnMetadata[] meta) {

        if (meta.length != columnCount) {
            throw new JournalRuntimeException("Invalid length for column metadata array");
        }
        System.arraycopy(columnMetadata, 0, meta, 0, meta.length);
    }

    @Override
    public ColumnMetadata getColumn(int columnIndex) {
        return columnMetadata[columnIndex];
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

        columnName.of(name);

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

    public String getId() {
        return id;
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

    public ColumnMetadata getTimestampMetadata() {
        return timestampMetadata;
    }

    public int getTxCountHint() {
        return ioBlockTxCount;
    }

    public boolean isCompatible(JournalMetadata that, boolean ignorePartitionType) {
        if (that == null
                || (this.getPartitionType() != that.getPartitionType() && !ignorePartitionType)
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

    public boolean isPartialMapped() {
        return partialMapping;
    }

    @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
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

        StringBuilder b = new StringBuilder();
        sep(b);
        b.append('|');
        pad(b, TO_STRING_COL1_PAD, "Location:");
        pad(b, TO_STRING_COL2_PAD, location).append('\n');


        b.append('|');
        pad(b, TO_STRING_COL1_PAD, "Partition by");
        pad(b, TO_STRING_COL2_PAD, partitionBy.name()).append('\n');
        sep(b);


        for (int i = 0; i < columnCount; i++) {
            b.append('|');
            pad(b, TO_STRING_COL1_PAD, Integer.toString(i));
            col(b, columnMetadata[i]);
            b.append('\n');
        }

        sep(b);

        return b.toString();
    }

    public void write(UnstructuredFile buf) {
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
        buf.put(keyColumn);
        buf.put(lag);
        buf.setAppendOffset(buf.getPos());
    }

    private static StringBuilder pad(StringBuilder b, int w, String value) {
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

    private static void sep(StringBuilder b) {
        b.append('+');
        for (int i = 0; i < TO_STRING_COL1_PAD + TO_STRING_COL2_PAD + 5; i++) {
            b.append('-');
        }
        b.append("+\n");
    }

    private void col(StringBuilder b, ColumnMetadata m) {
        pad(b, TO_STRING_COL2_PAD, (m.distinctCountHint > 0 ? m.distinctCountHint + " ~ " : "") + (m.indexed ? '#' : "") + m.name + (m.sameAs != null ? " -> " + m.sameAs : "") + ' ' + m.type.name() + '(' + m.size + ')');
    }
}
