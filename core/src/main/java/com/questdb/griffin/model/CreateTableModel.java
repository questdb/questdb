/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.griffin.model;

import com.questdb.cairo.ColumnType;
import com.questdb.cairo.PartitionBy;
import com.questdb.cairo.TableStructure;
import com.questdb.std.*;
import com.questdb.std.str.CharSink;

public class CreateTableModel implements Mutable, ExecutionModel, Sinkable, TableStructure {
    public static final ObjectFactory<CreateTableModel> FACTORY = CreateTableModel::new;
    private static final int COLUMN_FLAG_CACHED = 1;
    private static final int COLUMN_FLAG_INDEXED = 2;
    private final CharSequenceObjHashMap<ColumnCastModel> columnCastModels = new CharSequenceObjHashMap<>();
    private final LongList columnBits = new LongList();
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private final CharSequenceIntHashMap columnNameIndexMap = new CharSequenceIntHashMap();
    private ExpressionNode name;
    private QueryModel queryModel;
    private ExpressionNode timestamp;
    private ExpressionNode partitionBy;

    private CreateTableModel() {
    }

    public boolean addColumn(CharSequence name, int type, int symbolCapacity) {
        if (columnNameIndexMap.put(name, columnNames.size())) {
            columnNames.add(Chars.stringOf(name));
            columnBits.add(Numbers.encodeLowHighInts(type, symbolCapacity));
            columnBits.add(Numbers.encodeLowHighInts(COLUMN_FLAG_CACHED, 0));
            return true;
        }
        return false;
    }

    public boolean addColumnCastModel(ColumnCastModel model) {
        return columnCastModels.put(model.getName().token, model);
    }

    public CreateTableModel cached(boolean cached) {
        int last = columnBits.size() - 1;
        assert last > 0;
        assert getLowAt(last - 1) == ColumnType.SYMBOL;
        if (cached) {
            columnBits.setQuick(last, Numbers.encodeLowHighInts(getLowAt(last) | COLUMN_FLAG_CACHED, getHighAt(last)));
        } else {
            columnBits.setQuick(last, Numbers.encodeLowHighInts(getLowAt(last) & ~COLUMN_FLAG_CACHED, getHighAt(last)));
        }
        return this;
    }

    @Override
    public void clear() {
        columnCastModels.clear();
        queryModel = null;
        timestamp = null;
        partitionBy = null;
        name = null;
        columnBits.clear();
        columnNames.clear();
        columnNameIndexMap.clear();
    }

    public CharSequenceObjHashMap<ColumnCastModel> getColumnCastModels() {
        return columnCastModels;
    }

    @Override
    public int getColumnCount() {
        return columnNames.size();
    }

    @Override
    public CharSequence getColumnName(int index) {
        return columnNames.getQuick(index);
    }

    @Override
    public int getColumnType(int index) {
        return getLowAt(index * 2);
    }

    @Override
    public int getIndexBlockCapacity(int index) {
        return getHighAt(index * 2 + 1);
    }

    @Override
    public boolean getIndexedFlag(int index) {
        return (getLowAt(index * 2 + 1) & COLUMN_FLAG_INDEXED) != 0;
    }

    @Override
    public int getPartitionBy() {
        return partitionBy == null ? PartitionBy.NONE : PartitionBy.fromString(partitionBy.token);
    }

    public void setPartitionBy(ExpressionNode partitionBy) {
        this.partitionBy = partitionBy;
    }

    @Override
    public boolean getSymbolCacheFlag(int index) {
        return (getLowAt(index * 2 + 1) & COLUMN_FLAG_CACHED) != 0;
    }

    @Override
    public int getSymbolCapacity(int index) {
        int capacity = getHighAt(index * 2);
        assert capacity != -1;
        return capacity;
    }

    @Override
    public CharSequence getTableName() {
        return name.token;
    }

    @Override
    public int getTimestampIndex() {
        return timestamp == null ? -1 : getColumnIndex(timestamp.token);
    }

    public int getColumnIndex(CharSequence columnName) {
        return columnNameIndexMap.get(columnName);
    }

    @Override
    public int getModelType() {
        return ExecutionModel.CREATE_TABLE;
    }

    public ExpressionNode getName() {
        return name;
    }

    public void setName(ExpressionNode name) {
        this.name = name;
    }

    public QueryModel getQueryModel() {
        return queryModel;
    }

    public void setQueryModel(QueryModel queryModel) {
        this.queryModel = queryModel;
    }

    public ExpressionNode getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(ExpressionNode timestamp) {
        this.timestamp = timestamp;
    }

    public void setIndexFlags(boolean indexFlag, int indexValueBlockSize) {
        setIndexFlags0(columnBits.size() - 1, indexFlag, indexValueBlockSize);
    }

    public void setIndexFlags(int columnIndex, boolean indexFlag, int indexValueBlockSize) {
        setIndexFlags0(columnIndex * 2 + 1, indexFlag, indexValueBlockSize);
    }

    public void symbolCapacity(int capacity) {
        final int pos = columnBits.size() - 2;
        assert pos > -1;
        final int type = getLowAt(pos);
        assert type == ColumnType.SYMBOL;
        columnBits.setQuick(pos, Numbers.encodeLowHighInts(type, capacity));
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("create table ");
        sink.put(getName().token);
        if (getQueryModel() != null) {
            sink.put(" as (");
            getQueryModel().toSink(sink);
            sink.put(')');
            for (int i = 0, n = getColumnCount(); i < n; i++) {
                if (getIndexedFlag(i)) {
                    sink.put(", index(");
                    sink.put(getColumnName(i));
                    sink.put(" capacity ");
                    sink.put(getIndexBlockCapacity(i));
                    sink.put(')');
                }
            }
            final ObjList<CharSequence> castColumns = getColumnCastModels().keys();
            for (int i = 0, n = castColumns.size(); i < n; i++) {
                final CharSequence column = castColumns.getQuick(i);
                final ColumnCastModel m = getColumnCastModels().get(column);
                final int type = m.getColumnType();
                sink.put(", cast(");
                sink.put(column);
                sink.put(" as ");
                sink.put(ColumnType.nameOf(type));
                sink.put(':');
                sink.put(m.getColumnTypePos());
                if (type == ColumnType.SYMBOL) {
                    sink.put(" capacity ");
                    sink.put(m.getSymbolCapacity());
                    if (m.getSymbolCacheFlag()) {
                        sink.put(" cache");
                    } else {
                        sink.put(" nocache");
                    }

                    if (m.isIndexed()) {
                        sink.put(" index capacity ");
                        sink.put(m.getIndexValueBlockSize());
                    }
                }
                sink.put(')');
            }
        } else {
            sink.put(" (");
            int count = getColumnCount();
            for (int i = 0; i < count; i++) {
                if (i > 0) {
                    sink.put(", ");
                }
                sink.put(getColumnName(i));
                sink.put(' ');
                sink.put(ColumnType.nameOf(getColumnType(i)));

                if (getColumnType(i) == ColumnType.SYMBOL) {
                    sink.put(" capacity ");
                    sink.put(getSymbolCapacity(i));
                    if (getSymbolCacheFlag(i)) {
                        sink.put(" cache");
                    } else {
                        sink.put(" nocache");
                    }
                }

                if (getIndexedFlag(i)) {
                    sink.put(" index capacity ");
                    sink.put(getIndexBlockCapacity(i));
                }
            }
            sink.put(')');
        }

        if (getTimestamp() != null) {
            sink.put(" timestamp(");
            sink.put(getTimestamp().token);
            sink.put(')');
        }

        if (partitionBy != null) {
            sink.put(" partition by ").put(partitionBy.token);
        }
    }

    private int getHighAt(int index) {
        return Numbers.decodeHighInt(columnBits.getQuick(index));
    }

    private int getLowAt(int index) {
        return Numbers.decodeLowInt(columnBits.getQuick(index));
    }

    private void setIndexFlags0(int index, boolean indexFlag, int indexValueBlockSize) {
        assert index > 0;
        final int flags = getLowAt(index);
        if (indexFlag) {
            assert indexValueBlockSize > 1;
            columnBits.setQuick(index, Numbers.encodeLowHighInts(flags | COLUMN_FLAG_INDEXED, Numbers.ceilPow2(indexValueBlockSize)));
        } else {
            columnBits.setQuick(index, Numbers.encodeLowHighInts(flags & ~COLUMN_FLAG_INDEXED, Numbers.ceilPow2(indexValueBlockSize)));
        }
    }
}
