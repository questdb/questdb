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

package com.questdb.griffin.model;

import com.questdb.cairo.ColumnType;
import com.questdb.std.*;
import com.questdb.std.str.CharSink;

public class CreateTableModel implements Mutable, ExecutionModel, Sinkable {
    public static final ObjectFactory<CreateTableModel> FACTORY = CreateTableModel::new;
    private static final long COLUMN_FLAG_CHACHED = 1L;
    private static final long COLUMN_FLAG_INDEXED = 2L;
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
            columnBits.add(((long) symbolCapacity << 32) | type);
            columnBits.add(COLUMN_FLAG_CHACHED);
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
        assert ((int) columnBits.getQuick(last - 1) == ColumnType.SYMBOL);
        long bits = columnBits.getQuick(last);
        if (cached) {
            columnBits.setQuick(last, bits | COLUMN_FLAG_CHACHED);
        } else {
            columnBits.setQuick(last, bits & ~COLUMN_FLAG_CHACHED);
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

    public int getColumnCount() {
        return columnNames.size();
    }

    public int getColumnIndex(CharSequence columnName) {
        return columnNameIndexMap.get(columnName);
    }

    public CharSequence getColumnName(int index) {
        return columnNames.getQuick(index);
    }

    public int getColumnType(int index) {
        return (int) columnBits.getQuick(index * 2);
    }

    public int getIndexBlockCapacity(int index) {
        return (int) (columnBits.getQuick(index * 2 + 1) >> 32);
    }

    public boolean getIndexedFlag(int index) {
        return (columnBits.getQuick(index * 2 + 1) & COLUMN_FLAG_INDEXED) == COLUMN_FLAG_INDEXED;
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

    public ExpressionNode getPartitionBy() {
        return partitionBy;
    }

    public void setPartitionBy(ExpressionNode partitionBy) {
        this.partitionBy = partitionBy;
    }

    public QueryModel getQueryModel() {
        return queryModel;
    }

    public void setQueryModel(QueryModel queryModel) {
        this.queryModel = queryModel;
    }

    public boolean getSymbolCacheFlag(int index) {
        return (columnBits.getQuick(index * 2 + 1) & COLUMN_FLAG_CHACHED) == COLUMN_FLAG_CHACHED;
    }

    public int getSymbolCapacity(int index) {
        return (int) (columnBits.getQuick(index * 2) >> 32);
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
        int pos = columnBits.size() - 2;
        assert pos > -1;
        long bits = columnBits.getQuick(pos);
        assert ((int) bits == ColumnType.SYMBOL);
        bits = (((long) capacity) << 32) | (int) bits;
        columnBits.setQuick(pos, bits);
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

        if (getPartitionBy() != null) {
            sink.put(" partition by ").put(getPartitionBy().token);
        }
    }

    private void setIndexFlags0(int columnIndex, boolean indexFlag, int indexValueBlockSize) {
        assert columnIndex > 0;
        long bits = columnBits.getQuick(columnIndex);
        if (indexFlag) {
            assert indexValueBlockSize > 1;
            columnBits.setQuick(columnIndex, bits | ((long) Numbers.ceilPow2(indexValueBlockSize) << 32) | COLUMN_FLAG_INDEXED);
        } else {
            columnBits.setQuick(columnIndex, bits & ~COLUMN_FLAG_INDEXED);
        }
    }
}
