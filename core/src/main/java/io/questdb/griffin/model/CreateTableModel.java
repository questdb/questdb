/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.model;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableStructure;
import io.questdb.griffin.SqlException;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

public class CreateTableModel implements Mutable, ExecutionModel, Sinkable, TableStructure {
    public static final ObjectFactory<CreateTableModel> FACTORY = CreateTableModel::new;
    private static final int COLUMN_FLAG_CACHED = 1;
    private static final int COLUMN_FLAG_INDEXED = COLUMN_FLAG_CACHED << 1;
    private static final int COLUMN_FLAG_DEDUP_KEY = COLUMN_FLAG_INDEXED << 1;
    private final LongList columnBits = new LongList();
    private final CharSequenceObjHashMap<ColumnCastModel> columnCastModels = new CharSequenceObjHashMap<>();
    private final LowerCaseCharSequenceIntHashMap columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private boolean ignoreIfExists = false;
    private ExpressionNode likeTableName;
    private int maxUncommittedRows;
    private ExpressionNode name;
    private long o3MaxLag;
    private ExpressionNode partitionBy;
    private QueryModel queryModel;
    private ExpressionNode timestamp;
    private CharSequence volumeAlias;
    private boolean walEnabled;

    private CreateTableModel() {
    }

    public void addColumn(CharSequence name, int type, int symbolCapacity) throws SqlException {
        addColumn(0, name, type, symbolCapacity);
    }

    public void addColumn(int position, CharSequence name, int type, int symbolCapacity) throws SqlException {
        if (!columnNameIndexMap.put(name, columnNames.size())) {
            throw SqlException.duplicateColumn(position, name);
        }
        columnNames.add(Chars.toString(name));
        columnBits.add(
                Numbers.encodeLowHighInts(type, symbolCapacity),
                Numbers.encodeLowHighInts(COLUMN_FLAG_CACHED, 0)
        );
    }

    public boolean addColumnCastModel(ColumnCastModel model) {
        return columnCastModels.put(model.getName().token, model);
    }

    public CreateTableModel cached(boolean cached) {
        int last = columnBits.size() - 1;
        assert last > 0;
        assert ColumnType.isSymbol(getLowAt(last - 1));
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
        likeTableName = null;
        name = null;
        volumeAlias = null;
        columnBits.clear();
        columnNames.clear();
        columnNameIndexMap.clear();
        ignoreIfExists = false;
    }

    public CharSequenceObjHashMap<ColumnCastModel> getColumnCastModels() {
        return columnCastModels;
    }

    @Override
    public int getColumnCount() {
        return columnNames.size();
    }

    public int getColumnIndex(CharSequence columnName) {
        return columnNameIndexMap.get(columnName);
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

    public ExpressionNode getLikeTableName() {
        return likeTableName;
    }

    @Override
    public int getMaxUncommittedRows() {
        return maxUncommittedRows;
    }

    @Override
    public int getModelType() {
        return CREATE_TABLE;
    }

    public ExpressionNode getName() {
        return name;
    }

    @Override
    public long getO3MaxLag() {
        return o3MaxLag;
    }

    @Override
    public int getPartitionBy() {
        return partitionBy == null ? PartitionBy.NONE : PartitionBy.fromString(partitionBy.token);
    }

    public QueryModel getQueryModel() {
        return queryModel;
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

    public ExpressionNode getTimestamp() {
        return timestamp;
    }

    @Override
    public int getTimestampIndex() {
        return timestamp == null ? -1 : getColumnIndex(timestamp.token);
    }

    public CharSequence getVolumeAlias() {
        return volumeAlias;
    }

    @Override
    public boolean isDedupKey(int index) {
        return (getLowAt(index * 2 + 1) & COLUMN_FLAG_DEDUP_KEY) != 0;
    }

    public boolean isIgnoreIfExists() {
        return ignoreIfExists;
    }

    @Override
    public boolean isIndexed(int index) {
        return (getLowAt(index * 2 + 1) & COLUMN_FLAG_INDEXED) != 0;
    }

    @Override
    public boolean isSequential(int columnIndex) {
        // todo: expose this flag on CREATE TABLE statement
        return false;
    }

    @Override
    public boolean isWalEnabled() {
        return walEnabled;
    }

    public void setDedupKeyFlag(int index) {
        int flagsIndex = index * 2 + 1;
        int flags = getLowAt(flagsIndex) | COLUMN_FLAG_DEDUP_KEY;
        columnBits.setQuick(flagsIndex, Numbers.encodeLowHighInts(flags, getHighAt(flagsIndex)));
    }

    public void setIgnoreIfExists(boolean flag) {
        this.ignoreIfExists = flag;
    }

    public void setIndexFlags(boolean indexFlag, int indexValueBlockSize) {
        setIndexFlags0(columnBits.size() - 1, indexFlag, indexValueBlockSize);
    }

    public void setIndexFlags(int columnIndex, boolean indexFlag, int indexValueBlockSize) {
        setIndexFlags0(columnIndex * 2 + 1, indexFlag, indexValueBlockSize);
    }

    public void setLikeTableName(ExpressionNode tableName) {
        this.likeTableName = tableName;
    }

    public void setMaxUncommittedRows(int maxUncommittedRows) {
        this.maxUncommittedRows = maxUncommittedRows;
    }

    public void setName(ExpressionNode name) {
        this.name = name;
    }

    public void setO3MaxLag(long o3MaxLag) {
        this.o3MaxLag = o3MaxLag;
    }

    public void setPartitionBy(ExpressionNode partitionBy) {
        this.partitionBy = partitionBy;
    }

    public void setQueryModel(QueryModel queryModel) {
        this.queryModel = queryModel;
    }

    public void setTimestamp(ExpressionNode timestamp) {
        this.timestamp = timestamp;
    }

    public void setVolumeAlias(CharSequence volumeAlias) {
        // set if the create table statement contains IN VOLUME 'volumeAlias'.
        // volumePath will be resolved by the compiler
        this.volumeAlias = Chars.toString(volumeAlias);
    }

    public void setWalEnabled(boolean walEnabled) {
        this.walEnabled = walEnabled;
    }

    public void symbolCapacity(int capacity) {
        final int pos = columnBits.size() - 2;
        assert pos > -1;
        final int type = getLowAt(pos);
        assert ColumnType.isSymbol(type);
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
                if (isIndexed(i)) {
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
                if (ColumnType.isSymbol(type)) {
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
            if (getLikeTableName() != null) {
                sink.put("like ");
                sink.put(getLikeTableName().token);
            } else {
                int count = getColumnCount();
                for (int i = 0; i < count; i++) {
                    if (i > 0) {
                        sink.put(", ");
                    }
                    sink.put(getColumnName(i));
                    sink.put(' ');
                    sink.put(ColumnType.nameOf(getColumnType(i)));

                    if (ColumnType.isSymbol(getColumnType(i))) {
                        sink.put(" capacity ");
                        sink.put(getSymbolCapacity(i));
                        if (getSymbolCacheFlag(i)) {
                            sink.put(" cache");
                        } else {
                            sink.put(" nocache");
                        }
                    }

                    if (isIndexed(i)) {
                        sink.put(" index capacity ");
                        sink.put(getIndexBlockCapacity(i));
                    }
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
            if (walEnabled) {
                sink.put(" wal");
            }
        }

        if (volumeAlias != null) {
            sink.put(" in volume '").put(volumeAlias).put('\'');
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
