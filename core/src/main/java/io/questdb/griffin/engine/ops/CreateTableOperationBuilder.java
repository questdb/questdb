/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.TouchUpColumnModel;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

public class CreateTableOperationBuilder implements Mutable, ExecutionModel, Sinkable {
    public static final ObjectFactory<CreateTableOperationBuilder> FACTORY = CreateTableOperationBuilder::new;
    static final int COLUMN_FLAG_CACHED = 1;
    static final int COLUMN_FLAG_INDEXED = COLUMN_FLAG_CACHED << 1;
    static final int COLUMN_FLAG_DEDUP_KEY = COLUMN_FLAG_INDEXED << 1;
    private static final IntList castGroups = new IntList();
    // This list encodes column attributes from the column definition of the "create table" SQL
    // these attributes include: column type, index flag, index capacity and symbol capacity.
    // The list is populated by the SQL parser at compile time.
    // For create-as-select SQL, parser does not populate this list. Instead, the list
    // is used at the execution time to capture the attributes of columns of the "select" SQL.
    private final LongList columnBits = new LongList();
    private final LowerCaseCharSequenceIntHashMap columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    private final ObjList<String> columnNames = new ObjList<>();
    private final CharSequenceObjHashMap<TouchUpColumnModel> touchUpColumnModels = new CharSequenceObjHashMap<>();
    private final IntIntHashMap typeCasts = new IntIntHashMap();
    private long batchO3MaxLag = -1;
    private long batchSize = -1;
    private int defaultSymbolCapacity;
    private boolean ignoreIfExists = false;
    private ExpressionNode likeTableNameExpr;
    private int maxUncommittedRows;
    private long o3MaxLag;
    private ExpressionNode partitionByExpr;
    private QueryModel queryModel;
    private RecordCursorFactory recordCursorFactory;
    private ExpressionNode tableNameExpr;
    private ExpressionNode timestampExpr;
    private CharSequence volumeAlias;
    private boolean walEnabled;

    public void addColumn(int columnPosition, CharSequence columnName, int columnType, int symbolCapacity) throws SqlException {
        if (!columnNameIndexMap.put(columnName, columnNames.size())) {
            throw SqlException.duplicateColumn(columnPosition, columnName);
        }
        // todo: columnNames need not be strings, they can be made strings when they are copied to the operation
        columnNames.add(Chars.toString(columnName));
        columnBits.add(
                Numbers.encodeLowHighInts(columnType, symbolCapacity),
                Numbers.encodeLowHighInts(COLUMN_FLAG_CACHED, 0)
        );
    }

    public CreateTableOperation build(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, CharSequence sqlText) throws SqlException {
        tableNameExpr.token = Chars.toString(tableNameExpr.token);
        if (queryModel != null) {
            setFactory(compiler.generateSelectWithRetries(queryModel, sqlExecutionContext, false));
            return new CreateTableOperation(
                    Chars.toString(tableNameExpr.token),
                    tableNameExpr.position,
                    getPartitionByFromExpr(),
                    Chars.toString(volumeAlias),
                    ignoreIfExists,
                    timestampExpr != null ? Chars.toString(timestampExpr.token) : null,
                    timestampExpr != null ? timestampExpr.position : 0,
                    batchSize,
                    batchO3MaxLag,
                    defaultSymbolCapacity,
                    recordCursorFactory,
                    Chars.toString(sqlText),
                    touchUpColumnModels
            );
        }

        // create table "like" and "as select" are mutually exclusive
        // todo: write a test for the above to ensure correctness of the diagnostic message
        if (this.likeTableNameExpr != null) {
            TableToken likeTableNameToken = compiler.getEngine().getTableTokenIfExists(this.likeTableNameExpr.token);
            if (likeTableNameToken == null) {
                throw SqlException.tableDoesNotExist(this.likeTableNameExpr.position, this.likeTableNameExpr.token);
            }
            return new CreateTableOperation(
                    Chars.toString(tableNameExpr.token),
                    tableNameExpr.position,
                    getPartitionByFromExpr(),
                    Chars.toString(volumeAlias),
                    likeTableNameToken.getTableName(),
                    likeTableNameExpr.position,
                    ignoreIfExists
            );
        }

        return new CreateTableOperation(
                Chars.toString(tableNameExpr.token),
                tableNameExpr.position,
                getPartitionByFromExpr(),
                Chars.toString(volumeAlias),
                ignoreIfExists,
                columnNames,
                columnBits,
                getTimestampIndex(),
                o3MaxLag,
                maxUncommittedRows,
                walEnabled
        );
    }

    @Override
    public void clear() {
        typeCasts.clear();
        touchUpColumnModels.clear();
        queryModel = null;
        timestampExpr = null;
        partitionByExpr = null;
        likeTableNameExpr = null;
        tableNameExpr = null;
        volumeAlias = null;
        columnBits.clear();
        columnNames.clear();
        columnNameIndexMap.clear();
        ignoreIfExists = false;
        o3MaxLag = -1;
        batchO3MaxLag = -1;
        batchSize = -1;
    }

    public int getColumnIndex(CharSequence columnName) {
        return columnNameIndexMap.get(columnName);
    }

    public int getColumnType(int columnIndex) {
        return getLowAt(columnIndex * 2);
    }

    @Override
    public int getModelType() {
        return CREATE_TABLE;
    }

    public int getPartitionByFromExpr() {
        return partitionByExpr == null ? PartitionBy.NONE : PartitionBy.fromString(partitionByExpr.token);
    }

    public QueryModel getQueryModel() {
        return queryModel;
    }

    @Override
    public CharSequence getTableName() {
        return tableNameExpr.token;
    }

    public ExpressionNode getTableNameExpr() {
        return tableNameExpr;
    }

    public ExpressionNode getTimestampExpr() {
        return timestampExpr;
    }

    public int getTimestampIndex() {
        return timestampExpr != null ? getColumnIndex(timestampExpr.token) : -1;
    }

    public CharSequenceObjHashMap<TouchUpColumnModel> getTouchUpColumnModels() {
        return touchUpColumnModels;
    }

    public boolean isAtomic() {
        return batchSize == -1;
    }

    public boolean isIndexed(int index) {
        return (getLowAt(index * 2 + 1) & COLUMN_FLAG_INDEXED) != 0;
    }

    public boolean isWalEnabled() {
        return walEnabled;
    }

    public void setBatchO3MaxLag(long batchO3MaxLag) {
        this.batchO3MaxLag = batchO3MaxLag;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public void setDedupKeyFlag(int index) {
        int flagsIndex = index * 2 + 1;
        int flags = getLowAt(flagsIndex) | COLUMN_FLAG_DEDUP_KEY;
        columnBits.setQuick(flagsIndex, Numbers.encodeLowHighInts(flags, getHighAt(flagsIndex)));
    }

    public void setDefaultSymbolCapacity(int defaultSymbolCapacity) {
        this.defaultSymbolCapacity = defaultSymbolCapacity;
    }

    public void setFactory(RecordCursorFactory factory) throws SqlException {
        this.recordCursorFactory = factory;
        final RecordMetadata metadata = factory.getMetadata();
        CharSequenceObjHashMap<TouchUpColumnModel> touchUpModels = touchUpColumnModels;
        ObjList<CharSequence> touchUpColumnNames = touchUpModels.keys();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            columnNameIndexMap.put(metadata.getColumnName(i), i);
        }

        for (int i = 0, n = touchUpColumnNames.size(); i < n; i++) {
            CharSequence columnName = touchUpColumnNames.getQuick(i);
            int index = metadata.getColumnIndexQuiet(columnName);
            TouchUpColumnModel touchUp = touchUpModels.get(columnName);
            // the only reason why columns cannot be found at this stage is
            // concurrent table modification of table structure
            if (index == -1) {
                throw SqlException.invalidColumn(touchUp.getColumnNamePos(), columnName);
            }
            int from = metadata.getColumnType(index);
            int to = touchUp.getColumnType();
            if (to == ColumnType.UNDEFINED) {
                touchUp.setColumnType(from);
            } else if (isCompatibleCast(from, to)) {
                typeCasts.put(index, to);
            } else {
                throw SqlException.unsupportedCast(touchUp.getColumnTypePos(), columnName, from, to);
            }
        }
    }

    public void setIgnoreIfExists(boolean flag) {
        this.ignoreIfExists = flag;
    }

    public void setIndexedColumn(CharSequence columnName, int columnNamePosition, int indexValueBlockSize) throws SqlException {
        int columnIndex = getColumnIndex(columnName);
        if (columnIndex == -1) {
            throw SqlException.invalidColumn(columnNamePosition, columnName);
        }
        int flagsIndex = columnIndex * 2 + 1;
        int flags = getLowAt(flagsIndex) | COLUMN_FLAG_INDEXED;
        columnBits.setQuick(flagsIndex, Numbers.encodeLowHighInts(flags, Numbers.ceilPow2(indexValueBlockSize)));
    }

    public void setLikeTableNameExpr(ExpressionNode expr) {
        this.likeTableNameExpr = expr;
    }

    public void setMaxUncommittedRows(int maxUncommittedRows) {
        this.maxUncommittedRows = maxUncommittedRows;
    }

    public void setO3MaxLag(long o3MaxLag) {
        this.o3MaxLag = o3MaxLag;
    }

    public void setPartitionByExpr(ExpressionNode partitionByExpr) {
        this.partitionByExpr = partitionByExpr;
    }

    public void setQueryModel(QueryModel queryModel) {
        this.queryModel = queryModel;
    }

    public void setTableNameExpr(ExpressionNode expr) {
        this.tableNameExpr = expr;
    }

    public void setTimestampExpr(ExpressionNode expr) {
        this.timestampExpr = expr;
    }

    public void setVolumeAlias(CharSequence volumeAlias) {
        // set if the "create table" statement contains IN VOLUME 'volumeAlias'.
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
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("create");
        if (!isAtomic()) {
            sink.putAscii(" batch ");
            sink.put(batchSize);
            if (batchO3MaxLag != -1) {
                sink.putAscii(" o3MaxLag ");
                sink.put(batchO3MaxLag);
            }
        } else {
            sink.putAscii(" atomic");
        }
        sink.putAscii(" table ");
        sink.put(getTableNameExpr().token);
        if (getQueryModel() != null) {
            sink.putAscii(" as (");
            getQueryModel().toSink(sink);
            sink.putAscii(')');
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                if (isIndexed(i)) {
                    sink.putAscii(", index(");
                    sink.put(columnNames.getQuick(i));
                    sink.putAscii(" capacity ");
                    sink.put(getHighAt(i * 2 + 1));
                    sink.putAscii(')');
                }
            }
            final ObjList<CharSequence> castColumns = getTouchUpColumnModels().keys();
            for (int i = 0, n = castColumns.size(); i < n; i++) {
                final CharSequence column = castColumns.getQuick(i);
                final TouchUpColumnModel m = getTouchUpColumnModels().get(column);
                final int type = m.getColumnType();
                sink.putAscii(", cast(");
                sink.put(column);
                sink.putAscii(" as ");
                sink.put(ColumnType.nameOf(type));
                sink.putAscii(':');
                sink.put(m.getColumnTypePos());
                if (ColumnType.isSymbol(type)) {
                    sink.putAscii(" capacity ");
                    sink.put(m.getSymbolCapacity());
                    if (m.getSymbolCacheFlag()) {
                        sink.putAscii(" cache");
                    } else {
                        sink.putAscii(" nocache");
                    }

                    if (m.isIndexed()) {
                        sink.putAscii(" index capacity ");
                        sink.put(m.getIndexValueBlockSize());
                    }
                }
                sink.putAscii(')');
            }
        } else {
            sink.putAscii(" (");
            if (likeTableNameExpr != null) {
                sink.putAscii("like ");
                sink.put(likeTableNameExpr.token);
            } else {
                int count = columnNames.size();
                for (int i = 0; i < count; i++) {
                    if (i > 0) {
                        sink.putAscii(", ");
                    }
                    sink.put(columnNames.getQuick(i));
                    sink.putAscii(' ');
                    sink.put(ColumnType.nameOf(getLowAt(i * 2)));

                    if (ColumnType.isSymbol(getLowAt(i * 2))) {
                        sink.putAscii(" capacity ");
                        sink.put(getHighAt(i * 2));
                        if ((getLowAt(i * 2 + 1) & COLUMN_FLAG_CACHED) != 0) {
                            sink.putAscii(" cache");
                        } else {
                            sink.putAscii(" nocache");
                        }
                    }

                    if (isIndexed(i)) {
                        sink.putAscii(" index capacity ");
                        sink.put(getHighAt(i * 2 + 1));
                    }
                }
            }
            sink.putAscii(')');
        }

        if (getTimestampExpr() != null) {
            sink.putAscii(" timestamp(");
            sink.put(getTimestampExpr().token);
            sink.putAscii(')');
        }

        if (partitionByExpr != null) {
            sink.putAscii(" partition by ").put(partitionByExpr.token);
            if (walEnabled) {
                sink.putAscii(" wal");
            }
        }

        if (volumeAlias != null) {
            sink.putAscii(" in volume '").put(volumeAlias).putAscii('\'');
        }
    }

    public void updateIndexFlagOfCurrentLastColumn(boolean indexFlag, int indexValueBlockSize) {
        int index = columnBits.size() - 1;
        assert index > 0;
        int flags = getLowAt(index);
        if (indexFlag) {
            assert indexValueBlockSize > 1;
            flags |= COLUMN_FLAG_INDEXED;
        } else {
            flags &= ~COLUMN_FLAG_INDEXED;
        }
        columnBits.setQuick(index, Numbers.encodeLowHighInts(flags, Numbers.ceilPow2(indexValueBlockSize)));
    }

    public void updateSymbolCacheFlagOfCurrentLastColumn(boolean cached) {
        int last = columnBits.size() - 1;
        assert last > 0;
        assert ColumnType.isSymbol(getLowAt(last - 1));
        int flags = getLowAt(last);
        if (cached) {
            flags |= COLUMN_FLAG_CACHED;
        } else {
            flags &= ~COLUMN_FLAG_CACHED;
        }
        columnBits.setQuick(last, Numbers.encodeLowHighInts(flags, getHighAt(last)));
    }

    private static boolean isCompatibleCast(int from, int to) {
        if (isIPv4Cast(from, to)) {
            return true;
        }
        return castGroups.getQuick(ColumnType.tagOf(from)) == castGroups.getQuick(ColumnType.tagOf(to));
    }

    private static boolean isIPv4Cast(int from, int to) {
        return (from == ColumnType.STRING && to == ColumnType.IPv4) || (from == ColumnType.VARCHAR && to == ColumnType.IPv4);
    }

    private int getHighAt(int index) {
        return Numbers.decodeHighInt(columnBits.getQuick(index));
    }

    private int getLowAt(int index) {
        return Numbers.decodeLowInt(columnBits.getQuick(index));
    }

    static {
        castGroups.extendAndSet(ColumnType.BOOLEAN, 2);
        castGroups.extendAndSet(ColumnType.BYTE, 1);
        castGroups.extendAndSet(ColumnType.SHORT, 1);
        castGroups.extendAndSet(ColumnType.CHAR, 1);
        castGroups.extendAndSet(ColumnType.INT, 1);
        castGroups.extendAndSet(ColumnType.LONG, 1);
        castGroups.extendAndSet(ColumnType.FLOAT, 1);
        castGroups.extendAndSet(ColumnType.DOUBLE, 1);
        castGroups.extendAndSet(ColumnType.DATE, 1);
        castGroups.extendAndSet(ColumnType.TIMESTAMP, 1);
        castGroups.extendAndSet(ColumnType.STRING, 3);
        castGroups.extendAndSet(ColumnType.VARCHAR, 3);
        castGroups.extendAndSet(ColumnType.SYMBOL, 3);
        castGroups.extendAndSet(ColumnType.BINARY, 4);
    }
}
