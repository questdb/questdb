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
import io.questdb.griffin.model.ColumnCastModel;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
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
    // For create-as-select SQL, parser does not populate this list. Instead the list
    // is used at the execution time to capture the attributes of columns of the "select" SQL.
    private final LongList columnBits = new LongList();
    private final CharSequenceObjHashMap<ColumnCastModel> columnCastModels = new CharSequenceObjHashMap<>();
    private final LowerCaseCharSequenceIntHashMap columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    private final ObjList<String> columnNames = new ObjList<>();
    private final CharSequenceIntHashMap createAsSelectIndexCapacities = new CharSequenceIntHashMap();
    private final CharSequenceIntHashMap createAsSelectIndexColumnNamePositions = new CharSequenceIntHashMap();
    // todo: perhaps consolidate both these maps into one str-to-long?
    private final CharSequenceBoolHashMap createAsSelectIndexFlags = new CharSequenceBoolHashMap();
    private final IntIntHashMap typeCasts = new IntIntHashMap();
    private long batchO3MaxLag = -1;
    private long batchSize = -1;
    private boolean ignoreIfExists = false;
    private ExpressionNode likeTableName;
    private int maxUncommittedRows;
    private long o3MaxLag;
    private ExpressionNode partitionBy;
    private QueryModel queryModel;
    private RecordCursorFactory recordCursorFactory;
    private ExpressionNode tableNameExpr;
    private ExpressionNode timestamp;
    private CharSequence volumeAlias;
    private boolean walEnabled;

    public CreateTableOperationBuilder() {
    }

    public void addColumn(int columnPosition, CharSequence columnName, int columnType, int symbolCapacity) throws SqlException {
        if (!columnNameIndexMap.put(columnName, columnNames.size())) {
            throw SqlException.duplicateColumn(columnPosition, columnName);
        }
        // todo: columnNames need not to be strings, they can be made
        //       strings when they are copied to the operation
        columnNames.add(Chars.toString(columnName));
        columnBits.add(
                Numbers.encodeLowHighInts(columnType, symbolCapacity),
                Numbers.encodeLowHighInts(COLUMN_FLAG_CACHED, 0)
        );
    }

    public boolean addColumnCastModel(ColumnCastModel model) {
        return columnCastModels.put(model.getColumnName().token, model);
    }

    public CreateTableOperation build(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (queryModel != null) {
            setFactory(
                    compiler.generateSelectWithRetries(
                            queryModel,
                            sqlExecutionContext,
                            false
                    )
            );
        }

        // create table "like" and "as select" are mutually exclusive
        // todo: write a test for the above to ensure correctness of the diagnostic message
        final String likeTableName;
        final int likeTableNamePos;
        if (this.likeTableName != null) {
            TableToken likeTableNameToken = compiler.getEngine().getTableTokenIfExists(this.likeTableName.token);
            if (likeTableNameToken == null) {
                throw SqlException.tableDoesNotExist(this.likeTableName.position, this.likeTableName.token);
            }
            likeTableName = likeTableNameToken.getTableName();
            likeTableNamePos = this.likeTableName.position;
        } else {
            likeTableName = null;
            likeTableNamePos = -1;
        }

        return new CreateTableOperation(
                Chars.toString(tableNameExpr.token),
                tableNameExpr.position,
                columnNames,
                columnBits,
                getTimestampIndex(),
                getPartitionBy(),
                ignoreIfExists,
                likeTableName,
                likeTableNamePos,
                recordCursorFactory,
                batchSize,
                batchO3MaxLag,
                o3MaxLag,
                maxUncommittedRows,
                Chars.toString(volumeAlias),
                walEnabled,
                columnCastModels,
                createAsSelectIndexColumnNamePositions,
                createAsSelectIndexFlags,
                createAsSelectIndexCapacities
        );
    }

    @Override
    public void clear() {
        typeCasts.clear();
        columnCastModels.clear();
        queryModel = null;
        timestamp = null;
        partitionBy = null;
        likeTableName = null;
        tableNameExpr = null;
        volumeAlias = null;
        columnBits.clear();
        columnNames.clear();
        columnNameIndexMap.clear();
        ignoreIfExists = false;
        o3MaxLag = -1;
        batchO3MaxLag = -1;
        batchSize = -1;
        createAsSelectIndexFlags.clear();
        createAsSelectIndexCapacities.clear();
        createAsSelectIndexColumnNamePositions.clear();
    }

    public CharSequenceObjHashMap<ColumnCastModel> getColumnCastModels() {
        return columnCastModels;
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

    public int getPartitionBy() {
        return partitionBy == null ? PartitionBy.NONE : PartitionBy.fromString(partitionBy.token);
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

    public ExpressionNode getTimestamp() {
        return timestamp;
    }

    public int getTimestampIndex() {
        return timestamp != null ? getColumnIndex(timestamp.token) : -1;
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

    public void setCreateAsSelectIndexFlag(CharSequence columnName, int columnNamePosition, boolean indexFlag, int indexValueBlockSize) {
        String columnNameStr = Chars.toString(columnName);
        createAsSelectIndexFlags.put(columnNameStr, indexFlag);
        createAsSelectIndexCapacities.put(columnNameStr, indexValueBlockSize);
        createAsSelectIndexColumnNamePositions.put(columnNameStr, columnNamePosition);
    }

    public void setDedupKeyFlag(int index) {
        int flagsIndex = index * 2 + 1;
        int flags = getLowAt(flagsIndex) | COLUMN_FLAG_DEDUP_KEY;
        columnBits.setQuick(flagsIndex, Numbers.encodeLowHighInts(flags, getHighAt(flagsIndex)));
    }

    public void setFactory(RecordCursorFactory factory) throws SqlException {
        this.recordCursorFactory = factory;
        final RecordMetadata metadata = factory.getMetadata();
        CharSequenceObjHashMap<ColumnCastModel> castModels = columnCastModels;
        ObjList<CharSequence> castColumnNames = castModels.keys();

        for (int i = 0, n = castColumnNames.size(); i < n; i++) {
            CharSequence columnName = castColumnNames.getQuick(i);
            int index = metadata.getColumnIndexQuiet(columnName);
            ColumnCastModel ccm = castModels.get(columnName);
            // the only reason why columns cannot be found at this stage is
            // concurrent table modification of table structure
            if (index == -1) {
                // Cast isn't going to go away when we reparse SQL. We must make this
                // permanent error
                throw SqlException.invalidColumn(ccm.getColumnNamePos(), columnName);
            }
            int from = metadata.getColumnType(index);
            int to = ccm.getColumnType();
            if (isCompatibleCase(from, to)) {
                int modelColumnIndex = getColumnIndex(columnName);
                if (!ColumnType.isSymbol(to) && isIndexed(modelColumnIndex)) {
                    throw SqlException.$(ccm.getColumnTypePos(), "indexes are supported only for SYMBOL columns: ").put(columnName);
                }
                typeCasts.put(index, to);
            } else {
                throw SqlException.unsupportedCast(ccm.getColumnTypePos(), columnName, from, to);
            }
        }
    }

    public void setIgnoreIfExists(boolean flag) {
        this.ignoreIfExists = flag;
    }

    public void setLikeTableName(ExpressionNode tableName) {
        this.likeTableName = tableName;
    }

    public void setMaxUncommittedRows(int maxUncommittedRows) {
        this.maxUncommittedRows = maxUncommittedRows;
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

    public void setTableNameExpr(ExpressionNode tableNameExpr) {
        this.tableNameExpr = tableNameExpr;
    }

    public void setTimestamp(ExpressionNode timestamp) {
        this.timestamp = timestamp;
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
            final ObjList<CharSequence> castColumns = getColumnCastModels().keys();
            for (int i = 0, n = castColumns.size(); i < n; i++) {
                final CharSequence column = castColumns.getQuick(i);
                final ColumnCastModel m = getColumnCastModels().get(column);
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
            if (likeTableName != null) {
                sink.putAscii("like ");
                sink.put(likeTableName.token);
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

        if (getTimestamp() != null) {
            sink.putAscii(" timestamp(");
            sink.put(getTimestamp().token);
            sink.putAscii(')');
        }

        if (partitionBy != null) {
            sink.putAscii(" partition by ").put(partitionBy.token);
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

    private static boolean isCompatibleCase(int from, int to) {
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
