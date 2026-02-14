/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.TableUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.table.ShowCreateTableRecordCursorFactory.ttlToSink;

public class CreateTableOperationBuilderImpl implements CreateTableOperationBuilder, Mutable {
    private static final IntList castGroups = new IntList();
    private final LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> columnModels = new LowerCaseCharSequenceObjHashMap<>();
    private final LowerCaseCharSequenceIntHashMap columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private long batchO3MaxLag = -1;
    private long batchSize = -1;
    private int defaultSymbolCapacity;
    private boolean ignoreIfExists = false;
    private ExpressionNode likeTableNameExpr;
    private int maxUncommittedRows;
    private long o3MaxLag = -1;
    private ExpressionNode partitionByExpr;
    // transient field, unoptimized AS SELECT model, used in toSink()
    private QueryModel selectModel;
    private CharSequence selectText;
    private int selectTextPosition;
    private int tableKind = TableUtils.TABLE_KIND_REGULAR_TABLE;
    private ExpressionNode tableNameExpr;
    private ExpressionNode timestampExpr;
    private int ttlHoursOrMonths;
    private int ttlPosition;
    private CharSequence volumeAlias;
    private int volumePosition;
    private boolean walEnabled;

    public void addColumnModel(CharSequence columnName, CreateTableColumnModel model) throws SqlException {
        if (columnModels.get(columnName) != null) {
            throw SqlException.duplicateColumn(model.getColumnNamePos(), columnName);
        }
        columnNameIndexMap.put(columnName, columnModels.size());
        columnModels.put(columnName, model);
        columnNames.add(columnName);
    }

    @Override
    public CreateTableOperationImpl build(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            CharSequence sqlText
    ) throws SqlException {
        if (selectText != null) {
            return new CreateTableOperationImpl(
                    Chars.toString(sqlText),
                    Chars.toString(tableNameExpr.token),
                    tableNameExpr.position,
                    Chars.toString(selectText),
                    selectTextPosition,
                    ignoreIfExists,
                    getPartitionByFromExpr(),
                    partitionByExpr == null ? 0 : partitionByExpr.position,
                    timestampExpr != null ? Chars.toString(timestampExpr.token) : null,
                    timestampExpr != null ? timestampExpr.position : 0,
                    Chars.toString(volumeAlias),
                    volumePosition,
                    ttlHoursOrMonths,
                    ttlPosition,
                    walEnabled,
                    defaultSymbolCapacity,
                    maxUncommittedRows,
                    o3MaxLag,
                    columnModels,
                    batchSize,
                    batchO3MaxLag,
                    tableKind
            );
        }

        if (likeTableNameExpr != null) {
            TableToken likeTableNameToken = compiler.getEngine().getTableTokenIfExists(likeTableNameExpr.token);
            if (likeTableNameToken == null) {
                throw SqlException.tableDoesNotExist(likeTableNameExpr.position, likeTableNameExpr.token);
            }
            return new CreateTableOperationImpl(
                    Chars.toString(sqlText),
                    Chars.toString(tableNameExpr.token),
                    tableNameExpr.position,
                    getPartitionByFromExpr(),
                    partitionByExpr == null ? 0 : partitionByExpr.position,
                    Chars.toString(volumeAlias),
                    volumePosition,
                    likeTableNameToken.getTableName(),
                    likeTableNameExpr.position,
                    ignoreIfExists
            );
        }

        return new CreateTableOperationImpl(
                Chars.toString(sqlText),
                Chars.toString(tableNameExpr.token),
                tableNameExpr.position,
                getPartitionByFromExpr(),
                partitionByExpr == null ? 0 : partitionByExpr.position,
                Chars.toString(volumeAlias),
                volumePosition,
                ignoreIfExists,
                columnNames,
                columnModels,
                getTimestampIndex(),
                o3MaxLag,
                maxUncommittedRows,
                ttlHoursOrMonths,
                ttlPosition,
                walEnabled
        );
    }

    @Override
    public void clear() {
        columnNameIndexMap.clear();
        columnNames.clear();
        columnModels.clear();
        batchO3MaxLag = -1;
        batchSize = -1;
        defaultSymbolCapacity = 0;
        ignoreIfExists = false;
        likeTableNameExpr = null;
        maxUncommittedRows = 0;
        o3MaxLag = -1;
        partitionByExpr = null;
        tableNameExpr = null;
        timestampExpr = null;
        selectText = null;
        selectTextPosition = 0;
        selectModel = null;
        volumeAlias = null;
        volumePosition = 0;
        ttlHoursOrMonths = 0;
        ttlPosition = 0;
        walEnabled = false;
        tableKind = TableUtils.TABLE_KIND_REGULAR_TABLE;
    }

    public int getColumnCount() {
        return columnNames.size();
    }

    public int getColumnIndex(CharSequence columnName) {
        return columnNameIndexMap.get(columnName);
    }

    public @Nullable CreateTableColumnModel getColumnModel(CharSequence columnName) {
        return columnModels.get(columnName);
    }

    public CharSequence getColumnName(int index) {
        return columnNames.get(index);
    }

    public int getPartitionByFromExpr() {
        return partitionByExpr == null ? PartitionBy.NONE : PartitionBy.fromString(partitionByExpr.token);
    }

    @Override
    public QueryModel getQueryModel() {
        return selectModel;
    }

    public CharSequence getSelectText() {
        return selectText;
    }

    @Override
    public CharSequence getTableName() {
        return tableNameExpr.token;
    }

    @Override
    public ExpressionNode getTableNameExpr() {
        return tableNameExpr;
    }

    public ExpressionNode getTimestampExpr() {
        return timestampExpr;
    }

    public int getTimestampIndex() {
        return timestampExpr != null ? getColumnIndex(timestampExpr.token) : -1;
    }

    public int getTtlHoursOrMonths() {
        return ttlHoursOrMonths;
    }

    public CharSequence getVolumeAlias() {
        return volumeAlias;
    }

    public boolean isAtomic() {
        return batchSize == -1;
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

    public void setDefaultSymbolCapacity(int defaultSymbolCapacity) {
        this.defaultSymbolCapacity = defaultSymbolCapacity;
    }

    public void setIgnoreIfExists(boolean flag) {
        this.ignoreIfExists = flag;
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

    @Override
    public void setSelectModel(QueryModel selectModel) {
        this.selectModel = selectModel;
    }

    public void setSelectText(CharSequence selectText, int selectTextPosition) {
        this.selectText = selectText;
        this.selectTextPosition = selectTextPosition;
    }

    public void setTableNameExpr(ExpressionNode expr) {
        this.tableNameExpr = expr;
    }

    public void setTimestampExpr(ExpressionNode expr) {
        this.timestampExpr = expr;
    }

    public void setTtlHoursOrMonths(int ttlHoursOrMonths) {
        this.ttlHoursOrMonths = ttlHoursOrMonths;
    }

    public void setTtlPosition(int ttlPosition) {
        this.ttlPosition = ttlPosition;
    }

    public void setVolumeAlias(CharSequence volumeAlias, int volumePosition) {
        // set if the "create table" statement contains IN VOLUME 'volumeAlias'.
        // volumePath will be resolved by the compiler
        this.volumeAlias = Chars.toString(volumeAlias);
        this.volumePosition = volumePosition;
    }

    public void setWalEnabled(boolean walEnabled) {
        this.walEnabled = walEnabled;
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
        if (selectModel != null) {
            sink.putAscii(" as (");
            selectModel.toSink(sink);
            sink.putAscii(')');
            final ObjList<CharSequence> castColumns = columnModels.keys();
            for (int i = 0, n = castColumns.size(); i < n; i++) {
                final CharSequence column = castColumns.getQuick(i);
                final CreateTableColumnModel model = columnModels.get(column);
                final int type = model.getColumnType();
                if (type > 0) {
                    sink.putAscii(", cast(");
                    sink.put(column);
                    sink.putAscii(" as ");
                    sink.put(ColumnType.nameOf(type));
                    sink.putAscii(':');
                    sink.put(model.getColumnTypePos());
                    if (ColumnType.isSymbol(type)) {
                        symbolClauseToSink(sink, model);
                    }
                } else if (model.isIndexed()) {
                    sink.putAscii(", index(").put(column);
                    sink.putAscii(" capacity ");
                    sink.put(model.getIndexValueBlockSize());
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
                    CharSequence columnName = columnNames.getQuick(i);
                    CreateTableColumnModel model = columnModels.get(columnName);
                    sink.put(columnName);
                    sink.putAscii(' ');
                    sink.put(ColumnType.nameOf(model.getColumnType()));
                    if (ColumnType.isSymbol(model.getColumnType())) {
                        symbolClauseToSink(sink, model);
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
        ttlToSink(ttlHoursOrMonths, sink);
        if (volumeAlias != null) {
            sink.putAscii(" in volume '").put(volumeAlias).putAscii('\'');
        }
    }

    private static boolean isIPv4Cast(int from, int to) {
        return (from == ColumnType.STRING && to == ColumnType.IPv4) || (from == ColumnType.VARCHAR && to == ColumnType.IPv4);
    }

    private static void symbolClauseToSink(@NotNull CharSink<?> sink, CreateTableColumnModel model) {
        sink.putAscii(" capacity ");
        sink.put(model.getSymbolCapacity());
        if (model.getSymbolCacheFlag()) {
            sink.putAscii(" cache");
        } else {
            sink.putAscii(" nocache");
        }
        if (model.isIndexed()) {
            sink.putAscii(" index capacity ");
            sink.put(model.getIndexValueBlockSize());
        }
    }

    static boolean isCompatibleCast(int from, int to) {
        if (from == to || isIPv4Cast(from, to)) {
            return true;
        }
        if (ColumnType.tagOf(from) == ColumnType.LONG && ColumnType.isStringyType(to)) {
            return true;
        }
        return castGroups.getQuick(ColumnType.tagOf(from)) == castGroups.getQuick(ColumnType.tagOf(to));
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
