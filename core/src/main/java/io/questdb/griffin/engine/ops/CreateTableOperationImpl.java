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
import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.ops.CreateTableOperationBuilderImpl.*;

public class CreateTableOperationImpl implements CreateTableOperation {
    // augmentedColumnMetadata contains information from "cast models", the extra syntax
    // to augment "create as select" semantic. The map is keyed on column names.
    //
    // One thing to note about this data is that it's only used for create-as-select.
    // This is because column types, capacities, and flags can be specified without
    // extra syntax. On the other hand, create-as-select does not use columnBits.
    // For create-as-select, we move the information from this map into columnBits after
    // column indexes are known. That is, after the "select" part is executed.
    // Note that we must not hard-map "cast" parameters to column indices. These indices
    // are liable to change every time "select" is recompiled, for example in case of
    // wildcard usage, e.g. create x as select * from y. When "y" changes, such as via
    // drop column, column indices will shift.
    private final LowerCaseCharSequenceObjHashMap<TableColumnMetadata> augmentedColumnMetadata = new LowerCaseCharSequenceObjHashMap<>();
    private final long batchO3MaxLag;
    private final long batchSize;
    private final LowerCaseCharSequenceIntHashMap colNameToCastClausePos = new LowerCaseCharSequenceIntHashMap();
    private final LowerCaseCharSequenceIntHashMap colNameToDedupClausePos = new LowerCaseCharSequenceIntHashMap();
    private final LowerCaseCharSequenceIntHashMap colNameToIndexClausePos = new LowerCaseCharSequenceIntHashMap();
    private final LongList columnBits = new LongList();
    private final ObjList<String> columnNames = new ObjList<>();
    private final CreateTableOperationFuture future = new CreateTableOperationFuture();
    private final boolean ignoreIfExists;
    private final String likeTableName;
    // position of the "like" table name in the SQL text, for error reporting
    private final int likeTableNamePosition;
    private final String selectText;
    private final String sqlText;
    private final String tableName;
    private final int tableNamePosition;
    private final String timestampColumnName;
    private final int timestampColumnNamePosition;
    private final String volumeAlias;
    private int defaultSymbolCapacity = -1;
    private int maxUncommittedRows;
    private long o3MaxLag;
    private int partitionBy;
    private RecordCursorFactory recordCursorFactory;
    private int timestampIndex = -1;
    private int ttlHoursOrMonths;
    private boolean walEnabled;

    public CreateTableOperationImpl(
            String sqlText,
            String tableName,
            int tableNamePosition,
            int partitionBy,
            String volumeAlias,
            String likeTableName,
            int likeTableNamePosition,
            boolean ignoreIfExists
    ) {
        this.sqlText = sqlText;
        this.tableName = tableName;
        this.tableNamePosition = tableNamePosition;
        this.partitionBy = partitionBy;
        this.volumeAlias = volumeAlias;
        this.likeTableName = likeTableName;
        this.likeTableNamePosition = likeTableNamePosition;
        this.ignoreIfExists = ignoreIfExists;
        this.selectText = null;
        this.timestampColumnName = null;
        this.timestampColumnNamePosition = 0;
        this.batchSize = 0;
        this.batchO3MaxLag = 0;
    }

    public CreateTableOperationImpl(
            String sqlText,
            String tableName,
            int tableNamePosition,
            int partitionBy,
            String volumeAlias,
            boolean ignoreIfExists,
            @Transient ObjList<CharSequence> columnNames,
            @Transient LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> createColumnModelMap,
            int timestampIndex,
            long o3MaxLag,
            int maxUncommittedRows,
            int ttlHoursOrMonths,
            boolean walEnabled
    ) {
        this.sqlText = sqlText;
        this.tableName = tableName;
        this.tableNamePosition = tableNamePosition;
        this.partitionBy = partitionBy;
        this.volumeAlias = volumeAlias;
        this.ignoreIfExists = ignoreIfExists;
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            CharSequence colName = columnNames.get(i);
            this.columnNames.add(Chars.toString(colName));
            CreateTableColumnModel model = createColumnModelMap.get(colName);
            addColumnBits(
                    model.getColumnType(),
                    model.getSymbolCacheFlag(),
                    model.getSymbolCapacity(),
                    model.isIndexed(),
                    model.getIndexValueBlockSize(),
                    model.isDedupKey()
            );
        }
        // this is a vanilla "create table" with fixed columns and fixed timestamp index
        this.timestampColumnName = null;
        this.timestampColumnNamePosition = 0;
        this.timestampIndex = timestampIndex;
        this.o3MaxLag = o3MaxLag;
        this.maxUncommittedRows = maxUncommittedRows;
        this.ttlHoursOrMonths = ttlHoursOrMonths;
        this.walEnabled = walEnabled;

        this.selectText = null;
        this.recordCursorFactory = null;
        this.likeTableName = null;
        this.likeTableNamePosition = -1;
        this.batchSize = 0;
        this.batchO3MaxLag = 0;
    }

    /**
     * Constructs operation for "create as select" only. The following considerations should be met:
     * - model validation must be dynamic, the operation is re-runnable and "select" part of the SQL is non-constant
     * - some column types and type attributes can be overridden
     * - data copy operation is involved and batching parameters must be provided
     *
     * @param sqlText                     text of the SQL, that includes "create table..."
     * @param tableName                   name of the table to be created
     * @param selectText                  text of the nested AS SELECT statement
     * @param tableNamePosition           the position of table name in user's input, it is used for error reporting
     * @param ignoreIfExists              "if exists" flag, table won't be created silently if it exists already
     * @param partitionBy                 partition type
     * @param timestampColumnName         designated timestamp column name
     * @param timestampColumnNamePosition designated timestamp column name in user's input
     * @param volumeAlias                 the name of the "volume" where table is created, volumes are use to create table on different physical disks
     * @param walEnabled                  WAL flag
     * @param defaultSymbolCapacity       the default symbol capacity value, usually comes from the configuration
     * @param maxUncommittedRows          max uncommitted rows for non-WAL tables, this is written to table's metadata to be used by ingress protocols
     * @param o3MaxLag                    o3 commit lag, another performance optimisation parameter for non-WAL tables.
     * @param recordCursorFactory         the factory for the "select" part of the "create as select" SQL
     * @param createColumnModelMap        maps that contains type casts and additional index flags
     * @param batchSize                   number of rows in commit batch when data is moved from the select into the
     *                                    new table. Special value of -1 means "atomic" commit. This corresponds to "batch" keyword on the SQL.
     * @param batchO3MaxLag               lag windows in rows, which helps timestamp ordering code to smooth out timestamp jitter
     * @throws SqlException is throw in case of validation errors
     */
    public CreateTableOperationImpl(
            String sqlText,
            String tableName,
            String selectText,
            int tableNamePosition,
            boolean ignoreIfExists,
            int partitionBy,
            String timestampColumnName,
            int timestampColumnNamePosition,
            String volumeAlias,
            int ttlHoursOrMonths,
            boolean walEnabled,
            int defaultSymbolCapacity,
            int maxUncommittedRows,
            long o3MaxLag,
            RecordCursorFactory recordCursorFactory,
            @Transient LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> createColumnModelMap,
            long batchSize,
            long batchO3MaxLag
    ) throws SqlException {
        this.sqlText = sqlText;
        this.tableName = tableName;
        this.selectText = selectText;
        this.tableNamePosition = tableNamePosition;
        this.partitionBy = partitionBy;
        this.volumeAlias = volumeAlias;
        this.ignoreIfExists = ignoreIfExists;
        this.timestampColumnName = timestampColumnName;
        this.timestampColumnNamePosition = timestampColumnNamePosition;
        this.ttlHoursOrMonths = ttlHoursOrMonths;
        this.defaultSymbolCapacity = defaultSymbolCapacity;
        this.recordCursorFactory = recordCursorFactory;
        this.batchSize = batchSize;
        this.batchO3MaxLag = batchO3MaxLag;
        this.o3MaxLag = o3MaxLag;
        this.maxUncommittedRows = maxUncommittedRows;
        this.walEnabled = walEnabled;

        this.likeTableName = null;
        this.likeTableNamePosition = -1;

        // This constructor is for a "create as select", column names will be scraped from the record
        // cursor at runtime. Column augmentation data comes from the following sources in the SQL:
        // - cast models, provides column types
        // - (symbol) column index data, e.g. index flag and index capacity
        // - (symbol) column cache flag
        assert columnNames.size() == 0;
        assert columnBits.size() == 0;
        ObjList<CharSequence> colNames = createColumnModelMap.keys();
        for (int i = 0, n = colNames.size(); i < n; i++) {
            CharSequence columnName = colNames.get(i);
            CreateTableColumnModel model = createColumnModelMap.get(columnName);
            if (model.isIndexed() && model.getColumnType() != ColumnType.SYMBOL) {
                throw SqlException
                        .$(model.getIndexColumnPos(), "indexes are supported only for SYMBOL columns: ")
                        .put(columnName);
            }
            String columnNameStr = Chars.toString(columnName);
            int symbolCapacity = model.getSymbolCapacity();
            if (symbolCapacity == -1) {
                symbolCapacity = defaultSymbolCapacity;
            }
            if (model.isDedupKey()) {
                colNameToDedupClausePos.put(columnName, model.getDedupColumnPos());
            }
            if (model.isIndexed()) {
                colNameToIndexClausePos.put(columnName, model.getIndexColumnPos());
            }
            if (model.isCast()) {
                colNameToCastClausePos.put(columnName, model.getColumnNamePos());
            }
            TableColumnMetadata tcm = new TableColumnMetadata(
                    columnNameStr,
                    model.getColumnType(),
                    model.isIndexed(),
                    model.getIndexValueBlockSize(),
                    true,
                    null,
                    -1, // writer index is irrelevant here
                    model.isDedupKey(),
                    -1, // replacingIndex is irrelevant here
                    model.getSymbolCacheFlag(),
                    symbolCapacity
            );
            augmentedColumnMetadata.put(columnNameStr, tcm);
        }
    }

    @Override
    public void close() {
        recordCursorFactory = Misc.free(recordCursorFactory);
    }

    @Override
    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = sqlExecutionContext.getCairoEngine().getSqlCompiler()) {
            compiler.execute(this, sqlExecutionContext);
        }
        return future;
    }

    @Override
    public long getBatchO3MaxLag() {
        return batchO3MaxLag;
    }

    @Override
    public long getBatchSize() {
        return batchSize;
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
    public CharSequence getLikeTableName() {
        return likeTableName;
    }

    @Override
    public int getLikeTableNamePosition() {
        return likeTableNamePosition;
    }

    @Override
    public int getMaxUncommittedRows() {
        return maxUncommittedRows;
    }

    @Override
    public long getMetadataVersion() {
        // new table only
        return 0;
    }

    @Override
    public long getO3MaxLag() {
        return o3MaxLag;
    }

    @Override
    public int getOperationCode() {
        return OperationCodes.CREATE_TABLE;
    }

    @Override
    public OperationFuture getOperationFuture() {
        return future;
    }

    @Override
    public int getPartitionBy() {
        return partitionBy;
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        return recordCursorFactory;
    }

    @Override
    public String getSelectText() {
        return selectText;
    }

    @Override
    public String getSqlText() {
        return sqlText;
    }

    @Override
    public boolean getSymbolCacheFlag(int index) {
        return (getLowAt(index * 2 + 1) & COLUMN_FLAG_CACHED) != 0;
    }

    @Override
    public int getSymbolCapacity(int index) {
        int capacity = getHighAt(index * 2);
        assert capacity != -1 : "Symbol capacity is not set";
        return capacity;
    }

    @Override
    public CharSequence getTableName() {
        return tableName;
    }

    @Override
    public int getTableNamePosition() {
        return tableNamePosition;
    }

    @Override
    public int getTimestampIndex() {
        return timestampIndex;
    }

    @Override
    public int getTtlHoursOrMonths() {
        return ttlHoursOrMonths;
    }

    @Override
    public CharSequence getVolumeAlias() {
        return volumeAlias;
    }

    @Override
    public boolean ignoreIfExists() {
        return ignoreIfExists;
    }

    @Override
    public boolean isDedupKey(int index) {
        return (getLowAt(index * 2 + 1) & COLUMN_FLAG_DEDUP_KEY) != 0;
    }

    @Override
    public boolean isIndexed(int index) {
        return (getLowAt(index * 2 + 1) & COLUMN_FLAG_INDEXED) != 0;
    }

    @Override
    public boolean isWalEnabled() {
        return walEnabled;
    }

    @Override
    public void updateFromLikeTableMetadata(TableMetadata likeTableMetadata) {
        this.maxUncommittedRows = likeTableMetadata.getMaxUncommittedRows();
        this.o3MaxLag = likeTableMetadata.getO3MaxLag();
        this.partitionBy = likeTableMetadata.getPartitionBy();
        this.timestampIndex = likeTableMetadata.getTimestampIndex();
        this.walEnabled = likeTableMetadata.isWalEnabled();
        this.ttlHoursOrMonths = likeTableMetadata.getTtlHoursOrMonths();
        columnNames.clear();
        columnBits.clear();
        for (int i = 0; i < likeTableMetadata.getColumnCount(); i++) {
            TableColumnMetadata colMeta = likeTableMetadata.getColumnMetadata(i);
            addColumnBits(
                    colMeta.getColumnType(),
                    colMeta.isSymbolCacheFlag(),
                    colMeta.getSymbolCapacity(),
                    colMeta.isSymbolIndexFlag(),
                    colMeta.getIndexValueBlockCapacity(),
                    colMeta.isDedupKeyFlag()
            );
            columnNames.add(colMeta.getColumnName());
        }
    }

    /**
     * SQLCompiler side API to set affected rows count after the operation has been executed.
     *
     * @param affectedRowsCount the number of rows inserted in the table after it has been created. Typically,
     *                          this is 0 or the number of rows from "create as select"
     */
    @Override
    public void updateOperationFutureAffectedRowsCount(long affectedRowsCount) {
        future.of(affectedRowsCount);
    }

    /**
     * This is SQLCompiler side API to set table token after the operation has been executed.
     *
     * @param tableToken table token of the newly created table
     */
    @Override
    public void updateOperationFutureTableToken(TableToken tableToken) {
        future.tableToken = tableToken;
    }

    @Override
    public void validateAndUpdateMetadataFromSelect(RecordMetadata metadata) throws SqlException {
        // This method must only be called in case of "create-as-select".
        // Here we remap data keyed on column names (from cast maps) to
        // data keyed on column index. We assume that "columnBits" are free to use
        // in case of "create-as-select" because they don't capture any useful data
        // at SQL parse time.
        columnBits.clear();
        if (timestampColumnName == null) {
            timestampIndex = metadata.getTimestampIndex();
        } else {
            timestampIndex = metadata.getColumnIndexQuiet(timestampColumnName);
            if (timestampIndex == -1) {
                throw SqlException.position(timestampColumnNamePosition)
                        .put("designated timestamp column doesn't exist [name=").put(timestampColumnName).put(']');
            }
            int timestampColType = metadata.getColumnType(timestampIndex);
            if (timestampColType != ColumnType.TIMESTAMP) {
                throw SqlException.position(timestampColumnNamePosition)
                        .put("TIMESTAMP column expected [actual=").put(ColumnType.nameOf(timestampColType)).put(']');
            }
        }
        ObjList<CharSequence> castColNames = colNameToCastClausePos.keys();
        for (int i = 0, n = castColNames.size(); i < n; i++) {
            CharSequence castColName = castColNames.get(i);
            if (metadata.getColumnIndexQuiet(castColName) < 0) {
                throw SqlException.position(colNameToCastClausePos.get(castColName))
                        .put("CAST column doesn't exist [column=").put(castColName).put(']');
            }
        }
        ObjList<CharSequence> indexColNames = colNameToIndexClausePos.keys();
        for (int i = 0, n = indexColNames.size(); i < n; i++) {
            CharSequence indexedColName = indexColNames.get(i);
            if (metadata.getColumnIndexQuiet(indexedColName) < 0) {
                throw SqlException.position(colNameToIndexClausePos.get(indexedColName))
                        .put("INDEX column doesn't exist [column=").put(indexedColName).put(']');
            }
        }
        ObjList<CharSequence> dedupColNames = colNameToDedupClausePos.keys();
        for (int i = 0, n = dedupColNames.size(); i < n; i++) {
            CharSequence dedupColName = dedupColNames.get(i);
            if (metadata.getColumnIndexQuiet(dedupColName) < 0) {
                throw SqlException.position(colNameToDedupClausePos.get(dedupColName))
                        .put("DEDUP column doesn't exist [column=").put(dedupColName).put(']');
            }
        }
        columnNames.clear();
        boolean hasDedup = false;
        boolean isTimestampDeduped = false;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            String columnName = metadata.getColumnName(i);
            TableColumnMetadata augMeta = augmentedColumnMetadata.get(columnName);

            int columnType;
            int symbolCapacity;
            boolean symbolCacheFlag;
            boolean symbolIndexed;
            boolean isDedupKey;
            int indexBlockCapacity;
            if (augMeta != null) {
                columnType = augMeta.getColumnType();
                int fromType = metadata.getColumnType(i);
                if (!isCompatibleCast(fromType, columnType)) {
                    throw SqlException.unsupportedCast(
                            colNameToCastClausePos.get(columnName), columnName, fromType, columnType);
                }
                symbolCapacity = augMeta.getSymbolCapacity();
                symbolCacheFlag = augMeta.isSymbolCacheFlag();
                symbolIndexed = augMeta.isSymbolIndexFlag();
                isDedupKey = augMeta.isDedupKeyFlag();
                indexBlockCapacity = augMeta.getIndexValueBlockCapacity();
            } else {
                columnType = metadata.getColumnType(i);
                if (ColumnType.isNull(columnType)) {
                    throw SqlException
                            .$(0, "cannot create NULL-type column, please use type cast, e.g. ")
                            .put(columnName).put("::").put("type");
                }
                symbolCapacity = defaultSymbolCapacity;
                symbolCacheFlag = true;
                symbolIndexed = false;
                isDedupKey = false;
                indexBlockCapacity = 0;
            }

            if (!ColumnType.isSymbol(columnType) && symbolIndexed) {
                throw SqlException.$(0, "indexes are supported only for SYMBOL columns: ").put(columnName);
            }
            if (isDedupKey) {
                hasDedup = true;
                if (i == timestampIndex) {
                    isTimestampDeduped = true;
                }
            }
            columnNames.add(columnName);
            addColumnBits(
                    columnType,
                    symbolCacheFlag,
                    symbolCapacity,
                    symbolIndexed,
                    indexBlockCapacity,
                    isDedupKey
            );
        }
        if (hasDedup && !isTimestampDeduped) {
            // Report the error's position in SQL as the position of the first column in the DEDUP list
            int firstDedupColumnPos = Integer.MAX_VALUE;
            for (int i = 0, n = dedupColNames.size(); i < n; i++) {
                int dedupColPos = colNameToDedupClausePos.get(dedupColNames.get(i));
                if (firstDedupColumnPos > dedupColPos) {
                    firstDedupColumnPos = dedupColPos;
                }
            }
            throw SqlException.position(firstDedupColumnPos)
                    .put("deduplicate key list must include dedicated timestamp column");
        }
    }

    private void addColumnBits(
            int columnType,
            boolean symbolCacheFlag,
            int symbolCapacity,
            boolean indexFlag,
            int indexBlockCapacity,
            boolean dedupFlag
    ) {
        int flags = (symbolCacheFlag ? COLUMN_FLAG_CACHED : 0)
                | (indexFlag ? COLUMN_FLAG_INDEXED : 0)
                | (dedupFlag ? COLUMN_FLAG_DEDUP_KEY : 0);
        columnBits.add(
                Numbers.encodeLowHighInts(columnType, symbolCapacity),
                Numbers.encodeLowHighInts(flags, indexBlockCapacity)
        );
    }

    private int getHighAt(int index) {
        return Numbers.decodeHighInt(columnBits.getQuick(index));
    }

    private int getLowAt(int index) {
        return Numbers.decodeLowInt(columnBits.getQuick(index));
    }

}
