/*+*****************************************************************************
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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.CopyDataProgressReporter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
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
    private final LowerCaseCharSequenceObjHashMap<ObjList<CharSequence>> augmentedCoveringNames = new LowerCaseCharSequenceObjHashMap<>();
    private final boolean autoIncludeTimestamp;
    private final LowerCaseCharSequenceIntHashMap colNameToCastClausePos = new LowerCaseCharSequenceIntHashMap();
    private final LowerCaseCharSequenceIntHashMap colNameToDedupClausePos = new LowerCaseCharSequenceIntHashMap();
    private final LowerCaseCharSequenceIntHashMap colNameToIndexClausePos = new LowerCaseCharSequenceIntHashMap();
    private final LongList columnBits = new LongList();
    private final ObjList<String> columnNames = new ObjList<>();
    private final ObjList<IntList> coveringColumnIndicesList = new ObjList<>();
    private final CreateTableOperationFuture future = new CreateTableOperationFuture();
    private final IntList parquetEncodingConfigs = new IntList();
    private final String selectText;
    private final String sqlText;
    private long batchO3MaxLag;
    private long batchSize;
    private int defaultSymbolCapacity = -1;
    private boolean ignoreIfExists;
    private String likeTableName;
    // position of the "like" table name in the SQL text, for error reporting
    private int likeTableNamePosition;
    private int maxUncommittedRows;
    private boolean needRegister = true;
    private long o3MaxLag;
    private int partitionBy;
    private int partitionByPosition;
    private CopyDataProgressReporter reporter = CopyDataProgressReporter.NOOP;
    private int selectSqlScanDirection;
    private int selectTextPosition;
    private int tableKind = TableUtils.TABLE_KIND_REGULAR_TABLE;
    private String tableName;
    private int tableNamePosition;
    private String timestampColumnName;
    private int timestampColumnNamePosition;
    private int timestampIndex = -1;
    private int timestampType;
    private int ttlHoursOrMonths;
    private int ttlPosition;
    private String volumeAlias;
    private int volumePosition;
    private boolean walEnabled;

    public CreateTableOperationImpl(
            String selectText,
            String tableName,
            int partitionBy,
            boolean walEnabled,
            int defaultSymbolCapacity,
            String sqlText,
            boolean needRegister
    ) {
        this.autoIncludeTimestamp = false;
        this.selectText = selectText;
        this.tableName = tableName;
        this.partitionBy = partitionBy;
        this.defaultSymbolCapacity = defaultSymbolCapacity;
        this.walEnabled = walEnabled;
        this.sqlText = sqlText;
        this.needRegister = needRegister;
    }

    public CreateTableOperationImpl(
            @NotNull String sqlText,
            @NotNull String tableName,
            int tableNamePosition,
            int partitionBy,
            int partitionByPosition,
            @Nullable String volumeAlias,
            int volumePosition,
            @Nullable String likeTableName,
            int likeTableNamePosition,
            boolean ignoreIfExists
    ) {
        this.sqlText = sqlText;
        this.tableName = tableName;
        this.tableNamePosition = tableNamePosition;
        this.partitionBy = partitionBy;
        this.partitionByPosition = partitionByPosition;
        this.volumeAlias = volumeAlias;
        this.volumePosition = volumePosition;
        this.autoIncludeTimestamp = false;
        this.likeTableName = likeTableName;
        this.likeTableNamePosition = likeTableNamePosition;
        this.ignoreIfExists = ignoreIfExists;
        this.selectText = null;
        this.selectTextPosition = 0;
        this.timestampColumnName = null;
        this.timestampColumnNamePosition = 0;
        this.batchSize = 0;
        this.batchO3MaxLag = 0;
    }

    public CreateTableOperationImpl(
            @NotNull String sqlText,
            @NotNull String tableName,
            int tableNamePosition,
            int partitionBy,
            int partitionByPosition,
            @Nullable String volumeAlias,
            int volumePosition,
            boolean ignoreIfExists,
            @Transient ObjList<CharSequence> columnNames,
            @Transient LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> createColumnModelMap,
            int timestampIndex,
            long o3MaxLag,
            int maxUncommittedRows,
            int ttlHoursOrMonths,
            int ttlPosition,
            boolean walEnabled,
            boolean autoIncludeTimestamp
    ) throws SqlException {
        this.autoIncludeTimestamp = autoIncludeTimestamp;
        this.sqlText = sqlText;
        this.tableName = tableName;
        this.tableNamePosition = tableNamePosition;
        this.partitionBy = partitionBy;
        this.volumeAlias = volumeAlias;
        this.volumePosition = volumePosition;
        this.ignoreIfExists = ignoreIfExists;
        this.partitionByPosition = partitionByPosition;
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            CharSequence colName = columnNames.get(i);
            this.columnNames.add(Chars.toString(colName));
            CreateTableColumnModel model = createColumnModelMap.get(colName);
            ObjList<CharSequence> coverNames = model.getCoveringColumnNames();
            // Set the COVERING flag whenever resolveCoveringColumnIndices
            // will produce a non-empty list — that is, either the user gave
            // an INCLUDE clause, or auto-include applies (POSTING index on
            // a column other than the designated timestamp).
            boolean hasCovering = coverNames.size() > 0
                    || (autoIncludeTimestamp && timestampIndex >= 0 && i != timestampIndex
                    && IndexType.isPosting(model.getIndexType()));
            addColumnBits(
                    model.getColumnType(),
                    model.getSymbolCacheFlag(),
                    model.getSymbolCapacity(),
                    model.getIndexType(),
                    model.getIndexValueBlockSize(),
                    model.isDedupKey(),
                    hasCovering,
                    model.getParquetEncodingConfig()
            );
        }
        resolveCoveringColumnIndices(columnNames, createColumnModelMap, timestampIndex);
        // this is a vanilla "create table" with fixed columns and fixed timestamp index
        this.timestampColumnName = null;
        this.timestampColumnNamePosition = 0;
        this.timestampIndex = timestampIndex;
        this.o3MaxLag = o3MaxLag;
        this.maxUncommittedRows = maxUncommittedRows;
        this.ttlHoursOrMonths = ttlHoursOrMonths;
        this.ttlPosition = ttlPosition;
        this.walEnabled = walEnabled;

        this.selectText = null;
        this.selectTextPosition = 0;
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
     * @param tableNamePosition           the position of table name in user's input, it is used for error reporting
     * @param selectText                  text of the nested AS SELECT statement
     * @param selectTextPosition          the position of the nested AS SELECT statement, it is used for error reporting
     * @param ignoreIfExists              "if exists" flag, table won't be created silently if it exists already
     * @param partitionBy                 partition type
     * @param timestampColumnName         designated timestamp column name
     * @param timestampColumnNamePosition designated timestamp column name in user's input
     * @param volumeAlias                 the name of the "volume" where table is created, volumes are used to create table on different physical disks
     * @param walEnabled                  WAL flag
     * @param defaultSymbolCapacity       the default symbol capacity value, usually comes from the configuration
     * @param maxUncommittedRows          max uncommitted rows for non-WAL tables, this is written to table's metadata to be used by ingress protocols
     * @param o3MaxLag                    o3 commit lag, another performance optimisation parameter for non-WAL tables.
     * @param createColumnModelMap        maps that contains type casts and additional index flags
     * @param batchSize                   number of rows in commit batch when data is moved from the select into the
     *                                    new table. Special value of -1 means "atomic" commit. This corresponds to "batch" keyword on the SQL.
     * @param batchO3MaxLag               lag windows in rows, which helps timestamp ordering code to smooth out timestamp jitter
     * @param tableKind                   table kind, REGULAR_TABLE, TEMP_PARQUET_EXPORT see TableUtils.TABLE_KIND_* constants
     */
    public CreateTableOperationImpl(
            String sqlText,
            @NotNull String tableName,
            int tableNamePosition,
            @NotNull String selectText,
            int selectTextPosition,
            boolean ignoreIfExists,
            int partitionBy,
            int partitionByPosition,
            @Nullable String timestampColumnName,
            int timestampColumnNamePosition,
            @Nullable String volumeAlias,
            int volumePosition,
            int ttlHoursOrMonths,
            int ttlPosition,
            boolean walEnabled,
            int defaultSymbolCapacity,
            int maxUncommittedRows,
            long o3MaxLag,
            @Transient LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> createColumnModelMap,
            long batchSize,
            long batchO3MaxLag,
            int tableKind,
            boolean autoIncludeTimestamp
    ) {
        this.autoIncludeTimestamp = autoIncludeTimestamp;
        this.sqlText = sqlText;
        this.tableName = tableName;
        this.tableNamePosition = tableNamePosition;
        this.selectText = selectText;
        this.selectTextPosition = selectTextPosition;
        this.partitionBy = partitionBy;
        this.partitionByPosition = partitionByPosition;
        this.volumeAlias = volumeAlias;
        this.volumePosition = volumePosition;
        this.ignoreIfExists = ignoreIfExists;
        this.timestampColumnName = timestampColumnName;
        this.timestampColumnNamePosition = timestampColumnNamePosition;
        this.ttlHoursOrMonths = ttlHoursOrMonths;
        this.ttlPosition = ttlPosition;
        this.defaultSymbolCapacity = defaultSymbolCapacity;
        this.batchSize = batchSize;
        this.batchO3MaxLag = batchO3MaxLag;
        this.o3MaxLag = o3MaxLag;
        this.maxUncommittedRows = maxUncommittedRows;
        this.walEnabled = walEnabled;

        this.likeTableName = null;
        this.likeTableNamePosition = -1;
        this.tableKind = tableKind;

        // This constructor is for a "create as select", column names will be scraped from the record
        // cursor at runtime. Column augmentation data comes from the following sources in the SQL:
        // - cast models, provides column types
        // - (symbol) column index data, e.g. index flag and index capacity
        // - (symbol) column cache flag
        initColumnMetadata(createColumnModelMap);
    }

    @Override
    public void close() {
    }

    @Override
    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = sqlExecutionContext.getCairoEngine().getSqlCompiler()) {
            compiler.execute(this, sqlExecutionContext);
        }
        return future;
    }

    public LowerCaseCharSequenceObjHashMap<TableColumnMetadata> getAugmentedColumnMetadata() {
        return augmentedColumnMetadata;
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
    public CopyDataProgressReporter getCopyDataProgressReporter() {
        return reporter;
    }

    @Override
    public IntList getCoveringColumnIndices(int index) {
        if (index < coveringColumnIndicesList.size()) {
            return coveringColumnIndicesList.getQuick(index);
        }
        return null;
    }

    @Override
    public int getIndexBlockCapacity(int index) {
        return getHighAt(index * 2 + 1);
    }

    @Override
    public byte getIndexType(int index) {
        return (byte) ((getLowAt(index * 2 + 1) & COLUMN_FLAG_INDEX_TYPE_MASK) >> COLUMN_FLAG_INDEX_TYPE_SHIFT);
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
    public int getParquetEncodingConfig(int index) {
        return parquetEncodingConfigs.getQuick(index);
    }

    @Override
    public int getPartitionBy() {
        return partitionBy;
    }

    @Override
    public int getSelectSqlScanDirection() {
        return selectSqlScanDirection;
    }

    @Override
    public String getSelectText() {
        return selectText;
    }

    @Override
    public int getSelectTextPosition() {
        return selectTextPosition;
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
    public int getTableKind() {
        return tableKind;
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

    public int getTtlPosition() {
        return ttlPosition;
    }

    @Override
    public CharSequence getVolumeAlias() {
        return volumeAlias;
    }

    @Override
    public int getVolumePosition() {
        return volumePosition;
    }

    @Override
    public boolean ignoreIfExists() {
        return ignoreIfExists;
    }

    public void initColumnMetadata(@Transient LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> createColumnModelMap) {
        assert columnNames.size() == 0;
        assert columnBits.size() == 0;

        colNameToDedupClausePos.clear();
        colNameToIndexClausePos.clear();
        colNameToCastClausePos.clear();
        augmentedColumnMetadata.clear();
        augmentedCoveringNames.clear();
        final ObjList<CharSequence> colNames = createColumnModelMap.keys();
        for (int i = 0, n = colNames.size(); i < n; i++) {
            final CharSequence columnName = colNames.get(i);
            final CreateTableColumnModel model = createColumnModelMap.get(columnName);
            final String columnNameStr = Chars.toString(columnName);
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
            final TableColumnMetadata columnMetadata = new TableColumnMetadata(
                    columnNameStr,
                    model.getColumnType(),
                    model.getIndexType(),
                    model.getIndexValueBlockSize(),
                    true,
                    null,
                    -1, // writer index is irrelevant here
                    model.isDedupKey(),
                    -1, // replacingIndex is irrelevant here
                    model.getSymbolCacheFlag(),
                    symbolCapacity
            );
            columnMetadata.setParquetEncodingConfig(model.getParquetEncodingConfig());
            ObjList<CharSequence> coverNames = model.getCoveringColumnNames();
            if (coverNames.size() > 0) {
                ObjList<CharSequence> copy = new ObjList<>(coverNames.size());
                for (int j = 0, m = coverNames.size(); j < m; j++) {
                    copy.add(Chars.toString(coverNames.get(j)));
                }
                augmentedCoveringNames.put(columnNameStr, copy);
            }
            augmentedColumnMetadata.put(columnNameStr, columnMetadata);
        }
    }

    @Override
    public boolean isCovering(int index) {
        return (getLowAt(index * 2 + 1) & COLUMN_FLAG_COVERING) != 0;
    }

    @Override
    public boolean isDedupKey(int index) {
        return (getLowAt(index * 2 + 1) & COLUMN_FLAG_DEDUP_KEY) != 0;
    }

    @Override
    public boolean isWalEnabled() {
        return walEnabled;
    }

    @Override
    public boolean needRegister() {
        return needRegister;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void setCopyDataProgressReporter(CopyDataProgressReporter reporter) {
        this.reporter = reporter;
    }

    public void setPartitionBy(int partitionBy) {
        this.partitionBy = partitionBy;
    }

    public void setTableKind(int tableKind) {
        this.tableKind = tableKind;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTimestampColumnName(String timestampColumnName) {
        this.timestampColumnName = timestampColumnName;
    }

    public void setTimestampColumnNamePosition(int timestampColumnNamePosition) {
        this.timestampColumnNamePosition = timestampColumnNamePosition;
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
        coveringColumnIndicesList.clear();
        parquetEncodingConfigs.clear();
        for (int i = 0; i < likeTableMetadata.getColumnCount(); i++) {
            TableColumnMetadata colMeta = likeTableMetadata.getColumnMetadata(i);
            addColumnBits(
                    colMeta.getColumnType(),
                    colMeta.isSymbolCacheFlag(),
                    colMeta.getSymbolCapacity(),
                    colMeta.getIndexType(),
                    colMeta.getIndexValueBlockCapacity(),
                    colMeta.isDedupKeyFlag(),
                    colMeta.isCovering(),
                    colMeta.getParquetEncodingConfig()
            );
            coveringColumnIndicesList.add(colMeta.getCoveringColumnIndices());
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
    public void validateAndUpdateMetadataFromSelect(RecordMetadata metadata, int scanDirection) throws SqlException {
        // This method must only be called in case of "create-as-select".
        // Here we remap data keyed on column names (from cast maps) to
        // data keyed on column index. We assume that "columnBits" are free to use
        // in case of "create-as-select" because they don't capture any useful data
        // at SQL parse time.
        assert this.selectText != null;
        this.columnBits.clear();
        this.coveringColumnIndicesList.clear();
        this.parquetEncodingConfigs.clear();
        if (this.timestampColumnName == null) {
            int timestampIndex = metadata.getTimestampIndex();
            if (timestampIndex > -1 && scanDirection == RecordCursorFactory.SCAN_DIRECTION_FORWARD) {
                this.timestampIndex = timestampIndex;
                timestampType = metadata.getTimestampType();
                this.selectSqlScanDirection = scanDirection;
            }
        } else {
            this.timestampIndex = metadata.getColumnIndexQuiet(this.timestampColumnName);
            if (this.timestampIndex == -1) {
                throw SqlException.position(this.timestampColumnNamePosition)
                        .put("designated timestamp column doesn't exist [name=").put(this.timestampColumnName).put(']');
            }
            timestampType = metadata.getColumnType(this.timestampIndex);
            if (!ColumnType.isTimestamp(timestampType)) {
                throw SqlException.position(this.timestampColumnNamePosition)
                        .put("TIMESTAMP column expected [actual=").put(ColumnType.nameOf(timestampType)).put(']');
            }
        }
        if (this.timestampIndex == -1 && this.partitionBy != PartitionBy.NONE) {
            throw SqlException.position(this.partitionByPosition)
                    .put("partitioning is possible only on tables with designated timestamps");
        }

        ObjList<CharSequence> castColNames = this.colNameToCastClausePos.keys();
        for (int i = 0, n = castColNames.size(); i < n; i++) {
            CharSequence castColName = castColNames.get(i);
            if (metadata.getColumnIndexQuiet(castColName) < 0) {
                throw SqlException.position(this.colNameToCastClausePos.get(castColName))
                        .put("CAST column doesn't exist [column=").put(castColName).put(']');
            }
        }
        ObjList<CharSequence> indexColNames = this.colNameToIndexClausePos.keys();
        for (int i = 0, n = indexColNames.size(); i < n; i++) {
            CharSequence indexedColName = indexColNames.get(i);
            if (metadata.getColumnIndexQuiet(indexedColName) < 0) {
                throw SqlException.position(this.colNameToIndexClausePos.get(indexedColName))
                        .put("INDEX column doesn't exist [column=").put(indexedColName).put(']');
            }
        }
        ObjList<CharSequence> dedupColNames = this.colNameToDedupClausePos.keys();
        for (int i = 0, n = dedupColNames.size(); i < n; i++) {
            CharSequence dedupColName = dedupColNames.get(i);
            if (metadata.getColumnIndexQuiet(dedupColName) < 0) {
                throw SqlException.position(this.colNameToDedupClausePos.get(dedupColName))
                        .put("DEDUP column doesn't exist [column=").put(dedupColName).put(']');
            }
        }

        this.columnNames.clear();
        boolean hasDedup = false;
        boolean isTimestampDeduped = false;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            final String columnName = metadata.getColumnName(i);
            final TableColumnMetadata augMeta = this.augmentedColumnMetadata.get(columnName);
            if (!TableUtils.isValidColumnName(columnName, 255)) {
                throw SqlException.position(this.tableNamePosition)
                        .put("invalid column name [name=")
                        .put(columnName)
                        .put(", position=")
                        .put(i)
                        .put(']');
            }

            int columnType;
            int symbolCapacity;
            boolean symbolCacheFlag;
            byte indexType;
            boolean isDedupKey;
            int indexBlockCapacity;
            int parquetEncodingConfig;
            if (augMeta != null) {
                final int fromType = metadata.getColumnType(i);
                columnType = augMeta.getColumnType();
                if (columnType == ColumnType.UNDEFINED) {
                    columnType = fromType;
                }
                if (!isCompatibleCast(fromType, columnType)) {
                    throw SqlException.unsupportedCast(this.colNameToCastClausePos.get(columnName), columnName, fromType, columnType);
                }
                symbolCapacity = augMeta.getSymbolCapacity();
                symbolCacheFlag = augMeta.isSymbolCacheFlag();
                indexType = augMeta.getIndexType();
                isDedupKey = augMeta.isDedupKeyFlag();
                indexBlockCapacity = augMeta.getIndexValueBlockCapacity();
                parquetEncodingConfig = augMeta.getParquetEncodingConfig();
            } else {
                columnType = metadata.getColumnType(i);
                if (ColumnType.isNull(columnType)) {
                    throw SqlException
                            .$(0, "cannot create NULL-type column, please use type cast, e.g. ")
                            .put(columnName).put("::").put("type");
                }
                symbolCapacity = this.defaultSymbolCapacity;
                symbolCacheFlag = true;
                indexType = IndexType.NONE;
                isDedupKey = false;
                indexBlockCapacity = 0;
                parquetEncodingConfig = 0;
            }

            if (!ColumnType.isSymbol(columnType) && IndexType.isIndexed(indexType)) {
                throw SqlException.$(0, "indexes are supported only for SYMBOL columns: ").put(columnName);
            }
            if (isDedupKey) {
                hasDedup = true;
                if (i == this.timestampIndex) {
                    isTimestampDeduped = true;
                }
            }
            this.columnNames.add(columnName);
            ObjList<CharSequence> coverNames = augmentedCoveringNames.get(columnName);
            // Set the COVERING flag whenever resolveCoveringFromAugmented
            // will produce a non-empty list — that is, either the user gave
            // an INCLUDE clause, or auto-include applies (POSTING index on
            // a column other than the designated timestamp).
            boolean hasCovering = (coverNames != null && coverNames.size() > 0)
                    || (autoIncludeTimestamp && this.timestampIndex >= 0 && i != this.timestampIndex
                    && IndexType.isPosting(indexType));
            this.addColumnBits(
                    columnType,
                    symbolCacheFlag,
                    symbolCapacity,
                    indexType,
                    indexBlockCapacity,
                    isDedupKey,
                    hasCovering,
                    parquetEncodingConfig
            );
        }
        resolveCoveringFromAugmented(metadata);
        if (hasDedup && !isTimestampDeduped) {
            // Report the error's position in SQL as the position of the first column in the DEDUP list
            int firstDedupColumnPos = Integer.MAX_VALUE;
            for (int i = 0, n = dedupColNames.size(); i < n; i++) {
                int dedupColPos = this.colNameToDedupClausePos.get(dedupColNames.get(i));
                if (firstDedupColumnPos > dedupColPos) {
                    firstDedupColumnPos = dedupColPos;
                }
            }
            throw SqlException.position(firstDedupColumnPos)
                    .put("deduplicate key list must include dedicated timestamp column");
        }
    }

    private static void maybeAppendTimestamp(IntList indices, int timestampIndex) {
        if (!indices.contains(timestampIndex)) {
            indices.add(timestampIndex);
        }
    }

    private void addColumnBits(
            int columnType,
            boolean symbolCacheFlag,
            int symbolCapacity,
            byte indexType,
            int indexBlockCapacity,
            boolean dedupFlag,
            boolean coveringFlag,
            int parquetEncodingConfig
    ) {
        int flags = (symbolCacheFlag ? COLUMN_FLAG_CACHED : 0)
                | ((indexType & 0x07) << COLUMN_FLAG_INDEX_TYPE_SHIFT)
                | (dedupFlag ? COLUMN_FLAG_DEDUP_KEY : 0)
                | (coveringFlag ? COLUMN_FLAG_COVERING : 0);
        columnBits.add(
                Numbers.encodeLowHighInts(columnType, symbolCapacity),
                Numbers.encodeLowHighInts(flags, indexBlockCapacity)
        );
        parquetEncodingConfigs.add(parquetEncodingConfig);
    }

    private int getHighAt(int index) {
        return Numbers.decodeHighInt(columnBits.getQuick(index));
    }

    private int getLowAt(int index) {
        return Numbers.decodeLowInt(columnBits.getQuick(index));
    }

    private void resolveCoveringColumnIndices(
            ObjList<CharSequence> allColumnNames,
            LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> modelMap,
            int timestampIndex
    ) throws SqlException {
        // The indices stored here are positions in the CREATE-TABLE column
        // list, which equal writerIndex (stable physical slot) at create time
        // because no DROPs have happened yet. .pci stores writerIndex for
        // stability across subsequent DROPs.
        coveringColumnIndicesList.clear();
        LowerCaseCharSequenceIntHashMap nameToIndex = new LowerCaseCharSequenceIntHashMap();
        for (int i = 0, n = allColumnNames.size(); i < n; i++) {
            nameToIndex.put(allColumnNames.get(i), i);
        }
        for (int i = 0, n = allColumnNames.size(); i < n; i++) {
            CreateTableColumnModel model = modelMap.get(allColumnNames.get(i));
            ObjList<CharSequence> coverNames = model.getCoveringColumnNames();
            IntList coverPositions = model.getCoveringColumnPositions();
            IntList indices = null;
            if (coverNames.size() > 0) {
                indices = new IntList(coverNames.size());
                for (int j = 0, m = coverNames.size(); j < m; j++) {
                    CharSequence covName = coverNames.get(j);
                    int covPos = coverPositions.getQuick(j);
                    int idx = nameToIndex.get(covName);
                    if (idx < 0) {
                        throw SqlException.position(covPos)
                                .put("INCLUDE column doesn't exist [column=").put(covName).put(']');
                    }
                    if (idx == i) {
                        throw SqlException.position(covPos)
                                .put("INCLUDE must not contain the indexed column [column=").put(covName).put(']');
                    }
                    for (int k = 0; k < j; k++) {
                        if (indices.getQuick(k) == idx) {
                            throw SqlException.position(covPos)
                                    .put("duplicate column in INCLUDE [column=").put(covName).put(']');
                        }
                    }
                    indices.add(idx);
                }
            }
            // Auto-append the designated timestamp on POSTING indexes,
            // including the bare INDEX TYPE POSTING case where the user
            // gave no INCLUDE list. Skip when this column is itself the
            // timestamp (you cannot cover yourself).
            if (autoIncludeTimestamp && timestampIndex >= 0 && i != timestampIndex
                    && IndexType.isPosting(model.getIndexType())) {
                if (indices == null) {
                    indices = new IntList(1);
                }
                maybeAppendTimestamp(indices, timestampIndex);
            }
            coveringColumnIndicesList.add(indices);
        }
    }

    private void resolveCoveringFromAugmented(RecordMetadata metadata) {
        coveringColumnIndicesList.clear();
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            ObjList<CharSequence> coverNames = augmentedCoveringNames.get(columnNames.get(i));
            IntList indices = null;
            if (coverNames != null && coverNames.size() > 0) {
                indices = new IntList(coverNames.size());
                for (int j = 0, m = coverNames.size(); j < m; j++) {
                    int idx = metadata.getColumnIndexQuiet(coverNames.get(j));
                    if (idx < 0) {
                        throw CairoException.nonCritical()
                                .put("INCLUDE column doesn't exist [column=").put(coverNames.get(j)).put(']');
                    }
                    if (idx == i) {
                        throw CairoException.nonCritical()
                                .put("INCLUDE must not contain the indexed column [column=").put(coverNames.get(j)).put(']');
                    }
                    indices.add(idx);
                }
            }
            // Auto-append the designated timestamp on POSTING indexes,
            // including the no-INCLUDE case (the out-of-line INDEX(...)
            // clause used by CTAS does not accept INCLUDE today, so this
            // is the only way users get covering on CTAS). Skip when this
            // column is itself the timestamp.
            if (autoIncludeTimestamp && timestampIndex >= 0 && i != timestampIndex
                    && IndexType.isPosting(getIndexType(i))) {
                if (indices == null) {
                    indices = new IntList(1);
                }
                maybeAppendTimestamp(indices, timestampIndex);
            }
            coveringColumnIndicesList.add(indices);
        }
    }

    String getTimestampColumnName() {
        return timestampColumnName;
    }

    int getTimestampColumnNamePosition() {
        return timestampColumnNamePosition;
    }

    int getTimestampType() {
        return timestampType;
    }
}
