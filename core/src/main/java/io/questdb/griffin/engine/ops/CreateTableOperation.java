package io.questdb.griffin.engine.ops;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.TouchUpColumnModel;
import io.questdb.mp.SCSequence;
import io.questdb.std.*;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.ops.CreateTableOperationBuilder.*;

public class CreateTableOperation implements TableStructure, QuietCloseable {
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
    private final CharSequenceObjHashMap<TableColumnMetadata> augmentedColumnMetadata = new CharSequenceObjHashMap<>();
    private final long batchO3MaxLag;
    private final long batchSize;
    private final LongList columnBits = new LongList();
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private final CreateTableOperationFuture future = new CreateTableOperationFuture();
    private final String likeTableName;
    // position of the "like" table name in the SQL text, for error reporting
    private final int likeTableNamePosition;
    private final String tableName;
    private final int tableNamePosition;
    private final String volumeAlias;
    private boolean ignoreIfExists;
    private int maxUncommittedRows;
    private long o3MaxLag;
    private int partitionBy;
    private RecordCursorFactory recordCursorFactory;
    private int timestampIndex;
    private boolean walEnabled;

    public CreateTableOperation(
            String tableName,
            int tableNamePosition,
            String volumeAlias,
            String likeTableName,
            int likeTableNamePosition,
            boolean ignoreIfExists
    ) {
        this.tableName = tableName;
        this.tableNamePosition = tableNamePosition;
        this.volumeAlias = volumeAlias;
        this.likeTableName = likeTableName;
        this.likeTableNamePosition = likeTableNamePosition;

        this.batchSize = 0;
        this.batchO3MaxLag = 0;
    }

    public CreateTableOperation(
            String tableName,
            int tableNamePosition,
            String volumeAlias,
            boolean ignoreIfExists,
            ObjList<String> columnNames,
            LongList columnBits,
            int timestampIndex,
            int partitionBy,
            long o3MaxLag,
            int maxUncommittedRows,
            boolean walEnabled
    ) {
        this.tableName = tableName;
        this.tableNamePosition = tableNamePosition;
        this.volumeAlias = volumeAlias;
        this.ignoreIfExists = ignoreIfExists;
        this.maxUncommittedRows = maxUncommittedRows;
        this.o3MaxLag = o3MaxLag;
        this.partitionBy = partitionBy;
        this.timestampIndex = timestampIndex;
        this.walEnabled = walEnabled;
        this.columnNames.addAll(columnNames);
        this.columnBits.add(columnBits);

        this.batchSize = 0;
        this.batchO3MaxLag = 0;
        this.recordCursorFactory = null;
        this.likeTableName = null;
        this.likeTableNamePosition = -1;
    }

    public CreateTableOperation(
            String tableName,
            int tableNamePosition,
            String volumeAlias,
            boolean ignoreIfExists,
            long batchSize,
            long batchO3MaxLag,
            RecordCursorFactory recordCursorFactory,
            @Transient CharSequenceObjHashMap<TouchUpColumnModel> touchUpColumnModelMap
    ) throws SqlException {
        this.tableName = tableName;
        this.tableNamePosition = tableNamePosition;
        this.volumeAlias = volumeAlias;
        this.recordCursorFactory = recordCursorFactory;
        this.batchSize = batchSize;
        this.batchO3MaxLag = batchO3MaxLag;

        this.likeTableName = null;
        this.likeTableNamePosition = -1;

        // This constructor is for a "create as select", column names will be scraped from the record
        // cursor at runtime. Column augmentation data comes from the following sources in the SQL:
        // - cast models, provides column types
        // - (symbol) column index data, e.g. index flag and index capacity
        // - (symbol) column cache flag
        assert columnNames.size() == 0;
        assert columnBits.size() == 0;
        ObjList<CharSequence> touchedUpColNames = touchUpColumnModelMap.keys();
        for (int i = 0, n = touchedUpColNames.size(); i < n; i++) {
            CharSequence columnName = touchedUpColNames.get(i);
            TouchUpColumnModel touchUpModel = touchUpColumnModelMap.get(columnName);
            if (touchUpModel.isIndexed()) {
                // perform some basic validation
                if (touchUpModel.getColumnType() != ColumnType.SYMBOL) {
                    throw SqlException
                            .$(touchUpModel.getIndexColumnPos(), "indexes are supported only for SYMBOL columns: ")
                            .put(columnName);
                }
            }
            String columnNameStr = Chars.toString(columnName);
            TableColumnMetadata tcm = new TableColumnMetadata(
                    columnNameStr,
                    touchUpModel.getColumnType(),
                    touchUpModel.isIndexed(),
                    touchUpModel.getIndexValueBlockSize(),
                    true,
                    null,
                    -1, // writer index is irrelevant here
                    false,// dedup flag cannot be set on "create as select", not yet
                    -1, // replacingIndex is irrelevant here
                    touchUpModel.getSymbolCacheFlag(),
                    touchUpModel.getSymbolCapacity()
            );
            augmentedColumnMetadata.put(columnNameStr, tcm);
        }
    }

    @Override
    public void close() {
        recordCursorFactory = Misc.free(recordCursorFactory);
    }

    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = sqlExecutionContext.getCairoEngine().getSqlCompiler()) {
            compiler.execute(this, sqlExecutionContext);
        }
        return getFuture();
    }

    public long getBatchO3MaxLag() {
        return batchO3MaxLag;
    }

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

    /**
     * SQLCompiler side API to get future associated with this operation.
     *
     * @return mutable future associated with this operation
     */
    public CreateTableOperationFuture getFuture() {
        return future;
    }

    @Override
    public int getIndexBlockCapacity(int index) {
        return getHighAt(index * 2 + 1);
    }

    public CharSequence getLikeTableName() {
        return likeTableName;
    }

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
    public int getPartitionBy() {
        return partitionBy;
    }

    public RecordCursorFactory getRecordCursorFactory() {
        return recordCursorFactory;
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

    public int getTableNamePosition() {
        return tableNamePosition;
    }

    @Override
    public int getTimestampIndex() {
        return timestampIndex;
    }

    public IntIntHashMap getTypeCasts() {
        return null;
    }

    public CharSequence getVolumeAlias() {
        return volumeAlias;
    }

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

    public void updateFromLikeTableMetadata(TableMetadata likeTableMetadata) {
        this.maxUncommittedRows = likeTableMetadata.getMaxUncommittedRows();
        this.o3MaxLag = likeTableMetadata.getO3MaxLag();
        this.partitionBy = likeTableMetadata.getPartitionBy();
        this.timestampIndex = likeTableMetadata.getTimestampIndex();
        this.walEnabled = likeTableMetadata.isWalEnabled();
        columnNames.clear();
        columnBits.clear();
        for (int i = 0; i < likeTableMetadata.getColumnCount(); i++) {
            TableColumnMetadata colMeta = likeTableMetadata.getColumnMetadata(i);
            int columnType = colMeta.getColumnType();
            boolean isIndexed = colMeta.isSymbolIndexFlag();
            boolean isCached = colMeta.isSymbolCacheFlag();
            boolean isDedupKey = colMeta.isDedupKeyFlag();
            int symbolCapacity = colMeta.getSymbolCapacity();
            int indexBlockCapacity = colMeta.getIndexValueBlockCapacity();
            int flags = (isCached ? COLUMN_FLAG_CACHED : 0) | (isIndexed ? COLUMN_FLAG_INDEXED : 0) |
                    (isDedupKey ? COLUMN_FLAG_DEDUP_KEY : 0);
            columnNames.add(colMeta.getColumnName());
            columnBits.add(
                    Numbers.encodeLowHighInts(columnType, symbolCapacity),
                    Numbers.encodeLowHighInts(flags, indexBlockCapacity)
            );
        }
    }

    /**
     * SQLCompiler side API to set affected rows count after the operation has been executed.
     *
     * @param affectedRowsCount the number of rows inserted in the table after it has been created. Typically,
     *                          this is 0 or the number of rows from "create as select"
     */
    public void updateOperationFutureAffectedRowsCount(long affectedRowsCount) {
        future.of(affectedRowsCount);
    }

    /**
     * This is SQLCompiler side API to set table token after the operation has been executed.
     *
     * @param tableToken table token of the newly created table
     */
    public void updateOperationFutureTableToken(TableToken tableToken) {
        future.tableToken = tableToken;
    }

    public void validateAndUpdateMetadataFromSelect(RecordMetadata metadata) throws SqlException {
        // This method must only be called in case of "create-as-select".
        // Here we remap data keyed on column names (from cast maps) to
        // data keyed on column index. We assume that "columnBits" are free to use
        // in case of "create-as-select" because they don't capture any useful data
        // at SQL parse time.
        columnBits.clear();
        timestampIndex = metadata.getTimestampIndex();

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
                symbolCapacity = augMeta.getSymbolCapacity();
                symbolCacheFlag = augMeta.isSymbolCacheFlag();
                symbolIndexed = augMeta.isSymbolIndexFlag();
                isDedupKey = augMeta.isDedupKeyFlag();
                indexBlockCapacity = augMeta.getIndexValueBlockCapacity();
            } else {
                columnType = metadata.getColumnType(i);
                TableColumnMetadata colMeta = metadata.getColumnMetadata(i);
                symbolCapacity = colMeta.getSymbolCapacity();
                symbolCacheFlag = colMeta.isSymbolCacheFlag();
                symbolIndexed = colMeta.isSymbolIndexFlag();
                isDedupKey = colMeta.isDedupKeyFlag();
                indexBlockCapacity = colMeta.getIndexValueBlockCapacity();
            }

            if (ColumnType.isNull(columnType)) {
                throw SqlException.$(0, "cannot create NULL-type column, please use type cast, e.g. ").put(columnName).put("::").put("type");
            }
            if (!ColumnType.isSymbol(columnType) && symbolIndexed) {
                throw SqlException.$(0, "indexes are supported only for SYMBOL columns: ").put(columnName);
            }

            columnNames.add(columnName);
            int flags = (symbolCacheFlag ? COLUMN_FLAG_CACHED : 0) | (symbolIndexed ? COLUMN_FLAG_INDEXED : 0) |
                    (isDedupKey ? COLUMN_FLAG_DEDUP_KEY : 0);
            columnBits.add(
                    Numbers.encodeLowHighInts(columnType, symbolCapacity),
                    Numbers.encodeLowHighInts(flags, indexBlockCapacity)
            );
        }
    }

    private int getHighAt(int index) {
        return Numbers.decodeHighInt(columnBits.getQuick(index));
    }

    private int getLowAt(int index) {
        return Numbers.decodeLowInt(columnBits.getQuick(index));
    }

    public static class CreateTableOperationFuture extends DoneOperationFuture {
        private TableToken tableToken;

        public TableToken getTableToken() {
            return tableToken;
        }
    }
}
