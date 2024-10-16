package io.questdb.griffin.engine.ops;

import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ColumnCastModel;
import io.questdb.mp.SCSequence;
import io.questdb.std.*;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.ops.CreateTableOperationBuilder.COLUMN_FLAG_CACHED;

public class CreateTableOperation implements TableStructure, QuietCloseable {
    private final long batchO3MaxLag;
    private final long batchSize;
    private final LongList columnBits = new LongList();
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private final CharSequenceIntHashMap columnTypeMap = new CharSequenceIntHashMap();
    private final CreateTableOperationFuture future = new CreateTableOperationFuture();
    private final boolean ignoreIfExists;
    private final String likeTableName;
    private final int likeTableNamePosition;
    private final int maxUncommittedRows;
    private final long o3MaxLag;
    private final int partitionBy;
    // two cast maps, one for symbol cache flag and the other for symbol capacity
    // those values come from "cast models", the extra syntax to augment
    // "create as select" semantic. These maps are keyed on column names
    //
    // One thing to note about these maps is that they are unused for non-create-as-select,
    // this is because column types, capacities and flags can be specified without
    // extra syntax. At the same time, "columnBits" are unused for create-as-select.
    // For create-as-select we should move the information from these maps onto
    // columnBit after column indexes are known. E.g. after "select" part is executed.
    // Note that we must not hardcode "cast" parameters to the column indexes. These indexes
    // are liable to change every time "select" is recompiled, for example in case of wildcard
    // usage, e.g. create x as select * from y. When "y" changes, such as via drop column,
    // column indexes will shift.
    // todo: when all test pass, we can consolidate these maps into 1, e.g. text-to-long64
    private final CharSequenceBoolHashMap symbolCacheFlagMap = new CharSequenceBoolHashMap();
    private final CharSequenceIntHashMap symbolCapacityMap = new CharSequenceIntHashMap();
    private final String tableName;
    private final int tableNamePosition;
    private final int timestampIndex;
    private final String volumeAlias;
    private final boolean walEnabled;
    private RecordCursorFactory recordCursorFactory;

    public CreateTableOperation(
            String tableName,
            int tableNamePosition,
            ObjList<CharSequence> columnNames,
            LongList columnBits,
            int timestampIndex,
            int partitionBy,
            boolean ignoreIfExists,
            String likeTableName,
            int likeTableNamePosition,
            RecordCursorFactory recordCursorFactory,
            long batchSize,
            long batchO3MaxLag,
            long o3MaxLag,
            int maxUncommittedRows,
            String volumeAlias,
            boolean walEnabled,
            @Transient CharSequenceObjHashMap<ColumnCastModel> columnCastModeMap
    ) {
        // At this point we're creating immutable column names for the operation.
        // We take this opportunity to capture immutable column names for the cast models
        // that could be optionally provided
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            CharSequence columnName = columnNames.getQuick(i);
            String columnNameStr = Chars.toString(columnName);
            this.columnNames.add(columnNameStr);
            int index = columnCastModeMap.keyIndex(columnName);
            if (index < 0) {
                ColumnCastModel ccm = columnCastModeMap.valueAt(index);
                this.symbolCacheFlagMap.put(columnNameStr, ccm.getSymbolCacheFlag());
                this.symbolCapacityMap.put(columnNameStr, ccm.getSymbolCapacity());
                this.columnTypeMap.put(columnNameStr, ccm.getColumnType());
            }
        }
        this.columnBits.add(columnBits);
        this.likeTableName = likeTableName;
        this.likeTableNamePosition = likeTableNamePosition;
        this.maxUncommittedRows = maxUncommittedRows;
        this.o3MaxLag = o3MaxLag;
        this.batchSize = batchSize;
        this.batchO3MaxLag = batchO3MaxLag;
        this.partitionBy = partitionBy;
        this.ignoreIfExists = ignoreIfExists;
        this.recordCursorFactory = recordCursorFactory;
        this.tableName = tableName;
        this.tableNamePosition = tableNamePosition;
        this.timestampIndex = timestampIndex;
        this.volumeAlias = volumeAlias;
        this.walEnabled = walEnabled;
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
        assert capacity != -1;
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
        return (getLowAt(index * 2 + 1) & CreateTableOperationBuilder.COLUMN_FLAG_DEDUP_KEY) != 0;
    }

    @Override
    public boolean isIndexed(int index) {
        return (getLowAt(index * 2 + 1) & CreateTableOperationBuilder.COLUMN_FLAG_INDEXED) != 0;
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

    public void updateMetadataFromSelect(RecordMetadata metadata) {
        // This method must only be called in case of "create-as-select".
        // Here we remap data keyed on  column names (from cast maps) to
        // data keyed on column index. We assume that "columnBits" are free to use
        // in case of "create-as-select" because they don't capture any useful data
        // at SQL parse time.
        columnBits.clear();

        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            final int columnType;
            int index = columnTypeMap.keyIndex(metadata.getColumnName(i));
            if (index < 0) {
                columnType = columnTypeMap.valueAt(index);
            } else {
                columnType = metadata.getColumnType(i);
            }

            final int symbolCapacity;
            index = symbolCapacityMap.keyIndex(metadata.getColumnName(i));
            if (index < 0) {
                symbolCapacity = symbolCapacityMap.valueAt(index);
            } else {
                symbolCapacity = 0;
            }

            final int symbolCacheFlag;
            index = symbolCacheFlagMap.keyIndex(metadata.getColumnName(i));
            if (index < 0) {
                symbolCacheFlag = symbolCacheFlagMap.valueAt(index) ? COLUMN_FLAG_CACHED : 0;
            } else {
                symbolCacheFlag = 0;
            }
            columnBits.add(
                    Numbers.encodeLowHighInts(columnType, symbolCapacity),
                    Numbers.encodeLowHighInts(symbolCacheFlag, 0)
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
