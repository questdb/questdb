package io.questdb.griffin.engine.ops;

import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import io.questdb.std.*;
import org.jetbrains.annotations.Nullable;

public class CreateTableOperation implements TableStructure {
    private final long batchO3MaxLag;
    private final long batchSize;
    private final LongList columnBits = new LongList();
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private final CreateTableOperationFuture future = new CreateTableOperationFuture();
    private final boolean ignoreIfExists;
    private final String likeTableName;
    private final int likeTableNamePosition;
    private final int maxUncommittedRows;
    private final long o3MaxLag;
    private final int partitionBy;
    private final RecordCursorFactory recordCursorFactory;
    private final String tableName;
    private final int tableNamePosition;
    private final int timestampIndex;
    private final String volumeAlias;
    private final boolean walEnabled;

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
            boolean walEnabled
    ) {
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            this.columnNames.add(Chars.toString(columnNames.getQuick(i)));
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

    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = sqlExecutionContext.getCairoEngine().getSqlCompiler()) {
            compiler.execute(this, sqlExecutionContext);
        }
        return null;
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
        return (getLowAt(index * 2 + 1) & CreateTableOperationBuilder.COLUMN_FLAG_CACHED) != 0;
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
