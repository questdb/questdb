package io.questdb.griffin.engine.ops;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableColumnMetadata;
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
    private final CharSequenceObjHashMap<TableColumnMetadata> augmentedColumnMetadata = new CharSequenceObjHashMap<>();
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
    private final String tableName;
    private final int tableNamePosition;
    private final int timestampIndex;
    private final String volumeAlias;
    private final boolean walEnabled;
    private RecordCursorFactory recordCursorFactory;

    public CreateTableOperation(
            String tableName,
            int tableNamePosition,
            ObjList<String> columnNames,
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
            @Transient CharSequenceObjHashMap<ColumnCastModel> columnCastModeMap,
            @Transient CharSequenceIntHashMap createAsSelectIndexColumnNamePositions,
            @Transient CharSequenceBoolHashMap createAsSelectIndexFlags,
            @Transient CharSequenceIntHashMap createAsSelectIndexCapacities
    ) throws SqlException {

        if (recordCursorFactory != null) {
            // column names and bits are populated from "create table" column list
            this.columnNames.addAll(columnNames);
            this.columnBits.add(columnBits);
        } else {
            // we have "create as select" SQL, column names will be scraped from the
            // record cursor during executions time. We might have column
            // augmentation data from the following sources:
            // - cast models, provides column types
            // - (symbol) column index data, e.g. index flag and index capacity
            // - (symbol) column cache flag
            ObjList<CharSequence> castColumnNames = columnCastModeMap.keys();
            for (int i = 0, n = castColumnNames.size(); i < n; i++) {
                CharSequence columnName = castColumnNames.get(i);
                ColumnCastModel ccm = columnCastModeMap.get(columnName);
                int index = createAsSelectIndexFlags.keyIndex(columnName);
                if (index < 0) {
                    boolean augIndexFlag = createAsSelectIndexFlags.get(columnName);
                    int augIndexPos = createAsSelectIndexColumnNamePositions.get(columnName);
                    int augIndexValueBlockCapacity = createAsSelectIndexCapacities.get(columnName);
                    // perform some basic validation
                    if (ccm.getColumnType() != ColumnType.SYMBOL && augIndexFlag) {
                        throw SqlException.$(augIndexPos, "index flag cannot be applied to ").put(ColumnType.nameOf(ccm.getColumnType()));
                    }

                    String columnNameStr = Chars.toString(columnName);
                    TableColumnMetadata tcm = new TableColumnMetadata(
                            columnNameStr,
                            ccm.getColumnType(),
                            augIndexFlag,
                            augIndexValueBlockCapacity,
                            true,
                            null,
                            -1, // writer index is irrelevant here
                            false,// dedup flag cannot be set on "create as select", not yet
                            -1, // irrelvant
                            ccm.getSymbolCacheFlag(),
                            ccm.getSymbolCapacity()
                    );
                    augmentedColumnMetadata.put(columnNameStr, tcm);
                } else {
                    // "index" clause is not used concurrently with the "cast" clause
                    String columnNameStr = Chars.toString(columnName);
                    TableColumnMetadata tcm = new TableColumnMetadata(
                            columnNameStr,
                            ccm.getColumnType(),
                            ccm.isIndexed(),
                            ccm.getIndexValueBlockSize(),
                            true,
                            null,
                            -1, // writer index is irrelevant here
                            false, // dedup flag cannot be set on "create as select", not yet
                            -1,
                            ccm.getSymbolCacheFlag(),
                            ccm.getSymbolCapacity()
                    );
                    augmentedColumnMetadata.put(columnNameStr, tcm);
                }
            }
            // column bits must be unset at the compile time
            // we wil be using this bitset at the runtime
            assert columnBits.size() == 0;
        }

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

    public void validateAndUpdateMetadataFromSelect(RecordMetadata metadata) {
        // This method must only be called in case of "create-as-select".
        // Here we remap data keyed on  column names (from cast maps) to
        // data keyed on column index. We assume that "columnBits" are free to use
        // in case of "create-as-select" because they don't capture any useful data
        // at SQL parse time.
        columnBits.clear();

        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            TableColumnMetadata augMeta = augmentedColumnMetadata.get(metadata.getColumnName(i));
            int columnType;
            boolean symbolIndexed;

            if (augMeta != null) {
                columnType = augMeta.getColumnType();
                symbolIndexed = augMeta.isSymbolIndexFlag();
            } else {
                columnType = metadata.getColumnType(i);
                symbolIndexed = false;
            }

            // todo:
            if (!ColumnType.isSymbol(metadata.getColumnType(i)) && model.isIndexed(i)) {
                throw SqlException.$(0, "indexes are supported only for SYMBOL columns: ").put(columnName);
            }

            if (ColumnType.isNull(metadata.getColumnType(index))) {
                throw SqlException.$(0, "cannot create NULL-type column, please use type cast, e.g. ").put(columnName).put("::").put("type");
            }

            columnBits.add(
                    Numbers.encodeLowHighInts(columnType, symbolCapacity),
                    Numbers.encodeLowHighInts(symbolCacheFlag, 0)
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
