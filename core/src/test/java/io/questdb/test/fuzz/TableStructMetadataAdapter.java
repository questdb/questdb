package io.questdb.test.fuzz;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.sql.RecordMetadata;

public class TableStructMetadataAdapter implements TableStructure {
    private final CairoConfiguration configuration;
    private final boolean isWalEnabled;
    private final RecordMetadata metadata;
    private final int partitionBy;
    private final String tableName;
    private final int timestampIndex;

    public TableStructMetadataAdapter(
            String tableName,
            boolean isWalEnabled,
            RecordMetadata metadata,
            CairoConfiguration configuration,
            int partitionBy,
            int timestampIndex
    ) {
        this.tableName = tableName;
        this.isWalEnabled = isWalEnabled;
        this.metadata = metadata;
        this.configuration = configuration;
        this.partitionBy = partitionBy;
        this.timestampIndex = timestampIndex;
    }

    @Override
    public int getColumnCount() {
        return metadata.getColumnCount();
    }

    @Override
    public CharSequence getColumnName(int columnIndex) {
        return metadata.getColumnName(columnIndex);
    }

    @Override
    public int getColumnType(int columnIndex) {
        int columnType = metadata.getColumnType(columnIndex);
        assert columnType > 0;
        return columnType;
    }

    @Override
    public int getIndexBlockCapacity(int columnIndex) {
        return 256;
    }

    @Override
    public int getMaxUncommittedRows() {
        return configuration.getMaxUncommittedRows();
    }

    @Override
    public long getO3MaxLag() {
        return configuration.getO3MaxLag();
    }

    @Override
    public int getPartitionBy() {
        return partitionBy;
    }

    @Override
    public boolean getSymbolCacheFlag(int columnIndex) {
        return ColumnType.isSymbol(metadata.getColumnType(columnIndex));
    }

    @Override
    public int getSymbolCapacity(int columnIndex) {
        return 4096;
    }

    @Override
    public CharSequence getTableName() {
        return tableName;
    }

    @Override
    public int getTimestampIndex() {
        return timestampIndex;
    }

    @Override
    public boolean isDedupKey(int columnIndex) {
        return false;
    }

    @Override
    public boolean isIndexed(int columnIndex) {
        return ColumnType.isSymbol(metadata.getColumnType(columnIndex));
    }

    @Override
    public boolean isSequential(int columnIndex) {
        return true;
    }

    @Override
    public boolean isWalEnabled() {
        return isWalEnabled;
    }
}
