package io.questdb.cairo.view;

import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;

public class ViewMetadata extends GenericRecordMetadata implements TableMetadata {
    private final TableToken viewToken;

    public ViewMetadata(TableToken viewToken) {
        this.viewToken = viewToken;
    }

    public static ViewMetadata newInstance(TableToken viewToken) {
        return new ViewMetadata(viewToken);
    }

    public static ViewMetadata newInstance(TableToken viewToken, RecordMetadata sourceMetadata) {
        if (sourceMetadata != null) {
            final ViewMetadata metadata = newInstance(viewToken);
            for (int i = 0, n = sourceMetadata.getColumnCount(); i < n; i++) {
                metadata.add(
                        new TableColumnMetadata(
                                sourceMetadata.getColumnName(i),
                                sourceMetadata.getColumnType(i),
                                false,
                                0,
                                false,
                                sourceMetadata.getMetadata(i),
                                i,
                                false
                        )
                );
            }
            metadata.setTimestampIndex(sourceMetadata.getTimestampIndex());
            return metadata;
        }
        return null;
    }

    @Override
    public void close() {
        // nothing to release
    }

    @Override
    public int getIndexBlockCapacity(int columnIndex) {
        return 0;
    }

    @Override
    public int getMaxUncommittedRows() {
        return 0;
    }

    @Override
    public long getMetadataVersion() {
        return 0L;
    }

    @Override
    public long getO3MaxLag() {
        return 0L;
    }

    @Override
    public int getPartitionBy() {
        return PartitionBy.NOT_APPLICABLE;
    }

    @Override
    public boolean getSymbolCacheFlag(int columnIndex) {
        return false;
    }

    @Override
    public int getSymbolCapacity(int columnIndex) {
        return 0;
    }

    @Override
    public int getTableId() {
        return viewToken.getTableId();
    }

    @Override
    public CharSequence getTableName() {
        return viewToken.getTableName();
    }

    @Override
    public TableToken getTableToken() {
        return viewToken;
    }

    @Override
    public int getTimestampIndex() {
        return timestampIndex;
    }

    @Override
    public boolean isIndexed(int columnIndex) {
        return false;
    }

    @Override
    public boolean isWalEnabled() {
        return true;
    }
}
