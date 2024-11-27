package io.questdb.griffin.engine.ops;

import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlException;
import org.jetbrains.annotations.Nullable;

public interface CreateTableOperation extends TableStructure, Operation {
    long getBatchO3MaxLag();

    long getBatchSize();

    @Nullable CharSequence getLikeTableName();

    int getLikeTableNamePosition();

    RecordCursorFactory getRecordCursorFactory();

    CharSequence getSelectText();

    CharSequence getSqlText();

    int getTableNamePosition();

    OperationFuture getOperationFuture();

    CharSequence getVolumeAlias();

    boolean ignoreIfExists();

    void updateFromLikeTableMetadata(TableMetadata likeTableMetadata);

    void updateOperationFutureAffectedRowsCount(long insertCount);

    void updateOperationFutureTableToken(TableToken tableToken);

    void validateAndUpdateMetadataFromSelect(RecordMetadata metadata) throws SqlException;
}
