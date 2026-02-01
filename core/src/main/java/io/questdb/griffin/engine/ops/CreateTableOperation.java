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

import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.CopyDataProgressReporter;
import io.questdb.griffin.SqlException;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import org.jetbrains.annotations.Nullable;

public interface CreateTableOperation extends TableStructure, Operation {
    LowerCaseCharSequenceObjHashMap<TableColumnMetadata> getAugmentedColumnMetadata();

    long getBatchO3MaxLag();

    long getBatchSize();

    CopyDataProgressReporter getCopyDataProgressReporter();

    @Nullable
    CharSequence getLikeTableName();

    int getLikeTableNamePosition();

    OperationFuture getOperationFuture();

    int getSelectSqlScanDirection();

    String getSelectText();

    int getSelectTextPosition();

    CharSequence getSqlText();

    int getTableKind();

    int getTableNamePosition();

    CharSequence getVolumeAlias();

    int getVolumePosition();

    String getTimestampColumnName();

    int getTimestampColumnNamePosition();

    int getTimestampType();

    boolean ignoreIfExists();

    boolean needRegister();

    void setCopyDataProgressReporter(CopyDataProgressReporter reporter);

    void initColumnMetadata(
            io.questdb.std.ObjList<CharSequence> columnNames,
            LowerCaseCharSequenceObjHashMap<io.questdb.griffin.model.CreateTableColumnModel> createColumnModelMap);

    void initColumnMetadata(
            LowerCaseCharSequenceObjHashMap<io.questdb.griffin.model.CreateTableColumnModel> createColumnModelMap);

    void updateFromLikeTableMetadata(TableMetadata likeTableMetadata);

    void updateOperationFutureAffectedRowsCount(long insertCount);

    void updateOperationFutureTableToken(TableToken tableToken);

    void validateAndUpdateMetadataFromSelect(RecordMetadata metadata, int scanDirection,
            io.questdb.griffin.FunctionParser functionParser) throws SqlException;

    io.questdb.std.ObjList<io.questdb.griffin.model.CreateTableColumnModel> getColumns();
}
