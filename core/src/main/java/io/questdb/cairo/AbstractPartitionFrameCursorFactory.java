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

package io.questdb.cairo;

import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

abstract class AbstractPartitionFrameCursorFactory implements PartitionFrameCursorFactory {
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private final RecordMetadata metadata;
    private final long metadataVersion;
    private final TableToken tableToken;
    private final boolean updateQuery;
    private final String viewName;
    private final int viewPosition;

    AbstractPartitionFrameCursorFactory(
            TableToken tableToken,
            long metadataVersion,
            RecordMetadata metadata,
            @Nullable String viewName,
            int viewPosition,
            boolean updateQuery
    ) {
        this.tableToken = tableToken;
        this.metadataVersion = metadataVersion;
        this.metadata = metadata;
        this.viewName = viewName;
        this.viewPosition = viewPosition;
        this.updateQuery = updateQuery;
    }

    @Override
    public void close() {
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public boolean supportsTableRowId(TableToken tableToken) {
        return this.tableToken.equals(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.meta("on").val(tableToken.getTableName());
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("{\"name\":\"")
                .put(this.getClass().getSimpleName())
                .putAscii("\", \"table\":\"")
                .put(tableToken)
                .putAscii("\"}");
    }

    void authorizeSelect(SqlExecutionContext executionContext, @NotNull IntList columnIndexes) throws SqlException {
        final SecurityContext securityContext = executionContext.getSecurityContext();
        if (viewName != null) {
            // reading table via view, check access to view
            final CairoEngine engine = executionContext.getCairoEngine();
            final TableToken viewToken = engine.verifyTableName(viewName);
            final ViewDefinition viewDefinition = engine.getViewGraph().getViewDefinition(viewToken);
            if (viewDefinition == null) {
                throw SqlException.viewDoesNotExist(viewPosition, viewName);
            }
            securityContext.authorizeSelect(viewDefinition);

            // columns not referenced by the view require explicit permission
            final LowerCaseCharSequenceHashSet depCols = viewDefinition.getDependencies().get(tableToken.getTableName());
            if (!depCols.contains("*")) {
                columnNames.clear();
                for (int i = 0, n = columnIndexes.size(); i < n; i++) {
                    final String columnName = metadata.getColumnName(columnIndexes.getQuick(i));
                    if (!depCols.contains(columnName)) {
                        columnNames.add(columnName);
                    }
                }
                if (columnNames.size() > 0) {
                    securityContext.authorizeSelect(tableToken, columnNames);
                }
            }
        } else if (columnIndexes.size() > 0) {
            columnNames.clear();
            for (int i = 0, n = columnIndexes.size(); i < n; i++) {
                final String columnName = metadata.getColumnName(columnIndexes.getQuick(i));
                columnNames.add(columnName);
            }
            securityContext.authorizeSelect(tableToken, columnNames);
        } else {
            if (!updateQuery) {
                securityContext.authorizeSelectOnAnyColumn(tableToken);
            }
        }
    }

    TableReader getReader(SqlExecutionContext executionContext) {
        return executionContext.getReader(
                tableToken,
                metadataVersion
        );
    }
}
