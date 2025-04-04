/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.PartitionBy;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

import static io.questdb.griffin.engine.table.ShowCreateTableRecordCursorFactory.ttlToSink;

public class CreateMatViewOperationBuilderImpl implements CreateMatViewOperationBuilder, Mutable, Sinkable {
    public static final ObjectFactory<CreateMatViewOperationBuilderImpl> FACTORY = CreateMatViewOperationBuilderImpl::new;
    private final CreateTableOperationBuilderImpl createTableOperationBuilder = new CreateTableOperationBuilderImpl();
    private String baseTableName;
    private int baseTableNamePosition = 0;
    private int refreshType = -1;
    private String timeZone;
    private String timeZoneOffset;

    @Override
    public CreateMatViewOperation build(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, CharSequence sqlText) throws SqlException {
        final CreateTableOperationImpl createTableOperation = createTableOperationBuilder.build(compiler, sqlExecutionContext, sqlText);
        return new CreateMatViewOperationImpl(
                Chars.toString(sqlText),
                createTableOperation,
                refreshType,
                baseTableName,
                baseTableNamePosition,
                timeZone,
                timeZoneOffset
        );
    }

    @Override
    public void clear() {
        createTableOperationBuilder.clear();
        refreshType = -1;
        baseTableName = null;
        timeZone = null;
        timeZoneOffset = null;
    }

    public CreateTableOperationBuilderImpl getCreateTableOperationBuilder() {
        return createTableOperationBuilder;
    }

    @Override
    public QueryModel getQueryModel() {
        return createTableOperationBuilder.getQueryModel();
    }

    @Override
    public CharSequence getTableName() {
        return createTableOperationBuilder.getTableName();
    }

    @Override
    public ExpressionNode getTableNameExpr() {
        return createTableOperationBuilder.getTableNameExpr();
    }

    public void setBaseTableName(String baseTableName) {
        this.baseTableName = baseTableName;
    }

    public void setBaseTableNamePosition(int baseTableNamePosition) {
        this.baseTableNamePosition = baseTableNamePosition;
    }

    public void setRefreshType(int refreshType) {
        this.refreshType = refreshType;
    }

    @Override
    public void setSelectModel(QueryModel selectModel) {
        createTableOperationBuilder.setSelectModel(selectModel);
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public void setTimeZoneOffset(String timeZoneOffset) {
        this.timeZoneOffset = timeZoneOffset;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("create materialized view ");
        sink.put(createTableOperationBuilder.getTableName());
        if (baseTableName != null) {
            sink.putAscii(" with base ");
            sink.put(baseTableName);
        }
        sink.putAscii(" as (");
        if (createTableOperationBuilder.getQueryModel() != null) {
            createTableOperationBuilder.getQueryModel().toSink(sink);
        }
        sink.putAscii(')');
        for (int i = 0, n = createTableOperationBuilder.getColumnCount(); i < n; i++) {
            final CharSequence columnName = createTableOperationBuilder.getColumnName(i);
            final CreateTableColumnModel columnModel = createTableOperationBuilder.getColumnModel(columnName);
            if (columnModel != null && columnModel.isIndexed()) {
                sink.putAscii(", index(");
                sink.put(columnName);
                sink.putAscii(" capacity ");
                sink.put(columnModel.getIndexValueBlockSize());
                sink.putAscii(')');
            }
        }

        if (createTableOperationBuilder.getTimestampExpr() != null) {
            sink.putAscii(" timestamp(");
            createTableOperationBuilder.getTimestampExpr().toSink(sink);
            sink.putAscii(')');
        }

        sink.putAscii(" partition by ").put(PartitionBy.toString(createTableOperationBuilder.getPartitionByFromExpr()));

        final int ttlHoursOrMonths = createTableOperationBuilder.getTtlHoursOrMonths();
        ttlToSink(ttlHoursOrMonths, sink);

        final CharSequence volumeAlias = createTableOperationBuilder.getVolumeAlias();
        if (volumeAlias != null) {
            sink.putAscii(" in volume '").put(volumeAlias).putAscii('\'');
        }
    }
}
