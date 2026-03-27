/*+*****************************************************************************
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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CreatePayloadTransformOperation implements Operation {
    private final int dlqPartitionBy;
    private final String dlqTable;
    private final int dlqTablePosition;
    private final String dlqTtlUnit;
    private final int dlqTtlUnitPosition;
    private final long dlqTtlValue;
    private final int dlqTtlValuePosition;
    private final boolean ignoreIfExists;
    private final boolean isReplace;
    private final String name;
    private final int namePosition;
    private final String selectSql;
    private final int selectSqlPosition;
    private final String sqlText;
    private final String targetTable;
    private final int targetTablePosition;

    public CreatePayloadTransformOperation(
            @NotNull String sqlText,
            @NotNull String name,
            int namePosition,
            @NotNull String targetTable,
            int targetTablePosition,
            @NotNull String selectSql,
            int selectSqlPosition,
            @Nullable String dlqTable,
            int dlqTablePosition,
            int dlqPartitionBy,
            long dlqTtlValue,
            int dlqTtlValuePosition,
            @Nullable String dlqTtlUnit,
            int dlqTtlUnitPosition,
            boolean ignoreIfExists,
            boolean isReplace
    ) {
        this.sqlText = sqlText;
        this.name = name;
        this.namePosition = namePosition;
        this.targetTable = targetTable;
        this.targetTablePosition = targetTablePosition;
        this.selectSql = selectSql;
        this.selectSqlPosition = selectSqlPosition;
        this.dlqTable = dlqTable;
        this.dlqTablePosition = dlqTablePosition;
        this.dlqPartitionBy = dlqPartitionBy;
        this.dlqTtlValue = dlqTtlValue;
        this.dlqTtlValuePosition = dlqTtlValuePosition;
        this.dlqTtlUnit = dlqTtlUnit;
        this.dlqTtlUnitPosition = dlqTtlUnitPosition;
        this.ignoreIfExists = ignoreIfExists;
        this.isReplace = isReplace;
    }

    @Override
    public void close() {
    }

    @Override
    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        final CairoEngine engine = sqlExecutionContext.getCairoEngine();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.execute(this, sqlExecutionContext);
        }
        return ImmutableDoneOperationFuture.INSTANCE;
    }

    public int getDlqPartitionBy() {
        return dlqPartitionBy;
    }

    public String getDlqTable() {
        return dlqTable;
    }

    public int getDlqTablePosition() {
        return dlqTablePosition;
    }

    public String getDlqTtlUnit() {
        return dlqTtlUnit;
    }

    public int getDlqTtlUnitPosition() {
        return dlqTtlUnitPosition;
    }

    public long getDlqTtlValue() {
        return dlqTtlValue;
    }

    public int getDlqTtlValuePosition() {
        return dlqTtlValuePosition;
    }

    public String getName() {
        return name;
    }

    public int getNamePosition() {
        return namePosition;
    }

    @Override
    public int getOperationCode() {
        return OperationCodes.CREATE_PAYLOAD_TRANSFORM;
    }

    @Override
    public OperationFuture getOperationFuture() {
        return ImmutableDoneOperationFuture.INSTANCE;
    }

    public String getSelectSql() {
        return selectSql;
    }

    public int getSelectSqlPosition() {
        return selectSqlPosition;
    }

    public String getSqlText() {
        return sqlText;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public int getTargetTablePosition() {
        return targetTablePosition;
    }

    public boolean ignoreIfExists() {
        return ignoreIfExists;
    }

    public boolean isReplace() {
        return isReplace;
    }
}
