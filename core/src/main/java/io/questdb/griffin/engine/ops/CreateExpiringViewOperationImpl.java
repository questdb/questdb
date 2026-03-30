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

import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import org.jetbrains.annotations.Nullable;

public class CreateExpiringViewOperationImpl implements CreateExpiringViewOperation {
    private final String baseTableName;
    private final int baseTableNamePosition;
    private final long cleanupIntervalMicros;
    private final int expiryColumnIndex;
    private final String expiryPredicateSql;
    private final int expiryType;
    private final boolean ifNotExists;
    private final DoneOperationFuture operationFuture = new DoneOperationFuture();
    private final String sqlText;
    private final String viewName;
    private final int viewNamePosition;

    public CreateExpiringViewOperationImpl(
            String sqlText,
            String viewName,
            int viewNamePosition,
            String baseTableName,
            int baseTableNamePosition,
            String expiryPredicateSql,
            int expiryType,
            int expiryColumnIndex,
            long cleanupIntervalMicros,
            boolean ifNotExists
    ) {
        this.sqlText = sqlText;
        this.viewName = viewName;
        this.viewNamePosition = viewNamePosition;
        this.baseTableName = baseTableName;
        this.baseTableNamePosition = baseTableNamePosition;
        this.expiryPredicateSql = expiryPredicateSql;
        this.expiryType = expiryType;
        this.expiryColumnIndex = expiryColumnIndex;
        this.cleanupIntervalMicros = cleanupIntervalMicros;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public void close() {
    }

    @Override
    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = sqlExecutionContext.getCairoEngine().getSqlCompiler()) {
            compiler.execute(this, sqlExecutionContext);
        }
        return operationFuture;
    }

    @Override
    public String getBaseTableName() {
        return baseTableName;
    }

    @Override
    public int getBaseTableNamePosition() {
        return baseTableNamePosition;
    }

    @Override
    public long getCleanupIntervalMicros() {
        return cleanupIntervalMicros;
    }

    @Override
    public int getExpiryColumnIndex() {
        return expiryColumnIndex;
    }

    @Override
    public String getExpiryPredicateSql() {
        return expiryPredicateSql;
    }

    @Override
    public int getExpiryType() {
        return expiryType;
    }

    @Override
    public int getOperationCode() {
        return OperationCodes.CREATE_EXPIRING_VIEW;
    }

    @Override
    public OperationFuture getOperationFuture() {
        return operationFuture;
    }

    @Override
    public CharSequence getSqlText() {
        return sqlText;
    }

    @Override
    public String getViewName() {
        return viewName;
    }

    @Override
    public int getViewNamePosition() {
        return viewNamePosition;
    }

    @Override
    public boolean ignoreIfExists() {
        return ifNotExists;
    }

    public void updateOperationFutureTableToken(TableToken tableToken) {
        operationFuture.of(0);
    }
}
