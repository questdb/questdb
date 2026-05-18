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
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import org.jetbrains.annotations.Nullable;

public class CreateLiveViewOperation implements Operation {
    private final @Nullable LiveViewDefinition.LvAnchorSpec anchorSpec;
    private final String baseTableName;
    private final int baseTableNamePosition;
    private final long flushEveryInterval;
    private final char flushEveryIntervalUnit;
    private final boolean ignoreIfExists;
    private final long inMemoryInterval;
    private final char inMemoryIntervalUnit;
    private final int partitionBy;
    private final String selectSql;
    private final String viewName;
    private final int viewNamePosition;

    public CreateLiveViewOperation(
            String viewName,
            int viewNamePosition,
            String baseTableName,
            int baseTableNamePosition,
            String selectSql,
            long flushEveryInterval,
            char flushEveryIntervalUnit,
            long inMemoryInterval,
            char inMemoryIntervalUnit,
            int partitionBy,
            boolean ignoreIfExists,
            @Nullable LiveViewDefinition.LvAnchorSpec anchorSpec
    ) {
        this.viewName = viewName;
        this.viewNamePosition = viewNamePosition;
        this.baseTableName = baseTableName;
        this.baseTableNamePosition = baseTableNamePosition;
        this.selectSql = selectSql;
        this.flushEveryInterval = flushEveryInterval;
        this.flushEveryIntervalUnit = flushEveryIntervalUnit;
        this.inMemoryInterval = inMemoryInterval;
        this.inMemoryIntervalUnit = inMemoryIntervalUnit;
        this.partitionBy = partitionBy;
        this.ignoreIfExists = ignoreIfExists;
        this.anchorSpec = anchorSpec;
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

    public @Nullable LiveViewDefinition.LvAnchorSpec getAnchorSpec() {
        return anchorSpec;
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public int getBaseTableNamePosition() {
        return baseTableNamePosition;
    }

    public long getFlushEveryInterval() {
        return flushEveryInterval;
    }

    public char getFlushEveryIntervalUnit() {
        return flushEveryIntervalUnit;
    }

    public long getInMemoryInterval() {
        return inMemoryInterval;
    }

    public char getInMemoryIntervalUnit() {
        return inMemoryIntervalUnit;
    }

    @Override
    public int getOperationCode() {
        return io.questdb.cairo.OperationCodes.CREATE_LIVE_VIEW;
    }

    @Override
    public OperationFuture getOperationFuture() {
        return ImmutableDoneOperationFuture.INSTANCE;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public String getSelectSql() {
        return selectSql;
    }

    public String getViewName() {
        return viewName;
    }

    public int getViewNamePosition() {
        return viewNamePosition;
    }

    public boolean isIgnoreIfExists() {
        return ignoreIfExists;
    }
}
