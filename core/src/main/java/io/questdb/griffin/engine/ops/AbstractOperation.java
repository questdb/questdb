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

import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.tasks.TableWriterTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractOperation implements AsyncWriterCommand, QuietCloseable {
    private static final long NO_CORRELATION_ID = -1L;
    @Nullable
    protected SecurityContext securityContext;
    @Nullable
    protected SqlExecutionContext sqlExecutionContext;
    @Nullable
    protected CharSequence sqlText;
    protected int tableNamePosition;
    @Nullable
    protected TableToken tableToken;
    private String cmdName;
    private int cmdType;
    private long correlationId;
    private int tableId;
    private long tableVersion;

    public void clearCommandCorrelationId() {
        setCommandCorrelationId(NO_CORRELATION_ID);
    }

    public void clearSecurityContext() {
        Misc.clear(securityContext);
    }

    @Override
    public void close() {
        // todo: temporary fix, until a proper lifecycle around CompiledQuery
        // is implemented
//        this.tableToken = null;
    }

    @Override
    public int getCmdType() {
        return cmdType;
    }

    @Override
    public String getCommandName() {
        return cmdName;
    }

    @Override
    public long getCorrelationId() {
        return correlationId;
    }

    public @Nullable SqlExecutionContext getSqlExecutionContext() {
        return sqlExecutionContext;
    }

    public @Nullable CharSequence getSqlText() {
        return sqlText;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public int getTableNamePosition() {
        return tableNamePosition;
    }

    @Override
    public @NotNull TableToken getTableToken() {
        assert tableToken != null : "initialized operation";
        return tableToken;
    }

    @Override
    public long getTableVersion() {
        return tableVersion;
    }

    public boolean isForceWalBypass() {
        return false;
    }

    @Override
    public void serialize(TableWriterTask task) {
        task.of(cmdType, tableId, tableToken);
        task.setInstance(correlationId);
    }

    @Override
    public void setCommandCorrelationId(long correlationId) {
        this.correlationId = correlationId;
    }

    public void withContext(@NotNull SqlExecutionContext sqlExecutionContext) {
        this.sqlExecutionContext = sqlExecutionContext;
        this.securityContext = sqlExecutionContext.getSecurityContext();
    }

    public void withSecurityContext(@Nullable SecurityContext securityContext) {
        this.securityContext = securityContext;
    }

    public void withSqlStatement(CharSequence sqlStatement) {
        this.sqlText = sqlStatement;
    }

    protected void init(
            int cmdType,
            String cmdName,
            TableToken tableToken,
            int tableId,
            long tableVersion,
            int tableNamePosition
    ) {
        this.cmdType = cmdType;
        this.cmdName = cmdName;
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.tableVersion = tableVersion;
        this.tableNamePosition = tableNamePosition;
        this.correlationId = NO_CORRELATION_ID;
    }
}
