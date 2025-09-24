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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;

import java.io.Closeable;

class OperationExecutor implements Closeable {
    private final BindVariableService bindVariableService;
    private final CairoEngine engine;
    private final WalApplySqlExecutionContext executionContext;
    private final Rnd rnd;

    OperationExecutor(
            CairoEngine engine,
            int sharedQueryWorkerCount
    ) {
        rnd = new Rnd();
        bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
        executionContext = new WalApplySqlExecutionContext(
                engine,
                sharedQueryWorkerCount
        );
        executionContext.with(
                engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                bindVariableService,
                rnd,
                -1,
                null
        );
        this.engine = engine;
    }

    @Override
    public void close() {
        Misc.free(executionContext);
    }

    /**
     * Returns result of underlying {@link AlterOperation#matViewInvalidationReason()}.
     */
    public String executeAlter(TableWriter tableWriter, CharSequence alterSql, long seqTxn) throws SqlException {
        final TableToken tableToken = tableWriter.getTableToken();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            executionContext.remapTableNameResolutionTo(tableToken);
            CompiledQuery compiledQuery;
            int stallCount = 0;
            while (true) {
                try {
                    compiledQuery = compiler.compile(alterSql, executionContext);
                    break;
                } catch (TableReferenceOutOfDateException ex) {
                    // The table is renamed in the table registry
                    // just before the compilation of this ALTER
                    TableToken updatedToken = engine.getUpdatedTableToken(tableToken);
                    if (updatedToken != null && !updatedToken.equals(tableToken)) {
                        tableWriter.updateTableToken(updatedToken);
                        executionContext.remapTableNameResolutionTo(updatedToken);
                    } else {
                        // This is a transient error, we should retry
                        // it can happen if the table renamed in the middle
                        // of alter compilation but then renamed back.
                        // This is highly unlikely to stall in real life
                        // but keeping the DB in live lock is not a good idea, hence there is a limit
                        if (stallCount++ > 10) {
                            throw ex;
                        }
                    }
                }
            }
            try (AlterOperation alterOp = compiledQuery.getAlterOperation()) {
                alterOp.withContext(executionContext);
                assert !alterOp.isStructural() : "alter operation must not be structural when applied as SQL";
                tableWriter.apply(alterOp, seqTxn);
                return alterOp.matViewInvalidationReason();
            }
        } catch (SqlException ex) {
            tableWriter.markSeqTxnCommitted(seqTxn);
            throw ex;
        }
    }

    public long executeUpdate(TableWriter tableWriter, CharSequence updateSql, long seqTxn) throws SqlException {
        final TableToken tableToken = tableWriter.getTableToken();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            executionContext.remapTableNameResolutionTo(tableToken);
            final CompiledQuery compiledQuery = compiler.compile(updateSql, executionContext);
            try (UpdateOperation updateOperation = compiledQuery.getUpdateOperation()) {
                updateOperation.withSqlStatement(updateSql);
                updateOperation.withContext(executionContext);
                return tableWriter.apply(updateOperation, seqTxn);
            }
        }
        // Do not catch the exception and mark transaction as committed
        // it can be transient, like table does not exist and should be retried.
    }

    public BindVariableService getBindVariableService() {
        return bindVariableService;
    }

    public void resetRnd(long seed0, long seed1) {
        rnd.reset(seed0, seed1);
    }

    public void setNowAndFixClock(long now, int nowTimestampType) {
        executionContext.setNowAndFixClock(now, nowTimestampType);
    }
}
