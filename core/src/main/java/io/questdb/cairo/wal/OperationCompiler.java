/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;

import java.io.Closeable;

class OperationCompiler implements Closeable {
    private final BindVariableService bindVariableService;
    private final CairoEngine engine;
    private final TableRenameSupportExecutionContext renameSupportExecutionContext;
    private final Rnd rnd;

    OperationCompiler(
            CairoEngine engine,
            int workerCount,
            int sharedWorkerCount
    ) {
        rnd = new Rnd();
        bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
        renameSupportExecutionContext = new TableRenameSupportExecutionContext(
                engine,
                workerCount,
                sharedWorkerCount
        );
        renameSupportExecutionContext.with(
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
        Misc.free(renameSupportExecutionContext);
    }

    public AlterOperation compileAlterSql(CharSequence alterSql, TableToken tableToken) throws SqlException {
        renameSupportExecutionContext.remapTableNameResolutionTo(tableToken);
        AlterOperation alterOp;
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final CompiledQuery compiledQuery = compiler.compile(alterSql, renameSupportExecutionContext);
            alterOp = compiledQuery.getAlterOperation();
        }
        alterOp.withContext(renameSupportExecutionContext);
        return alterOp;
    }

    public UpdateOperation compileUpdateSql(CharSequence updateSql, TableToken tableToken) throws SqlException {
        renameSupportExecutionContext.remapTableNameResolutionTo(tableToken);
        UpdateOperation updateOperation;
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final CompiledQuery compiledQuery = compiler.compile(updateSql, renameSupportExecutionContext);
            updateOperation = compiledQuery.getUpdateOperation();
        }
        updateOperation.withContext(renameSupportExecutionContext);
        return updateOperation;
    }

    public BindVariableService getBindVariableService() {
        return bindVariableService;
    }

    public void resetRnd(long seed0, long seed1) {
        rnd.reset(seed0, seed1);
    }

    public void setNowAndFixClock(long now) {
        renameSupportExecutionContext.setNowAndFixClock(now);
    }
}
