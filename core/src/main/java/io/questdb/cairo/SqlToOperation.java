/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.std.ClosableInstance;
import io.questdb.std.Rnd;

import java.io.Closeable;

public class SqlToOperation implements Closeable {
    private final Rnd rnd;
    private final BindVariableService bindVariableService;
    private final SqlExecutionContext sqlExecutionContext;
    private final CairoEngine engine;

    public SqlToOperation(CairoEngine engine, int workerCount, int sharedWorkerCount) {
        rnd = new Rnd();
        bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
        sqlExecutionContext = new SqlExecutionContextImpl(
                engine,
                workerCount,
                sharedWorkerCount
        ).with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        bindVariableService,
                        rnd,
                        -1,
                        null
                )
                .withWalApplication();
        this.engine = engine;
    }

    @Override
    public void close() {
        sqlExecutionContext.close();
    }

    public Rnd getRnd() {
        return rnd;
    }

    public Rnd getRandom() {
        return rnd;
    }

    public BindVariableService getBindVariableService() {
        return bindVariableService;
    }

    public AlterOperation toAlterOperation(CharSequence alterStatement) {
        try (ClosableInstance<SqlCompiler> sqlCompiler = engine.getAdhocSqlCompiler()) {
            final CompiledQuery compiledQuery = sqlCompiler.instance().compile(alterStatement, sqlExecutionContext);
            final AlterOperation alterOperation = compiledQuery.getAlterOperation();
            alterOperation.withContext(sqlExecutionContext);
            return alterOperation;
        } catch (SqlException e) {
            // we could not compile SQL query, but this should not stop us from
            // processing other operations.
            throw CairoException.nonCritical()
                    .put(e.getFlyweightMessage())
                    .position(e.getPosition());
        }
    }

    public UpdateOperation toUpdateOperation(CharSequence updateStatement) {
        try (ClosableInstance<SqlCompiler> sqlCompiler = engine.getAdhocSqlCompiler()) {
            final CompiledQuery compiledQuery = sqlCompiler.instance().compile(updateStatement, sqlExecutionContext);
            final UpdateOperation updateOperation = compiledQuery.getUpdateOperation();
            updateOperation.withContext(sqlExecutionContext);
            return updateOperation;
        } catch (SqlException e) {
            // we could not compile SQL query, but this should not stop us from
            // processing other operations.
            throw CairoException.nonCritical()
                    .put(e.getFlyweightMessage())
                    .position(e.getPosition());
        }
    }
}
