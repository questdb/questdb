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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.std.str.Path;

/**
 * Execution context of a session whose "does the name exist" check ran before another
 * session registered the name. {@link #getTableStatus(Path, CharSequence)} reports
 * TABLE_DOES_NOT_EXIST for every name, so the CREATE ... IF NOT EXISTS fast path in
 * SqlCompilerImpl does not fire, and the CREATE reaches CairoEngine, where it loses the
 * name race to the object that already exists.
 */
public class StaleTableStatusExecutionContext extends SqlExecutionContextImpl {

    public StaleTableStatusExecutionContext(CairoEngine engine) {
        super(engine, 1);
        with(engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(), null);
    }

    /**
     * Compiles and executes one DDL statement in this context.
     *
     * @return SqlCompiler.execute()'s result: true when the DDL took effect, false for a no-op
     */
    public boolean executeDdl(CharSequence ddl) throws SqlException {
        try (SqlCompiler compiler = getCairoEngine().getSqlCompiler()) {
            final CompiledQuery cq = compiler.compile(ddl, this);
            try (Operation op = cq.getOperation()) {
                return compiler.execute(op, this);
            }
        }
    }

    /**
     * Compiles and executes one DDL statement in this context.
     *
     * @return the affected rows count the operation future reports
     */
    public long executeDdlAffectedRows(CharSequence ddl) throws SqlException {
        try (SqlCompiler compiler = getCairoEngine().getSqlCompiler()) {
            final CompiledQuery cq = compiler.compile(ddl, this);
            try (Operation op = cq.getOperation(); OperationFuture future = op.execute(this, null)) {
                future.await();
                return future.getAffectedRowsCount();
            }
        }
    }

    @Override
    public int getTableStatus(Path path, CharSequence tableName) {
        return TableUtils.TABLE_DOES_NOT_EXIST;
    }
}
