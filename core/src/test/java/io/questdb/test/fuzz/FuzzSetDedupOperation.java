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

package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;

/**
 * {@code ALTER TABLE ... DEDUP ENABLE <mode> KEYS(<designated timestamp>)}, in the mode
 * {@link TableUtils#DEDUP_MODE_UPSERT} or {@link TableUtils#DEDUP_MODE_SKIP}.
 */
public class FuzzSetDedupOperation implements FuzzTransactionOperation {
    private final int dedupMode;

    public FuzzSetDedupOperation(int dedupMode) {
        assert dedupMode == TableUtils.DEDUP_MODE_UPSERT || dedupMode == TableUtils.DEDUP_MODE_SKIP;
        this.dedupMode = dedupMode;
    }

    @Override
    public boolean apply(Rnd tempRnd, CairoEngine engine, TableWriterAPI wApi, int virtualTimestampIndex, LongList excludedTsIntervals) {
        final TableRecordMetadata metadata = wApi.getMetadata();
        final String sql = "ALTER TABLE " + wApi.getTableToken().getTableName()
                + " DEDUP ENABLE " + (dedupMode == TableUtils.DEDUP_MODE_SKIP ? "SKIP" : "UPSERT")
                + " KEYS(" + metadata.getColumnName(metadata.getTimestampIndex()) + ")";
        try (
                SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1);
                SqlCompiler sqlCompiler = engine.getSqlCompiler()
        ) {
            context.with(AllowAllSecurityContext.INSTANCE);
            final CompiledQuery query = sqlCompiler.compile(sql, context);
            final AlterOperation alterOp = query.getAlterOperation();
            alterOp.withSqlStatement(sql);
            alterOp.withContext(context);
            wApi.apply(alterOp, false);
            return true;
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }
}
