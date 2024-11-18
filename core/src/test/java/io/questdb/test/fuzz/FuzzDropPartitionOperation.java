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

package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.std.Chars;
import io.questdb.std.Rnd;

public class FuzzDropPartitionOperation implements FuzzTransactionOperation {

    private final long cutoffTimestamp;

    public FuzzDropPartitionOperation(long cutoffTimestamp) {
        this.cutoffTimestamp = cutoffTimestamp;
    }

    @Override
    public boolean apply(Rnd tempRnd, CairoEngine engine, TableWriterAPI wApi, int virtualTimestampIndex) {
        try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1);
             SqlCompiler sqlCompiler = engine.getSqlCompiler()
        ) {
            context.with(AllowAllSecurityContext.INSTANCE);
            TableRecordMetadata metadata = wApi.getMetadata();
            String sql = String.format("ALTER TABLE %s DROP PARTITION WHERE %s < %d AND %s > %d - 86400000000",
                    wApi.getTableToken().getTableName(),
                    metadata.getColumnName(metadata.getTimestampIndex()),
                    cutoffTimestamp,
                    metadata.getColumnName(metadata.getTimestampIndex()),
                    cutoffTimestamp
            );

            try {
                CompiledQuery query = sqlCompiler.compile(sql, context);
                AlterOperation alterOp = query.getAlterOperation();
                alterOp.withSqlStatement(sql);
                alterOp.withContext(context);
                wApi.apply(alterOp, false);
                return true;
            } catch (CairoException e) {
                // Table can be re-created at this moment by a drop/create fuzz step.
                // Give it a bit of time to appear.
                if (Chars.contains(e.getFlyweightMessage(), "table does not exist") || e.isTableDropped()) {
                    // Table can be dropped by another drop/recreate fuzz operation.
                    return true;
                } else {
                    throw e;
                }
            }
        } catch (Exception e) {
            return false;
        }
    }
}
