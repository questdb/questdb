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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;

/**
 * Detaches a partition and attaches it back. Between the two, it can run a statement that moves the partition the
 * columns were added at, which tells whether a partition without a column-top record holds a column: TRUNCATE, or a
 * DROP PARTITION of every partition from a given day on. The attach must keep reading a column added after the
 * partition was written as NULL there.
 * <p>
 * The test names the detached copy as ATTACH looks for it, see {@code cairo.attach.partition.suffix}, so a WAL table
 * applies the three statements in one go. A DETACH of a partition the table does not hold, or of its active
 * partition, fails, and so does the ATTACH then: both fail the same way on WAL and non-WAL tables, and a WAL table
 * skips them, so the operation goes on past them.
 */
public class FuzzDetachAttachPartitionOperation implements FuzzTransactionOperation {
    public static final int BETWEEN_DROP_NEWEST = 2;
    public static final int BETWEEN_NOTHING = 0;
    public static final int BETWEEN_TRUNCATE = 1;
    // what apply() reports for a statement
    private static final int APPLIED = 0;
    private static final int FAILED = 1;
    private static final int TABLE_GONE = 2;
    private final int between;
    // the first partition BETWEEN_DROP_NEWEST drops
    private final long dropFromTimestamp;
    private final long partitionTimestamp;

    public FuzzDetachAttachPartitionOperation(long partitionTimestamp, int between, long dropFromTimestamp) {
        this.partitionTimestamp = partitionTimestamp;
        this.between = between;
        this.dropFromTimestamp = dropFromTimestamp;
    }

    @Override
    public boolean apply(Rnd rnd, CairoEngine engine, TableWriterAPI wApi, int virtualTimestampIndex, LongList excludedTsIntervals) {
        try (
                SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1);
                SqlCompiler compiler = engine.getSqlCompiler()
        ) {
            context.with(AllowAllSecurityContext.INSTANCE);
            final TableRecordMetadata metadata = wApi.getMetadata();
            final StringSink partitionName = new StringSink();
            PartitionBy.setSinkForPartition(partitionName, metadata.getTimestampType(), PartitionBy.DAY, partitionTimestamp);
            final String tableName = TestUtils.randomiseCase(rnd, wApi.getTableToken().getTableName());
            final int detached = apply(compiler, context, wApi, "ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + partitionName + '\'');
            if (detached == TABLE_GONE) {
                return true;
            }
            switch (between) {
                case BETWEEN_TRUNCATE -> wApi.truncateSoft();
                case BETWEEN_DROP_NEWEST -> {
                    final String dropSql = "ALTER TABLE " + tableName + " DROP PARTITION WHERE "
                            + FuzzTransactionOperation.partitionsFromFilter(metadata, dropFromTimestamp);
                    if (apply(compiler, context, wApi, dropSql) == TABLE_GONE) {
                        return true;
                    }
                }
                default -> {
                    assert between == BETWEEN_NOTHING;
                }
            }
            final String attachSql = "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + partitionName + '\'';
            if (apply(compiler, context, wApi, attachSql) == FAILED && detached == APPLIED) {
                // A WAL table reports neither: it applies the statements later, and skips them when they fail. A
                // partition a non-WAL table detached must attach back, or it loses the partition, and so would the WAL
                // table, unnoticed by the comparison of the two.
                throw new AssertionError("a detached partition did not attach back [table=" + tableName + ", partition=" + partitionName + ']');
            }
            return true;
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }

    public int getBetween() {
        return between;
    }

    public long getDropFromTimestamp() {
        return dropFromTimestamp;
    }

    public long getPartitionTimestamp() {
        return partitionTimestamp;
    }

    private static int apply(SqlCompiler compiler, SqlExecutionContextImpl context, TableWriterAPI wApi, String sql) throws SqlException {
        try {
            final AlterOperation alterOp = compiler.compile(sql, context).getAlterOperation();
            alterOp.withSqlStatement(sql);
            alterOp.withContext(context);
            wApi.apply(alterOp, false);
            return APPLIED;
        } catch (CairoException e) {
            if (Chars.contains(e.getFlyweightMessage(), "table does not exist") || e.isTableDropped()) {
                // a drop and create step of the parallel WAL apply recreates the table
                return TABLE_GONE;
            }
            // a partition the table does not hold, or its active one, which a WAL table skips too
            if (e.isWALTolerable()) {
                return FAILED;
            }
            throw e;
        }
    }
}
