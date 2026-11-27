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
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.test.cairo.fuzz.FailureFileFacade;

// VACUUM reads the applied table, independently of this writer's WAL position.
public class FuzzVacuumOperation implements FuzzTransactionOperation {
    private static final String COLUMN_PURGE_QUEUE_FULL_MESSAGE = "failed to schedule column version purge, queue is full";
    private static final String PURGE_DISCOVERY_QUEUE_FULL_MESSAGE = "cannot schedule vacuum action, queue is full";
    private static final long QUEUE_FULL_TIMEOUT_NANOS = 30_000_000_000L;

    @Override
    public boolean apply(Rnd rnd, CairoEngine engine, TableWriterAPI tableWriter, int virtualTimestampIndex, LongList excludedTsIntervals) {
        final FailureFileFacade failureFileFacade = engine.getConfiguration().getFilesFacade() instanceof FailureFileFacade facade ? facade : null;
        final int failuresObserved = failureFileFacade != null ? failureFileFacade.failureGenerated() : 0;
        try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE);
             SqlCompiler compiler = engine.getSqlCompiler();
             O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1)
        ) {
            vacuum(compiler, context, purgeJob, "VACUUM TABLE \"" + tableWriter.getTableToken().getTableName().replace("\"", "\"\"") + "\"");
            purgeJob.drain(0);
        } catch (SqlException e) {
            if (failureFileFacade != null && failureFileFacade.failureGenerated() > failuresObserved || e.isTableDoesNotExist()) {
                return false;
            }
            throw new RuntimeException(e);
        } catch (Throwable th) {
            if (failureFileFacade != null && failureFileFacade.failureGenerated() > failuresObserved) {
                return false;
            }
            if (th instanceof CairoException ce && ce.isTableDoesNotExist()) {
                return false;
            }
            throw th;
        }
        return false;
    }

    // VACUUM rejects work while the partition purge discovery queue or ColumnPurgeJob's input queue is full
    // and asks the caller to retry. With VACUUM before every transaction from several writer threads, the
    // purge workers can fall behind on a loaded host, and a single-writer WAL run queues every operation
    // before any apply worker starts. This thread drains the discovery queue itself and retries. A full
    // ColumnPurgeJob queue skips the VACUUM instead: FuzzRunner runs that job between transactions, on this
    // thread when it applies them one by one, so a wait here would not see the queue drain. Neither loses
    // work: the next VACUUM rescans the table directory and re-discovers every stale partition and column
    // version.
    private static void vacuum(SqlCompiler compiler, SqlExecutionContext context, O3PartitionPurgeJob purgeJob, String sql) throws SqlException {
        final long deadline = System.nanoTime() + QUEUE_FULL_TIMEOUT_NANOS;
        while (true) {
            try {
                compiler.compile(sql, context);
                return;
            } catch (SqlException e) {
                if (!Chars.contains(e.getFlyweightMessage(), PURGE_DISCOVERY_QUEUE_FULL_MESSAGE) || System.nanoTime() > deadline) {
                    throw e;
                }
                // This thread consumes the discovery queue itself, so the retry does not depend on another worker.
                purgeJob.drain(0);
            } catch (CairoException e) {
                if (!Chars.contains(e.getFlyweightMessage(), COLUMN_PURGE_QUEUE_FULL_MESSAGE)) {
                    throw e;
                }
                return;
            }
        }
    }
}
