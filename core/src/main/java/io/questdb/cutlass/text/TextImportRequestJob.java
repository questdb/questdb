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

package io.questdb.cutlass.text;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;

public class TextImportRequestJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(TextImportRequestJob.class);

    private final RingQueue<TextImportRequestTask> requestQueue;
    private final Sequence requestSubSeq;
    private final CharSequence statusTableName;
    private final MicrosecondClock clock;
    private final int logRetentionDays;
    private final LongList partitionsToRemove = new LongList();
    private TableWriter writer;
    private SqlCompiler sqlCompiler;
    private SqlExecutionContextImpl sqlExecutionContext;
    private TextImportRequestTask task;
    private final ParallelCsvFileImporter.PhaseStatusReporter updateStatusRef = this::updateStatus;
    private ParallelCsvFileImporter importer;
    private final TextImportExecutionContext textImportExecutionContext;

    public TextImportRequestJob(
            final CairoEngine engine,
            int workerCount,
            @Nullable FunctionFactoryCache functionFactoryCache
    ) throws SqlException {
        this.requestQueue = engine.getMessageBus().getTextImportRequestQueue();
        this.requestSubSeq = engine.getMessageBus().getTextImportRequestSubSeq();
        this.importer = new ParallelCsvFileImporter(engine, workerCount);

        CairoConfiguration configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.statusTableName = configuration.getSystemTableNamePrefix() + "parallel_text_import_log";

        this.sqlCompiler = new SqlCompiler(engine, functionFactoryCache, null);
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        this.sqlExecutionContext.with(AllowAllCairoSecurityContext.INSTANCE, null, null);
        this.sqlCompiler.compile(
                "CREATE TABLE IF NOT EXISTS \"" + statusTableName + "\" (" +
                        "ts timestamp, " + // 0
                        "id symbol, " + // 1
                        "table symbol, " + // 2
                        "file symbol, " + // 3
                        "stage symbol, " + // 4
                        "status symbol, " + // 5
                        "message string," + // 6
                        "rows_handled long," + // 7
                        "rows_imported long," + // 8
                        "errors long" + // 9
                        ") timestamp(ts) partition by DAY",
                sqlExecutionContext
        );
        this.writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, statusTableName, "QuestDB system");
        this.logRetentionDays = configuration.getSqlCopyLogRetentionDays();
        this.textImportExecutionContext = engine.getTextImportExecutionContext();
        enforceLogRetention();
    }

    @Override
    public void close() throws IOException {
        this.importer = Misc.free(importer);
        this.writer = Misc.free(this.writer);
        this.sqlCompiler = Misc.free(sqlCompiler);
        this.sqlExecutionContext = Misc.free(sqlExecutionContext);
    }

    void enforceLogRetention() {
        if (logRetentionDays < 1) {
            writer.truncate();
            return;
        }
        if (writer.getPartitionCount() > 0) {
            partitionsToRemove.clear();
            for (int i = writer.getPartitionCount() - logRetentionDays - 1; i > -1; i--) {
                partitionsToRemove.add(writer.getPartitionTimestamp(i));
            }

            for (int i = 0, sz = partitionsToRemove.size(); i < sz; i++) {
                writer.removePartition(partitionsToRemove.getQuick(i));
            }
        }
    }

    @Override
    protected boolean runSerially() {
        long cursor = requestSubSeq.next();
        if (cursor > -1) {
            task = requestQueue.get(cursor);
            try {
                importer.of(
                        task.getTableName(),
                        task.getFileName(),
                        task.getPartitionBy(),
                        task.getDelimiter(),
                        task.getTimestampColumnName(),
                        task.getTimestampFormat(),
                        task.isHeaderFlag(),
                        textImportExecutionContext.getCircuitBreaker()
                );
                importer.setStatusReporter(updateStatusRef);
                importer.process();
            } catch (TextImportException e) {
                updateStatus(
                        TextImportTask.NO_PHASE,
                        e.isCancelled() ? TextImportTask.STATUS_CANCELLED : TextImportTask.STATUS_FAILED,
                        e.getMessage(),
                        0,
                        0,
                        0
                );
            } finally {
                requestSubSeq.done(cursor);
                textImportExecutionContext.resetActiveImportId();
            }
            enforceLogRetention();
            return true;
        }
        return false;
    }

    private void updateStatus(
            byte phase,
            byte status,
            final CharSequence msg,
            long rowsHandled,
            long rowsImported,
            long errors
    ) {
        if (writer != null) {
            try {
                TableWriter.Row row = writer.newRow(clock.getTicks());
                row.putSym(1, task.getImportId());
                row.putSym(2, task.getTableName());
                row.putSym(3, task.getFileName());
                row.putSym(4, TextImportTask.getPhaseName(phase));
                row.putSym(5, TextImportTask.getStatusName(status));
                row.putStr(6, msg);
                row.putLong(7, rowsHandled);
                row.putLong(8, rowsImported);
                row.putLong(9, errors);
                row.append();
                writer.commit();
            } catch (Throwable th) {
                LOG.error().$("error saving to parallel import log table, unable to insert")
                        .$(", releasing writer and stopping log table updates [table=").$(statusTableName)
                        .$(", error=").$(th)
                        .I$();
                writer = Misc.free(writer);
            }
        }
    }
}
