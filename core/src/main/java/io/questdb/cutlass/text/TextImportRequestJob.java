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

import io.questdb.cairo.*;
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
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.cutlass.text.TextImportTask.getPhaseName;
import static io.questdb.cutlass.text.TextImportTask.getStatusName;

public class TextImportRequestJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(TextImportRequestJob.class);

    private final RingQueue<TextImportRequestTask> requestQueue;
    private final Sequence requestSubSeq;
    private final CharSequence statusTableName;
    private final MicrosecondClock clock;
    private final int logRetentionDays;
    private final LongList partitionsToRemove = new LongList();
    private final TextImportExecutionContext textImportExecutionContext;
    private TableWriter writer;
    private SqlCompiler sqlCompiler;
    private SqlExecutionContextImpl sqlExecutionContext;
    private TextImportRequestTask task;
    private final ParallelCsvFileImporter.PhaseStatusReporter updateStatusRef = this::updateStatus;
    private Path path;
    private ParallelCsvFileImporter parallelImporter;
    private final CairoEngine engine;
    private SerialCsvFileImporter serialImporter;


    public TextImportRequestJob(
            final CairoEngine engine,
            int workerCount,
            @Nullable FunctionFactoryCache functionFactoryCache
    ) throws SqlException {
        this.requestQueue = engine.getMessageBus().getTextImportRequestQueue();
        this.requestSubSeq = engine.getMessageBus().getTextImportRequestSubSeq();
        this.parallelImporter = new ParallelCsvFileImporter(engine, workerCount);
        this.serialImporter = new SerialCsvFileImporter(engine);

        CairoConfiguration configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.statusTableName = configuration.getSystemTableNamePrefix() + "text_import_log";

        this.sqlCompiler = new SqlCompiler(engine, functionFactoryCache, null);
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        this.sqlExecutionContext.with(AllowAllCairoSecurityContext.INSTANCE, null, null);
        this.sqlCompiler.compile(
                "CREATE TABLE IF NOT EXISTS \"" + statusTableName + "\" (" +
                        "ts timestamp, " + // 0
                        "id long, " + // 1
                        "table symbol, " + // 2
                        "file symbol, " + // 3
                        "phase symbol, " + // 4
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
        this.path = new Path();
        this.engine = engine;
        enforceLogRetention();
    }

    @Override
    public void close() throws IOException {
        this.parallelImporter = Misc.free(parallelImporter);
        this.serialImporter = Misc.free(serialImporter);
        this.writer = Misc.free(this.writer);
        this.sqlCompiler = Misc.free(sqlCompiler);
        this.sqlExecutionContext = Misc.free(sqlExecutionContext);
        this.path = Misc.free(path);
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

    private boolean useParallelImport() {
        if (task.getTimestampColumnName() != null) {
            return true;
        }
        if (engine.getStatus(task.getSecurityContext(), path, task.getTableName()) != TableUtils.TABLE_EXISTS) {
            return false;
        }
        try (TableReader reader = engine.getReader(task.getSecurityContext(), task.getTableName())) {
            return PartitionBy.isPartitioned(reader.getPartitionedBy());
        }
    }

    @Override
    protected boolean runSerially() {
        long cursor = requestSubSeq.next();
        if (cursor > -1) {
            task = requestQueue.get(cursor);
            try {
                if (useParallelImport()) {
                    LOG.info().$("Using ParallelImport strategy. [table=").$(task.getTableName()).$(", timestamp=").$(task.getTimestampColumnName()).I$();
                    parallelImporter.of(
                            task.getTableName(),
                            task.getFileName(),
                            task.getPartitionBy(),
                            task.getDelimiter(),
                            task.getTimestampColumnName(),
                            task.getTimestampFormat(),
                            task.isHeaderFlag(),
                            textImportExecutionContext.getCircuitBreaker(),
                            task.getSecurityContext()
                    );
                    parallelImporter.setStatusReporter(updateStatusRef);
                    parallelImporter.process();
                } else {
                    LOG.info().$("Using SerialImport strategy. [table=").$(task.getTableName()).$(", timestamp=").$(task.getTimestampColumnName()).I$();
                    serialImporter.of(
                            task.getTableName(),
                            task.getFileName(),
                            task.isHeaderFlag(),
                            task.getAtomicity(),
                            textImportExecutionContext.getCircuitBreaker(),
                            task.getSecurityContext()

                    );
                    serialImporter.setStatusReporter(updateStatusRef);
                    serialImporter.process();
                }
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
                row.putLong(1, task.getImportId());
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
                LOG.error()
                        .$("could not update status table [importId=").$(task.getImportId())
                        .$(", statusTableName=").$(statusTableName)
                        .$(", tableName=").$(task.getTableName())
                        .$(", fileName=").$(task.getFileName())
                        .$(", phase=").$(getPhaseName(phase))
                        .$(", status=").$(getStatusName(phase))
                        .$(", msg=").$(msg)
                        .$(", rowsHandled=").$(rowsHandled)
                        .$(", rowsImported=").$(rowsImported)
                        .$(", errors=").$(errors)
                        .$(", error=`").$(th).$('`')
                        .I$();
                writer = Misc.free(writer);
            }
        }
    }
}
