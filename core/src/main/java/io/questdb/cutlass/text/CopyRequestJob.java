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

package io.questdb.cutlass.text;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
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
import io.questdb.std.Numbers;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;

import java.io.Closeable;

import static io.questdb.cutlass.text.CopyTask.getPhaseName;
import static io.questdb.cutlass.text.CopyTask.getStatusName;

public class CopyRequestJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(CopyRequestJob.class);
    private final Clock clock;
    private final CopyContext copyContext;
    private final CairoEngine engine;
    private final int logRetentionDays;
    private final LongList partitionsToRemove = new LongList();
    private final RingQueue<CopyRequestTask> requestQueue;
    private final Sequence requestSubSeq;
    private final TableToken statusTableToken;
    private final StringSink utf16StringSink = new StringSink();
    private final Utf8StringSink utf8StringSink = new Utf8StringSink();
    private ParallelCsvFileImporter parallelImporter;
    private Path path;
    private SerialCsvFileImporter serialImporter;
    private SqlExecutionContextImpl sqlExecutionContext;
    private CopyRequestTask task;
    private TableWriter writer;
    private final ParallelCsvFileImporter.PhaseStatusReporter updateStatusRef = this::updateStatus;

    public CopyRequestJob(final CairoEngine engine, int workerCount) throws SqlException {
        try {
            this.requestQueue = engine.getMessageBus().getTextImportRequestQueue();
            this.requestSubSeq = engine.getMessageBus().getTextImportRequestSubSeq();
            this.parallelImporter = new ParallelCsvFileImporter(engine, workerCount);
            this.serialImporter = new SerialCsvFileImporter(engine);

            CairoConfiguration configuration = engine.getConfiguration();
            this.clock = configuration.getMicrosecondClock();

            // Set sharedQueryWorkerCount as 0, no need to do parallel query execution in this job,
            this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 0);
            this.sqlExecutionContext.with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null);
            final String statusTableName = configuration.getSystemTableNamePrefix() + "text_import_log";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                this.statusTableToken = compiler.query()
                        .$("CREATE TABLE IF NOT EXISTS \"")
                        .$(statusTableName)
                        .$("\" (" +
                                "ts timestamp, " + // 0
                                "id string, " + // 1
                                "table_name symbol, " + // 2
                                "file symbol, " + // 3
                                "phase symbol, " + // 4
                                "status symbol, " + // 5
                                "message string," + // 6
                                "rows_handled long," + // 7
                                "rows_imported long," + // 8
                                "errors long" + // 9
                                ") timestamp(ts) partition by DAY BYPASS WAL"
                        )
                        .createTable(sqlExecutionContext);
            }

            this.writer = engine.getWriter(statusTableToken, "QuestDB system");
            this.logRetentionDays = configuration.getSqlCopyLogRetentionDays();
            this.copyContext = engine.getCopyContext();
            this.path = new Path();
            this.engine = engine;
            enforceLogRetention();
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    @Override
    public void close() {
        this.parallelImporter = Misc.free(parallelImporter);
        this.serialImporter = Misc.free(serialImporter);
        this.writer = Misc.free(this.writer);
        this.sqlExecutionContext = Misc.free(sqlExecutionContext);
        this.path = Misc.free(path);
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
                final int idColumnType = writer.getMetadata().getColumnType(1);
                if (idColumnType == ColumnType.VARCHAR) {
                    utf8StringSink.clear();
                    Numbers.appendHex(utf8StringSink, task.getCopyID(), true);
                    row.putVarchar(1, utf8StringSink);
                } else {
                    utf16StringSink.clear();
                    Numbers.appendHex(utf16StringSink, task.getCopyID(), true);
                    row.putStr(1, utf16StringSink);
                }
                row.putSym(2, task.getTableName());
                row.putSym(3, task.getFileName());
                row.putSym(4, CopyTask.getPhaseName(phase));
                row.putSym(5, CopyTask.getStatusName(status));
                final int msgColumnType = writer.getMetadata().getColumnType(6);
                if (msgColumnType == ColumnType.VARCHAR) {
                    utf8StringSink.clear();
                    utf8StringSink.put(msg);
                    row.putVarchar(6, utf8StringSink);
                } else {
                    row.putStr(6, msg);
                }
                row.putLong(7, rowsHandled);
                row.putLong(8, rowsImported);
                row.putLong(9, errors);
                row.append();
                writer.commit();
            } catch (Throwable th) {
                LOG.error()
                        .$("could not update status table [importId=").$hexPadded(task.getCopyID())
                        .$(", statusTableName=").$(statusTableToken)
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

            // if we closed the writer, we need to reopen it again
            if (writer == null) {
                try {
                    writer = engine.getWriter(statusTableToken, "QuestDB system");
                } catch (Throwable e) {
                    LOG.error()
                            .$("could not re-open writer [table=").$(statusTableToken)
                            .$(", error=`").$(e).$('`')
                            .I$();
                }
            }
        }
    }

    private boolean useParallelImport() {
        TableToken tableToken = engine.getTableTokenIfExists(task.getTableName());
        if (engine.getTableStatus(path, tableToken) != TableUtils.TABLE_EXISTS) {
            return task.getPartitionBy() >= 0 && task.getPartitionBy() != PartitionBy.NONE;
        }
        try (TableReader reader = engine.getReader(tableToken)) {
            return PartitionBy.isPartitioned(reader.getPartitionedBy());
        }
    }

    void enforceLogRetention() {
        if (writer != null) {
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
    }

    @Override
    protected boolean runSerially() {
        long cursor = requestSubSeq.next();
        if (cursor > -1) {
            task = requestQueue.get(cursor);
            try {
                if (useParallelImport()) {
                    parallelImporter.setStatusReporter(updateStatusRef);
                    parallelImporter.of(
                            task.getTableName(),
                            task.getFileName(),
                            task.getCopyID(),
                            task.getPartitionBy(),
                            task.getDelimiter(),
                            task.getTimestampColumnName(),
                            task.getTimestampFormat(),
                            task.isHeaderFlag(),
                            copyContext.getCircuitBreaker(),
                            task.getAtomicity()
                    );
                    parallelImporter.setStatusReporter(updateStatusRef);
                    parallelImporter.process(task.getSecurityContext());
                } else {
                    serialImporter.of(
                            task.getTableName(),
                            task.getFileName(),
                            task.getCopyID(),
                            task.getDelimiter(),
                            task.getTimestampColumnName(),
                            task.getTimestampFormat(),
                            task.isHeaderFlag(),
                            copyContext.getCircuitBreaker(),
                            task.getAtomicity()
                    );
                    serialImporter.setStatusReporter(updateStatusRef);
                    serialImporter.process(task.getSecurityContext());
                }
            } catch (TextImportException e) {
                updateStatus(
                        CopyTask.NO_PHASE,
                        e.isCancelled() ? CopyTask.STATUS_CANCELLED : CopyTask.STATUS_FAILED,
                        e.getMessage(),
                        0,
                        0,
                        0
                );
            } finally {
                requestSubSeq.done(cursor);
                copyContext.clear();
            }
            enforceLogRetention();
            return true;
        }
        return false;
    }
}
