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

package io.questdb.cutlass.parquet;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.network.NetworkError;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class CopyExportRequestJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(CopyExportRequestJob.class);
    private final MicrosecondClock clock;
    private final CopyExportContext copyContext;
    private final CairoEngine engine;
    private final RingQueue<CopyExportRequestTask> requestQueue;
    private final Sequence requestSubSeq;
    private final SqlExecutionContextImpl sqlExecutionContext;
    private final TableToken statusTableToken;
    private final Utf8StringSink utf8StringSink = new Utf8StringSink();
    private SerialParquetExporter serialExporter;
    private TableWriter writer;

    public CopyExportRequestJob(final CairoEngine engine) throws SqlException {
        try {
            this.requestQueue = engine.getMessageBus().getCopyExportRequestQueue();
            this.requestSubSeq = engine.getMessageBus().getCopyExportRequestSubSeq();
            this.serialExporter = new SerialParquetExporter(engine);

            CairoConfiguration configuration = engine.getConfiguration();
            this.clock = configuration.getMicrosecondClock();

            final String statusTableName = configuration.getSystemTableNamePrefix() + "copy_export_log";
            int logRetentionDays = configuration.getSqlCopyLogRetentionDays();
            sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                sqlExecutionContext.with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null);
                this.statusTableToken = compiler.query()
                        .$("CREATE TABLE IF NOT EXISTS \"")
                        .$(statusTableName)
                        .$("\" (" +
                                "ts TIMESTAMP, " + // 0
                                "id VARCHAR, " + // 1
                                "table_name SYMBOL, " + // 2
                                "files VARCHAR, " + // 3
                                "phase SYMBOL, " + // 4
                                "status SYMBOL, " + // 5
                                "message VARCHAR, " + // 6
                                "errors LONG" + // 7
                                ") timestamp(ts) PARTITION BY DAY\n" +
                                "TTL " + logRetentionDays + " DAYS BYPASS WAL;"
                        )
                        .createTable(sqlExecutionContext);
            }

            this.writer = engine.getWriter(statusTableToken, "QuestDB system");
            this.copyContext = engine.getCopyExportContext();
            this.copyContext.setReporter(this::updateStatus);
            this.engine = engine;
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    @Override
    public void close() {
        this.serialExporter = Misc.free(serialExporter);
        this.writer = Misc.free(this.writer);
    }

    public void updateStatus(
            CopyExportRequestTask.Phase phase,
            CopyExportRequestTask.Status status,
            CopyExportRequestTask task,
            Utf8Sequence files,
            @Nullable final CharSequence msg,
            long errors
    ) {
        if (writer != null) {
            try {
                TableWriter.Row row = writer.newRow(clock.getTicks());
                utf8StringSink.clear();
                Numbers.appendHex(utf8StringSink, task.getCopyID(), true);
                row.putVarchar(1, utf8StringSink);
                row.putSym(2, task.getTableName());
                row.putVarchar(3, files);
                row.putSym(4, phase.getName());
                row.putSym(5, status.getName());
                utf8StringSink.clear();
                utf8StringSink.put(msg);
                row.putVarchar(6, utf8StringSink);
                row.putLong(7, errors);
                row.append();
                writer.commit();
            } catch (Throwable th) {
                writer = Misc.free(writer);
                LOG.error()
                        .$("could not update status table [exportId=").$hexPadded(task.getCopyID())
                        .$(", statusTableName=").$(statusTableToken)
                        .$(", tableName=").$(task.getTableName())
                        .$(", fileName=").$(files)
                        .$(", phase=").$(phase.getName())
                        .$(", status=").$(status.getName())
                        .$(", msg=").$(msg)
                        .$(", errors=").$(errors)
                        .$(", error=`").$(th).$('`')
                        .I$();
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
            if (task.getResult() != null) {
                task.getResult().report(phase, status, msg);
            }
        }
    }

    @Override
    protected boolean runSerially() {
        long cursor = requestSubSeq.next();
        if (cursor > -1) {
            CopyExportRequestTask task = requestQueue.get(cursor);
            CopyExportRequestTask.Phase phase = CopyExportRequestTask.Phase.WAITING;
            try {
                if (copyContext.getCircuitBreaker().checkIfTripped()) {
                    LOG.errorW().$("copy was cancelled [copyId=").$hexPadded(task.getCopyID()).$(']').$();
                    throw CopyExportException.instance(phase, -1).put("cancelled by user").setInterruption(true).setCancellation(true);
                }
                this.updateStatus(CopyExportRequestTask.Phase.WAITING, CopyExportRequestTask.Status.FINISHED, task, null, "", 0);
                sqlExecutionContext.with(task.getSecurityContext(), null, null, -1, copyContext.getCircuitBreaker());
                serialExporter.of(
                        task,
                        copyContext.getCircuitBreaker(),
                        this::updateStatus
                );

                phase = serialExporter.process(sqlExecutionContext); // throws CopyExportException

                if (task.getSuspendEvent() != null) {
                    phase = CopyExportRequestTask.Phase.SENDING_DATA;
                    utf8StringSink.clear();
                    utf8StringSink.put("sending signal to waiting thread [fd=").put(task.getSuspendEvent().getFd()).put(']');
                    updateStatus(phase, CopyExportRequestTask.Status.STARTED, task,
                            null, utf8StringSink.asAsciiCharSequence(), 0);
                    task.getSuspendEvent().trigger();
                    updateStatus(phase, CopyExportRequestTask.Status.FINISHED, task,
                            null, "signal sent", 0);
                }
                Utf8StringSink files = serialExporter.getFiles();
                updateStatus(CopyExportRequestTask.Phase.SUCCESS, CopyExportRequestTask.Status.FINISHED, task, files, files.size() == 0 ? "Empty Table" : "", 0);
            } catch (CopyExportException e) {
                updateStatus(
                        e.getPhase(),
                        copyContext.getCircuitBreaker().checkIfTripped() ? CopyExportRequestTask.Status.CANCELLED : CopyExportRequestTask.Status.FAILED,
                        task,
                        null,
                        e.getFlyweightMessage(),
                        e.getErrno()
                );
            } catch (NetworkError e) { // SuspendEvent::trigger() may throw
                updateStatus(
                        phase,
                        copyContext.getCircuitBreaker().checkIfTripped() ? CopyExportRequestTask.Status.CANCELLED : CopyExportRequestTask.Status.FAILED,
                        task,
                        null,
                        e.getFlyweightMessage(),
                        e.getErrno()
                );
            } catch (Throwable e) {
                updateStatus(
                        phase,
                        copyContext.getCircuitBreaker().checkIfTripped() ? CopyExportRequestTask.Status.CANCELLED : CopyExportRequestTask.Status.FAILED,
                        task,
                        null,
                        e.getMessage(),
                        -1
                );
            } finally {
                task.clear();
                requestSubSeq.done(cursor);
                copyContext.clear();
            }

            return true;
        }
        return false;
    }
}
