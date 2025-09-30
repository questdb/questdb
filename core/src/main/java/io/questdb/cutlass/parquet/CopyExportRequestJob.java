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
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.network.NetworkError;
import io.questdb.network.SuspendEvent;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class CopyExportRequestJob extends AbstractQueueConsumerJob<CopyExportRequestTask> implements Closeable {
    private static final Log LOG = LogFactory.getLog(CopyExportRequestJob.class);
    private final MicrosecondClock clock;
    private final CairoEngine engine;
    private final TableToken statusTableToken;
    private final Utf8StringSink utf8StringSink = new Utf8StringSink();
    private SerialParquetExporter serialExporter;

    public CopyExportRequestJob(final CairoEngine engine, int workerId) throws SqlException {
        super(engine.getMessageBus().getCopyExportRequestQueue(), engine.getMessageBus().getCopyExportRequestSubSeq());
        try {
            assert workerId >= 0;
            SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
            serialExporter = new SerialParquetExporter(sqlExecutionContext);
            CairoConfiguration configuration = engine.getConfiguration();
            this.clock = configuration.getMicrosecondClock();
            final String statusTableName = configuration.getSystemTableNamePrefix() + "copy_export_log";
            int logRetentionDays = configuration.getSqlCopyLogRetentionDays();

            if (workerId == 0) {
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    sqlExecutionContext.with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null);
                    this.statusTableToken = compiler.query()
                            .$("CREATE TABLE IF NOT EXISTS \"")
                            .$(statusTableName)
                            .$("\" (" +
                                    "ts TIMESTAMP, " + // 0
                                    "id VARCHAR, " + // 1
                                    "table_name SYMBOL, " + // 2
                                    "export_dir SYMBOL, " + // 3
                                    "num_exported_files INT, " + // 4
                                    "phase SYMBOL, " + // 5
                                    "status SYMBOL, " + // 6
                                    "message VARCHAR, " + // 7
                                    "errors LONG" + // 8
                                    ") timestamp(ts) PARTITION BY DAY\n" +
                                    "TTL " + logRetentionDays + " DAYS WAL;"
                            )
                            .createTable(sqlExecutionContext);
                }
            } else {
                this.statusTableToken = engine.getTableTokenIfExists(statusTableName);
                if (this.statusTableToken == null) {
                    throw SqlException.$(0, "could not find export log table ").put(statusTableName);
                }
            }
            this.engine = engine;
            engine.getCopyExportContext().setReporter(this::updateStatus);
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    @Override
    public void close() {
        this.serialExporter = Misc.free(serialExporter);
    }

    public void updateStatus(
            CopyExportRequestTask.Phase phase,
            CopyExportRequestTask.Status status,
            CopyExportRequestTask task,
            CharSequence exportDir,
            int numOfFiles,
            @Nullable final CharSequence msg,
            long errors
    ) {
        try (TableWriter writer = engine.getWriter(statusTableToken, "QuestDB system")) {
            TableWriter.Row row = writer.newRow(clock.getTicks());
            utf8StringSink.clear();
            Numbers.appendHex(utf8StringSink, task.getCopyID(), true);
            row.putVarchar(1, utf8StringSink);
            row.putSym(2, task.getTableName());
            row.putSym(3, exportDir);
            row.putInt(4, numOfFiles);
            row.putSym(5, phase.getName());
            row.putSym(6, status.getName());
            utf8StringSink.clear();
            utf8StringSink.put(msg);
            row.putVarchar(7, utf8StringSink);
            row.putLong(8, errors);
            row.append();
            writer.commit();
        } catch (Throwable e) {
            LOG.error()
                    .$("could not update status table [exportId=").$hexPadded(task.getCopyID())
                    .$(", statusTableName=").$(statusTableToken)
                    .$(", tableName=").$(task.getTableName())
                    .$(", exportDir=").$(exportDir)
                    .$(", numOfFiles=").$(numOfFiles)
                    .$(", phase=").$(phase.getName())
                    .$(", status=").$(status.getName())
                    .$(", msg=").$(msg)
                    .$(", errors=").$(errors)
                    .$(", error=`").$(e).$('`')
                    .I$();
        }

        if (task.getResult() != null) {
            task.getResult().report(phase, status, msg);
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        CopyExportRequestTask task = queue.get(cursor);
        SqlExecutionCircuitBreaker circuitBreaker = task.getCircuitBreaker();
        CopyExportRequestTask.Phase phase = CopyExportRequestTask.Phase.WAITING;
        boolean triggered = false;
        try {
            if (circuitBreaker.checkIfTripped()) {
                LOG.errorW().$("copy was cancelled [copyId=").$hexPadded(task.getCopyID()).$(']').$();
                throw CopyExportException.instance(phase, -1).put("cancelled by user").setInterruption(true).setCancellation(true);
            }
            this.updateStatus(CopyExportRequestTask.Phase.WAITING, CopyExportRequestTask.Status.FINISHED, task, null, Numbers.INT_NULL, "", 0);
            serialExporter.getSqlExecutionContext().with(task.getSecurityContext(), null, null, -1, circuitBreaker);
            serialExporter.of(
                    task,
                    circuitBreaker,
                    this::updateStatus
            );
            phase = serialExporter.process(); // throws CopyExportException

            if (task.getSuspendEvent() != null) {
                phase = CopyExportRequestTask.Phase.SENDING_DATA;
                utf8StringSink.clear();
                utf8StringSink.put("sending signal to waiting thread [fd=").put(task.getSuspendEvent().getFd()).put(']');
                triggered = true;
                updateStatus(phase, CopyExportRequestTask.Status.STARTED, task,
                        null, Numbers.INT_NULL, utf8StringSink.asAsciiCharSequence(), 0);
                task.getSuspendEvent().trigger();
                updateStatus(phase, CopyExportRequestTask.Status.FINISHED, task,
                        null, Numbers.INT_NULL, "signal sent", 0);
            }
            updateStatus(CopyExportRequestTask.Phase.SUCCESS, CopyExportRequestTask.Status.FINISHED, task, serialExporter.getExportPath(), serialExporter.getNumOfFiles(), null, 0);
        } catch (CopyExportException e) {
            updateStatus(
                    e.getPhase(),
                    circuitBreaker.checkIfTripped() ? CopyExportRequestTask.Status.CANCELLED : CopyExportRequestTask.Status.FAILED,
                    task,
                    null,
                    Numbers.INT_NULL,
                    e.getFlyweightMessage(),
                    e.getErrno()
            );
        } catch (NetworkError e) { // SuspendEvent::trigger() may throw
            updateStatus(
                    phase,
                    circuitBreaker.checkIfTripped() ? CopyExportRequestTask.Status.CANCELLED : CopyExportRequestTask.Status.FAILED,
                    task,
                    null,
                    Numbers.INT_NULL,
                    e.getFlyweightMessage(),
                    e.getErrno()
            );
        } catch (Throwable e) {
            updateStatus(
                    phase,
                    circuitBreaker.checkIfTripped() ? CopyExportRequestTask.Status.CANCELLED : CopyExportRequestTask.Status.FAILED,
                    task,
                    null,
                    Numbers.INT_NULL,
                    e.getMessage(),
                    -1
            );
        } finally {
            SuspendEvent event = task.getSuspendEvent();
            task.clear();
            subSeq.done(cursor);
            if (!triggered && event != null) {
                event.trigger();
            }
        }
        return true;
    }
}
