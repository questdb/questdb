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
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cutlass.text.CopyContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cutlass.text.CopyImportTask.getPhaseName;
import static io.questdb.cutlass.text.CopyImportTask.getStatusName;

public class CopyExportRequestJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(CopyExportRequestJob.class);
    private final MicrosecondClock clock;
    private final CopyContext copyContext;
    private final CairoEngine engine;
    private final int logRetentionDays;
    private final RingQueue<CopyExportRequestTask> requestQueue;
    private final Sequence requestSubSeq;
    private final TableToken statusTableToken;
    private final Utf8StringSink utf8StringSink = new Utf8StringSink();
    private Path path;
    private SerialParquetExporter serialExporter;
    private SqlExecutionContextImpl sqlExecutionContext;
    private CopyExportRequestTask task;
    private WalWriter writer;

    public CopyExportRequestJob(final CairoEngine engine) throws SqlException {
        try {
            this.requestQueue = engine.getMessageBus().getCopyExportRequestQueue();
            this.requestSubSeq = engine.getMessageBus().getCopyExportRequestSubSeq();
            this.path = new Path();
            this.serialExporter = new SerialParquetExporter(engine, path);

            CairoConfiguration configuration = engine.getConfiguration();
            this.clock = configuration.getMicrosecondClock();

            this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
            this.sqlExecutionContext.with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null);
            final String statusTableName = configuration.getSystemTableNamePrefix() + "copy_export_log";
            this.logRetentionDays = configuration.getSqlCopyLogRetentionDays();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                this.statusTableToken = compiler.query()
                        .$("CREATE TABLE IF NOT EXISTS \"")
                        .$(statusTableName)
                        .$("\" (" +
                                "ts TIMESTAMP, " + // 0
                                "id VARCHAR, " + // 1
                                "table_name SYMBOL, " + // 2
                                "file SYMBOL, " + // 3
                                "phase SYMBOL, " + // 4
                                "status SYMBOL, " + // 5
                                "message VARCHAR," + // 6
                                "errors LONG" + // 7
                                ") timestamp(ts) PARTITION BY DAY\n" +
                                "TTL " + logRetentionDays + " DAYS WAL;"
                        )
                        .createTable(sqlExecutionContext);
            }

            this.writer = engine.getWalWriter(statusTableToken);
            this.copyContext = engine.getCopyContext();

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
        this.sqlExecutionContext = Misc.free(sqlExecutionContext);
        this.path = Misc.free(path);
    }

    private void updateStatus(
            byte phase,
            byte status,
            @Nullable final CharSequence msg,
            long errors
    ) {
        if (writer != null) {
            try {
                TableWriter.Row row = writer.newRow(clock.getTicks());
                Numbers.appendHex(utf8StringSink, task.getCopyID(), true);
                row.putVarchar(1, utf8StringSink);
                row.putSym(2, task.getTableName());
                row.putSym(3, task.getFileName());
                row.putSym(4, CopyExportTask.getPhaseName(phase));
                row.putSym(5, CopyExportTask.getStatusName(status));
                utf8StringSink.clear();
                utf8StringSink.put(msg);
                row.putVarchar(6, utf8StringSink);
                row.putLong(7, errors);
                row.append();
                writer.commit();
            } catch (Throwable th) {
                LOG.error()
                        .$("could not update status table [exportId=").$hexPadded(task.getCopyID())
                        .$(", statusTableName=").$(statusTableToken)
                        .$(", tableName=").$(task.getTableName())
                        .$(", fileName=").$(task.getFileName())
                        .$(", phase=").$(getPhaseName(phase))
                        .$(", status=").$(getStatusName(phase))
                        .$(", msg=").$(msg)
                        .$(", errors=").$(errors)
                        .$(", error=`").$(th).$('`')
                        .I$();
                writer = Misc.free(writer);
            }

            // if we closed the writer, we need to reopen it again
            if (writer == null) {
                try {
                    writer = engine.getWalWriter(statusTableToken);
                } catch (Throwable e) {
                    LOG.error()
                            .$("could not re-open writer [table=").$(statusTableToken)
                            .$(", error=`").$(e).$('`')
                            .I$();
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
                serialExporter.of(
                        task.getTableName(),
                        task.getFileName(),
                        task.getCopyID(),
                        copyContext.getCircuitBreaker(),
                        this::updateStatus
                );
                serialExporter.process(task.getSecurityContext());
            } catch (CopyExportException e) {
                updateStatus(
                        CopyExportTask.NO_PHASE,
                        e.isCancelled() ? CopyExportTask.STATUS_CANCELLED : CopyExportTask.STATUS_FAILED,
                        e.getMessage(),
                        0
                );
            } finally {
                requestSubSeq.done(cursor);
                copyContext.clear();
            }
            return true;
        }
        return false;
    }
}
