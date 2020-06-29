/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.microtime.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class TelemetryJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(TelemetryJob.class);

    private final CharSequence tableName = "telemetry";
    private final CharSequence configTableName = "telemetry_config";
    private final QueueConsumer<TelemetryTask> myConsumer = this::newRowConsumer;
    private final StringSink idSink = new StringSink();

    private final MicrosecondClock clock;
    private final PropServerConfiguration configuration;
    private final boolean enabled;
    private final TableWriter writer;
    private final TableWriter writerConfig;
    private final RingQueue<TelemetryTask> queue;
    private final SCSequence subSeq;

    public TelemetryJob(PropServerConfiguration configuration, CairoEngine engine, @Nullable MessageBus messageBus) throws SqlException, CairoException {
        final CairoConfiguration cairoConfig = configuration.getCairoConfiguration();

        this.clock = cairoConfig.getMicrosecondClock();
        this.configuration = configuration;
        this.enabled = configuration.getTelemetryConfiguration().getEnabled();
        this.queue = messageBus.getTelemetryQueue();
        this.subSeq = messageBus.getTelemetrySubSequence();

        try (
                Path path = new Path();
                SqlCompiler compiler = new SqlCompiler(engine)
        ) {
            final SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(messageBus, 1, engine);
            sqlExecutionContext.with(AllowAllCairoSecurityContext.INSTANCE, null, null);

            if (getTableStatus(path, tableName) == TableUtils.TABLE_DOES_NOT_EXIST) {
                compiler.compile("CREATE TABLE " + tableName + " (ts timestamp, event short)", sqlExecutionContext);
            }

            if (getTableStatus(path, configTableName) == TableUtils.TABLE_DOES_NOT_EXIST) {
                compiler.compile("CREATE TABLE " + configTableName + " (id long256, enabled boolean)", sqlExecutionContext);
            }
        }

        try {
            this.writer = new TableWriter(cairoConfig, tableName);
        } catch (CairoException ex) {
            LOG.error().$("could not open [table=").utf8(tableName).$("]").$();
            throw ex;
        }

        // todo: close writerConfig. We currently keep it opened to prevent users from modifying the table.
        // Once we have a permission system, we can use that instead.
        try {
            this.writerConfig = new TableWriter(cairoConfig, configTableName);
        } catch (CairoException ex) {
            LOG.error().$("could not open [table=").utf8(configTableName).$("]").$();
            throw ex;
        }

        try (TableReader reader = new TableReader(cairoConfig, configTableName)) {
            final NanosecondClock nanosecondClock = cairoConfig.getNanosecondClock();

            if (reader.size() > 0) {
                reader.getCursor().getRecord().getLong256(0, idSink);
                final boolean _enabled = reader.getCursor().getRecord().getBool(1);

                if (enabled != _enabled) {
                    final TableWriter.Row row = writerConfig.newRow();
                    row.putLong256(0, idSink);
                    row.putBool(1, enabled);
                    row.append();
                    writerConfig.commit();
                }
            } else {
                final TableWriter.Row row = writerConfig.newRow();
                row.putLong256(0, nanosecondClock.getTicks(), clock.getTicks(), 0, 0);
                row.putBool(1, enabled);
                row.append();
                writerConfig.commit();
                reader.reload();
                reader.getCursor().getRecord().getLong256(0, idSink);
            }
        }

        newRow(TelemetryEvent.UP);
    }

    public int getTableStatus(Path path, CharSequence tableName) {
        return TableUtils.exists(
                configuration.getCairoConfiguration().getFilesFacade(),
                path,
                configuration.getCairoConfiguration().getRoot(),
                tableName,
                0,
                tableName.length()
        );
    }

    private void newRow(short event) {
        if (enabled) {
            final TableWriter.Row row = writer.newRow();
            row.putDate(0, clock.getTicks());
            row.putShort(1, event);
            row.append();
        }
    }

    private void newRowConsumer(TelemetryTask telemetryRow) {
        final TableWriter.Row row = writer.newRow();
        row.putDate(0, telemetryRow.ts);
        row.putShort(1, telemetryRow.event);
        row.append();
    }

    @Override
    public boolean runSerially() {
        if (enabled) {
            subSeq.consumeAll(queue, myConsumer);
            writer.commit();
        }

        return false;
    }

    @Override
    public void close() {
        runSerially();
        newRow(TelemetryEvent.DOWN);
        writer.commit();
        Misc.free(writer);
        Misc.free(writerConfig);
    }
}
