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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.QueueConsumer;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.NanosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class TelemetryJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(TelemetryJob.class);

    private final static CharSequence tableName = "telemetry";
    private final static CharSequence configTableName = "telemetry_config";
    private final MicrosecondClock clock;
    private final CairoConfiguration configuration;
    private final RingQueue<TelemetryTask> queue;
    private final SCSequence subSeq;
    private boolean enabled;
    private TableWriter writer;
    private final QueueConsumer<TelemetryTask> myConsumer = this::newRowConsumer;
    private TableWriter writerConfig;

    public TelemetryJob(CairoEngine engine) throws SqlException {
        this(engine, null);
    }

    public TelemetryJob(CairoEngine engine, @Nullable FunctionFactoryCache functionFactoryCache) throws SqlException {
        this.configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.enabled = configuration.getTelemetryConfiguration().getEnabled();
        this.queue = engine.getTelemetryQueue();
        this.subSeq = engine.getTelemetrySubSequence();

        try (final SqlCompiler compiler = new SqlCompiler(engine, engine.getMessageBus(), functionFactoryCache)) {
            final SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1, engine.getMessageBus());
            sqlExecutionContext.with(AllowAllCairoSecurityContext.INSTANCE, null, null);

            try (final Path path = new Path()) {
                if (getTableStatus(path, tableName) == TableUtils.TABLE_DOES_NOT_EXIST) {
                    compiler.compile("CREATE TABLE " + tableName + " (created timestamp, event short, origin short) timestamp(created)", sqlExecutionContext);
                }

                if (getTableStatus(path, configTableName) == TableUtils.TABLE_DOES_NOT_EXIST) {
                    compiler.compile("CREATE TABLE " + configTableName + " (id long256, enabled boolean)", sqlExecutionContext);
                }
            }

            try {
                this.writer = new TableWriter(configuration, tableName);
            } catch (CairoException ex) {
                LOG.error()
                        .$("could not open [table=`").utf8(tableName)
                        .$("`, ex=").$(ex.getFlyweightMessage())
                        .$(", errno=").$(ex.getErrno())
                        .$(']').$();
                enabled = false;
                return;
            }

            // todo: close writerConfig. We currently keep it opened to prevent users from
            // modifying the table.
            // Once we have a permission system, we can use that instead.
            try {
                this.writerConfig = new TableWriter(configuration, configTableName);
            } catch (CairoException ex) {
                Misc.free(writer);
                LOG.error()
                        .$("could not open [table=`").utf8(configTableName)
                        .$("`, ex=").$(ex.getFlyweightMessage())
                        .$(", errno=").$(ex.getErrno())
                        .$(']').$();
                enabled = false;
                return;
            }

            final CompiledQuery cc = compiler.compile(configTableName + " LIMIT -1", sqlExecutionContext);

            try (final RecordCursor cursor = cc.getRecordCursorFactory().getCursor(sqlExecutionContext)) {
                if (cursor.hasNext()) {
                    final Record record = cursor.getRecord();
                    final boolean _enabled = record.getBool(1);

                    // if the configuration changed to enable or disable telemetry
                    // we need to update the table to reflect that
                    if (enabled != _enabled) {
                        final TableWriter.Row row = writerConfig.newRow();
                        final Long256 l256 = record.getLong256A(0);
                        row.putLong256(0, l256);
                        row.putBool(1, enabled);
                        row.append();
                        writerConfig.commit();
                        LOG.info()
                                .$("instance config changes [id=").$256(l256.getLong0(), l256.getLong1(), 0, 0)
                                .$(", enabled=").$(this.enabled)
                                .$(']').$();
                    } else {
                        final Long256 l256 = record.getLong256A(0);
                        LOG.error()
                                .$("instance [id=").$256(l256.getLong0(), l256.getLong1(), 0, 0)
                                .$(", enabled=").$(this.enabled)
                                .$(']').$();
                    }
                } else {
                    // if there are no record for telemetry id we need to create one using clocks
                    final MicrosecondClock clock = configuration.getMicrosecondClock();
                    final NanosecondClock nanosecondClock = configuration.getNanosecondClock();
                    final TableWriter.Row row = writerConfig.newRow();
                    final long a = nanosecondClock.getTicks();
                    final long b = clock.getTicks();
                    row.putLong256(0, a, b, 0, 0);
                    row.putBool(1, enabled);
                    row.append();
                    writerConfig.commit();
                    LOG.info()
                            .$("new instance [id=").$256(a, b, 0, 0)
                            .$(", enabled=").$(this.enabled)
                            .$(']').$();
                }
            }

            newRow(Telemetry.SYSTEM_EVENT_UP);
        }
    }

    @Override
    public void close() {
        if (enabled) {
            runSerially();
            newRow(Telemetry.SYSTEM_EVENT_DOWN);
            writer.commit();
            Misc.free(writer);
            Misc.free(writerConfig);
        }
    }

    public int getTableStatus(Path path, CharSequence tableName) {
        return TableUtils.exists(
                configuration.getFilesFacade(),
                path,
                configuration.getRoot(),
                tableName,
                0,
                tableName.length()
        );
    }

    @Override
    public boolean runSerially() {
        if (enabled && subSeq.consumeAll(queue, myConsumer)) {
            writer.commit();
        }
        return false;
    }

    private void newRow(short event) {
        if (enabled) {
            try {
                final TableWriter.Row row = writer.newRow(clock.getTicks());
                row.putShort(1, event);
                row.putShort(2, Telemetry.ORIGIN_INTERNAL);
                row.append();
            } catch (CairoException e) {
                LOG.error()
                        .$("Could not insert a new row in telemetry table [error=").$(e.getFlyweightMessage())
                        .$(", errno=").$(e.getErrno())
                        .$(']').$();
            }
        }
    }

    private void newRowConsumer(TelemetryTask telemetryRow) {
        try {
            final TableWriter.Row row = writer.newRow(telemetryRow.created);
            row.putShort(1, telemetryRow.event);
            row.putShort(2, telemetryRow.origin);
            row.append();
        } catch (CairoException e) {
            LOG.error()
                    .$("Could not insert a new row in telemetry table [error=").$(e.getFlyweightMessage())
                    .$(", errno=").$(e.getErrno())
                    .$(']').$();
        }
    }
}
