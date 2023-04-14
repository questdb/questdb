/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.mp.QueueConsumer;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.std.Misc;
import io.questdb.std.ObjectFactory;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.tasks.AbstractTelemetryTask;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

public final class Telemetry<T extends AbstractTelemetryTask> implements Closeable {
    private static final Log LOG = LogFactory.getLog(Telemetry.class);

    private final boolean enabled;
    private MicrosecondClock clock;
    private MPSequence telemetryPubSeq;
    private RingQueue<T> telemetryQueue;
    private SCSequence telemetrySubSeq;
    private TelemetryType<T> telemetryType;
    private TableWriter writer;

    private final QueueConsumer<T> taskConsumer = this::consume;

    public Telemetry(TelemetryTypeBuilder<T> builder, CairoConfiguration configuration) {
        TelemetryType<T> type = builder.build(configuration);
        final TelemetryConfiguration telemetryConfiguration = type.getTelemetryConfiguration(configuration);
        enabled = telemetryConfiguration.getEnabled();
        if (enabled) {
            this.telemetryType = type;
            clock = configuration.getMicrosecondClock();
            telemetryQueue = new RingQueue<>(type.getTaskFactory(), telemetryConfiguration.getQueueCapacity());
            telemetryPubSeq = new MPSequence(telemetryQueue.getCycle());
            telemetrySubSeq = new SCSequence();
            telemetryPubSeq.then(telemetrySubSeq).then(telemetryPubSeq);
        }
    }

    @Override
    public void close() {
        if (writer == null) {
            return;
        }

        consumeAll();
        Misc.free(telemetryQueue);

        telemetryType.logStatus(writer, TelemetrySystemEvent.SYSTEM_DOWN, clock.getTicks());
        writer = Misc.free(writer);
    }

    public void consume(T task) {
        task.writeTo(writer, clock.getTicks());
    }

    public void consumeAll() {
        if (enabled && telemetrySubSeq.consumeAll(telemetryQueue, taskConsumer)) {
            writer.commit();
        }
    }

    public void init(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (!enabled) {
            return;
        }
        String tableName = telemetryType.getTableName();
        compiler.compile(telemetryType.getCreateSql(), sqlExecutionContext);
        final TableToken tableToken = engine.verifyTableName(tableName);
        try {
            writer = engine.getWriter(tableToken, "telemetry");
        } catch (CairoException ex) {
            LOG.error()
                    .$("could not open [table=`").utf8(tableToken.getTableName())
                    .$("`, ex=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .$(']').$();
        }

        telemetryType.logStatus(writer, TelemetrySystemEvent.SYSTEM_UP, clock.getTicks());
    }

    public boolean isEnabled() {
        return enabled;
    }

    public T nextTask() {
        if (!enabled) {
            return null;
        }

        long cursor = telemetryPubSeq.next();
        if (cursor < 0) {
            return null;
        }

        return telemetryQueue.get(cursor);
    }

    public void store() {
        telemetryPubSeq.done(telemetryPubSeq.current());
    }

    public interface TelemetryType<T extends AbstractTelemetryTask> {
        String getCreateSql();

        String getTableName();

        ObjectFactory<T> getTaskFactory();

        //todo: we could tailor the config for each telemetry type
        //we could set different queue sizes or disable telemetry per type, for example
        default TelemetryConfiguration getTelemetryConfiguration(@NotNull CairoConfiguration configuration) {
            return configuration.getTelemetryConfiguration();
        }

        default void logStatus(TableWriter writer, short systemStatus, long micros) {
        }
    }

    public interface TelemetryTypeBuilder<T extends AbstractTelemetryTask> {
        TelemetryType<T> build(CairoConfiguration configuration);
    }
}
