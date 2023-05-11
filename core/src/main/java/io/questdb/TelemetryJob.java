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

import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.TelemetryWalTask;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class TelemetryJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(TelemetryJob.class);
    private final Telemetry<TelemetryTask> telemetry;
    private final Telemetry<TelemetryWalTask> telemetryWal;
    private final TelemetryConfigLogger telemetryConfigLogger;

    public TelemetryJob(CairoEngine engine) throws SqlException {
        this(engine, null);
    }

    public TelemetryJob(CairoEngine engine, @Nullable FunctionFactoryCache functionFactoryCache) throws SqlException {
        telemetry = engine.getTelemetry();
        telemetryWal = engine.getTelemetryWal();
        telemetryConfigLogger = new TelemetryConfigLogger(engine);

        try (final SqlCompiler compiler = new SqlCompiler(engine, functionFactoryCache, null)) {
            final SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
            sqlExecutionContext.with(
                    engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                    null,
                    null
            );

            telemetry.init(engine, compiler, sqlExecutionContext);
            telemetryWal.init(engine, compiler, sqlExecutionContext);
            telemetryConfigLogger.init(compiler, sqlExecutionContext);
        }
    }

    @Override
    public void close() {
        telemetry.close();
        telemetryWal.close();
        telemetryConfigLogger.close();
    }

    @Override
    public boolean runSerially() {
        try {
            telemetry.consumeAll();
        } catch (Throwable th) {
            LOG.error().$("failed to process telemetry event").$(th).$();
        }
        try {
            telemetryWal.consumeAll();
        } catch (Throwable th) {
            LOG.error().$("failed to process wal telemetry event").$(th).$();
        }
        return false;
    }
}
