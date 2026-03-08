/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.tasks.AbstractTelemetryTask;

import java.io.Closeable;

public class TelemetryJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(TelemetryJob.class);

    private final ObjList<Telemetry<? extends AbstractTelemetryTask>> telemetries;
    private final TelemetryConfigLogger telemetryConfigLogger;

    public TelemetryJob(CairoEngine engine) throws SqlException {
        try {
            // owned by the engine, should be closed by the engine
            telemetries = engine.getTelemetries();

            // owned by the job, should be closed by the job
            telemetryConfigLogger = new TelemetryConfigLogger(engine);

            try (final SqlCompiler compiler = engine.getSqlCompiler()) {
                final SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1) {
                    @Override
                    public boolean shouldLogSql() {
                        return false;
                    }
                };
                sqlExecutionContext.with(
                        engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null
                );

                for (int i = 0, n = telemetries.size(); i < n; i++) {
                    telemetries.getQuick(i).init(engine, compiler, sqlExecutionContext);
                }
                telemetryConfigLogger.init(engine, compiler, sqlExecutionContext);
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        for (int i = 0, n = telemetries.size(); i < n; i++) {
            telemetries.getQuick(i).clear();
        }
        Misc.free(telemetryConfigLogger);
    }

    @Override
    public boolean runSerially() {
        for (int i = 0, n = telemetries.size(); i < n; i++) {
            try {
                telemetries.getQuick(i).consumeAll();
            } catch (Throwable th) {
                LOG.error().$("failed to process ").$(telemetries.getQuick(i).getName()).$(" event").$(th).$();
            }
        }
        return false;
    }
}
