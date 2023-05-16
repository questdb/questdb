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

package io.questdb.tasks;

import io.questdb.Telemetry;
import io.questdb.TelemetryOrigin;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ObjectFactory;

public class TelemetryTask implements AbstractTelemetryTask {
    public static final String TABLE_NAME = "telemetry";

    private static final Log LOG = LogFactory.getLog(TelemetryTask.class);
    public static final Telemetry.TelemetryTypeBuilder<TelemetryTask> TELEMETRY = configuration -> new Telemetry.TelemetryType<TelemetryTask>() {
        private final TelemetryTask systemStatusTask = new TelemetryTask();

        @Override
        public SqlCompiler.QueryBuilder getCreateSql(SqlCompiler.QueryBuilder builder) {
            return builder
                    .$("CREATE TABLE IF NOT EXISTS \"")
                    .$(TABLE_NAME)
                    .$("\" (" +
                            "created timestamp, " +
                            "event short, " +
                            "origin short" +
                            ") timestamp(created)"
                    );
        }

        @Override
        public String getTableName() {
            return TABLE_NAME;
        }

        @Override
        public ObjectFactory<TelemetryTask> getTaskFactory() {
            return TelemetryTask::new;
        }

        @Override
        public void logStatus(TableWriter writer, short systemStatus, long micros) {
            systemStatusTask.origin = TelemetryOrigin.INTERNAL;
            systemStatusTask.event = systemStatus;
            systemStatusTask.writeTo(writer, micros);
            writer.commit();
        }

        @Override
        public boolean shouldLogClasses() {
            return true;
        }
    };
    private short event;
    private short origin;

    private TelemetryTask() {
    }

    public static void store(Telemetry<TelemetryTask> telemetry, short origin, short event) {
        final TelemetryTask task = telemetry.nextTask();
        if (task != null) {
            task.origin = origin;
            task.event = event;
            telemetry.store();
        }
    }

    @Override
    public void writeTo(TableWriter writer, long timestamp) {
        try {
            final TableWriter.Row row = writer.newRow(timestamp);
            row.putShort(1, event);
            row.putShort(2, origin);
            row.append();
        } catch (CairoException e) {
            LOG.error().$("Could not insert a new ").$(TABLE_NAME).$(" row [errno=").$(e.getErrno())
                    .$(", error=").$(e.getFlyweightMessage())
                    .$(']').$();
        }
    }
}
