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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ObjectFactory;
import org.jetbrains.annotations.NotNull;

public class TelemetryWalTask implements AbstractTelemetryTask {
    public static final String TABLE_NAME = "telemetry_wal";
    public static final Telemetry.TelemetryTypeBuilder<TelemetryWalTask> WAL_TELEMETRY = configuration -> {
        String tableName = configuration.getSystemTableNamePrefix() + TABLE_NAME;
        return new Telemetry.TelemetryType<TelemetryWalTask>() {
            @Override
            public SqlCompiler.QueryBuilder getCreateSql(SqlCompiler.QueryBuilder builder) {
                return builder.$("CREATE TABLE IF NOT EXISTS \"")
                        .$(tableName)
                        .$("\" (" +
                                "created timestamp, " +
                                "event short, " +
                                "tableId int, " +
                                "walId int, " +
                                "seqTxn long, " +
                                "rowCount long," +
                                "physicalRowCount long," +
                                "latency float" +
                                ") timestamp(created) partition by MONTH BYPASS WAL"
                        );
            }

            @Override
            public String getTableName() {
                return tableName;
            }

            @Override
            public ObjectFactory<TelemetryWalTask> getTaskFactory() {
                return TelemetryWalTask::new;
            }
        };
    };
    private static final Log LOG = LogFactory.getLog(TelemetryWalTask.class);
    private short event;
    private float latency; // millis
    private long physicalRowCount;
    private long rowCount;
    private long seqTxn;
    private int tableId;
    private int walId;

    private TelemetryWalTask() {
    }

    public static void store(@NotNull Telemetry<TelemetryWalTask> telemetry, short event, int tableId, int walId, long seqTxn, long rowCount, long physicalRowCount, long latencyUs) {
        final TelemetryWalTask task = telemetry.nextTask();
        if (task != null) {
            task.event = event;
            task.tableId = tableId;
            task.walId = walId;
            task.seqTxn = seqTxn;
            task.rowCount = rowCount;
            task.physicalRowCount = physicalRowCount;
            task.latency = latencyUs / 1000.0f; // millis
            telemetry.store();
        }
    }

    @Override
    public void writeTo(TableWriter writer, long timestamp) {
        try {
            final TableWriter.Row row = writer.newRow(timestamp);
            row.putShort(1, event);
            row.putInt(2, tableId);
            row.putInt(3, walId);
            row.putLong(4, seqTxn);
            row.putLong(5, rowCount);
            row.putLong(6, physicalRowCount);
            row.putFloat(7, latency);
            row.append();
        } catch (CairoException e) {
            LOG.error().$("Could not insert a new ").$(TABLE_NAME).$(" row [errno=").$(e.getErrno())
                    .$(", error=").$(e.getFlyweightMessage())
                    .$(']').$();
        }
    }
}
