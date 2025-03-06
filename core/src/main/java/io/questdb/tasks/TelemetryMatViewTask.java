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

package io.questdb.tasks;

import io.questdb.Telemetry;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.QueryBuilder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ObjectFactory;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;

public class TelemetryMatViewTask implements AbstractTelemetryTask {
    public static final String NAME = "MAT VIEW TELEMETRY";
    public static final String TABLE_NAME = "telemetry_mat_view";
    public static final Telemetry.TelemetryTypeBuilder<TelemetryMatViewTask> MAT_VIEW_TELEMETRY = configuration -> {
        final String tableName = configuration.getSystemTableNamePrefix() + TABLE_NAME;
        return new Telemetry.TelemetryType<>() {
            @Override
            public QueryBuilder getCreateSql(QueryBuilder builder) {
                return builder.$("CREATE TABLE IF NOT EXISTS '")
                        .$(tableName)
                        .$("' (" +
                                "created TIMESTAMP, " +
                                "event SHORT, " +
                                "view_table_id INT, " +
                                "base_table_txn LONG, " +
                                "invalidation_reason VARCHAR, " +
                                "latency FLOAT " +
                                ") TIMESTAMP(created) PARTITION BY DAY TTL 1 WEEK BYPASS WAL"
                        );
            }

            @Override
            public String getName() {
                return NAME;
            }

            @Override
            public String getTableName() {
                return tableName;
            }

            @Override
            public ObjectFactory<TelemetryMatViewTask> getTaskFactory() {
                return TelemetryMatViewTask::new;
            }
        };
    };
    private static final Log LOG = LogFactory.getLog(TelemetryMatViewTask.class);
    private final Utf8StringSink invalidationReason = new Utf8StringSink();
    private long baseTableTxn;
    private short event;
    private float latency; // millis
    private long queueCursor;
    private int viewTableId;

    private TelemetryMatViewTask() {
    }

    public static void store(
            @NotNull Telemetry<TelemetryMatViewTask> telemetry,
            short event,
            int viewTableId,
            long baseTableTxn,
            CharSequence errorMessage,
            long latencyUs
    ) {
        final TelemetryMatViewTask task = telemetry.nextTask();
        if (task != null) {
            task.event = event;
            task.viewTableId = viewTableId;
            task.baseTableTxn = baseTableTxn;
            task.invalidationReason.clear();
            task.invalidationReason.put(errorMessage);
            task.latency = latencyUs / 1000.0f; // millis
            telemetry.store(task);
        }
    }

    public long getQueueCursor() {
        return queueCursor;
    }

    @Override
    public void setQueueCursor(long cursor) {
        this.queueCursor = cursor;
    }

    @Override
    public void writeTo(TableWriter writer, long timestamp) {
        try {
            final TableWriter.Row row = writer.newRow(timestamp);
            row.putShort(1, event);
            row.putInt(2, viewTableId);
            row.putLong(3, baseTableTxn);
            row.putVarchar(4, invalidationReason);
            row.putFloat(5, latency);
            row.append();
        } catch (CairoException e) {
            LOG.error().$("Could not insert a new ").$(TABLE_NAME).$(" row [errno=").$(e.getErrno())
                    .$(", error=").$(e.getFlyweightMessage())
                    .$(']').$();
        }
    }
}
