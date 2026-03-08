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

package io.questdb.tasks;

import io.questdb.Telemetry;
import io.questdb.TelemetryConfiguration;
import io.questdb.TelemetryConfigurationWrapper;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.QueryBuilder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ObjectFactory;
import org.jetbrains.annotations.NotNull;

public class TelemetryWalTask implements AbstractTelemetryTask {
    public static final String NAME = "WAL TELEMETRY";
    public static final String TABLE_NAME = "telemetry_wal";
    public static final Telemetry.TelemetryTypeBuilder<TelemetryWalTask> WAL_TELEMETRY = configuration -> {
        final String tableName = configuration.getSystemTableNamePrefix() + TABLE_NAME;
        return new Telemetry.TelemetryType<>() {
            @Override
            public QueryBuilder getCreateSql(QueryBuilder builder, int ttlWeeks) {
                return builder.$("CREATE TABLE IF NOT EXISTS '")
                        .$(tableName)
                        .$("' (" +
                                "created TIMESTAMP, " +
                                "event SHORT, " +
                                "tableId INT, " +
                                "walId INT, " +
                                "seqTxn LONG, " +
                                "rowCount LONG, " +
                                "physicalRowCount LONG, " +
                                "latency FLOAT, " +
                                "minTimestamp TIMESTAMP, " +
                                "maxTimestamp TIMESTAMP" +
                                ") TIMESTAMP(created) PARTITION BY DAY TTL ").$(ttlWeeks > 0 ? ttlWeeks : 1).$(" WEEKS BYPASS WAL"
                        );
            }

            @Override
            public int getExpectedColumnCount() {
                return 10;
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
            public ObjectFactory<TelemetryWalTask> getTaskFactory() {
                return TelemetryWalTask::new;
            }

            // Hardcoded configuration for telemetry_wal table:
            // - Always enabled regardless of the main telemetry setting
            // - Throttling disabled (0L) to record every WAL event without rate limiting
            // - TTL fixed at 1 week
            @Override
            public TelemetryConfiguration getTelemetryConfiguration(@NotNull CairoConfiguration configuration) {
                return new TelemetryConfigurationWrapper(configuration.getTelemetryConfiguration()) {
                    @Override
                    public boolean getEnabled() {
                        return true;
                    }

                    @Override
                    public long getThrottleIntervalMicros() {
                        return 0L;
                    }

                    @Override
                    public int getTtlWeeks() {
                        return 1;
                    }
                };
            }
        };
    };
    private static final Log LOG = LogFactory.getLog(TelemetryWalTask.class);
    private short event;
    private float latency; // millis
    private long maxTimestamp;
    private long minTimestamp;
    private long physicalRowCount;
    private long queueCursor;
    private long rowCount;
    private long seqTxn;
    private int tableId;
    private int walId;

    private TelemetryWalTask() {
    }

    public static void store(
            @NotNull Telemetry<TelemetryWalTask> telemetry,
            short event,
            int tableId,
            int walId,
            long seqTxn,
            long rowCount,
            long physicalRowCount,
            long latencyUs,
            long minTimestamp,
            long maxTimestamp
    ) {
        final TelemetryWalTask task = telemetry.nextTask();
        if (task != null) {
            task.event = event;
            task.tableId = tableId;
            task.walId = walId;
            task.seqTxn = seqTxn;
            task.rowCount = rowCount;
            task.physicalRowCount = physicalRowCount;
            task.latency = latencyUs / 1000.0f; // millis
            task.minTimestamp = minTimestamp;
            task.maxTimestamp = maxTimestamp;
            telemetry.store(task);
        }
    }

    @Override
    public int getEventKey() {
        return ((event & 0xFFFF) << 20) | (tableId & 0xFFFFF);
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
            row.putInt(2, tableId);
            row.putInt(3, walId);
            row.putLong(4, seqTxn);
            row.putLong(5, rowCount);
            row.putLong(6, physicalRowCount);
            row.putFloat(7, latency);
            row.putTimestamp(8, minTimestamp);
            row.putTimestamp(9, maxTimestamp);
            row.append();
        } catch (CairoException e) {
            LOG.error().$("Could not insert a new ").$(TABLE_NAME).$(" row [errno=").$(e.getErrno())
                    .$(", error=").$safe(e.getFlyweightMessage())
                    .$(']').$();
        }
    }
}
