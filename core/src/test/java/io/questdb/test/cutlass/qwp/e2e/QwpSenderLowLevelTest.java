/*+*****************************************************************************
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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Low-level QWP tests that use {@link QwpTableBuffer} / {@link QwpTableBuffer.ColumnBuffer}
 * directly, bypassing the fluent sender API.
 */
public class QwpSenderLowLevelTest extends AbstractQwpWebSocketTest {

    @Test
    public void testDateColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE test_date (" +
                    "event_date DATE, " +
                    "ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = connectWs(port)) {
                // Row 1: 2024-01-01 00:00:00 UTC (epoch millis)
                column(sender, "test_date", "event_date", TYPE_DATE, false).addLong(1_704_067_200_000L);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);

                // Row 2: 2024-06-15 12:30:00 UTC (epoch millis)
                column(sender, "test_date", "event_date", TYPE_DATE, false).addLong(1_718_454_600_000L);
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);

                // Row 3: 1970-01-01 00:00:00 UTC (epoch zero)
                column(sender, "test_date", "event_date", TYPE_DATE, false).addLong(0L);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertQuery("SELECT count() FROM test_date")
                    .noLeakCheck()
                    .returnsOnce("count\n3\n");
            assertQuery("SELECT event_date FROM test_date ORDER BY ts")
                    .noLeakCheck()
                    .returnsOnce("event_date\n2024-01-01T00:00:00.000Z\n2024-06-15T12:30:00.000Z\n1970-01-01T00:00:00.000Z\n");
        });
    }

    @Test
    public void testNullLong256() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                // Row 1: non-null value
                column(sender, "test_null_long256", "value", TYPE_LONG256, true).addLong256(1L, 2L, 3L, 4L);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);
                // Row 2: null
                column(sender, "test_null_long256", "value", TYPE_LONG256, true).addNull();
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);
                // Row 3: non-null value
                column(sender, "test_null_long256", "value", TYPE_LONG256, true).addLong256(5L, 6L, 7L, 8L);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertQuery("SELECT count() FROM test_null_long256 WHERE value IS NULL")
                    .noLeakCheck()
                    .returnsOnce("count\n1\n");
            assertQuery("SELECT count() FROM test_null_long256 WHERE value IS NOT NULL")
                    .noLeakCheck()
                    .returnsOnce("count\n2\n");
        });
    }

    @Test
    public void testNullTimestamp() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                // Row 1: non-null timestamp
                column(sender, "test_null_ts", "event_time", TYPE_TIMESTAMP, true).addLong(1_609_459_200_000_000L);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);
                // Row 2: null timestamp
                column(sender, "test_null_ts", "event_time", TYPE_TIMESTAMP, true).addNull();
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);
                // Row 3: non-null timestamp
                column(sender, "test_null_ts", "event_time", TYPE_TIMESTAMP, true).addLong(1_609_459_200_000_001L);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertQuery("SELECT count() FROM test_null_ts WHERE event_time IS NULL")
                    .noLeakCheck()
                    .returnsOnce("count\n1\n");
            assertQuery("SELECT count() FROM test_null_ts WHERE event_time IS NOT NULL")
                    .noLeakCheck()
                    .returnsOnce("count\n2\n");
        });
    }

    @Test
    public void testNullUuid() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                // Row 1: non-null UUID
                column(sender, "test_null_uuid", "id", TYPE_UUID, true)
                        .addUuid(0x0123456789ABCDEFL, 0xFEDCBA9876543210L);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);
                // Row 2: null UUID
                column(sender, "test_null_uuid", "id", TYPE_UUID, true).addNull();
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);
                // Row 3: non-null UUID
                column(sender, "test_null_uuid", "id", TYPE_UUID, true)
                        .addUuid(0xAAAABBBBCCCCDDDDL, 0x1111222233334444L);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertQuery("SELECT count() FROM test_null_uuid WHERE id IS NULL")
                    .noLeakCheck()
                    .returnsOnce("count\n1\n");
            assertQuery("SELECT count() FROM test_null_uuid WHERE id IS NOT NULL")
                    .noLeakCheck()
                    .returnsOnce("count\n2\n");
        });
    }

    @Test
    public void testOmittedDateColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE omit_date (" +
                    "col DATE, " +
                    "ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = connectWs(port)) {
                // 2024-01-01T00:00:00Z in millis
                column(sender, "omit_date", "col", TYPE_DATE, true).addLong(1_704_067_200_000L);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);

                column(sender, "omit_date", "col", TYPE_DATE, true).addNull();
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);

                // 2023-06-15T00:00:00Z in millis
                column(sender, "omit_date", "col", TYPE_DATE, true).addLong(1_686_787_200_000L);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);

                column(sender, "omit_date", "col", TYPE_DATE, true).addNull();
                sender.at(1_000_000_000_003L, ChronoUnit.MICROS);

                // 2025-12-31T00:00:00Z in millis
                column(sender, "omit_date", "col", TYPE_DATE, true).addLong(1_767_139_200_000L);
                sender.at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertQuery("SELECT count() FROM omit_date WHERE col IS NULL")
                    .noLeakCheck()
                    .returnsOnce("count\n2\n");
            assertQuery("SELECT count() FROM omit_date WHERE col IS NOT NULL")
                    .noLeakCheck()
                    .returnsOnce("count\n3\n");
        });
    }

    @Test
    public void testOmittedGeoHashColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE omit_geohash (" +
                    "col GEOHASH(5b), " +
                    "ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = connectWs(port)) {
                column(sender, "omit_geohash", "col", TYPE_GEOHASH, true).addGeoHash(0b10110L, 5);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);

                column(sender, "omit_geohash", "col", TYPE_GEOHASH, true).addNull();
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);

                column(sender, "omit_geohash", "col", TYPE_GEOHASH, true).addGeoHash(0b11111L, 5);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);

                column(sender, "omit_geohash", "col", TYPE_GEOHASH, true).addNull();
                sender.at(1_000_000_000_003L, ChronoUnit.MICROS);

                column(sender, "omit_geohash", "col", TYPE_GEOHASH, true).addGeoHash(0b01010L, 5);
                sender.at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertQuery("SELECT count() FROM omit_geohash WHERE col IS NULL")
                    .noLeakCheck()
                    .returnsOnce("count\n2\n");
            assertQuery("SELECT count() FROM omit_geohash WHERE col IS NOT NULL")
                    .noLeakCheck()
                    .returnsOnce("count\n3\n");
        });
    }

    private static QwpTableBuffer.ColumnBuffer column(
            QwpWebSocketSender sender,
            String table,
            String name,
            byte type,
            boolean useNullBitmap
    ) {
        // Prepare each row before injecting raw values into its buffer.
        sender.table(table);
        return sender.getTableBuffer(table).getOrCreateColumn(name, type, useNullBitmap);
    }
}
