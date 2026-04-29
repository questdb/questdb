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
                QwpTableBuffer buf = sender.getTableBuffer("test_date");
                QwpTableBuffer.ColumnBuffer dateCol = buf.getOrCreateColumn("event_date", TYPE_DATE, false);

                // Row 1: 2024-01-01 00:00:00 UTC (epoch millis)
                dateCol.addLong(1_704_067_200_000L);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);

                // Row 2: 2024-06-15 12:30:00 UTC (epoch millis)
                dateCol.addLong(1_718_454_600_000L);
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);

                // Row 3: 1970-01-01 00:00:00 UTC (epoch zero)
                dateCol.addLong(0L);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql("SELECT count() FROM test_date", "count\n3\n");
            assertSql(
                    "SELECT event_date FROM test_date ORDER BY ts",
                    "event_date\n2024-01-01T00:00:00.000Z\n2024-06-15T12:30:00.000Z\n1970-01-01T00:00:00.000Z\n"
            );
        });
    }

    @Test
    public void testNullLong256() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                // Use fast-path API to send null LONG256 via null bitmap
                QwpTableBuffer buf = sender.getTableBuffer("test_null_long256");
                QwpTableBuffer.ColumnBuffer col = buf.getOrCreateColumn("value", TYPE_LONG256, true);

                // Row 1: non-null value
                col.addLong256(1L, 2L, 3L, 4L);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);
                // Row 2: null
                col.addNull();
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);
                // Row 3: non-null value
                col.addLong256(5L, 6L, 7L, 8L);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT count() FROM test_null_long256 WHERE value IS NULL",
                    "count\n1\n"
            );
            assertSql(
                    "SELECT count() FROM test_null_long256 WHERE value IS NOT NULL",
                    "count\n2\n"
            );
        });
    }

    @Test
    public void testNullTimestamp() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                // Use fast-path API to send null timestamp via null bitmap
                QwpTableBuffer buf = sender.getTableBuffer("test_null_ts");
                QwpTableBuffer.ColumnBuffer tsCol = buf.getOrCreateColumn("event_time", TYPE_TIMESTAMP, true);

                // Row 1: non-null timestamp
                tsCol.addLong(1_609_459_200_000_000L);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);
                // Row 2: null timestamp
                tsCol.addNull();
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);
                // Row 3: non-null timestamp
                tsCol.addLong(1_609_459_200_000_001L);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT count() FROM test_null_ts WHERE event_time IS NULL",
                    "count\n1\n"
            );
            assertSql(
                    "SELECT count() FROM test_null_ts WHERE event_time IS NOT NULL",
                    "count\n2\n"
            );
        });
    }

    @Test
    public void testNullUuid() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = connectWs(port)) {
                // Use fast-path API to send null UUID via null bitmap
                QwpTableBuffer buf = sender.getTableBuffer("test_null_uuid");
                QwpTableBuffer.ColumnBuffer col = buf.getOrCreateColumn("id", TYPE_UUID, true);

                // Row 1: non-null UUID
                col.addUuid(0x0123456789ABCDEFL, 0xFEDCBA9876543210L);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);
                // Row 2: null UUID
                col.addNull();
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);
                // Row 3: non-null UUID
                col.addUuid(0xAAAABBBBCCCCDDDDL, 0x1111222233334444L);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT count() FROM test_null_uuid WHERE id IS NULL",
                    "count\n1\n"
            );
            assertSql(
                    "SELECT count() FROM test_null_uuid WHERE id IS NOT NULL",
                    "count\n2\n"
            );
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
                QwpTableBuffer buf = sender.getTableBuffer("omit_date");
                QwpTableBuffer.ColumnBuffer col = buf.getOrCreateColumn("col", TYPE_DATE, true);

                // 2024-01-01T00:00:00Z in millis
                col.addLong(1_704_067_200_000L);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);

                col.addNull();
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);

                // 2023-06-15T00:00:00Z in millis
                col.addLong(1_686_787_200_000L);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);

                col.addNull();
                sender.at(1_000_000_000_003L, ChronoUnit.MICROS);

                // 2025-12-31T00:00:00Z in millis
                col.addLong(1_767_139_200_000L);
                sender.at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT count() FROM omit_date WHERE col IS NULL",
                    "count\n2\n"
            );
            assertSql(
                    "SELECT count() FROM omit_date WHERE col IS NOT NULL",
                    "count\n3\n"
            );
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
                QwpTableBuffer buf = sender.getTableBuffer("omit_geohash");
                QwpTableBuffer.ColumnBuffer col = buf.getOrCreateColumn("col", TYPE_GEOHASH, true);

                col.addGeoHash(0b10110L, 5);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);

                col.addNull();
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);

                col.addGeoHash(0b11111L, 5);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);

                col.addNull();
                sender.at(1_000_000_000_003L, ChronoUnit.MICROS);

                col.addGeoHash(0b01010L, 5);
                sender.at(1_000_000_000_004L, ChronoUnit.MICROS);
            }

            drainWalQueue();
            assertSql(
                    "SELECT count() FROM omit_geohash WHERE col IS NULL",
                    "count\n2\n"
            );
            assertSql(
                    "SELECT count() FROM omit_geohash WHERE col IS NOT NULL",
                    "count\n3\n"
            );
        });
    }
}
