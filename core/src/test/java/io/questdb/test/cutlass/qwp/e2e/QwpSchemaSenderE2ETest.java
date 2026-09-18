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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.Sender;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class QwpSchemaSenderE2ETest extends AbstractQwpWebSocketTest {

    @Test
    public void testDeniedSchemaFailsLocallyAndNextAuthorizedTableRemainsUsable() throws Exception {
        execute("create table schema_sender_denied (id uuid, ts timestamp) timestamp(ts) partition by day wal");
        execute("create table schema_sender_allowed (id uuid, marker varchar, ts timestamp) "
                + "timestamp(ts) partition by day wal");
        DenyAllSecurityContext context = new DenyAllSecurityContext() {
            @Override
            public void authorizeHttp() {
            }

            @Override
            public void authorizeInsert(TableToken tableToken) {
                if (tableToken.getTableName().equals("schema_sender_denied")) {
                    throw CairoException.authorization().put("insert denied for test");
                }
            }
        };

        runInContext(port -> {
            try (Sender sender = connectWs(
                    port,
                    0,
                    0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
            )) {
                LineSenderSchemaException denied = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.table("schema_sender_denied")
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.ACCESS_DENIED, denied.getReason());

                sender.table("schema_sender_allowed")
                        .stringColumn("id", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
                        .stringColumn("marker", "allowed")
                        .atNow();
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            assertQuery("select count() from schema_sender_denied")
                    .noLeakCheck()
                    .returnsOnce("count\n0\n");
            assertQuery("select marker, id from schema_sender_allowed")
                    .noLeakCheck()
                    .returnsOnce("marker\tid\nallowed\tbbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\n");
        }, context);
    }

    @Test
    public void testAutoFlushCloseAndResetUseSchemaBoundRows() throws Exception {
        execute("create table schema_sender_lifecycle (id uuid, marker varchar, ts timestamp) "
                + "timestamp(ts) partition by day wal");

        runInContext(port -> {
            try (Sender sender = connectWs(
                    port,
                    1,
                    0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
            )) {
                sender.table("schema_sender_lifecycle")
                        .stringColumn("id", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                        .stringColumn("marker", "auto")
                        .atNow();
                // The first data frame is FSN 0. DESCRIBE consumes no FSN, and
                // no flush API is called here: reaching ACK 0 proves rows=1
                // triggered publication by itself.
                Assert.assertTrue("row-triggered auto-flush was not ACKed", sender.awaitAckedFsn(0, 10_000));
                drainWalQueue();
                assertQuery("select marker, id from schema_sender_lifecycle")
                        .noLeakCheck()
                        .returnsOnce("marker\tid\nauto\taaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\n");
            }

            try (Sender sender = connectWs(
                    port,
                    0,
                    0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
            )) {
                sender.table("schema_sender_lifecycle")
                        .stringColumn("id", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
                        .stringColumn("marker", "discarded")
                        .atNow();
                sender.reset();
                sender.table("schema_sender_lifecycle")
                        .stringColumn("id", "cccccccc-cccc-cccc-cccc-cccccccccccc")
                        .stringColumn("marker", "close")
                        .atNow();
                // close() owns the only flush of this sender. reset() must have
                // discarded the prior pending row from the current generation.
            }

            drainWalQueue();
            assertQuery("select marker, id from schema_sender_lifecycle order by marker")
                    .noLeakCheck()
                    .returnsOnce("marker\tid\n"
                            + "auto\taaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\n"
                            + "close\tcccccccc-cccc-cccc-cccc-cccccccccccc\n");
        });
    }

    @Test
    public void testMissingSchemaInfersTableAndNextKnownTableRemainsUsable() throws Exception {
        execute("create table schema_sender_known (id uuid, marker varchar, ts timestamp) "
                + "timestamp(ts) partition by day wal");

        runInContext(port -> {
            try (Sender sender = connectWs(
                    port,
                    0,
                    0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
            )) {
                sender.table("schema_sender_missing")
                        .uuidColumn("id", 0xaaaaaaaaaaaaaaaaL, 0xaaaaaaaaaaaaaaaaL)
                        .stringColumn("marker", "inferred-a")
                        .atNow();
                long inferredFsn = sender.flushAndGetSequence();
                Assert.assertTrue(inferredFsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(inferredFsn, 10_000));

                LineSenderSchemaException invalid = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.table("schema_sender_missing").stringColumn("marker", "inferred-b")
                                .stringColumn("failed_only", "must-be-rolled-back")
                                .stringColumn("id", "not-a-uuid")
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, invalid.getReason());

                // ACK feedback replaces the inferred binding with the resulting
                // server schema. The next table() prepares the row with that
                // snapshot and must convert the string into known UUID;
                // the failed B row and its new-only column stay rolled back.
                sender.table("schema_sender_missing").stringColumn("id", "cccccccc-cccc-cccc-cccc-cccccccccccc")
                        .stringColumn("marker", "inferred-c")
                        .atNow();

                sender.table("schema_sender_known")
                        .stringColumn("id", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
                        .stringColumn("marker", "known")
                        .atNow();
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            assertQuery("select marker, id from schema_sender_missing order by marker")
                    .noLeakCheck()
                    .returnsOnce("marker\tid\n"
                            + "inferred-a\taaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\n"
                            + "inferred-c\tcccccccc-cccc-cccc-cccc-cccccccccccc\n");
            assertQuery("select count() from table_columns('schema_sender_missing') "
                    + "where \"column\" = 'failed_only'")
                    .noLeakCheck()
                    .returnsOnce("count\n0\n");
            assertQuery("select \"column\", type from table_columns('schema_sender_missing') order by \"column\"")
                    .noLeakCheck()
                    .returnsOnce("column\ttype\n"
                            + "id\tUUID\n"
                            + "marker\tVARCHAR\n"
                            + "timestamp\tTIMESTAMP\n");
            assertQuery("select marker, id from schema_sender_known")
                    .noLeakCheck()
                    .returnsOnce("marker\tid\nknown\tbbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\n");
        });
    }

    @Test
    public void testStaleUuidFailureStaysPinnedUntilAckFeedback() throws Exception {
        execute("create table schema_sender_stale "
                + "(id uuid, marker varchar, failed_b varchar, ts timestamp) "
                + "timestamp(ts) partition by day wal");

        runInContext(port -> {
            try (Sender sender = connectWs(
                    port,
                    0,
                    0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
            )) {
                sender.table("schema_sender_stale")
                        .stringColumn("id", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                        .stringColumn("marker", "A")
                        .atNow();

                assertQuery("select count() from schema_sender_stale")
                        .noLeakCheck()
                        .returnsOnce("count\n0\n");
                execute("alter table schema_sender_stale drop column id");
                execute("alter table schema_sender_stale add column id varchar");

                // The pending batch keeps validating against the UUID snapshot it
                // pinned; the rejection is local and rolls the whole row back.
                LineSenderSchemaException invalid = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.table("schema_sender_stale").stringColumn("marker", "B")
                                .stringColumn("failed_b", "must-be-rolled-back")
                                .stringColumn("id", "not-a-uuid")
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, invalid.getReason());
                LineSenderSchemaException stillInvalid = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.table("schema_sender_stale").stringColumn("id", "not-a-uuid")
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, stillInvalid.getReason());

                // The frame ships with the stale identity, so its ACK piggybacks the
                // VARCHAR schema. The send loop applies that feedback before it advances
                // the ack watermark, so the next batch adopts it with no lookup of its own.
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
                sender.table("schema_sender_stale")
                        .stringColumn("id", "not-a-uuid")
                        .stringColumn("marker", "C")
                        .atNow();
                fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            assertQuery("select marker, id, failed_b from schema_sender_stale order by marker")
                    .noLeakCheck()
                    .returnsOnce("marker\tid\tfailed_b\n"
                            + "A\taaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\t\n"
                            + "C\tnot-a-uuid\t\n");
        });
    }

    @Test
    public void testUnrelatedSchemaChangeKeepsInvalidUuidReason() throws Exception {
        execute("create table schema_sender_unrelated "
                + "(id uuid, marker varchar, failed_b varchar, ts timestamp) "
                + "timestamp(ts) partition by day wal");

        runInContext(port -> {
            try (Sender sender = connectWs(
                    port,
                    0,
                    0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
            )) {
                sender.table("schema_sender_unrelated")
                        .stringColumn("id", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                        .stringColumn("marker", "A")
                        .atNow();
                execute("alter table schema_sender_unrelated add column unrelated long");

                LineSenderSchemaException invalid = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.table("schema_sender_unrelated").stringColumn("marker", "B")
                                .stringColumn("failed_b", "must-be-rolled-back")
                                .stringColumn("id", "not-a-uuid")
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, invalid.getReason());

                sender.table("schema_sender_unrelated").stringColumn("id", "cccccccc-cccc-cccc-cccc-cccccccccccc")
                        .stringColumn("marker", "C")
                        .atNow();
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            assertQuery("select marker, id, failed_b, unrelated from schema_sender_unrelated order by marker")
                    .noLeakCheck()
                    .returnsOnce("marker\tid\tfailed_b\tunrelated\n"
                            + "A\taaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\t\tnull\n"
                            + "C\tcccccccc-cccc-cccc-cccc-cccccccccccc\t\tnull\n");
        });
    }

    @Test
    public void testPublicSenderUsesMicroDesignatedTimestampAndRollsBackInvalidAt() throws Exception {
        assertDesignatedTimestampRows("schema_sender_ts_micro", "timestamp", 1_234_567L, 2_345_678L);
    }

    @Test
    public void testPublicSenderUsesNanoDesignatedTimestampAndRollsBackInvalidAt() throws Exception {
        assertDesignatedTimestampRows("schema_sender_ts_nano", "timestamp_ns", 1_234_567_890L, 2_345_678_901L);
    }

    @Test
    public void testPublicSenderLooksUpSchemaConvertsUuidAndRollsBackInvalidRow() throws Exception {
        execute("create table schema_sender_uuid "
                + "(id uuid, marker varchar, failed_b varchar, ts timestamp) "
                + "timestamp(ts) partition by day wal");

        runInContext(port -> {
            UUID c = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
            try (Sender sender = connectWs(
                    port,
                    0,
                    0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
            )) {
                sender.table("schema_sender_uuid")
                        .stringColumn("id", "11111111-1111-1111-1111-111111111111")
                        // First value wins: neither an invalid string nor another setter
                        // may re-parse or replace the accepted UUID value.
                        .stringColumn("id", "not-a-uuid")
                        .uuidColumn("id", c.getLeastSignificantBits(), c.getMostSignificantBits())
                        .stringColumn("marker", "A")
                        .atNow();

                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.table("schema_sender_uuid").stringColumn("marker", "B")
                                .stringColumn("failed_b", "must-be-rolled-back")
                                .stringColumn("id", "not-a-uuid")
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("UUID"));

                // The failed setter cancels B. A fresh table() opens the next row
                // against the batch's pinned binding.
                sender.table("schema_sender_uuid").uuidColumn("id", c.getLeastSignificantBits(), c.getMostSignificantBits())
                        .stringColumn("marker", "C")
                        .atNow();

                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue("published FSN", fsn >= 0);
                Assert.assertTrue("ACK did not reach published FSN", sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            assertQuery("select marker, id, failed_b from schema_sender_uuid order by marker")
                    .noLeakCheck()
                    .returnsOnce("marker\tid\tfailed_b\n"
                            + "A\t11111111-1111-1111-1111-111111111111\t\n"
                            + "C\t123e4567-e89b-12d3-a456-426614174000\t\n");
        });
    }

    private void assertDesignatedTimestampRows(
            String tableName,
            String timestampType,
            long primitiveValue,
            long instantValue
    ) throws Exception {
        execute("create table " + tableName + " (marker varchar, ts " + timestampType + ") "
                + "timestamp(ts) partition by day wal");

        runInContext(port -> {
            try (Sender sender = connectWs(
                    port,
                    0,
                    0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
            )) {
                sender.table(tableName)
                        .stringColumn("marker", "primitive")
                        .at(primitiveValue, timestampType.equals("timestamp_ns") ? ChronoUnit.NANOS : ChronoUnit.MICROS);

                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.table(tableName).stringColumn("marker", "failed")
                                .at(Long.MAX_VALUE, ChronoUnit.DAYS)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());

                Instant instant = timestampType.equals("timestamp_ns")
                        ? Instant.ofEpochSecond(2, 345_678_901)
                        : Instant.ofEpochSecond(2, 345_678_000);
                sender.table(tableName).stringColumn("marker", "instant").at(instant);

                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue("published FSN", fsn >= 0);
                Assert.assertTrue("ACK did not reach published FSN", sender.awaitAckedFsn(fsn, 10_000));
            }

            try (Sender cold = connectWs(
                    port,
                    0,
                    0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L)
            )) {
                // There is no warm cache or bound layout: table() prepares the
                // schema before atNow() completes the timestamp-only row.
                cold.table(tableName).atNow();
                long fsn = cold.flushAndGetSequence();
                Assert.assertTrue("cold atNow FSN", fsn >= 0);
                Assert.assertTrue(cold.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            assertQuery("select count() rows_count, count(marker) marker_count from " + tableName)
                    .noLeakCheck()
                    .returnsOnce("rows_count\tmarker_count\n3\t2\n");
            assertQuery("select marker, cast(ts as long) ts from " + tableName
                    + " where marker is not null order by ts")
                    .noLeakCheck()
                    .returnsOnce("marker\tts\n"
                            + "primitive\t" + primitiveValue + "\n"
                            + "instant\t" + instantValue + "\n");
            assertQuery("select count() from " + tableName
                    + " where marker is null and cast(ts as long) > " + instantValue)
                    .noLeakCheck()
                    .returnsOnce("count\n1\n");
        });
    }
}
