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

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.Sender;
import io.questdb.client.SenderConnectionEvent;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Drives the real client against a real QWP ingress and pins when a sender
 * looks a cached table schema up again after
 * {@code ALTER TABLE ... ALTER COLUMN ... TYPE} widens a column. The tests move
 * the sender's clock by hand, so none depends on how long the ALTER takes.
 * <p>
 * The expected values come from Java arithmetic: a FLOAT column holds
 * {@code (float) 0.1}, which reads back as {@code (double) 0.1f =
 * 0.10000000149011612} once the column is DOUBLE; a microsecond column holds
 * {@code 1_000_000_123} nanoseconds as {@code 1_000_000} microseconds.
 */
public class QwpSchemaMaxAgeE2ETest extends AbstractQwpWebSocketTest {
    private static final long MAX_AGE_NANOS = TimeUnit.MILLISECONDS.toNanos(QwpWebSocketSender.SCHEMA_MAX_AGE_MILLIS);
    private static final String NARROWED_POINT_ONE = Double.toString((double) 0.1f);

    @Test
    public void testAcknowledgedBatchExtendsSchemaLifetime() throws Exception {
        execute("CREATE TABLE extended (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        runInContext(port -> {
            AtomicLong clock = new AtomicLong();
            try (Sender sender = sender(port, "auto", clock)) {
                sender.table("extended").longColumn("v", 1).at(1, ChronoUnit.MICROS);
                Assert.assertTrue(sender.drain(10_000));
                clock.set(MAX_AGE_NANOS * 3 / 5);
                sender.table("extended").longColumn("v", 2).at(2, ChronoUnit.MICROS);
                Assert.assertTrue(sender.drain(10_000));

                execute("ALTER TABLE extended ALTER COLUMN v TYPE LONG");
                drainWalQueue();

                // The reply to the second batch confirmed the INT schema less than the
                // age limit ago, so the sender still converts against it.
                clock.set(MAX_AGE_NANOS * 3 / 2);
                LineSenderSchemaException e = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.table("extended").longColumn("v", 3_000_000_000L)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                // The rejection evicted the snapshot, so the retry looks the table up.
                sender.table("extended").longColumn("v", 3_000_000_000L).at(3, ChronoUnit.MICROS);
                Assert.assertTrue(sender.drain(10_000));
            }
            drainWalQueue();
            assertQuery("SELECT v FROM extended")
                    .noLeakCheck()
                    .expectSize()
                    .returns("v\n1\n2\n3000000000\n");
        });
    }

    @Test
    public void testBatchOfAnotherTableDoesNotExtendSchemaLifetime() throws Exception {
        execute("CREATE TABLE altered (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE busy (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        runInContext(port -> {
            AtomicLong clock = new AtomicLong();
            try (Sender sender = sender(port, "auto", clock)) {
                sender.table("altered").longColumn("v", 1).at(1, ChronoUnit.MICROS);
                sender.table("busy").longColumn("v", 1).at(1, ChronoUnit.MICROS);
                Assert.assertTrue(sender.drain(10_000));
                clock.set(MAX_AGE_NANOS * 3 / 5);
                sender.table("busy").longColumn("v", 2).at(2, ChronoUnit.MICROS);
                Assert.assertTrue(sender.drain(10_000));

                execute("ALTER TABLE altered ALTER COLUMN v TYPE LONG");
                drainWalQueue();

                // Only the first batch wrote the altered table, so its schema is older
                // than the age limit and the sender looks it up again.
                clock.set(MAX_AGE_NANOS * 3 / 2);
                sender.table("altered").longColumn("v", 3_000_000_000L).at(3, ChronoUnit.MICROS);
                Assert.assertTrue(sender.drain(10_000));
            }
            drainWalQueue();
            assertQuery("SELECT v FROM altered")
                    .noLeakCheck()
                    .expectSize()
                    .returns("v\n1\n3000000000\n");
            assertQuery("SELECT v FROM busy")
                    .noLeakCheck()
                    .expectSize()
                    .returns("v\n1\n2\n");
        });
    }

    @Test
    public void testIdleSenderRechecksFloatWidenedToDouble() throws Exception {
        for (String mode : new String[]{"auto", "strict"}) {
            String table = "widened_float_" + mode;
            execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            runInContext(port -> {
                AtomicLong clock = new AtomicLong();
                try (Sender sender = sender(port, mode, clock)) {
                    sender.table(table).doubleColumn("v", 0.1).at(1, ChronoUnit.MICROS);
                    Assert.assertTrue(sender.drain(10_000));

                    execute("ALTER TABLE " + table + " ALTER COLUMN v TYPE DOUBLE");
                    drainWalQueue();

                    clock.set(MAX_AGE_NANOS + 1);
                    sender.table(table).doubleColumn("v", 0.1).at(2, ChronoUnit.MICROS);
                    sender.table(table).doubleColumn("v", 0.1).at(3, ChronoUnit.MICROS);
                    Assert.assertTrue(sender.drain(10_000));
                }
                drainWalQueue();
                assertQuery("SELECT v FROM " + table)
                        .noLeakCheck()
                        .expectSize()
                        .returns("v\n" + NARROWED_POINT_ONE + "\n0.1\n0.1\n");
            });
        }
    }

    @Test
    public void testIdleSenderRechecksIntWidenedToLong() throws Exception {
        execute("CREATE TABLE widened_int (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        runInContext(port -> {
            AtomicLong clock = new AtomicLong();
            try (Sender sender = sender(port, "auto", clock)) {
                sender.table("widened_int").longColumn("v", 1).at(1, ChronoUnit.MICROS);
                Assert.assertTrue(sender.drain(10_000));

                execute("ALTER TABLE widened_int ALTER COLUMN v TYPE LONG");
                drainWalQueue();

                // The INT snapshot would reject this value; the fresh LONG schema takes it.
                clock.set(MAX_AGE_NANOS + 1);
                sender.table("widened_int").longColumn("v", 3_000_000_000L).at(2, ChronoUnit.MICROS);
                Assert.assertTrue(sender.drain(10_000));
            }
            drainWalQueue();
            assertQuery("SELECT v FROM widened_int")
                    .noLeakCheck()
                    .expectSize()
                    .returns("v\n1\n3000000000\n");
        });
    }

    @Test
    public void testIdleSenderRechecksTimestampWidenedToNanos() throws Exception {
        execute("CREATE TABLE widened_ts (c TIMESTAMP, i TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        runInContext(port -> {
            AtomicLong clock = new AtomicLong();
            Instant instant = Instant.ofEpochSecond(1, 123);
            try (Sender sender = sender(port, "auto", clock)) {
                sender.table("widened_ts")
                        .timestampColumn("c", 1_000_000_123L, ChronoUnit.NANOS)
                        .timestampColumn("i", instant)
                        .at(1, ChronoUnit.MICROS);
                Assert.assertTrue(sender.drain(10_000));

                execute("ALTER TABLE widened_ts ALTER COLUMN c TYPE TIMESTAMP_NS");
                execute("ALTER TABLE widened_ts ALTER COLUMN i TYPE TIMESTAMP_NS");
                drainWalQueue();

                clock.set(MAX_AGE_NANOS + 1);
                sender.table("widened_ts")
                        .timestampColumn("c", 1_000_000_123L, ChronoUnit.NANOS)
                        .timestampColumn("i", instant)
                        .at(2, ChronoUnit.MICROS);
                Assert.assertTrue(sender.drain(10_000));
            }
            drainWalQueue();
            assertQuery("SELECT c, i FROM widened_ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            c\ti
                            1970-01-01T00:00:01.000000000Z\t1970-01-01T00:00:01.000000000Z
                            1970-01-01T00:00:01.000000123Z\t1970-01-01T00:00:01.000000123Z
                            """);
        });
    }

    @Test
    public void testReconnectedSenderRechecksSchemaAlteredDuringOutage() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE outage_float (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE outage_marker (id LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            int port = RestartableQwpServer.pickFreePort();
            String sfDir = temp.newFolder("qwp-schema-max-age-reconnect").getAbsolutePath();
            CountDownLatch disconnected = new CountDownLatch(1);
            AtomicLong clock = new AtomicLong();
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();
                try (Sender sender = outageSender(port, sfDir, "schema_mode=auto;", disconnected, clock)) {
                    sender.table("outage_float").doubleColumn("v", 0.1).at(1, ChronoUnit.MICROS);
                    Assert.assertTrue(sender.drain(30_000));

                    server.stop();
                    Assert.assertTrue("sender did not observe the outage", disconnected.await(10, TimeUnit.SECONDS));
                    execute("ALTER TABLE outage_float ALTER COLUMN v TYPE DOUBLE");
                    drainWalQueue();
                    server.start();
                    // Reconnects through another table, so no reply reports the change.
                    sender.table("outage_marker").longColumn("id", 1).at(1, ChronoUnit.MICROS);
                    Assert.assertTrue(sender.drain(30_000));

                    clock.set(MAX_AGE_NANOS + 1);
                    sender.table("outage_float").doubleColumn("v", 0.1).at(2, ChronoUnit.MICROS);
                    Assert.assertTrue(sender.drain(30_000));
                }
                drainWalQueue();
                assertQuery("SELECT v FROM outage_float")
                        .noLeakCheck()
                        .expectSize()
                        .returns("v\n" + NARROWED_POINT_ONE + "\n0.1\n");
            }
        });
    }

    @Test
    public void testSchemaAtAgeLimitIsNotRechecked() throws Exception {
        execute("CREATE TABLE age_limit (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        runInContext(port -> {
            AtomicLong clock = new AtomicLong();
            try (Sender sender = sender(port, "auto", clock)) {
                sender.table("age_limit").longColumn("v", 1).at(1, ChronoUnit.MICROS);
                Assert.assertTrue(sender.drain(10_000));

                execute("ALTER TABLE age_limit ALTER COLUMN v TYPE LONG");
                drainWalQueue();

                // Confirmed exactly the age limit ago: the sender keeps the INT snapshot.
                clock.set(MAX_AGE_NANOS);
                LineSenderSchemaException e = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.table("age_limit").longColumn("v", 3_000_000_000L)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
            }
            drainWalQueue();
            assertQuery("SELECT v FROM age_limit")
                    .noLeakCheck()
                    .expectSize()
                    .returns("v\n1\n");
        });
    }

    @Test
    public void testStrictWritesAgedSchemaWhileServerIsDown() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE strict_down (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            int port = RestartableQwpServer.pickFreePort();
            String sfDir = temp.newFolder("qwp-schema-max-age-down").getAbsolutePath();
            CountDownLatch disconnected = new CountDownLatch(1);
            AtomicLong clock = new AtomicLong();
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();
                // A schema wait far longer than the assertion below: a re-check that
                // waited for the wire would hold the row until the server returns.
                try (Sender sender = outageSender(port, sfDir, "schema_mode=strict;schema_wait_millis=600000;", disconnected, clock)) {
                    sender.table("strict_down").doubleColumn("v", 1.5).at(1, ChronoUnit.MICROS);
                    Assert.assertTrue(sender.drain(30_000));

                    server.stop();
                    Assert.assertTrue("sender did not observe the outage", disconnected.await(10, TimeUnit.SECONDS));
                    clock.set(MAX_AGE_NANOS + 1);
                    Future<?> write = executor.submit(() -> {
                        sender.table("strict_down").doubleColumn("v", 2.5).at(2, ChronoUnit.MICROS);
                        sender.flush();
                    });
                    write.get(30, TimeUnit.SECONDS);

                    server.start();
                    Assert.assertTrue(sender.drain(30_000));
                }
                drainWalQueue();
                assertQuery("SELECT v FROM strict_down")
                        .noLeakCheck()
                        .expectSize()
                        .returns("v\n1.5\n2.5\n");
            } finally {
                executor.shutdownNow();
            }
        });
    }

    private static Sender outageSender(
            int port,
            String sfDir,
            String extraConfig,
            CountDownLatch disconnected,
            AtomicLong clock
    ) {
        Sender sender = Sender.builder("ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=100;"
                        + "auto_flush_rows=2147483647;auto_flush_bytes=0;auto_flush_interval=2147483646;"
                        + extraConfig)
                .connectionListener(event -> {
                    if (event.getKind() == SenderConnectionEvent.Kind.DISCONNECTED) {
                        disconnected.countDown();
                    }
                })
                .build();
        ((QwpWebSocketSender) sender).setNanoClockForTesting(clock::get);
        return sender;
    }

    private static Sender sender(int port, String mode, AtomicLong clock) {
        Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";schema_mode=" + mode
                + ";auto_flush_rows=2147483647;auto_flush_bytes=0;auto_flush_interval=2147483646;"
                + "close_flush_timeout_millis=0;");
        ((QwpWebSocketSender) sender).setNanoClockForTesting(clock::get);
        return sender;
    }
}
