/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.LineSenderServerException;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class QwpSchemaSenderInferenceE2ETest extends AbstractQwpWebSocketTest {

    @Test
    public void testAckAdoptsAutoCreatedColumnAndFailedRowDefinitionIsRolledBack() throws Exception {
        execute("create table schema_infer_column (marker varchar, ts timestamp) "
                + "timestamp(ts) partition by day wal");

        runInContext(port -> {
            try (Sender sender = connectWs(port, 0, 0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_infer_column")
                        .longColumn("added", 7)
                        .stringColumn("marker", "A")
                        .at(1_000_000, ChronoUnit.MICROS);
                long firstFsn = sender.flushAndGetSequence();
                Assert.assertTrue(firstFsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(firstFsn, 10_000));

                LineSenderSchemaException invalid = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.stringColumn("marker", "B")
                                .stringColumn("failed_only", "must-not-exist")
                                .stringColumn("added", "not-a-long")
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, invalid.getReason());

                // The ACK supplied the resulting server schema. Switching from
                // longColumn to stringColumn now converts against known LONG.
                sender.stringColumn("added", "8")
                        .stringColumn("marker", "C")
                        .at(2_000_000, ChronoUnit.MICROS);
                long secondFsn = sender.flushAndGetSequence();
                Assert.assertTrue(secondFsn > firstFsn);
                Assert.assertTrue(sender.awaitAckedFsn(secondFsn, 10_000));
            }

            drainWalQueue();
            assertQuery("select marker, added, cast(ts as long) ts from schema_infer_column order by marker")
                    .noLeakCheck()
                    .expectSize().returns("marker\tadded\tts\nA\t7\t1000000\nC\t8\t2000000\n");
            assertQuery("select count() from table_columns('schema_infer_column') "
                    + "where \"column\" = 'failed_only'")
                    .noLeakCheck()
                    .expectSize().noRandomAccess().returns("count\n0\n");
            assertQuery("select type from table_columns('schema_infer_column') where \"column\" = 'added'")
                    .noLeakCheck()
                    .noRandomAccess().returns("type\nLONG\n");
        });
    }

    @Test
    public void testMissingTimestampOnlyTablesInferDesignatedUnitsAndAtNow() throws Exception {
        runInContext(port -> {
            try (Sender sender = connectWs(port, 0, 0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_infer_at_now").atNow();
                sender.table("schema_infer_at_micro").at(1_234_567, ChronoUnit.MICROS);
                sender.table("schema_infer_at_nano").at(2_345_678_901L, ChronoUnit.NANOS);
                sender.table("schema_infer_at_instant")
                        .at(Instant.ofEpochSecond(3, 456_789_123));
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            assertQuery("select count() from schema_infer_at_now")
                    .noLeakCheck().expectSize().noRandomAccess().returns("count\n1\n");
            assertTimestampOnlyTable("schema_infer_at_now", "TIMESTAMP", null);
            assertTimestampOnlyTable("schema_infer_at_micro", "TIMESTAMP", 1_234_567L);
            assertTimestampOnlyTable("schema_infer_at_nano", "TIMESTAMP_NS", 2_345_678_901L);
            // Missing-table Instant inference intentionally preserves the public
            // Sender's microsecond normalization.
            assertTimestampOnlyTable("schema_infer_at_instant", "TIMESTAMP", 3_456_789L);
        });
    }

    @Test
    public void testMissingTableInferenceDoesNotBypassDisabledAutoCreate() throws Exception {
        runInContextNoAutoCreate(port -> {
            assertServerRejected(
                    port,
                    sender -> sender.table("schema_infer_disabled")
                            .longColumn("value", 42)
                            .atNow(),
                    SenderError.Category.INTERNAL_ERROR,
                    SenderError.Category.PROTOCOL_VIOLATION,
                    "failed to create table update details",
                    "schema_infer_disabled"
            );
            Assert.assertNull(engine.getTableTokenIfExists("schema_infer_disabled"));
        });
    }

    @Test
    public void testMissingColumnInferenceDoesNotBypassAddColumnAuthorization() throws Exception {
        execute("create table schema_infer_add_denied (marker varchar, ts timestamp) "
                + "timestamp(ts) partition by day wal");
        DenyAllSecurityContext context = new DenyAllSecurityContext() {
            @Override
            public void authorizeHttp() {
            }

            @Override
            public void authorizeInsert(TableToken tableToken) {
            }

            @Override
            public void authorizeAlterTableAddColumn(TableToken tableToken) {
                throw CairoException.authorization().put("add column denied for inference test");
            }
        };

        runInContext(port -> {
            assertServerRejected(
                    port,
                    sender -> sender.table("schema_infer_add_denied")
                            .stringColumn("marker", "rejected")
                            .longColumn("added", 42)
                            .atNow(),
                    SenderError.Category.SECURITY_ERROR,
                    SenderError.Category.SECURITY_ERROR,
                    "add column denied for inference test"
            );
            drainWalQueue();
            assertQuery("select count() from schema_infer_add_denied")
                    .noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
            assertQuery("select count() from table_columns('schema_infer_add_denied') "
                    + "where \"column\" = 'added'")
                    .noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
        }, context);
    }

    @Test
    public void testMissingTableInferenceDoesNotBypassCreateAuthorization() throws Exception {
        DenyAllSecurityContext context = new DenyAllSecurityContext() {
            @Override
            public void authorizeHttp() {
            }

            @Override
            public void authorizeTableCreate() {
                throw CairoException.authorization().put("create table denied for inference test");
            }
        };

        runInContext(port -> {
            assertServerRejected(
                    port,
                    sender -> sender.table("schema_infer_create_denied")
                            .longColumn("value", 42)
                            .atNow(),
                    SenderError.Category.SECURITY_ERROR,
                    SenderError.Category.SECURITY_ERROR,
                    "create table denied for inference test"
            );
            Assert.assertNull(engine.getTableTokenIfExists("schema_infer_create_denied"));
        }, context);
    }

    @Test
    public void testKnownTableWithoutDesignatedTimestampRejectsExplicitAtAndNextWalTableWorks() throws Exception {
        execute("create table schema_infer_no_designated (marker varchar)");
        execute("create table schema_infer_after_reject (marker varchar, ts timestamp) "
                + "timestamp(ts) partition by day wal");

        runInContext(port -> {
            try (Sender sender = connectWs(port, 0, 0,
                    TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                LineSenderSchemaException error = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> sender.table("schema_infer_no_designated")
                                .stringColumn("marker", "failed")
                                .at(1, ChronoUnit.MICROS)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, error.getReason());

                sender.table("schema_infer_after_reject")
                        .stringColumn("marker", "accepted")
                        .atNow();
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }

            drainWalQueue();
            assertQuery("select count() from schema_infer_no_designated")
                    .noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
            assertQuery("select marker from schema_infer_after_reject")
                    .noLeakCheck().expectSize().returns("marker\naccepted\n");
        });
    }

    private void assertTimestampOnlyTable(String table, String expectedType, Long expectedValue) throws Exception {
        assertQuery("select type from table_columns('" + table + "') where \"column\" = 'timestamp'")
                .noLeakCheck().noRandomAccess().returns("type\n" + expectedType + "\n");
        if (expectedValue != null) {
            assertQuery("select cast(timestamp as long) timestamp from " + table)
                    .noLeakCheck().expectSize().returns("timestamp\n" + expectedValue + "\n");
        }
    }

    private static void assertServerRejected(
            int port,
            Consumer<Sender> action,
            SenderError.Category expectedServerCategory,
            SenderError.Category expectedTerminalCategory,
            String... expectedMessageParts
    ) {
        CompletableFuture<SenderError> first = new CompletableFuture<>();
        CompletableFuture<SenderError> terminal = new CompletableFuture<>();
        SenderErrorHandler handler = error -> {
            first.complete(error);
            if (error.getAppliedPolicy() == SenderError.Policy.TERMINAL) {
                terminal.complete(error);
            }
        };
        QwpWebSocketSender sender = connectWs(port, handler, 1);
        SenderError.Category closeCategory = null;
        try {
            action.accept(sender);
            try {
                sender.flushAndGetSequence();
            } catch (LineSenderServerException ignored) {
                // The I/O thread may publish the terminal before this poll.
            }
            SenderError serverError;
            SenderError error;
            try {
                serverError = first.get(10, TimeUnit.SECONDS);
                error = terminal.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new AssertionError("server rejection did not become terminal", e);
            }
            Assert.assertSame(expectedServerCategory, serverError.getCategory());
            Assert.assertSame(expectedTerminalCategory, error.getCategory());
            String message = serverError.getServerMessage();
            for (String part : expectedMessageParts) {
                Assert.assertTrue("expected rejection containing '" + part + "' but got: " + message,
                        message != null && message.contains(part));
            }
            closeCategory = expectedTerminalCategory;
        } finally {
            assertRejectionTerminalOnClose(sender, terminal, closeCategory, expectedMessageParts);
        }
    }

}
