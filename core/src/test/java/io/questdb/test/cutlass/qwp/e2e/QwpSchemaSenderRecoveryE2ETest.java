/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class QwpSchemaSenderRecoveryE2ETest extends AbstractQwpWebSocketTest {
    private static final String TABLE = "schema_sender_recovery";

    @Test
    public void testPublicSenderRecoversSchemaFrameAfterProducerProcessCrash() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table " + TABLE
                    + " (id uuid, sym symbol, marker varchar, ts timestamp)"
                    + " timestamp(ts) partition by day wal");
            File sfRoot = temp.newFolder("qwp-schema-sender-recovery");

            runInContext(port -> {
                long fsn;
                byte[] publishedFrame;
                try (DataGate gate = new DataGate(port, true)) {
                    try (ChildProcess child = new ChildProcess(startProducer(gate.getPort(), sfRoot, TABLE, "single"))) {
                        String published = child.awaitLine("PUBLISHED ", 15_000);
                        fsn = Long.parseLong(published.substring("PUBLISHED ".length()));
                        Assert.assertTrue("child did not publish an SF frame", fsn >= 0);
                        Assert.assertTrue("gate did not capture the schema data frame",
                                gate.awaitData(15, TimeUnit.SECONDS));
                        publishedFrame = gate.getDataFrame();
                        Assert.assertTrue((publishedFrame[5] & QwpConstants.FLAG_SCHEMA) != 0);
                        assertQuery("select count() from " + TABLE)
                                .noLeakCheck()
                                .expectSize().noRandomAccess().returns("count\n0\n");

                        child.process.destroyForcibly();
                        Assert.assertTrue("producer JVM did not terminate",
                                child.process.waitFor(15, TimeUnit.SECONDS));
                        Assert.assertNotEquals("producer must be killed, not exit cleanly", 0, child.process.exitValue());
                    }
                }

                try (DataGate recoveryGate = new DataGate(port, false);
                     Sender recovery = recoverySender(recoveryGate.getPort(), sfRoot)) {
                    Assert.assertTrue("recovered frame was not ACKed",
                            recovery.awaitAckedFsn(fsn, 30_000));
                    Assert.assertTrue("recovery gate did not capture a table-bearing frame",
                            recoveryGate.awaitData(15, TimeUnit.SECONDS));
                    Assert.assertArrayEquals("persisted QWP bytes changed across process restart",
                            publishedFrame, recoveryGate.getDataFrame());
                }

                drainWalQueue();
                assertQuery("select marker, sym, id from " + TABLE + " order by marker")
                        .noLeakCheck()
                        .expectSize().returns("marker\tsym\tid\n"
                                + "A\talpha\t123e4567-e89b-12d3-a456-426614174000\n"
                                + "C\tgamma\t223e4567-e89b-12d3-a456-426614174001\n");

                try (Sender reopened = recoverySender(port, sfRoot)) {
                    Assert.assertTrue("ACKed store-and-forward state did not drain", reopened.drain(15_000));
                }
                drainWalQueue();
                assertQuery("select count() from " + TABLE)
                        .noLeakCheck()
                        .expectSize().noRandomAccess().returns("count\n2\n");
            });
        });
    }

    @Test
    public void testPublicSenderRecoversStaleIdentityFrameAfterRetype() throws Exception {
        assertMemoryLeak(() -> {
            String table = TABLE + "_evolution";
            execute("create table " + table
                    + " (id uuid, sym symbol, marker varchar, ts timestamp)"
                    + " timestamp(ts) partition by day wal");
            File sfRoot = temp.newFolder("qwp-schema-sender-recovery-evolution");

            runInContext(port -> {
                long fsn;
                byte[] publishedFrame;
                try (DataGate gate = new DataGate(port, true);
                     ChildProcess child = new ChildProcess(startProducer(gate.getPort(), sfRoot, table, "evolution"))) {
                    child.awaitLine("A_READY", 15_000);
                    // The producer's batch stays pinned to the UUID snapshot across the
                    // retype: the withheld frame carries no ACK, so nothing can update
                    // its cache, and the frame ships with the stale identity.
                    execute("alter table " + table + " drop column id");
                    execute("alter table " + table + " add column id varchar");
                    child.send("CONTINUE");
                    String published = child.awaitLine("PUBLISHED ", 15_000);
                    fsn = Long.parseLong(published.substring("PUBLISHED ".length()));
                    Assert.assertTrue(gate.awaitData(15, TimeUnit.SECONDS));
                    publishedFrame = gate.getDataFrame();
                    assertStaleUuidBlock(publishedFrame, engine.verifyTableName(table).getTableId());
                    assertQuery("select count() from " + table).noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
                    child.process.destroyForcibly();
                    Assert.assertTrue(child.process.waitFor(15, TimeUnit.SECONDS));
                    Assert.assertNotEquals(0, child.process.exitValue());
                }

                try (DataGate recoveryGate = new DataGate(port, false);
                     Sender recovery = recoverySender(recoveryGate.getPort(), sfRoot)) {
                    Assert.assertTrue(recovery.awaitAckedFsn(fsn, 30_000));
                    Assert.assertTrue(recoveryGate.awaitData(15, TimeUnit.SECONDS));
                    Assert.assertArrayEquals(publishedFrame, recoveryGate.getDataFrame());
                }
                drainWalQueue();
                // The server converts the recovered UUID wire values into the retyped column.
                assertQuery("select marker, sym, id from " + table + " order by marker")
                        .noLeakCheck()
                        .expectSize().returns("marker\tsym\tid\n"
                                + "A\talpha\t123e4567-e89b-12d3-a456-426614174000\n"
                                + "C\tgamma\t223e4567-e89b-12d3-a456-426614174001\n");
            });
        });
    }

    private static Sender recoverySender(int port, File sfRoot) {
        return Sender.fromConfig("ws::addr=localhost:" + port
                + ";sf_dir=" + sfRoot.getAbsolutePath()
                + ";sender_id=recovery;sf_max_segment_bytes=1m;"
                + "close_flush_timeout_millis=30000;");
    }

    private static void assertStaleUuidBlock(byte[] frame, int tableId) throws Exception {
        long address = Unsafe.malloc(frame.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < frame.length; i++) {
                Unsafe.putByte(address + i, frame[i]);
            }
            QwpMessageCursor cursor = new QwpMessageCursor();
            cursor.of(address, frame.length, new ObjList<>());
            Assert.assertTrue(cursor.hasNextTable());
            QwpTableBlockCursor block = cursor.nextTable();
            Assert.assertTrue(block.hasKnownSchemaIdentity());
            Assert.assertEquals(tableId, block.getSchemaTableId());
            Assert.assertEquals(2, block.getRowCount());
            Assert.assertEquals(QwpConstants.TYPE_UUID, block.getColumnDef(0).getTypeCode());
            Assert.assertFalse(cursor.hasNextTable());
        } finally {
            Unsafe.free(address, frame.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static Process startProducer(int port, File sfRoot, String table, String mode) throws IOException {
        File java = new File(new File(System.getProperty("java.home"), "bin"), "java");
        File childTmp = new File(System.getProperty("java.io.tmpdir"), "child");
        Assert.assertTrue(childTmp.exists() || childTmp.mkdirs());
        return new ProcessBuilder(
                java.getAbsolutePath(),
                "--enable-native-access=ALL-UNNAMED",
                "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
                "-Djava.io.tmpdir=" + childTmp.getAbsolutePath(),
                "-cp", System.getProperty("java.class.path"),
                ProducerMain.class.getName(),
                Integer.toString(port),
                sfRoot.getAbsolutePath(),
                table,
                mode
        ).redirectErrorStream(true).start();
    }

    public static final class ProducerMain {
        public static void main(String[] args) throws Exception {
            int port = Integer.parseInt(args[0]);
            String sfRoot = args[1];
            String table = args[2];
            boolean evolution = "evolution".equals(args[3]);
            Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                    + ";sf_dir=" + sfRoot + ";sender_id=recovery;sf_max_segment_bytes=1m;"
                    + "auto_flush_rows=2147483647;auto_flush_bytes=0;auto_flush_interval=2147483646;"
                    + "close_flush_timeout_millis=0;");
            sender.table(table)
                    .stringColumn("id", "123e4567-e89b-12d3-a456-426614174000")
                    .symbol("sym", "alpha")
                    .stringColumn("marker", "A")
                    .at(Instant.parse("2026-01-01T00:00:00Z"));
            if (evolution) {
                System.out.println("A_READY");
                System.out.flush();
                new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)).readLine();
            }
            try {
                sender.table(table)
                        .symbol("sym", "beta")
                        .stringColumn("marker", "B")
                        .stringColumn("id", "not-a-uuid");
                throw new AssertionError("expected schema rejection");
            } catch (LineSenderSchemaException expected) {
                // The batch validates against its pinned UUID snapshot either way.
                if (expected.getReason() != LineSenderSchemaException.Reason.INVALID_VALUE) {
                    throw expected;
                }
            }
            sender.table(table)
                    .stringColumn("id", "223e4567-e89b-12d3-a456-426614174001")
                    .symbol("sym", "gamma")
                    .stringColumn("marker", "C")
                    .at(Instant.parse("2026-01-01T00:00:01Z"));
            long fsn = sender.flushAndGetSequence();
            System.out.println("PUBLISHED " + fsn);
            System.out.flush();
            // The parent kills this JVM. Deliberately never close Sender.
            while (System.in.read() != -1) {
                // bounded by the parent process lifetime
            }
        }
    }

    private static final class ChildProcess implements Closeable {
        private final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<>();
        private final Process process;
        private final StringBuilder seen = new StringBuilder();
        private final Thread stdout;

        private ChildProcess(Process process) {
            this.process = process;
            stdout = new Thread(() -> {
                try (BufferedReader in = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        synchronized (seen) {
                            seen.append(line).append('\n');
                        }
                        lines.add(line);
                    }
                } catch (IOException ignore) {
                    // Process teardown closes the stream.
                }
            }, "qwp-schema-recovery-child-output");
            stdout.setDaemon(true);
            stdout.start();
        }

        @Override
        public void close() throws IOException {
            if (process.isAlive()) {
                process.destroyForcibly();
                try {
                    Assert.assertTrue("producer JVM did not terminate during cleanup",
                            process.waitFor(15, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
            try {
                stdout.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            Assert.assertFalse("child stdout reader did not terminate", stdout.isAlive());
        }

        private String awaitLine(String prefix, long timeoutMillis) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            while (System.nanoTime() < deadline) {
                String line = lines.poll(100, TimeUnit.MILLISECONDS);
                if (line != null && line.startsWith(prefix)) {
                    return line;
                }
                if (!process.isAlive()) {
                    Assert.fail("producer exited before readiness [exit=" + process.exitValue()
                            + ", output=" + output() + ']');
                }
            }
            Assert.fail("timed out waiting for child output " + prefix + " [output=" + output() + ']');
            return null;
        }

        private void send(String line) throws IOException {
            process.getOutputStream().write((line + '\n').getBytes(StandardCharsets.UTF_8));
            process.getOutputStream().flush();
        }

        private String output() {
            synchronized (seen) {
                return seen.toString();
            }
        }
    }

    private static final class DataGate implements Closeable {
        private final CountDownLatch data = new CountDownLatch(1);
        private final boolean blockData;
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final Thread relay;
        private final ServerSocket server;
        private final int upstreamPort;
        private volatile byte[] dataFrame;
        private volatile boolean closed;
        private volatile Socket downstream;
        private volatile Thread replies;
        private volatile Socket upstream;

        private DataGate(int upstreamPort, boolean blockData) throws IOException {
            this.upstreamPort = upstreamPort;
            this.blockData = blockData;
            server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
            relay = new Thread(this::run, "qwp-schema-recovery-gate");
            relay.setDaemon(true);
            relay.start();
        }

        @Override
        public void close() {
            synchronized (this) {
                closed = true;
                closeQuietly(server);
                closeQuietly(downstream);
                closeQuietly(upstream);
            }
            try {
                relay.join(5_000);
                Assert.assertFalse("gate relay did not terminate", relay.isAlive());
                Thread replyThread = replies;
                if (replyThread != null) {
                    replyThread.join(5_000);
                    Assert.assertFalse("server reply pump did not terminate", replyThread.isAlive());
                }
                assertHealthy();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while closing WebSocket data gate", e);
            }
        }

        private boolean awaitData(long timeout, TimeUnit unit) throws InterruptedException {
            boolean captured = data.await(timeout, unit);
            assertHealthy();
            return captured;
        }

        private byte[] getDataFrame() {
            assertHealthy();
            return dataFrame;
        }

        private void assertHealthy() {
            Throwable error = failure.get();
            if (error != null) {
                throw new AssertionError("WebSocket data gate failed", error);
            }
        }

        private int getPort() {
            return server.getLocalPort();
        }

        private static void closeQuietly(Closeable closeable) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException ignore) {
                }
            }
        }

        private static byte[] readFully(InputStream in, int length) throws IOException {
            byte[] bytes = new byte[length];
            int offset = 0;
            while (offset < length) {
                int n = in.read(bytes, offset, length - offset);
                if (n < 0) {
                    throw new EOFException();
                }
                offset += n;
            }
            return bytes;
        }

        private static void forwardHttpUpgrade(InputStream in, OutputStream out) throws IOException {
            int state = 0;
            while (state < 4) {
                int b = in.read();
                if (b < 0) {
                    throw new EOFException();
                }
                out.write(b);
                state = b == "\r\n\r\n".charAt(state) ? state + 1 : (b == '\r' ? 1 : 0);
            }
            out.flush();
        }

        private void clientToServer(InputStream in, OutputStream out) throws IOException {
            forwardHttpUpgrade(in, out);
            for (; ; ) {
                int b0 = in.read();
                int b1 = in.read();
                if (b1 < 0) {
                    return;
                }
                if ((b0 & 0x80) == 0) {
                    throw new IOException("fragmented WebSocket frames are outside this bounded fixture");
                }
                if ((b0 & 0x0f) == 0) {
                    throw new IOException("continuation WebSocket frames are outside this bounded fixture");
                }
                int lengthCode = b1 & 0x7f;
                byte[] extended = lengthCode == 126 ? readFully(in, 2)
                        : lengthCode == 127 ? readFully(in, 8) : new byte[0];
                long length = lengthCode;
                if (lengthCode == 126) {
                    length = ((extended[0] & 0xff) << 8) | (extended[1] & 0xff);
                } else if (lengthCode == 127) {
                    if ((extended[0] & 0x80) != 0) {
                        throw new IOException("invalid negative WebSocket payload length");
                    }
                    length = 0;
                    for (byte b : extended) {
                        length = (length << 8) | (b & 0xff);
                    }
                }
                if (length > 16 * 1024 * 1024) {
                    throw new IOException("oversized test frame");
                }
                byte[] mask = (b1 & 0x80) != 0 ? readFully(in, 4) : new byte[0];
                if (mask.length == 0) {
                    throw new IOException("client WebSocket frame is not masked");
                }
                byte[] payload = readFully(in, (int) length);
                byte[] decoded = payload.clone();
                if (mask.length != 0) {
                    for (int i = 0; i < decoded.length; i++) {
                        decoded[i] ^= mask[i & 3];
                    }
                }
                boolean qwpControl = decoded.length >= 12
                        && (decoded[5] & QwpSchemaProtocol.FLAG_CONTROL) != 0;
                boolean tableBearing = decoded.length >= 8
                        && ((decoded[6] & 0xff) | ((decoded[7] & 0xff) << 8)) != 0;
                if ((b0 & 0x0f) == 2 && !qwpControl && tableBearing) {
                    if (dataFrame != null) {
                        throw new IOException("fixture expected exactly one table-bearing frame");
                    }
                    dataFrame = decoded;
                    data.countDown();
                    if (blockData) {
                        continue;
                    }
                }
                out.write(b0);
                out.write(b1);
                out.write(extended);
                out.write(mask);
                out.write(payload);
                out.flush();
            }
        }

        private void run() {
            try (Socket child = server.accept();
                 Socket questdb = new Socket(InetAddress.getLoopbackAddress(), upstreamPort)) {
                synchronized (this) {
                    if (closed) {
                        return;
                    }
                    downstream = child;
                    upstream = questdb;
                }
                replies = new Thread(() -> {
                    try {
                        questdb.getInputStream().transferTo(child.getOutputStream());
                    } catch (IOException ignore) {
                    }
                }, "qwp-schema-recovery-gate-replies");
                replies.setDaemon(true);
                replies.start();
                clientToServer(child.getInputStream(), questdb.getOutputStream());
            } catch (IOException ignore) {
                if (!closed) {
                    failure.compareAndSet(null, ignore);
                    data.countDown();
                }
            }
        }
    }
}
