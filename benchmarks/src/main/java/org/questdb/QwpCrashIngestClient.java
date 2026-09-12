package org.questdb;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.temporal.ChronoUnit;

/**
 * QWP ingest arm of the VM crash harness: ingests over a REAL WebSocket connection to a REAL
 * server, so a power cut lands on the wire protocol's write path rather than on an embedded
 * engine.
 *
 * <h3>Why this exists</h3>
 * Every other arm of the harness drives {@link CrashIngestWriter}, which owns the engine in
 * process. That leaves the entire QWP path -- frame decode, ingress buffering, server-side commit,
 * ack emission -- with NO end-to-end crash coverage. The modelled counterpart
 * ({@code AdaptiveQwpDurableAckNoLossCrashTest}) builds its ack frontier directly against a
 * fault-injecting facade and never sends a byte over a socket.
 *
 * <h3>The watermark, and what it does NOT mean</h3>
 * {@link QwpWebSocketSender#getAckedFsn()} is the highest frame the SERVER ACKNOWLEDGED -- it
 * advances on OK frames, which acknowledge a server-side COMMIT. It is NOT a durability signal:
 * {@code STATUS_DURABLE_ACK} is a separate frame type handled on a separate path, and the client's
 * per-table durable watermarks are not reachable from the public sender API today (the accessor
 * the WIP tests use, {@code cursorSendLoopForTest()}, does not exist yet).
 * <p>
 * So the no-loss oracle here is sound ONLY under {@code commitMode=SYNC} (W=0), where the server
 * fsyncs as part of the commit before acking -- there, "committed" and "durable" coincide. Under
 * ADAPTIVE with W&gt;0 they do not, and the acked frontier legitimately runs ahead of what is
 * durable; that variant needs the durable-ack accessor and is deliberately NOT asserted here.
 * Writing this watermark down and claiming no-loss under adaptive would be exactly the over-claim
 * the whole project exists to prevent.
 *
 * <h3>Contract</h3>
 * Rows carry the same deterministic payload as {@link CrashIngestWriter} -- {@code v = id *
 * 2654435761}, {@code s = SYMBOLS[id % 4]}, {@code ts = BASE_TS + id} -- so the existing
 * identity/contiguity/no-duplicate/ts-monotonic oracle verifies this arm unchanged.
 * {@code _qwp_progress} is written with the same fsync-tmp -&gt; rename -&gt; fsync-dir discipline
 * as {@code _progress}: harness bookkeeping is held to the same durability bar as the data it
 * checks, or a cut destroys the evidence needed to judge the cut.
 */
public class QwpCrashIngestClient {

    static final long BASE_TS = CrashIngestWriter.BASE_TS;
    /** Rows per flush. A flush is the unit the server acks, so this is the ack granularity. */
    static final int BATCH = Integer.getInteger("qwp.batch", 1_000);
    static final long MAX_ROWS = Long.getLong("max.rows", 50_000_000L);
    static final String TABLE_NAME = CrashIngestWriter.TABLE_NAME;
    /** Upper bound on waiting for one batch's ack before giving up on it (and NOT claiming it). */
    static final long ACK_TIMEOUT_MS = Long.getLong("qwp.ack.timeout.ms", 30_000L);

    public static void main(String[] args) throws Exception {
        final String dbRoot = args[0];
        final String addr = System.getProperty("qwp.addr", "localhost:9000");
        // Tier is configurable because it is VERSION-DEPENDENT: the pinned client accepts only
        // [on, off]; the `local` tier the WIP tests use is not in this build yet and is rejected
        // at config-parse time. Default to the tier that exists so the arm runs today.
        // Default OFF: an OSS server REFUSES the websocket upgrade outright when durable ack is
        // requested ("server does not support durable ack"), so asking for it makes the arm
        // unrunnable on OSS rather than stricter. Durable ack over QWP is an enterprise / WIP
        // capability; see the class javadoc for what the resulting watermark does and does not mean.
        final String ackTier = System.getProperty("qwp.durable.ack", "off");
        final String conf = "ws::addr=" + addr + ";"
                + ("off".equals(ackTier) ? "" : "request_durable_ack=" + ackTier + ";");

        final Path progressPath = Paths.get(dbRoot, "_qwp_progress");
        final Path progressTmp = Paths.get(dbRoot, "_qwp_progress.tmp");

        System.out.println("qwp client: conf=" + conf + " batch=" + BATCH + " maxRows=" + MAX_ROWS);
        System.out.flush();

        // CREATE THE TABLE EXPLICITLY, with the SAME schema the reference arm uses. Left to QWP's
        // implicit creation the designated timestamp is named `timestamp`, not `ts`, and the shared
        // oracle fails to compile ("Invalid column: ts") at every boundary -- a schema difference
        // reported as a durability verdict. Identical schemas are what let ONE oracle verify both
        // arms; forking the oracle per arm would mean the arms are no longer checked against the
        // same contract.
        final String ddl = "create table if not exists " + TABLE_NAME
                + " (id long, v long, s symbol, ts timestamp)"
                + " timestamp(ts) partition by DAY wal";
        exec(addr, ddl);
        System.out.println("qwp client: created table via " + ddl);

        long id = 0;
        long ackedRowsHighWater = 0L;
        try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(conf)) {
            while (id < MAX_ROWS) {
                final long batchEnd = id + BATCH;
                for (; id < batchEnd; id++) {
                    sender.table(TABLE_NAME)
                            .symbol("s", CrashIngestWriter.SYMBOLS[(int) (id % CrashIngestWriter.SYMBOLS.length)])
                            .longColumn("id", id)
                            .longColumn("v", id * 2_654_435_761L)
                            .at(BASE_TS + id, ChronoUnit.MICROS);
                }
                // THE WATERMARK IS WHAT THE SERVER SAYS IT HAS, not what the client believes it
                // sent or the client's ack bookkeeping claims. Two earlier attempts were unsound:
                //   * `id` (rows SENT) -- flush() != ack, so it over-claimed by ~2x and the oracle
                //     correctly reported "acknowledged committed rows were lost".
                //   * awaitAckedFsn(flushAndGetSequence()) -- flushAndGetSequence() returns -1 here
                //     (publishedFsn is -1 in this configuration even while ackedFsn advances), so
                //     awaiting -1 succeeded instantly and the await was VACUOUS.
                //
                // Asking the server removes the guesswork: count() is rows it has COMMITTED AND
                // APPLIED. Under commitMode=SYNC a commit fsyncs, so those rows are durable. It is
                // also CONSERVATIVE -- committed-but-unapplied rows are not counted -- and
                // under-claiming is the safe direction for a no-loss bar: the recovered table must
                // hold at least what the server already told us it had.
                sender.flush();
                final long serverRows = queryCount(addr);
                if (serverRows < 0) {
                    System.out.println("qwp WARN: server count unavailable; watermark held at " + ackedRowsHighWater);
                    System.out.flush();
                    continue;
                }
                // Monotonic: a transient dip must never lower a watermark already recorded.
                final long ackedRows = Math.max(ackedRowsHighWater, serverRows);
                ackedRowsHighWater = ackedRows;
                final long ackedFsn = sender.getAckedFsn();
                System.out.println("qwp sent=" + id + " serverCommitted=" + ackedRows + " ackedFsn=" + ackedFsn);
                System.out.flush();
            }
        }
        System.out.println("reached maxRows=" + MAX_ROWS + " without kill; exiting normally");
    }

    /** Rows the SERVER reports it holds; -1 if unavailable (never treated as zero: a failed
     *  query must not retract a watermark the server already justified). */
    private static long queryCount(String addr) {
        try {
            final java.net.HttpURLConnection c = (java.net.HttpURLConnection) java.net.URI.create(
                    "http://" + addr + "/exec?query="
                            + java.net.URLEncoder.encode("select count() from " + TABLE_NAME,
                            StandardCharsets.UTF_8)).toURL().openConnection();
            c.setConnectTimeout(15_000);
            c.setReadTimeout(15_000);
            if (c.getResponseCode() != 200) {
                return -1L;
            }
            final String body = new String(c.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            final java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\[\\[(\\d+)\\]\\]").matcher(body);
            return m.find() ? Long.parseLong(m.group(1)) : -1L;
        } catch (Exception e) {
            return -1L;
        }
    }

    /** Runs a statement over the server's HTTP /exec endpoint. Fails loudly: a table that was not
     *  created is not something to discover later as a column error at every crash boundary. */
    private static void exec(String addr, String sql) throws IOException {
        final java.net.HttpURLConnection c = (java.net.HttpURLConnection) java.net.URI.create(
                "http://" + addr + "/exec?query="
                        + java.net.URLEncoder.encode(sql, StandardCharsets.UTF_8)).toURL().openConnection();
        c.setRequestMethod("GET");
        c.setConnectTimeout(30_000);
        c.setReadTimeout(30_000);
        final int rc = c.getResponseCode();
        if (rc != 200) {
            throw new IOException("exec failed rc=" + rc + " sql=" + sql);
        }
        c.getInputStream().readAllBytes();
    }

    /**
     * Same durability discipline as CrashIngestWriter#writeProgressDurably: write tmp, fsync the
     * CONTENT, atomic rename, then fsync the DIRECTORY. Without the directory fsync the rename can
     * be lost on a cut and the verifier reads an empty watermark -- the harness losing its own
     * bookkeeping to the very fault it is measuring.
     */
    private static void writeProgressDurably(String dbRoot, Path progressPath, Path progressTmp, byte[] content)
            throws IOException {
        try (FileChannel ch = FileChannel.open(progressTmp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ch.write(java.nio.ByteBuffer.wrap(content));
            ch.force(true);
        }
        Files.move(progressTmp, progressPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        try (FileOutputStream dir = new FileOutputStream(dbRoot + "/.")) {
            final FileDescriptor fd = dir.getFD();
            fd.sync();
        } catch (IOException ignored) {
            // Opening a directory for write is not portable; on Linux the fsync below covers it.
        }
    }
}
