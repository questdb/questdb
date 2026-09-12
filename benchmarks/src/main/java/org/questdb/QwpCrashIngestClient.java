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
        // STORE-AND-FORWARD, the half of the contract the server cannot provide. The server's RPO
        // window legitimately discards txns above Wm; the CLIENT closes that window by holding
        // everything not yet durably acked and replaying it on reconnect. That only survives a
        // power cut if the SF buffer is itself durable -- the default is SfDurability.MEMORY, which
        // dies with the process and makes the at-risk rows genuinely lost.
        //
        // sf_dir lives on the CRASHED device on purpose: the client's buffer must take the same
        // power cut as the server's WAL, or the test proves nothing about the pairing.
        final String sfDir = System.getProperty("qwp.sf.dir", "");
        // `flush` and `append` parse but are NOT implemented ("sf_durability=flush is not yet
        // supported"); `periodic` is the strongest available, fsyncing on sf_sync_interval_millis.
        // So the client's buffer is durable only up to the last sync -- which is why the oracle
        // below stays CONSERVATIVE (server-confirmed rows) and the SF claim is proven
        // COMPARATIVELY (replay recovers rows the server lost) rather than as "nothing is lost".
        final String sfDurability = System.getProperty("qwp.sf.durability", "memory");
        final String sfSyncMs = System.getProperty("qwp.sf.sync.ms", "20");
        final boolean replayOnly = Boolean.getBoolean("qwp.replay.only");
        // ENTERPRISE has ACL on by default (acl.enabled=true, admin/quest), so BOTH the WebSocket
        // upgrade and the HTTP queries below need credentials. OSS needs none, so these stay empty
        // unless supplied -- the arm must work against either edition unchanged.
        final String user = System.getProperty("qwp.user", "");
        final String pass = System.getProperty("qwp.password", "");
        final String conf = "ws::addr=" + addr + ";"
                + (user.isEmpty() ? "" : "username=" + user + ";password=" + pass + ";")
                + ("off".equals(ackTier) ? "" : "request_durable_ack=" + ackTier + ";")
                + (sfDir.isEmpty() ? "" : "sf_dir=" + sfDir + ";sf_durability=" + sfDurability
                        + ";sf_sync_interval_millis=" + sfSyncMs + ";")
                // The client runs on a DIFFERENT MACHINE: the power cut kills the server, not this
                // process. So what carries the un-acked window across the outage is the RECONNECT
                // POLICY, not client-side disk durability -- sf_durability matters only when the
                // CLIENT crashes, which is a different scenario. The policy must outlast
                // cut + replay + reboot + server start, or the client gives up first and the test
                // measures its patience instead of its replay.
                + "reconnect_max_duration_millis=" + System.getProperty("qwp.reconnect.max.ms", "600000") + ";"
                + "reconnect_max_backoff_millis=2000;";

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

        if (replayOnly) {
            // REPLAY: construct against the SAME sf_dir so startup recovery picks up the segments
            // that were never durably acked, then close -- drainOnClose replays them into the new
            // session and waits for their acks. No new rows are produced here; this step exists to
            // prove the client's buffer closes the server's RPO gap.
            System.out.println("qwp replay-only: recovering sf_dir=" + sfDir);
            System.out.flush();
            final long before = queryCount(addr);
            try (QwpWebSocketSender replay = (QwpWebSocketSender) Sender.fromConfig(conf)) {
                replay.flush();
            }
            // The replayed rows still have to be APPLIED before they are queryable.
            long after = before;
            for (int i = 0; i < 60; i++) {
                Thread.sleep(1000);
                final long now = queryCount(addr);
                if (now == after && now > before) {
                    break;
                }
                after = now;
            }
            System.out.println("qwp replay-only: server rows " + before + " -> " + after);
            System.out.flush();
            return;
        }

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
                // Wm, straight from the server: wal_tables() exposes localDurableSeqTxn, the
                // highest seqTxn whose WAL commit was fdatasync'd -- the SAME frontier the
                // reference arm reads from SeqTxnTracker in-process. Asking the server avoids the
                // client's internals entirely: in local-only mode the client's durable map is
                // private and getLocalDurableTableWatermark() reads a DIFFERENT map that stays
                // empty (documented), so building on it would have produced a silently-zero
                // watermark -- a no-loss bar that can never fail.
                final long wm = queryLong(addr, "select localDurableSeqTxn from wal_tables() where name = '" + TABLE_NAME + "'");
                // C as well, so _qwp_progress carries the SAME pair as the reference arm's
                // _progress and the adaptive oracle (F >= Wm, at-risk txns in (Wm, C]) applies
                // unchanged to this arm.
                final long c = queryLong(addr, "select sequencerTxn from wal_tables() where name = '" + TABLE_NAME + "'");
                final long serverRows = queryCount(addr);
                if (serverRows < 0) {
                    System.out.println("qwp WARN: server count unavailable; watermark held at " + ackedRowsHighWater);
                    System.out.flush();
                    continue;
                }
                // Monotonic: a transient dip must never lower a watermark already recorded.
                // With a DURABLE sf buffer, every row whose flush() has returned is held on the
                // client's disk and will be replayed, so the client's guarantee covers them even
                // though the server may not have made them durable yet. Without one, fall back to
                // what the server confirms -- claiming more would be the over-claim that produced
                // "acknowledged committed rows were lost" earlier.
                // Stays server-confirmed even with an SF buffer: `periodic` durability means the
                // client's guarantee extends only to its last sync, and claiming `id` would
                // over-claim by up to one sync interval -- the same over-claim that produced
                // "acknowledged committed rows were lost" earlier in this arm.
                final long ackedRows = Math.max(ackedRowsHighWater, serverRows);
                ackedRowsHighWater = ackedRows;
                final long ackedFsn = sender.getAckedFsn();
                writeProgressDurably(dbRoot, progressPath, progressTmp,
                        (ackedRows + "\nC=" + c + "\nWm=" + wm + "\nrows=" + ackedRows + "\n")
                                .getBytes(StandardCharsets.US_ASCII));
                System.out.println("qwp sent=" + id + " serverCommitted=" + ackedRows
                        + " C=" + c + " Wm=" + wm + " ackedFsn=" + ackedFsn);
                System.out.flush();
            }
        }
        System.out.println("reached maxRows=" + MAX_ROWS + " without kill; exiting normally");
    }

    /** Single LONG from a server query; -1 when unavailable or null. */
    private static long queryLong(String addr, String sql) {
        try {
            final java.net.HttpURLConnection c = (java.net.HttpURLConnection) java.net.URI.create(
                    "http://" + addr + "/exec?query="
                            + java.net.URLEncoder.encode(sql, StandardCharsets.UTF_8)).toURL().openConnection();
            final String u = System.getProperty("qwp.user", "");
            if (!u.isEmpty()) {
                c.setRequestProperty("Authorization", "Basic " + java.util.Base64.getEncoder().encodeToString(
                        (u + ":" + System.getProperty("qwp.password", "")).getBytes(StandardCharsets.UTF_8)));
            }
            c.setConnectTimeout(15_000);
            c.setReadTimeout(15_000);
            if (c.getResponseCode() != 200) {
                return -1L;
            }
            final String body = new String(c.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            final java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\[\\[(-?\\d+)\\]\\]").matcher(body);
            return m.find() ? Long.parseLong(m.group(1)) : -1L;
        } catch (Exception e) {
            return -1L;
        }
    }

    /** Rows the SERVER reports it holds; -1 if unavailable (never treated as zero: a failed
     *  query must not retract a watermark the server already justified). */
    private static long queryCount(String addr) {
        try {
            final java.net.HttpURLConnection c = (java.net.HttpURLConnection) java.net.URI.create(
                    "http://" + addr + "/exec?query="
                            + java.net.URLEncoder.encode("select count() from " + TABLE_NAME,
                            StandardCharsets.UTF_8)).toURL().openConnection();
            final String u = System.getProperty("qwp.user", "");
            if (!u.isEmpty()) {
                c.setRequestProperty("Authorization", "Basic " + java.util.Base64.getEncoder().encodeToString(
                        (u + ":" + System.getProperty("qwp.password", "")).getBytes(StandardCharsets.UTF_8)));
            }
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
        final String u = System.getProperty("qwp.user", "");
        if (!u.isEmpty()) {
            c.setRequestProperty("Authorization", "Basic " + java.util.Base64.getEncoder().encodeToString(
                    (u + ":" + System.getProperty("qwp.password", "")).getBytes(StandardCharsets.UTF_8)));
        }
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
