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

package org.questdb;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.std.str.DirectUtf8Sequence;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Application-style benchmark for the QWP egress path when the SELECT can be
 * satisfied entirely from a posting+covering index. The schema declares one
 * indexed SYMBOL with a wide INCLUDE list, and the workload streams every row
 * matching a single symbol value back to the client. Compares three storage
 * paths over QWP plus a PG wire control:
 * <ul>
 *   <li>QWP + covering: posting list iterates rowids, sidecar serves the
 *       projected columns -- no main column files touched.</li>
 *   <li>QWP + posting only: same posting list, but {@code /*+ no_covering *}{@code /}
 *       forces the executor to read projected columns from the main files.</li>
 *   <li>QWP + full scan: {@code /*+ no_index *}{@code /} disables the index entirely;
 *       the engine evaluates the WHERE clause column-by-column over every row.</li>
 *   <li>PG wire + covering: same SQL as the headline path, different protocol.</li>
 * </ul>
 * Each measurement run accumulates a checksum over the streamed bytes so the
 * JIT cannot eliminate the loop, and reports rows/s plus MiB/s on the wire.
 * <p>
 * Workload defaults: 100M rows total across 100 distinct symbol values, so a
 * single-symbol filter selects ~1M rows. The projected row carries 12 columns
 * (timestamp + indexed symbol + 5 doubles + 2 longs + 1 int + 2 varchars),
 * giving ~90 bytes per row on the wire. The indexed table is partitioned by
 * HOUR (~290 partitions); all but the most recent are sealed by the time the
 * read phase starts, so the covering path covers >99% of selected rows.
 * <p>
 * Prerequisites:
 * <ul>
 *   <li>A QuestDB server listening on 9000 (HTTP/WS) and 8812 (PG wire).</li>
 *   <li>Server compiled with covering+posting index support.</li>
 * </ul>
 * <p>
 * Tunables:
 * <ul>
 *   <li>{@code -DrowCount=<n>} (default 100_000_000)</li>
 *   <li>{@code -DsymbolCardinality=<n>} (default 100; rows per symbol = rowCount / cardinality)</li>
 *   <li>{@code -DfilterValue=<sym>} (default {@code s_0}; the single value selected by WHERE)</li>
 *   <li>{@code -DmeasureRuns=<n>} (default 3; reports the best run, prints all)</li>
 *   <li>{@code -Dskip.populate=true} to re-use an existing table between runs</li>
 * </ul>
 */
public class QwpEgressCoveringBenchmark {

    private static final int DATA_SPAN_DAYS;
    private static final int DEFAULT_DATA_SPAN_DAYS = 0;
    private static final String DEFAULT_FILTER_VALUE = "s_0";
    private static final int DEFAULT_MEASURE_RUNS = 3;
    private static final long DEFAULT_ROW_COUNT = 100_000_000L;
    // Fallback spacing used when DATA_SPAN_DAYS is 0/unset. Distinct constant
    // so the ingest loop can pick at runtime without re-reading System props.
    private static final long DEFAULT_ROW_SPACING_MICROS = 10_000L;
    private static final int DEFAULT_SYMBOL_CARDINALITY = 100;
    private static final String FILTER_VALUE;
    private static final String HOST = "localhost";
    private static final int HTTP_PORT = 9000;
    private static final int MEASURE_RUNS;
    private static final int NOTE_POOL_SIZE = 4096;
    private static final int PG_PORT = 8812;
    private static final long PROGRESS_INTERVAL = 5_000_000;
    // Column projection used by every read path. Order matches the QWP onBatch
    // column indices in runQwp() -- changing this list requires updating the
    // hoisted base/idx pulls in the inner loop.
    private static final String PROJECTION =
            "ts, sym, price, qty, d1, d2, d3, id, seq, size, venue, note";
    private static final long ROW_COUNT;
    private static final long ROW_SPACING_MICROS;
    private static final boolean SKIP_POPULATE;
    private static final int SYMBOL_CARDINALITY;
    private static final String TABLE_NAME = "egress_covering_bench";
    // 256 venues / 4096 notes is enough that wire-level compression cannot
    // collapse them to a single dict entry. Pre-built so the ingest loop reuses
    // references rather than allocating per row.
    private static final int VENUE_POOL_SIZE = 256;

    public static void main(String[] args) throws Exception {
        if (!SKIP_POPULATE) {
            recreateTable();
            ingestRows();
            addCoveringIndex();
        } else {
            System.out.println("skip.populate=true, re-using existing " + TABLE_NAME);
        }

        System.out.println();
        System.out.printf("Filter: WHERE sym = '%s'  (1 of %,d distinct values, ~%,d matching rows)%n",
                FILTER_VALUE, SYMBOL_CARDINALITY, ROW_COUNT / SYMBOL_CARDINALITY);

        System.out.println();
        System.out.println("=== Cold warm-up (runs discarded) ===");
        runQwpCovering(true);
        runQwpPostingOnly(true);
        runQwpNoIndex(true);
        runPgWireCovering(true);

        System.out.println();
        System.out.println("=== Measurement (best of " + MEASURE_RUNS + " reported) ===");
        Result qwpCov = bestOf("QWP cov", MEASURE_RUNS, () -> runQwpCovering(false));
        Result qwpPost = bestOf("QWP post", MEASURE_RUNS, () -> runQwpPostingOnly(false));
        Result qwpScan = bestOf("QWP scan", MEASURE_RUNS, () -> runQwpNoIndex(false));
        Result pgCov = bestOf("PG cov", MEASURE_RUNS, () -> runPgWireCovering(false));

        System.out.println();
        System.out.println("=== Comparison ===");
        System.out.printf("%-28s %12s %14s %12s%n", "Path", "time(ms)", "rows/sec", "MiB/sec");
        System.out.printf("%-28s %12s %14s %12s%n", "----", "--------", "--------", "-------");
        printRow("QWP egress + covering", qwpCov);
        printRow("QWP egress + posting only", qwpPost);
        printRow("QWP egress + full scan", qwpScan);
        printRow("PostgreSQL + covering", pgCov);
    }

    @FunctionalInterface
    private interface RunFn {
        Result run() throws Exception;
    }

    private static Result bestOf(String label, int runs, RunFn fn) throws Exception {
        Result best = null;
        for (int i = 0; i < runs; i++) {
            Result r = fn.run();
            if (best == null || r.elapsedNanos < best.elapsedNanos) {
                best = r;
            }
        }
        System.out.printf("  best %s : %,d ms%n", label, TimeUnit.NANOSECONDS.toMillis(best.elapsedNanos));
        return best;
    }

    private static Connection createPgConnection() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", "admin");
        p.setProperty("password", "quest");
        p.setProperty("sslmode", "disable");
        p.setProperty("binaryTransfer", "true");
        p.setProperty("preferQueryMode", "extended");
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        return DriverManager.getConnection(
                String.format("jdbc:postgresql://%s:%d/qdb", HOST, PG_PORT), p);
    }

    private static void ingestRows() {
        double spanDays = (double) (ROW_COUNT * ROW_SPACING_MICROS) / (86_400.0 * 1_000_000.0);
        System.out.printf("Ingesting %,d rows over QWP/WebSocket (%,d distinct symbols, "
                        + "%dus spacing -> ~%.1f day span)...%n",
                ROW_COUNT, SYMBOL_CARDINALITY, ROW_SPACING_MICROS, spanDays);
        long start = System.nanoTime();

        // Pre-compute symbol pool. Cardinality is bounded (100s) so this is cheap
        // and the ingest loop indexes by (i % SYMBOL_CARDINALITY).
        String[] symPool = new String[SYMBOL_CARDINALITY];
        for (int i = 0; i < SYMBOL_CARDINALITY; i++) {
            symPool[i] = "s_" + i;
        }
        // Mid-cardinality varchar pools. Re-used per row so allocation stays
        // off the hot path; the rotating index decorrelates them from sym.
        String[] venuePool = new String[VENUE_POOL_SIZE];
        for (int i = 0; i < VENUE_POOL_SIZE; i++) {
            venuePool[i] = "ven_" + i;
        }
        String[] notePool = new String[NOTE_POOL_SIZE];
        for (int i = 0; i < NOTE_POOL_SIZE; i++) {
            notePool[i] = "note_" + i;
        }

        try (Sender sender = Sender.fromConfig(
                "ws::addr=" + HOST + ":" + HTTP_PORT + ";auto_flush_rows=10000;compression=raw")) {
            for (long i = 1; i <= ROW_COUNT; i++) {
                int symIdx = (int) (i % SYMBOL_CARDINALITY);
                int venueIdx = (int) ((i * 17) % VENUE_POOL_SIZE);
                int noteIdx = (int) ((i * 31) % NOTE_POOL_SIZE);
                // ILP requires all symbol() calls before any non-symbol column setters.
                sender.table(TABLE_NAME)
                        .symbol("sym", symPool[symIdx])
                        .longColumn("id", i)
                        .longColumn("seq", i * 7L)
                        .longColumn("size", 100 + (i & 0x3FF))
                        .doubleColumn("price", 100.0 + (i * 0.001))
                        .doubleColumn("qty", (i & 0xFFF) * 0.5)
                        .doubleColumn("d1", i * 0.25)
                        .doubleColumn("d2", i * 0.5)
                        .doubleColumn("d3", i * 0.75)
                        .stringColumn("venue", venuePool[venueIdx])
                        .stringColumn("note", notePool[noteIdx])
                        .at(i * ROW_SPACING_MICROS, ChronoUnit.MICROS);
                if (i % PROGRESS_INTERVAL == 0) {
                    long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                    System.out.printf("  %,d / %,d rows (%,d ms)%n", i, ROW_COUNT, ms);
                }
            }
            sender.flush();
        }

        // Wait for WAL to finish applying all rows.
        System.out.println("Waiting for WAL apply to complete...");
        try (Connection c = createPgConnection(); Statement st = c.createStatement()) {
            for (int attempt = 0; attempt < 1200; attempt++) {
                try (ResultSet rs = st.executeQuery("SELECT count() FROM " + TABLE_NAME)) {
                    rs.next();
                    long count = rs.getLong(1);
                    if (count == ROW_COUNT) {
                        long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                        System.out.printf("  applied %,d rows (total ingest %,d ms)%n", count, ms);
                        return;
                    }
                }
                Thread.sleep(500);
            }
            throw new AssertionError("timed out waiting for WAL apply");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void log(String label, boolean warmup, long elapsedNanos, long rows, long checksumOrBytes) {
        String phase = warmup ? "[warmup]" : "[measure]";
        System.out.printf("%s %s : %,d rows in %,d ms (checksum/bytes=0x%x)%n",
                phase, label, rows, TimeUnit.NANOSECONDS.toMillis(elapsedNanos), checksumOrBytes);
    }

    private static void printRow(String label, Result r) {
        double secs = r.elapsedNanos / 1e9;
        double rowsPerSec = r.rows / secs;
        double mibPerSec = r.bytes / secs / (1024.0 * 1024.0);
        System.out.printf("%-28s %12d %,14.0f %12.2f%n",
                label, TimeUnit.NANOSECONDS.toMillis(r.elapsedNanos), rowsPerSec, mibPerSec);
    }

    private static void addCoveringIndex() throws Exception {
        // Build the posting+covering index after ingest is complete. Doing it
        // up-front (declared on CREATE TABLE) makes every WAL commit pay for
        // sealPostingIndexesForLastPartitionFastLag, which rebuilds the active
        // partition's sidecars on each commit -- crippling ingest throughput
        // for DAY-sized partitions. Building once after ingest lets the WAL
        // apply path stay cheap, then incurs a single rebuild over all rows.
        System.out.println("Building posting+covering index on sym ...");
        long start = System.nanoTime();
        try (Connection c = createPgConnection(); Statement st = c.createStatement()) {
            st.execute("ALTER TABLE " + TABLE_NAME
                    + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE"
                    + " (price, qty, d1, d2, d3, id, seq, size, venue, note)");
            // ALTER for WAL tables is async; wait_wal_table blocks until the
            // writer's applied txn catches up to the sequencer's, i.e. the
            // index build has finished.
            try (ResultSet rs = st.executeQuery(
                    "SELECT wait_wal_table('" + TABLE_NAME + "')")) {
                rs.next();
                rs.getBoolean(1);
            }
        }
        long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        System.out.printf("  index build complete (%,d ms)%n", ms);
    }

    private static void recreateTable() throws Exception {
        try (Connection c = createPgConnection(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            // No index at create time -- index is added via ALTER after ingest
            // (see addCoveringIndex). Partitioned by DAY: per-partition setup
            // cost dominates posting+covering reads, and DAY keeps the count
            // small without forcing a tiny ts range. The sym INCLUDE list lives
            // in the ALTER, not here.
            st.execute("CREATE TABLE " + TABLE_NAME + " ("
                    + " ts TIMESTAMP,"
                    + " sym SYMBOL CAPACITY 256,"
                    + " price DOUBLE, qty DOUBLE, d1 DOUBLE, d2 DOUBLE, d3 DOUBLE,"
                    + " id LONG, seq LONG, size LONG,"
                    + " venue VARCHAR, note VARCHAR"
                    + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        }
    }

    private static Result runPgWireCovering(boolean warmup) throws Exception {
        long rows = 0;
        long checksum = 0;
        long bytes = 0;
        long start = System.nanoTime();
        try (Connection c = createPgConnection(); Statement st = c.createStatement()) {
            // setFetchSize is honoured only with autoCommit=false + forward-only.
            // Without it the driver materialises the full result into heap.
            c.setAutoCommit(false);
            st.setFetchSize(10_000);
            String sql = "SELECT " + PROJECTION + " FROM " + TABLE_NAME
                    + " WHERE sym = '" + FILTER_VALUE + "'";
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    long ts = rs.getTimestamp(1).getTime() * 1000L;
                    String sym = rs.getString(2);
                    double price = rs.getDouble(3);
                    double qty = rs.getDouble(4);
                    double d1 = rs.getDouble(5);
                    double d2 = rs.getDouble(6);
                    double d3 = rs.getDouble(7);
                    long id = rs.getLong(8);
                    long seq = rs.getLong(9);
                    long size = rs.getLong(10);
                    String venue = rs.getString(11);
                    String note = rs.getString(12);
                    checksum ^= ts ^ id ^ seq ^ size
                            ^ Double.doubleToLongBits(price)
                            ^ Double.doubleToLongBits(qty)
                            ^ Double.doubleToLongBits(d1)
                            ^ Double.doubleToLongBits(d2)
                            ^ Double.doubleToLongBits(d3)
                            ^ (sym != null ? sym.length() : 0)
                            ^ (venue != null ? venue.length() : 0)
                            ^ (note != null ? note.length() : 0);
                    // PG DataRow wire size: 1B 'D' tag + 4B msg len + 2B col count
                    // + 12 col length prefixes (4B each) + fixed-width values + var values.
                    // Fixed: ts(8) + 5*double(40) + 3*long(24) = 72B.
                    int symBytes = sym != null ? sym.getBytes(StandardCharsets.UTF_8).length : 0;
                    int venueBytes = venue != null ? venue.getBytes(StandardCharsets.UTF_8).length : 0;
                    int noteBytes = note != null ? note.getBytes(StandardCharsets.UTF_8).length : 0;
                    bytes += 7 + 12 * 4 + 72 + symBytes + venueBytes + noteBytes;
                    rows++;
                }
            }
        }
        long elapsed = System.nanoTime() - start;
        log("PG cov", warmup, elapsed, rows, checksum);
        return new Result(elapsed, rows, bytes);
    }

    private static Result runQwp(String label, String hint, boolean warmup) throws Exception {
        final long[] rowsSeen = {0};
        final long[] bytesSeen = {0};
        final long[] checksum = {0};
        String sql = "SELECT " + hint + " " + PROJECTION + " FROM " + TABLE_NAME
                + " WHERE sym = '" + FILTER_VALUE + "'";
        long start = System.nanoTime();
        try (QwpQueryClient client = QwpQueryClient.fromConfig(
                "ws::addr=" + HOST + ":" + HTTP_PORT + ";client_id=qwp-covering-bench/1.0;compression=raw;")) {
            client.connect();
            client.execute(sql, new QwpColumnBatchHandler() {
                @Override
                public void onBatch(QwpColumnBatch batch) {
                    int n = batch.getRowCount();
                    // Hoist fixed-width column bases and non-null indices once per batch.
                    // Column order matches PROJECTION:
                    //   0 ts  1 sym  2 price  3 qty  4 d1  5 d2  6 d3
                    //   7 id  8 seq  9 size  10 venue  11 note
                    long tsBase = batch.valuesAddr(0);
                    long priceBase = batch.valuesAddr(2);
                    long qtyBase = batch.valuesAddr(3);
                    long d1Base = batch.valuesAddr(4);
                    long d2Base = batch.valuesAddr(5);
                    long d3Base = batch.valuesAddr(6);
                    long idBase = batch.valuesAddr(7);
                    long seqBase = batch.valuesAddr(8);
                    long sizeBase = batch.valuesAddr(9);
                    int[] tsIdx = batch.nonNullIndex(0);
                    int[] priceIdx = batch.nonNullIndex(2);
                    int[] qtyIdx = batch.nonNullIndex(3);
                    int[] d1Idx = batch.nonNullIndex(4);
                    int[] d2Idx = batch.nonNullIndex(5);
                    int[] d3Idx = batch.nonNullIndex(6);
                    int[] idIdx = batch.nonNullIndex(7);
                    int[] seqIdx = batch.nonNullIndex(8);
                    int[] sizeIdx = batch.nonNullIndex(9);
                    for (int r = 0; r < n; r++) {
                        long ts = io.questdb.std.Unsafe.getLong(tsBase + 8L * tsIdx[r]);
                        long priceBits = io.questdb.std.Unsafe.getLong(priceBase + 8L * priceIdx[r]);
                        long qtyBits = io.questdb.std.Unsafe.getLong(qtyBase + 8L * qtyIdx[r]);
                        long d1 = io.questdb.std.Unsafe.getLong(d1Base + 8L * d1Idx[r]);
                        long d2 = io.questdb.std.Unsafe.getLong(d2Base + 8L * d2Idx[r]);
                        long d3 = io.questdb.std.Unsafe.getLong(d3Base + 8L * d3Idx[r]);
                        long id = io.questdb.std.Unsafe.getLong(idBase + 8L * idIdx[r]);
                        long seq = io.questdb.std.Unsafe.getLong(seqBase + 8L * seqIdx[r]);
                        long size = io.questdb.std.Unsafe.getLong(sizeBase + 8L * sizeIdx[r]);
                        DirectUtf8Sequence sym = batch.getStrA(1, r);
                        DirectUtf8Sequence venue = batch.getStrA(10, r);
                        DirectUtf8Sequence note = batch.getStrB(11, r);
                        checksum[0] ^= ts ^ priceBits ^ qtyBits ^ d1 ^ d2 ^ d3
                                ^ id ^ seq ^ size
                                ^ (sym != null ? sym.size() : 0)
                                ^ (venue != null ? venue.size() : 0)
                                ^ (note != null ? note.size() : 0);
                    }
                    rowsSeen[0] += n;
                    // Wire bytes: native slice [payloadAddr .. payloadLimit) plus a 10-byte
                    // WebSocket header (worst case for >65 KiB frames; batches here always
                    // exceed that).
                    bytesSeen[0] += (batch.payloadLimit() - batch.payloadAddr()) + 10L;
                }

                @Override
                public void onEnd(long totalRows) {
                }

                @Override
                public void onError(byte status, String message) {
                    throw new RuntimeException("QWP error: " + message);
                }
            });
        }
        long elapsed = System.nanoTime() - start;
        log(label, warmup, elapsed, rowsSeen[0], checksum[0]);
        return new Result(elapsed, rowsSeen[0], bytesSeen[0]);
    }

    private static Result runQwpCovering(boolean warmup) throws Exception {
        return runQwp("QWP cov", "", warmup);
    }

    private static Result runQwpNoIndex(boolean warmup) throws Exception {
        return runQwp("QWP scan", "/*+ no_index */", warmup);
    }

    private static Result runQwpPostingOnly(boolean warmup) throws Exception {
        return runQwp("QWP post", "/*+ no_covering */", warmup);
    }

    private record Result(long elapsedNanos, long rows, long bytes) {
    }

    static {
        ROW_COUNT = Long.getLong("rowCount", DEFAULT_ROW_COUNT);
        SYMBOL_CARDINALITY = Integer.getInteger("symbolCardinality", DEFAULT_SYMBOL_CARDINALITY);
        FILTER_VALUE = System.getProperty("filterValue", DEFAULT_FILTER_VALUE);
        MEASURE_RUNS = Integer.getInteger("measureRuns", DEFAULT_MEASURE_RUNS);
        SKIP_POPULATE = Boolean.parseBoolean(System.getProperty("skip.populate", "false"));
        DATA_SPAN_DAYS = Integer.getInteger("dataSpanDays", DEFAULT_DATA_SPAN_DAYS);
        // dataSpanDays>0 distributes ROW_COUNT rows evenly across that many
        // days, which controls partition count under PARTITION BY DAY. The
        // resulting spacing is rounded down to whole microseconds; with very
        // small spans this can collapse to 0/1 and produce duplicate ts, so
        // clamp to 1us minimum.
        if (DATA_SPAN_DAYS > 0) {
            long spanMicros = (long) DATA_SPAN_DAYS * 86_400L * 1_000_000L;
            long spacing = spanMicros / ROW_COUNT;
            ROW_SPACING_MICROS = Math.max(1L, spacing);
        } else {
            ROW_SPACING_MICROS = DEFAULT_ROW_SPACING_MICROS;
        }
    }
}
