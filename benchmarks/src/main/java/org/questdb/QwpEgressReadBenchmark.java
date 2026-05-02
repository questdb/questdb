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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
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
 * Application-style benchmark that measures SELECT throughput from a locally
 * running QuestDB instance over three wire protocols and prints a comparison:
 * <ul>
 *   <li>QWP egress (WebSocket, binary columnar) -- the protocol added in this PR</li>
 *   <li>PostgreSQL wire (binary transfer)</li>
 *   <li>HTTP /exec (JSON)</li>
 * </ul>
 * This is the narrow variant: five columns (designated timestamp, one LONG,
 * one DOUBLE, one low-cardinality SYMBOL, one VARCHAR). {@link QwpEgressReadBenchmarkWide}
 * is a companion that runs the same three paths over a 15-column row with five
 * extra DOUBLEs and five high-cardinality SYMBOLs.
 * <p>
 * Prerequisites:
 * <ul>
 *   <li>A QuestDB server listening on 9000 (HTTP/WS) and 8812 (PG wire).</li>
 *   <li>The default ILP/HTTP port for ingest (9000).</li>
 * </ul>
 * <p>
 * Tune the workload via system properties:
 * <ul>
 *   <li>{@code -DrowCount=<n>} (default 10_000_000)</li>
 *   <li>{@code -Dskip.populate=true} to re-use an existing table</li>
 * </ul>
 */
public class QwpEgressReadBenchmark {

    private static final long DEFAULT_ROW_COUNT = 10_000_000L;
    private static final String HOST = "localhost";
    private static final int HTTP_PORT = 9000;
    private static final int PG_PORT = 8812;
    private static final long PROGRESS_INTERVAL = 1_000_000;
    private static final long ROW_COUNT;
    private static final boolean SKIP_POPULATE;
    private static final String TABLE_NAME = "egress_bench";

    static {
        ROW_COUNT = Long.getLong("rowCount", DEFAULT_ROW_COUNT);
        SKIP_POPULATE = Boolean.parseBoolean(System.getProperty("skip.populate", "false"));
    }

    public static void main(String[] args) throws Exception {
        if (!SKIP_POPULATE) {
            recreateTable();
            ingestRows();
        } else {
            System.out.println("skip.populate=true, re-using existing " + TABLE_NAME);
        }

        System.out.println();
        System.out.println("=== Cold warm-up (runs discarded) ===");
        runQwp(/*warmup=*/ true);
//        runPgWire(/*warmup=*/ true);
//        runHttpExec(/*warmup=*/ true);

        System.out.println();
        System.out.println("=== Measurement ===");
        Result qwp = runQwp(false);
//        Result pg = runPgWire(false);
//        Result http = runHttpExec(false);

        System.out.println();
        System.out.println("=== Comparison ===");
        System.out.printf("%-20s %12s %12s %12s%n", "Protocol", "time(ms)", "rows/sec", "MiB/sec");
        System.out.printf("%-20s %12s %12s %12s%n", "--------", "--------", "--------", "-------");
        printRow("QWP egress (WS)", qwp);
//        printRow("PostgreSQL wire", pg);
//        printRow("HTTP /exec JSON", http);
    }

    private static void printRow(String label, Result r) {
        double secs = r.elapsedNanos / 1e9;
        double rowsPerSec = r.rows / secs;
        double mibPerSec = r.bytes / secs / (1024.0 * 1024.0);
        System.out.printf("%-20s %12d %,12.0f %12.2f%n",
                label, TimeUnit.NANOSECONDS.toMillis(r.elapsedNanos), rowsPerSec, mibPerSec);
    }

    // ------------------------------------------------------------------
    // Workload
    // ------------------------------------------------------------------

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

    private static void recreateTable() throws Exception {
        try (Connection c = createPgConnection(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            // Columns: designated timestamp, one LONG id, one DOUBLE price, one SYMBOL sym, one VARCHAR note.
            // Representative shape for time-series analytics.
            st.execute("CREATE TABLE " + TABLE_NAME + " ("
                    + "ts TIMESTAMP, id LONG, price DOUBLE, sym SYMBOL, note VARCHAR"
                    + ") TIMESTAMP(ts) PARTITION BY HOUR WAL");
        }
    }

    private static void ingestRows() {
        System.out.printf("Ingesting %,d rows over QWP/WebSocket...%n", ROW_COUNT);
        long start = System.nanoTime();
        String[] symbols = {"AAPL", "MSFT", "GOOG", "AMZN", "META", "TSLA", "NVDA", "NFLX"};
        try (Sender sender = Sender.fromConfig("ws::addr=" + HOST + ":" + HTTP_PORT + ";auto_flush_rows=50000;compression=raw")) {
            for (long i = 1; i <= ROW_COUNT; i++) {
                // ILP requires all symbol() calls before any non-symbol column setters.
                sender.table(TABLE_NAME)
                        .symbol("sym", symbols[(int) (i % symbols.length)])
                        .longColumn("id", i)
                        .doubleColumn("price", i * 1.5)
                        .stringColumn("note", "n" + (i & 0xFFF))
                        .at(i * 10_000L, ChronoUnit.MICROS); // 10ms spacing
                if (i % PROGRESS_INTERVAL == 0) {
                    long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                    System.out.printf("  %,d / %,d rows (%,d ms)%n", i, ROW_COUNT, ms);
                }
            }
            sender.flush();
        }

        // Wait for WAL to finish applying before the read phase.
        System.out.println("Waiting for WAL apply to complete...");
        try (Connection c = createPgConnection(); Statement st = c.createStatement()) {
            for (int attempt = 0; attempt < 600; attempt++) {
                try (ResultSet rs = st.executeQuery("SELECT count() FROM " + TABLE_NAME)) {
                    rs.next();
                    long count = rs.getLong(1);
                    if (count == ROW_COUNT) {
                        System.out.printf("  applied %,d rows%n", count);
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

    // ------------------------------------------------------------------
    // QWP egress
    // ------------------------------------------------------------------

    private static Result runQwp(boolean warmup) throws Exception {
        final long[] rowsSeen = {0};
        final long[] bytesSeen = {0};
        final long[] checksum = {0};
        long start = System.nanoTime();
        try (QwpQueryClient client = QwpQueryClient.fromConfig(
                "ws::addr=" + HOST + ":" + HTTP_PORT + ";client_id=qwp-egress-bench/1.0;compression=raw")) {
            client.connect();
            client.execute("SELECT ts, id, price, sym, note FROM " + TABLE_NAME, new QwpColumnBatchHandler() {
                @Override
                public void onBatch(QwpColumnBatch batch) {
                    int n = batch.getRowCount();
                    // Fixed-width 8-byte columns: ts (TIMESTAMP), id (LONG), price (DOUBLE).
                    // Sym (SYMBOL) and note (VARCHAR) use DirectUtf8 views.
                    long tsBase = batch.valuesAddr(0);
                    long idBase = batch.valuesAddr(1);
                    long priceBase = batch.valuesAddr(2);
                    int[] tsIdx = batch.nonNullIndex(0);
                    int[] idIdx = batch.nonNullIndex(1);
                    int[] priceIdx = batch.nonNullIndex(2);
                    for (int r = 0; r < n; r++) {
                        long ts = io.questdb.std.Unsafe.getLong(tsBase + 8L * tsIdx[r]);
                        long id = io.questdb.std.Unsafe.getLong(idBase + 8L * idIdx[r]);
                        long priceBits = io.questdb.std.Unsafe.getLong(priceBase + 8L * priceIdx[r]);
                        DirectUtf8Sequence sym = batch.getStrA(3, r);
                        DirectUtf8Sequence note = batch.getStrB(4, r);
                        checksum[0] ^= ts ^ id ^ priceBits
                                ^ (sym != null ? sym.size() : 0)
                                ^ (note != null ? note.size() : 0);
                    }
                    rowsSeen[0] += n;
                    // Sum the actual QWP message bytes delivered in this frame. The batch
                    // view holds a native slice [payloadAddr .. payloadLimit) that matches
                    // the WebSocket payload length reported by the frame parser. Add 10
                    // bytes per batch to approximate the WebSocket header for large frames.
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
        log("QWP", warmup, elapsed, rowsSeen[0], checksum[0]);
        return new Result(elapsed, rowsSeen[0], bytesSeen[0]);
    }

    // ------------------------------------------------------------------
    // PostgreSQL wire
    // ------------------------------------------------------------------

    private static Result runPgWire(boolean warmup) throws Exception {
        long rows = 0;
        long checksum = 0;
        long bytes = 0;
        long start = System.nanoTime();
        try (Connection c = createPgConnection(); Statement st = c.createStatement()) {
            // PG JDBC honours setFetchSize only when autoCommit is false and the
            // result set is forward-only -- otherwise the driver materialises the
            // entire result into heap. With 10M rows at 5 columns that OOMs.
            c.setAutoCommit(false);
            st.setFetchSize(10_000);
            try (ResultSet rs = st.executeQuery("SELECT ts, id, price, sym, note FROM " + TABLE_NAME)) {
                while (rs.next()) {
                    // Normalise to epoch microseconds so the checksum matches the QWP path.
                    long ts = rs.getTimestamp(1).getTime() * 1000L;
                    long id = rs.getLong(2);
                    double price = rs.getDouble(3);
                    String sym = rs.getString(4);
                    String note = rs.getString(5);
                    checksum ^= ts ^ id ^ Double.doubleToLongBits(price)
                            ^ (sym != null ? sym.length() : 0)
                            ^ (note != null ? note.length() : 0);
                    // PG DataRow wire size per row in binary mode: 1 byte 'D' msg tag,
                    // 4 bytes msg length, 2 bytes col count, then 4-byte length prefix +
                    // value for each of the 5 columns. ts/id/price are 8 bytes each, sym +
                    // note vary - measure directly rather than estimating.
                    int symBytes = sym != null ? sym.getBytes(StandardCharsets.UTF_8).length : 0;
                    int noteBytes = note != null ? note.getBytes(StandardCharsets.UTF_8).length : 0;
                    bytes += 7 + 5 * 4 + 8 * 3 + symBytes + noteBytes;
                    rows++;
                }
            }
        }
        long elapsed = System.nanoTime() - start;
        log("PG", warmup, elapsed, rows, checksum);
        return new Result(elapsed, rows, bytes);
    }

    // ------------------------------------------------------------------
    // HTTP /exec JSON
    // ------------------------------------------------------------------

    private static Result runHttpExec(boolean warmup) throws Exception {
        long bytes = 0;
        long start = System.nanoTime();
        String sql = "SELECT+ts,id,price,sym,note+FROM+" + TABLE_NAME;
        URL url = new URL("http://" + HOST + ":" + HTTP_PORT + "/exec?query=" + sql + "&count=true");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("Accept-Encoding", "identity");
        long rows = 0;
        try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8), 128 * 1024)) {
            // JSON response is one line with {"columns":[...],"dataset":[[...],[...]...]}. Scan for '[' to count rows.
            char[] buf = new char[16 * 1024];
            int n;
            while ((n = in.read(buf)) > 0) {
                bytes += n;
                for (int i = 0; i < n; i++) {
                    if (buf[i] == '[') rows++;
                }
            }
        }
        long elapsed = System.nanoTime() - start;
        // Rows counter was incremented for every '[' including the outer wrappers; subtract those.
        long rowCount = rows > 1 ? rows - 2 : 0;
        log("HTTP", warmup, elapsed, rowCount, bytes);
        return new Result(elapsed, rowCount, bytes);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static void log(String label, boolean warmup, long elapsedNanos, long rows, long checksumOrBytes) {
        String phase = warmup ? "[warmup]" : "[measure]";
        System.out.printf("%s %s : %,d rows in %,d ms (checksum/bytes=0x%x)%n",
                phase, label, rows, TimeUnit.NANOSECONDS.toMillis(elapsedNanos), checksumOrBytes);
    }

    private record Result(long elapsedNanos, long rows, long bytes) {
    }
}
