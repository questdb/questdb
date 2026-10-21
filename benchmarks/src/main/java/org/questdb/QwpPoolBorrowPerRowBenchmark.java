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

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.std.ObjList;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Benchmark for pooled {@link Sender} ingest where the application borrows a
 * sender for every row, the shape of a handler that writes one message per call.
 * Each pass ingests the rows into a freshly created WAL table through a pool of
 * one sender and prints:
 * <ul>
 *   <li>{@code loop ms} — time to run the borrow loop;</li>
 *   <li>{@code close ms} — time until {@link QuestDB#close()} returns, so every
 *   row has been sent and acknowledged;</li>
 *   <li>{@code applied ms} — time until the server has applied every WAL
 *   transaction of the pass, after which the row count is checked;</li>
 *   <li>{@code WAL txns} — the number of WAL transactions (commits) the pass
 *   produced.</li>
 * </ul>
 * All times are measured from the first borrow. Every configuration first runs
 * once at the warm-up row count, unreported.
 *
 * <p>The commit window ({@code commit_rows}, {@code commit_interval_ms}) removes
 * the per-row commits but still ships one frame per {@code close()}, so its gain
 * depends on what a commit costs on the server. Run the server once with the
 * default {@code cairo.commit.mode} and once with {@code QDB_CAIRO_COMMIT_MODE=sync}
 * to see both ends. The header line prints the server's commit mode.
 *
 * <p>Run against a QuestDB instance listening on port 9000 (HTTP/WS) and 8812
 * (PgWire), for example one started from this repository's build with the JVM
 * options from {@code questdb.sh}:
 * <pre>
 * java [JVM options] -p core/target/questdb-[version].jar -m io.questdb/io.questdb.ServerMain -d [root]
 * java -cp benchmarks/target/benchmarks.jar -Drows=1000000 -Dwarmup=50000 -Dreps=2 \
 *      org.questdb.QwpPoolBorrowPerRowBenchmark
 * </pre>
 * Use {@code -Dhost} for a server on another machine.
 */
public class QwpPoolBorrowPerRowBenchmark {
    private static final String HOST = System.getProperty("host", "localhost");
    private static final int HTTP_PORT = 9000;
    private static final int PG_PORT = 8812;
    private static final String POOL_CONFIG = "ws::addr=" + HOST + ":" + HTTP_PORT
            + ";sender_pool_min=1;sender_pool_max=1;query_pool_min=0;close_flush_timeout_millis=600000;";
    private static final int REPS = Integer.getInteger("reps", 2);
    private static final int ROWS = Integer.getInteger("rows", 1_000_000);
    private static final String TABLE_NAME = "pool_borrow_bench";
    private static final int WARMUP_ROWS = Integer.getInteger("warmup", 50_000);

    public static void main(String[] args) throws Exception {
        final ObjList<Config> configs = new ObjList<>();
        configs.add(new Config("held lease, default", "", false));
        configs.add(new Config("borrow per row, default", "", true));
        configs.add(new Config("borrow per row, flush_on_close=on (previous behaviour)", "flush_on_close=on;", true));
        configs.add(new Config(
                "borrow per row, flush_on_close=on + window 1000/100",
                "flush_on_close=on;commit_rows=1000;commit_interval_ms=100;",
                true
        ));
        try (Connection conn = createConnection();
             Statement st = conn.createStatement()) {
            System.out.printf("rows=%d cairo.commit.mode=%s%n", ROWS, readCommitMode(st));
            System.out.printf("%-55s %9s %9s %10s %10s %12s%n",
                    "configuration", "loop ms", "close ms", "applied ms", "WAL txns", "rows/s");
            for (int i = 0, n = configs.size(); i < n; i++) {
                Config c = configs.getQuick(i);
                runOnce(st, c.label, c.extraConfig, c.isBorrowPerRow, WARMUP_ROWS, false);
            }
            for (int rep = 0; rep < REPS; rep++) {
                for (int i = 0, n = configs.size(); i < n; i++) {
                    Config c = configs.getQuick(i);
                    runOnce(st, c.label, c.extraConfig, c.isBorrowPerRow, ROWS, true);
                }
            }
        }
    }

    private static void awaitApplied(Statement st, long seqTxn) throws Exception {
        while (true) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT suspended, writerTxn FROM wal_tables() WHERE name = '" + TABLE_NAME + "'")) {
                if (rs.next()) {
                    if (rs.getBoolean(1)) {
                        throw new IllegalStateException("table suspended: " + TABLE_NAME);
                    }
                    if (rs.getLong(2) >= seqTxn) {
                        return;
                    }
                }
            }
            Thread.sleep(1);
        }
    }

    private static Connection createConnection() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        properties.setProperty("binaryTransfer", Boolean.toString(true));
        properties.setProperty("preferQueryMode", "extended");
        String url = String.format("jdbc:postgresql://%s:%d/qdb", HOST, PG_PORT);
        return DriverManager.getConnection(url, properties);
    }

    private static String readCommitMode(Statement st) throws Exception {
        try (ResultSet rs = st.executeQuery(
                "SELECT value FROM (SHOW PARAMETERS) WHERE property_path = 'cairo.commit.mode'")) {
            return rs.next() ? rs.getString(1) : "unknown";
        }
    }

    private static long readSeqTxn(Statement st) throws Exception {
        try (ResultSet rs = st.executeQuery(
                "SELECT sequencerTxn FROM wal_tables() WHERE name = '" + TABLE_NAME + "'")) {
            if (!rs.next()) {
                throw new IllegalStateException("table not found: " + TABLE_NAME);
            }
            return rs.getLong(1);
        }
    }

    private static void recreateTable(Statement st) throws Exception {
        st.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
        st.execute("CREATE TABLE " + TABLE_NAME
                + " (sym SYMBOL, px DOUBLE, qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private static void runOnce(
            Statement st,
            String label,
            String extraConfig,
            boolean isBorrowPerRow,
            int rows,
            boolean isMeasured
    ) throws Exception {
        recreateTable(st);
        final long seqTxnBefore = readSeqTxn(st);
        final long start;
        final long loopEnd;
        try (QuestDB db = QuestDB.connect(POOL_CONFIG + extraConfig)) {
            start = System.nanoTime();
            if (isBorrowPerRow) {
                for (int i = 0; i < rows; i++) {
                    try (Sender sender = db.borrowSender()) {
                        sender.table(TABLE_NAME).symbol("sym", "ABC").doubleColumn("px", i).longColumn("qty", i).atNow();
                    }
                }
            } else {
                try (Sender sender = db.borrowSender()) {
                    for (int i = 0; i < rows; i++) {
                        sender.table(TABLE_NAME).symbol("sym", "ABC").doubleColumn("px", i).longColumn("qty", i).atNow();
                    }
                }
            }
            loopEnd = System.nanoTime();
        }
        final long closeEnd = System.nanoTime();
        final long seqTxn = readSeqTxn(st);
        awaitApplied(st, seqTxn);
        final long appliedEnd = System.nanoTime();

        try (ResultSet rs = st.executeQuery("SELECT count() FROM " + TABLE_NAME)) {
            rs.next();
            final long count = rs.getLong(1);
            if (count != rows) {
                throw new AssertionError(label + ": expected " + rows + " rows but got " + count);
            }
        }
        if (isMeasured) {
            final long appliedMs = (appliedEnd - start) / 1_000_000;
            System.out.printf("%-55s %9d %9d %10d %10d %12d%n",
                    label,
                    (loopEnd - start) / 1_000_000,
                    (closeEnd - start) / 1_000_000,
                    appliedMs,
                    seqTxn - seqTxnBefore,
                    rows * 1000L / Math.max(1, appliedMs)
            );
        }
    }

    private record Config(String label, String extraConfig, boolean isBorrowPerRow) {
    }
}
