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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal64;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Writes one seeded corpus through SQL INSERT, ILP over HTTP and QWP over WebSocket, then
 * digests every WAL segment file of the three tables and compares the digest with the
 * recorded one. A WAL writer refactor that keeps the digest writes the same segment bytes
 * on all three ingestion paths.
 * <p>
 * Each path commits once per batch: SQL commits per statement, the ILP sender sends one
 * HTTP request per batch, and the QWP sender waits for the acknowledgement of each batch.
 * Commit boundaries therefore do not depend on timing. WAL apply is off, so no job reads or
 * purges the segments. Set the environment variable {@code QDB_TEST_SEGMENT_DIGEST_RECORD}
 * to rewrite the recorded digest instead of comparing with it.
 */
public class WalSegmentCorpusDigestTest extends AbstractBootstrapTest {
    private static final int BATCH_COUNT = 40;
    private static final String DIGEST_FILE = "src/test/resources/wal-segment-digest/segment-digest.txt";
    private static final int MAX_BATCH_ROWS = 200;
    private static final String RECORD_ENV = "QDB_TEST_SEGMENT_DIGEST_RECORD";
    private static final Pattern SEGMENT_FILE = Pattern.compile("wal\\d+/\\d+/[^/]+");
    private static final long TS_BASE = 1_767_225_600_000_000L; // 2026-01-01T00:00:00Z
    private static final long TS_RANGE = 3 * 86_400_000_000L;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testSegmentBytesMatchRecordedDigest() throws Exception {
        assertMemoryLeak(() -> {
            final String[] tableNames = {"sql_t", "ilp_t", "qwp_t"};
            final String[] dirNames = new String[tableNames.length];
            final String dbRoot;
            try (TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false",
                    PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT.getEnvVarName(), "300",
                    PropertyKey.CAIRO_WAL_INACTIVE_WRITER_TTL.getEnvVarName(), "3600000"
            )) {
                createTables(serverMain);
                final int port = serverMain.getHttpServerPort();
                writeSql(serverMain, new Rnd(0x5EED_0001L, 0xC0FF_EE01L));
                writeIlp(port, new Rnd(0x5EED_0002L, 0xC0FF_EE02L));
                writeQwp(port, new Rnd(0x5EED_0003L, 0xC0FF_EE03L));
                for (int i = 0; i < tableNames.length; i++) {
                    dirNames[i] = serverMain.getEngine().getTableTokenIfExists(tableNames[i]).getDirName();
                }
                dbRoot = serverMain.getEngine().getConfiguration().getDbRoot();
            }
            // The server is closed: every WAL writer has truncated its files to their append offsets.
            final String actual = digestSegments(dbRoot, tableNames, dirNames);
            // The corpus must exercise segment rollover and columns added mid-stream on every path.
            for (String tableName : tableNames) {
                Assert.assertTrue(tableName + " never rolled a segment",
                        Pattern.compile("(?m)^" + tableName + "/wal\\d+/1/").matcher(actual).find());
            }
            Assert.assertTrue("ILP never added extra_ilp", actual.contains("/extra_ilp.d "));
            Assert.assertTrue("QWP never added extra_q1", actual.contains("/extra_q1.d "));
            Assert.assertTrue("QWP never added extra_q2", actual.contains("/extra_q2.d "));

            final Path digestFile = Paths.get(DIGEST_FILE);
            if (System.getenv(RECORD_ENV) != null) {
                Files.createDirectories(digestFile.getParent());
                Files.writeString(digestFile, actual);
                return;
            }
            Assert.assertEquals(Files.readString(digestFile), actual);
        });
    }

    private static byte[] bytes(Rnd rnd) {
        final byte[] bytes = new byte[rnd.nextInt(17)];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = rnd.nextByte();
        }
        return bytes;
    }

    private static void createTables(TestServerMain serverMain) {
        serverMain.execute("""
                CREATE TABLE sql_t (
                    sym SYMBOL, i INT, l LONG, d DOUBLE, st STRING, vc VARCHAR, bo BOOLEAN,
                    ts2 TIMESTAMP, u UUID, arr DOUBLE[], ts TIMESTAMP
                ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
        serverMain.execute("""
                CREATE TABLE ilp_t (
                    sym SYMBOL, l LONG, d DOUBLE, st STRING, vc VARCHAR, bo BOOLEAN, ts2 TIMESTAMP, ts TIMESTAMP
                ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
        serverMain.execute("""
                CREATE TABLE qwp_t (
                    sym SYMBOL, sym_from_str SYMBOL, b BYTE, sh SHORT, i INT, i_from_long INT, l LONG,
                    f FLOAT, d DOUBLE, d_from_float DOUBLE, c CHAR, bo BOOLEAN, st STRING, vc VARCHAR,
                    ts2 TIMESTAMP, tsn TIMESTAMP_NS, u UUID, l256 LONG256, g GEOHASH(4c),
                    dec64 DECIMAL(18, 4), dec128 DECIMAL(30, 6), arr DOUBLE[], bin BINARY, ip IPV4,
                    ts TIMESTAMP
                ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
    }

    private static String digestSegments(String dbRoot, String[] tableNames, String[] dirNames) throws Exception {
        final MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        final HexFormat hex = HexFormat.of();
        final StringBuilder digest = new StringBuilder();
        for (int t = 0; t < tableNames.length; t++) {
            final Path tableDir = Paths.get(dbRoot, dirNames[t]);
            final List<String> segmentFiles = new ArrayList<>();
            try (Stream<Path> files = Files.walk(tableDir)) {
                files.filter(Files::isRegularFile)
                        .map(p -> tableDir.relativize(p).toString().replace('\\', '/'))
                        .filter(rel -> SEGMENT_FILE.matcher(rel).matches() && !rel.endsWith(".lock"))
                        .forEach(segmentFiles::add);
            }
            segmentFiles.sort(null);
            for (String rel : segmentFiles) {
                final byte[] content = Files.readAllBytes(tableDir.resolve(rel));
                digest.append(tableNames[t]).append('/').append(rel)
                        .append(' ').append(content.length)
                        .append(' ').append(hex.formatHex(sha256.digest(content)))
                        .append('\n');
            }
        }
        return digest.toString();
    }

    private static boolean isNull(Rnd rnd) {
        return rnd.nextInt(7) == 0;
    }

    private static String symbol(Rnd rnd) {
        return "s" + rnd.nextInt(50);
    }

    private static String text(Rnd rnd) {
        final int len = rnd.nextInt(12);
        final StringBuilder sb = new StringBuilder(len + 1);
        for (int i = 0; i < len; i++) {
            sb.append((char) ('a' + rnd.nextInt(26)));
        }
        if (rnd.nextInt(8) == 0) {
            sb.append('ü');
        }
        return sb.toString();
    }

    private static long timestamp(Rnd rnd) {
        return TS_BASE + rnd.nextLong(TS_RANGE);
    }

    private static void writeIlp(int port, Rnd rnd) {
        try (Sender sender = Sender.fromConfig("http::addr=localhost:" + port + ";auto_flush=off;")) {
            for (int batch = 0; batch < BATCH_COUNT; batch++) {
                final int rows = 1 + rnd.nextInt(MAX_BATCH_ROWS);
                for (int r = 0; r < rows; r++) {
                    sender.table("ilp_t");
                    if (!isNull(rnd)) {
                        sender.symbol("sym", symbol(rnd));
                    }
                    // an ILP row needs at least one field: l is never null
                    sender.longColumn("l", rnd.nextLong());
                    if (!isNull(rnd)) {
                        sender.doubleColumn("d", rnd.nextInt(1_000_000) / 100.0);
                    }
                    if (!isNull(rnd)) {
                        sender.stringColumn("st", text(rnd));
                    }
                    if (!isNull(rnd)) {
                        sender.stringColumn("vc", text(rnd));
                    }
                    if (!isNull(rnd)) {
                        sender.boolColumn("bo", rnd.nextBoolean());
                    }
                    if (!isNull(rnd)) {
                        sender.timestampColumn("ts2", timestamp(rnd), ChronoUnit.MICROS);
                    }
                    if (batch >= 15 && !isNull(rnd)) {
                        sender.longColumn("extra_ilp", rnd.nextLong());
                    }
                    sender.at(timestamp(rnd), ChronoUnit.MICROS);
                }
                sender.flush();
            }
        }
    }

    private static void writeQwp(int port, Rnd rnd) {
        final List<String> errors = new CopyOnWriteArrayList<>();
        try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.builder(Sender.Transport.WEBSOCKET)
                .address("localhost:" + port)
                .autoFlushRows(1_000_000)
                .autoFlushIntervalMillis(3_600_000)
                .errorHandler(error -> errors.add(String.valueOf(error)))
                .build()) {
            for (int batch = 0; batch < BATCH_COUNT; batch++) {
                final int rows = 1 + rnd.nextInt(MAX_BATCH_ROWS);
                for (int r = 0; r < rows; r++) {
                    sender.table("qwp_t");
                    if (!isNull(rnd)) {
                        sender.symbol("sym", symbol(rnd));
                    }
                    if (!isNull(rnd)) {
                        sender.stringColumn("sym_from_str", symbol(rnd));
                    }
                    if (!isNull(rnd)) {
                        sender.byteColumn("b", (byte) rnd.nextInt(100));
                    }
                    if (!isNull(rnd)) {
                        sender.shortColumn("sh", (short) rnd.nextInt(30_000));
                    }
                    if (!isNull(rnd)) {
                        sender.intColumn("i", rnd.nextInt());
                    }
                    if (!isNull(rnd)) {
                        sender.longColumn("i_from_long", rnd.nextInt(1_000_000));
                    }
                    sender.longColumn("l", rnd.nextLong());
                    if (!isNull(rnd)) {
                        sender.floatColumn("f", rnd.nextInt(100_000) / 10f);
                    }
                    if (!isNull(rnd)) {
                        sender.doubleColumn("d", rnd.nextInt(1_000_000) / 100.0);
                    }
                    if (!isNull(rnd)) {
                        sender.floatColumn("d_from_float", rnd.nextInt(100_000) / 10f);
                    }
                    if (!isNull(rnd)) {
                        sender.charColumn("c", (char) ('a' + rnd.nextInt(26)));
                    }
                    if (!isNull(rnd)) {
                        sender.boolColumn("bo", rnd.nextBoolean());
                    }
                    if (!isNull(rnd)) {
                        sender.stringColumn("st", text(rnd));
                    }
                    if (!isNull(rnd)) {
                        sender.stringColumn("vc", text(rnd));
                    }
                    if (!isNull(rnd)) {
                        sender.timestampColumn("ts2", timestamp(rnd), ChronoUnit.MICROS);
                    }
                    if (!isNull(rnd)) {
                        sender.timestampColumn("tsn", timestamp(rnd), ChronoUnit.MICROS);
                    }
                    if (!isNull(rnd)) {
                        sender.uuidColumn("u", rnd.nextLong(), rnd.nextLong());
                    }
                    if (!isNull(rnd)) {
                        sender.long256Column("l256", rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
                    }
                    if (!isNull(rnd)) {
                        sender.geoHashColumn("g", rnd.nextLong() & 0xF_FFFFL, 20);
                    }
                    if (!isNull(rnd)) {
                        sender.decimalColumn("dec64", Decimal64.fromLong(rnd.nextLong() % 1_000_000_000_000L, 4));
                    }
                    if (!isNull(rnd)) {
                        sender.decimalColumn("dec128", Decimal128.fromLong(rnd.nextLong() % 1_000_000_000_000L, 6));
                    }
                    if (!isNull(rnd)) {
                        sender.doubleArray("arr", new double[]{rnd.nextInt(1000) / 10.0, rnd.nextInt(1000) / 10.0});
                    }
                    if (!isNull(rnd)) {
                        sender.binaryColumn("bin", bytes(rnd));
                    }
                    if (!isNull(rnd)) {
                        sender.intColumn("ip", 1 + rnd.nextInt(Integer.MAX_VALUE - 1));
                    }
                    if (batch >= 10 && !isNull(rnd)) {
                        sender.longColumn("extra_q1", rnd.nextLong());
                    }
                    if (batch >= 25 && !isNull(rnd)) {
                        sender.longColumn("extra_q2", rnd.nextLong());
                    }
                    sender.at(timestamp(rnd), ChronoUnit.MICROS);
                }
                Assert.assertTrue("QWP batch " + batch + " not acknowledged, errors: " + errors, sender.drain(60_000));
                Assert.assertTrue("QWP batch " + batch + " rejected: " + errors, errors.isEmpty());
            }
        }
    }

    private static void writeSql(TestServerMain serverMain, Rnd rnd) {
        final StringBuilder sql = new StringBuilder();
        for (int batch = 0; batch < BATCH_COUNT; batch++) {
            sql.setLength(0);
            sql.append("INSERT INTO sql_t (sym, i, l, d, st, vc, bo, ts2, u, arr, ts) VALUES ");
            final int rows = 1 + rnd.nextInt(MAX_BATCH_ROWS);
            for (int r = 0; r < rows; r++) {
                if (r > 0) {
                    sql.append(", ");
                }
                sql.append('(');
                sql.append(isNull(rnd) ? "NULL" : "'" + symbol(rnd) + "'").append(", ");
                sql.append(isNull(rnd) ? "NULL" : Integer.toString(rnd.nextInt(1_000_000))).append(", ");
                sql.append(isNull(rnd) ? "NULL" : Long.toString(rnd.nextLong())).append(", ");
                sql.append(isNull(rnd) ? "NULL" : Double.toString(rnd.nextInt(1_000_000) / 100.0)).append(", ");
                sql.append(isNull(rnd) ? "NULL" : "'" + text(rnd) + "'").append(", ");
                sql.append(isNull(rnd) ? "NULL" : "'" + text(rnd) + "'").append(", ");
                sql.append(rnd.nextBoolean()).append(", ");
                sql.append(isNull(rnd) ? "NULL" : timestamp(rnd) + "::TIMESTAMP").append(", ");
                sql.append(isNull(rnd) ? "NULL" : "'" + new UUID(rnd.nextLong(), rnd.nextLong()) + "'").append(", ");
                sql.append(isNull(rnd) ? "NULL" : "ARRAY[" + rnd.nextInt(1000) / 10.0 + ", " + rnd.nextInt(1000) / 10.0 + "]").append(", ");
                sql.append(timestamp(rnd)).append("::TIMESTAMP");
                sql.append(')');
            }
            serverMain.execute(sql.toString());
        }
    }
}
