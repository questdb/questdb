/*******************************************************************************
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

package io.questdb.test.cairo.types;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.PhysicalDescriptor;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.line.LineUdpSender;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.network.NetworkError;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Long256;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The ingestion part of the conformance kit (User Story 4): every kit type through ILP over
 * TCP, UDP and HTTP, QWP ingest and egress, CSV import ({@code /imp}) and Parquet import
 * ({@code read_parquet}), into WAL and non-WAL tables.
 * <p>
 * Every client is the real {@code java-questdb-client} at the commit the branch pins
 * ({@link #CLIENT_COMMIT}); nothing here forges a frame. Each value row goes out in the
 * protocol's natural form for the column type (the form is part of the recording), with the
 * NULL row as an omitted column; a type a protocol has no form for sends its text form, and
 * the recording holds what the server does with it. A section records what was sent, what the
 * server or the client answered, and what the table stores.
 * <p>
 * ILP over TCP and UDP answers nothing: the test sends a last row, the fence, on the same
 * connection and polls, with a bound, until the fence is visible, so every row before it has
 * been processed. ILP over TCP runs with {@code line.tcp.disconnect.on.error=false}, so a
 * rejected line does not drop the rows after it. ILP over HTTP and QWP answer per row: each
 * row goes in its own request or sender, so a rejected row does not hide the others.
 * <p>
 * CSV import uses {@code /imp}: it needs no import root, answers synchronously, and takes the
 * CSV that {@code /exp} writes for the same rows, the round trip users run. Parquet import
 * reads the file of a partition converted with {@code CONVERT PARTITION TO PARQUET}.
 * <p>
 * A type registered later runs where its resource line enables a path, with no recording
 * (F123). It sends the form of its definition's accessor family: a type in INT's family sends
 * INT's form, one in VARCHAR's family VARCHAR's, and a family the kit has no form for fails,
 * naming it. Its NULL row is the omitted column, as for every type. {@link #checkLater} judges
 * what the table stores, or what QWP egress sends, by invariants 1 and 2
 * ({@link TypeConformanceInvariants}). The NULL row's error, which NOT_NULL requires, is the
 * protocol's answer where the protocol answers per row (ILP over HTTP, QWP). ILP over TCP and
 * UDP answer nothing, so there NOT_NULL requires that the row is not stored. The other paths
 * take their values from a table the kit writes with SQL, so the error is that write's. The
 * ILP fence rows of a NOT_NULL later type carry a value, so the type does not refuse them. ILP
 * over HTTP and QWP refuse a non-WAL table for every type, so there a later type's rows must
 * all be refused and none stored.
 * <p>
 * ILP over HTTP and QWP refuse non-WAL tables by design, so their non-WAL runs have sections of
 * their own ({@code ilp-http-nonwal}, {@code qwp-nonwal}).
 * <p>
 * Masks, applied before the comparison: the test root directory becomes {@code <root>}; the
 * mode's own table names ({@code dst_n1}, {@code dst_w1}) become {@code dst}; the error id of
 * an ILP over HTTP response, whose prefix is random per server, becomes {@code id: <id>}.
 */
@RunWith(Parameterized.class)
public class TypeConformanceIngestTest extends AbstractBootstrapTest {
    private static final String BOUNDARY = "------------------------kitboundary7f3a";
    // the java-questdb-client commit the recording was made with
    private static final String CLIENT_COMMIT = "0b9b5766c2";
    private static final String FENCE = "~fence";
    private static final long FENCE_TS = 3_600L * TypeConformanceValues.SECOND;
    private static final long FENCE_WAIT_NANOS = TimeUnit.SECONDS.toNanos(60);
    private static final Pattern HTTP_ERROR_ID = Pattern.compile("id: [0-9a-f]+-[0-9]+");
    private static final int LOCALHOST = Numbers.parseIPv4Quiet("127.0.0.1");
    private static final String[] MODES = {"nonwal-day", "wal-day"};
    private static final Map<String, String> RECORDINGS = new HashMap<>();
    private static final Pattern TABLE_NAME = Pattern.compile("dst_[nw]1");
    private final ObjList<TypeConformanceValues.Row> rows;
    private final TypeConformanceTypes.Entry type;
    // what QWP egress sent for a later type, by value row: the bits (null for a NULL) and the text
    private Map<String, long[]> egressBits;
    private Map<String, String> egressTexts;

    public TypeConformanceIngestTest(String label) {
        this.type = TypeConformanceTypes.byLabel(label);
        this.rows = TypeConformanceValues.rowsOf(type);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        final Collection<Object[]> data = new ArrayList<>();
        for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n; i++) {
            data.add(new Object[]{TypeConformanceTypes.ALL.getQuick(i).label});
        }
        return data;
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(
                PropertyKey.PG_ENABLED + "=false",
                PropertyKey.HTTP_MIN_ENABLED + "=false",
                PropertyKey.LINE_UDP_ENABLED + "=true",
                PropertyKey.LINE_UDP_UNICAST + "=true",
                PropertyKey.LINE_TCP_DISCONNECT_ON_ERROR + "=false",
                PropertyKey.LINE_TCP_COMMIT_INTERVAL_DEFAULT + "=50",
                PropertyKey.CAIRO_SQL_COPY_ROOT + "=" + root
        ));
    }

    @Test
    public void testCsvImport() throws Exception {
        runPath("ingest.csv", (server, mode, section) -> {
            final ObjList<Value> values = sourceValues(server, section);
            if (values == null) {
                return;
            }
            final String table = createTarget(server, mode, section);
            if (table == null) {
                return;
            }
            final Utf8StringSink csv = new Utf8StringSink();
            try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                final String exportStatus = get(client, "/exp", "SELECT k, v, ts FROM src", csv);
                if (!"200".equals(exportStatus)) {
                    section.put("error: /exp status ").put(exportStatus).put(": ").put(csv).put('\n');
                    return;
                }
            } catch (Throwable e) {
                section.put("error: /exp: ").put(oneLine(e.getMessage())).put('\n');
                return;
            }
            try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                final Utf8StringSink response = new Utf8StringSink();
                final HttpClient.Request request = client.newRequest("127.0.0.1", HTTP_PORT)
                        .POST()
                        .url("/imp")
                        .query("name", table)
                        .query("forceHeader", "true")
                        .header("Content-Type", "multipart/form-data; boundary=" + BOUNDARY)
                        .withContent();
                request.putAscii("--").putAscii(BOUNDARY).putAscii("\r\n")
                        .putAscii("Content-Disposition: form-data; name=\"data\"; filename=\"").putAscii(table).putAscii(".csv\"\r\n")
                        .putAscii("Content-Type: application/octet-stream\r\n\r\n")
                        .put(csv)
                        .putAscii("\r\n--").putAscii(BOUNDARY).putAscii("--\r\n");
                final HttpClient.ResponseHeaders headers = request.send();
                headers.await();
                final String status = Utf8s.toString(headers.getStatusCode());
                headers.getResponse().copyTextTo(response);
                section.put("/imp status ").put(status).put('\n').put(response);
                if (response.size() > 0 && response.byteAt(response.size() - 1) != '\n') {
                    section.put('\n');
                }
            } catch (Throwable e) {
                section.put("error: /imp: ").put(oneLine(e.getMessage())).put('\n');
            }
            awaitWal(server, mode, table, section);
            section.put(print(server, "SELECT k, v FROM " + table));
        });
    }

    @Test
    public void testIlpHttp() throws Exception {
        runPath("ingest.ilp-http", (server, mode, section) -> {
            final ObjList<Value> values = sourceValues(server, section);
            if (values == null) {
                return;
            }
            final String table = createTarget(server, mode, section);
            if (table == null) {
                return;
            }
            // every row in its own request, so a rejected row does not hide the others
            for (int i = 0, n = values.size(); i < n; i++) {
                final Value value = values.getQuick(i);
                section.put(value.label).put('\t');
                try (Sender sender = Sender.builder(Sender.Transport.HTTP).address("127.0.0.1").port(HTTP_PORT).build()) {
                    section.put(ilpRow(sender, table, value, i * TypeConformanceValues.SECOND));
                    sender.flush();
                    section.put("\tok\n");
                } catch (Throwable e) {
                    section.put("\terror: ").put(oneLine(e.getMessage())).put('\n');
                }
            }
            awaitWal(server, mode, table, section);
            section.put(print(server, "SELECT k, v FROM " + table));
        });
    }

    @Test
    public void testIlpTcp() throws Exception {
        runPath("ingest.ilp-tcp", (server, mode, section) -> sendIlpFenced(
                server,
                mode,
                section,
                false,
                () -> Sender.builder(Sender.Transport.TCP)
                        .address("127.0.0.1")
                        .port(ILP_PORT)
                        .protocolVersion(Sender.PROTOCOL_VERSION_V3)
                        .build()
        ));
    }

    @Test
    public void testIlpUdp() throws Exception {
        runPath("ingest.ilp-udp", (server, mode, section) -> sendIlpFenced(
                server,
                mode,
                section,
                true,
                // the builder's UDP transport is QWP over UDP; ILP over UDP is LineUdpSender
                () -> new LineUdpSender(LOCALHOST, LOCALHOST, ILP_PORT, 2048, 1)
        ));
    }

    @Test
    public void testParquetImport() throws Exception {
        runPath("ingest.parquet", (server, mode, section) -> {
            final ObjList<Value> values = sourceValues(server, section);
            if (values == null) {
                return;
            }
            // day 0 goes to Parquet; day 1, the active partition, stays native
            final StringSink errors = new StringSink();
            TypeConformanceValues.writeRows(server.getEngine(), server.getSqlExecutionContext(), "src", rows, "", TypeConformanceValues.SECOND * 86_400L, 0, rows.size(), 1, true, errors);
            if (type.isLater()) {
                // a NOT_NULL type refuses the NULL row: invariant 2 judges that, the path goes on
                section.put(errors);
                errors.clear();
            }
            execute(server, "ALTER TABLE src CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'", errors);
            if (errors.length() > 0) {
                section.put(errors);
                return;
            }
            final String file = findParquetFile(server, "src");
            if (file == null) {
                section.put("error: no Parquet file for partition 1970-01-01\n");
                return;
            }
            final String read = "SELECT k, v, ts FROM read_parquet('" + file + "')";
            section.put("read_parquet\n").put(printWithTypes(server, "SELECT k, v FROM read_parquet('" + file + "')"));
            final String table = createTarget(server, mode, section);
            if (table == null) {
                return;
            }
            final StringSink insert = new StringSink();
            execute(server, "INSERT INTO " + table + " (k, v, ts) " + read, insert);
            section.put(insert);
            awaitWal(server, mode, table, section);
            section.put(print(server, "SELECT k, v FROM " + table));
        });
    }

    @Test
    public void testQwp() throws Exception {
        runPath("ingest.qwp", (server, mode, section) -> {
            final ObjList<Value> values = sourceValues(server, section);
            if (values == null) {
                return;
            }
            final String table = createTarget(server, mode, section);
            if (table == null) {
                return;
            }
            // every row in its own sender: a rejected frame ends its sender in a terminal error
            for (int i = 0, n = values.size(); i < n; i++) {
                final Value value = values.getQuick(i);
                final List<SenderError> errors = new CopyOnWriteArrayList<>();
                section.put(value.label).put('\t');
                String outcome;
                QwpWebSocketSender sender = null;
                try {
                    sender = (QwpWebSocketSender) Sender.builder(Sender.Transport.WEBSOCKET)
                            .address("127.0.0.1:" + HTTP_PORT)
                            .errorHandler(errors::add)
                            .maxFrameRejections(1)
                            .poisonMinEscalationWindowMillis(0)
                            .closeFlushTimeoutMillis(30_000L)
                            .build();
                    section.put(qwpRow(sender, table, value, i * TypeConformanceValues.SECOND));
                    final long fsn = sender.flushAndGetSequence();
                    outcome = sender.awaitAckedFsn(fsn, 30_000L) ? "ok" : awaitError(errors);
                } catch (Throwable e) {
                    outcome = "error: " + oneLine(e.getMessage());
                }
                if (sender != null) {
                    try {
                        sender.close();
                    } catch (Throwable e) {
                        if ("ok".equals(outcome)) {
                            outcome = "close error: " + oneLine(e.getMessage());
                        }
                    }
                }
                section.put('\t').put(outcome).put('\n');
            }
            awaitWal(server, mode, table, section);
            section.put(print(server, "SELECT k, v FROM " + table));
        });
    }

    @Test
    public void testQwpEgress() throws Exception {
        runPath("ingest.qwp-egress", (server, mode, section) -> {
            final ObjList<Value> values = sourceValues(server, section);
            if (values == null) {
                return;
            }
            final String table = createTarget(server, mode, section);
            if (table == null) {
                return;
            }
            final StringSink errors = new StringSink();
            TypeConformanceValues.writeRows(server.getEngine(), server.getSqlExecutionContext(), table, rows, "", 0, 0, rows.size(), 1, true, errors);
            section.put(errors);
            awaitWal(server, mode, table, section);
            if (type.isLater()) {
                egressBits = new HashMap<>();
                egressTexts = new HashMap<>();
            }
            try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                client.connect();
                client.execute("SELECT k, v FROM " + table, new QwpColumnBatchHandler() {
                    @Override
                    public void onBatch(QwpColumnBatch batch) {
                        for (int r = 0, n = batch.getRowCount(); r < n; r++) {
                            final String label = batch.getString(0, r);
                            section.put(label).put('\t');
                            final int start = section.length();
                            renderQwp(batch, 1, r, section);
                            if (type.isLater()) {
                                egressBits.put(label, qwpBits(batch, 1, r));
                                egressTexts.put(label, section.subSequence(start, section.length()).toString());
                            }
                            section.put('\n');
                        }
                    }

                    @Override
                    public void onEnd(long totalRows) {
                        section.put("end rows=").put(totalRows).put('\n');
                    }

                    @Override
                    public void onError(byte status, String message) {
                        section.put("error: status=").put(status).put(' ').put(oneLine(message)).put('\n');
                    }
                });
            } catch (Throwable e) {
                section.put("error: ").put(oneLine(e.getMessage())).put('\n');
            }
        });
    }

    private static String awaitError(List<SenderError> errors) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (errors.isEmpty() && System.nanoTime() < deadline) {
            Os.sleep(1);
        }
        if (errors.isEmpty()) {
            return "error: no ack and no error within 30s";
        }
        final SenderError error = errors.get(0);
        return "rejected " + error.getCategory() + " " + error.getAppliedPolicy() + ": " + oneLine(error.getServerMessage());
    }

    private static void awaitWal(TestServerMain server, String mode, String table, StringSink section) {
        if (!mode.startsWith("wal")) {
            return;
        }
        try {
            server.awaitTable(table);
        } catch (Throwable e) {
            section.put("error: ").put(oneLine(e.getMessage())).put('\n');
        }
    }

    private static String code(String mode) {
        return mode.startsWith("wal") ? "w1" : "n1";
    }

    private static void execute(TestServerMain server, String sql, StringSink errors) {
        try {
            server.getEngine().execute(sql, server.getSqlExecutionContext());
        } catch (Throwable e) {
            errors.put("error: ").put(oneLine(e.getMessage())).put('\n');
        }
    }

    private static String findParquetFile(TestServerMain server, String table) throws Exception {
        final CairoEngine engine = server.getEngine();
        final TableToken token = engine.verifyTableName(table);
        final Path dir = Paths.get(engine.getConfiguration().getDbRoot(), token.getDirName());
        try (Stream<Path> files = Files.walk(dir)) {
            return files
                    .filter(p -> "data.parquet".equals(p.getFileName().toString()) && p.getParent().getFileName().toString().startsWith("1970-01-01"))
                    .map(Path::toString)
                    .findFirst()
                    .orElse(null);
        }
    }

    private static String get(HttpClient client, String url, String query, Utf8StringSink sink) {
        final HttpClient.ResponseHeaders headers = client.newRequest("127.0.0.1", HTTP_PORT)
                .GET()
                .url(url)
                .query("query", query)
                .send();
        headers.await();
        sink.clear();
        headers.getResponse().copyTextTo(sink);
        return Utf8s.toString(headers.getStatusCode());
    }

    /**
     * The error the NULL row of a type registered later raised, or null when the path took it.
     * ILP over HTTP and QWP answer each row; ILP over TCP and UDP answer nothing. The other paths
     * read the rows from a table the kit writes with SQL, where every row of a later type is raw
     * bits except the NULL row, so the literal INSERT's error is that row's.
     */
    private static String nullError(String path, StringSink section) {
        return switch (path) {
            case "ingest.ilp-http", "ingest.qwp" -> rowOutcome(section, "null");
            case "ingest.ilp-tcp", "ingest.ilp-udp" -> null;
            default -> sqlInsertError(section);
        };
    }

    private static String oneLine(String text) {
        return text == null ? "null" : text.replace('\n', ' ').replace('\r', ' ');
    }

    private static String print(TestServerMain server, String sql) {
        try {
            return TestUtils.printSqlToString(server.getEngine(), server.getSqlExecutionContext(), sql, new StringSink());
        } catch (Throwable e) {
            return "error: " + oneLine(e.getMessage()) + '\n';
        }
    }

    private static String printWithTypes(TestServerMain server, String sql) {
        final StringSink sink = new StringSink();
        try (SqlCompiler compiler = server.getEngine().getSqlCompiler()) {
            TestUtils.printSqlWithTypes(compiler, server.getSqlExecutionContext(), sql, sink);
        } catch (Throwable e) {
            return "error: " + oneLine(e.getMessage()) + '\n';
        }
        return sink.toString();
    }

    /**
     * A value as QWP egress sent it, as the bits of the wire type's width, the form
     * {@link TypeConformanceValues#readValue} reads from a table; null for a NULL.
     */
    private static long[] qwpBits(QwpColumnBatch batch, int col, int row) {
        if (batch.isNull(col, row)) {
            return null;
        }
        final byte wireType = batch.getColumnWireType(col);
        final long[] bits = new long[4];
        switch (wireType) {
            case QwpConstants.TYPE_BOOLEAN -> bits[0] = batch.getBoolValue(col, row) ? 1 : 0;
            case QwpConstants.TYPE_BYTE -> bits[0] = batch.getByteValue(col, row) & 0xFFL;
            case QwpConstants.TYPE_SHORT -> bits[0] = batch.getShortValue(col, row) & 0xFFFFL;
            case QwpConstants.TYPE_CHAR -> bits[0] = batch.getCharValue(col, row);
            case QwpConstants.TYPE_INT, QwpConstants.TYPE_IPv4 -> bits[0] = batch.getIntValue(col, row) & 0xFFFF_FFFFL;
            case QwpConstants.TYPE_LONG, QwpConstants.TYPE_DATE, QwpConstants.TYPE_TIMESTAMP,
                 QwpConstants.TYPE_TIMESTAMP_NANOS -> bits[0] = batch.getLongValue(col, row);
            case QwpConstants.TYPE_FLOAT ->
                    bits[0] = Float.floatToRawIntBits(batch.getFloatValue(col, row)) & 0xFFFF_FFFFL;
            case QwpConstants.TYPE_DOUBLE -> bits[0] = Double.doubleToRawLongBits(batch.getDoubleValue(col, row));
            case QwpConstants.TYPE_UUID -> {
                bits[0] = batch.getUuidLo(col, row);
                bits[1] = batch.getUuidHi(col, row);
            }
            case QwpConstants.TYPE_LONG256 -> {
                for (int w = 0; w < 4; w++) {
                    bits[w] = batch.getLong256Word(col, row, w);
                }
            }
            case QwpConstants.TYPE_VARCHAR -> {
                return TypeConformanceValues.Row.pack(batch.getString(col, row).getBytes(StandardCharsets.UTF_8));
            }
            default -> throw new AssertionError("the kit reads no bits for QWP wire type " + wireType);
        }
        return bits;
    }

    private static void renderArray(double[] array, StringSink sink) {
        sink.put('[');
        for (int i = 0; i < array.length; i++) {
            if (i > 0) {
                sink.put(',');
            }
            sink.put(array[i]);
        }
        sink.put(']');
    }

    private static void renderHex(byte[] bytes, StringSink sink) {
        for (byte b : bytes) {
            sink.put(Character.forDigit((b >> 4) & 0xF, 16)).put(Character.forDigit(b & 0xF, 16));
        }
    }

    /**
     * Prints one value the way the QWP query client decodes it: the wire type, then the value
     * through the client getter of that wire type.
     */
    private static void renderQwp(QwpColumnBatch batch, int col, int row, StringSink sink) {
        final byte wireType = batch.getColumnWireType(col);
        sink.put("wire=").put(wireType).put(' ');
        if (batch.isNull(col, row)) {
            sink.put("null");
            return;
        }
        switch (wireType) {
            case QwpConstants.TYPE_BOOLEAN -> sink.put(batch.getBoolValue(col, row));
            case QwpConstants.TYPE_BYTE -> sink.put(batch.getByteValue(col, row));
            case QwpConstants.TYPE_SHORT -> sink.put(batch.getShortValue(col, row));
            case QwpConstants.TYPE_CHAR -> sink.put(batch.getCharValue(col, row));
            case QwpConstants.TYPE_INT, QwpConstants.TYPE_IPv4 -> sink.put(batch.getIntValue(col, row));
            case QwpConstants.TYPE_LONG, QwpConstants.TYPE_DATE, QwpConstants.TYPE_TIMESTAMP,
                 QwpConstants.TYPE_TIMESTAMP_NANOS -> sink.put(batch.getLongValue(col, row));
            case QwpConstants.TYPE_FLOAT -> sink.put(batch.getFloatValue(col, row));
            case QwpConstants.TYPE_DOUBLE -> sink.put(batch.getDoubleValue(col, row));
            case QwpConstants.TYPE_VARCHAR -> sink.put(batch.getString(col, row));
            case QwpConstants.TYPE_SYMBOL -> sink.put(batch.getSymbol(col, row));
            case QwpConstants.TYPE_UUID -> sink.put("hi=").put(Long.toHexString(batch.getUuidHi(col, row)))
                    .put(" lo=").put(Long.toHexString(batch.getUuidLo(col, row)));
            case QwpConstants.TYPE_LONG256 -> {
                for (int w = 3; w >= 0; w--) {
                    sink.put(Long.toHexString(batch.getLong256Word(col, row, w))).put(w > 0 ? ":" : "");
                }
            }
            case QwpConstants.TYPE_GEOHASH -> sink.put(Long.toHexString(batch.getGeohashValue(col, row)))
                    .put('/').put(batch.getGeohashPrecisionBits(col));
            case QwpConstants.TYPE_BINARY -> renderHex(batch.getBinary(col, row), sink);
            case QwpConstants.TYPE_DECIMAL64 -> {
                final Decimal64 decimal = new Decimal64();
                batch.getDecimal64(col, row, decimal);
                sink.put(decimal.toString());
            }
            case QwpConstants.TYPE_DECIMAL128 -> {
                final Decimal128 decimal = new Decimal128();
                batch.getDecimal128(col, row, decimal);
                sink.put(decimal.toString());
            }
            case QwpConstants.TYPE_DECIMAL256 -> {
                final Decimal256 decimal = new Decimal256();
                batch.getDecimal256(col, row, decimal);
                sink.put(decimal.toString());
            }
            case QwpConstants.TYPE_DOUBLE_ARRAY -> {
                sink.put("dims=").put(batch.getArrayNDims(col, row)).put(' ');
                renderArray(batch.getDoubleArrayElements(col, row), sink);
            }
            default -> sink.put("no rendering for wire type ").put(wireType);
        }
    }

    /**
     * The answer of a path that sends each row alone ({@code <label>\t<form>\t<outcome>}): null
     * when the row went through, else the outcome.
     */
    private static String rowOutcome(StringSink section, String label) {
        for (String line : section.toString().split("\n")) {
            if (line.startsWith(label + "\t")) {
                final String[] parts = line.split("\t", 3);
                return parts.length < 3 || "ok".equals(parts[2]) ? null : parts[2];
            }
        }
        return null;
    }

    private static String sqlInsertError(StringSink section) {
        for (String line : section.toString().split("\n")) {
            if (line.startsWith("error: insert")) {
                return line;
            }
        }
        return null;
    }

    private void assertSection(String path, String mode, CharSequence actual) {
        String masked = actual.toString().replace(root, "<root>");
        masked = TABLE_NAME.matcher(masked).replaceAll("dst");
        masked = HTTP_ERROR_ID.matcher(masked).replaceAll("id: <id>");
        String section = path.substring("ingest.".length());
        if (!mode.startsWith("wal") && ("ingest.ilp-http".equals(path) || "ingest.qwp".equals(path))) {
            // ILP over HTTP and QWP refuse non-WAL tables by design: their own section
            section += "-nonwal";
        }
        TypeConformanceRecording.assertSection(type, section, mode, RECORDINGS.get(type.label), TypeConformanceRecording.escape(masked));
    }

    /**
     * Invariants 1 and 2 for a type registered later (F123), on what the path stored in the
     * mode's target table or, for QWP egress, on what the server sent. A failure message ends
     * with the path's section, which holds each row's form and answer.
     */
    private void checkLater(TestServerMain server, String path, String mode, StringSink section) {
        try {
            final Map<String, long[]> bits;
            final Map<String, String> texts;
            if (egressBits != null) {
                bits = egressBits;
                texts = egressTexts;
            } else {
                bits = new HashMap<>();
                texts = new HashMap<>();
                readTarget(server, path, mode, bits, texts);
            }
            checkLater(path, mode, section, bits, texts);
        } catch (AssertionError e) {
            throw new AssertionError(e.getMessage() + "\n" + section, e);
        }
    }

    /**
     * Every value row arrives and reads back as written (invariant 1). The NULL row, which leaves
     * the column out, is refused under NOT_NULL and arrives under every other policy; it and the
     * sentinel-pattern row then behave as the policy says (invariant 2). A var-size type has no
     * sentinel-pattern row.
     *
     * @param bits  the value of each row that arrived, by label: the bits, or null for a NULL
     * @param texts how each row that arrived prints, by label
     */
    private void checkLater(String path, String mode, StringSink section, Map<String, long[]> bits, Map<String, String> texts) {
        if (!mode.startsWith("wal") && ("ingest.ilp-http".equals(path) || "ingest.qwp".equals(path))) {
            // refused by design for every type: each row is answered with an error, none stored
            for (int i = 0, n = rows.size(); i < n; i++) {
                final String label = rows.getQuick(i).label;
                if (rowOutcome(section, label) == null) {
                    Assert.fail(TypeConformanceInvariants.context(type, label, path, mode) + ": a non-WAL table must refuse the row");
                }
            }
            Assert.assertTrue(TypeConformanceInvariants.context(type, "-", path, mode) + ": a non-WAL table must store nothing", bits.isEmpty());
            return;
        }
        final String policy = TypeConformanceInvariants.policyOf(type);
        TypeConformanceValues.Row sentinel = null;
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            if (row.isNull()) {
                continue;
            }
            if (!bits.containsKey(row.label)) {
                Assert.fail(TypeConformanceInvariants.context(type, row.label, path, mode) + ": the value row did not arrive");
            }
            final long[] read = bits.get(row.label);
            if (read == null) {
                Assert.fail(TypeConformanceInvariants.context(type, row.label, path, mode) + ": the value arrived as NULL");
            }
            TypeConformanceInvariants.assertReadsBackAsWritten(type, row.label, path, mode, row.bits, read);
            if ("sentinel".equals(row.label)) {
                sentinel = row;
            }
        }
        final String nullText = texts.get("null");
        for (int i = 0, n = rows.size(); i < n; i++) {
            final String label = rows.getQuick(i).label;
            if (label.startsWith("sentinel_")) {
                TypeConformanceInvariants.assertOtherSentinel(type, label, path, mode, nullText, texts.get(label));
            }
        }
        final boolean isNotNull = TypeConformanceInvariants.POLICY_NOT_NULL.equals(policy);
        if (isNotNull && bits.containsKey("null")) {
            Assert.fail(TypeConformanceInvariants.context(type, "null", path, mode)
                    + ": NOT_NULL, the row that leaves the column out must be refused, but it arrived as " + nullText);
        }
        if (!isNotNull && !bits.containsKey("null")) {
            Assert.fail(TypeConformanceInvariants.context(type, "null", path, mode) + ": " + policy
                    + ", the row that leaves the column out did not arrive");
        }
        if (sentinel == null || (isNotNull && ("ingest.ilp-tcp".equals(path) || "ingest.ilp-udp".equals(path)))) {
            // ILP over TCP and UDP answer nothing: under NOT_NULL the refusal is the missing row
            return;
        }
        TypeConformanceInvariants.assertNullPolicy(
                type,
                path,
                mode,
                nullText,
                bits.get("null"),
                nullError(path, section),
                texts.get("sentinel"),
                bits.get("sentinel"),
                sentinel.bits
        );
    }

    /**
     * Creates the mode's target table, {@code dst_n1} (non-WAL) or {@code dst_w1} (WAL); on
     * failure appends the error and returns null.
     */
    private String createTarget(TestServerMain server, String mode, StringSink section) {
        final String table = "dst_" + code(mode);
        final StringSink errors = new StringSink();
        execute(server, "CREATE TABLE " + table + " (k VARCHAR, v " + type.ddl + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + (mode.startsWith("wal") ? "WAL" : "BYPASS WAL"), errors);
        if (errors.length() > 0) {
            section.put(errors);
            return null;
        }
        return table;
    }

    /**
     * The tag whose protocol form the kit sends: the type's own for an existing type, as the
     * recordings hold it; for a type registered later, its definition's accessor family (F123).
     * A family the kit has no form for fails, naming it.
     */
    private int formTag() {
        if (!type.isLater()) {
            return ColumnType.tagOf(type.columnType);
        }
        final PhysicalDescriptor.Accessor family = ColumnType.getTypeDriver(type.columnType).getAccessor();
        return switch (family) {
            case BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, STRING, VARCHAR -> family.opcode();
            default -> throw new AssertionError("type " + type.label + " has accessor family " + family
                    + ": the kit has no protocol form for that family yet");
        };
    }

    /**
     * Puts one value row as an ILP row: the value in the ILP form of the column type, or the
     * column left out for the NULL row. Returns the form.
     */
    private String ilpRow(Sender sender, String table, Value value, long ts) {
        final int tag = formTag();
        sender.table(table);
        String form;
        if (tag == ColumnType.SYMBOL) {
            // ILP symbols come before the other columns
            form = value.isNull ? "omitted" : "symbol " + value.text;
            if (!value.isNull) {
                sender.symbol("v", value.text);
            }
            sender.stringColumn("k", value.label);
        } else {
            sender.stringColumn("k", value.label);
            form = value.isNull ? "omitted" : ilpValue(sender, value, tag);
        }
        sender.at(ts, ChronoUnit.MICROS);
        return form;
    }

    private String ilpValue(Sender sender, Value value, int tag) {
        switch (tag) {
            case ColumnType.BOOLEAN -> {
                sender.boolColumn("v", value.l != 0);
                return "bool " + (value.l != 0);
            }
            case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG -> {
                sender.longColumn("v", value.l);
                return "long " + value.l;
            }
            case ColumnType.DATE -> {
                sender.timestampColumn("v", value.l, ChronoUnit.MILLIS);
                return "timestamp " + value.l + " ms";
            }
            case ColumnType.TIMESTAMP -> {
                final ChronoUnit unit = type.columnType == ColumnType.TIMESTAMP_NANO ? ChronoUnit.NANOS : ChronoUnit.MICROS;
                sender.timestampColumn("v", value.l, unit);
                return "timestamp " + value.l + " " + unit;
            }
            case ColumnType.FLOAT, ColumnType.DOUBLE -> {
                sender.doubleColumn("v", value.d);
                return "double " + value.d;
            }
            case ColumnType.ARRAY -> {
                if (value.array2 != null) {
                    sender.doubleArray("v", value.array2);
                } else {
                    sender.doubleArray("v", value.array1);
                }
                return "array " + value.text;
            }
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64,
                 ColumnType.DECIMAL128, ColumnType.DECIMAL256 -> {
                sender.decimalColumn("v", value.text);
                return "decimal " + value.text;
            }
            default -> {
                // CHAR, STRING, VARCHAR, geohash, UUID, LONG128, IPv4, LONG256 and BINARY: ILP has
                // no field of their own, the value goes as its text form
                sender.stringColumn("v", value.text);
                return "string " + value.text;
            }
        }
    }

    /**
     * The value rows of a type registered later, every row including the NULL row, from their
     * bits as the form {@link #formTag()} picks takes them.
     */
    private ObjList<Value> laterValues() {
        final int tag = formTag();
        final ObjList<Value> values = new ObjList<>();
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            final Value value = new Value(row.label);
            if (!value.isNull) {
                final long bits = row.bits[0];
                switch (tag) {
                    case ColumnType.BOOLEAN -> value.l = bits != 0 ? 1 : 0;
                    case ColumnType.BYTE -> value.l = (byte) bits;
                    case ColumnType.SHORT -> value.l = (short) bits;
                    case ColumnType.INT -> value.l = (int) bits;
                    case ColumnType.LONG -> value.l = bits;
                    case ColumnType.FLOAT -> value.d = Float.intBitsToFloat((int) bits);
                    case ColumnType.DOUBLE -> value.d = Double.longBitsToDouble(bits);
                    // STRING and VARCHAR: the text
                    default -> value.text = new String(row.bytes(), StandardCharsets.UTF_8);
                }
            }
            values.add(value);
        }
        return values;
    }

    /**
     * Starts a fence row in {@code table}; the caller ends it. The fence has only k, as the
     * recordings hold it, except for a NOT_NULL type registered later: its fence also carries a
     * value (the zero row's, else the first value row's), so the type does not refuse it.
     */
    private void putFence(Sender sender, String table, String fence, ObjList<Value> values) {
        sender.table(table).stringColumn("k", fence);
        if (!type.isLater() || !TypeConformanceInvariants.POLICY_NOT_NULL.equals(TypeConformanceInvariants.policyOf(type))) {
            return;
        }
        Value fenceValue = null;
        for (int i = 0, n = values.size(); i < n; i++) {
            final Value value = values.getQuick(i);
            if (!value.isNull && (fenceValue == null || "zero".equals(value.label))) {
                fenceValue = value;
            }
        }
        if (fenceValue != null) {
            ilpValue(sender, fenceValue, formTag());
        }
    }

    /**
     * Puts one value row through the QWP sender, in the wire type of the column type, or with
     * the column left out for the NULL row. Returns the form.
     */
    private String qwpRow(QwpWebSocketSender sender, String table, Value value, long ts) {
        final int tag = formTag();
        sender.table(table);
        sender.stringColumn("k", value.label);
        String form = "omitted";
        if (!value.isNull) {
            form = switch (tag) {
                case ColumnType.BOOLEAN -> {
                    sender.boolColumn("v", value.l != 0);
                    yield "bool " + (value.l != 0);
                }
                case ColumnType.BYTE -> {
                    sender.byteColumn("v", (byte) value.l);
                    yield "byte " + value.l;
                }
                case ColumnType.SHORT -> {
                    sender.shortColumn("v", (short) value.l);
                    yield "short " + value.l;
                }
                case ColumnType.CHAR -> {
                    sender.charColumn("v", (char) value.l);
                    yield "char " + value.l;
                }
                case ColumnType.INT -> {
                    sender.intColumn("v", (int) value.l);
                    yield "int " + value.l;
                }
                case ColumnType.LONG -> {
                    sender.longColumn("v", value.l);
                    yield "long " + value.l;
                }
                case ColumnType.DATE -> {
                    // the sender has no DATE column: a timestamp in milliseconds
                    sender.timestampColumn("v", value.l, ChronoUnit.MILLIS);
                    yield "timestamp " + value.l + " ms";
                }
                case ColumnType.TIMESTAMP -> {
                    final ChronoUnit unit = type.columnType == ColumnType.TIMESTAMP_NANO ? ChronoUnit.NANOS : ChronoUnit.MICROS;
                    sender.timestampColumn("v", value.l, unit);
                    yield "timestamp " + value.l + " " + unit;
                }
                case ColumnType.FLOAT -> {
                    sender.floatColumn("v", (float) value.d);
                    yield "float " + (float) value.d;
                }
                case ColumnType.DOUBLE -> {
                    sender.doubleColumn("v", value.d);
                    yield "double " + value.d;
                }
                case ColumnType.SYMBOL -> {
                    sender.symbol("v", value.text);
                    yield "symbol " + value.text;
                }
                case ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG -> {
                    final int bits = ColumnType.getGeoHashBits(type.columnType);
                    sender.geoHashColumn("v", value.l, bits);
                    yield "geohash " + Long.toHexString(value.l) + "/" + bits;
                }
                case ColumnType.UUID, ColumnType.LONG128 -> {
                    // LONG128 has no wire type of its own: the closest is UUID
                    sender.uuidColumn("v", value.words[0], value.words[1]);
                    yield "uuid lo=" + Long.toHexString(value.words[0]) + " hi=" + Long.toHexString(value.words[1]);
                }
                case ColumnType.LONG256 -> {
                    sender.long256Column("v", value.words[0], value.words[1], value.words[2], value.words[3]);
                    yield "long256 " + value.text;
                }
                case ColumnType.IPv4 -> {
                    sender.ipv4Column("v", (int) value.l);
                    yield "ipv4 " + (int) value.l;
                }
                case ColumnType.BINARY -> {
                    sender.binaryColumn("v", value.bin);
                    final StringSink hex = new StringSink();
                    renderHex(value.bin, hex);
                    yield "binary " + hex;
                }
                case ColumnType.ARRAY -> {
                    if (value.array2 != null) {
                        sender.doubleArray("v", value.array2);
                    } else {
                        sender.doubleArray("v", value.array1);
                    }
                    yield "array " + value.text;
                }
                case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64,
                     ColumnType.DECIMAL128, ColumnType.DECIMAL256 -> {
                    sender.decimalColumn("v", value.text);
                    yield "decimal " + value.text;
                }
                default -> {
                    // STRING and VARCHAR: the VARCHAR wire type
                    sender.stringColumn("v", value.text);
                    yield "string " + value.text;
                }
            };
        }
        sender.at(ts, ChronoUnit.MICROS);
        return form;
    }

    /**
     * Reads what the mode's target table stores for a type registered later, fences left out:
     * each row's value as {@link TypeConformanceValues#readValue} reads it, and its printed text.
     */
    private void readTarget(TestServerMain server, String path, String mode, Map<String, long[]> bits, Map<String, String> texts) {
        final String sql = "SELECT k, v FROM dst_" + code(mode) + " WHERE NOT k LIKE '" + FENCE + "%'";
        final SqlExecutionContext context = server.getSqlExecutionContext();
        try (
                SqlCompiler compiler = server.getEngine().getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(context)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                bits.put(record.getVarcharA(0).toString(), TypeConformanceValues.readValue(record, 1, type));
            }
        } catch (Throwable e) {
            Assert.fail(TypeConformanceInvariants.context(type, "-", path, mode) + ": cannot read the target table: " + e.getMessage());
        }
        final String[] lines = print(server, sql).split("\n");
        for (int i = 1; i < lines.length; i++) {
            final int tab = lines[i].indexOf('\t');
            texts.put(lines[i].substring(0, tab), lines[i].substring(tab + 1));
        }
    }

    private void runPath(String path, PathBody body) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startServer()) {
                for (String mode : MODES) {
                    if (!TypeConformanceInvariants.isEnabled(type, path, mode)) {
                        continue;
                    }
                    final StringSink section = new StringSink();
                    egressBits = null;
                    egressTexts = null;
                    body.run(server, mode, section);
                    if (type.isLater()) {
                        checkLater(server, path, mode, section);
                    } else {
                        assertSection(path, mode, section);
                    }
                    // the next mode starts from an empty database
                    for (String table : new String[]{"src", "dst_" + code(mode)}) {
                        final StringSink ignored = new StringSink();
                        execute(server, "DROP TABLE IF EXISTS " + table, ignored);
                    }
                }
            }
        });
    }

    /**
     * Waits, with a bound, until the fence row {@code fence} is visible in {@code table}; false
     * when it did not arrive or the table was suspended, with the reason in {@code section}.
     */
    private boolean awaitFence(TestServerMain server, String table, String fence, StringSink section) {
        final long deadline = System.nanoTime() + FENCE_WAIT_NANOS;
        final String fenceSql = "SELECT count() FROM " + table + " WHERE k = '" + fence + "'";
        while (!print(server, fenceSql).equals("count\n1\n")) {
            final TableToken token = server.getEngine().verifyTableName(table);
            if (token.isWal() && server.getEngine().getTableSequencerAPI().isSuspended(token)) {
                section.put("error: table is suspended\n");
                return false;
            }
            if (System.nanoTime() > deadline) {
                section.put("error: the fence row ").put(fence).put(" did not arrive\n");
                return false;
            }
            Os.sleep(10);
        }
        return true;
    }

    /**
     * Sends every value row over one ILP connection, one row per flush, and waits until a fence
     * row sent after it is visible, so every row before the fence has been processed. TCP sends
     * one fence after the last row. UDP, where the receiver may drop a burst of datagrams, sends
     * each row with its own fence in one datagram and waits for it before the next row; the
     * stored rows print without the fences.
     */
    private void sendIlpFenced(TestServerMain server, String mode, StringSink section, boolean isFencePerRow, SenderFactory factory) throws Exception {
        final ObjList<Value> values = sourceValues(server, section);
        if (values == null) {
            return;
        }
        final String table = createTarget(server, mode, section);
        if (table == null) {
            return;
        }
        try (Sender sender = factory.newSender()) {
            for (int i = 0, n = values.size(); i < n; i++) {
                final Value value = values.getQuick(i);
                final long ts = i * TypeConformanceValues.SECOND;
                section.put(value.label).put('\t');
                try {
                    section.put(ilpRow(sender, table, value, ts)).put('\n');
                } catch (Throwable e) {
                    // TCP and UDP can neither cancel a row nor reset the buffer: the row goes out
                    // without v, as the NULL row does
                    section.put("client error: ").put(oneLine(e.getMessage())).put("; row sent without v\n");
                    sender.at(ts, ChronoUnit.MICROS);
                }
                if (isFencePerRow) {
                    final String fence = FENCE + i;
                    putFence(sender, table, fence, values);
                    sender.at(FENCE_TS + i, ChronoUnit.MICROS);
                    sender.flush();
                    if (!awaitFence(server, table, fence, section)) {
                        break;
                    }
                } else {
                    // one row per flush, so a row the server refuses is refused alone
                    sender.flush();
                }
            }
            if (!isFencePerRow) {
                putFence(sender, table, FENCE, values);
                sender.at(FENCE_TS, ChronoUnit.MICROS);
                sender.flush();
            }
        }
        if (!isFencePerRow) {
            awaitFence(server, table, FENCE, section);
            section.put(print(server, "SELECT k, v FROM " + table));
        } else {
            section.put(print(server, "SELECT k, v FROM " + table + " WHERE NOT k LIKE '" + FENCE + "%'"));
        }
    }

    /**
     * Writes the value rows into the non-WAL table {@code src} with SQL and reads each back:
     * the typed value by the column type and its text form. Returns null when the type cannot be
     * stored, with the error in {@code section}.
     */
    private ObjList<Value> sourceValues(TestServerMain server, StringSink section) throws Exception {
        final StringSink errors = new StringSink();
        execute(server, "CREATE TABLE src (k VARCHAR, v " + type.ddl + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", errors);
        if (errors.length() > 0) {
            section.put(errors);
            return null;
        }
        TypeConformanceValues.writeRows(server.getEngine(), server.getSqlExecutionContext(), "src", rows, "", 0, 0, rows.size(), 1, true, errors);
        if (errors.length() > 0) {
            section.put(errors);
        }
        if (type.isLater()) {
            // every row, the NULL row too, whether or not src took it (a NOT_NULL type refuses it)
            return laterValues();
        }
        final ObjList<Value> values = new ObjList<>();
        final SqlExecutionContext context = server.getSqlExecutionContext();
        final int tag = ColumnType.tagOf(type.columnType);
        try (
                SqlCompiler compiler = server.getEngine().getSqlCompiler();
                RecordCursorFactory factory = compiler.compile("SELECT k, v FROM src", context).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(context)
        ) {
            final RecordMetadata metadata = factory.getMetadata();
            final Record record = cursor.getRecord();
            final StringSink text = new StringSink();
            while (cursor.hasNext()) {
                final Value value = new Value(record.getVarcharA(0).toString());
                text.clear();
                CursorPrinter.printColumn(record, metadata, 1, text);
                value.text = text.toString();
                switch (tag) {
                    case ColumnType.BOOLEAN -> value.l = record.getBool(1) ? 1 : 0;
                    case ColumnType.BYTE -> value.l = record.getByte(1);
                    case ColumnType.SHORT -> value.l = record.getShort(1);
                    case ColumnType.CHAR -> value.l = record.getChar(1);
                    case ColumnType.INT -> value.l = record.getInt(1);
                    case ColumnType.IPv4 -> value.l = record.getIPv4(1);
                    case ColumnType.LONG -> value.l = record.getLong(1);
                    case ColumnType.DATE -> value.l = record.getDate(1);
                    case ColumnType.TIMESTAMP -> value.l = record.getTimestamp(1);
                    case ColumnType.GEOBYTE -> value.l = record.getGeoByte(1);
                    case ColumnType.GEOSHORT -> value.l = record.getGeoShort(1);
                    case ColumnType.GEOINT -> value.l = record.getGeoInt(1);
                    case ColumnType.GEOLONG -> value.l = record.getGeoLong(1);
                    case ColumnType.FLOAT -> value.d = record.getFloat(1);
                    case ColumnType.DOUBLE -> value.d = record.getDouble(1);
                    case ColumnType.UUID, ColumnType.LONG128 ->
                            value.words = new long[]{record.getLong128Lo(1), record.getLong128Hi(1)};
                    case ColumnType.LONG256 -> {
                        final Long256 long256 = record.getLong256A(1);
                        value.words = new long[]{long256.getLong0(), long256.getLong1(), long256.getLong2(), long256.getLong3()};
                    }
                    case ColumnType.BINARY -> {
                        final BinarySequence bin = record.getBin(1);
                        if (bin != null) {
                            value.bin = new byte[(int) bin.length()];
                            for (int i = 0; i < value.bin.length; i++) {
                                value.bin[i] = bin.byteAt(i);
                            }
                        }
                    }
                    case ColumnType.ARRAY -> {
                        final ArrayView array = record.getArray(1, metadata.getColumnType(1));
                        if (!array.isNull()) {
                            if (array.getDimCount() == 1) {
                                value.array1 = new double[array.getDimLen(0)];
                                for (int i = 0; i < value.array1.length; i++) {
                                    value.array1[i] = array.getDouble(i);
                                }
                            } else {
                                value.array2 = new double[array.getDimLen(0)][array.getDimLen(1)];
                                for (int i = 0, flat = 0; i < value.array2.length; i++) {
                                    for (int j = 0; j < value.array2[i].length; j++) {
                                        value.array2[i][j] = array.getDouble(flat++);
                                    }
                                }
                            }
                        }
                    }
                    default -> {
                        // STRING, VARCHAR, SYMBOL and the decimals go as their text form
                    }
                }
                values.add(value);
            }
        }
        return values;
    }

    private TestServerMain startServer() {
        // fixed ports close with a delay; a bind failure right after another server closed retries
        NetworkError lastError = null;
        for (int attempt = 0; attempt < 100; attempt++) {
            try {
                return startWithEnvVariables();
            } catch (NetworkError e) {
                if (e.getMessage() == null || !Chars.contains(e.getMessage(), "could not bind socket")) {
                    throw e;
                }
                lastError = e;
                Os.sleep(100);
            }
        }
        throw lastError;
    }

    @FunctionalInterface
    private interface PathBody {
        void run(TestServerMain server, String mode, StringSink section) throws Exception;
    }

    @FunctionalInterface
    private interface SenderFactory {
        Sender newSender();
    }

    private static final class Value {
        final boolean isNull;
        final String label;
        double[] array1;
        double[][] array2;
        byte[] bin;
        double d;
        long l;
        String text;
        long[] words;

        Value(String label) {
            this.label = label;
            this.isNull = "null".equals(label);
        }
    }

    // recorded with java-questdb-client 0b9b5766c2 (CLIENT_COMMIT); masks: see the class comment
    // recordings: start
    static {
        rec("BOOLEAN", """
                ## ilp-tcp
                min\tbool false
                max\tbool true
                null\tomitted
                k\tv
                min\tfalse
                max\ttrue
                null\tfalse
                ~fence\tfalse
                ## ilp-http-nonwal
                min\tbool false\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tbool true\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tbool false\tok
                max\tbool true\tok
                null\tomitted\tok
                k\tv
                min\tfalse
                max\ttrue
                null\tfalse
                ## qwp-nonwal
                min\tbool false\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tbool true\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tbool false\tok
                max\tbool true\tok
                null\tomitted\tok
                k\tv
                min\tfalse
                max\ttrue
                null\tfalse
                ## ilp-udp
                min\tbool false
                max\tbool true
                null\tomitted
                k\tv
                min\tfalse
                max\ttrue
                null\tfalse
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                  BOOLEAN  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\tfalse
                max\ttrue
                null\tfalse
                ## qwp-egress
                min\twire=1 false
                max\twire=1 true
                null\twire=1 false
                end rows=3
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\tfalse:BOOLEAN
                max:VARCHAR\ttrue:BOOLEAN
                null:VARCHAR\tfalse:BOOLEAN
                k\tv
                min\tfalse
                max\ttrue
                null\tfalse
                """);
        rec("BYTE", """
                ## ilp-udp
                min\tlong -128
                max\tlong 127
                other_null\tlong -1
                null\tomitted
                k\tv
                min\t-128
                max\t127
                other_null\t-1
                null\t0
                ## qwp-nonwal
                min\tbyte -128\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tbyte 127\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                other_null\tbyte -1\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tbyte -128\tok
                max\tbyte 127\tok
                other_null\tbyte -1\tok
                null\tomitted\tok
                k\tv
                min\t-128
                max\t127
                other_null\t-1
                null\t0
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-128:BYTE
                max:VARCHAR\t127:BYTE
                other_null:VARCHAR\t-1:BYTE
                null:VARCHAR\t0:BYTE
                k\tv
                min\t-128
                max\t127
                other_null\t-1
                null\t0
                ## qwp-egress
                min\twire=2 -128
                max\twire=2 127
                other_null\twire=2 -1
                null\twire=2 0
                end rows=4
                ## ilp-http-nonwal
                min\tlong -128\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tlong 127\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                other_null\tlong -1\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tlong -128\tok
                max\tlong 127\tok
                other_null\tlong -1\tok
                null\tomitted\tok
                k\tv
                min\t-128
                max\t127
                other_null\t-1
                null\t0
                ## ilp-tcp
                min\tlong -128
                max\tlong 127
                other_null\tlong -1
                null\tomitted
                k\tv
                min\t-128
                max\t127
                other_null\t-1
                null\t0
                ~fence\t0
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 4  |                 |         |              |\\u000d
                |  Rows imported  |                                                 4  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                     BYTE  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t-128
                max\t127
                other_null\t-1
                null\t0
                """);
        rec("SHORT", """
                ## ilp-tcp
                min\tlong -32768
                max\tlong 32767
                other_null\tlong -1
                null\tomitted
                k\tv
                min\t-32768
                max\t32767
                other_null\t-1
                null\t0
                ~fence\t0
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-32768:SHORT
                max:VARCHAR\t32767:SHORT
                other_null:VARCHAR\t-1:SHORT
                null:VARCHAR\t0:SHORT
                k\tv
                min\t-32768
                max\t32767
                other_null\t-1
                null\t0
                ## qwp-egress
                min\twire=3 -32768
                max\twire=3 32767
                other_null\twire=3 -1
                null\twire=3 0
                end rows=4
                ## ilp-http-nonwal
                min\tlong -32768\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tlong 32767\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                other_null\tlong -1\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tlong -32768\tok
                max\tlong 32767\tok
                other_null\tlong -1\tok
                null\tomitted\tok
                k\tv
                min\t-32768
                max\t32767
                other_null\t-1
                null\t0
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 4  |                 |         |              |\\u000d
                |  Rows imported  |                                                 4  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                    SHORT  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t-32768
                max\t32767
                other_null\t-1
                null\t0
                ## qwp-nonwal
                min\tshort -32768\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tshort 32767\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                other_null\tshort -1\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tshort -32768\tok
                max\tshort 32767\tok
                other_null\tshort -1\tok
                null\tomitted\tok
                k\tv
                min\t-32768
                max\t32767
                other_null\t-1
                null\t0
                ## ilp-udp
                min\tlong -32768
                max\tlong 32767
                other_null\tlong -1
                null\tomitted
                k\tv
                min\t-32768
                max\t32767
                other_null\t-1
                null\t0
                """);
        rec("CHAR", """
                ## ilp-tcp
                min\tstring\s
                max\tstring \\uffff
                other_null\tstring \\uffff
                null\tomitted
                k\tv
                null\t
                ~fence\t
                ## ilp-http-nonwal
                min\tstring \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring \\uffff\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                other_null\tstring \\uffff\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tstring \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst, column: v; cast error from protocol type: STRING to column type: CHAR [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring \\uffff\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst, column: v; cast error from protocol type: STRING to column type: CHAR [http-status=400, id: <id>, code: invalid, line: 1]
                other_null\tstring \\uffff\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst, column: v; cast error from protocol type: STRING to column type: CHAR [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\tok
                k\tv
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 4  |                 |         |              |\\u000d
                |  Rows imported  |                                                 4  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                     CHAR  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t
                max\t\\uffff
                other_null\t\\uffff
                null\t
                ## qwp-nonwal
                min\tchar 0\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tchar 65535\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                other_null\tchar 65535\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tchar 0\tok
                max\tchar 65535\tok
                other_null\tchar 65535\tok
                null\tomitted\tok
                k\tv
                min\t
                max\t\\uffff
                other_null\t\\uffff
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t:CHAR
                max:VARCHAR\t\\uffff:CHAR
                other_null:VARCHAR\t\\uffff:CHAR
                null:VARCHAR\t:CHAR
                k\tv
                min\t
                max\t\\uffff
                other_null\t\\uffff
                null\t
                ## qwp-egress
                min\twire=22 \\u0000
                max\twire=22 \\uffff
                other_null\twire=22 \\uffff
                null\twire=22 \\u0000
                end rows=4
                ## ilp-udp
                min\tstring\s
                max\tstring \\uffff
                other_null\tstring \\uffff
                null\tomitted
                k\tv
                min\t
                max\t\\uffff
                other_null\t\\uffff
                null\t
                """);
        rec("INT", """
                ## ilp-tcp
                min\tlong -2147483647
                max\tlong 2147483647
                sentinel\tlong -2147483648
                null\tomitted
                k\tv
                min\t-2147483647
                max\t2147483647
                sentinel\tnull
                null\tnull
                ~fence\tnull
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-2147483647:INT
                max:VARCHAR\t2147483647:INT
                sentinel:VARCHAR\tnull:INT
                null:VARCHAR\tnull:INT
                k\tv
                min\t-2147483647
                max\t2147483647
                sentinel\tnull
                null\tnull
                ## qwp-nonwal
                min\tint -2147483647\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tint 2147483647\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                sentinel\tint -2147483648\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tint -2147483647\tok
                max\tint 2147483647\tok
                sentinel\tint -2147483648\tok
                null\tomitted\tok
                k\tv
                min\t-2147483647
                max\t2147483647
                sentinel\tnull
                null\tnull
                ## ilp-http-nonwal
                min\tlong -2147483647\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tlong 2147483647\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                sentinel\tlong -2147483648\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tlong -2147483647\tok
                max\tlong 2147483647\tok
                sentinel\tlong -2147483648\tok
                null\tomitted\tok
                k\tv
                min\t-2147483647
                max\t2147483647
                sentinel\tnull
                null\tnull
                ## ilp-udp
                min\tlong -2147483647
                max\tlong 2147483647
                sentinel\tlong -2147483648
                null\tomitted
                k\tv
                min\t-2147483647
                max\t2147483647
                sentinel\tnull
                null\tnull
                ## qwp-egress
                min\twire=4 -2147483647
                max\twire=4 2147483647
                sentinel\twire=4 null
                null\twire=4 null
                end rows=4
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 4  |                 |         |              |\\u000d
                |  Rows imported  |                                                 4  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                      INT  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t-2147483647
                max\t2147483647
                sentinel\tnull
                null\tnull
                """);
        rec("LONG", """
                ## ilp-udp
                min\tlong -9223372036854775807
                max\tlong 9223372036854775807
                sentinel\tlong -9223372036854775808
                null\tomitted
                k\tv
                min\t-9223372036854775807
                max\t9223372036854775807
                sentinel\tnull
                null\tnull
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-9223372036854775807:LONG
                max:VARCHAR\t9223372036854775807:LONG
                sentinel:VARCHAR\tnull:LONG
                null:VARCHAR\tnull:LONG
                k\tv
                min\t-9223372036854775807
                max\t9223372036854775807
                sentinel\tnull
                null\tnull
                ## qwp-egress
                min\twire=5 -9223372036854775807
                max\twire=5 9223372036854775807
                sentinel\twire=5 null
                null\twire=5 null
                end rows=4
                ## ilp-http-nonwal
                min\tlong -9223372036854775807\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tlong 9223372036854775807\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                sentinel\tlong -9223372036854775808\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: Could not parse entire line, field value is invalid. Field: v; value: nulli [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tlong -9223372036854775807\tok
                max\tlong 9223372036854775807\tok
                sentinel\tlong -9223372036854775808\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: Could not parse entire line, field value is invalid. Field: v; value: nulli [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\tok
                k\tv
                min\t-9223372036854775807
                max\t9223372036854775807
                null\tnull
                ## qwp-nonwal
                min\tlong -9223372036854775807\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tlong 9223372036854775807\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                sentinel\tlong -9223372036854775808\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tlong -9223372036854775807\tok
                max\tlong 9223372036854775807\tok
                sentinel\tlong -9223372036854775808\tok
                null\tomitted\tok
                k\tv
                min\t-9223372036854775807
                max\t9223372036854775807
                sentinel\tnull
                null\tnull
                ## ilp-tcp
                min\tlong -9223372036854775807
                max\tlong 9223372036854775807
                sentinel\tlong -9223372036854775808
                null\tomitted
                k\tv
                min\t-9223372036854775807
                max\t9223372036854775807
                sentinel\tnull
                null\tnull
                ~fence\tnull
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 4  |                 |         |              |\\u000d
                |  Rows imported  |                                                 4  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                     LONG  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t-9223372036854775807
                max\t9223372036854775807
                sentinel\tnull
                null\tnull
                """);
        rec("DATE", """
                ## qwp-egress
                min\twire=11 -9223372036854775807
                max\twire=11 9223372036854775807
                sentinel\twire=11 null
                null\twire=11 null
                end rows=4
                ## ilp-udp
                min\tclient error: long overflow; row sent without v
                max\tclient error: long overflow; row sent without v
                sentinel\tclient error: long overflow; row sent without v
                null\tomitted
                k\tv
                min\t
                max\t
                sentinel\t
                null\t
                ## qwp-nonwal
                min\ttimestamp -9223372036854775807 ms\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\ttimestamp 9223372036854775807 ms\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                sentinel\ttimestamp -9223372036854775808 ms\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\ttimestamp -9223372036854775807 ms\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot write TIMESTAMP to column [column=v, type=DATE]
                max\ttimestamp 9223372036854775807 ms\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot write TIMESTAMP to column [column=v, type=DATE]
                sentinel\ttimestamp -9223372036854775808 ms\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot write TIMESTAMP to column [column=v, type=DATE]
                null\tomitted\tok
                k\tv
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 4  |                 |         |              |\\u000d
                |  Rows imported  |                                                 4  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                     DATE  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t
                max\t
                sentinel\t
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-292275055-05-16T16:47:04.193Z:DATE
                max:VARCHAR\t292278994-08-17T07:12:55.807Z:DATE
                sentinel:VARCHAR\t:DATE
                null:VARCHAR\t:DATE
                k\tv
                min\t-292275055-05-16T16:47:04.193Z
                max\t292278994-08-17T07:12:55.807Z
                sentinel\t
                null\t
                ## ilp-http-nonwal
                min\t\terror: long overflow
                max\t\terror: long overflow
                sentinel\t\terror: long overflow
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\t\terror: long overflow
                max\t\terror: long overflow
                sentinel\t\terror: long overflow
                null\tomitted\tok
                k\tv
                null\t
                ## ilp-tcp
                min\tclient error: long overflow; row sent without v
                max\tclient error: long overflow; row sent without v
                sentinel\tclient error: long overflow; row sent without v
                null\tomitted
                k\tv
                min\t
                max\t
                sentinel\t
                null\t
                ~fence\t
                """);
        rec("TIMESTAMP", """
                ## qwp-nonwal
                min\ttimestamp -9223372036854775807 Micros\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\ttimestamp 9223372036854775807 Micros\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                sentinel\ttimestamp -9223372036854775808 Micros\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\ttimestamp -9223372036854775807 Micros\tok
                max\ttimestamp 9223372036854775807 Micros\tok
                sentinel\ttimestamp -9223372036854775808 Micros\tok
                null\tomitted\tok
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                max\t294247-01-10T04:00:54.775807Z
                sentinel\t
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-290308-01-01T19:59:05.224193Z:TIMESTAMP
                max:VARCHAR\t294247-01-10T04:00:54.775807Z:TIMESTAMP
                sentinel:VARCHAR\t:TIMESTAMP
                null:VARCHAR\t:TIMESTAMP
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                max\t294247-01-10T04:00:54.775807Z
                sentinel\t
                null\t
                ## ilp-tcp
                min\ttimestamp -9223372036854775807 Micros
                max\ttimestamp 9223372036854775807 Micros
                sentinel\ttimestamp -9223372036854775808 Micros
                null\tomitted
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                max\t294247-01-10T04:00:54.775807Z
                sentinel\t
                null\t
                ~fence\t
                ## qwp-egress
                min\twire=10 -9223372036854775807
                max\twire=10 9223372036854775807
                sentinel\twire=10 null
                null\twire=10 null
                end rows=4
                ## ilp-udp
                min\ttimestamp -9223372036854775807 Micros
                max\ttimestamp 9223372036854775807 Micros
                sentinel\ttimestamp -9223372036854775808 Micros
                null\tomitted
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                max\t294247-01-10T04:00:54.775807Z
                sentinel\t
                null\t
                ## ilp-http-nonwal
                min\ttimestamp -9223372036854775807 Micros\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\ttimestamp 9223372036854775807 Micros\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                sentinel\ttimestamp -9223372036854775808 Micros\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: Could not parse entire line, field value is invalid. Field: v; value: nullt [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\ttimestamp -9223372036854775807 Micros\tok
                max\ttimestamp 9223372036854775807 Micros\tok
                sentinel\ttimestamp -9223372036854775808 Micros\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: Could not parse entire line, field value is invalid. Field: v; value: nullt [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\tok
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                max\t294247-01-10T04:00:54.775807Z
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 4  |                 |         |              |\\u000d
                |  Rows imported  |                                                 4  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                TIMESTAMP  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t
                max\t
                sentinel\t
                null\t
                """);
        rec("FLOAT", """
                ## qwp-nonwal
                min\tfloat -3.4028235E38\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tfloat 3.4028235E38\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                nan\tfloat NaN\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                literal_inf\tfloat NaN\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                negzero\tfloat -0.0\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                inf\tfloat Infinity\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                ninf\tfloat -Infinity\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tfloat -3.4028235E38\tok
                max\tfloat 3.4028235E38\tok
                nan\tfloat NaN\tok
                literal_inf\tfloat NaN\tok
                negzero\tfloat -0.0\tok
                null\tomitted\tok
                inf\tfloat Infinity\tok
                ninf\tfloat -Infinity\tok
                k\tv
                min\t-3.4028235E38
                max\t3.4028235E38
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 8  |                 |         |              |\\u000d
                |  Rows imported  |                                                 8  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                    FLOAT  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t-3.4028235E38
                max\t3.4028235E38
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## qwp-egress
                min\twire=6 -3.4028235E38
                max\twire=6 3.4028235E38
                nan\twire=6 null
                literal_inf\twire=6 null
                negzero\twire=6 -0.0
                null\twire=6 null
                inf\twire=6 Infinity
                ninf\twire=6 -Infinity
                end rows=8
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-3.4028235E38:FLOAT
                max:VARCHAR\t3.4028235E38:FLOAT
                nan:VARCHAR\tnull:FLOAT
                literal_inf:VARCHAR\tnull:FLOAT
                negzero:VARCHAR\t-0.0:FLOAT
                null:VARCHAR\tnull:FLOAT
                inf:VARCHAR\tnull:FLOAT
                ninf:VARCHAR\tnull:FLOAT
                k\tv
                min\t-3.4028235E38
                max\t3.4028235E38
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## ilp-tcp
                min\tdouble -3.4028234663852886E38
                max\tdouble 3.4028234663852886E38
                nan\tdouble NaN
                literal_inf\tdouble NaN
                negzero\tdouble -0.0
                null\tomitted
                inf\tdouble Infinity
                ninf\tdouble -Infinity
                k\tv
                min\t-3.4028235E38
                max\t3.4028235E38
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ~fence\tnull
                ## ilp-http-nonwal
                min\tdouble -3.4028234663852886E38\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tdouble 3.4028234663852886E38\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                nan\tdouble NaN\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                literal_inf\tdouble NaN\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                negzero\tdouble -0.0\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                inf\tdouble Infinity\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                ninf\tdouble -Infinity\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tdouble -3.4028234663852886E38\tok
                max\tdouble 3.4028234663852886E38\tok
                nan\tdouble NaN\tok
                literal_inf\tdouble NaN\tok
                negzero\tdouble -0.0\tok
                null\tomitted\tok
                inf\tdouble Infinity\tok
                ninf\tdouble -Infinity\tok
                k\tv
                min\t-3.4028235E38
                max\t3.4028235E38
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## ilp-udp
                min\tdouble -3.4028234663852886E38
                max\tdouble 3.4028234663852886E38
                nan\tdouble NaN
                literal_inf\tdouble NaN
                negzero\tdouble -0.0
                null\tomitted
                inf\tdouble Infinity
                ninf\tdouble -Infinity
                k\tv
                min\t-3.4028235E38
                max\t3.4028235E38
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                """);
        rec("DOUBLE", """
                ## ilp-udp
                min\tdouble -1.7976931348623157E308
                max\tdouble 1.7976931348623157E308
                nan\tdouble NaN
                literal_inf\tdouble NaN
                negzero\tdouble -0.0
                null\tomitted
                inf\tdouble Infinity
                ninf\tdouble -Infinity
                k\tv
                min\t-1.7976931348623157E308
                max\t1.7976931348623157E308
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-1.7976931348623157E308:DOUBLE
                max:VARCHAR\t1.7976931348623157E308:DOUBLE
                nan:VARCHAR\tnull:DOUBLE
                literal_inf:VARCHAR\tnull:DOUBLE
                negzero:VARCHAR\t-0.0:DOUBLE
                null:VARCHAR\tnull:DOUBLE
                inf:VARCHAR\tnull:DOUBLE
                ninf:VARCHAR\tnull:DOUBLE
                k\tv
                min\t-1.7976931348623157E308
                max\t1.7976931348623157E308
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## ilp-http-nonwal
                min\tdouble -1.7976931348623157E308\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tdouble 1.7976931348623157E308\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                nan\tdouble NaN\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                literal_inf\tdouble NaN\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                negzero\tdouble -0.0\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                inf\tdouble Infinity\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                ninf\tdouble -Infinity\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tdouble -1.7976931348623157E308\tok
                max\tdouble 1.7976931348623157E308\tok
                nan\tdouble NaN\tok
                literal_inf\tdouble NaN\tok
                negzero\tdouble -0.0\tok
                null\tomitted\tok
                inf\tdouble Infinity\tok
                ninf\tdouble -Infinity\tok
                k\tv
                min\t-1.7976931348623157E308
                max\t1.7976931348623157E308
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 8  |                 |         |              |\\u000d
                |  Rows imported  |                                                 8  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                   DOUBLE  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t-1.7976931348623157E308
                max\t1.7976931348623157E308
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## qwp-nonwal
                min\tdouble -1.7976931348623157E308\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tdouble 1.7976931348623157E308\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                nan\tdouble NaN\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                literal_inf\tdouble NaN\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                negzero\tdouble -0.0\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                inf\tdouble Infinity\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                ninf\tdouble -Infinity\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tdouble -1.7976931348623157E308\tok
                max\tdouble 1.7976931348623157E308\tok
                nan\tdouble NaN\tok
                literal_inf\tdouble NaN\tok
                negzero\tdouble -0.0\tok
                null\tomitted\tok
                inf\tdouble Infinity\tok
                ninf\tdouble -Infinity\tok
                k\tv
                min\t-1.7976931348623157E308
                max\t1.7976931348623157E308
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## ilp-tcp
                min\tdouble -1.7976931348623157E308
                max\tdouble 1.7976931348623157E308
                nan\tdouble NaN
                literal_inf\tdouble NaN
                negzero\tdouble -0.0
                null\tomitted
                inf\tdouble Infinity
                ninf\tdouble -Infinity
                k\tv
                min\t-1.7976931348623157E308
                max\t1.7976931348623157E308
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ~fence\tnull
                ## qwp-egress
                min\twire=7 -1.7976931348623157E308
                max\twire=7 1.7976931348623157E308
                nan\twire=7 null
                literal_inf\twire=7 null
                negzero\twire=7 -0.0
                null\twire=7 null
                inf\twire=7 Infinity
                ninf\twire=7 -Infinity
                end rows=8
                """);
        rec("STRING", """
                ## qwp-egress
                empty\twire=15\s
                min\twire=15 \s
                max\twire=15 ü€😀�
                escape\twire=15 a"b,c\\d'e
                null\twire=15 null
                end rows=5
                ## parquet
                read_parquet
                k\tv
                empty:VARCHAR\t:STRING
                min:VARCHAR\t :STRING
                max:VARCHAR\tü€😀�:STRING
                escape:VARCHAR\ta"b,c\\d'e:STRING
                null:VARCHAR\t:STRING
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 5  |                 |         |              |\\u000d
                |  Rows imported  |                                                 5  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                   STRING  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\\\d'e
                null\t
                ## ilp-tcp
                empty\tstring\s
                min\tstring \s
                max\tstring ü€😀�
                escape\tstring a"b,c\\d'e
                null\tomitted
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ~fence\t
                ## qwp-nonwal
                empty\tstring \terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                min\tstring  \terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tstring ü€😀�\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                escape\tstring a"b,c\\d'e\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                empty\tstring \tok
                min\tstring  \tok
                max\tstring ü€😀�\tok
                escape\tstring a"b,c\\d'e\tok
                null\tomitted\tok
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ## ilp-udp
                empty\tstring\s
                min\tstring \s
                max\tstring ü€😀�
                escape\tstring a"b,c\\d'e
                null\tomitted
                k\tv
                empty\t
                min\t\s
                max\tü€\\ude00�
                escape\ta\\"b,c\\\\d'e
                null\t
                ## ilp-http-nonwal
                empty\tstring \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                min\tstring  \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring ü€😀�\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                escape\tstring a"b,c\\d'e\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                empty\tstring \tok
                min\tstring  \tok
                max\tstring ü€😀�\tok
                escape\tstring a"b,c\\d'e\tok
                null\tomitted\tok
                k\tv
                empty\t
                min\t\s
                max\tü€??�
                escape\ta"b,c\\d'e
                null\t
                """);
        rec("SYMBOL", """
                ## qwp-egress
                empty\twire=9\s
                min\twire=9 \s
                max\twire=9 ü€😀�
                escape\twire=9 a"b,c\\d'e
                null\twire=9 null
                end rows=5
                ## ilp-http-nonwal
                empty\tsymbol \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                min\tsymbol  \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tsymbol ü€😀�\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                escape\tsymbol a"b,c\\d'e\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                empty\tsymbol \tok
                min\tsymbol  \tok
                max\tsymbol ü€😀�\tok
                escape\tsymbol a"b,c\\d'e\tok
                null\tomitted\tok
                k\tv
                empty\t
                min\t\s
                max\tü€??�
                escape\ta"b,c\\d'e
                null\t
                ## ilp-udp
                empty\tsymbol\s
                min\tsymbol \s
                max\tsymbol ü€😀�
                escape\tsymbol a"b,c\\d'e
                null\tomitted
                k\tv
                min\t\s
                max\tü€\\ude00�
                escape\ta"b,c\\d'e
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 5  |                 |         |              |\\u000d
                |  Rows imported  |                                                 5  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                   SYMBOL  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\\\d'e
                null\t
                ## parquet
                read_parquet
                k\tv
                empty:VARCHAR\t:VARCHAR
                min:VARCHAR\t :VARCHAR
                max:VARCHAR\tü€😀�:VARCHAR
                escape:VARCHAR\ta"b,c\\d'e:VARCHAR
                null:VARCHAR\t:VARCHAR
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ## ilp-tcp
                empty\tsymbol\s
                min\tsymbol \s
                max\tsymbol ü€😀�
                escape\tsymbol a"b,c\\d'e
                null\tomitted
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ~fence\t
                ## qwp-nonwal
                empty\tsymbol \terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                min\tsymbol  \terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tsymbol ü€😀�\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                escape\tsymbol a"b,c\\d'e\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                empty\tsymbol \tok
                min\tsymbol  \tok
                max\tsymbol ü€😀�\tok
                escape\tsymbol a"b,c\\d'e\tok
                null\tomitted\tok
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                """);
        rec("LONG256", """
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 4  |                 |         |              |\\u000d
                |  Rows imported  |                                                 4  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                  LONG256  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\t
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t0x00:LONG256
                max:VARCHAR\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff:LONG256
                sentinel:VARCHAR\t:LONG256
                null:VARCHAR\t:LONG256
                k\tv
                min\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\t
                null\t
                ## qwp-egress
                min\twire=13 0:0:0:0
                max\twire=13 ffffffffffffffff:ffffffffffffffff:ffffffffffffffff:ffffffffffffffff
                sentinel\twire=13 null
                null\twire=13 null
                end rows=4
                ## qwp-nonwal
                min\tlong256 0x00\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tlong256 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                sentinel\tlong256 \terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tlong256 0x00\tok
                max\tlong256 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\tok
                sentinel\tlong256 \tok
                null\tomitted\tok
                k\tv
                min\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\t
                null\t
                ## ilp-http-nonwal
                min\tstring 0x00\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                sentinel\tstring \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tstring 0x00\tok
                max\tstring 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\tok
                sentinel\tstring \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst, column: v; cast error from protocol type: STRING to column type: LONG256 [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\tok
                k\tv
                min\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                null\t
                ## ilp-tcp
                min\tstring 0x00
                max\tstring 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\tstring\s
                null\tomitted
                k\tv
                min\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                null\t
                ~fence\t
                ## ilp-udp
                min\tstring 0x00
                max\tstring 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\tstring\s
                null\tomitted
                k\tv
                min\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                null\t
                """);
        rec("GEOBYTE", """
                ## ilp-http-nonwal
                min\tstring 0000000\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring 1111111\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tstring 0000000\tok
                max\tstring 1111111\tok
                null\tomitted\tok
                k\tv
                min\t0000000
                max\t0000100
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t0000000:GEOHASH(7b)
                max:VARCHAR\t1111111:GEOHASH(7b)
                null:VARCHAR\t:GEOHASH(7b)
                k\tv
                min\t0000000
                max\t1111111
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |              GEOHASH(7b)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t0000000
                max\t0000100
                null\t
                ## ilp-udp
                min\tstring 0000000
                max\tstring 1111111
                null\tomitted
                k\tv
                min\t0000000
                max\t0000100
                null\t
                ## ilp-tcp
                min\tstring 0000000
                max\tstring 1111111
                null\tomitted
                k\tv
                min\t0000000
                max\t0000100
                null\t
                ~fence\t
                ## qwp-egress
                min\twire=14 0/7
                max\twire=14 7f/7
                null\twire=14 null
                end rows=3
                ## qwp-nonwal
                min\tgeohash 0/7\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tgeohash 7f/7\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tgeohash 0/7\tok
                max\tgeohash 7f/7\tok
                null\tomitted\tok
                k\tv
                min\t0000000
                max\t1111111
                null\t
                """);
        rec("GEOSHORT", """
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |              GEOHASH(3c)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t000
                max\tzzz
                null\t
                ## qwp-egress
                min\twire=14 0/15
                max\twire=14 7fff/15
                null\twire=14 null
                end rows=3
                ## ilp-udp
                min\tstring 000
                max\tstring zzz
                null\tomitted
                k\tv
                min\t000
                max\tzzz
                null\t
                ## qwp-nonwal
                min\tgeohash 0/15\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tgeohash 7fff/15\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tgeohash 0/15\tok
                max\tgeohash 7fff/15\tok
                null\tomitted\tok
                k\tv
                min\t000
                max\tzzz
                null\t
                ## ilp-http-nonwal
                min\tstring 000\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring zzz\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tstring 000\tok
                max\tstring zzz\tok
                null\tomitted\tok
                k\tv
                min\t000
                max\tzzz
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t000:GEOHASH(3c)
                max:VARCHAR\tzzz:GEOHASH(3c)
                null:VARCHAR\t:GEOHASH(3c)
                k\tv
                min\t000
                max\tzzz
                null\t
                ## ilp-tcp
                min\tstring 000
                max\tstring zzz
                null\tomitted
                k\tv
                min\t000
                max\tzzz
                null\t
                ~fence\t
                """);
        rec("GEOINT", """
                ## ilp-udp
                min\tstring 000000
                max\tstring zzzzzz
                null\tomitted
                k\tv
                min\t000000
                max\tzzzzzz
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t000000:GEOHASH(6c)
                max:VARCHAR\tzzzzzz:GEOHASH(6c)
                null:VARCHAR\t:GEOHASH(6c)
                k\tv
                min\t000000
                max\tzzzzzz
                null\t
                ## ilp-http-nonwal
                min\tstring 000000\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring zzzzzz\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tstring 000000\tok
                max\tstring zzzzzz\tok
                null\tomitted\tok
                k\tv
                min\t000000
                max\tzzzzzz
                null\t
                ## ilp-tcp
                min\tstring 000000
                max\tstring zzzzzz
                null\tomitted
                k\tv
                min\t000000
                max\tzzzzzz
                null\t
                ~fence\t
                ## qwp-nonwal
                min\tgeohash 0/30\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tgeohash 3fffffff/30\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tgeohash 0/30\tok
                max\tgeohash 3fffffff/30\tok
                null\tomitted\tok
                k\tv
                min\t000000
                max\tzzzzzz
                null\t
                ## qwp-egress
                min\twire=14 0/30
                max\twire=14 3fffffff/30
                null\twire=14 null
                end rows=3
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |              GEOHASH(6c)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t000000
                max\tzzzzzz
                null\t
                """);
        rec("GEOLONG", """
                ## ilp-udp
                min\tstring 00000000
                max\tstring zzzzzzzz
                null\tomitted
                k\tv
                min\t00000000
                max\tzzzzzzzz
                null\t
                ## qwp-nonwal
                min\tgeohash 0/40\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tgeohash ffffffffff/40\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tgeohash 0/40\tok
                max\tgeohash ffffffffff/40\tok
                null\tomitted\tok
                k\tv
                min\t00000000
                max\t
                null\t
                ## qwp-egress
                min\twire=14 0/40
                max\twire=14 ffffffffff/40
                null\twire=14 null
                end rows=3
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t00000000:GEOHASH(8c)
                max:VARCHAR\tzzzzzzzz:GEOHASH(8c)
                null:VARCHAR\t:GEOHASH(8c)
                k\tv
                min\t00000000
                max\tzzzzzzzz
                null\t
                ## ilp-tcp
                min\tstring 00000000
                max\tstring zzzzzzzz
                null\tomitted
                k\tv
                min\t00000000
                max\tzzzzzzzz
                null\t
                ~fence\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |              GEOHASH(8c)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t00000000
                max\tzzzzzzzz
                null\t
                ## ilp-http-nonwal
                min\tstring 00000000\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring zzzzzzzz\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tstring 00000000\tok
                max\tstring zzzzzzzz\tok
                null\tomitted\tok
                k\tv
                min\t00000000
                max\tzzzzzzzz
                null\t
                """);
        rec("BINARY", """
                ## ilp-udp
                empty\tstring\s
                max\tstring 00000000 00 01 02 fd fe ff
                null\tomitted
                k\tv
                null\t
                ## ilp-tcp
                empty\tstring\s
                max\tstring 00000000 00 01 02 fd fe ff
                null\tomitted
                k\tv
                null\t
                ~fence\t
                ## csv
                /imp status 200
                cannot import text into BINARY column [index=1]
                k\tv
                ## ilp-http-nonwal
                empty\tstring \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring 00000000 00 01 02 fd fe ff\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                empty\tstring \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst, column: v; cast error from protocol type: STRING to column type: BINARY [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring 00000000 00 01 02 fd fe ff\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst, column: v; cast error from protocol type: STRING to column type: BINARY [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\tok
                k\tv
                null\t
                ## qwp-nonwal
                empty\tbinary \terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tbinary 000102fdfeff\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                empty\tbinary \tok
                max\tbinary 000102fdfeff\tok
                null\tomitted\tok
                k\tv
                empty\t
                max\t00000000 00 01 02 fd fe ff
                null\t
                ## parquet
                read_parquet
                k\tv
                empty:VARCHAR\t:BINARY
                max:VARCHAR\t00000000 00 01 02 fd fe ff:BINARY
                null:VARCHAR\t:BINARY
                k\tv
                empty\t
                max\t00000000 00 01 02 fd fe ff
                null\t
                ## qwp-egress
                empty\twire=23\s
                max\twire=23 000102fdfeff
                null\twire=23 null
                end rows=3
                """);
        rec("UUID", """
                ## qwp-nonwal
                min\tuuid lo=0 hi=0\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tuuid lo=ffffffffffffffff hi=ffffffffffffffff\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                sentinel\tuuid lo=8000000000000000 hi=8000000000000000\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tuuid lo=0 hi=0\tok
                max\tuuid lo=ffffffffffffffff hi=ffffffffffffffff\tok
                sentinel\tuuid lo=8000000000000000 hi=8000000000000000\tok
                null\tomitted\tok
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t00000000-0000-0000-0000-000000000000:UUID
                max:VARCHAR\tffffffff-ffff-ffff-ffff-ffffffffffff:UUID
                sentinel:VARCHAR\t:UUID
                null:VARCHAR\t:UUID
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 4  |                 |         |              |\\u000d
                |  Rows imported  |                                                 4  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                     UUID  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                ## ilp-tcp
                min\tstring 00000000-0000-0000-0000-000000000000
                max\tstring ffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\tstring\s
                null\tomitted
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                null\t
                ~fence\t
                ## ilp-udp
                min\tstring 00000000-0000-0000-0000-000000000000
                max\tstring ffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\tstring\s
                null\tomitted
                k\tv
                null\t
                ## qwp-egress
                min\twire=12 hi=0 lo=0
                max\twire=12 hi=ffffffffffffffff lo=ffffffffffffffff
                sentinel\twire=12 null
                null\twire=12 null
                end rows=4
                ## ilp-http-nonwal
                min\tstring 00000000-0000-0000-0000-000000000000\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring ffffffff-ffff-ffff-ffff-ffffffffffff\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                sentinel\tstring \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tstring 00000000-0000-0000-0000-000000000000\tok
                max\tstring ffffffff-ffff-ffff-ffff-ffffffffffff\tok
                sentinel\tstring \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst, column: v; cast error from protocol type: STRING to column type: UUID [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\tok
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                null\t
                """);
        rec("LONG128", """
                ## ilp-udp
                min\tstring 00000000-0000-0000-0000-000000000000
                max\tstring ffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\tstring\s
                null\tomitted
                k\tv
                null\t
                ## csv
                error: /exp status 400: {"query":"SELECT k, v, ts FROM src","error":"[-1] column type not supported [column=v, type=LONG128]","position":0}
                ## ilp-http-nonwal
                min\tstring 00000000-0000-0000-0000-000000000000\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring ffffffff-ffff-ffff-ffff-ffffffffffff\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                sentinel\tstring \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tstring 00000000-0000-0000-0000-000000000000\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst, column: v; cast error from protocol type: STRING to column type: LONG128 [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring ffffffff-ffff-ffff-ffff-ffffffffffff\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst, column: v; cast error from protocol type: STRING to column type: LONG128 [http-status=400, id: <id>, code: invalid, line: 1]
                sentinel\tstring \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst, column: v; cast error from protocol type: STRING to column type: LONG128 [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\tok
                k\tv
                null\t
                ## qwp-egress
                error: status=6 QWP egress: unsupported column type LONG128
                ## qwp-nonwal
                min\tuuid lo=0 hi=0\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tuuid lo=ffffffffffffffff hi=ffffffffffffffff\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                sentinel\tuuid lo=8000000000000000 hi=8000000000000000\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tuuid lo=0 hi=0\terror: server rejected batch: PROTOCOL_VIOLATION fsn=[0,0] - frame at fsn=0 rejected 1 consecutive times with no acceptance at or beyond it -- poisoned frame, replay cannot succeed (last: server NACK status=0x9 (WRITE_ERROR): unsupported column type for columnar write: LONG128)
                max\tuuid lo=ffffffffffffffff hi=ffffffffffffffff\terror: server rejected batch: PROTOCOL_VIOLATION fsn=[0,0] - frame at fsn=0 rejected 1 consecutive times with no acceptance at or beyond it -- poisoned frame, replay cannot succeed (last: server NACK status=0x9 (WRITE_ERROR): unsupported column type for columnar write: LONG128)
                sentinel\tuuid lo=8000000000000000 hi=8000000000000000\terror: server rejected batch: PROTOCOL_VIOLATION fsn=[0,0] - frame at fsn=0 rejected 1 consecutive times with no acceptance at or beyond it -- poisoned frame, replay cannot succeed (last: server NACK status=0x9 (WRITE_ERROR): unsupported column type for columnar write: LONG128)
                null\tomitted\tok
                k\tv
                null\t
                ## ilp-tcp
                min\tstring 00000000-0000-0000-0000-000000000000
                max\tstring ffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\tstring\s
                null\tomitted
                k\tv
                null\t
                ~fence\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t00000000-0000-0000-0000-000000000000:LONG128
                max:VARCHAR\tffffffff-ffff-ffff-ffff-ffffffffffff:LONG128
                sentinel:VARCHAR\t:LONG128
                null:VARCHAR\t:LONG128
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                """);
        rec("IPv4", """
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t0.0.0.1:IPv4
                max:VARCHAR\t255.255.255.255:IPv4
                sentinel:VARCHAR\t:IPv4
                null:VARCHAR\t:IPv4
                k\tv
                min\t0.0.0.1
                max\t255.255.255.255
                sentinel\t
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 4  |                 |         |              |\\u000d
                |  Rows imported  |                                                 4  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                     IPv4  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t0.0.0.1
                max\t255.255.255.255
                sentinel\t
                null\t
                ## qwp-egress
                min\twire=24 1
                max\twire=24 -1
                sentinel\twire=24 null
                null\twire=24 null
                end rows=4
                ## ilp-http-nonwal
                min\tstring 0.0.0.1\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring 255.255.255.255\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                sentinel\tstring \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tstring 0.0.0.1\tok
                max\tstring 255.255.255.255\tok
                sentinel\tstring \tok
                null\tomitted\tok
                k\tv
                min\t0.0.0.1
                max\t255.255.255.255
                sentinel\t
                null\t
                ## ilp-tcp
                min\tstring 0.0.0.1
                max\tstring 255.255.255.255
                sentinel\tstring\s
                null\tomitted
                k\tv
                min\t0.0.0.1
                max\t255.255.255.255
                sentinel\t
                null\t
                ~fence\t
                ## qwp-nonwal
                min\tipv4 1\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tipv4 -1\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                sentinel\tipv4 0\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tipv4 1\tok
                max\tipv4 -1\tok
                sentinel\tipv4 0\tok
                null\tomitted\tok
                k\tv
                min\t0.0.0.1
                max\t255.255.255.255
                sentinel\t
                null\t
                ## ilp-udp
                min\tstring 0.0.0.1
                max\tstring 255.255.255.255
                sentinel\tstring\s
                null\tomitted
                k\tv
                min\t0.0.0.1
                max\t255.255.255.255
                sentinel\t
                null\t
                """);
        rec("VARCHAR", """
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 5  |                 |         |              |\\u000d
                |  Rows imported  |                                                 5  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |                  VARCHAR  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\\\d'e
                null\t
                ## ilp-tcp
                empty\tstring\s
                min\tstring \s
                max\tstring ü€😀�
                escape\tstring a"b,c\\d'e
                null\tomitted
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ~fence\t
                ## parquet
                read_parquet
                k\tv
                empty:VARCHAR\t:VARCHAR
                min:VARCHAR\t :VARCHAR
                max:VARCHAR\tü€😀�:VARCHAR
                escape:VARCHAR\ta"b,c\\d'e:VARCHAR
                null:VARCHAR\t:VARCHAR
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ## ilp-http-nonwal
                empty\tstring \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                min\tstring  \terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring ü€😀�\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                escape\tstring a"b,c\\d'e\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                empty\tstring \tok
                min\tstring  \tok
                max\tstring ü€😀�\tok
                escape\tstring a"b,c\\d'e\tok
                null\tomitted\tok
                k\tv
                empty\t
                min\t\s
                max\tü€??�
                escape\ta"b,c\\d'e
                null\t
                ## ilp-udp
                empty\tstring\s
                min\tstring \s
                max\tstring ü€😀�
                escape\tstring a"b,c\\d'e
                null\tomitted
                k\tv
                empty\t
                min\t\s
                max\tü€?�
                escape\ta\\"b,c\\\\d'e
                null\t
                ## qwp-egress
                empty\twire=15\s
                min\twire=15 \s
                max\twire=15 ü€😀�
                escape\twire=15 a"b,c\\d'e
                null\twire=15 null
                end rows=5
                ## qwp-nonwal
                empty\tstring \terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                min\tstring  \terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tstring ü€😀�\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                escape\tstring a"b,c\\d'e\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                empty\tstring \tok
                min\tstring  \tok
                max\tstring ü€😀�\tok
                escape\tstring a"b,c\\d'e\tok
                null\tomitted\tok
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                """);
        rec("DOUBLE[]", """
                ## csv
                /imp status 200
                no adapter for type [id=2587, name=DOUBLE[]]
                k\tv
                ## ilp-tcp
                min\tarray [-1.7976931348623157E308]
                max\tarray [1.7976931348623157E308]
                empty\tarray []
                specials\tarray [null,null,null,-0.0]
                null\tomitted
                k\tv
                min\t[-1.7976931348623157E308]
                max\t[1.7976931348623157E308]
                empty\t[]
                specials\t[null,null,null,-0.0]
                null\tnull
                ~fence\tnull
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t[-1.7976931348623157E308]:DOUBLE[]
                max:VARCHAR\t[1.7976931348623157E308]:DOUBLE[]
                empty:VARCHAR\t[]:DOUBLE[]
                specials:VARCHAR\t[null,null,null,-0.0]:DOUBLE[]
                null:VARCHAR\tnull:DOUBLE[]
                k\tv
                min\t[-1.7976931348623157E308]
                max\t[1.7976931348623157E308]
                empty\t[]
                specials\t[null,null,null,-0.0]
                null\tnull
                ## ilp-udp
                min\tclient error: current protocol version does not support double-array; row sent without v
                max\tclient error: current protocol version does not support double-array; row sent without v
                empty\tclient error: current protocol version does not support double-array; row sent without v
                specials\tclient error: current protocol version does not support double-array; row sent without v
                null\tomitted
                k\tv
                min\tnull
                max\tnull
                empty\tnull
                specials\tnull
                null\tnull
                ## qwp-nonwal
                min\tarray [-1.7976931348623157E308]\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tarray [1.7976931348623157E308]\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                empty\tarray []\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                specials\tarray [null,null,null,-0.0]\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tarray [-1.7976931348623157E308]\tok
                max\tarray [1.7976931348623157E308]\tok
                empty\tarray []\tok
                specials\tarray [null,null,null,-0.0]\tok
                null\tomitted\tok
                k\tv
                min\t[-1.7976931348623157E308]
                max\t[1.7976931348623157E308]
                empty\t[]
                specials\t[null,null,null,-0.0]
                null\tnull
                ## ilp-http-nonwal
                min\tarray [-1.7976931348623157E308]\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tarray [1.7976931348623157E308]\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                empty\tarray []\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                specials\tarray [null,null,null,-0.0]\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tarray [-1.7976931348623157E308]\tok
                max\tarray [1.7976931348623157E308]\tok
                empty\tarray []\tok
                specials\tarray [null,null,null,-0.0]\tok
                null\tomitted\tok
                k\tv
                min\t[-1.7976931348623157E308]
                max\t[1.7976931348623157E308]
                empty\t[]
                specials\t[null,null,null,-0.0]
                null\tnull
                ## qwp-egress
                min\twire=17 dims=1 [-1.7976931348623157E308]
                max\twire=17 dims=1 [1.7976931348623157E308]
                empty\twire=17 dims=1 []
                specials\twire=17 dims=1 [NaN,NaN,NaN,-0.0]
                null\twire=17 null
                end rows=5
                """);
        rec("DECIMAL8", """
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |             DECIMAL(2,1)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t-9.9
                max\t9.9
                null\t
                ## ilp-http-nonwal
                min\tdecimal -9.9\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tdecimal 9.9\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tdecimal -9.9\tok
                max\tdecimal 9.9\tok
                null\tomitted\tok
                k\tv
                min\t-9.9
                max\t9.9
                null\t
                ## qwp-egress
                error: status=6 QWP egress: unsupported column type DECIMAL(2,1)
                ## ilp-udp
                min\tclient error: current protocol version does not support decimal; row sent without v
                max\tclient error: current protocol version does not support decimal; row sent without v
                null\tomitted
                k\tv
                min\t
                max\t
                null\t
                ## ilp-tcp
                min\tdecimal -9.9
                max\tdecimal 9.9
                null\tomitted
                k\tv
                min\t-9.9
                max\t9.9
                null\t
                ~fence\t
                ## qwp-nonwal
                min\tdecimal -9.9\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tdecimal 9.9\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tdecimal -9.9\tok
                max\tdecimal 9.9\tok
                null\tomitted\tok
                k\tv
                min\t-9.9
                max\t9.9
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-9.9:DECIMAL(2,1)
                max:VARCHAR\t9.9:DECIMAL(2,1)
                null:VARCHAR\t:DECIMAL(2,1)
                k\tv
                min\t-9.9
                max\t9.9
                null\t
                """);
        rec("DECIMAL16", """
                ## ilp-udp
                min\tclient error: current protocol version does not support decimal; row sent without v
                max\tclient error: current protocol version does not support decimal; row sent without v
                null\tomitted
                k\tv
                min\t
                max\t
                null\t
                ## qwp-egress
                error: status=6 QWP egress: unsupported column type DECIMAL(4,2)
                ## qwp-nonwal
                min\tdecimal -99.99\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tdecimal 99.99\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tdecimal -99.99\tok
                max\tdecimal 99.99\tok
                null\tomitted\tok
                k\tv
                min\t-99.99
                max\t99.99
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-99.99:DECIMAL(4,2)
                max:VARCHAR\t99.99:DECIMAL(4,2)
                null:VARCHAR\t:DECIMAL(4,2)
                k\tv
                min\t-99.99
                max\t99.99
                null\t
                ## ilp-http-nonwal
                min\tdecimal -99.99\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tdecimal 99.99\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tdecimal -99.99\tok
                max\tdecimal 99.99\tok
                null\tomitted\tok
                k\tv
                min\t-99.99
                max\t99.99
                null\t
                ## ilp-tcp
                min\tdecimal -99.99
                max\tdecimal 99.99
                null\tomitted
                k\tv
                min\t-99.99
                max\t99.99
                null\t
                ~fence\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |             DECIMAL(4,2)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t-99.99
                max\t99.99
                null\t
                """);
        rec("DECIMAL32", """
                ## ilp-http-nonwal
                min\tdecimal -999999999\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tdecimal 999999999\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tdecimal -999999999\tok
                max\tdecimal 999999999\tok
                null\tomitted\tok
                k\tv
                min\t-999999999
                max\t999999999
                null\t
                ## qwp-egress
                error: status=6 QWP egress: unsupported column type DECIMAL(9,0)
                ## qwp-nonwal
                min\tdecimal -999999999\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tdecimal 999999999\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tdecimal -999999999\tok
                max\tdecimal 999999999\tok
                null\tomitted\tok
                k\tv
                min\t-999999999
                max\t999999999
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-999999999:DECIMAL(9,0)
                max:VARCHAR\t999999999:DECIMAL(9,0)
                null:VARCHAR\t:DECIMAL(9,0)
                k\tv
                min\t-999999999
                max\t999999999
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |             DECIMAL(9,0)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t-999999999
                max\t999999999
                null\t
                ## ilp-tcp
                min\tdecimal -999999999
                max\tdecimal 999999999
                null\tomitted
                k\tv
                min\t-999999999
                max\t999999999
                null\t
                ~fence\t
                ## ilp-udp
                min\tclient error: current protocol version does not support decimal; row sent without v
                max\tclient error: current protocol version does not support decimal; row sent without v
                null\tomitted
                k\tv
                min\t
                max\t
                null\t
                """);
        rec("DECIMAL64", """
                ## qwp-nonwal
                min\tdecimal -999999999999.9999\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tdecimal 999999999999.9999\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tdecimal -999999999999.9999\tok
                max\tdecimal 999999999999.9999\tok
                null\tomitted\tok
                k\tv
                min\t-999999999999.9999
                max\t999999999999.9999
                null\t
                ## ilp-udp
                min\tclient error: current protocol version does not support decimal; row sent without v
                max\tclient error: current protocol version does not support decimal; row sent without v
                null\tomitted
                k\tv
                min\t
                max\t
                null\t
                ## qwp-egress
                min\twire=19 -999999999999.9999
                max\twire=19 999999999999.9999
                null\twire=19 null
                end rows=3
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-999999999999.9999:DECIMAL(16,4)
                max:VARCHAR\t999999999999.9999:DECIMAL(16,4)
                null:VARCHAR\t:DECIMAL(16,4)
                k\tv
                min\t-999999999999.9999
                max\t999999999999.9999
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |            DECIMAL(16,4)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t-999999999999.9999
                max\t999999999999.9999
                null\t
                ## ilp-http-nonwal
                min\tdecimal -999999999999.9999\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tdecimal 999999999999.9999\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tdecimal -999999999999.9999\tok
                max\tdecimal 999999999999.9999\tok
                null\tomitted\tok
                k\tv
                min\t-999999999999.9999
                max\t999999999999.9999
                null\t
                ## ilp-tcp
                min\tdecimal -999999999999.9999
                max\tdecimal 999999999999.9999
                null\tomitted
                k\tv
                min\t-999999999999.9999
                max\t999999999999.9999
                null\t
                ~fence\t
                """);
        rec("DECIMAL128", """
                ## ilp-udp
                min\tclient error: current protocol version does not support decimal; row sent without v
                max\tclient error: current protocol version does not support decimal; row sent without v
                null\tomitted
                k\tv
                min\t
                max\t
                null\t
                ## ilp-http-nonwal
                min\tdecimal -9999999999999999999999999999.9999999999\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tdecimal 9999999999999999999999999999.9999999999\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tdecimal -9999999999999999999999999999.9999999999\tok
                max\tdecimal 9999999999999999999999999999.9999999999\tok
                null\tomitted\tok
                k\tv
                min\t-9999999999999999999999999999.9999999999
                max\t9999999999999999999999999999.9999999999
                null\t
                ## ilp-tcp
                min\tdecimal -9999999999999999999999999999.9999999999
                max\tdecimal 9999999999999999999999999999.9999999999
                null\tomitted
                k\tv
                min\t-9999999999999999999999999999.9999999999
                max\t9999999999999999999999999999.9999999999
                null\t
                ~fence\t
                ## qwp-nonwal
                min\tdecimal -9999999999999999999999999999.9999999999\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tdecimal 9999999999999999999999999999.9999999999\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tdecimal -9999999999999999999999999999.9999999999\tok
                max\tdecimal 9999999999999999999999999999.9999999999\tok
                null\tomitted\tok
                k\tv
                min\t-9999999999999999999999999999.9999999999
                max\t9999999999999999999999999999.9999999999
                null\t
                ## qwp-egress
                min\twire=20 -9999999999999999999999999999.9999999999
                max\twire=20 9999999999999999999999999999.9999999999
                null\twire=20 null
                end rows=3
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-9999999999999999999999999999.9999999999:DECIMAL(38,10)
                max:VARCHAR\t9999999999999999999999999999.9999999999:DECIMAL(38,10)
                null:VARCHAR\t:DECIMAL(38,10)
                k\tv
                min\t-9999999999999999999999999999.9999999999
                max\t9999999999999999999999999999.9999999999
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |           DECIMAL(38,10)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t-9999999999999999999999999999.9999999999
                max\t9999999999999999999999999999.9999999999
                null\t
                """);
        rec("DECIMAL256", """
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |           DECIMAL(76,20)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                ## ilp-tcp
                min\tdecimal -99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\tdecimal 99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\tomitted
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                ~fence\t
                ## ilp-udp
                min\tclient error: current protocol version does not support decimal; row sent without v
                max\tclient error: current protocol version does not support decimal; row sent without v
                null\tomitted
                k\tv
                min\t
                max\t
                null\t
                ## ilp-http-nonwal
                min\tdecimal -99999999999999999999999999999999999999999999999999999999.99999999999999999999\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tdecimal 99999999999999999999999999999999999999999999999999999999.99999999999999999999\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tdecimal -99999999999999999999999999999999999999999999999999999999.99999999999999999999\tok
                max\tdecimal 99999999999999999999999999999999999999999999999999999999.99999999999999999999\tok
                null\tomitted\tok
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999:DECIMAL(76,20)
                max:VARCHAR\t99999999999999999999999999999999999999999999999999999999.99999999999999999999:DECIMAL(76,20)
                null:VARCHAR\t:DECIMAL(76,20)
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                ## qwp-egress
                min\twire=21 -99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\twire=21 99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\twire=21 null
                end rows=3
                ## qwp-nonwal
                min\tdecimal -99999999999999999999999999999999999999999999999999999999.99999999999999999999\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tdecimal 99999999999999999999999999999999999999999999999999999999.99999999999999999999\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tdecimal -99999999999999999999999999999999999999999999999999999999.99999999999999999999\tok
                max\tdecimal 99999999999999999999999999999999999999999999999999999999.99999999999999999999\tok
                null\tomitted\tok
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                """);
        rec("INTERVAL", """
                ## ilp-tcp
                error: [31] non-persisted type: INTERVAL
                ## qwp-egress
                error: [31] non-persisted type: INTERVAL
                ## parquet
                error: [31] non-persisted type: INTERVAL
                ## qwp-nonwal
                error: [31] non-persisted type: INTERVAL
                ## qwp
                error: [31] non-persisted type: INTERVAL
                ## ilp-udp
                error: [31] non-persisted type: INTERVAL
                ## csv
                error: [31] non-persisted type: INTERVAL
                ## ilp-http-nonwal
                error: [31] non-persisted type: INTERVAL
                ## ilp-http
                error: [31] non-persisted type: INTERVAL
                """);
        rec("VARCHAR_SLICE", """
                ## qwp-nonwal
                error: [31] unsupported column type: VARCHAR_SLICE
                ## qwp
                error: [31] unsupported column type: VARCHAR_SLICE
                ## csv
                error: [31] unsupported column type: VARCHAR_SLICE
                ## qwp-egress
                error: [31] unsupported column type: VARCHAR_SLICE
                ## ilp-tcp
                error: [31] unsupported column type: VARCHAR_SLICE
                ## ilp-http-nonwal
                error: [31] unsupported column type: VARCHAR_SLICE
                ## ilp-http
                error: [31] unsupported column type: VARCHAR_SLICE
                ## ilp-udp
                error: [31] unsupported column type: VARCHAR_SLICE
                ## parquet
                error: [31] unsupported column type: VARCHAR_SLICE
                """);
        rec("TIMESTAMP_NS", """
                ## ilp-udp
                min\ttimestamp -9223372036854775807 Nanos
                max\ttimestamp 9223372036854775807 Nanos
                sentinel\ttimestamp -9223372036854775808 Nanos
                null\tomitted
                k\tv
                min\t1969-09-16T05:57:07.963145225Z
                max\t1970-04-17T18:02:52.036854775Z
                sentinel\t1969-09-16T05:57:07.963145225Z
                null\t
                ## qwp-nonwal
                min\ttimestamp -9223372036854775807 Nanos\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\ttimestamp 9223372036854775807 Nanos\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                sentinel\ttimestamp -9223372036854775808 Nanos\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\ttimestamp -9223372036854775807 Nanos\tok
                max\ttimestamp 9223372036854775807 Nanos\tok
                sentinel\ttimestamp -9223372036854775808 Nanos\tok
                null\tomitted\tok
                k\tv
                min\t1677-01-01T00:12:43.145224193Z
                max\t2262-04-11T23:47:16.854775807Z
                sentinel\t
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 4  |                 |         |              |\\u000d
                |  Rows imported  |                                                 4  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |             TIMESTAMP_NS  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t
                max\t
                sentinel\t
                null\t
                ## ilp-http-nonwal
                min\ttimestamp -9223372036854775807 Nanos\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\ttimestamp 9223372036854775807 Nanos\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                sentinel\ttimestamp -9223372036854775808 Nanos\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: Could not parse entire line, field value is invalid. Field: v; value: nulln [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\ttimestamp -9223372036854775807 Nanos\tok
                max\ttimestamp 9223372036854775807 Nanos\tok
                sentinel\ttimestamp -9223372036854775808 Nanos\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: Could not parse entire line, field value is invalid. Field: v; value: nulln [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\tok
                k\tv
                min\t1677-01-01T00:12:43.145224193Z
                max\t2262-04-11T23:47:16.854775807Z
                null\t
                ## ilp-tcp
                min\ttimestamp -9223372036854775807 Nanos
                max\ttimestamp 9223372036854775807 Nanos
                sentinel\ttimestamp -9223372036854775808 Nanos
                null\tomitted
                k\tv
                min\t1677-01-01T00:12:43.145224193Z
                max\t2262-04-11T23:47:16.854775807Z
                sentinel\t
                null\t
                ~fence\t
                ## qwp-egress
                min\twire=16 -9223372036854775807
                max\twire=16 9223372036854775807
                sentinel\twire=16 null
                null\twire=16 null
                end rows=4
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t1677-01-01T00:12:43.145224193Z:TIMESTAMP_NS
                max:VARCHAR\t2262-04-11T23:47:16.854775807Z:TIMESTAMP_NS
                sentinel:VARCHAR\t:TIMESTAMP_NS
                null:VARCHAR\t:TIMESTAMP_NS
                k\tv
                min\t1677-01-01T00:12:43.145224193Z
                max\t2262-04-11T23:47:16.854775807Z
                sentinel\t
                null\t
                """);
        rec("GEOHASH(1c)", """
                ## ilp-tcp
                min\tstring 0
                max\tstring z
                null\tomitted
                k\tv
                min\t0
                max\tz
                null\t
                ~fence\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t0:GEOHASH(1c)
                max:VARCHAR\tz:GEOHASH(1c)
                null:VARCHAR\t:GEOHASH(1c)
                k\tv
                min\t0
                max\tz
                null\t
                ## ilp-udp
                min\tstring 0
                max\tstring z
                null\tomitted
                k\tv
                min\t0
                max\tz
                null\t
                ## ilp-http-nonwal
                min\tstring 0\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring z\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tstring 0\tok
                max\tstring z\tok
                null\tomitted\tok
                k\tv
                min\t0
                max\tz
                null\t
                ## qwp-egress
                min\twire=14 0/5
                max\twire=14 1f/5
                null\twire=14 null
                end rows=3
                ## qwp-nonwal
                min\tgeohash 0/5\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tgeohash 1f/5\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tgeohash 0/5\tok
                max\tgeohash 1f/5\tok
                null\tomitted\tok
                k\tv
                min\t0
                max\tz
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |              GEOHASH(1c)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t0
                max\tz
                null\t
                """);
        rec("GEOHASH(8b)", """
                ## ilp-udp
                min\tstring 00000000
                max\tstring 11111111
                null\tomitted
                k\tv
                min\t00000000
                max\t00001000
                null\t
                ## qwp-nonwal
                min\tgeohash 0/8\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tgeohash ff/8\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tgeohash 0/8\tok
                max\tgeohash ff/8\tok
                null\tomitted\tok
                k\tv
                min\t00000000
                max\t
                null\t
                ## ilp-tcp
                min\tstring 00000000
                max\tstring 11111111
                null\tomitted
                k\tv
                min\t00000000
                max\t00001000
                null\t
                ~fence\t
                ## qwp-egress
                min\twire=14 0/8
                max\twire=14 ff/8
                null\twire=14 null
                end rows=3
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t00000000:GEOHASH(8b)
                max:VARCHAR\t11111111:GEOHASH(8b)
                null:VARCHAR\t:GEOHASH(8b)
                k\tv
                min\t00000000
                max\t11111111
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |              GEOHASH(8b)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t00000000
                max\t00001000
                null\t
                ## ilp-http-nonwal
                min\tstring 00000000\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring 11111111\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tstring 00000000\tok
                max\tstring 11111111\tok
                null\tomitted\tok
                k\tv
                min\t00000000
                max\t00001000
                null\t
                """);
        rec("GEOHASH(31b)", """
                ## ilp-tcp
                min\tstring 0000000000000000000000000000000
                max\tstring 1111111111111111111111111111111
                null\tomitted
                k\tv
                min\t0000000000000000000000000000000
                max\t0000100001000010000100001000010
                null\t
                ~fence\t
                ## ilp-udp
                min\tstring 0000000000000000000000000000000
                max\tstring 1111111111111111111111111111111
                null\tomitted
                k\tv
                min\t0000000000000000000000000000000
                max\t0000100001000010000100001000010
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |             GEOHASH(31b)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t0000000000000000000000000000000
                max\t0000100001000010000100001000010
                null\t
                ## ilp-http-nonwal
                min\tstring 0000000000000000000000000000000\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring 1111111111111111111111111111111\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tstring 0000000000000000000000000000000\tok
                max\tstring 1111111111111111111111111111111\tok
                null\tomitted\tok
                k\tv
                min\t0000000000000000000000000000000
                max\t0000100001000010000100001000010
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t0000000000000000000000000000000:GEOHASH(31b)
                max:VARCHAR\t1111111111111111111111111111111:GEOHASH(31b)
                null:VARCHAR\t:GEOHASH(31b)
                k\tv
                min\t0000000000000000000000000000000
                max\t1111111111111111111111111111111
                null\t
                ## qwp-nonwal
                min\tgeohash 0/31\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tgeohash 7fffffff/31\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tgeohash 0/31\tok
                max\tgeohash 7fffffff/31\tok
                null\tomitted\tok
                k\tv
                min\t0000000000000000000000000000000
                max\t1111111111111111111111111111111
                null\t
                ## qwp-egress
                min\twire=14 0/31
                max\twire=14 7fffffff/31
                null\twire=14 null
                end rows=3
                """);
        rec("GEOHASH(12c)", """
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t000000000000:GEOHASH(12c)
                max:VARCHAR\tzzzzzzzzzzzz:GEOHASH(12c)
                null:VARCHAR\t:GEOHASH(12c)
                k\tv
                min\t000000000000
                max\tzzzzzzzzzzzz
                null\t
                ## ilp-http-nonwal
                min\tstring 000000000000\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tstring zzzzzzzzzzzz\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tstring 000000000000\tok
                max\tstring zzzzzzzzzzzz\tok
                null\tomitted\tok
                k\tv
                min\t000000000000
                max\tzzzzzzzzzzzz
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |             GEOHASH(12c)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t000000000000
                max\tzzzzzzzzzzzz
                null\t
                ## qwp-egress
                min\twire=14 0/60
                max\twire=14 fffffffffffffff/60
                null\twire=14 null
                end rows=3
                ## ilp-tcp
                min\tstring 000000000000
                max\tstring zzzzzzzzzzzz
                null\tomitted
                k\tv
                min\t000000000000
                max\tzzzzzzzzzzzz
                null\t
                ~fence\t
                ## qwp-nonwal
                min\tgeohash 0/60\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tgeohash fffffffffffffff/60\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tgeohash 0/60\tok
                max\tgeohash fffffffffffffff/60\tok
                null\tomitted\tok
                k\tv
                min\t000000000000
                max\tzzzzzzzzzzzz
                null\t
                ## ilp-udp
                min\tstring 000000000000
                max\tstring zzzzzzzzzzzz
                null\tomitted
                k\tv
                min\t000000000000
                max\tzzzzzzzzzzzz
                null\t
                """);
        rec("DECIMAL(5,2)", """
                ## qwp-egress
                error: status=6 QWP egress: unsupported column type DECIMAL(5,2)
                ## ilp-http-nonwal
                min\tdecimal -999.99\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tdecimal 999.99\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tdecimal -999.99\tok
                max\tdecimal 999.99\tok
                null\tomitted\tok
                k\tv
                min\t-999.99
                max\t999.99
                null\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |             DECIMAL(5,2)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t-999.99
                max\t999.99
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-999.99:DECIMAL(5,2)
                max:VARCHAR\t999.99:DECIMAL(5,2)
                null:VARCHAR\t:DECIMAL(5,2)
                k\tv
                min\t-999.99
                max\t999.99
                null\t
                ## ilp-udp
                min\tclient error: current protocol version does not support decimal; row sent without v
                max\tclient error: current protocol version does not support decimal; row sent without v
                null\tomitted
                k\tv
                min\t
                max\t
                null\t
                ## ilp-tcp
                min\tdecimal -999.99
                max\tdecimal 999.99
                null\tomitted
                k\tv
                min\t-999.99
                max\t999.99
                null\t
                ~fence\t
                ## qwp-nonwal
                min\tdecimal -999.99\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tdecimal 999.99\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tdecimal -999.99\tok
                max\tdecimal 999.99\tok
                null\tomitted\tok
                k\tv
                min\t-999.99
                max\t999.99
                null\t
                """);
        rec("DECIMAL(18,3)", """
                ## ilp-http-nonwal
                min\tdecimal -999999999999999.999\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tdecimal 999999999999999.999\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tdecimal -999999999999999.999\tok
                max\tdecimal 999999999999999.999\tok
                null\tomitted\tok
                k\tv
                min\t-999999999999999.999
                max\t999999999999999.999
                null\t
                ## ilp-udp
                min\tclient error: current protocol version does not support decimal; row sent without v
                max\tclient error: current protocol version does not support decimal; row sent without v
                null\tomitted
                k\tv
                min\t
                max\t
                null\t
                ## qwp-nonwal
                min\tdecimal -999999999999999.999\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tdecimal 999999999999999.999\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tdecimal -999999999999999.999\tok
                max\tdecimal 999999999999999.999\tok
                null\tomitted\tok
                k\tv
                min\t-999999999999999.999
                max\t999999999999999.999
                null\t
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t-999999999999999.999:DECIMAL(18,3)
                max:VARCHAR\t999999999999999.999:DECIMAL(18,3)
                null:VARCHAR\t:DECIMAL(18,3)
                k\tv
                min\t-999999999999999.999
                max\t999999999999999.999
                null\t
                ## ilp-tcp
                min\tdecimal -999999999999999.999
                max\tdecimal 999999999999999.999
                null\tomitted
                k\tv
                min\t-999999999999999.999
                max\t999999999999999.999
                null\t
                ~fence\t
                ## csv
                /imp status 200
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |      Location:  |                                            dst  |        Pattern  | Locale  |      Errors  |\\u000d
                |   Partition by  |                                               DAY  |                 |         |              |\\u000d
                |      Timestamp  |                                                ts  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |   Rows handled  |                                                 3  |                 |         |              |\\u000d
                |  Rows imported  |                                                 3  |                 |         |              |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                |              0  |                                                 k  |                  VARCHAR  |           0  |\\u000d
                |              1  |                                                 v  |            DECIMAL(18,3)  |           0  |\\u000d
                |              2  |                                                ts  |                TIMESTAMP  |           0  |\\u000d
                +-----------------------------------------------------------------------------------------------------------------+\\u000d
                k\tv
                min\t-999999999999999.999
                max\t999999999999999.999
                null\t
                ## qwp-egress
                min\twire=19 -999999999999999.999
                max\twire=19 999999999999999.999
                null\twire=19 null
                end rows=3
                """);
        rec("DOUBLE[][]", """
                ## ilp-http-nonwal
                min\tarray [[-1.7976931348623157E308]]\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                max\tarray [[1.7976931348623157E308]]\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                empty\tarray []\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                specials\tarray [[null,null,null,-0.0]]\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                null\tomitted\terror: Could not flush buffer: failed to parse line protocol:errors encountered on line(s):\\u000aerror in line 1: table: dst; cannot insert in non-WAL table [http-status=400, id: <id>, code: invalid, line: 1]
                k\tv
                ## ilp-http
                min\tarray [[-1.7976931348623157E308]]\tok
                max\tarray [[1.7976931348623157E308]]\tok
                empty\tarray []\tok
                specials\tarray [[null,null,null,-0.0]]\tok
                null\tomitted\tok
                k\tv
                min\t[[-1.7976931348623157E308]]
                max\t[[1.7976931348623157E308]]
                empty\t[]
                specials\t[[null,null,null,-0.0]]
                null\tnull
                ## parquet
                read_parquet
                k\tv
                min:VARCHAR\t[[-1.7976931348623157E308]]:DOUBLE[][]
                max:VARCHAR\t[[1.7976931348623157E308]]:DOUBLE[][]
                empty:VARCHAR\t[]:DOUBLE[][]
                specials:VARCHAR\t[[null,null,null,-0.0]]:DOUBLE[][]
                null:VARCHAR\tnull:DOUBLE[][]
                k\tv
                min\t[[-1.7976931348623157E308]]
                max\t[[1.7976931348623157E308]]
                empty\t[]
                specials\t[[null,null,null,-0.0]]
                null\tnull
                ## qwp-egress
                min\twire=17 dims=2 [-1.7976931348623157E308]
                max\twire=17 dims=2 [1.7976931348623157E308]
                empty\twire=17 dims=2 []
                specials\twire=17 dims=2 [NaN,NaN,NaN,-0.0]
                null\twire=17 null
                end rows=5
                ## ilp-tcp
                min\tarray [[-1.7976931348623157E308]]
                max\tarray [[1.7976931348623157E308]]
                empty\tarray []
                specials\tarray [[null,null,null,-0.0]]
                null\tomitted
                k\tv
                min\t[[-1.7976931348623157E308]]
                max\t[[1.7976931348623157E308]]
                empty\t[]
                specials\t[[null,null,null,-0.0]]
                null\tnull
                ~fence\tnull
                ## qwp-nonwal
                min\tarray [[-1.7976931348623157E308]]\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                max\tarray [[1.7976931348623157E308]]\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                empty\tarray []\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                specials\tarray [[null,null,null,-0.0]]\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                null\tomitted\terror: server rejected batch: SCHEMA_MISMATCH (status=0x3) fsn=[0,0] seq=0 - cannot insert into non-WAL table: dst
                k\tv
                ## qwp
                min\tarray [[-1.7976931348623157E308]]\tok
                max\tarray [[1.7976931348623157E308]]\tok
                empty\tarray []\tok
                specials\tarray [[null,null,null,-0.0]]\tok
                null\tomitted\tok
                k\tv
                min\t[[-1.7976931348623157E308]]
                max\t[[1.7976931348623157E308]]
                empty\t[]
                specials\t[[null,null,null,-0.0]]
                null\tnull
                ## ilp-udp
                min\tclient error: current protocol version does not support double-array; row sent without v
                max\tclient error: current protocol version does not support double-array; row sent without v
                empty\tclient error: current protocol version does not support double-array; row sent without v
                specials\tclient error: current protocol version does not support double-array; row sent without v
                null\tomitted
                k\tv
                min\tnull
                max\tnull
                empty\tnull
                specials\tnull
                null\tnull
                ## csv
                /imp status 200
                no adapter for type [id=18971, name=DOUBLE[][]]
                k\tv
                """);
        rec("INTERVAL(us)", """
                ## qwp-egress
                error: [31] non-persisted type: INTERVAL
                ## csv
                error: [31] non-persisted type: INTERVAL
                ## qwp-nonwal
                error: [31] non-persisted type: INTERVAL
                ## qwp
                error: [31] non-persisted type: INTERVAL
                ## ilp-udp
                error: [31] non-persisted type: INTERVAL
                ## ilp-http-nonwal
                error: [31] non-persisted type: INTERVAL
                ## ilp-http
                error: [31] non-persisted type: INTERVAL
                ## ilp-tcp
                error: [31] non-persisted type: INTERVAL
                ## parquet
                error: [31] non-persisted type: INTERVAL
                """);
        rec("INTERVAL(ns)", """
                ## ilp-tcp
                error: [31] non-persisted type: INTERVAL
                ## qwp-nonwal
                error: [31] non-persisted type: INTERVAL
                ## qwp
                error: [31] non-persisted type: INTERVAL
                ## csv
                error: [31] non-persisted type: INTERVAL
                ## qwp-egress
                error: [31] non-persisted type: INTERVAL
                ## ilp-udp
                error: [31] non-persisted type: INTERVAL
                ## ilp-http-nonwal
                error: [31] non-persisted type: INTERVAL
                ## ilp-http
                error: [31] non-persisted type: INTERVAL
                ## parquet
                error: [31] non-persisted type: INTERVAL
                """);
    }
    // recordings: end

    private static void rec(String label, String recording) {
        RECORDINGS.put(label, recording);
    }
}
