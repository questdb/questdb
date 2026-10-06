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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientException;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractTest;
import io.questdb.test.cutlass.http.HttpQueryTestBuilder;
import io.questdb.test.cutlass.http.HttpServerConfigurationBuilder;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;

/**
 * The HTTP part of the conformance kit: every kit type through {@code /query} (JSON), {@code /exp}
 * as CSV and {@code /exp} as Parquet, against an HTTP server of {@link HttpQueryTestBuilder}, under
 * its memory-leak check.
 * <p>
 * What is recorded, per type ({@link TypeConformanceRecording}):
 * <ul>
 * <li> the status line and every response header, one line each;</li>
 * <li> {@code http.json}: the {@code /query} body, split losslessly into a head line, one line per
 * dataset row labelled with its value row, and a tail line; the rows join with a comma;</li>
 * <li> {@code http.csv}: the {@code /exp} CSV body, one line per CSV line labelled with its value
 * row; CSV lines end with CR LF, and the escaped CR prints as a backslash, {@code u} and {@code
 * 000d};</li>
 * <li> {@code http.parquet}: the byte length and SHA-256 of the {@code /exp} Parquet body, its
 * {@code created_by} and {@code questdb} key-value metadata, and the file read back through {@code
 * read_parquet} (column names and types, then the rows). The digest makes any byte change fail; the
 * decoded rows keep the content reviewable without committing binary recordings.</li>
 * </ul>
 * <p>
 * Modes: the Parquet export takes one of several paths by query shape ({@code ParquetExportMode}):
 * {@code direct} ({@code SELECT k, v FROM t}, page frames zero-copy), {@code cursor} (the same rows
 * through a filter, materialized row by row; BINARY goes through a temp table) and {@code hybrid}
 * (a computed column next to the zero-copy ones). Each has its own section ({@code http.parquet},
 * {@code http.parquet-cursor}, {@code http.parquet-hybrid}): the cursor path writes SYMBOL as
 * STRING (type code 11) where the direct path keeps SYMBOL (type code 12), and the hybrid query
 * exports one more column. JSON and CSV print through record cursors and run once. The table is
 * non-WAL and partitioned by day: WAL gives the same output on every path (both modes store the
 * same bytes, as the storage part shows), so WAL is not a mode here.
 * <p>
 * Masks: the value of the {@code Date} header (the test server's clock) becomes {@code <date>}; the
 * clock number in the export file name ({@code questdb-query-<n>}) becomes {@code <clock>}; the
 * database root becomes {@code <dbRoot>}; in the client's error when the server closes the
 * connection, the errno becomes {@code <errno>}. The Parquet bytes need no mask: {@code created_by}
 * is the fixed text {@code QuestDB version 9.0}, and the {@code questdb} key holds only the schema
 * version and the column type codes.
 * <p>
 * A type registered later runs where {@link TypeConformanceInvariants#isEnabled} says so and is
 * checked without a recording: invariant 2 on every path by the printed values of its NULL and
 * sentinel-pattern rows (under NONE, on JSON and CSV, also of its zero row), and invariant 1 on the
 * three Parquet paths by the bits read back from the exported file. Invariant 1 is not checked on
 * JSON and CSV, because the kit does not derive the expected text of such a type's values.
 */
@RunWith(Parameterized.class)
public class TypeConformanceHttpTest extends AbstractTest {
    private static final String[] MODES = {"nonwal-day"};
    private static final String[] PARQUET_MODES = {"direct", "cursor", "hybrid"};
    private static final Map<String, String> RECORDINGS = new HashMap<>();
    private final ObjList<TypeConformanceValues.Row> rows;
    private final TypeConformanceTypes.Entry type;

    public TypeConformanceHttpTest(String label) {
        this.type = TypeConformanceTypes.byLabel(label);
        this.rows = TypeConformanceValues.rowsOf(type);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return TypeConformanceTypes.parameters();
    }

    @Test
    public void testCsv() throws Exception {
        for (String mode : MODES) {
            if (!TypeConformanceInvariants.isEnabled(type, "http.csv", mode)) {
                continue;
            }
            tester().run((engine, executionContext) -> {
                final StringSink section = new StringSink();
                if (createTable(engine, executionContext, mode, section)) {
                    final Utf8StringSink body = new Utf8StringSink();
                    if (get(section, "/exp", "SELECT k, v FROM t", "csv", body) == 0) {
                        assertAnswered("http.csv", mode, section);
                        return;
                    }
                    if (type.isLater()) {
                        checkLaterText(engine, executionContext, "http.csv", mode, section, csvValues(body.toString()));
                        return;
                    }
                    appendCsv(section, body.toString());
                }
                assertSection("http.csv", mode, section);
            });
        }
    }

    @Test
    public void testJson() throws Exception {
        for (String mode : MODES) {
            if (!TypeConformanceInvariants.isEnabled(type, "http.json", mode)) {
                continue;
            }
            tester().run((engine, executionContext) -> {
                final StringSink section = new StringSink();
                if (createTable(engine, executionContext, mode, section)) {
                    final Utf8StringSink body = new Utf8StringSink();
                    if (get(section, "/query", "SELECT k, v FROM t", null, body) == 0) {
                        assertAnswered("http.json", mode, section);
                        return;
                    }
                    if (type.isLater()) {
                        checkLaterText(engine, executionContext, "http.json", mode, section, jsonValues(body.toString()));
                        return;
                    }
                    appendJson(section, body.toString());
                }
                assertSection("http.json", mode, section);
            });
        }
    }

    @Test
    public void testParquet() throws Exception {
        for (String tableMode : MODES) {
            for (String exportMode : PARQUET_MODES) {
                final String mode = tableMode + "-" + exportMode;
                final String path = "direct".equals(exportMode) ? "http.parquet" : "http.parquet-" + exportMode;
                if (!TypeConformanceInvariants.isEnabled(type, path, mode)) {
                    continue;
                }
                final String query = switch (exportMode) {
                    case "direct" -> "SELECT k, v FROM t";
                    case "cursor" -> "SELECT k, v FROM t WHERE k != 'no row'";
                    default -> "SELECT k, v, 7 AS c FROM t";
                };
                tester().run((engine, executionContext) -> {
                    final StringSink section = new StringSink();
                    if (createTable(engine, executionContext, tableMode, section)) {
                        final Utf8StringSink body = new Utf8StringSink();
                        final int status = get(section, "/exp", query, "parquet", body);
                        if (status == 200) {
                            appendParquet(engine, executionContext, section, body, path, mode);
                        } else if (status > 0) {
                            section.put("body\t").put(body.toString()).put('\n');
                        }
                        if (status != 200 && type.isLater()) {
                            // a later type has no recording to hold a refused export: the export must work
                            Assert.fail(TypeConformanceInvariants.context(type, "-", path, mode) + ": the export failed: " + section);
                        }
                    } else if (type.isLater()) {
                        Assert.fail(TypeConformanceInvariants.context(type, "-", path, mode) + ": " + section);
                    }
                    if (!type.isLater()) {
                        assertSection(path, mode, section);
                    }
                });
            }
        }
    }

    private static void appendCsv(StringSink section, String body) {
        final String[] lines = body.split("\n", -1);
        for (int i = 0, n = lines.length; i < n; i++) {
            if (i == n - 1 && lines[i].isEmpty()) {
                section.put("eof\tnewline\n");
                return;
            }
            section.put(csvLabel(lines[i])).put('\t').put(lines[i]).put('\n');
        }
        section.put("eof\tno newline\n");
    }

    private static void appendJson(StringSink section, String body) {
        final ObjList<String> elements = new ObjList<>();
        final String[] headAndTail = new String[2];
        if (!splitJsonDataset(body, headAndTail, elements)) {
            section.put("body\t").put(body).put('\n');
            return;
        }
        section.put("head\t").put(headAndTail[0]).put('\n');
        for (int i = 0, n = elements.size(); i < n; i++) {
            final String element = elements.getQuick(i);
            section.put(jsonLabel(element)).put('\t').put(element).put('\n');
        }
        section.put("tail\t").put(headAndTail[1]).put('\n');
    }

    /**
     * The value row of a CSV line: its first field, unquoted.
     */
    private static String csvLabel(String line) {
        if (line.startsWith("\"")) {
            final int end = line.indexOf('"', 1);
            return end > 0 ? line.substring(1, end) : line;
        }
        final int comma = line.indexOf(',');
        return comma > -1 ? line.substring(0, comma) : line;
    }

    /**
     * Maps each value row to its printed value in a CSV body: the text after the first comma.
     */
    private static Map<String, String> csvValues(String body) {
        final Map<String, String> values = new HashMap<>();
        final String[] lines = body.split("\n");
        for (int i = 1; i < lines.length; i++) {
            final int comma = lines[i].indexOf(',');
            if (comma > -1) {
                values.put(csvLabel(lines[i]), lines[i].substring(comma + 1));
            }
        }
        return values;
    }

    private static String extractAscii(byte[] bytes, String start, boolean isJson) {
        final byte[] needle = start.getBytes(StandardCharsets.US_ASCII);
        outer:
        for (int i = 0, n = bytes.length - needle.length; i <= n; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (bytes[i + j] != needle[j]) {
                    continue outer;
                }
            }
            int end = i;
            if (isJson) {
                // a JSON object without strings that hold braces: balance the braces
                int depth = 0;
                for (; end < bytes.length; end++) {
                    if (bytes[end] == '{') {
                        depth++;
                    } else if (bytes[end] == '}' && --depth == 0) {
                        end++;
                        break;
                    }
                }
            } else {
                while (end < bytes.length && bytes[end] >= 0x20 && bytes[end] < 0x7F) {
                    end++;
                }
            }
            return new String(bytes, i, end - i, StandardCharsets.US_ASCII);
        }
        return "(absent)";
    }

    private static String jsonLabel(String element) {
        if (element.startsWith("[\"")) {
            final int end = element.indexOf('"', 2);
            if (end > 1) {
                return element.substring(2, end);
            }
        }
        return "row";
    }

    /**
     * Maps each value row to its printed value in a {@code /query} body: the element text after
     * the row label.
     */
    private static Map<String, String> jsonValues(String body) {
        final Map<String, String> values = new HashMap<>();
        final ObjList<String> elements = new ObjList<>();
        if (splitJsonDataset(body, new String[2], elements)) {
            for (int i = 0, n = elements.size(); i < n; i++) {
                final String element = elements.getQuick(i);
                final String label = jsonLabel(element);
                final int valueStart = element.indexOf(',', label.length() + 3);
                values.put(label, valueStart > -1 ? element.substring(valueStart + 1, element.length() - 1) : element);
            }
        }
        return values;
    }

    private static String mask(CharSequence text) {
        return text.toString()
                .replace(root, "<dbRoot>")
                .replaceAll("(?m)^header\tDate: .*$", "header\tDate: <date>")
                .replaceAll("questdb-query-[0-9]+", "questdb-query-<clock>");
    }


    /**
     * Splits a {@code /query} body at its dataset: {@code head} ends with {@code "dataset":[},
     * each element is one row array, {@code tail} starts with the closing bracket. Returns false
     * when the body has no dataset or the pieces would not join back into the body.
     */
    private static boolean splitJsonDataset(String body, String[] headAndTail, ObjList<String> elements) {
        final String marker = "\"dataset\":[";
        final int dataset = body.indexOf(marker);
        if (dataset < 0) {
            return false;
        }
        int i = dataset + marker.length();
        final String head = body.substring(0, i);
        final int n = body.length();
        while (i < n && body.charAt(i) == '[') {
            final int start = i;
            int depth = 0;
            boolean isInString = false;
            for (; i < n; i++) {
                final char c = body.charAt(i);
                if (isInString) {
                    if (c == '\\') {
                        i++;
                    } else if (c == '"') {
                        isInString = false;
                    }
                } else if (c == '"') {
                    isInString = true;
                } else if (c == '[') {
                    depth++;
                } else if (c == ']' && --depth == 0) {
                    i++;
                    break;
                }
            }
            elements.add(body.substring(start, i));
            if (i < n && body.charAt(i) == ',') {
                i++;
            }
        }
        final String tail = body.substring(i);
        final StringBuilder joined = new StringBuilder(head);
        for (int e = 0, m = elements.size(); e < m; e++) {
            if (e > 0) {
                joined.append(',');
            }
            joined.append(elements.getQuick(e));
        }
        joined.append(tail);
        if (!joined.toString().equals(body)) {
            elements.clear();
            return false;
        }
        headAndTail[0] = head;
        headAndTail[1] = tail;
        return true;
    }

    /**
     * A server over a fresh database: every run in a test starts from an empty root.
     */
    private static HttpQueryTestBuilder tester() {
        TestUtils.removeTestPath(root);
        TestUtils.createTestPath(root);
        return new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .withCopyExportRoot(root + "/export")
                .withCopyInputRoot(root + "/export");
    }

    private void appendParquet(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            StringSink section,
            Utf8StringSink body,
            String path,
            String mode
    ) throws Exception {
        final byte[] bytes = new byte[body.size()];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = body.byteAt(i);
        }
        section.put("bytes\t").put(bytes.length).put(" sha256=")
                .put(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))).put('\n');
        section.put("created_by\t").put(extractAscii(bytes, "QuestDB version", false)).put('\n');
        section.put("questdb\t").put(extractAscii(bytes, "{\"version\":", true)).put('\n');

        final String file = "kit_" + TypeConformanceTypes.ALL.indexOf(type) + ".parquet";
        final java.nio.file.Path exportDir = Paths.get(root, "export");
        Files.createDirectories(exportDir);
        Files.write(exportDir.resolve(file), bytes);
        final String readBack = "SELECT * FROM read_parquet('" + file + "')";
        if (type.isLater()) {
            checkLaterParquet(engine, executionContext, readBack, path, mode, section);
            return;
        }
        try (RecordCursorFactory factory = engine.select(readBack, executionContext)) {
            final RecordMetadata metadata = factory.getMetadata();
            section.put("columns\t");
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (i > 0) {
                    section.put(' ');
                }
                section.put(metadata.getColumnName(i)).put(':').put(ColumnType.nameOf(metadata.getColumnType(i)));
            }
            section.put('\n');
        } catch (Throwable e) {
            section.put("error: read_parquet: ").put(e.getMessage()).put('\n');
            return;
        }
        section.put(query(engine, executionContext, readBack));
    }

    /**
     * The server closed the connection instead of answering. The recording holds this outcome for
     * an existing type; for a type registered later it is a failure.
     */
    private void assertAnswered(String path, String mode, StringSink section) {
        if (type.isLater()) {
            Assert.fail(TypeConformanceInvariants.context(type, "-", path, mode) + ": no answer: " + section);
        }
        assertSection(path, mode, section);
    }

    private void assertSection(String path, String mode, CharSequence actual) {
        TypeConformanceRecording.assertSection(type, path, mode, RECORDINGS.get(type.label), mask(TypeConformanceRecording.escape(actual)));
    }

    private void checkLaterParquet(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            String readBack,
            String path,
            String mode,
            StringSink section
    ) throws Exception {
        final String nullError = TypeConformanceInvariants.nullRowWriteError(type, path, mode, section);
        final Map<String, long[]> bits = new HashMap<>();
        try (
                RecordCursorFactory factory = engine.select(readBack, executionContext);
                RecordCursor cursor = factory.getCursor(executionContext)
        ) {
            final Record record = cursor.getRecord();
            final int k = factory.getMetadata().getColumnIndex("k");
            final int v = factory.getMetadata().getColumnIndex("v");
            while (cursor.hasNext()) {
                bits.put(record.getVarcharA(k).toString(), TypeConformanceValues.readValue(record, v, type));
            }
        }
        final Map<String, String> texts = new HashMap<>();
        final String[] lines = query(engine, executionContext, "SELECT k, v FROM (" + readBack + ")").split("\n");
        for (int i = 1; i < lines.length; i++) {
            final int tab = lines[i].indexOf('\t');
            texts.put(lines[i].substring(0, tab), lines[i].substring(tab + 1));
        }
        TypeConformanceValues.Row sentinel = null;
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            if (row.isNull() || !bits.containsKey(row.label)) {
                continue;
            }
            TypeConformanceInvariants.assertReadsBackAsWritten(type, row.label, path, mode, row.bits, bits.get(row.label));
            if ("sentinel".equals(row.label)) {
                sentinel = row;
            }
        }
        for (int i = 0, n = rows.size(); i < n; i++) {
            final String label = rows.getQuick(i).label;
            if (label.startsWith("sentinel_") && texts.containsKey(label)) {
                TypeConformanceInvariants.assertOtherSentinel(type, label, path, mode, texts.get("null"), texts.get(label));
            }
        }
        if (sentinel != null) {
            TypeConformanceInvariants.assertNullPolicy(
                    type,
                    path,
                    mode,
                    texts.get("null"),
                    bits.get("null"),
                    nullError,
                    texts.get("sentinel"),
                    bits.get("sentinel"),
                    sentinel.bits
            );
        }
    }

    /**
     * Invariant 2 by the printed values of a text protocol, for a type registered later; the
     * stored bits come from the table.
     */
    private void checkLaterText(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            String path,
            String mode,
            StringSink section,
            Map<String, String> texts
    ) throws Exception {
        final String nullError = TypeConformanceInvariants.nullRowWriteError(type, path, mode, section);
        final Map<String, long[]> bits = new HashMap<>();
        try (
                RecordCursorFactory factory = engine.select("SELECT k, v FROM t", executionContext);
                RecordCursor cursor = factory.getCursor(executionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                bits.put(record.getVarcharA(0).toString(), TypeConformanceValues.readValue(record, 1, type));
            }
        }
        TypeConformanceValues.Row sentinel = null;
        for (int i = 0, n = rows.size(); i < n; i++) {
            if ("sentinel".equals(rows.getQuick(i).label)) {
                sentinel = rows.getQuick(i);
            }
        }
        if (sentinel == null) {
            return;
        }
        for (int i = 0, n = rows.size(); i < n; i++) {
            final String label = rows.getQuick(i).label;
            if (label.startsWith("sentinel_") && texts.containsKey(label)) {
                TypeConformanceInvariants.assertOtherSentinel(type, label, path, mode, texts.get("null"), texts.get(label));
            }
        }
        TypeConformanceInvariants.assertNullPolicy(
                type,
                path,
                mode,
                texts.get("null"),
                bits.get("null"),
                nullError,
                texts.get("sentinel"),
                bits.get("sentinel"),
                sentinel.bits
        );
        if (TypeConformanceInvariants.POLICY_NONE.equals(TypeConformanceInvariants.policyOf(type))) {
            Assert.assertEquals(
                    TypeConformanceInvariants.context(type, "null", path, mode) + ": NONE, the NULL row must print as the zero row",
                    texts.get("zero"),
                    texts.get("null")
            );
        }
    }

    /**
     * Creates table {@code t} and writes the type's value rows; a type that cannot be stored
     * leaves its error in {@code section} and returns false.
     */
    private boolean createTable(CairoEngine engine, SqlExecutionContext executionContext, String mode, StringSink section) {
        final boolean isWal = mode.startsWith("wal");
        try {
            engine.execute("CREATE TABLE t (k VARCHAR, v " + type.ddl + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY "
                    + (isWal ? "WAL" : "BYPASS WAL"), executionContext);
        } catch (Throwable e) {
            section.put("error: create: ").put(e.getMessage()).put('\n');
            return false;
        }
        TypeConformanceValues.writeRows(engine, executionContext, "t", rows, "", 0, 0, rows.size(), 1, true, section);
        if (isWal) {
            drainWalQueue(engine);
        }
        return true;
    }

    /**
     * Sends a GET and appends the status line and every header to {@code section}; the body goes
     * to {@code body}. Returns the status code, or 0 when the server closed the connection
     * instead of answering; that error goes to {@code section}, with the client's errno masked.
     */
    private int get(StringSink section, String url, String query, @Nullable String format, Utf8StringSink body) {
        try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
            return get(client, section, url, query, format, body);
        } catch (HttpClientException e) {
            section.put("error: ").put(e.getMessage().replaceAll("^\\[-?[0-9]+] ", "").replaceAll("\\[errno=-?[0-9]*]", "[errno=<errno>]")).put('\n');
            return 0;
        }
    }

    private int get(HttpClient client, StringSink section, String url, String query, @Nullable String format, Utf8StringSink body) {
        final HttpClient.Request request = client.newRequest("localhost", 9001);
        request.GET().url(url).query("query", query);
        if (format != null) {
            request.query("fmt", format);
        }
        final HttpClient.ResponseHeaders response = request.send();
        response.await();
        section.put("status\t").put(response.getStatusCode()).put(' ').put(response.getStatusText()).put('\n');
        final ObjList<? extends Utf8Sequence> names = response.getHeaderNames();
        for (int i = 0, n = names.size(); i < n; i++) {
            section.put("header\t").put(names.getQuick(i)).put(": ").put(response.getHeader(names.getQuick(i))).put('\n');
        }
        response.getResponse().copyTextTo(body);
        return Integer.parseInt(response.getStatusCode().toString());
    }

    private String query(CairoEngine engine, SqlExecutionContext executionContext, String sql) {
        final StringSink sink = new StringSink();
        try {
            TestUtils.printSql(engine, executionContext, sql, sink);
        } catch (Throwable e) {
            return "error: " + e.getMessage() + '\n';
        }
        return sink.toString();
    }

    // recordings: start
    static {
        rec("BOOLEAN", """
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min",false\\u000d
                max\t"max",true\\u000d
                null\t"null",false\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t576 sha256=042a14b413c43cafc93fd5641b2946c92ac30bc2ee23258b2a39fd6830415f7b
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":1,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:BOOLEAN
                k\tv
                min\tfalse
                max\ttrue
                null\tfalse
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t576 sha256=042a14b413c43cafc93fd5641b2946c92ac30bc2ee23258b2a39fd6830415f7b
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":1,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:BOOLEAN
                k\tv
                min\tfalse
                max\ttrue
                null\tfalse
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t826 sha256=76720ffd17d80658d4addde63c52048418fe1909a0fe6f6f904f6ae949d6e60b
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":1,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:BOOLEAN c:INT
                k\tv\tc
                min\tfalse\t7
                max\ttrue\t7
                null\tfalse\t7
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"BOOLEAN"}],"timestamp":-1,"dataset":[
                min\t["min",false]
                max\t["max",true]
                null\t["null",false]
                tail\t],"count":3}
                """);
        rec("BYTE", """
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"BYTE"}],"timestamp":-1,"dataset":[
                min\t["min",-128]
                max\t["max",127]
                other_null\t["other_null",-1]
                null\t["null",0]
                tail\t],"count":4}
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t665 sha256=60b26200483e0ea1886973cf04483838847b8d9cdebae47b82b068b5c1813fe4
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":2,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:BYTE
                k\tv
                min\t-128
                max\t127
                other_null\t-1
                null\t0
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t665 sha256=60b26200483e0ea1886973cf04483838847b8d9cdebae47b82b068b5c1813fe4
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":2,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:BYTE
                k\tv
                min\t-128
                max\t127
                other_null\t-1
                null\t0
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t914 sha256=e00af73e89700df2d35001f1944ff66edb82ff7870b7fcfb2f3a76333decdcae
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":2,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:BYTE c:INT
                k\tv\tc
                min\t-128\t7
                max\t127\t7
                other_null\t-1\t7
                null\t0\t7
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min",-128\\u000d
                max\t"max",127\\u000d
                other_null\t"other_null",-1\\u000d
                null\t"null",0\\u000d
                eof\tnewline
                """);
        rec("SHORT", """
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min",-32768\\u000d
                max\t"max",32767\\u000d
                other_null\t"other_null",-1\\u000d
                null\t"null",0\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t665 sha256=2cd716e4ef89b08502406814871b829b95b8ad796bb46dff2c5c7da23cce7278
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":3,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:SHORT
                k\tv
                min\t-32768
                max\t32767
                other_null\t-1
                null\t0
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t665 sha256=2cd716e4ef89b08502406814871b829b95b8ad796bb46dff2c5c7da23cce7278
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":3,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:SHORT
                k\tv
                min\t-32768
                max\t32767
                other_null\t-1
                null\t0
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t914 sha256=f88fc10c11f3a9a981631e830458a92bb59ac63368d7fd64df3b1d12232c1a90
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":3,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:SHORT c:INT
                k\tv\tc
                min\t-32768\t7
                max\t32767\t7
                other_null\t-1\t7
                null\t0\t7
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"SHORT"}],"timestamp":-1,"dataset":[
                min\t["min",-32768]
                max\t["max",32767]
                other_null\t["other_null",-1]
                null\t["null",0]
                tail\t],"count":4}
                """);
        rec("CHAR", """
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t665 sha256=72eee948b62e0453917e05dc34979c583351d61a875072da549876e9e6f129e1
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":4,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:CHAR
                k\tv
                min\t
                max\t\\uffff
                other_null\t\\uffff
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t665 sha256=72eee948b62e0453917e05dc34979c583351d61a875072da549876e9e6f129e1
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":4,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:CHAR
                k\tv
                min\t
                max\t\\uffff
                other_null\t\\uffff
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t914 sha256=43df33d15bc738524b19c5c9e8e676f7c6dab17777f7e19a18171ab4c4097be4
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":4,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:CHAR c:INT
                k\tv\tc
                min\t\t7
                max\t\\uffff\t7
                other_null\t\\uffff\t7
                null\t\t7
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min",\\u000d
                max\t"max",\\uffff\\u000d
                other_null\t"other_null",\\uffff\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"CHAR"}],"timestamp":-1,"dataset":[
                min\t["min",""]
                max\t["max","\\uffff"]
                other_null\t["other_null","\\uffff"]
                null\t["null",""]
                tail\t],"count":4}
                """);
        rec("INT", """
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"INT"}],"timestamp":-1,"dataset":[
                min\t["min",-2147483647]
                max\t["max",2147483647]
                sentinel\t["sentinel",null]
                null\t["null",null]
                tail\t],"count":4}
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min",-2147483647\\u000d
                max\t"max",2147483647\\u000d
                sentinel\t"sentinel",\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t637 sha256=a1f1e0f863a348f24511a63d32559008eca7402028b3a2ee56a2beafd3b9abe4
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":5,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:INT
                k\tv
                min\t-2147483647
                max\t2147483647
                sentinel\tnull
                null\tnull
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t637 sha256=a1f1e0f863a348f24511a63d32559008eca7402028b3a2ee56a2beafd3b9abe4
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":5,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:INT
                k\tv
                min\t-2147483647
                max\t2147483647
                sentinel\tnull
                null\tnull
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t886 sha256=42bdc8fed1055b2cfae9102a366d352ce17ffe24f6db30e23b192807ddebc3ef
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":5,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:INT c:INT
                k\tv\tc
                min\t-2147483647\t7
                max\t2147483647\t7
                sentinel\tnull\t7
                null\tnull\t7
                """);
        rec("LONG", """
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min",-9223372036854775807\\u000d
                max\t"max",9223372036854775807\\u000d
                sentinel\t"sentinel",\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"LONG"}],"timestamp":-1,"dataset":[
                min\t["min",-9223372036854775807]
                max\t["max",9223372036854775807]
                sentinel\t["sentinel",null]
                null\t["null",null]
                tail\t],"count":4}
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t675 sha256=a49a2c428eb2d0aaf96274e45387d999e56c57353fd2ef78c1cf6f561d81e747
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":6,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:LONG
                k\tv
                min\t-9223372036854775807
                max\t9223372036854775807
                sentinel\tnull
                null\tnull
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t675 sha256=a49a2c428eb2d0aaf96274e45387d999e56c57353fd2ef78c1cf6f561d81e747
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":6,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:LONG
                k\tv
                min\t-9223372036854775807
                max\t9223372036854775807
                sentinel\tnull
                null\tnull
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t924 sha256=81255231a13d07984d4980b040fb2f51008d4c06ed09fbc8c5f7139e5c29e675
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":6,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:LONG c:INT
                k\tv\tc
                min\t-9223372036854775807\t7
                max\t9223372036854775807\t7
                sentinel\tnull\t7
                null\tnull\t7
                """);
        rec("DATE", """
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"DATE"}],"timestamp":-1,"dataset":[
                min\t["min","-292275055-05-16T16:47:04.193Z"]
                max\t["max","292278994-08-17T07:12:55.807Z"]
                sentinel\t["sentinel",null]
                null\t["null",null]
                tail\t],"count":4}
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","-292275055-05-16T16:47:04.193Z"\\u000d
                max\t"max","292278994-08-17T07:12:55.807Z"\\u000d
                sentinel\t"sentinel",\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t686 sha256=aaff04c4cdb3760745da3c8c7b4c1c4eafe0dd2a87dd43f4bd0ab2c4cf3987a6
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":7,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DATE
                k\tv
                min\t-292275055-05-16T16:47:04.193Z
                max\t292278994-08-17T07:12:55.807Z
                sentinel\t
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t686 sha256=aaff04c4cdb3760745da3c8c7b4c1c4eafe0dd2a87dd43f4bd0ab2c4cf3987a6
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":7,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DATE
                k\tv
                min\t-292275055-05-16T16:47:04.193Z
                max\t292278994-08-17T07:12:55.807Z
                sentinel\t
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t935 sha256=f2749867a98bc33026ef572a2c9861ff012251b34e9f1dbb36751d8d9ba46b4e
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":7,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:DATE c:INT
                k\tv\tc
                min\t-292275055-05-16T16:47:04.193Z\t7
                max\t292278994-08-17T07:12:55.807Z\t7
                sentinel\t\t7
                null\t\t7
                """);
        rec("TIMESTAMP", """
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","-290308-01-01T19:59:05.224193Z"\\u000d
                max\t"max","294247-01-10T04:00:54.775807Z"\\u000d
                sentinel\t"sentinel",\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t686 sha256=ed9d31a3b1d0d8143e0424e0a02fa2c99241610662725f725e7e99161d509e92
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":8,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:TIMESTAMP
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                max\t294247-01-10T04:00:54.775807Z
                sentinel\t
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t686 sha256=ed9d31a3b1d0d8143e0424e0a02fa2c99241610662725f725e7e99161d509e92
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":8,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:TIMESTAMP
                k\tv
                min\t-290308-01-01T19:59:05.224193Z
                max\t294247-01-10T04:00:54.775807Z
                sentinel\t
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t935 sha256=8344a6902037b8fd8dec330ddb1de40287574c4a7f11fb9cfca912f8f4343d3c
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":8,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:TIMESTAMP c:INT
                k\tv\tc
                min\t-290308-01-01T19:59:05.224193Z\t7
                max\t294247-01-10T04:00:54.775807Z\t7
                sentinel\t\t7
                null\t\t7
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"TIMESTAMP"}],"timestamp":-1,"dataset":[
                min\t["min","-290308-01-01T19:59:05.224193Z"]
                max\t["max","294247-01-10T04:00:54.775807Z"]
                sentinel\t["sentinel",null]
                null\t["null",null]
                tail\t],"count":4}
                """);
        rec("FLOAT", """
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"FLOAT"}],"timestamp":-1,"dataset":[
                min\t["min",-3.4028235E38]
                max\t["max",3.4028235E38]
                nan\t["nan",null]
                literal_inf\t["literal_inf",null]
                negzero\t["negzero",-0.0]
                null\t["null",null]
                inf\t["inf",null]
                ninf\t["ninf",null]
                tail\t],"count":8}
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t657 sha256=ba4abf70d25466cbadfcad81e7241beb38ccdcc2f99b96882dcabe95de0233e0
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":9,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:FLOAT
                k\tv
                min\t-3.4028235E38
                max\t3.4028235E38
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t657 sha256=ba4abf70d25466cbadfcad81e7241beb38ccdcc2f99b96882dcabe95de0233e0
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":9,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:FLOAT
                k\tv
                min\t-3.4028235E38
                max\t3.4028235E38
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t909 sha256=5bfeb89f00d638ea34a0cc0db6866e493bcab2b7a0bbd41b847d25ace9c6477f
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":9,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:FLOAT c:INT
                k\tv\tc
                min\t-3.4028235E38\t7
                max\t3.4028235E38\t7
                nan\tnull\t7
                literal_inf\tnull\t7
                negzero\t-0.0\t7
                null\tnull\t7
                inf\tnull\t7
                ninf\tnull\t7
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min",-3.4028235E38\\u000d
                max\t"max",3.4028235E38\\u000d
                nan\t"nan",\\u000d
                literal_inf\t"literal_inf",\\u000d
                negzero\t"negzero",-0.0\\u000d
                null\t"null",\\u000d
                inf\t"inf",\\u000d
                ninf\t"ninf",\\u000d
                eof\tnewline
                """);
        rec("DOUBLE", """
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min",-1.7976931348623157E308\\u000d
                max\t"max",1.7976931348623157E308\\u000d
                nan\t"nan",\\u000d
                literal_inf\t"literal_inf",\\u000d
                negzero\t"negzero",-0.0\\u000d
                null\t"null",\\u000d
                inf\t"inf",\\u000d
                ninf\t"ninf",\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t704 sha256=50f3e7fa6f54cc2736b39f548866a6d333a7f7ae397d8f5dc6db84fee9ed6b49
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":10,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DOUBLE
                k\tv
                min\t-1.7976931348623157E308
                max\t1.7976931348623157E308
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t704 sha256=50f3e7fa6f54cc2736b39f548866a6d333a7f7ae397d8f5dc6db84fee9ed6b49
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":10,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DOUBLE
                k\tv
                min\t-1.7976931348623157E308
                max\t1.7976931348623157E308
                nan\tnull
                literal_inf\tnull
                negzero\t-0.0
                null\tnull
                inf\tnull
                ninf\tnull
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t956 sha256=a86fdfc9419bbdc83b05eb2ba760039f771432054a86d75ea94ac897a154feaa
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":10,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:DOUBLE c:INT
                k\tv\tc
                min\t-1.7976931348623157E308\t7
                max\t1.7976931348623157E308\t7
                nan\tnull\t7
                literal_inf\tnull\t7
                negzero\t-0.0\t7
                null\tnull\t7
                inf\tnull\t7
                ninf\tnull\t7
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"DOUBLE"}],"timestamp":-1,"dataset":[
                min\t["min",-1.7976931348623157E308]
                max\t["max",1.7976931348623157E308]
                nan\t["nan",null]
                literal_inf\t["literal_inf",null]
                negzero\t["negzero",-0.0]
                null\t["null",null]
                inf\t["inf",null]
                ninf\t["ninf",null]
                tail\t],"count":8}
                """);
        rec("STRING", """
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                empty\t"empty",""\\u000d
                min\t"min"," "\\u000d
                max\t"max","ü€😀�"\\u000d
                escape\t"escape","a""b,c\\\\d'e"\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"STRING"}],"timestamp":-1,"dataset":[
                empty\t["empty",""]
                min\t["min"," "]
                max\t["max","ü€😀�"]
                escape\t["escape","a\\"b,c\\\\d'e"]
                null\t["null",null]
                tail\t],"count":5}
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t691 sha256=28466d6e23fb014e29d75561edf5b4606783584a1b0ba6519b680ee63e22e018
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":11,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:STRING
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t691 sha256=28466d6e23fb014e29d75561edf5b4606783584a1b0ba6519b680ee63e22e018
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":11,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:STRING
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t940 sha256=aacbfc8d6fb0e639b41877e06df5465dbbb944bd657cb05bbb8214e3c326749d
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":11,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:STRING c:INT
                k\tv\tc
                empty\t\t7
                min\t \t7
                max\tü€😀�\t7
                escape\ta"b,c\\d'e\t7
                null\t\t7
                """);
        rec("SYMBOL", """
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t742 sha256=0001c0efb819922c732fa94869d6edf55e2e45fd9883d0cfbc1daec9f53d4df2
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":12,"column_top":0,"format":1,"id":1}]}
                columns\tk:VARCHAR v:VARCHAR
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t691 sha256=28466d6e23fb014e29d75561edf5b4606783584a1b0ba6519b680ee63e22e018
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":11,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:STRING
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t991 sha256=38b6f886d8756b0dd36a3f87fd06d0872aeeaa394e0967ba9647cbc207b4792d
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":12,"column_top":0,"format":1,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:VARCHAR c:INT
                k\tv\tc
                empty\t\t7
                min\t \t7
                max\tü€😀�\t7
                escape\ta"b,c\\d'e\t7
                null\t\t7
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                empty\t"empty",""\\u000d
                min\t"min"," "\\u000d
                max\t"max","ü€😀�"\\u000d
                escape\t"escape","a""b,c\\\\d'e"\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"SYMBOL"}],"timestamp":-1,"dataset":[
                empty\t["empty",""]
                min\t["min"," "]
                max\t["max","ü€😀�"]
                escape\t["escape","a\\"b,c\\\\d'e"]
                null\t["null",null]
                tail\t],"count":5}
                """);
        rec("LONG256", """
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t878 sha256=c6fe3f4ee40430a3176a11855d10c0663c2e5fec8656cd80a83b2d714d0dfaef
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":13,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:LONG256
                k\tv
                min\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\t
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t878 sha256=c6fe3f4ee40430a3176a11855d10c0663c2e5fec8656cd80a83b2d714d0dfaef
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":13,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:LONG256
                k\tv
                min\t0x00
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
                sentinel\t
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t1127 sha256=f4fcb7e8e2c42bf42c52f5f5f161c0cc984b6296bda1661239be69f2ae858033
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":13,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:LONG256 c:INT
                k\tv\tc
                min\t0x00\t7
                max\t0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\t7
                sentinel\t\t7
                null\t\t7
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min",0x00\\u000d
                max\t"max",0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\\u000d
                sentinel\t"sentinel",\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"LONG256"}],"timestamp":-1,"dataset":[
                min\t["min","0x00"]
                max\t["max","0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"]
                sentinel\t["sentinel",""]
                null\t["null",""]
                tail\t],"count":4}
                """);
        rec("GEOBYTE", """
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","0000000"\\u000d
                max\t"max","1111111"\\u000d
                null\t"null",null\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t620 sha256=3dab8ea159c5daf2bd3f50eabcd51bd739fe3ffdf0ae95d60aa69c69ed4d73fe
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":67342,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(7b)
                k\tv
                min\t0000000
                max\t1111111
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t620 sha256=3dab8ea159c5daf2bd3f50eabcd51bd739fe3ffdf0ae95d60aa69c69ed4d73fe
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":67342,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(7b)
                k\tv
                min\t0000000
                max\t1111111
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t870 sha256=ca726a5f3544ffa020ec8a26009f0d58326f94bdcbe28b9b96f3e7bc2857eba8
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":67342,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:GEOHASH(7b) c:INT
                k\tv\tc
                min\t0000000\t7
                max\t1111111\t7
                null\t\t7
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"GEOHASH(7b)"}],"timestamp":-1,"dataset":[
                min\t["min","0000000"]
                max\t["max","1111111"]
                null\t["null",null]
                tail\t],"count":3}
                """);
        rec("GEOSHORT", """
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t620 sha256=da4759adc81456044bde1f78ec599b2344e56566c69e045f0506686e6c0837e2
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":69391,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(3c)
                k\tv
                min\t000
                max\tzzz
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t620 sha256=da4759adc81456044bde1f78ec599b2344e56566c69e045f0506686e6c0837e2
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":69391,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(3c)
                k\tv
                min\t000
                max\tzzz
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t870 sha256=c08b586e064e3e27d7c7668abae5a9d75f03b05f5f259fd543d5d2adf7af6d5e
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":69391,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:GEOHASH(3c) c:INT
                k\tv\tc
                min\t000\t7
                max\tzzz\t7
                null\t\t7
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","000"\\u000d
                max\t"max","zzz"\\u000d
                null\t"null",null\\u000d
                eof\tnewline
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"GEOHASH(3c)"}],"timestamp":-1,"dataset":[
                min\t["min","000"]
                max\t["max","zzz"]
                null\t["null",null]
                tail\t],"count":3}
                """);
        rec("GEOINT", """
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t620 sha256=c9f33a4765f67d9cf5e7d22ba32ac41dde86045b59fe6870c636768eaa03870a
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":73232,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(6c)
                k\tv
                min\t000000
                max\tzzzzzz
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t620 sha256=c9f33a4765f67d9cf5e7d22ba32ac41dde86045b59fe6870c636768eaa03870a
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":73232,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(6c)
                k\tv
                min\t000000
                max\tzzzzzz
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t870 sha256=468f3d80c2f94ca03f9b86ee636f6603e49d47129d5bb6a410f2a99506f7466b
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":73232,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:GEOHASH(6c) c:INT
                k\tv\tc
                min\t000000\t7
                max\tzzzzzz\t7
                null\t\t7
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","000000"\\u000d
                max\t"max","zzzzzz"\\u000d
                null\t"null",null\\u000d
                eof\tnewline
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"GEOHASH(6c)"}],"timestamp":-1,"dataset":[
                min\t["min","000000"]
                max\t["max","zzzzzz"]
                null\t["null",null]
                tail\t],"count":3}
                """);
        rec("GEOLONG", """
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t656 sha256=e3d2140dde93276163906d5fbc39674a23db6d81ae12dfad379df67b7ff844c2
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":75793,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(8c)
                k\tv
                min\t00000000
                max\tzzzzzzzz
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t656 sha256=e3d2140dde93276163906d5fbc39674a23db6d81ae12dfad379df67b7ff844c2
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":75793,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(8c)
                k\tv
                min\t00000000
                max\tzzzzzzzz
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t906 sha256=cd4db7ef386577b4e64cb6ea7402c4fce9743dfd3feeda8dd7d7f156dc565cd0
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":75793,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:GEOHASH(8c) c:INT
                k\tv\tc
                min\t00000000\t7
                max\tzzzzzzzz\t7
                null\t\t7
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"GEOHASH(8c)"}],"timestamp":-1,"dataset":[
                min\t["min","00000000"]
                max\t["max","zzzzzzzz"]
                null\t["null",null]
                tail\t],"count":3}
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","00000000"\\u000d
                max\t"max","zzzzzzzz"\\u000d
                null\t"null",null\\u000d
                eof\tnewline
                """);
        rec("BINARY", """
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                empty\t"empty",\\u000d
                max\t"max",\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"BINARY"}],"timestamp":-1,"dataset":[
                empty\t["empty",[]]
                max\t["max",[]]
                null\t["null",[]]
                tail\t],"count":3}
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t620 sha256=3d5020254a29a103f2c4dfb78c588282d01e40d69131d4445448b6fcb82d05d6
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":18,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:BINARY
                k\tv
                empty\t
                max\t00000000 00 01 02 fd fe ff
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t620 sha256=3d5020254a29a103f2c4dfb78c588282d01e40d69131d4445448b6fcb82d05d6
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":18,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:BINARY
                k\tv
                empty\t
                max\t00000000 00 01 02 fd fe ff
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t870 sha256=0f8c8e0a085172a747bf27e4ca302a2600b4f6274d78fe764f5cd84b0c44f55b
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":18,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:BINARY c:INT
                k\tv\tc
                empty\t\t7
                max\t00000000 00 01 02 fd fe ff\t7
                null\t\t7
                """);
        rec("UUID", """
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min",00000000-0000-0000-0000-000000000000\\u000d
                max\t"max",ffffffff-ffff-ffff-ffff-ffffffffffff\\u000d
                sentinel\t"sentinel",\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"UUID"}],"timestamp":-1,"dataset":[
                min\t["min","00000000-0000-0000-0000-000000000000"]
                max\t["max","ffffffff-ffff-ffff-ffff-ffffffffffff"]
                sentinel\t["sentinel",null]
                null\t["null",null]
                tail\t],"count":4}
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t750 sha256=d7fecfe5e2bbdb10b4af648459b07762b94dae650f07ce67667e6d1b8ebc57f6
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":19,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:UUID
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t750 sha256=d7fecfe5e2bbdb10b4af648459b07762b94dae650f07ce67667e6d1b8ebc57f6
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":19,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:UUID
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t999 sha256=58c33bce5dea432885a72a6b040e0701926001bf103e23d1973809db729f4d7e
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":19,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:UUID c:INT
                k\tv\tc
                min\t00000000-0000-0000-0000-000000000000\t7
                max\tffffffff-ffff-ffff-ffff-ffffffffffff\t7
                sentinel\t\t7
                null\t\t7
                """);
        rec("LONG128", """
                ## http.csv
                status\t400 Bad request
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                {"query":"SELECT k\t{"query":"SELECT k, v FROM t","error":"[-1] column type not supported [column=v, type=LONG128]","position":0}
                eof\tno newline
                ## http.json
                status\t400 Bad request
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                body\t{"query":"SELECT k, v FROM t","error":"column type not supported [column=v, type=LONG128]","position":0}
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t746 sha256=4ccb961d072bbe6d2290fbeded93ab22144e98ecbf6109eff6f6cb5bd64d9b76
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":24,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:LONG128
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t746 sha256=4ccb961d072bbe6d2290fbeded93ab22144e98ecbf6109eff6f6cb5bd64d9b76
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":24,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:LONG128
                k\tv
                min\t00000000-0000-0000-0000-000000000000
                max\tffffffff-ffff-ffff-ffff-ffffffffffff
                sentinel\t
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t995 sha256=356f1dc44aa85824e6175071f238f8fe53270fef06626297952d4650b8619b8b
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":24,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:LONG128 c:INT
                k\tv\tc
                min\t00000000-0000-0000-0000-000000000000\t7
                max\tffffffff-ffff-ffff-ffff-ffffffffffff\t7
                sentinel\t\t7
                null\t\t7
                """);
        rec("IPv4", """
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min",0.0.0.1\\u000d
                max\t"max",255.255.255.255\\u000d
                sentinel\t"sentinel",\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t647 sha256=ded8c26b5d0f4b4fe897c27cfa4cb0179893e1942273036a632ac63c1a62dc51
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":25,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:IPv4
                k\tv
                min\t0.0.0.1
                max\t255.255.255.255
                sentinel\t
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t647 sha256=ded8c26b5d0f4b4fe897c27cfa4cb0179893e1942273036a632ac63c1a62dc51
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":25,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:IPv4
                k\tv
                min\t0.0.0.1
                max\t255.255.255.255
                sentinel\t
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t896 sha256=6ad93af7ca6683d4a418e67ab29b1eafd7d6a2135acf7b2d9336df824df1e1f3
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":25,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:IPv4 c:INT
                k\tv\tc
                min\t0.0.0.1\t7
                max\t255.255.255.255\t7
                sentinel\t\t7
                null\t\t7
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"IPv4"}],"timestamp":-1,"dataset":[
                min\t["min","0.0.0.1"]
                max\t["max","255.255.255.255"]
                sentinel\t["sentinel",null]
                null\t["null",null]
                tail\t],"count":4}
                """);
        rec("VARCHAR", """
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"VARCHAR"}],"timestamp":-1,"dataset":[
                empty\t["empty",""]
                min\t["min"," "]
                max\t["max","ü€😀�"]
                escape\t["escape","a\\"b,c\\\\d'e"]
                null\t["null",null]
                tail\t],"count":5}
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t691 sha256=5a760909b45fadf9de0b835d7b04d87ed96a7c52c94dcd47eea7568d535bf4d2
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":26,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:VARCHAR
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t691 sha256=5a760909b45fadf9de0b835d7b04d87ed96a7c52c94dcd47eea7568d535bf4d2
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":26,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:VARCHAR
                k\tv
                empty\t
                min\t\s
                max\tü€😀�
                escape\ta"b,c\\d'e
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t940 sha256=9f4c55f6d52bf8eb80f7d4d84495f7944f477dff35f3cc8e2f46ca05881f85c7
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":26,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:VARCHAR c:INT
                k\tv\tc
                empty\t\t7
                min\t \t7
                max\tü€😀�\t7
                escape\ta"b,c\\d'e\t7
                null\t\t7
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                empty\t"empty",""\\u000d
                min\t"min"," "\\u000d
                max\t"max","ü€😀�"\\u000d
                escape\t"escape","a""b,c\\\\d'e"\\u000d
                null\t"null",\\u000d
                eof\tnewline
                """);
        rec("DOUBLE[]", """
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t632 sha256=92cfdb897da5ba42d5f14ea3bae4cdf8ddd718231e8d7eb5f214db4e26945304
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":2587,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DOUBLE[]
                k\tv
                min\t[-1.7976931348623157E308]
                max\t[1.7976931348623157E308]
                empty\t[null]
                specials\t[null,null,null,-0.0]
                null\tnull
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t632 sha256=92cfdb897da5ba42d5f14ea3bae4cdf8ddd718231e8d7eb5f214db4e26945304
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":2587,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DOUBLE[]
                k\tv
                min\t[-1.7976931348623157E308]
                max\t[1.7976931348623157E308]
                empty\t[null]
                specials\t[null,null,null,-0.0]
                null\tnull
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t853 sha256=ef97fb2ee250e68d8f855710e69bacf6b90798fd325cb951bb8c1801ca5b157a
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":2587,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:DOUBLE[] c:INT
                k\tv\tc
                min\t[-1.7976931348623157E308]\t7
                max\t[1.7976931348623157E308]\t7
                empty\t[null]\t7
                specials\t[null,null,null,-0.0]\t7
                null\tnull\t7
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","[-1.7976931348623157E308]"\\u000d
                max\t"max","[1.7976931348623157E308]"\\u000d
                empty\t"empty","[]"\\u000d
                specials\t"specials","[null,null,null,-0.0]"\\u000d
                null\t"null","null"\\u000d
                eof\tnewline
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"ARRAY","dim":1,"elemType":"DOUBLE"}],"timestamp":-1,"dataset":[
                min\t["min",[-1.7976931348623157E308]]
                max\t["max",[1.7976931348623157E308]]
                empty\t["empty",[]]
                specials\t["specials",[null,null,null,-0.0]]
                null\t["null",null]
                tail\t],"count":5}
                """);
        rec("DECIMAL8", """
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t598 sha256=e8cb57c88035d07899e06bf6446cf6aeb0213de4c8446db7bbd4ed7c4bb72b5a
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":262684,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(2,1)
                k\tv
                min\t-9.9
                max\t9.9
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t598 sha256=e8cb57c88035d07899e06bf6446cf6aeb0213de4c8446db7bbd4ed7c4bb72b5a
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":262684,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(2,1)
                k\tv
                min\t-9.9
                max\t9.9
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t848 sha256=8460b73a234a2d2cf9d96da5adf8f6b3c0929fade2fd9dd2ac89eb1b622cf374
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":262684,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:DECIMAL(2,1) c:INT
                k\tv\tc
                min\t-9.9\t7
                max\t9.9\t7
                null\t\t7
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"DECIMAL(2,1)"}],"timestamp":-1,"dataset":[
                min\t["min","-9.9"]
                max\t["max","9.9"]
                null\t["null",null]
                tail\t],"count":3}
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","-9.9"\\u000d
                max\t"max","9.9"\\u000d
                null\t"null",\\u000d
                eof\tnewline
                """);
        rec("DECIMAL16", """
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t608 sha256=464dd18f1980c1f7fdb9e538efcfeee07ea8d3974ecc3432578ccf74787f84b8
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":525341,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(4,2)
                k\tv
                min\t-99.99
                max\t99.99
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t608 sha256=464dd18f1980c1f7fdb9e538efcfeee07ea8d3974ecc3432578ccf74787f84b8
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":525341,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(4,2)
                k\tv
                min\t-99.99
                max\t99.99
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t858 sha256=52b2fc93aecb586bf8179d9bca9c4aef9058ba77a55de71c1c4358f26319c71e
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":525341,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:DECIMAL(4,2) c:INT
                k\tv\tc
                min\t-99.99\t7
                max\t99.99\t7
                null\t\t7
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","-99.99"\\u000d
                max\t"max","99.99"\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"DECIMAL(4,2)"}],"timestamp":-1,"dataset":[
                min\t["min","-99.99"]
                max\t["max","99.99"]
                null\t["null",null]
                tail\t],"count":3}
                """);
        rec("DECIMAL32", """
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"DECIMAL(9,0)"}],"timestamp":-1,"dataset":[
                min\t["min","-999999999"]
                max\t["max","999999999"]
                null\t["null",null]
                tail\t],"count":3}
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t626 sha256=7b65d1c203fb98b9a0fdf798acce64308bb1d0c74711bbab8377cd14cfe02931
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":2334,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(9,0)
                k\tv
                min\t-999999999
                max\t999999999
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t626 sha256=7b65d1c203fb98b9a0fdf798acce64308bb1d0c74711bbab8377cd14cfe02931
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":2334,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(9,0)
                k\tv
                min\t-999999999
                max\t999999999
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t876 sha256=000141d27c51f2e4009cc43039657613db69fa26020d41141dad9b412ff6815c
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":2334,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:DECIMAL(9,0) c:INT
                k\tv\tc
                min\t-999999999\t7
                max\t999999999\t7
                null\t\t7
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","-999999999"\\u000d
                max\t"max","999999999"\\u000d
                null\t"null",\\u000d
                eof\tnewline
                """);
        rec("DECIMAL64", """
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"DECIMAL(16,4)"}],"timestamp":-1,"dataset":[
                min\t["min","-999999999999.9999"]
                max\t["max","999999999999.9999"]
                null\t["null",null]
                tail\t],"count":3}
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","-999999999999.9999"\\u000d
                max\t"max","999999999999.9999"\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t673 sha256=68a8600723b97c930a860b13c9d6b12fc4d1093df7991af72b4352efa6918c62
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":1052703,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(16,4)
                k\tv
                min\t-999999999999.9999
                max\t999999999999.9999
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t673 sha256=68a8600723b97c930a860b13c9d6b12fc4d1093df7991af72b4352efa6918c62
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":1052703,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(16,4)
                k\tv
                min\t-999999999999.9999
                max\t999999999999.9999
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t923 sha256=e6ca7d0a6314967e95974702fb7445950a4dab8bda6534c2d1f670cb6d85e1eb
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":1052703,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:DECIMAL(16,4) c:INT
                k\tv\tc
                min\t-999999999999.9999\t7
                max\t999999999999.9999\t7
                null\t\t7
                """);
        rec("DECIMAL128", """
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","-9999999999999999999999999999.9999999999"\\u000d
                max\t"max","9999999999999999999999999999.9999999999"\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t755 sha256=099389cc6c2e89e976c07bcbce3e201a1b26cd37192d60c3dfe19b696bf40785
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":2631200,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(38,10)
                k\tv
                min\t-9999999999999999999999999999.9999999999
                max\t9999999999999999999999999999.9999999999
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t755 sha256=099389cc6c2e89e976c07bcbce3e201a1b26cd37192d60c3dfe19b696bf40785
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":2631200,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(38,10)
                k\tv
                min\t-9999999999999999999999999999.9999999999
                max\t9999999999999999999999999999.9999999999
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t1005 sha256=77d3b86078f49c16e56780bdb1a5e7a367aa7965239c80018b220012f54d807f
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":2631200,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:DECIMAL(38,10) c:INT
                k\tv\tc
                min\t-9999999999999999999999999999.9999999999\t7
                max\t9999999999999999999999999999.9999999999\t7
                null\t\t7
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"DECIMAL(38,10)"}],"timestamp":-1,"dataset":[
                min\t["min","-9999999999999999999999999999.9999999999"]
                max\t["max","9999999999999999999999999999.9999999999"]
                null\t["null",null]
                tail\t],"count":3}
                """);
        rec("DECIMAL256", """
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","-99999999999999999999999999999999999999999999999999999999.99999999999999999999"\\u000d
                max\t"max","99999999999999999999999999999999999999999999999999999999.99999999999999999999"\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"DECIMAL(76,20)"}],"timestamp":-1,"dataset":[
                min\t["min","-99999999999999999999999999999999999999999999999999999999.99999999999999999999"]
                max\t["max","99999999999999999999999999999999999999999999999999999999.99999999999999999999"]
                null\t["null",null]
                tail\t],"count":3}
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t917 sha256=138b5909fc94644f0c1e3ba72ff64d04cf60e7181a7f5287117f6cd1de387dc6
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":5262369,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(76,20)
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t917 sha256=138b5909fc94644f0c1e3ba72ff64d04cf60e7181a7f5287117f6cd1de387dc6
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":5262369,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(76,20)
                k\tv
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t1167 sha256=584c30ddb491b5f7db6baf47c3a26988be359eeeaf2d6124acdc4fddb9717e71
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":5262369,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:DECIMAL(76,20) c:INT
                k\tv\tc
                min\t-99999999999999999999999999999999999999999999999999999999.99999999999999999999\t7
                max\t99999999999999999999999999999999999999999999999999999999.99999999999999999999\t7
                null\t\t7
                """);
        rec("INTERVAL", """
                ## http.csv
                error: create: [29] non-persisted type: INTERVAL
                ## http.json
                error: create: [29] non-persisted type: INTERVAL
                ## http.parquet
                error: create: [29] non-persisted type: INTERVAL
                ## http.parquet-cursor
                error: create: [29] non-persisted type: INTERVAL
                ## http.parquet-hybrid
                error: create: [29] non-persisted type: INTERVAL
                """);
        rec("VARCHAR_SLICE", """
                ## http.csv
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## http.parquet
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## http.parquet-cursor
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## http.parquet-hybrid
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## http.json
                error: create: [29] unsupported column type: VARCHAR_SLICE
                """);
        rec("TIMESTAMP_NS", """
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"TIMESTAMP_NS"}],"timestamp":-1,"dataset":[
                min\t["min","1677-01-01T00:12:43.145224193Z"]
                max\t["max","2262-04-11T23:47:16.854775807Z"]
                sentinel\t["sentinel",null]
                null\t["null",null]
                tail\t],"count":4}
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","1677-01-01T00:12:43.145224193Z"\\u000d
                max\t"max","2262-04-11T23:47:16.854775807Z"\\u000d
                sentinel\t"sentinel",\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t689 sha256=d2b22c5eb53c0edb3dba6ee4cd239729c9c1204bcaec3e6dc51a8a112fb36966
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":262152,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:TIMESTAMP_NS
                k\tv
                min\t1677-01-01T00:12:43.145224193Z
                max\t2262-04-11T23:47:16.854775807Z
                sentinel\t
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t689 sha256=d2b22c5eb53c0edb3dba6ee4cd239729c9c1204bcaec3e6dc51a8a112fb36966
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":262152,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:TIMESTAMP_NS
                k\tv
                min\t1677-01-01T00:12:43.145224193Z
                max\t2262-04-11T23:47:16.854775807Z
                sentinel\t
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t938 sha256=9e601136a101c801b9672154a1c6787e4477c599d41a9102899bf27c22209449
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":262152,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:TIMESTAMP_NS c:INT
                k\tv\tc
                min\t1677-01-01T00:12:43.145224193Z\t7
                max\t2262-04-11T23:47:16.854775807Z\t7
                sentinel\t\t7
                null\t\t7
                """);
        rec("GEOHASH(1c)", """
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","0"\\u000d
                max\t"max","z"\\u000d
                null\t"null",null\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t620 sha256=aa5188802dc1617c3d53f7f6a97747a389f60027449b63a972ffc17e6a710b4f
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":66830,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(1c)
                k\tv
                min\t0
                max\tz
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t620 sha256=aa5188802dc1617c3d53f7f6a97747a389f60027449b63a972ffc17e6a710b4f
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":66830,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(1c)
                k\tv
                min\t0
                max\tz
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t870 sha256=fd023bc3621723e98c88c30e7e817f6e31f6eae785a83181fdc887e315aab24f
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":66830,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:GEOHASH(1c) c:INT
                k\tv\tc
                min\t0\t7
                max\tz\t7
                null\t\t7
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"GEOHASH(1c)"}],"timestamp":-1,"dataset":[
                min\t["min","0"]
                max\t["max","z"]
                null\t["null",null]
                tail\t],"count":3}
                """);
        rec("GEOHASH(8b)", """
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t620 sha256=66d481a42776360f232a383757327016c70a413a98b34b3b68f89c68f5a7bece
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":67599,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(8b)
                k\tv
                min\t00000000
                max\t11111111
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t620 sha256=66d481a42776360f232a383757327016c70a413a98b34b3b68f89c68f5a7bece
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":67599,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(8b)
                k\tv
                min\t00000000
                max\t11111111
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t870 sha256=092ad6f01a9ad67bbefd12d9c4425a95860020bfb0b906082e73c2784e03dcfc
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":67599,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:GEOHASH(8b) c:INT
                k\tv\tc
                min\t00000000\t7
                max\t11111111\t7
                null\t\t7
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"GEOHASH(8b)"}],"timestamp":-1,"dataset":[
                min\t["min","00000000"]
                max\t["max","11111111"]
                null\t["null",null]
                tail\t],"count":3}
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","00000000"\\u000d
                max\t"max","11111111"\\u000d
                null\t"null",null\\u000d
                eof\tnewline
                """);
        rec("GEOHASH(31b)", """
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t620 sha256=7dc4eecd2b35dcd924ecb541ec6b8fc5e834b8640654a89b4ef69409fc2f8cdf
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":73488,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(31b)
                k\tv
                min\t0000000000000000000000000000000
                max\t1111111111111111111111111111111
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t620 sha256=7dc4eecd2b35dcd924ecb541ec6b8fc5e834b8640654a89b4ef69409fc2f8cdf
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":73488,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(31b)
                k\tv
                min\t0000000000000000000000000000000
                max\t1111111111111111111111111111111
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t870 sha256=0658625fdbc78593b73f18e85d0049d6f1a20e01cf71798ca7e91238b7411f98
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":73488,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:GEOHASH(31b) c:INT
                k\tv\tc
                min\t0000000000000000000000000000000\t7
                max\t1111111111111111111111111111111\t7
                null\t\t7
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","0000000000000000000000000000000"\\u000d
                max\t"max","1111111111111111111111111111111"\\u000d
                null\t"null",null\\u000d
                eof\tnewline
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"GEOHASH(31b)"}],"timestamp":-1,"dataset":[
                min\t["min","0000000000000000000000000000000"]
                max\t["max","1111111111111111111111111111111"]
                null\t["null",null]
                tail\t],"count":3}
                """);
        rec("GEOHASH(12c)", """
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"GEOHASH(12c)"}],"timestamp":-1,"dataset":[
                min\t["min","000000000000"]
                max\t["max","zzzzzzzzzzzz"]
                null\t["null",null]
                tail\t],"count":3}
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","000000000000"\\u000d
                max\t"max","zzzzzzzzzzzz"\\u000d
                null\t"null",null\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t656 sha256=eeb0e5472407175498917d26e502d88ad2d6172f2fb78ef2eee20a4a7792c3a6
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":80913,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(12c)
                k\tv
                min\t000000000000
                max\tzzzzzzzzzzzz
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t656 sha256=eeb0e5472407175498917d26e502d88ad2d6172f2fb78ef2eee20a4a7792c3a6
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":80913,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:GEOHASH(12c)
                k\tv
                min\t000000000000
                max\tzzzzzzzzzzzz
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t906 sha256=f7862cb90f02f245cb6bccd6895a75ef693a1dfbccd9fa33cda608e5c0127e42
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":80913,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:GEOHASH(12c) c:INT
                k\tv\tc
                min\t000000000000\t7
                max\tzzzzzzzzzzzz\t7
                null\t\t7
                """);
        rec("DECIMAL(5,2)", """
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"DECIMAL(5,2)"}],"timestamp":-1,"dataset":[
                min\t["min","-999.99"]
                max\t["max","999.99"]
                null\t["null",null]
                tail\t],"count":3}
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","-999.99"\\u000d
                max\t"max","999.99"\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t628 sha256=04f8e08a5bb8f24d6c61f950cfdf3345dd131c7511fee979e10f843fc9785cd5
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":525598,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(5,2)
                k\tv
                min\t-999.99
                max\t999.99
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t628 sha256=04f8e08a5bb8f24d6c61f950cfdf3345dd131c7511fee979e10f843fc9785cd5
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":525598,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(5,2)
                k\tv
                min\t-999.99
                max\t999.99
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t878 sha256=44e652f4923fcc8c7c6874cb18606d9fee9720d525d4946ad3f524d10bbf322f
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":525598,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:DECIMAL(5,2) c:INT
                k\tv\tc
                min\t-999.99\t7
                max\t999.99\t7
                null\t\t7
                """);
        rec("DECIMAL(18,3)", """
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t672 sha256=0592f0e18e88365fc5b8ca98fa9cbe5e2d98b364aa3074b85dbdfd944a27626b
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":791071,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(18,3)
                k\tv
                min\t-999999999999999.999
                max\t999999999999999.999
                null\t
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t672 sha256=0592f0e18e88365fc5b8ca98fa9cbe5e2d98b364aa3074b85dbdfd944a27626b
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":791071,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DECIMAL(18,3)
                k\tv
                min\t-999999999999999.999
                max\t999999999999999.999
                null\t
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t922 sha256=41fd9fc0a24fe9d294397539d503f18102a541fa264b052ab98f04f099261aae
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":791071,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:DECIMAL(18,3) c:INT
                k\tv\tc
                min\t-999999999999999.999\t7
                max\t999999999999999.999\t7
                null\t\t7
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","-999999999999999.999"\\u000d
                max\t"max","999999999999999.999"\\u000d
                null\t"null",\\u000d
                eof\tnewline
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"DECIMAL(18,3)"}],"timestamp":-1,"dataset":[
                min\t["min","-999999999999999.999"]
                max\t["max","999999999999999.999"]
                null\t["null",null]
                tail\t],"count":3}
                """);
        rec("DOUBLE[][]", """
                ## http.csv
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: text/csv; charset=utf-8
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.csv"
                header\tKeep-Alive: timeout=5, max=10000
                k\t"k","v"\\u000d
                min\t"min","[[-1.7976931348623157E308]]"\\u000d
                max\t"max","[[1.7976931348623157E308]]"\\u000d
                empty\t"empty","[]"\\u000d
                specials\t"specials","[[null,null,null,-0.0]]"\\u000d
                null\t"null","null"\\u000d
                eof\tnewline
                ## http.parquet
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t692 sha256=363c44a49e99e40f8211fba130466af309543d93d8a73f4a5a68d6211a501def
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":18971,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DOUBLE[][]
                k\tv
                min\t[[-1.7976931348623157E308]]
                max\t[[1.7976931348623157E308]]
                empty\t[[null]]
                specials\t[[null,null,null,-0.0]]
                null\tnull
                ## http.parquet-cursor
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t692 sha256=363c44a49e99e40f8211fba130466af309543d93d8a73f4a5a68d6211a501def
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":18971,"column_top":0,"id":1}]}
                columns\tk:VARCHAR v:DOUBLE[][]
                k\tv
                min\t[[-1.7976931348623157E308]]
                max\t[[1.7976931348623157E308]]
                empty\t[[null]]
                specials\t[[null,null,null,-0.0]]
                null\tnull
                ## http.parquet-hybrid
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/vnd.apache.parquet
                header\tContent-Disposition: attachment; filename="questdb-query-<clock>.parquet"
                header\tKeep-Alive: timeout=5, max=10000
                bytes\t913 sha256=668b8e8e2f687a1eccae1c9466fc0868276e1f252a21c8a58cd457c6fa73d991
                created_by\tQuestDB version 9.0
                questdb\t{"version":1,"schema":[{"column_type":26,"column_top":0,"id":0},{"column_type":18971,"column_top":0,"id":1},{"column_type":5,"column_top":0,"id":2}]}
                columns\tk:VARCHAR v:DOUBLE[][] c:INT
                k\tv\tc
                min\t[[-1.7976931348623157E308]]\t7
                max\t[[1.7976931348623157E308]]\t7
                empty\t[[null]]\t7
                specials\t[[null,null,null,-0.0]]\t7
                null\tnull\t7
                ## http.json
                status\t200 OK
                header\tServer: questDB/1.0
                header\tDate: <date>
                header\tTransfer-Encoding: chunked
                header\tContent-Type: application/json; charset=utf-8
                header\tKeep-Alive: timeout=5, max=10000
                head\t{"query":"SELECT k, v FROM t","columns":[{"name":"k","type":"VARCHAR"},{"name":"v","type":"ARRAY","dim":2,"elemType":"DOUBLE"}],"timestamp":-1,"dataset":[
                min\t["min",[[-1.7976931348623157E308]]]
                max\t["max",[[1.7976931348623157E308]]]
                empty\t["empty",[]]
                specials\t["specials",[[null,null,null,-0.0]]]
                null\t["null",null]
                tail\t],"count":5}
                """);
        rec("INTERVAL(us)", """
                ## http.csv
                error: create: [29] non-persisted type: INTERVAL
                ## http.json
                error: create: [29] non-persisted type: INTERVAL
                ## http.parquet
                error: create: [29] non-persisted type: INTERVAL
                ## http.parquet-cursor
                error: create: [29] non-persisted type: INTERVAL
                ## http.parquet-hybrid
                error: create: [29] non-persisted type: INTERVAL
                """);
        rec("INTERVAL(ns)", """
                ## http.csv
                error: create: [29] non-persisted type: INTERVAL
                ## http.json
                error: create: [29] non-persisted type: INTERVAL
                ## http.parquet
                error: create: [29] non-persisted type: INTERVAL
                ## http.parquet-cursor
                error: create: [29] non-persisted type: INTERVAL
                ## http.parquet-hybrid
                error: create: [29] non-persisted type: INTERVAL
                """);
    }
    // recordings: end

    private static void rec(String label, String recording) {
        RECORDINGS.put(label, recording);
    }
}
