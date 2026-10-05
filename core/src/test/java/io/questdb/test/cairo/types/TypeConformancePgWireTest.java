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

import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.cutlass.pgwire.BasePGTest;
import io.questdb.test.cutlass.pgwire.PGHexScripts;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * The PostgreSQL wire part of the conformance kit (User Story 4): every kit type's value rows
 * as the server sends them, in text format (simple query, path {@code pg.text}) and in binary
 * format (extended query with binary results, path {@code pg.binary}).
 * <p>
 * The recording holds the server's answer to the query, one message per line: DataRow
 * messages are labelled by their value row (column k), every other message by its type byte.
 * The answer to the startup and password messages is the same for every type, so it is
 * recorded once ({@link #AUTH_REQUEST}, {@link #READY}) and asserted in every test. The test
 * captures the answer with {@link PGHexScripts#exchange}, compares it with the recording
 * section by section ({@link TypeConformanceRecording}), and then replays the whole
 * conversation, client messages generated and server bytes recorded, as a hex script through
 * {@link PGHexScripts#playScript} on a fresh connection.
 * <p>
 * One mode: reads over the PG wire do not depend on WAL, partitioning or write order, which
 * the storage part covers. Types registered later run where their resource line lists
 * {@code pg.text} or {@code pg.binary}; {@link TypeConformanceInvariants} checks them: on
 * {@code pg.binary} every value must travel as its stored bits (big-endian, the type's width)
 * or, for a var-size type, as its accessor family's bytes, and the NULL row and the
 * sentinel-pattern row must behave as the NULL policy says; on {@code pg.text} only the
 * SENTINEL and BITMAP comparisons of those two rows are checked, because the kit does not
 * derive a later type's text form.
 * <p>
 * Masks: none. The server that {@code createPGServer(configuration, true)} starts sends a fixed
 * process id and secret key in BackendKeyData, and no other message carries a per-run value.
 */
@RunWith(Parameterized.class)
public class TypeConformancePgWireTest extends BasePGTest {
    // the server's answer to the startup message: AuthenticationCleartextPassword
    private static final String AUTH_REQUEST = "520000000800000003";
    private static final Log LOG = LogFactory.getLog(TypeConformancePgWireTest.class);
    private static final String MODE = "nonwal-day";
    private static final String PASSWORD = PGHexScripts.passwordMessage("quest");
    // the server's answer to the password, recorded at S12: AuthenticationOk; ParameterStatus
    // TimeZone=GMT, application_name=QuestDB, server_version=11.3, integer_datetimes=on,
    // client_encoding=UTF8; BackendKeyData with the fixed process id and secret key;
    // ReadyForQuery idle
    private static final String READY = "520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549";
    private static final Map<String, String> RECORDINGS = new HashMap<>();
    private static final String SQL = "SELECT k, v FROM t";
    private static final String STARTUP = PGHexScripts.startupMessage("admin", "qdb");
    private final ObjList<TypeConformanceValues.Row> rows;
    private final TypeConformanceTypes.Entry type;

    public TypeConformancePgWireTest(String label) {
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

    @Test
    public void testBinary() throws Exception {
        assertPath("pg.binary", PGHexScripts.extendedQueryMessages(SQL, 1));
    }

    @Test
    public void testText() throws Exception {
        assertPath("pg.text", PGHexScripts.queryMessage(SQL));
    }

    private static String decodeText(String hex) {
        final byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * The columns of a DataRow message (hex) as hex, null for a NULL column.
     */
    private static String[] dataRowColumns(String message) {
        final int columnCount = Integer.parseInt(message.substring(10, 14), 16);
        final String[] columns = new String[columnCount];
        int pos = 14;
        for (int c = 0; c < columnCount; c++) {
            final int len = (int) Long.parseLong(message.substring(pos, pos + 8), 16);
            pos += 8;
            if (len == -1) {
                columns[c] = null;
            } else {
                columns[c] = message.substring(pos, pos + 2 * len);
                pos += 2 * len;
            }
        }
        return columns;
    }

    /**
     * A binary value of a type registered later in the form its value rows hold: a fixed-size
     * value's big-endian bits, the type's width, checked; a var-size value by its accessor
     * family, where STRING and VARCHAR send their UTF-8 bytes.
     */
    private static long[] decodeBinary(TypeConformanceTypes.Entry type, TypeConformanceValues.Row row, String hex) {
        if (row.family == null) {
            if (hex.length() != 2 * row.width) {
                Assert.fail(TypeConformanceInvariants.context(type, row.label, "pg.binary", MODE)
                        + ": the binary value is not the type's " + row.width + " bytes: " + hex);
            }
            return decodeBigEndian(hex);
        }
        switch (row.family) {
            case STRING, VARCHAR -> {
                final byte[] bytes = new byte[hex.length() / 2];
                for (int i = 0; i < bytes.length; i++) {
                    bytes[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
                }
                return TypeConformanceValues.Row.pack(bytes);
            }
            default -> {
                Assert.fail(TypeConformanceInvariants.context(type, row.label, "pg.binary", MODE)
                        + ": the kit reads no binary value of accessor family " + row.family);
                return null;
            }
        }
    }

    private static long[] decodeBigEndian(String hex) {
        final long[] bits = new long[4];
        final int width = hex.length() / 2;
        for (int i = 0; i < width; i++) {
            final long b = Integer.parseInt(hex.substring(2 * (width - 1 - i), 2 * (width - i)), 16);
            bits[i / 8] |= b << (8 * (i % 8));
        }
        return bits;
    }

    private static String lines(String answer) {
        final StringSink sink = new StringSink();
        final ObjList<String> messages = PGHexScripts.splitMessages(answer);
        for (int i = 0, n = messages.size(); i < n; i++) {
            final String message = messages.getQuick(i);
            final char messageType = (char) Integer.parseInt(message.substring(0, 2), 16);
            if (messageType == 'D') {
                final String k = dataRowColumns(message)[0];
                sink.put(k == null ? "<null>" : decodeText(k));
            } else {
                sink.put(messageType);
            }
            sink.put('\t').put(message).put('\n');
        }
        return TypeConformanceRecording.escape(sink);
    }

    private void assertPath(String path, String queryHex) throws Exception {
        if (!TypeConformanceInvariants.isEnabled(type, path, MODE)) {
            return;
        }
        assertMemoryLeak(() -> {
            final StringSink steps = new StringSink();
            try {
                execute("CREATE TABLE t (k VARCHAR, v " + type.ddl + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            } catch (Throwable e) {
                steps.put("error: create: ").put(e.getMessage()).put('\n');
                assertSection(path, steps);
                return;
            }
            TypeConformanceValues.writeRows(engine, sqlExecutionContext, "t", rows, "", 0, 0, rows.size(), 1, true, steps);

            final NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            final PGConfiguration configuration = getStdPgWireConfig();
            final String authRequest;
            final String ready;
            final String answer;
            try (
                    PGServer server = createPGServer(configuration, true);
                    WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                final long fd = PGHexScripts.connect(nf, server.getPort());
                try {
                    authRequest = PGHexScripts.exchange(nf, fd, STARTUP, 'R');
                    ready = PGHexScripts.exchange(nf, fd, PASSWORD, 'Z');
                    answer = PGHexScripts.exchange(nf, fd, queryHex, 'Z');
                } finally {
                    nf.close(fd);
                }
            }

            Assert.assertEquals(TypeConformanceInvariants.context(type, "-", "pg.startup", MODE), AUTH_REQUEST, authRequest);
            Assert.assertEquals(TypeConformanceInvariants.context(type, "-", "pg.startup", MODE), READY, ready);
            if (type.isLater()) {
                checkLater(path, answer, steps);
            } else {
                assertSection(path, steps + lines(answer));
            }

            // the same conversation as a hex script on a fresh connection; the section above
            // has proven that the captured answer is the recorded one
            final String script = ">" + STARTUP + "\n<" + AUTH_REQUEST + "\n>" + PASSWORD + "\n<" + READY
                    + "\n>" + queryHex + "\n<" + answer + "\n";
            PGHexScripts.playScript(this, nf, script, configuration, null);
        });
    }

    private void assertSection(String path, CharSequence actual) {
        TypeConformanceRecording.assertSection(type, path, MODE, RECORDINGS.get(type.label), actual);
    }

    /**
     * Invariants for a type registered later: the value column of each DataRow, by value row.
     */
    private void checkLater(String path, String answer, StringSink steps) {
        final String policy = TypeConformanceInvariants.policyOf(type);
        final String nullError = TypeConformanceInvariants.nullRowWriteError(type, path, MODE, steps);
        final Map<String, String> values = new HashMap<>();
        final ObjList<String> messages = PGHexScripts.splitMessages(answer);
        for (int i = 0, n = messages.size(); i < n; i++) {
            final String message = messages.getQuick(i);
            final char messageType = (char) Integer.parseInt(message.substring(0, 2), 16);
            if (messageType == 'E') {
                Assert.fail(TypeConformanceInvariants.context(type, "-", path, MODE) + ": the server answered an error: " + message);
            }
            if (messageType == 'D') {
                final String[] columns = dataRowColumns(message);
                values.put(decodeText(columns[0]), columns[1]);
            }
        }
        final boolean isBinary = "pg.binary".equals(path);
        TypeConformanceValues.Row sentinel = null;
        for (int i = 0, n = rows.size(); i < n; i++) {
            final TypeConformanceValues.Row row = rows.getQuick(i);
            if (row.isNull() || !values.containsKey(row.label)) {
                continue;
            }
            if ("sentinel".equals(row.label)) {
                sentinel = row;
            }
            if (isBinary) {
                final String value = values.get(row.label);
                if (value == null) {
                    Assert.fail(TypeConformanceInvariants.context(type, row.label, path, MODE) + ": the value arrived as NULL");
                }
                TypeConformanceInvariants.assertReadsBackAsWritten(type, row.label, path, MODE, row.bits, decodeBinary(type, row, value));
            }
        }
        if (sentinel == null) {
            return;
        }
        final String nullValue = values.get("null");
        final String sentinelValue = values.get("sentinel");
        final String nullText = values.containsKey("null") ? (nullValue == null ? "<null>" : nullValue) : null;
        for (int i = 0, n = rows.size(); i < n; i++) {
            final String label = rows.getQuick(i).label;
            if (label.startsWith("sentinel_") && values.containsKey(label)) {
                final String value = values.get(label);
                TypeConformanceInvariants.assertOtherSentinel(type, label, path, MODE, nullText, value == null ? "<null>" : value);
            }
        }
        final String sentinelText = sentinelValue == null ? "<null>" : sentinelValue;
        if (isBinary) {
            TypeConformanceInvariants.assertNullPolicy(
                    type,
                    path,
                    MODE,
                    nullText,
                    nullValue == null ? null : decodeBigEndian(nullValue),
                    nullError,
                    sentinelText,
                    sentinelValue == null ? null : decodeBigEndian(sentinelValue),
                    sentinel.bits
            );
        } else if (TypeConformanceInvariants.POLICY_SENTINEL.equals(policy)) {
            Assert.assertEquals(TypeConformanceInvariants.context(type, "null", path, MODE)
                    + ": SENTINEL, the NULL row must read as the sentinel-pattern row", sentinelText, nullText);
        } else if (TypeConformanceInvariants.POLICY_BITMAP.equals(policy)) {
            Assert.assertNotEquals(TypeConformanceInvariants.context(type, "null", path, MODE)
                    + ": BITMAP, the NULL row and the sentinel-pattern row must stay distinct", sentinelText, nullText);
        }
    }

    private static void rec(String label, String recording) {
        RECORDINGS.put(label, recording);
    }

    // recordings: start
    static {
        rec("BOOLEAN", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000000100001ffffffff0000
                min\t44000000120002000000036d696e0000000166
                max\t44000000120002000000036d61780000000174
                null\t44000000130002000000046e756c6c0000000166
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000000100001ffffffff0001
                min\t44000000120002000000036d696e0000000100
                max\t44000000120002000000036d61780000000101
                null\t44000000130002000000046e756c6c0000000100
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("BYTE", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000000150002ffffffff0000
                min\t44000000150002000000036d696e000000042d313238
                max\t44000000140002000000036d617800000003313237
                other_null\t440000001a00020000000a6f746865725f6e756c6c000000022d31
                null\t44000000130002000000046e756c6c0000000130
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000000150002ffffffff0001
                min\t44000000130002000000036d696e00000002ff80
                max\t44000000130002000000036d617800000002007f
                other_null\t440000001a00020000000a6f746865725f6e756c6c00000002ffff
                null\t44000000140002000000046e756c6c000000020000
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                """);
        rec("SHORT", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000000150002ffffffff0001
                min\t44000000130002000000036d696e000000028000
                max\t44000000130002000000036d6178000000027fff
                other_null\t440000001a00020000000a6f746865725f6e756c6c00000002ffff
                null\t44000000140002000000046e756c6c000000020000
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000000150002ffffffff0000
                min\t44000000170002000000036d696e000000062d3332373638
                max\t44000000160002000000036d6178000000053332373637
                other_null\t440000001a00020000000a6f746865725f6e756c6c000000022d31
                null\t44000000130002000000046e756c6c0000000130
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                """);
        rec("CHAR", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000412ffff000000050001
                min\t44000000110002000000036d696effffffff
                max\t44000000140002000000036d617800000003efbfbf
                other_null\t440000001b00020000000a6f746865725f6e756c6c00000003efbfbf
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000412ffff000000050000
                min\t44000000110002000000036d696effffffff
                max\t44000000140002000000036d617800000003efbfbf
                other_null\t440000001b00020000000a6f746865725f6e756c6c00000003efbfbf
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                """);
        rec("INT", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000000170004ffffffff0001
                min\t44000000150002000000036d696e0000000480000001
                max\t44000000150002000000036d6178000000047fffffff
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000000170004ffffffff0000
                min\t440000001c0002000000036d696e0000000b2d32313437343833363437
                max\t440000001b0002000000036d61780000000a32313437343833363437
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                """);
        rec("LONG", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000000140008ffffffff0001
                min\t44000000190002000000036d696e000000088000000000000001
                max\t44000000190002000000036d6178000000087fffffffffffffff
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000000140008ffffffff0000
                min\t44000000250002000000036d696e000000142d39323233333732303336383534373735383037
                max\t44000000240002000000036d61780000001339323233333732303336383534373735383037
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                """);
        rec("DATE", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff000176000000000000020000045a0008ffffffff0001
                min\t44000000190002000000036d696e00000008fffca2fec4c823e8
                max\t44000000190002000000036d617800000008fffca2fec4c81c18
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff000076000000000000020000045a0008ffffffff0000
                min\t440000002e0002000000036d696e0000001d2d3239323237353035352d30352d31362031363a34373a30342e313933
                max\t440000002d0002000000036d61780000001c3239323237383939342d30382d31372030373a31323a35352e383037
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                """);
        rec("TIMESTAMP", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff000076000000000000020000045a0008ffffffff0000
                min\t440000002e0002000000036d696e0000001d2d3239303330382d30312d30312031393a35393a30352e323234313933
                max\t440000002d0002000000036d61780000001c3239343234372d30312d31302030343a30303a35342e373735383037
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff000176000000000000020000045a0008ffffffff0001
                min\t44000000190002000000036d696e000000087ffca2fec4c82001
                max\t44000000190002000000036d6178000000087ffca2fec4c81fff
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                """);
        rec("FLOAT", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000002bc0004ffffffff0001
                min\t44000000150002000000036d696e00000004ff7fffff
                max\t44000000150002000000036d6178000000047f7fffff
                nan\t44000000110002000000036e616effffffff
                literal_inf\t440000001900020000000b6c69746572616c5f696e66ffffffff
                negzero\t44000000190002000000076e65677a65726f0000000480000000
                null\t44000000120002000000046e756c6cffffffff
                inf\t4400000011000200000003696e66ffffffff
                ninf\t44000000120002000000046e696e66ffffffff
                C\t430000000d53454c454354203800
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000002bc0004ffffffff0000
                min\t440000001e0002000000036d696e0000000d2d332e34303238323335453338
                max\t440000001d0002000000036d61780000000c332e34303238323335453338
                nan\t44000000110002000000036e616effffffff
                literal_inf\t440000001900020000000b6c69746572616c5f696e66ffffffff
                negzero\t44000000190002000000076e65677a65726f000000042d302e30
                null\t44000000120002000000046e756c6cffffffff
                inf\t4400000011000200000003696e66ffffffff
                ninf\t44000000120002000000046e696e66ffffffff
                C\t430000000d53454c454354203800
                Z\t5a0000000549
                """);
        rec("DOUBLE", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000002bd0008ffffffff0001
                min\t44000000190002000000036d696e00000008ffefffffffffffff
                max\t44000000190002000000036d6178000000087fefffffffffffff
                nan\t44000000110002000000036e616effffffff
                literal_inf\t440000001900020000000b6c69746572616c5f696e66ffffffff
                negzero\t440000001d0002000000076e65677a65726f000000088000000000000000
                null\t44000000120002000000046e756c6cffffffff
                inf\t4400000011000200000003696e66ffffffff
                ninf\t44000000120002000000046e696e66ffffffff
                C\t430000000d53454c454354203800
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000002bd0008ffffffff0000
                min\t44000000280002000000036d696e000000172d312e3739373639333133343836323331353745333038
                max\t44000000270002000000036d617800000016312e3739373639333133343836323331353745333038
                nan\t44000000110002000000036e616effffffff
                literal_inf\t440000001900020000000b6c69746572616c5f696e66ffffffff
                negzero\t44000000190002000000076e65677a65726f000000042d302e30
                null\t44000000120002000000046e756c6cffffffff
                inf\t4400000011000200000003696e66ffffffff
                ninf\t44000000120002000000046e696e66ffffffff
                C\t430000000d53454c454354203800
                Z\t5a0000000549
                """);
        rec("STRING", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000413ffffffffffff0001
                empty\t4400000013000200000005656d70747900000000
                min\t44000000120002000000036d696e0000000120
                max\t440000001d0002000000036d61780000000cc3bce282acf09f9880efbfbd
                escape\t440000001d000200000006657363617065000000096122622c635c642765
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203500
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000413ffffffffffff0000
                empty\t4400000013000200000005656d70747900000000
                min\t44000000120002000000036d696e0000000120
                max\t440000001d0002000000036d61780000000cc3bce282acf09f9880efbfbd
                escape\t440000001d000200000006657363617065000000096122622c635c642765
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203500
                Z\t5a0000000549
                """);
        rec("SYMBOL", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000413ffffffffffff0000
                empty\t4400000013000200000005656d70747900000000
                min\t44000000120002000000036d696e0000000120
                max\t440000001d0002000000036d61780000000cc3bce282acf09f9880efbfbd
                escape\t440000001d000200000006657363617065000000096122622c635c642765
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203500
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000413ffffffffffff0001
                empty\t4400000013000200000005656d70747900000000
                min\t44000000120002000000036d696e0000000120
                max\t440000001d0002000000036d61780000000cc3bce282acf09f9880efbfbd
                escape\t440000001d000200000006657363617065000000096122622c635c642765
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203500
                Z\t5a0000000549
                """);
        rec("LONG256", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000413ffffffffffff0000
                min\t44000000150002000000036d696e0000000430783030
                max\t44000000530002000000036d617800000042307866666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000413ffffffffffff0001
                min\t44000000150002000000036d696e0000000430783030
                max\t44000000530002000000036d617800000042307866666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                """);
        rec("GEOBYTE", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000413ffffffffffff0001
                min\t44000000180002000000036d696e0000000730303030303030
                max\t44000000180002000000036d61780000000731313131313131
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000413ffffffffffff0000
                min\t44000000180002000000036d696e0000000730303030303030
                max\t44000000180002000000036d61780000000731313131313131
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("GEOSHORT", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000413ffffffffffff0000
                min\t44000000140002000000036d696e00000003303030
                max\t44000000140002000000036d6178000000037a7a7a
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000413ffffffffffff0001
                min\t44000000140002000000036d696e00000003303030
                max\t44000000140002000000036d6178000000037a7a7a
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("GEOINT", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000413ffffffffffff0000
                min\t44000000170002000000036d696e00000006303030303030
                max\t44000000170002000000036d6178000000067a7a7a7a7a7a
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000413ffffffffffff0001
                min\t44000000170002000000036d696e00000006303030303030
                max\t44000000170002000000036d6178000000067a7a7a7a7a7a
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("GEOLONG", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000413ffffffffffff0001
                min\t44000000190002000000036d696e000000083030303030303030
                max\t44000000190002000000036d6178000000087a7a7a7a7a7a7a7a
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000413ffffffffffff0000
                min\t44000000190002000000036d696e000000083030303030303030
                max\t44000000190002000000036d6178000000087a7a7a7a7a7a7a7a
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("BINARY", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000011ffffffffffff0001
                empty\t4400000013000200000005656d70747900000000
                max\t44000000170002000000036d617800000006000102fdfeff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000011ffffffffffff0001
                empty\t4400000013000200000005656d70747900000000
                max\t44000000170002000000036d617800000006000102fdfeff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("UUID", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000b860010ffffffff0000
                min\t44000000350002000000036d696e0000002430303030303030302d303030302d303030302d303030302d303030303030303030303030
                max\t44000000350002000000036d61780000002466666666666666662d666666662d666666662d666666662d666666666666666666666666
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000b860010ffffffff0001
                min\t44000000210002000000036d696e0000001000000000000000000000000000000000
                max\t44000000210002000000036d617800000010ffffffffffffffffffffffffffffffff
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                """);
        rec("LONG128", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000000ffffffffffff0000
                E\t4500000056433030303030004d756e737570706f7274656420636f6c756d6e207479706520696e20726573756c7420736574205b747970653d4c4f4e473132382c20636f6c756d6e3d315d00534552524f520050310000
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000000ffffffffffff0001
                E\t4500000056433030303030004d756e737570706f7274656420636f6c756d6e207479706520696e20726573756c7420736574205b747970653d4c4f4e473132382c20636f6c756d6e3d315d00534552524f520050310000
                Z\t5a0000000549
                """);
        rec("IPv4", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000413ffffffffffff0000
                min\t44000000180002000000036d696e00000007302e302e302e31
                max\t44000000200002000000036d61780000000f3235352e3235352e3235352e323535
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000413ffffffffffff0001
                min\t44000000180002000000036d696e00000007302e302e302e31
                max\t44000000200002000000036d61780000000f3235352e3235352e3235352e323535
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                """);
        rec("VARCHAR", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000413ffffffffffff0001
                empty\t4400000013000200000005656d70747900000000
                min\t44000000120002000000036d696e0000000120
                max\t440000001d0002000000036d61780000000cc3bce282acf09f9880efbfbd
                escape\t440000001d000200000006657363617065000000096122622c635c642765
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203500
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000413ffffffffffff0000
                empty\t4400000013000200000005656d70747900000000
                min\t44000000120002000000036d696e0000000120
                max\t440000001d0002000000036d61780000000cc3bce282acf09f9880efbfbd
                escape\t440000001d000200000006657363617065000000096122622c635c642765
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203500
                Z\t5a0000000549
                """);
        rec("DOUBLE[]", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000003feffffffffffff0000
                min\t440000002a0002000000036d696e000000197b2d312e37393736393331333438363233313537453330387d
                max\t44000000290002000000036d6178000000187b312e37393736393331333438363233313537453330387d
                empty\t4400000015000200000005656d707479000000027b7d
                specials\t440000002b0002000000087370656369616c73000000157b4e554c4c2c4e554c4c2c4e554c4c2c2d302e307d
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203500
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000003feffffffffffff0001
                min\t44000000310002000000036d696e000000200000000100000000000002bd000000010000000100000008ffefffffffffffff
                max\t44000000310002000000036d6178000000200000000100000000000002bd0000000100000001000000087fefffffffffffff
                empty\t4400000027000200000005656d707479000000140000000100000000000002bd0000000000000001
                specials\t44000000420002000000087370656369616c730000002c0000000101000000000002bd0000000400000001ffffffffffffffffffffffff000000088000000000000000
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203500
                Z\t5a0000000549
                """);
        rec("DECIMAL8", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000006a4ffffffffffff0000
                min\t44000000150002000000036d696e000000042d392e39
                max\t44000000140002000000036d617800000003392e39
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000006a4ffffffffffff0001
                min\t440000001d0002000000036d696e0000000c000200004000000100092328
                max\t440000001d0002000000036d61780000000c000200000000000100092328
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("DECIMAL16", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000006a4ffffffffffff0001
                min\t440000001d0002000000036d696e0000000c0002000040000002006326ac
                max\t440000001d0002000000036d61780000000c0002000000000002006326ac
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000006a4ffffffffffff0000
                min\t44000000170002000000036d696e000000062d39392e3939
                max\t44000000160002000000036d61780000000539392e3939
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("DECIMAL32", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000006a4ffffffffffff0000
                min\t440000001b0002000000036d696e0000000a2d393939393939393939
                max\t440000001a0002000000036d617800000009393939393939393939
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000006a4ffffffffffff0001
                min\t440000001f0002000000036d696e0000000e00030002400000000009270f270f
                max\t440000001f0002000000036d61780000000e00030002000000000009270f270f
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("DECIMAL64", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000006a4ffffffffffff0000
                min\t44000000230002000000036d696e000000122d3939393939393939393939392e39393939
                max\t44000000220002000000036d6178000000113939393939393939393939392e39393939
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000006a4ffffffffffff0001
                min\t44000000210002000000036d696e000000100004000240000004270f270f270f270f
                max\t44000000210002000000036d6178000000100004000200000004270f270f270f270f
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("DECIMAL128", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000006a4ffffffffffff0000
                min\t44000000390002000000036d696e000000282d393939393939393939393939393939393939393939393939393939392e39393939393939393939
                max\t44000000380002000000036d617800000027393939393939393939393939393939393939393939393939393939392e39393939393939393939
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000006a4ffffffffffff0001
                min\t440000002d0002000000036d696e0000001c000a00064000000a270f270f270f270f270f270f270f270f270f26ac
                max\t440000002d0002000000036d61780000001c000a00060000000a270f270f270f270f270f270f270f270f270f26ac
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("DECIMAL256", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000006a4ffffffffffff0000
                min\t440000005f0002000000036d696e0000004e2d39393939393939393939393939393939393939393939393939393939393939393939393939393939393939393939393939393939393939392e3939393939393939393939393939393939393939
                max\t440000005e0002000000036d61780000004d39393939393939393939393939393939393939393939393939393939393939393939393939393939393939393939393939393939393939392e3939393939393939393939393939393939393939
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000006a4ffffffffffff0001
                min\t440000003f0002000000036d696e0000002e0013000d40000014270f270f270f270f270f270f270f270f270f270f270f270f270f270f270f270f270f270f270f
                max\t440000003f0002000000036d61780000002e0013000d00000014270f270f270f270f270f270f270f270f270f270f270f270f270f270f270f270f270f270f270f
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("INTERVAL", """
                ## pg.binary
                error: create: [29] non-persisted type: INTERVAL
                ## pg.text
                error: create: [29] non-persisted type: INTERVAL
                """);
        rec("VARCHAR_SLICE", """
                ## pg.text
                error: create: [29] unsupported column type: VARCHAR_SLICE
                ## pg.binary
                error: create: [29] unsupported column type: VARCHAR_SLICE
                """);
        rec("TIMESTAMP_NS", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff000076000000000000020000045a0008ffffffff0000
                min\t440000002b0002000000036d696e0000001a313637372d30312d30312030303a31323a34332e313435323234
                max\t440000002b0002000000036d61780000001a323236322d30342d31312032333a34373a31362e383534373735
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff000176000000000000020000045a0008ffffffff0001
                min\t44000000190002000000036d696e00000008ffdbde631ee4cc09
                max\t44000000190002000000036d617800000008001d679a6aab73f7
                sentinel\t440000001600020000000873656e74696e656cffffffff
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203400
                Z\t5a0000000549
                """);
        rec("GEOHASH(1c)", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000413ffffffffffff0001
                min\t44000000120002000000036d696e0000000130
                max\t44000000120002000000036d6178000000017a
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000413ffffffffffff0000
                min\t44000000120002000000036d696e0000000130
                max\t44000000120002000000036d6178000000017a
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("GEOHASH(8b)", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000413ffffffffffff0001
                min\t44000000190002000000036d696e000000083030303030303030
                max\t44000000190002000000036d6178000000083131313131313131
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000413ffffffffffff0000
                min\t44000000190002000000036d696e000000083030303030303030
                max\t44000000190002000000036d6178000000083131313131313131
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("GEOHASH(31b)", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000413ffffffffffff0000
                min\t44000000300002000000036d696e0000001f30303030303030303030303030303030303030303030303030303030303030
                max\t44000000300002000000036d61780000001f31313131313131313131313131313131313131313131313131313131313131
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000413ffffffffffff0001
                min\t44000000300002000000036d696e0000001f30303030303030303030303030303030303030303030303030303030303030
                max\t44000000300002000000036d61780000001f31313131313131313131313131313131313131313131313131313131313131
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("GEOHASH(12c)", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff0001760000000000000200000413ffffffffffff0001
                min\t440000001d0002000000036d696e0000000c303030303030303030303030
                max\t440000001d0002000000036d61780000000c7a7a7a7a7a7a7a7a7a7a7a7a
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff0000760000000000000200000413ffffffffffff0000
                min\t440000001d0002000000036d696e0000000c303030303030303030303030
                max\t440000001d0002000000036d61780000000c7a7a7a7a7a7a7a7a7a7a7a7a
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("DECIMAL(5,2)", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000006a4ffffffffffff0001
                min\t440000001d0002000000036d696e0000000c000200004000000203e726ac
                max\t440000001d0002000000036d61780000000c000200000000000203e726ac
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000006a4ffffffffffff0000
                min\t44000000180002000000036d696e000000072d3939392e3939
                max\t44000000170002000000036d6178000000063939392e3939
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("DECIMAL(18,3)", """
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000006a4ffffffffffff0001
                min\t44000000230002000000036d696e00000012000500034000000303e7270f270f270f2706
                max\t44000000230002000000036d617800000012000500030000000303e7270f270f270f2706
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000006a4ffffffffffff0000
                min\t44000000250002000000036d696e000000142d3939393939393939393939393939392e393939
                max\t44000000240002000000036d6178000000133939393939393939393939393939392e393939
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203300
                Z\t5a0000000549
                """);
        rec("DOUBLE[][]", """
                ## pg.text
                T\t540000002e00026b0000000000000100000413ffffffffffff00007600000000000002000003feffffffffffff0000
                min\t440000002c0002000000036d696e0000001b7b7b2d312e37393736393331333438363233313537453330387d7d
                max\t440000002b0002000000036d61780000001a7b7b312e37393736393331333438363233313537453330387d7d
                empty\t4400000015000200000005656d707479000000027b7d
                specials\t440000002d0002000000087370656369616c73000000177b7b4e554c4c2c4e554c4c2c4e554c4c2c2d302e307d7d
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203500
                Z\t5a0000000549
                ## pg.binary
                1\t3100000004
                2\t3200000004
                T\t540000002e00026b0000000000000100000413ffffffffffff00017600000000000002000003feffffffffffff0001
                min\t44000000390002000000036d696e000000280000000200000000000002bd0000000100000001000000010000000100000008ffefffffffffffff
                max\t44000000390002000000036d6178000000280000000200000000000002bd00000001000000010000000100000001000000087fefffffffffffff
                empty\t440000002f000200000005656d7074790000001c0000000200000000000002bd00000000000000010000000000000001
                specials\t440000004a0002000000087370656369616c73000000340000000201000000000002bd00000001000000010000000400000001ffffffffffffffffffffffff000000088000000000000000
                null\t44000000120002000000046e756c6cffffffff
                C\t430000000d53454c454354203500
                Z\t5a0000000549
                """);
        rec("INTERVAL(us)", """
                ## pg.binary
                error: create: [29] non-persisted type: INTERVAL
                ## pg.text
                error: create: [29] non-persisted type: INTERVAL
                """);
        rec("INTERVAL(ns)", """
                ## pg.binary
                error: create: [29] non-persisted type: INTERVAL
                ## pg.text
                error: create: [29] non-persisted type: INTERVAL
                """);
    }
    // recordings: end
}
