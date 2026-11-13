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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.qwp.client.QwpSchemaFreshness;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.griffin.SqlException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;

import static io.questdb.test.cutlass.qwp.e2e.QwpSchemaE2ETestUtils.connectSchemaClient;
import static io.questdb.test.cutlass.qwp.e2e.QwpSchemaE2ETestUtils.describeKnown;
import static io.questdb.test.cutlass.qwp.e2e.QwpSchemaE2ETestUtils.receiveResponse;

/**
 * A sender converts values to the column types of the schema it cached, so a frame
 * it encoded before ALTER TABLE ... ALTER COLUMN ... TYPE reaches the server with the
 * old type on the wire: frames in flight, frames written within the schema age limit
 * and frames replayed from store-and-forward, which resend the same bytes. The server
 * converts such a value to the new column type the way the ALTER converted the rows
 * already in the table, and rejects a value the new type cannot hold, as it rejects
 * one a sender writes in that type.
 */
public class QwpSchemaStaleAlterE2ETest extends AbstractQwpWebSocketTest {
    private static final String DATE_TEXT_0 = "1970-01-02T00:00:00.000Z";
    private static final String DATE_TEXT_1 = "1969-12-31T23:59:59.999Z";
    private static final String DATE_TEXT_2 = "2024-02-29T12:34:56.789Z";
    // Every type ALTER COLUMN TYPE converts from or to. The SQL compiler decides which
    // pairs it supports.
    private static final String[] TYPES = {
            "BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP", "TIMESTAMP_NS",
            "STRING", "VARCHAR", "SYMBOL", "IPV4", "UUID", "CHAR",
            "DECIMAL(2,1)", "DECIMAL(4,2)", "DECIMAL(9,3)", "DECIMAL(18,2)", "DECIMAL(38,4)", "DECIMAL(76,6)"
    };
    private static final String UUID_0 = "11111111-2222-3333-4444-555555555555";
    private static final String UUID_1 = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    private int tableCount;

    @Test
    public void testBooleanToTemporal() throws Exception {
        runInContext(port -> {
            for (String to : new String[]{"DATE", "TIMESTAMP", "TIMESTAMP_NS"}) {
                staleCase("BOOLEAN", to)
                        .row(b -> b.boolColumn("c", true))
                        .row(b -> b.boolColumn("c", false))
                        .nullRow()
                        .expect("SELECT k, c::LONG v FROM %s ORDER BY k", """
                                k\tv
                                0\t1
                                1\t0
                                2\tnull
                                """)
                        .run(port);
            }
        });
    }

    @Test
    public void testCharToText() throws Exception {
        runInContext(port -> {
            for (String to : new String[]{"STRING", "VARCHAR", "SYMBOL"}) {
                staleCase("CHAR", to)
                        .row(b -> b.charColumn("c", 'a'))
                        .row(b -> b.charColumn("c", 'é'))
                        .row(b -> b.charColumn("c", '中'))
                        .nullRow()
                        .expect("SELECT k, c, length(c) len FROM %s ORDER BY k", """
                                k\tc\tlen
                                0\ta\t1
                                1\té\t1
                                2\t中\t1
                                3\t\t-1
                                """)
                        .run(port);
            }
        });
    }

    @Test
    public void testCharZeroToSymbolIsNull() throws Exception {
        // A CHAR of 0 is the CHAR column's NULL, and ALTER turns it into a NULL symbol.
        runInContext(port -> staleCase("CHAR", "SYMBOL")
                .row(b -> b.charColumn("c", (char) 0))
                .row(b -> b.charColumn("c", 'x'))
                .expect("SELECT k, c, c IS NULL n FROM %s ORDER BY k", """
                        k\tc\tn
                        0\t\ttrue
                        1\tx\tfalse
                        """)
                .run(port));
    }

    @Test
    public void testMalformedTextToChar() throws Exception {
        // A correct client never sends a lone UTF-8 lead byte; the frame's '~' becomes one.
        runInContext(port -> {
            staleCase("VARCHAR", "CHAR")
                    .row(b -> b.stringColumn("c", "~"))
                    .patch((byte) '~', (byte) 0xE9)
                    .expect("SELECT k, c IS NULL n FROM %s ORDER BY k", """
                            k\tn
                            0\ttrue
                            """)
                    .run(port);
            staleCase("SYMBOL", "CHAR")
                    .row(b -> b.symbol("c", "~"))
                    .patch((byte) '~', (byte) 0xE9)
                    .expect("SELECT k, c IS NULL n FROM %s ORDER BY k", """
                            k\tn
                            0\ttrue
                            """)
                    .run(port);
        });
    }

    @Test
    public void testDateToNumeric() throws Exception {
        runInContext(port -> {
            staleCase("DATE", "BYTE")
                    .row(b -> b.longColumn("c", Byte.MAX_VALUE))
                    .row(b -> b.longColumn("c", Byte.MIN_VALUE))
                    .nullRow()
                    .reject(b -> b.longColumn("c", Byte.MAX_VALUE + 1), "integer value 128 out of range for BYTE")
                    .reject(b -> b.longColumn("c", Byte.MIN_VALUE - 1), "integer value -129 out of range for BYTE")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t127
                            1\t-128
                            2\t0
                            """)
                    .run(port);
            staleCase("DATE", "SHORT")
                    .row(b -> b.longColumn("c", Short.MAX_VALUE))
                    .row(b -> b.longColumn("c", Short.MIN_VALUE))
                    .reject(b -> b.longColumn("c", Short.MAX_VALUE + 1), "integer value 32768 out of range for SHORT")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t32767
                            1\t-32768
                            """)
                    .run(port);
            staleCase("DATE", "INT")
                    .row(b -> b.longColumn("c", Integer.MAX_VALUE))
                    .row(b -> b.longColumn("c", Integer.MIN_VALUE + 1))
                    .row(b -> b.longColumn("c", -1))
                    .nullRow()
                    .reject(b -> b.longColumn("c", Integer.MAX_VALUE + 1L), "integer value 2147483648 out of range for INT")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t2147483647
                            1\t-2147483647
                            2\t-1
                            3\tnull
                            """)
                    .run(port);
            staleCase("DATE", "LONG")
                    .row(b -> b.longColumn("c", Long.MAX_VALUE))
                    .row(b -> b.longColumn("c", Long.MIN_VALUE + 1))
                    .nullRow()
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t9223372036854775807
                            1\t-9223372036854775807
                            2\tnull
                            """)
                    .run(port);
            // 2^24 + 1 and 2^53 + 1 are the first integers FLOAT and DOUBLE round.
            staleCase("DATE", "FLOAT")
                    .row(b -> b.longColumn("c", 16_777_217L))
                    .row(b -> b.longColumn("c", -5))
                    .nullRow()
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t1.6777216E7
                            1\t-5.0
                            2\tnull
                            """)
                    .run(port);
            staleCase("DATE", "DOUBLE")
                    .row(b -> b.longColumn("c", 9_007_199_254_740_993L))
                    .row(b -> b.longColumn("c", Long.MIN_VALUE + 1))
                    .nullRow()
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t9.007199254740992E15
                            1\t-9.223372036854776E18
                            2\tnull
                            """)
                    .run(port);
        });
    }

    @Test
    public void testDateToTimestamp() throws Exception {
        runInContext(port -> {
            // 2024-02-29T12:34:56.789Z is a leap day, -86_400_001 is before 1970.
            staleCase("DATE", "TIMESTAMP")
                    .row(b -> b.longColumn("c", 1))
                    .row(b -> b.longColumn("c", -1))
                    .row(b -> b.longColumn("c", 0))
                    .row(b -> b.longColumn("c", 1_709_210_096_789L))
                    .row(b -> b.longColumn("c", -86_400_001L))
                    .row(b -> b.longColumn("c", Long.MAX_VALUE / 1000))
                    .row(b -> b.longColumn("c", Long.MIN_VALUE / 1000))
                    .nullRow()
                    .reject(b -> b.longColumn("c", Long.MAX_VALUE / 1000 + 1), "timestamp overflow converting millis to micros: 9223372036854776")
                    .reject(b -> b.longColumn("c", Long.MIN_VALUE / 1000 - 1), "timestamp overflow converting millis to micros: -9223372036854776")
                    .expect("SELECT k, c::LONG v FROM %s ORDER BY k", """
                            k\tv
                            0\t1000
                            1\t-1000
                            2\t0
                            3\t1709210096789000
                            4\t-86400001000
                            5\t9223372036854775000
                            6\t-9223372036854775000
                            7\tnull
                            """)
                    .run(port);
            staleCase("DATE", "TIMESTAMP_NS")
                    .row(b -> b.longColumn("c", 1))
                    .row(b -> b.longColumn("c", -1))
                    .row(b -> b.longColumn("c", 1_709_210_096_789L))
                    .row(b -> b.longColumn("c", Long.MAX_VALUE / 1_000_000))
                    .row(b -> b.longColumn("c", Long.MIN_VALUE / 1_000_000))
                    .nullRow()
                    .reject(b -> b.longColumn("c", Long.MAX_VALUE / 1_000_000 + 1), "timestamp overflow converting millis to nanos: 9223372036855")
                    .reject(b -> b.longColumn("c", Long.MIN_VALUE / 1_000_000 - 1), "timestamp overflow converting millis to nanos: -9223372036855")
                    .expect("SELECT k, c::LONG v FROM %s ORDER BY k", """
                            k\tv
                            0\t1000000
                            1\t-1000000
                            2\t1709210096789000000
                            3\t9223372036854000000
                            4\t-9223372036854000000
                            5\tnull
                            """)
                    .run(port);
        });
    }

    @Test
    public void testDecimalToFloating() throws Exception {
        runInContext(port -> {
            String[][] cases = {
                    {"DECIMAL(2,1)", "1.5", "-9.9", "0.1"},
                    {"DECIMAL(4,2)", "99.99", "-0.01", "0.10"},
                    {"DECIMAL(9,3)", "123456.789", "-999999.999", "0.333"},
                    {"DECIMAL(18,2)", "2.50", "9999999999999999.99", "-0.07"},
                    {"DECIMAL(38,4)", "1234567890123456789012345678901234.5678", "-0.0001", "3.1416"},
                    {"DECIMAL(76,6)", "1" + "0".repeat(37) + ".000001", "-123456789012345678901234567890.123456", "0.000001"},
            };
            for (String[] c : cases) {
                for (String to : new String[]{"DOUBLE", "FLOAT"}) {
                    StringBuilder expected = new StringBuilder("k\tc\n");
                    StaleCase sc = staleCase(c[0], to);
                    for (int i = 1; i < c.length; i++) {
                        String text = c[i];
                        sc.row(b -> b.decimalColumn("c", text, new Decimal256()));
                        // Java's decimal parsers round correctly, as ALTER does.
                        expected.append(i - 1).append('\t')
                                .append(to.equals("DOUBLE") ? Double.toString(Double.parseDouble(text)) : Float.toString(Float.parseFloat(text)))
                                .append('\n');
                    }
                    sc.nullRow();
                    expected.append(c.length - 1).append("\tnull\n");
                    sc.expect("SELECT k, c FROM %s ORDER BY k", expected.toString()).run(port);
                }
            }
            // A FLOAT cannot hold 10^40, a DOUBLE can.
            String huge = "1" + "0".repeat(40);
            staleCase("DECIMAL(76,6)", "FLOAT")
                    .row(b -> b.decimalColumn("c", "-3.5", new Decimal256()))
                    .reject(b -> b.decimalColumn("c", huge, new Decimal256()), "out of range for FLOAT")
                    .reject(b -> b.decimalColumn("c", "-" + huge, new Decimal256()), "out of range for FLOAT")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t-3.5
                            """)
                    .run(port);
            // All 76 digits of the widest DECIMAL.
            String widest = "-9" + "8".repeat(69) + ".765432";
            staleCase("DECIMAL(76,6)", "DOUBLE")
                    .row(b -> b.decimalColumn("c", huge, new Decimal256()))
                    .row(b -> b.decimalColumn("c", widest, new Decimal256()))
                    .expect("SELECT k, c FROM %s ORDER BY k", "k\tc\n0\t1.0E40\n1\t" + Double.parseDouble(widest) + '\n')
                    .run(port);
        });
    }

    @Test
    public void testEveryColumnTypeChangeStoresFramesEncodedBeforeIt() throws Exception {
        runInContext(port -> {
            List<String> failures = new ArrayList<>();
            Set<String> pairs = new HashSet<>();
            for (String from : TYPES) {
                assertTypeChangesMatchAlter(port, from, pairs, failures);
            }
            Assert.assertEquals(String.join("\n", failures), 0, failures.size());
            // Guards the matrix against silently testing nothing.
            for (String pair : new String[]{
                    "DATE->TIMESTAMP", "DECIMAL(18,2)->DOUBLE", "SYMBOL->LONG", "VARCHAR->IPV4", "TIMESTAMP->SYMBOL"
            }) {
                Assert.assertTrue(pair, pairs.contains(pair));
            }
            Assert.assertTrue("pairs=" + pairs.size(), pairs.size() > 250);
        }, 65_536, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    @Test
    public void testFloatingToTemporal() throws Exception {
        runInContext(port -> {
            for (String from : new String[]{"DOUBLE", "FLOAT"}) {
                for (String to : new String[]{"DATE", "TIMESTAMP", "TIMESTAMP_NS"}) {
                    staleCase(from, to)
                            .row(b -> b.doubleColumn("c", 7))
                            .row(b -> b.doubleColumn("c", -3))
                            .row(b -> b.doubleColumn("c", -0.0))
                            .row(b -> b.doubleColumn("c", 16_777_216))
                            .nullRow()
                            .reject(b -> b.doubleColumn("c", 2.5), "double value 2.5 loses precision")
                            .reject(b -> b.doubleColumn("c", Double.POSITIVE_INFINITY), "double value Infinity loses precision")
                            .reject(b -> b.doubleColumn("c", Double.NEGATIVE_INFINITY), "double value -Infinity loses precision")
                            .expect("SELECT k, c::LONG v FROM %s ORDER BY k", """
                                    k\tv
                                    0\t7
                                    1\t-3
                                    2\t0
                                    3\t16777216
                                    4\tnull
                                    """)
                            .run(port);
                }
            }
        });
    }

    @Test
    public void testLegacyWireTypesNoColumnTypeChangeProducesStillFail() throws Exception {
        // ALTER COLUMN TYPE converts none of these pairs, so only a sender that encodes
        // its own types sends them, and the server keeps rejecting them.
        runInContext(port -> {
            assertLegacyRejected(port, "UUID", c -> c.column(QwpConstants.TYPE_DATE).addLong(1),
                    "type coercion from DATE to UUID is not supported [column=c]");
            assertLegacyRejected(port, "IPV4", c -> c.column(QwpConstants.TYPE_BINARY).addBinary(new byte[]{1}),
                    "type coercion from BINARY to IPv4 is not supported [column=c]");
            assertLegacyRejected(port, "IPV4", c -> c.column(QwpConstants.TYPE_BOOLEAN).addBoolean(true),
                    "type coercion from BOOLEAN to IPv4 is not supported [column=c]");
            assertLegacyRejected(port, "BOOLEAN", c -> c.column(QwpConstants.TYPE_UUID).addUuid(1, 2),
                    "cannot write UUID to column [column=c, type=BOOLEAN]");
            assertLegacyRejected(port, "BOOLEAN", c -> c.column(QwpConstants.TYPE_DECIMAL64).addDecimal64(new Decimal64(15, 1)),
                    "cannot write DECIMAL64 to column [column=c, type=BOOLEAN]");
            assertLegacyRejected(port, "UUID", c -> c.column(QwpConstants.TYPE_BOOLEAN).addBoolean(true),
                    "cannot write BOOLEAN to column [column=c, type=UUID]");
            assertLegacyRejected(port, "UUID", c -> c.column(QwpConstants.TYPE_TIMESTAMP).addLong(1),
                    "cannot write TIMESTAMP to column [column=c, type=UUID]");
            assertLegacyRejected(port, "INT", c -> c.column(QwpConstants.TYPE_DECIMAL64).addDecimal64(new Decimal64(15, 1)),
                    "cannot write DECIMAL64 to column [column=c, type=INT]");
            assertLegacyRejected(port, "LONG256", c -> c.column(QwpConstants.TYPE_SYMBOL).addSymbol("x"),
                    "cannot write SYMBOL to column [column=c, type=LONG256]");
            assertLegacyRejected(port, "TIMESTAMP", c -> c.column(QwpConstants.TYPE_UUID).addUuid(1, 2),
                    "type coercion from UUID to TIMESTAMP is not supported [column=c]");
            assertLegacyRejected(port, "CHAR", c -> c.column(QwpConstants.TYPE_BOOLEAN).addBoolean(true),
                    "cannot write BOOLEAN to column [column=c, type=CHAR]");
            assertLegacyRejected(port, "SYMBOL", c -> c.column(QwpConstants.TYPE_LONG256).addLong256(1, 2, 3, 4),
                    "cannot write LONG256 to column [column=c, type=SYMBOL]");
        });
    }

    @Test
    public void testLegacyNaNToBoolean() throws Exception {
        // A NULL row gives the column a null bitmap, so the NaN travels as a value.
        execute("CREATE TABLE legacy_nan (k LONG, d BOOLEAN, f BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        runInContext(port -> {
            try (WebSocketClient client = QwpSchemaE2ETestUtils.connectLegacyClient(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = new QwpTableBuffer("legacy_nan")) {
                table.getOrCreateColumn("k", QwpConstants.TYPE_LONG, true).addLong(0);
                table.getOrCreateColumn("d", QwpConstants.TYPE_DOUBLE, true).addDouble(Double.NaN);
                table.getOrCreateColumn("f", QwpConstants.TYPE_FLOAT, true).addFloat(Float.NaN);
                table.nextRow();
                table.getOrCreateColumn("k", QwpConstants.TYPE_LONG, true).addLong(1);
                table.getOrCreateColumn("d", QwpConstants.TYPE_DOUBLE, true).addNull();
                table.getOrCreateColumn("f", QwpConstants.TYPE_FLOAT, true).addNull();
                table.nextRow();
                table.getOrCreateColumn("k", QwpConstants.TYPE_LONG, true).addLong(2);
                table.getOrCreateColumn("d", QwpConstants.TYPE_DOUBLE, true).addDouble(-0.5);
                table.getOrCreateColumn("f", QwpConstants.TYPE_FLOAT, true).addFloat(0.25f);
                table.nextRow();
                send(client, encode(encoder, table));
                WebSocketResponse response = receiveResponse(client);
                Assert.assertTrue(response.getErrorMessage(), response.isSuccess());
            }
            drainWalQueue();
            assertQuery("SELECT k, d, f FROM legacy_nan ORDER BY k").noLeakCheck().expectSize().returns("""
                    k\td\tf
                    0\tfalse\tfalse
                    1\tfalse\tfalse
                    2\ttrue\ttrue
                    """);
        });
    }

    @Test
    public void testLargeFramesMatchAlter() throws Exception {
        // 1_000 rows with every 7th NULL cross the null bitmap's byte and word boundaries,
        // regular timestamps take the Gorilla encoding, and distinct values grow the symbol map.
        runInContext(port -> {
            assertLargeFrameMatchesAlter(port, "TIMESTAMP", "DATE",
                    (b, i) -> b.timestampColumn("c", 1_700_000_000_000_000L + i * 1_000_017L, ChronoUnit.MICROS));
            assertLargeFrameMatchesAlter(port, "TIMESTAMP_NS", "LONG",
                    (b, i) -> b.timestampColumn("c", 1_700_000_000_000_000_000L + i * 1_000_000_017L, ChronoUnit.NANOS));
            assertLargeFrameMatchesAlter(port, "TIMESTAMP_NS", "SYMBOL",
                    (b, i) -> b.timestampColumn("c", -1_000_000_000_000L + i * 999_999_937L, ChronoUnit.NANOS));
            assertLargeFrameMatchesAlter(port, "DATE", "TIMESTAMP_NS",
                    (b, i) -> b.longColumn("c", 1_700_000_000_000L - i * 86_400_013L));
            assertLargeFrameMatchesAlter(port, "DATE", "SYMBOL",
                    (b, i) -> b.longColumn("c", i * 3_600_000L - 500 * 3_600_000L));
            assertLargeFrameMatchesAlter(port, "SYMBOL", "INT",
                    (b, i) -> b.symbol("c", Integer.toString(i * 37 - 5_000)));
            assertLargeFrameMatchesAlter(port, "SYMBOL", "TIMESTAMP",
                    (b, i) -> b.symbol("c", "2024-02-" + (10 + i % 19) + "T12:34:56." + (100_000 + i) + "Z"));
            assertLargeFrameMatchesAlter(port, "DECIMAL(38,4)", "DOUBLE",
                    (b, i) -> b.decimalColumn("c", (i - 500) + "." + (i % 10_000), new Decimal256()));
            assertLargeFrameMatchesAlter(port, "DOUBLE", "BOOLEAN", (b, i) -> b.doubleColumn("c", i % 3 - 1));
            assertLargeFrameMatchesAlter(port, "UUID", "SYMBOL", (b, i) -> b.uuidColumn("c", i, i * 31L));
            assertLargeFrameMatchesAlter(port, "VARCHAR", "IPV4",
                    (b, i) -> b.stringColumn("c", "10." + (i / 256) + '.' + (i % 256) + ".1"));
        });
    }

    @Test
    public void testNumericToBoolean() throws Exception {
        runInContext(port -> {
            staleCase("LONG", "BOOLEAN")
                    .row(b -> b.longColumn("c", 0))
                    .row(b -> b.longColumn("c", 1))
                    .row(b -> b.longColumn("c", -1))
                    .row(b -> b.longColumn("c", Long.MIN_VALUE + 1))
                    .row(b -> b.longColumn("c", Long.MAX_VALUE))
                    .nullRow()
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\tfalse
                            1\ttrue
                            2\ttrue
                            3\ttrue
                            4\ttrue
                            5\tfalse
                            """)
                    .run(port);
            for (String from : new String[]{"BYTE", "SHORT", "INT", "DATE"}) {
                staleCase(from, "BOOLEAN")
                        .row(b -> b.longColumn("c", 0))
                        .row(b -> b.longColumn("c", 1))
                        .row(b -> b.longColumn("c", -128))
                        .row(b -> b.longColumn("c", 127))
                        .nullRow()
                        .expect("SELECT k, c FROM %s ORDER BY k", """
                                k\tc
                                0\tfalse
                                1\ttrue
                                2\ttrue
                                3\ttrue
                                4\tfalse
                                """)
                        .run(port);
            }
            for (String from : new String[]{"DOUBLE", "FLOAT"}) {
                staleCase(from, "BOOLEAN")
                        .row(b -> b.doubleColumn("c", 0.0))
                        .row(b -> b.doubleColumn("c", -0.0))
                        .row(b -> b.doubleColumn("c", Double.MIN_VALUE))
                        .row(b -> b.doubleColumn("c", -1.5))
                        .row(b -> b.doubleColumn("c", Double.POSITIVE_INFINITY))
                        .row(b -> b.doubleColumn("c", Double.NEGATIVE_INFINITY))
                        .row(b -> b.doubleColumn("c", Double.NaN))
                        .nullRow()
                        .expect("SELECT k, c FROM %s ORDER BY k", from.equals("DOUBLE") ? """
                                k\tc
                                0\tfalse
                                1\tfalse
                                2\ttrue
                                3\ttrue
                                4\ttrue
                                5\ttrue
                                6\tfalse
                                7\tfalse
                                """
                                // Double.MIN_VALUE rounds to 0 as a FLOAT.
                                : """
                                k\tc
                                0\tfalse
                                1\tfalse
                                2\tfalse
                                3\ttrue
                                4\ttrue
                                5\ttrue
                                6\tfalse
                                7\tfalse
                                """)
                        .run(port);
            }
            for (String from : new String[]{"TIMESTAMP", "TIMESTAMP_NS"}) {
                ChronoUnit unit = from.equals("TIMESTAMP") ? ChronoUnit.MICROS : ChronoUnit.NANOS;
                staleCase(from, "BOOLEAN")
                        .row(b -> b.timestampColumn("c", 0, unit))
                        .row(b -> b.timestampColumn("c", 1, unit))
                        .row(b -> b.timestampColumn("c", -1, unit))
                        .row(b -> b.timestampColumn("c", Long.MAX_VALUE, unit))
                        .nullRow()
                        .expect("SELECT k, c FROM %s ORDER BY k", """
                                k\tc
                                0\tfalse
                                1\ttrue
                                2\ttrue
                                3\ttrue
                                4\tfalse
                                """)
                        .run(port);
            }
        });
    }

    @Test
    public void testSenderKeepsWritingAcrossColumnTypeChanges() throws Exception {
        execute("""
                CREATE TABLE across_alter (
                    d DECIMAL(18,2), dt DATE, s SYMBOL, ip VARCHAR, ts TIMESTAMP
                ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
        runInContext(port -> {
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                    + ";schema_mode=auto;auto_flush_rows=2147483647;auto_flush_bytes=0;"
                    + "auto_flush_interval=2147483646;close_flush_timeout_millis=0;")) {
                QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                // The frozen clock keeps the cached schema within the age limit
                // however long the ALTERs take.
                ws.setNanoClockForTesting(() -> 0L);
                ws.table("across_alter").doubleColumn("d", 1.25).longColumn("dt", 1_000L)
                        .longColumn("s", 41).ipv4Column("ip", "10.0.0.1")
                        .at(1, ChronoUnit.MICROS);
                Assert.assertTrue(sender.drain(10_000));

                // The sender converts this row against the cached schema: DECIMAL64,
                // DATE, SYMBOL and VARCHAR on the wire.
                ws.table("across_alter").doubleColumn("d", 2.5).longColumn("dt", 86_400_000L)
                        .longColumn("s", 42).ipv4Column("ip", "1.2.3.4")
                        .at(2, ChronoUnit.MICROS);
                execute("ALTER TABLE across_alter ALTER COLUMN d TYPE DOUBLE");
                execute("ALTER TABLE across_alter ALTER COLUMN dt TYPE TIMESTAMP");
                execute("ALTER TABLE across_alter ALTER COLUMN s TYPE LONG");
                execute("ALTER TABLE across_alter ALTER COLUMN ip TYPE IPV4");
                sender.flush();
                Assert.assertTrue(sender.drain(10_000));

                // The reply to that frame reported the new schema, so this row
                // converts to the new types.
                ws.table("across_alter").doubleColumn("d", 3.75).longColumn("dt", 3_000L)
                        .longColumn("s", 43).ipv4Column("ip", "5.6.7.8")
                        .at(3, ChronoUnit.MICROS);
                sender.flush();
                Assert.assertTrue(sender.drain(10_000));
                Assert.assertEquals(0, ws.getTotalReconnectAttempts());
            }
            drainWalQueue();
            assertQuery("SELECT d, dt, s, ip FROM across_alter ORDER BY ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            d\tdt\ts\tip
                            1.25\t1970-01-01T00:00:01.000000Z\t41\t10.0.0.1
                            2.5\t1970-01-02T00:00:00.000000Z\t42\t1.2.3.4
                            3.75\t1970-01-01T00:00:00.003000Z\t43\t5.6.7.8
                            """);
        });
    }

    @Test
    public void testSenderRecoversFromRowsRejectedAfterColumnTypeChange() throws Exception {
        // The column changes type while the sender holds a pending row, and every row
        // after the change needs the new type. No frame reaches the server to earn a
        // reply that reports the change, so the sender learns of it only from the run
        // of rows the cached schema rejects.
        record Change(String from, String to, Consumer<Sender> write, String pending, String written) {
        }
        Change[] changes = {
                new Change("INT", "DOUBLE", s -> s.doubleColumn("v", 1.5), "7.0", "1.5"),
                new Change("INT", "LONG", s -> s.longColumn("v", 3_000_000_000L), "7", "3000000000"),
                new Change("INT", "VARCHAR", s -> s.stringColumn("v", "abc"), "7", "abc"),
                new Change("DECIMAL(5,2)", "DECIMAL(10,2)", s -> s.stringColumn("v", "123456.78"), "7.00", "123456.78"),
        };
        runInContext(port -> {
            int tableIndex = 0;
            // A transactional sender sends the pending rows without committing them;
            // the application's flush() commits them with the rows in the new type.
            for (String mode : new String[]{"auto", "strict"}) {
                for (String transaction : new String[]{"off", "on"}) {
                    for (Change change : changes) {
                        String table = "recover_" + tableIndex++;
                        execute("CREATE TABLE " + table + " (v " + change.from + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                        try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";schema_mode=" + mode
                                + ";transaction=" + transaction
                                + ";auto_flush_rows=2147483647;auto_flush_bytes=0;auto_flush_interval=2147483646;"
                                + "close_flush_timeout_millis=0;")) {
                            QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                            // The frozen clock keeps the cached schema within the age limit,
                            // so only the run of rejected rows can make the sender look again.
                            ws.setNanoClockForTesting(() -> 0L);
                            sender.table(table).longColumn("v", 7).at(1, ChronoUnit.MICROS);
                            execute("ALTER TABLE " + table + " ALTER COLUMN v TYPE " + change.to);
                            for (int i = 0; i < QwpSchemaFreshness.REJECTIONS_BEFORE_RECHECK; i++) {
                                try {
                                    change.write.accept(sender.table(table));
                                    Assert.fail(mode + " " + transaction + " " + change.to + ": expected rejection against the cached schema");
                                } catch (LineSenderSchemaException e) {
                                    Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                                }
                            }
                            change.write.accept(sender.table(table));
                            sender.at(2, ChronoUnit.MICROS);
                            sender.flush();
                            Assert.assertTrue(sender.drain(10_000));
                            Assert.assertEquals(0, ws.getTotalReconnectAttempts());
                        }
                        drainWalQueue();
                        assertQuery("SELECT v FROM " + table + " ORDER BY ts")
                                .noLeakCheck()
                                .expectSize()
                                .returns("v\n" + change.pending + "\n" + change.written + "\n");
                    }
                }
            }
        });
    }

    @Test
    public void testSenderSymbolToParsedTypes() throws Exception {
        // The sender writes symbols through its connection-wide dictionary, which the
        // server reads in delta mode.
        execute("""
                CREATE TABLE symbol_across_alter (
                    b SYMBOL, i SYMBOL, f SYMBOL, dt SYMBOL, t SYMBOL, tn SYMBOL,
                    ip SYMBOL, u SYMBOL, ch SYMBOL, ts TIMESTAMP
                ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
        runInContext(port -> {
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                    + ";schema_mode=auto;auto_flush_rows=2147483647;auto_flush_bytes=0;"
                    + "auto_flush_interval=2147483646;close_flush_timeout_millis=0;")) {
                QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                ws.setNanoClockForTesting(() -> 0L);
                ws.table("symbol_across_alter").symbol("b", "x").at(1, ChronoUnit.MICROS);
                Assert.assertTrue(sender.drain(10_000));

                for (int i = 0; i < 2; i++) {
                    ws.table("symbol_across_alter")
                            .symbol("b", i == 0 ? "TRUE" : "0")
                            .symbol("i", i == 0 ? "-2147483647" : "7")
                            .symbol("f", i == 0 ? "1.5" : "-0.25")
                            .symbol("dt", i == 0 ? DATE_TEXT_2 : DATE_TEXT_1)
                            .symbol("t", i == 0 ? "2024-02-29T12:34:56.789123Z" : "1969-12-31T23:59:59.999999Z")
                            .symbol("tn", i == 0 ? "2024-02-29T12:34:56.789123Z" : "1970-01-01T00:00:00.000001Z")
                            .symbol("ip", i == 0 ? "1.2.3.4" : "")
                            .symbol("u", i == 0 ? UUID_0 : UUID_1)
                            .symbol("ch", i == 0 ? "é" : "")
                            .at(10 + i, ChronoUnit.MICROS);
                }
                execute("ALTER TABLE symbol_across_alter ALTER COLUMN b TYPE BOOLEAN");
                execute("ALTER TABLE symbol_across_alter ALTER COLUMN i TYPE INT");
                execute("ALTER TABLE symbol_across_alter ALTER COLUMN f TYPE DOUBLE");
                execute("ALTER TABLE symbol_across_alter ALTER COLUMN dt TYPE DATE");
                execute("ALTER TABLE symbol_across_alter ALTER COLUMN t TYPE TIMESTAMP");
                execute("ALTER TABLE symbol_across_alter ALTER COLUMN tn TYPE TIMESTAMP_NS");
                execute("ALTER TABLE symbol_across_alter ALTER COLUMN ip TYPE IPV4");
                execute("ALTER TABLE symbol_across_alter ALTER COLUMN u TYPE UUID");
                execute("ALTER TABLE symbol_across_alter ALTER COLUMN ch TYPE CHAR");
                sender.flush();
                Assert.assertTrue(sender.drain(10_000));
                Assert.assertEquals(0, ws.getTotalReconnectAttempts());
            }
            drainWalQueue();
            assertQuery("SELECT b, i, f, dt, t, tn, ip, u, ch FROM symbol_across_alter WHERE ts > 1 ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b\ti\tf\tdt\tt\ttn\tip\tu\tch
                            true\t-2147483647\t1.5\t2024-02-29T12:34:56.789Z\t2024-02-29T12:34:56.789123Z\t2024-02-29T12:34:56.789123000Z\t1.2.3.4\t11111111-2222-3333-4444-555555555555\té
                            false\t7\t-0.25\t1969-12-31T23:59:59.999Z\t1969-12-31T23:59:59.999999Z\t1970-01-01T00:00:00.000001000Z\t\ta0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t
                            """);
        });
    }

    @Test
    public void testSymbolToParsedTypes() throws Exception {
        runInContext(port -> {
            staleCase("SYMBOL", "BOOLEAN")
                    .row(b -> b.symbol("c", "true"))
                    .row(b -> b.symbol("c", "FALSE"))
                    .row(b -> b.symbol("c", "1"))
                    .row(b -> b.symbol("c", "0"))
                    .nullRow()
                    .reject(b -> b.symbol("c", "yes"), "cannot parse boolean from string [value=yes")
                    .reject(b -> b.symbol("c", ""), "cannot parse boolean from string [value=")
                    .reject(b -> b.symbol("c", "x"), "cannot parse boolean from string [value=x")
                    .reject(b -> b.symbol("c", "nope"), "cannot parse boolean from string [value=nope")
                    .reject(b -> b.symbol("c", "falsy"), "cannot parse boolean from string [value=falsy")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\ttrue
                            1\tfalse
                            2\ttrue
                            3\tfalse
                            4\tfalse
                            """)
                    .run(port);
            staleCase("SYMBOL", "BYTE")
                    .row(b -> b.symbol("c", "127"))
                    .row(b -> b.symbol("c", "-128"))
                    .nullRow()
                    .reject(b -> b.symbol("c", "128"), "out of range for BYTE")
                    .reject(b -> b.symbol("c", "1.5"), "cannot parse BYTE from string [value=1.5")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t127
                            1\t-128
                            2\t0
                            """)
                    .run(port);
            staleCase("SYMBOL", "SHORT")
                    .row(b -> b.symbol("c", "-32768"))
                    .reject(b -> b.symbol("c", "32768"), "cannot parse SHORT from string [value=32768")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t-32768
                            """)
                    .run(port);
            staleCase("SYMBOL", "INT")
                    .row(b -> b.symbol("c", "2147483647"))
                    .row(b -> b.symbol("c", "-2147483647"))
                    .nullRow()
                    .reject(b -> b.symbol("c", "2147483648"), "cannot parse INT from string [value=2147483648")
                    .reject(b -> b.symbol("c", "żółć"), "cannot parse INT from string [value=żółć")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t2147483647
                            1\t-2147483647
                            2\tnull
                            """)
                    .run(port);
            staleCase("SYMBOL", "LONG")
                    .row(b -> b.symbol("c", "9223372036854775807"))
                    .row(b -> b.symbol("c", "1_000_000"))
                    .nullRow()
                    .reject(b -> b.symbol("c", "abc"), "cannot parse LONG from string [value=abc")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t9223372036854775807
                            1\t1000000
                            2\tnull
                            """)
                    .run(port);
            staleCase("SYMBOL", "FLOAT")
                    .row(b -> b.symbol("c", "0.1"))
                    .row(b -> b.symbol("c", "-3.4028235E38"))
                    .nullRow()
                    .reject(b -> b.symbol("c", "x1"), "cannot parse FLOAT from string [value=x1")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t0.1
                            1\t-3.4028235E38
                            2\tnull
                            """)
                    .run(port);
            staleCase("SYMBOL", "DOUBLE")
                    .row(b -> b.symbol("c", "0.1"))
                    .row(b -> b.symbol("c", "4.9E-324"))
                    .nullRow()
                    .reject(b -> b.symbol("c", "1.5.5"), "cannot parse DOUBLE from string [value=1.5.5")
                    // QuestDB prints Double.MIN_VALUE as 5.0E-324.
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t0.1
                            1\t5.0E-324
                            2\tnull
                            """)
                    .run(port);
            staleCase("SYMBOL", "DATE")
                    .row(b -> b.symbol("c", DATE_TEXT_2))
                    .row(b -> b.symbol("c", DATE_TEXT_1))
                    .nullRow()
                    .reject(b -> b.symbol("c", "2024-13-01"), "cannot parse DATE from string [value=2024-13-01")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t2024-02-29T12:34:56.789Z
                            1\t1969-12-31T23:59:59.999Z
                            2\t
                            """)
                    .run(port);
            staleCase("SYMBOL", "TIMESTAMP")
                    .row(b -> b.symbol("c", "2024-02-29T12:34:56.789123Z"))
                    .nullRow()
                    .reject(b -> b.symbol("c", "tomorrow"), "cannot parse timestamp from string [value=tomorrow")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t2024-02-29T12:34:56.789123Z
                            1\t
                            """)
                    .run(port);
            staleCase("SYMBOL", "TIMESTAMP_NS")
                    .row(b -> b.symbol("c", "1969-12-31T23:59:59.999999Z"))
                    .nullRow()
                    // TIMESTAMP_NS spans 1677-09-21 to 2262-04-11.
                    .reject(b -> b.symbol("c", "2300-01-01T00:00:00.000000Z"), "timestamp overflow converting micros to nanos: 10413792000000000")
                    .reject(b -> b.symbol("c", "1600-01-01T00:00:00.000000Z"), "timestamp overflow converting micros to nanos: -11676096000000000")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t1969-12-31T23:59:59.999999000Z
                            1\t
                            """)
                    .run(port);
            staleCase("SYMBOL", "UUID")
                    .row(b -> b.symbol("c", UUID_1))
                    .nullRow()
                    .reject(b -> b.symbol("c", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1"), "cannot parse UUID from string [value=a0eebc99")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\ta0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                            1\t
                            """)
                    .run(port);
            staleCase("SYMBOL", "CHAR")
                    .row(b -> b.symbol("c", "abc"))
                    .row(b -> b.symbol("c", "中文"))
                    .row(b -> b.symbol("c", ""))
                    .nullRow()
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\ta
                            1\t中
                            2\t
                            3\t
                            """)
                    .run(port);
        });
    }

    @Test
    public void testTemporalToSymbol() throws Exception {
        runInContext(port -> {
            staleCase("BOOLEAN", "SYMBOL")
                    .row(b -> b.boolColumn("c", true))
                    .row(b -> b.boolColumn("c", false))
                    .nullRow()
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\ttrue
                            1\tfalse
                            2\t
                            """)
                    .run(port);
            staleCase("DATE", "SYMBOL")
                    .row(b -> b.longColumn("c", 0))
                    .row(b -> b.longColumn("c", -1))
                    .row(b -> b.longColumn("c", 1_709_210_096_789L))
                    .nullRow()
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t1970-01-01T00:00:00.000Z
                            1\t1969-12-31T23:59:59.999Z
                            2\t2024-02-29T12:34:56.789Z
                            3\t
                            """)
                    .run(port);
            staleCase("TIMESTAMP", "SYMBOL")
                    .row(b -> b.timestampColumn("c", 1_709_210_096_789_123L, ChronoUnit.MICROS))
                    .row(b -> b.timestampColumn("c", -1, ChronoUnit.MICROS))
                    .nullRow()
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t2024-02-29T12:34:56.789123Z
                            1\t1969-12-31T23:59:59.999999Z
                            2\t
                            """)
                    .run(port);
            staleCase("TIMESTAMP_NS", "SYMBOL")
                    .row(b -> b.timestampColumn("c", 1_709_210_096_789_123_456L, ChronoUnit.NANOS))
                    .row(b -> b.timestampColumn("c", -1, ChronoUnit.NANOS))
                    .nullRow()
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t2024-02-29T12:34:56.789123456Z
                            1\t1969-12-31T23:59:59.999999999Z
                            2\t
                            """)
                    .run(port);
            staleCase("IPV4", "SYMBOL")
                    .row(b -> b.ipv4Column("c", "1.2.3.4"))
                    .row(b -> b.ipv4Column("c", "255.255.255.255"))
                    .row(b -> b.ipv4Column("c", "0.0.0.1"))
                    .nullRow()
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t1.2.3.4
                            1\t255.255.255.255
                            2\t0.0.0.1
                            3\t
                            """)
                    .run(port);
            staleCase("UUID", "SYMBOL")
                    .row(b -> b.uuidColumn("c", 0x4444555555555555L, 0x1111111122223333L))
                    .nullRow()
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t11111111-2222-3333-4444-555555555555
                            1\t
                            """)
                    .run(port);
        });
    }

    @Test
    public void testTextToIpv4() throws Exception {
        runInContext(port -> {
            staleCase("VARCHAR", "IPV4")
                    .row(b -> b.stringColumn("c", "1.2.3.4"))
                    .row(b -> b.stringColumn("c", "255.255.255.255"))
                    .row(b -> b.stringColumn("c", "0.0.0.0"))
                    .row(b -> b.stringColumn("c", ""))
                    .row(b -> b.stringColumn("c", "null"))
                    .nullRow()
                    .reject(b -> b.stringColumn("c", "256.0.0.1"), "cannot parse IPv4 from string [value=256.0.0.1")
                    .reject(b -> b.stringColumn("c", "1.2.3"), "cannot parse IPv4 from string [value=1.2.3")
                    .reject(b -> b.stringColumn("c", "żółć"), "cannot parse IPv4 from string [value=żółć")
                    .reject(b -> b.stringColumn("c", "."), "cannot parse IPv4 from string [value=.,")
                    .reject(b -> b.stringColumn("c", "...."), "cannot parse IPv4 from string [value=....,")
                    .row(b -> b.stringColumn("c", "..1.2.3.4.."))
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t1.2.3.4
                            1\t255.255.255.255
                            2\t
                            3\t
                            4\t
                            5\t
                            6\t1.2.3.4
                            """)
                    .run(port);
            staleCase("SYMBOL", "IPV4")
                    .row(b -> b.symbol("c", "10.0.0.1"))
                    .row(b -> b.symbol("c", ""))
                    .nullRow()
                    .reject(b -> b.symbol("c", "a.b.c.d"), "cannot parse IPv4 from string [value=a.b.c.d")
                    .expect("SELECT k, c FROM %s ORDER BY k", """
                            k\tc
                            0\t10.0.0.1
                            1\t
                            2\t
                            """)
                    .run(port);
        });
    }

    @Test
    public void testTimestampToDate() throws Exception {
        runInContext(port -> {
            // Truncates toward zero, as ALTER does.
            staleCase("TIMESTAMP", "DATE")
                    .row(b -> b.timestampColumn("c", 1_999, ChronoUnit.MICROS))
                    .row(b -> b.timestampColumn("c", -1_999, ChronoUnit.MICROS))
                    .row(b -> b.timestampColumn("c", 1_709_210_096_789_999L, ChronoUnit.MICROS))
                    .row(b -> b.timestampColumn("c", Long.MAX_VALUE, ChronoUnit.MICROS))
                    .row(b -> b.timestampColumn("c", Long.MIN_VALUE + 1, ChronoUnit.MICROS))
                    .nullRow()
                    .expect("SELECT k, c::LONG v FROM %s ORDER BY k", """
                            k\tv
                            0\t1
                            1\t-1
                            2\t1709210096789
                            3\t9223372036854775
                            4\t-9223372036854775
                            5\tnull
                            """)
                    .run(port);
            staleCase("TIMESTAMP_NS", "DATE")
                    .row(b -> b.timestampColumn("c", 1_999_999, ChronoUnit.NANOS))
                    .row(b -> b.timestampColumn("c", -1_999_999, ChronoUnit.NANOS))
                    .row(b -> b.timestampColumn("c", Long.MAX_VALUE, ChronoUnit.NANOS))
                    .nullRow()
                    .expect("SELECT k, c::LONG v FROM %s ORDER BY k", """
                            k\tv
                            0\t1
                            1\t-1
                            2\t9223372036854
                            3\tnull
                            """)
                    .run(port);
        });
    }

    @Test
    public void testTimestampToNumeric() throws Exception {
        runInContext(port -> {
            for (String from : new String[]{"TIMESTAMP", "TIMESTAMP_NS"}) {
                ChronoUnit unit = from.equals("TIMESTAMP") ? ChronoUnit.MICROS : ChronoUnit.NANOS;
                staleCase(from, "BYTE")
                        .row(b -> b.timestampColumn("c", 127, unit))
                        .row(b -> b.timestampColumn("c", -128, unit))
                        .nullRow()
                        .reject(b -> b.timestampColumn("c", 128, unit), "integer value 128 out of range for BYTE")
                        .expect("SELECT k, c FROM %s ORDER BY k", """
                                k\tc
                                0\t127
                                1\t-128
                                2\t0
                                """)
                        .run(port);
                staleCase(from, "SHORT")
                        .row(b -> b.timestampColumn("c", -32768, unit))
                        .nullRow()
                        .reject(b -> b.timestampColumn("c", -32769, unit), "integer value -32769 out of range for SHORT")
                        .expect("SELECT k, c FROM %s ORDER BY k", """
                                k\tc
                                0\t-32768
                                1\t0
                                """)
                        .run(port);
                staleCase(from, "INT")
                        .row(b -> b.timestampColumn("c", Integer.MAX_VALUE, unit))
                        .row(b -> b.timestampColumn("c", Integer.MIN_VALUE + 1, unit))
                        .nullRow()
                        .reject(b -> b.timestampColumn("c", 86_400_000_000L, unit), "integer value 86400000000 out of range for INT")
                        .expect("SELECT k, c FROM %s ORDER BY k", """
                                k\tc
                                0\t2147483647
                                1\t-2147483647
                                2\tnull
                                """)
                        .run(port);
                staleCase(from, "LONG")
                        .row(b -> b.timestampColumn("c", Long.MAX_VALUE, unit))
                        .row(b -> b.timestampColumn("c", Long.MIN_VALUE + 1, unit))
                        .row(b -> b.timestampColumn("c", 0, unit))
                        .nullRow()
                        .expect("SELECT k, c FROM %s ORDER BY k", """
                                k\tc
                                0\t9223372036854775807
                                1\t-9223372036854775807
                                2\t0
                                3\tnull
                                """)
                        .run(port);
                staleCase(from, "FLOAT")
                        .row(b -> b.timestampColumn("c", 16_777_217L, unit))
                        .nullRow()
                        .expect("SELECT k, c FROM %s ORDER BY k", """
                                k\tc
                                0\t1.6777216E7
                                1\tnull
                                """)
                        .run(port);
                staleCase(from, "DOUBLE")
                        .row(b -> b.timestampColumn("c", 9_007_199_254_740_993L, unit))
                        .row(b -> b.timestampColumn("c", -1, unit))
                        .nullRow()
                        .expect("SELECT k, c FROM %s ORDER BY k", """
                                k\tc
                                0\t9.007199254740992E15
                                1\t-1.0
                                2\tnull
                                """)
                        .run(port);
            }
        });
    }

    @Test
    public void testTimestampToTextKeepsMillisecondFormat() throws Exception {
        // The server has always written a TIMESTAMP value to a text column at millisecond
        // precision; ALTER writes the column's own precision. A frame encoded before the
        // ALTER keeps the server's format.
        runInContext(port -> {
            for (String to : new String[]{"STRING", "VARCHAR"}) {
                staleCase("TIMESTAMP", to)
                        .row(b -> b.timestampColumn("c", 1_709_210_096_789_123L, ChronoUnit.MICROS))
                        .nullRow()
                        .expect("SELECT k, c FROM %s ORDER BY k", """
                                k\tc
                                0\t2024-02-29T12:34:56.789Z
                                1\t
                                """)
                        .run(port);
                staleCase("TIMESTAMP_NS", to)
                        .row(b -> b.timestampColumn("c", 1_709_210_096_789_123_456L, ChronoUnit.NANOS))
                        .nullRow()
                        .expect("SELECT k, c FROM %s ORDER BY k", """
                                k\tc
                                0\t2024-02-29T12:34:56.789Z
                                1\t
                                """)
                        .run(port);
            }
        });
    }

    private static byte[] encode(QwpWebSocketEncoder encoder, QwpTableBuffer buffer) {
        encoder.getBuffer().reset();
        int length = encoder.encode(buffer);
        byte[] bytes = new byte[length];
        long address = encoder.getBuffer().getBufferPtr();
        for (int i = 0; i < length; i++) {
            bytes[i] = Unsafe.getByte(address + i);
        }
        buffer.reset();
        return bytes;
    }

    private static boolean isTimestampToText(String from, String to) {
        return from.startsWith("TIMESTAMP") && (to.equals("STRING") || to.equals("VARCHAR"));
    }

    private static String[] textValues(String to) {
        return switch (to) {
            case "BOOLEAN" -> new String[]{"true", "false", "TRUE"};
            case "BYTE", "SHORT", "INT", "LONG" -> new String[]{"7", "-3", "0"};
            case "FLOAT", "DOUBLE" -> new String[]{"7", "-3", "0.5"};
            case "DATE" -> new String[]{DATE_TEXT_0, DATE_TEXT_1, DATE_TEXT_2};
            case "TIMESTAMP", "TIMESTAMP_NS" ->
                    new String[]{"1970-01-02T00:00:00.000000Z", "1969-12-31T23:59:59.999999Z", "2024-02-29T12:34:56.789123Z"};
            case "IPV4" -> new String[]{"1.2.3.4", "255.255.255.255", "10.0.0.1"};
            case "UUID" -> new String[]{UUID_0, UUID_1, UUID_0};
            case "CHAR" -> new String[]{"a", "é", "Z"};
            case "STRING", "VARCHAR", "SYMBOL" -> new String[]{"abc", "żółć", "x"};
            default -> new String[]{"1.5", "-2.5", "0"};
        };
    }

    // Writes value i of the matrix row set to column name of type from; the set is
    // valid for every type that ALTER converts from to.
    private static void writeMatrixValue(QwpSchemaBinding b, String name, String from, String to, int i) {
        switch (from) {
            case "BOOLEAN" -> b.boolColumn(name, i != 1);
            case "BYTE", "SHORT", "INT", "LONG", "DATE" -> b.longColumn(name, new long[]{7, -3, 0}[i]);
            case "FLOAT", "DOUBLE" -> b.doubleColumn(name, new double[]{7, -3, 0}[i]);
            case "TIMESTAMP" -> b.timestampColumn(name, new long[]{7, -3, 0}[i], ChronoUnit.MICROS);
            case "TIMESTAMP_NS" -> b.timestampColumn(name, new long[]{7, -3, 0}[i], ChronoUnit.NANOS);
            case "STRING", "VARCHAR" -> b.stringColumn(name, textValues(to)[i]);
            case "SYMBOL" -> b.symbol(name, textValues(to)[i]);
            case "IPV4" -> b.ipv4Column(name, textValues("IPV4")[i]);
            case "UUID" -> b.uuidColumn(name, 0x4444555555555555L + i, 0x1111111122223333L);
            case "CHAR" -> b.charColumn(name, textValues("CHAR")[i].charAt(0));
            default -> b.decimalColumn(name, textValues(from)[i], new Decimal256());
        }
    }

    // A table with one column of type from per type in TYPES receives a frame encoded
    // before and one encoded after ALTER converts each column to its type, where ALTER
    // supports that. ALTER converts the first frame's rows, the server the second
    // frame's, and both must store the same values. A NULL row stores the NULL of the
    // new type: a BOOLEAN, BYTE or SHORT column cannot hold NULL, so ALTER sees false
    // or 0 there instead.
    private void assertTypeChangesMatchAlter(int port, String from, Set<String> pairs, List<String> failures) throws Exception {
        String table = "stale_" + tableCount++;
        StringBuilder ddl = new StringBuilder("CREATE TABLE ").append(table).append(" (k LONG");
        for (int t = 0; t < TYPES.length; t++) {
            ddl.append(", c").append(t).append(' ').append(from);
        }
        execute(ddl.append(", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL"));
        byte[] before;
        byte[] after;
        try (WebSocketClient client = connectSchemaClient(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer buffer = new QwpTableBuffer(table)) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, describeKnown(client, 1, table));
            before = encodeMatrixFrame(encoder, buffer, binding, from, 0);
            after = encodeMatrixFrame(encoder, buffer, binding, from, 100);
            send(client, before);
            WebSocketResponse response = receiveResponse(client);
            Assert.assertTrue(response.getErrorMessage(), response.isSuccess());
        }
        List<Integer> altered = new ArrayList<>();
        for (int t = 0; t < TYPES.length; t++) {
            if (TYPES[t].equals(from)) {
                continue;
            }
            try {
                execute("ALTER TABLE " + table + " ALTER COLUMN c" + t + " TYPE " + TYPES[t]);
                altered.add(t);
                pairs.add(from + "->" + TYPES[t]);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "incompatible column type change");
            }
        }
        try (WebSocketClient client = connectSchemaClient(port)) {
            send(client, after);
            WebSocketResponse response = receiveResponse(client);
            if (!response.isSuccess()) {
                failures.add(from + ": " + response.getErrorMessage());
                return;
            }
        }
        drainWalQueue();
        for (int i = 0, n = altered.size(); i < n; i++) {
            int t = altered.get(i);
            String to = TYPES[t];
            String converted = query("SELECT c" + t + " c FROM " + table + " WHERE k < 100 ORDER BY k");
            String stale = query("SELECT c" + t + " c FROM " + table + " WHERE k >= 100 ORDER BY k");
            String typedNull = query("SELECT NULL::" + to + " c");
            String[] convertedRows = converted.split("\n", -1);
            // header, 3 values, NULL, trailing empty string
            Assert.assertEquals(from + " -> " + to + ": " + converted, 6, convertedRows.length);
            String expected;
            if (isTimestampToText(from, to)) {
                // See testTimestampToTextKeepsMillisecondFormat. -3 ns truncates to 0 micros.
                expected = from.equals("TIMESTAMP")
                        ? "c\n1970-01-01T00:00:00.000Z\n1969-12-31T23:59:59.999Z\n1970-01-01T00:00:00.000Z\n"
                        : "c\n1970-01-01T00:00:00.000Z\n1970-01-01T00:00:00.000Z\n1970-01-01T00:00:00.000Z\n";
            } else {
                expected = convertedRows[0] + '\n' + convertedRows[1] + '\n' + convertedRows[2] + '\n' + convertedRows[3] + '\n';
            }
            expected += typedNull.split("\n", -1)[1] + '\n';
            if (!expected.equals(stale)) {
                failures.add(from + " -> " + to + ": expected [" + expected.replace('\n', '|')
                        + "] stale [" + stale.replace('\n', '|') + ']');
            }
        }
    }

    private void assertLegacyRejected(int port, String columnType, Consumer<LegacyRow> writer, String message) throws Exception {
        String table = "legacy_reject_" + tableCount++;
        execute("CREATE TABLE " + table + " (c " + columnType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        try (WebSocketClient client = QwpSchemaE2ETestUtils.connectLegacyClient(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer buffer = new QwpTableBuffer(table)) {
            writer.accept(type -> buffer.getOrCreateColumn("c", type, true));
            buffer.nextRow();
            send(client, encode(encoder, buffer));
            WebSocketResponse response = receiveResponse(client);
            Assert.assertFalse(message, response.isSuccess());
            Assert.assertEquals(message, WebSocketResponse.STATUS_SCHEMA_MISMATCH, response.getStatus());
            TestUtils.assertContains(response.getErrorMessage(), message);
        }
        drainWalQueue();
        assertQuery("SELECT count() FROM " + table).noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
    }

    private void assertLargeFrameMatchesAlter(
            int port,
            String from,
            String to,
            ObjIntConsumer<QwpSchemaBinding> writer
    ) throws Exception {
        final int rowCount = 1_000;
        String table = "stale_large_" + tableCount++;
        execute("CREATE TABLE " + table + " (k LONG, c " + from + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        byte[] before;
        byte[] after;
        try (WebSocketClient client = connectSchemaClient(port);
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer buffer = new QwpTableBuffer(table)) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, describeKnown(client, 1, table));
            for (int frame = 0; frame < 2; frame++) {
                for (int i = 0; i < rowCount; i++) {
                    binding.longColumn("k", frame * 1_000_000L + i);
                    if (i % 7 != 3) {
                        writer.accept(binding, i);
                    }
                    buffer.nextRow();
                }
                if (frame == 0) {
                    before = encode(encoder, buffer);
                    send(client, before);
                    WebSocketResponse response = receiveResponse(client);
                    Assert.assertTrue(response.getErrorMessage(), response.isSuccess());
                }
            }
            after = encode(encoder, buffer);
        }
        execute("ALTER TABLE " + table + " ALTER COLUMN c TYPE " + to);
        try (WebSocketClient client = connectSchemaClient(port)) {
            send(client, after);
            WebSocketResponse response = receiveResponse(client);
            Assert.assertTrue(from + " -> " + to + ": " + response.getErrorMessage(), response.isSuccess());
        }
        drainWalQueue();
        assertQuery("SELECT count() FROM " + table).noLeakCheck().noRandomAccess().expectSize().returns("count\n" + 2 * rowCount + '\n');
        assertSqlCursors(
                "SELECT c FROM " + table + " WHERE k < 1_000_000 ORDER BY k",
                "SELECT c FROM " + table + " WHERE k >= 1_000_000 ORDER BY k"
        );
    }

    private static byte[] encodeMatrixFrame(
            QwpWebSocketEncoder encoder,
            QwpTableBuffer buffer,
            QwpSchemaBinding binding,
            String from,
            int firstKey
    ) {
        for (int i = 0; i < 3; i++) {
            binding.longColumn("k", firstKey + i);
            for (int t = 0; t < TYPES.length; t++) {
                writeMatrixValue(binding, "c" + t, from, TYPES[t], i);
            }
            buffer.nextRow();
        }
        binding.longColumn("k", firstKey + 3);
        buffer.nextRow();
        return encode(encoder, buffer);
    }

    private static String query(String sql) throws SqlException {
        sink.clear();
        printSql(sql);
        return sink.toString();
    }

    private static void send(WebSocketClient client, byte[] bytes) {
        long address = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.putByte(address + i, bytes[i]);
            }
            client.sendBinary(address, bytes.length);
        } finally {
            Unsafe.free(address, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private StaleCase staleCase(String from, String to) {
        return new StaleCase(from, to);
    }

    @FunctionalInterface
    private interface LegacyRow {
        QwpTableBuffer.ColumnBuffer column(byte type);
    }

    // Rows a sender encodes against a column of type from before ALTER converts the
    // column to type to, and the frames it sends after the ALTER.
    private final class StaleCase {
        private final String from;
        private final List<String> rejectMessages = new ArrayList<>();
        private final List<Consumer<QwpSchemaBinding>> rejects = new ArrayList<>();
        private final List<Consumer<QwpSchemaBinding>> rows = new ArrayList<>();
        private final String to;
        private String expected;
        private String expectedSql;
        private byte patchFrom;
        private byte patchTo;

        private StaleCase(String from, String to) {
            this.from = from;
            this.to = to;
        }

        StaleCase expect(String sql, String expected) {
            this.expectedSql = sql;
            this.expected = expected;
            return this;
        }

        StaleCase nullRow() {
            rows.add(null);
            return this;
        }

        // Replaces the accepted frame's only byte that equals from.
        StaleCase patch(byte from, byte to) {
            this.patchFrom = from;
            this.patchTo = to;
            return this;
        }

        StaleCase reject(Consumer<QwpSchemaBinding> writer, String message) {
            rejects.add(writer);
            rejectMessages.add(message);
            return this;
        }

        StaleCase row(Consumer<QwpSchemaBinding> writer) {
            rows.add(writer);
            return this;
        }

        // Rejected frames go first, each on its own connection: the table and the
        // server must take the accepted frame after them.
        void run(int port) throws Exception {
            String table = "stale_case_" + tableCount++;
            String caseId = from + " -> " + to;
            execute("CREATE TABLE " + table + " (k LONG, c " + from + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            byte[] accepted;
            List<byte[]> rejected = new ArrayList<>();
            try (WebSocketClient client = connectSchemaClient(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer(table)) {
                QwpSchemaBinding binding = new QwpSchemaBinding(buffer, describeKnown(client, 1, table));
                for (int i = 0, n = rows.size(); i < n; i++) {
                    binding.longColumn("k", i);
                    Consumer<QwpSchemaBinding> row = rows.get(i);
                    if (row != null) {
                        row.accept(binding);
                    }
                    buffer.nextRow();
                }
                accepted = encode(encoder, buffer);
                if (patchFrom != patchTo) {
                    int at = -1;
                    for (int i = 0; i < accepted.length; i++) {
                        if (accepted[i] == patchFrom) {
                            Assert.assertEquals(caseId + ": the patched byte must be unique", -1, at);
                            at = i;
                        }
                    }
                    Assert.assertTrue(caseId, at > -1);
                    accepted[at] = patchTo;
                }
                for (int i = 0, n = rejects.size(); i < n; i++) {
                    binding.longColumn("k", 1_000 + i);
                    rejects.get(i).accept(binding);
                    buffer.nextRow();
                    rejected.add(encode(encoder, buffer));
                }
            }
            execute("ALTER TABLE " + table + " ALTER COLUMN c TYPE " + to);
            for (int i = 0, n = rejected.size(); i < n; i++) {
                try (WebSocketClient client = connectSchemaClient(port)) {
                    send(client, rejected.get(i));
                    WebSocketResponse response = receiveResponse(client);
                    Assert.assertFalse(caseId, response.isSuccess());
                    Assert.assertEquals(caseId + ": " + response.getErrorMessage(),
                            WebSocketResponse.STATUS_SCHEMA_MISMATCH, response.getStatus());
                    TestUtils.assertContains(caseId, response.getErrorMessage(), rejectMessages.get(i));
                }
            }
            try (WebSocketClient client = connectSchemaClient(port)) {
                send(client, accepted);
                WebSocketResponse response = receiveResponse(client);
                Assert.assertTrue(caseId + ": " + response.getErrorMessage(), response.isSuccess());
            }
            drainWalQueue();
            assertQuery(String.format(expectedSql, table)).noLeakCheck().expectSize().returns(expected);
        }
    }
}
