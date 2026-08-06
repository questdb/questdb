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

package io.questdb.test.cutlass.pgwire;

import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Drives the extended query protocol over a raw socket so the test can choose the result format
 * codes per column and validate DataRow framing byte for byte. pgjdbc does send per-column codes,
 * but PGStream.receiveTupleV3() reads each field by its own length prefix and never reconciles it
 * against the DataRow length, so the driver cannot observe a mis-declared row at all.
 */
public class PGResultFormatCodesTest extends BasePGTest {

    private static final short FORMAT_BINARY = 1;
    private static final short FORMAT_TEXT = 0;
    private static final int MAX_MESSAGE_LEN = 1 << 20;
    private static final int PG_OID_VARCHAR_ARRAY = 1015;
    private static final int PROTOCOL_VERSION_3_0 = 196_608;
    private static final int SOCKET_TIMEOUT_MS = 10_000;

    @Before
    public void setUp() {
        super.setUp();
        // Pad widths below are chosen against this, and the two sizes mean different things:
        //   400  - the row fits alone but two do not, so a row STRADDLES the buffer. That drives
        //          outRecord() down the "predict the tail size and patch the length prefix" path,
        //          which is where DataRow framing goes wrong.
        //   4000 - the row does not fit even in an empty buffer, so outRecord() gives up and calls
        //          estimateRecordSize() to name a required size. That is the only way to reach
        //          estimateColumnTxtSize().
        // Watch out when asserting the reported requiredSize: PGConnectionContext floors it at
        // 2 * sendBufferSize, so a row needing less than 2048 bytes proves nothing about the
        // estimate - the floor answers instead.
        sendBufferSize = 1024;
    }

    @Test
    public void testArrayWithNullElementsDeclaresCorrectRowLength() throws Exception {
        // A NULL element renders as the 4-byte "NULL" literal instead of a up-to-24-byte double
        // literal, which moves both the patched DataRow length and the size estimate.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            execute("""
                    CREATE TABLE nulls AS (
                      SELECT rnd_str(400, 400, 0) AS pad,
                             rnd_double_array(1, 2, 0, 6) AS a1
                      FROM long_sequence(20))""");

            final String sql = "SELECT pad, a1 FROM nulls";
            try (RawPGClient client = new RawPGClient(port)) {
                ObjList<ObjList<String>> reference = client.query(sql, formats(1, FORMAT_TEXT));
                ObjList<ObjList<String>> perColumn = client.query(sql, formats(2, FORMAT_TEXT));
                Assert.assertEquals(20, reference.size());
                int rowsWithNullElement = 0;
                for (int i = 0; i < 20; i++) {
                    if (perColumn.getQuick(i).getQuick(1).contains("NULL")) {
                        rowsWithNullElement++;
                    }
                }
                Assert.assertTrue("expected some NULL array elements", rowsWithNullElement > 0);
                assertRowsEqual(reference, perColumn);
            }
        });
    }

    @Test
    public void testBinaryIntervalAndDecimalBehindResumableVarcharRewindCleanly() throws Exception {
        // A VARCHAR first column is exempt from the "doesn't fit" bail-out because it can be sent in
        // chunks, so calculateRecordTailSize() walks PAST it and sizes the later columns. That is the
        // only way to reach its INTERVAL and DECIMAL arms - a STRING first column returns -1 straight
        // away and the walk never gets there. Those arms used to be "assert false", which fired
        // inside outRecord()'s overflow handler and replaced the exception being handled.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            execute("""
                    CREATE TABLE resumable AS (
                      SELECT rnd_str(4000, 4000, 0)::varchar AS v
                      FROM long_sequence(1))""");

            final String[] queries = {
                    "SELECT v, interval('2020-01-01', '2021-01-01') AS i FROM resumable",
                    "SELECT v, 1.5::decimal(10, 2) AS d FROM resumable"
            };
            try (RawPGClient client = new RawPGClient(port)) {
                for (String sql : queries) {
                    try {
                        // text VARCHAR + binary second column: mixed per-column formats
                        client.query(sql, new short[]{FORMAT_TEXT, FORMAT_BINARY});
                        Assert.fail("expected a send-buffer error for: " + sql);
                    } catch (AssertionError e) {
                        TestUtils.assertContains(e.getMessage(), "not enough space in send buffer");
                    }
                }
            }
        });
    }

    @Test
    public void testBinaryStridedDoubleArrayDeclaresCorrectRowLength() throws Exception {
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            execute("""
                    CREATE TABLE strided AS (
                      SELECT rnd_double_array(2, 0, 0, 10, 10) AS a
                      FROM long_sequence(2))""");

            try (RawPGClient client = new RawPGClient(port)) {
                ObjList<ObjList<String>> rows = client.query(
                        "SELECT a[3:,3:] FROM strided",
                        formats(1, FORMAT_BINARY)
                );
                Assert.assertEquals(2, rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    Assert.assertEquals(1, rows.getQuick(i).size());
                }
            }
        }, () -> sendBufferSize = 512);
    }

    @Test
    public void testBinaryVarcharArrayIsRejectedWithActionableError() throws Exception {
        // outColBinArr() only encodes fixed-width DOUBLE and LONG elements. A varchar array reaches
        // a projection through a bind variable, and asking for it in binary used to trip an
        // unconditional AssertionError inside countNotNull() - a critical log line and an opaque
        // "Unsupported array element type: 26" for what is a plain limitation.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            try (RawPGClient client = new RawPGClient(port)) {
                try {
                    client.queryParam("SELECT $1 AS arr", PG_OID_VARCHAR_ARRAY, "{hello,world}", FORMAT_BINARY);
                    Assert.fail("expected a rejection");
                } catch (AssertionError e) {
                    TestUtils.assertContains(e.getMessage(), "binary result format is not supported for arrays");
                    TestUtils.assertContains(e.getMessage(), "VARCHAR");
                }
                // the text format is the supported path and must keep working
                ObjList<ObjList<String>> rows =
                        client.queryParam("SELECT $1 AS arr", PG_OID_VARCHAR_ARRAY, "{hello,world}", FORMAT_TEXT);
                Assert.assertEquals(1, rows.size());
                Assert.assertEquals("{hello,world}", rows.getQuick(0).getQuick(0));
            }
        });
    }

    @Test
    public void testEmptyArrayDeclaresCorrectRowLength() throws Exception {
        // arrayToText() short-circuits an empty array to "{}" whatever its shape, so the shape walk
        // that sizes the literal must not run for it.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            execute("""
                    CREATE TABLE emptyarr AS (
                      SELECT rnd_str(400, 400, 0) AS pad,
                             rnd_double_array(1, 0, 0, 0) AS a1
                      FROM long_sequence(20))""");

            final String sql = "SELECT pad, a1 FROM emptyarr";
            try (RawPGClient client = new RawPGClient(port)) {
                ObjList<ObjList<String>> perColumn = client.query(sql, formats(2, FORMAT_TEXT));
                Assert.assertEquals(20, perColumn.size());
                for (int i = 0; i < 20; i++) {
                    Assert.assertEquals("{}", perColumn.getQuick(i).getQuick(1));
                }
            }
        });
    }

    @Test
    public void testGeoHashIPv4AndNullInBinaryFormatAreNotDroppedFromTheRow() throws Exception {
        // pgwire advertises IPv4, every geohash width and a NULL-typed column as PG_VARCHAR, so a
        // driver that asks for binary on the types it can decode asks for binary here. Each of
        // these needs its own BINARY_TYPE_* label in outRecord(): without one the arm falls to
        // "default" and emits no bytes for a field the DataRow header still counts.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            execute("""
                    CREATE TABLE geo AS (
                      SELECT rnd_ipv4() AS ip,
                             rnd_geohash(5) AS g1,
                             rnd_geohash(15) AS g3,
                             rnd_geohash(30) AS g6,
                             rnd_geohash(60) AS g12,
                             rnd_geohash(3) AS gbits
                      FROM long_sequence(19))""");
            // an all-NULL row, so the NULL arms of the IPv4 and geohash writers are covered too
            execute("INSERT INTO geo VALUES (null, null, null, null, null, null)");

            // "null AS n" has to sit in the projection, not in the table: only there does the
            // column keep ColumnType.NULL, which is the tag that had no binary label
            final String sql = "SELECT ip, g1, g3, g6, g12, gbits, null AS n FROM geo";
            try (RawPGClient client = new RawPGClient(port)) {
                ObjList<ObjList<String>> asText = client.query(sql, formats(7, FORMAT_TEXT));
                ObjList<ObjList<String>> asBinary = client.query(sql, formats(7, FORMAT_BINARY));

                Assert.assertEquals(20, asText.size());
                Assert.assertEquals(20, asBinary.size());
                // assert the shape of the values independently, so a regression that broke BOTH
                // renderings the same way cannot slip through assertRowsEqual()
                int nullRows = 0;
                for (int i = 0; i < 20; i++) {
                    ObjList<String> row = asBinary.getQuick(i);
                    Assert.assertEquals("column count of row " + i, 7, row.size());
                    Assert.assertNull("n must be NULL", row.getQuick(6));
                    if (row.getQuick(0) == null) {
                        nullRows++;
                        for (int j = 0; j < 6; j++) {
                            Assert.assertNull("row " + i + " column " + j, row.getQuick(j));
                        }
                        continue;
                    }
                    Assert.assertTrue("ip " + row.getQuick(0), row.getQuick(0).matches("\\d+\\.\\d+\\.\\d+\\.\\d+"));
                    Assert.assertEquals("g1", 1, row.getQuick(1).length());
                    Assert.assertEquals("g3", 3, row.getQuick(2).length());
                    Assert.assertEquals("g6", 6, row.getQuick(3).length());
                    Assert.assertEquals("g12", 12, row.getQuick(4).length());
                    // 3 bits is not a whole number of base-32 chars, so it renders as a bit string
                    Assert.assertTrue("gbits " + row.getQuick(5), row.getQuick(5).matches("[01]{3}"));
                }
                Assert.assertEquals("the all-NULL row must be present", 1, nullRows);
                // these types have no distinct binary encoding: they go out as varchar text under
                // either format code, so the two result sets must also agree
                assertRowsEqual(asText, asBinary);
            }
        });
    }

    @Test
    public void testLong128IsRejectedInsteadOfDroppingTheField() throws Exception {
        // LONG128 had no arm in outRecord()'s switch at all - not even a text one - so it fell to
        // "default" and wrote no bytes for a counted field. That desynchronised the client in plain
        // text mode, which is what every driver uses by default. No egress path renders LONG128, so
        // the query must fail cleanly instead.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            final String sql = "SELECT to_long128(1L, 2L) AS v, x FROM long_sequence(3)";
            try (RawPGClient client = new RawPGClient(port)) {
                for (short code : new short[]{FORMAT_TEXT, FORMAT_BINARY}) {
                    try {
                        client.query(sql, formats(2, code));
                        Assert.fail("expected a rejection for format code " + code);
                    } catch (AssertionError e) {
                        TestUtils.assertContains(e.getMessage(), "unsupported column type in result set");
                        TestUtils.assertContains(e.getMessage(), "LONG128");
                    }
                }
            }
        });
    }

    @Test
    public void testNullArrayBehindOverBufferVarcharStillStreams() throws Exception {
        // A NULL array renders the same 4-byte NULL marker under either format code, so its tail
        // contribution is still exact and the row remains resumable across send buffers. Bailing
        // out of calculateRecordTailSize() for every text array would turn this row into an error.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            execute("""
                    CREATE TABLE nullarr AS (
                      SELECT rnd_str(4000, 4000, 0)::varchar AS v,
                             null::double[] AS a
                      FROM long_sequence(1))""");

            try (RawPGClient client = new RawPGClient(port)) {
                ObjList<ObjList<String>> rows = client.query("SELECT v, a FROM nullarr", formats(2, FORMAT_TEXT));
                Assert.assertEquals(1, rows.size());
                Assert.assertEquals(4000, rows.getQuick(0).getQuick(0).length());
                Assert.assertNull(rows.getQuick(0).getQuick(1));
            }
        });
    }

    @Test
    public void testOverBufferRowWithTextArrayReportsSufficientRequiredSize() throws Exception {
        // A text array cannot be split across send buffers (outColTxtArr() renders atomically via
        // NoopArrayWriteState), so a row that does not fit on its own must fail with an explicit
        // error naming the size to configure. Two things are under test:
        //   1. estimateColumnTxtSize() has an ARRAY arm at all - without one estimateRecordSize()
        //      hits "assert false" and the client gets an internal error instead of this message;
        //   2. the size it names really is an upper bound. The array below has shape (2,1,1) with
        //      max-width double literals, which is the case a flat per-element brace allowance
        //      under-counts: arrayToText() emits one brace pair per shape-tree node (5 here), not
        //      one per element (2).
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            execute("""
                    CREATE TABLE big AS (
                      SELECT rnd_str(4000, 4000, 0) AS pad,
                             ARRAY[[[-1.2345678901234567E-308]], [[-1.2345678901234567E-308]]] AS a1
                      FROM long_sequence(1))""");

            // the true rendered widths, read back through a query small enough to fit the buffer
            final int padLen;
            final int arrLen;
            try (PreparedStatement stmt = connection.prepareStatement(
                    "SELECT length(pad), length(a1::varchar) FROM big");
                 ResultSet rs = stmt.executeQuery()) {
                Assert.assertTrue(rs.next());
                padLen = rs.getInt(1);
                arrLen = rs.getInt(2);
            }
            // estimateRecordSize() sums a 4-byte length prefix plus the value for each column
            final int trueRowSize = (Integer.BYTES + padLen) + (Integer.BYTES + arrLen);

            try (RawPGClient client = new RawPGClient(port)) {
                try {
                    client.query("SELECT pad, a1 FROM big", formats(2, FORMAT_TEXT));
                    Assert.fail("expected a send-buffer error");
                } catch (AssertionError e) {
                    final String message = e.getMessage();
                    TestUtils.assertContains(message, "not enough space in send buffer");
                    TestUtils.assertContains(message, "sendBufferSize=1024");
                    final Matcher m = Pattern.compile("requiredSize=(\\d+)").matcher(message);
                    Assert.assertTrue("no requiredSize in: " + message, m.find());
                    final long requiredSize = Long.parseLong(m.group(1));
                    Assert.assertTrue(
                            "requiredSize=" + requiredSize + " must cover the row's " + trueRowSize
                                    + " bytes (pad=" + padLen + ", array literal=" + arrLen + ")",
                            requiredSize >= trueRowSize
                    );
                }
            }
        });
    }

    @Test
    public void testOversizedEmptyArrayReportsSufficientRequiredSize() throws Exception {
        // Reaching arrayTxtSize() needs a row that does not fit the send buffer on its own. The
        // empty-array short-circuit matters because arrayToText() renders "{}" whatever the shape:
        // without it, a shape carrying a zero dimension counts astronomically many phantom braces.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            execute("""
                    CREATE TABLE oversized AS (
                      SELECT rnd_str(4000, 4000, 0) AS pad,
                             rnd_double_array(1, 0, 0, 0) AS empty
                      FROM long_sequence(1))""");

            final int emptyLen = renderedByteLength(connection, "SELECT empty FROM oversized");
            Assert.assertEquals("an empty array renders as {}", 2, emptyLen);

            try (RawPGClient client = new RawPGClient(port)) {
                assertRequiredSizeCoversRow(client, "SELECT pad, empty FROM oversized", 4000, emptyLen);
            }
        });
    }

    @Test
    public void testOversizedVarcharArrayReportsSufficientRequiredSize() throws Exception {
        // A varchar element has no width bound, so arrayTxtSize() measures the elements instead of
        // charging them the 24-byte double budget. The literal has to clear 2 * sendBufferSize too,
        // or the floor PGConnectionContext applies would answer instead of the estimate.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            final StringBuilder big = new StringBuilder("{");
            for (int i = 0; i < 4_000; i++) {
                big.append('a');
            }
            big.append('}');
            final String literal = big.toString();

            try (RawPGClient client = new RawPGClient(port)) {
                try {
                    client.queryParam("SELECT $1 AS arr", PG_OID_VARCHAR_ARRAY, literal, FORMAT_TEXT);
                    Assert.fail("expected a send-buffer error");
                } catch (AssertionError e) {
                    final String message = e.getMessage();
                    TestUtils.assertContains(message, "not enough space in send buffer");
                    final Matcher m = Pattern.compile("requiredSize=(\\d+)").matcher(message);
                    Assert.assertTrue("no requiredSize in: " + message, m.find());
                    // the literal round-trips unchanged, so the field needs its length plus a prefix
                    Assert.assertTrue(
                            "requiredSize=" + m.group(1) + " must cover the " + literal.length()
                                    + "-byte array literal",
                            Long.parseLong(m.group(1)) >= Integer.BYTES + literal.length()
                    );
                }
            }
        });
    }

    @Test
    public void testSingleBinaryFormatCodeAppliesToEveryColumn() throws Exception {
        // One format code covers all columns (getPgResultSetColumnFormatCode() reads codes.get(0)
        // when the count is <= 1), which is the shape libpq-style binary clients send. It reaches
        // the same BINARY_TYPE_* labels as per-column codes, so it needs its own case.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            execute("""
                    CREATE TABLE geo1 AS (
                      SELECT rnd_ipv4() AS ip,
                             rnd_geohash(30) AS g6
                      FROM long_sequence(5))""");

            final String sql = "SELECT ip, g6, null AS n FROM geo1";
            try (RawPGClient client = new RawPGClient(port)) {
                ObjList<ObjList<String>> allBinary = client.query(sql, formats(1, FORMAT_BINARY));
                ObjList<ObjList<String>> allText = client.query(sql, formats(1, FORMAT_TEXT));
                Assert.assertEquals(5, allBinary.size());
                for (int i = 0; i < 5; i++) {
                    Assert.assertEquals(3, allBinary.getQuick(i).size());
                    Assert.assertEquals(6, allBinary.getQuick(i).getQuick(1).length());
                    Assert.assertNull(allBinary.getQuick(i).getQuick(2));
                }
                assertRowsEqual(allText, allBinary);
            }
        });
    }

    @Test
    public void testTextArrayShorterThanBinary() throws Exception {
        // The other sign of the same defect: a 1-element array's text literal ("{1.5}" = 5 bytes)
        // is SHORTER than its binary encoding (24-byte header + 12), so the stale prediction
        // over-declared the row and the client blocked waiting for bytes never sent. That path
        // fails as a socket timeout rather than a framing assertion, so it needs its own case.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            execute("""
                    CREATE TABLE arr1 AS (
                      SELECT rnd_str(400, 400, 0) AS pad,
                             ARRAY[1.5] AS a1
                      FROM long_sequence(20))""");

            final String sql = "SELECT pad, a1 FROM arr1";
            try (RawPGClient client = new RawPGClient(port)) {
                ObjList<ObjList<String>> perColumn = client.query(sql, formats(2, FORMAT_TEXT));
                Assert.assertEquals(20, perColumn.size());
                for (int i = 0; i < 20; i++) {
                    Assert.assertEquals("{1.5}", perColumn.getQuick(i).getQuick(1));
                }
            }
        });
    }

    @Test
    public void testTextArrayWithPerColumnFormatCodesDeclaresCorrectRowLength() throws Exception {
        // More than one result format code makes isTextFormat() false, which switches overflow
        // handling from "rewind the whole row" to "predict the tail size and patch the already
        // written length prefix". The prediction has to be exact.
        //
        // ARRAY_LEN pins the direction of the old defect: a 6-element array's text literal is
        // longer than its binary encoding, so the stale prediction under-declared the row and the
        // client read the next message from inside the payload. testTextArrayShorterThanBinary()
        // covers the opposite sign, where the client instead blocks forever.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            execute("""
                    CREATE TABLE arr AS (
                      SELECT rnd_str(400, 400, 0) AS pad,
                             rnd_double_array(1, 0, 0, 6) AS a1
                      FROM long_sequence(20))""");

            final String sql = "SELECT pad, a1 FROM arr";
            try (RawPGClient client = new RawPGClient(port)) {
                // one format code takes the known good atomic-rewind path, and is the reference
                ObjList<ObjList<String>> reference = client.query(sql, formats(1, FORMAT_TEXT));
                ObjList<ObjList<String>> perColumn = client.query(sql, formats(2, FORMAT_TEXT));

                Assert.assertEquals(20, reference.size());
                for (int i = 0; i < 20; i++) {
                    ObjList<String> row = perColumn.getQuick(i);
                    Assert.assertEquals("column count of row " + i, 2, row.size());
                    Assert.assertEquals("pad length of row " + i, 400, row.getQuick(0).length());
                    // a 6-element 1-D double array literal: {d,d,d,d,d,d}
                    Assert.assertTrue("array literal " + row.getQuick(1),
                            row.getQuick(1).startsWith("{") && row.getQuick(1).endsWith("}"));
                    Assert.assertEquals("array element count of row " + i,
                            6, row.getQuick(1).split(",").length);
                }
                assertRowsEqual(reference, perColumn);
            }
        });
    }

    @Test
    public void testTextIntervalDecimalAndStringArrayReportSufficientRequiredSize() throws Exception {
        // These three reach estimateColumnTxtSize()/calculateColumnBinSize() in text format but had
        // no arm in either, so they hit "assert false" and surfaced as "unsupported type: 39"
        // instead of the send-buffer message. Asserting the message alone would not pin the sizes
        // they now report, so compare requiredSize against what the row actually renders to.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            execute("CREATE TABLE wide AS (SELECT rnd_str(4000, 4000, 0) AS pad FROM long_sequence(1))");

            final int intervalLen = renderedByteLength(connection,
                    "SELECT interval('2020-01-01', '2021-01-01') FROM long_sequence(1)");
            final int decimalLen = renderedByteLength(connection,
                    "SELECT 1.5::decimal(10, 2) FROM long_sequence(1)");
            final int schemasLen = renderedByteLength(connection,
                    "SELECT current_schemas(true) FROM long_sequence(1)");

            try (RawPGClient client = new RawPGClient(port)) {
                assertRequiredSizeCoversRow(client,
                        "SELECT pad, interval('2020-01-01', '2021-01-01') AS i FROM wide", 4000, intervalLen);
                assertRequiredSizeCoversRow(client,
                        "SELECT pad, 1.5::decimal(10, 2) AS d FROM wide", 4000, decimalLen);
                // ARRAY_STRING is sized by calculateColumnBinSize()'s STRING fall-through
                assertRequiredSizeCoversRow(client,
                        "SELECT pad, current_schemas(true) AS s FROM wide", 4000, schemasLen);
            }
        });
    }

    /**
     * Drives a row too large for the whole send buffer and checks the {@code requiredSize} the
     * server names really covers it. That is the only assertion that pins the per-type text-size
     * formulas: the error message alone is produced whatever number they return.
     *
     * @param valueLen rendered byte length of the second column, measured over a query small
     *                 enough to fit the buffer
     */
    private static void assertRequiredSizeCoversRow(
            RawPGClient client, String selectSql, int padLen, int valueLen
    ) throws IOException {
        // estimateRecordSize() sums a 4-byte length prefix plus the value for each column
        final int trueRowSize = (Integer.BYTES + padLen) + (Integer.BYTES + valueLen);
        try {
            client.query(selectSql, formats(2, FORMAT_TEXT));
            Assert.fail("expected a send-buffer error for: " + selectSql);
        } catch (AssertionError e) {
            final String message = e.getMessage();
            TestUtils.assertContains(message, "not enough space in send buffer");
            final Matcher m = Pattern.compile("requiredSize=(\\d+)").matcher(message);
            Assert.assertTrue("no requiredSize in: " + message, m.find());
            final long requiredSize = Long.parseLong(m.group(1));
            Assert.assertTrue(
                    selectSql + ": requiredSize=" + requiredSize + " must cover the row's "
                            + trueRowSize + " bytes (pad=" + padLen + ", value=" + valueLen + ")",
                    requiredSize >= trueRowSize
            );
        }
    }

    /**
     * Renders one value through a query small enough to fit the send buffer, and measures it.
     */
    private static int renderedByteLength(Connection connection, String sql) throws Exception {
        try (PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            Assert.assertTrue(rs.next());
            final String rendered = rs.getString(1);
            Assert.assertNotNull("expected a rendered value from " + sql, rendered);
            return rendered.getBytes(StandardCharsets.UTF_8).length;
        }
    }

    private static void assertRowsEqual(ObjList<ObjList<String>> expected, ObjList<ObjList<String>> actual) {
        Assert.assertEquals("row count", expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            ObjList<String> e = expected.getQuick(i);
            ObjList<String> a = actual.getQuick(i);
            Assert.assertEquals("column count of row " + i, e.size(), a.size());
            for (int j = 0, m = e.size(); j < m; j++) {
                Assert.assertEquals("row " + i + " column " + j, e.getQuick(j), a.getQuick(j));
            }
        }
    }

    private static short[] formats(int count, short code) {
        short[] codes = new short[count];
        for (int i = 0; i < count; i++) {
            codes[i] = code;
        }
        return codes;
    }

    /**
     * A raw pgwire frontend. It reads every backend message strictly: a DataRow whose declared
     * length does not match the bytes its declared field count consumes fails the test instead of
     * silently desynchronising, and a server that stops mid-message trips the socket timeout.
     */
    private static class RawPGClient implements AutoCloseable {
        private final DataInputStream in;
        private final OutputStream out;
        private final Socket socket;

        private RawPGClient(int port) throws IOException {
            socket = new Socket();
            try {
                socket.connect(new InetSocketAddress("127.0.0.1", port), SOCKET_TIMEOUT_MS);
                // a hang shows up as this timeout rather than as a stuck test
                socket.setSoTimeout(SOCKET_TIMEOUT_MS);
                in = new DataInputStream(socket.getInputStream());
                out = socket.getOutputStream();
                startup();
            } catch (Throwable t) {
                // try-with-resources never binds a resource whose constructor threw
                socket.close();
                throw t;
            }
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }

        public ObjList<ObjList<String>> query(String sql, short[] resultFormats) throws IOException {
            parse(sql);
            bind(resultFormats);
            execute();
            sync();
            return readToReadyForQuery();
        }

        /**
         * Runs a single-parameter statement, sending the parameter in text format and asking for the
         * result in {@code resultFormat}. The only route to a varchar-element array projection.
         */
        public ObjList<ObjList<String>> queryParam(
                String sql, int paramTypeOid, String paramText, short resultFormat
        ) throws IOException {
            byte[] sqlBytes = cString(sql);
            ByteBuffer parseBody = ByteBuffer.allocate(sqlBytes.length + 16);
            parseBody.put((byte) 0);              // unnamed statement
            parseBody.put(sqlBytes);
            parseBody.putShort((short) 1);        // one parameter type OID
            parseBody.putInt(paramTypeOid);
            send('P', parseBody);

            byte[] paramBytes = paramText.getBytes(StandardCharsets.UTF_8);
            ByteBuffer bindBody = ByteBuffer.allocate(paramBytes.length + 32);
            bindBody.put((byte) 0);               // unnamed portal
            bindBody.put((byte) 0);               // unnamed statement
            bindBody.putShort((short) 1);         // one parameter format code
            bindBody.putShort(FORMAT_TEXT);
            bindBody.putShort((short) 1);         // one parameter
            bindBody.putInt(paramBytes.length);
            bindBody.put(paramBytes);
            bindBody.putShort((short) 1);         // one result format code
            bindBody.putShort(resultFormat);
            send('B', bindBody);

            execute();
            sync();
            return readToReadyForQuery();
        }

        private static byte[] cString(String s) {
            byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
            byte[] bytes = new byte[utf8.length + 1];
            System.arraycopy(utf8, 0, bytes, 0, utf8.length);
            return bytes;
        }

        private void bind(short[] resultFormats) throws IOException {
            ByteBuffer body = ByteBuffer.allocate(64 + 2 * resultFormats.length);
            body.put((byte) 0);           // unnamed portal
            body.put((byte) 0);           // unnamed statement
            body.putShort((short) 0);     // no parameter format codes
            body.putShort((short) 0);     // no parameters
            body.putShort((short) resultFormats.length);
            for (short f : resultFormats) {
                body.putShort(f);
            }
            send('B', body);
        }

        private void execute() throws IOException {
            ByteBuffer body = ByteBuffer.allocate(8);
            body.put((byte) 0);           // unnamed portal
            body.putInt(0);               // no row limit
            send('E', body);
        }

        private void parse(String sql) throws IOException {
            byte[] sqlBytes = cString(sql);
            ByteBuffer body = ByteBuffer.allocate(sqlBytes.length + 8);
            body.put((byte) 0);           // unnamed statement
            body.put(sqlBytes);
            body.putShort((short) 0);     // no parameter type OIDs
            send('P', body);
        }

        private ObjList<String> readDataRow(byte[] body) {
            ByteBuffer bb = ByteBuffer.wrap(body);
            int columnCount = bb.getShort() & 0xffff;
            ObjList<String> row = new ObjList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                Assert.assertTrue(
                        "DataRow ran out of bytes at column " + i + " of " + columnCount
                                + ": the row declares more fields than it carries",
                        bb.remaining() >= Integer.BYTES
                );
                int len = bb.getInt();
                if (len == -1) {
                    row.add(null);
                    continue;
                }
                Assert.assertTrue(
                        "DataRow column " + i + " declares " + len + " bytes but only "
                                + bb.remaining() + " remain in the message",
                        len >= 0 && len <= bb.remaining()
                );
                byte[] value = new byte[len];
                bb.get(value);
                row.add(new String(value, StandardCharsets.UTF_8));
            }
            Assert.assertEquals(
                    "DataRow declared length does not match the bytes its fields consume",
                    0, bb.remaining()
            );
            return row;
        }

        private byte[] readMessage(byte[] type) throws IOException {
            type[0] = in.readByte();
            int len = in.readInt();
            // the defects under test desynchronise the stream, so this "length" can be read out of
            // the middle of a payload. Bound it from above too, or a bogus value up to 2^31
            // allocates its way to an OutOfMemoryError in the shared surefire fork.
            Assert.assertTrue(
                    "bogus message length " + len + " for type '" + (char) type[0] + "'",
                    len >= 4 && len <= MAX_MESSAGE_LEN
            );
            byte[] body = new byte[len - 4];
            in.readFully(body);
            return body;
        }

        private ObjList<ObjList<String>> readToReadyForQuery() throws IOException {
            ObjList<ObjList<String>> rows = new ObjList<>();
            byte[] type = new byte[1];
            String error = null;
            while (true) {
                byte[] body = readMessage(type);
                switch (type[0]) {
                    case 'D':
                        rows.add(readDataRow(body));
                        break;
                    case 'E':
                        // keep reading: the ReadyForQuery that closes the batch has to be consumed
                        // before we bail, or the next query on this connection reads it instead of
                        // its own response and silently reports no rows
                        error = new String(body, StandardCharsets.UTF_8).replace('\0', '|');
                        break;
                    case 'Z':
                        if (error != null) {
                            Assert.fail("server returned an error: " + error);
                        }
                        return rows;
                    default:
                        // T, C, S, 1, 2, n and friends carry no row data
                        break;
                }
            }
        }

        private void send(char type, ByteBuffer body) throws IOException {
            int len = body.position();
            ByteBuffer msg = ByteBuffer.allocate(len + 5);
            msg.put((byte) type);
            msg.putInt(len + 4);
            msg.put(body.array(), 0, len);
            out.write(msg.array(), 0, msg.position());
            out.flush();
        }

        private void sync() throws IOException {
            send('S', ByteBuffer.allocate(0));
        }

        private void startup() throws IOException {
            byte[] user = cString("user");
            byte[] userValue = cString("admin");
            byte[] database = cString("database");
            byte[] databaseValue = cString("qdb");
            int len = 4 + 4 + user.length + userValue.length + database.length + databaseValue.length + 1;
            ByteBuffer msg = ByteBuffer.allocate(len);
            msg.putInt(len);
            msg.putInt(PROTOCOL_VERSION_3_0);
            msg.put(user).put(userValue).put(database).put(databaseValue);
            msg.put((byte) 0);
            out.write(msg.array(), 0, msg.position());
            out.flush();

            byte[] type = new byte[1];
            while (true) {
                byte[] body;
                try {
                    body = readMessage(type);
                } catch (EOFException e) {
                    throw new IOException("server closed the socket during startup", e);
                }
                if (type[0] == 'R') {
                    int code = ByteBuffer.wrap(body).getInt();
                    if (code == 0) {
                        continue;
                    }
                    Assert.assertEquals("expected cleartext password auth", 3, code);
                    ByteBuffer pwd = ByteBuffer.allocate(32);
                    pwd.put(cString("quest"));
                    send('p', pwd);
                    continue;
                }
                if (type[0] == 'Z') {
                    return;
                }
                if (type[0] == 'E') {
                    Assert.fail("startup failed: " + new String(body, StandardCharsets.UTF_8));
                }
            }
        }
    }
}
