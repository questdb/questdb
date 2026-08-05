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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;

/**
 * Drives the extended query protocol over a raw socket so the test can choose the per-column
 * result format codes the JDBC driver never sends, and can validate DataRow framing byte for byte.
 * <p>
 * Both defects these tests cover surface to the client as a hang rather than an error: the server
 * finishes the query, believes it answered, and goes back to waiting for the next command, while
 * the client blocks forever on a DataRow that never completes. The connection then dies on the
 * pgwire idle timeout, which is what the server log shows ("disconnected [... src=idle]").
 */
public class PGResultFormatCodesTest extends BasePGTest {

    private static final short FORMAT_BINARY = 1;
    private static final short FORMAT_TEXT = 0;

    @Before
    public void setUp() {
        super.setUp();
        // small enough that a padded row straddles the send buffer, which is what drives
        // outRecord() down the "predict the tail size and patch the length prefix" path
        sendBufferSize = 1024;
    }

    @Test
    public void testGeoHashAndIPv4InBinaryFormatAreNotDroppedFromTheRow() throws Exception {
        // pgwire advertises IPv4 and every geohash width as PG_VARCHAR, so a driver that asks for
        // binary on the types it can decode will ask for binary here. outRecord() had no case label
        // for those combinations and fell through to "default", emitting zero bytes for a field the
        // DataRow header still counted. The client then reads the next message as that field.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement(
                    "CREATE TABLE geo AS (SELECT" +
                            " rnd_ipv4() AS ip," +
                            " rnd_geohash(5) AS g1," +
                            " rnd_geohash(15) AS g3," +
                            " rnd_geohash(30) AS g6," +
                            " rnd_geohash(60) AS g12" +
                            " FROM long_sequence(20))")) {
                stmt.execute();
            }

            final String sql = "SELECT ip, g1, g3, g6, g12 FROM geo";
            try (RawPGClient client = new RawPGClient(port)) {
                ObjList<ObjList<String>> asText = client.query(sql, formats(5, FORMAT_TEXT));
                ObjList<ObjList<String>> asBinary = client.query(sql, formats(5, FORMAT_BINARY));

                Assert.assertEquals(20, asText.size());
                // these five types have no distinct binary encoding: they go out as varchar text
                // under either format code, so the two result sets must be identical
                assertRowsEqual(asText, asBinary);
            }
        });
    }

    @Test
    public void testTextArrayWithPerColumnFormatCodesDeclaresCorrectRowLength() throws Exception {
        // More than one result format code makes isTextFormat() false, which switches overflow
        // handling from "rewind the whole row" to "predict the tail size and patch the already
        // written length prefix". The prediction used the binary array size for a column being
        // written as a PostgreSQL array literal, so the DataRow announced a length the server
        // never delivered.
        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement(
                    "CREATE TABLE arr AS (SELECT" +
                            " rnd_str(400, 400, 0) AS pad," +
                            " rnd_double_array(1, 0) AS a1" +
                            " FROM long_sequence(20))")) {
                stmt.execute();
            }

            final String sql = "SELECT pad, a1 FROM arr";
            try (RawPGClient client = new RawPGClient(port)) {
                // one format code takes the known good atomic-rewind path, and is the reference
                ObjList<ObjList<String>> reference = client.query(sql, formats(1, FORMAT_TEXT));
                ObjList<ObjList<String>> perColumn = client.query(sql, formats(2, FORMAT_TEXT));

                Assert.assertEquals(20, reference.size());
                assertRowsEqual(reference, perColumn);
            }
        });
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
            socket.connect(new InetSocketAddress("127.0.0.1", port), 30_000);
            // a hang shows up as this timeout rather than as a stuck test
            socket.setSoTimeout(30_000);
            InputStream rawIn = socket.getInputStream();
            in = new DataInputStream(rawIn);
            out = socket.getOutputStream();
            startup();
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

        private static byte[] cString(String s) {
            byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
            byte[] out = new byte[utf8.length + 1];
            System.arraycopy(utf8, 0, out, 0, utf8.length);
            return out;
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
            Assert.assertTrue("bogus message length " + len, len >= 4);
            byte[] body = new byte[len - 4];
            in.readFully(body);
            return body;
        }

        private ObjList<ObjList<String>> readToReadyForQuery() throws IOException {
            ObjList<ObjList<String>> rows = new ObjList<>();
            byte[] type = new byte[1];
            while (true) {
                byte[] body = readMessage(type);
                switch (type[0]) {
                    case 'D':
                        rows.add(readDataRow(body));
                        break;
                    case 'E':
                        Assert.fail("server returned an error: " + new String(body, StandardCharsets.UTF_8));
                        break;
                    case 'Z':
                        return rows;
                    default:
                        // T, C, 1, 2, n and friends carry no row data
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
            msg.putInt(196608); // protocol 3.0
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
