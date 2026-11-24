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

import io.questdb.PropertyKey;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PGInsertErrorTest extends BasePGTest {

    @Test
    public void testInsertComputeFailure() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.prepareCall(
                    "CREATE TABLE IF NOT EXISTS foo (\n" +
                            "            ts TIMESTAMP,\n" +
                            "            bar INT\n" +
                            "        ) TIMESTAMP (ts)\n" +
                            "          PARTITION BY DAY"
            ).execute();

            PreparedStatement ps = connection.prepareStatement("INSERT INTO foo VALUES (?, ?)");
            ps.executeBatch();
            ps.setTimestamp(1, new Timestamp(100002222L));
            ps.setLong(2, 1900L);
            ps.execute();

            ps.setTimestamp(1, new Timestamp(100002222L));
            ps.setLong(2, 19000000000L);
            try {
                ps.execute();
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "inconvertible value: 19000000000 [LONG -> INT]");
            }
        });
    }

    @Test
    public void testInsertComputeFailureBatch() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.prepareCall(
                    "CREATE TABLE IF NOT EXISTS foo (\n" +
                            "            ts TIMESTAMP,\n" +
                            "            bar INT\n" +
                            "        ) TIMESTAMP (ts)\n" +
                            "          PARTITION BY DAY"
            ).execute();

            // executing inserts in batches is not random
            // we are aiming to prepare first insert, which will start the implicit transaction
            // the second insert will fail, while writer is divided between insert and "pending" writer map.
            // failing insert must clean up correctly so that writer will not be double-closed.
            PreparedStatement ps = connection.prepareStatement("INSERT INTO foo VALUES (?, ?)");
            ps.executeBatch();
            ps.setTimestamp(1, new Timestamp(100002222L));
            ps.setLong(2, 1900L);
            ps.addBatch();

            ps.setTimestamp(1, new Timestamp(100002222L));
            ps.setLong(2, 19000000000L);
            ps.addBatch();
            try {
                ps.executeBatch();
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "inconvertible value: 19000000000 [LONG -> INT]");
            }
        });
    }

    @Test
    public void testNoTimestampFailedBinaryBindInsert() throws Exception {
        // A table without a designated timestamp, BINARY first: the bound BINARY value of the prepared INSERT, the first
        // value of the row, crosses the end of the page of b.d, which cannot be allocated. The INSERT fails, and the
        // same prepared statement, executed twice more, lands its rows after the committed ones.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final FilesFacade facade = allocateFailingFacade("b.d", isArmed);
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            // values of 8 + 92 bytes: the next one crosses the end of the first page of b.d
            final long n = Files.PAGE_SIZE / 100;
            final byte[] committed = bytesOf('A', 92);
            final byte[] c = bytesOf('C', 92);
            final byte[] d = bytesOf('D', 5);
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE t (b BINARY, x LONG)");
                statement.execute("INSERT INTO t SELECT from_base64('" + Base64.getEncoder().encodeToString(committed) + "'), x FROM long_sequence(" + n + ")");
            }
            try (PreparedStatement insert = connection.prepareStatement("INSERT INTO t VALUES (?, ?)")) {
                isArmed.set(true);
                insert.setBytes(1, bytesOf('B', 92));
                insert.setLong(2, n + 1);
                try {
                    insert.execute();
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "No space left");
                }
                Assert.assertFalse(isArmed.get());
                insert.setBytes(1, c);
                insert.setLong(2, n + 2);
                insert.execute();
                insert.setBytes(1, d);
                insert.setLong(2, n + 3);
                insert.execute();
            }
            final Base64.Encoder base64 = Base64.getEncoder();
            assertQuery("SELECT x, length(b) len, base64(b, 100) b FROM t WHERE x >= " + (n - 1) + " OR x IS NULL")
                    .noLeakCheck()
                    .returns("x\tlen\tb\n"
                            + (n - 1) + "\t92\t" + base64.encodeToString(committed) + '\n'
                            + n + "\t92\t" + base64.encodeToString(committed) + '\n'
                            + (n + 2) + "\t92\t" + base64.encodeToString(c) + '\n'
                            + (n + 3) + "\t5\t" + base64.encodeToString(d) + '\n');
            assertQuery("SELECT count(), sum(x) FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tsum\n" + (n + 2) + '\t' + (n * (n + 1) / 2 + n + 2 + n + 3) + '\n');
        }, () -> {
            setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
            AbstractCairoTest.ff = facade;
        });
    }

    @Test
    public void testNoTimestampFailedNullInsert() throws Exception {
        // A table without a designated timestamp: an INSERT of NULLs only, whose NULL of s needs the next page of s.d
        // after the NULL of x landed, fails. A prepared INSERT, executed twice, lands its rows after the committed
        // ones.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final FilesFacade facade = allocateFailingFacade("s.d", isArmed);
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            // 'abcdef' takes 16 bytes of s.d: the committed rows end its first page
            final long n = Files.PAGE_SIZE / 16;
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE t (x LONG, s STRING)");
                statement.execute("INSERT INTO t SELECT x, 'abcdef' FROM long_sequence(" + n + ")");
                isArmed.set(true);
                try {
                    statement.execute("INSERT INTO t VALUES (NULL, NULL)");
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "No space left");
                }
                Assert.assertFalse(isArmed.get());
            }
            try (PreparedStatement insert = connection.prepareStatement("INSERT INTO t VALUES (?, ?)")) {
                insert.setLong(1, n + 2);
                insert.setString(2, "abcdef");
                insert.execute();
                insert.setLong(1, n + 3);
                insert.setNull(2, Types.VARCHAR);
                insert.execute();
            }
            assertQuery("SELECT x, s FROM t WHERE x >= " + (n - 1) + " OR x IS NULL")
                    .noLeakCheck()
                    .returns("x\ts\n" + (n - 1) + "\tabcdef\n" + n + "\tabcdef\n" + (n + 2) + "\tabcdef\n" + (n + 3) + "\t\n");
            assertQuery("SELECT count(), sum(x) FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tsum\n" + (n + 2) + '\t' + (n * (n + 1) / 2 + n + 2 + n + 3) + '\n');
        }, () -> {
            setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
            AbstractCairoTest.ff = facade;
        });
    }

    // fails, once when armed, the allocation of disk space for the file of table t
    private static FilesFacade allocateFailingFacade(String fileName, AtomicBoolean isArmed) {
        final AtomicLong fileFd = new AtomicLong(-1);
        return new TestFilesFacadeImpl() {
            @Override
            public boolean allocate(long fd, long size) {
                if (fd == fileFd.get() && isArmed.compareAndSet(true, false)) {
                    return false;
                }
                return super.allocate(fd, size);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (Utf8s.containsAscii(name, Files.SEPARATOR + "t~") && Utf8s.endsWithAscii(name, Files.SEPARATOR + fileName)) {
                    fileFd.set(fd);
                }
                return fd;
            }
        };
    }

    private static byte[] bytesOf(char c, int len) {
        final byte[] bytes = new byte[len];
        Arrays.fill(bytes, (byte) c);
        return bytes;
    }
}
