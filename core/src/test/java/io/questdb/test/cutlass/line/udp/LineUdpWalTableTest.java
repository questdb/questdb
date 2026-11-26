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

package io.questdb.test.cutlass.line.udp;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cutlass.line.udp.DefaultLineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpLexer;
import io.questdb.cutlass.line.udp.LineUdpParserImpl;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LogCapture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * ILP over UDP writes through a {@code TableWriter} directly, bypassing the WAL, so it only ever
 * writes non-WAL tables: it creates new tables as non-WAL even when WAL is the default, and it
 * refuses an existing WAL table with a critical log entry, dropping that table's rows.
 */
public class LineUdpWalTableTest extends AbstractCairoTest {
    private static final LogCapture capture = new LogCapture();
    private static final String WAL_REJECTED_RE = " C i\\.q\\.c\\.l\\.u\\.LineUdpParserImpl ILP over UDP cannot write to a WAL table.*\\[table=";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        capture.start();
        setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, "true");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
    }

    @Test
    public void testAutoCreatedTableIsNotWal() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertTrue(engine.getConfiguration().getWalEnabledDefault());

            ingest("""
                    udp_new,tag=a v=1i 1000000000
                    udp_new,tag=b v=2i 2000000000
                    udp_new,tag=c v=3i 3000000000
                    """);

            final TableToken tableToken = engine.verifyTableName("udp_new");
            Assert.assertFalse("ILP over UDP must create non-WAL tables", tableToken.isWal());
            assertQuery("SELECT tag, v, timestamp FROM udp_new")
                    .timestamp("timestamp")
                    .expectSize()
                    .returns("""
                            tag\tv\ttimestamp
                            a\t1\t1970-01-01T00:00:01.000000Z
                            b\t2\t1970-01-01T00:00:02.000000Z
                            c\t3\t1970-01-01T00:00:03.000000Z
                            """);
        });
    }

    @Test
    public void testExistingNonWalTableAcceptsRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE udp_bypass (v LONG, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY BYPASS WAL");

            ingest("""
                    udp_bypass v=1i 1000000000
                    udp_bypass v=2i 2000000000
                    """);

            assertQuery("SELECT v, timestamp FROM udp_bypass")
                    .timestamp("timestamp")
                    .expectSize()
                    .returns("""
                            v\ttimestamp
                            1\t1970-01-01T00:00:01.000000Z
                            2\t1970-01-01T00:00:02.000000Z
                            """);
            capture.drain();
            capture.assertNotLogged("ILP over UDP cannot write to a WAL table");
        });
    }

    @Test
    public void testWalTableIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE udp_wal (v LONG, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            ingest("""
                    udp_wal v=1i 1000000000
                    udp_wal v=2i 2000000000
                    udp_wal,tag=x v=3i,extra=4i 3000000000
                    """);
            drainWalQueue();

            assertQuery("SELECT count() FROM udp_wal")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n0\n");
            // Rejected before any writer is taken: no column was added from the third line.
            assertQuery("SELECT \"column\" FROM table_columns('udp_wal')")
                    .noRandomAccess()
                    .returns("""
                            column
                            v
                            timestamp
                            """);
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("udp_wal")));

            // Logged as critical once for the table, not once per dropped line.
            capture.drain();
            capture.assertOnlyOnce(WAL_REJECTED_RE + ".*udp_wal");
        });
    }

    @Test
    public void testWalTableLinesDoNotLeakIntoOtherTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE udp_wal (v LONG, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            // Lines for the rejected WAL table interleave with lines for an accepted table: the
            // WAL table's lines must neither land in the table written just before them, nor stop
            // the accepted table from taking its own lines.
            ingest("""
                    udp_ok v=1i 1000000000
                    udp_wal v=100i 1500000000
                    udp_wal v=101i 1600000000
                    udp_ok v=2i 2000000000
                    udp_wal v=102i 2500000000
                    udp_ok v=3i 3000000000
                    """);
            drainWalQueue();

            Assert.assertFalse(engine.verifyTableName("udp_ok").isWal());
            assertQuery("SELECT v, timestamp FROM udp_ok")
                    .timestamp("timestamp")
                    .expectSize()
                    .returns("""
                            v\ttimestamp
                            1\t1970-01-01T00:00:01.000000Z
                            2\t1970-01-01T00:00:02.000000Z
                            3\t1970-01-01T00:00:03.000000Z
                            """);
            assertQuery("SELECT count() FROM udp_wal")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n0\n");

            capture.drain();
            capture.assertOnlyOnce(WAL_REJECTED_RE + ".*udp_wal");
        });
    }

    private static void ingest(String lines) {
        final byte[] bytes = lines.getBytes(Files.UTF_8);
        final int len = bytes.length;
        final long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.putByte(mem + i, bytes[i]);
            }
            try (
                    LineUdpParserImpl parser = new LineUdpParserImpl(engine, new DefaultLineUdpReceiverConfiguration());
                    LineUdpLexer lexer = new LineUdpLexer(4096)
            ) {
                lexer.withParser(parser);
                lexer.parse(mem, mem + len);
                lexer.parseLast();
                parser.commitAll();
            }
        } finally {
            Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
