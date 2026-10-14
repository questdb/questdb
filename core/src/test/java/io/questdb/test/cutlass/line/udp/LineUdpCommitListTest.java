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

import io.questdb.cutlass.line.udp.DefaultLineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpLexer;
import io.questdb.cutlass.line.udp.LineUdpParserImpl;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import java.util.TreeMap;

/**
 * Every table that received a UDP line must have its rows committed by commitAll(), whatever
 * order the tables arrive in. See <a href="https://github.com/questdb/questdb/issues/7847">#7847</a>.
 */
public class LineUdpCommitListTest extends AbstractCairoTest {

    @Test
    public void testDistinctTablesNoRepeat() throws Exception {
        assertEveryTableCommitted(1, "p", "q", "r", "s");
    }

    @Test
    public void testSingleTable() throws Exception {
        assertEveryTableCommitted(1, "p", "p", "p");
    }

    @Test
    public void testThreeTablesSwitchIntoSameTable() throws Exception {
        assertEveryTableCommitted(1, "q", "p", "r", "p", "s", "p");
    }

    @Test
    public void testTwoTablesAlternating() throws Exception {
        assertEveryTableCommitted(1, "p", "q", "p", "q", "p");
    }

    @Test
    public void testTwoTablesSwitchIntoSameTable() throws Exception {
        assertEveryTableCommitted(1, "q", "p", "r", "p");
    }

    @Test
    public void testTwoTablesSwitchIntoSameTableAcrossCommitCycles() throws Exception {
        assertEveryTableCommitted(3, "q", "p", "r", "p");
    }

    private static void parse(LineUdpLexer lexer, String text) {
        final byte[] bytes = text.getBytes(Files.UTF_8);
        final int len = bytes.length;
        final long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.putByte(mem + i, bytes[i]);
            }
            lexer.parse(mem, mem + len);
            lexer.parseLast();
        } finally {
            Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // Asserts all tables' row counts in one result set, so a failure shows every table side by side.
    private void assertCounts(TreeMap<String, Integer> expected, int cycles) throws Exception {
        final StringBuilder query = new StringBuilder();
        final StringBuilder want = new StringBuilder("t\tcount\n");
        for (var e : expected.entrySet()) {
            if (!query.isEmpty()) {
                query.append(" UNION ALL ");
            }
            query.append("SELECT '").append(e.getKey()).append("' t, count() FROM ").append(e.getKey());
            want.append(e.getKey()).append('\t').append(e.getValue() * cycles).append('\n');
        }
        assertQuery(query)
                .noRandomAccess()
                .noLeakCheck()
                .expectSize()
                .returns(want);
    }

    // Sends one line per entry of lineTables in a single datagram, then commits. Repeats this for
    // the given number of commit cycles and asserts row counts both while the parser still holds
    // its writers and after it closes them (which rolls back anything left uncommitted).
    private void assertEveryTableCommitted(int cycles, String... lineTables) throws Exception {
        assertMemoryLeak(() -> {
            final TreeMap<String, Integer> expected = new TreeMap<>();
            for (String t : lineTables) {
                expected.merge(t, 1, Integer::sum);
            }
            for (String t : expected.keySet()) {
                execute("CREATE TABLE " + t + " (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            }

            try (
                    LineUdpParserImpl parser = new LineUdpParserImpl(engine, new DefaultLineUdpReceiverConfiguration());
                    LineUdpLexer lexer = new LineUdpLexer(4096)
            ) {
                lexer.withParser(parser);
                for (int c = 0; c < cycles; c++) {
                    final StringBuilder lines = new StringBuilder();
                    for (int i = 0; i < lineTables.length; i++) {
                        final int n = c * lineTables.length + i;
                        lines.append(lineTables[i]).append(",s=a v=").append(n).append("i ")
                                .append(1_704_153_600_000_000_000L + n * 3_600_000_000_000L).append('\n');
                    }
                    parse(lexer, lines.toString());
                    parser.commitAll();
                    assertCounts(expected, c + 1);
                }
            }
            assertCounts(expected, cycles);
        });
    }
}
