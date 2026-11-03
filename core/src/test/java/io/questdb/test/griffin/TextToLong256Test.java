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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Text written into a LONG256 column follows one rule whatever writes it: an optional 0x or 0X,
 * then 1 to 64 hex digits; NULL and empty text are NULL; anything else fails with "inconvertible
 * value". The comparison of a LONG256 with text reads the text by the same rule. Each test feeds
 * the same texts through one path and checks the values against the values the texts spell,
 * worked out by hand.
 */
public class TextToLong256Test extends AbstractCairoTest {
    // the SHA-256 of 'alice', as sha256() returns it: hex digits without 0x
    private static final String SHA256_ALICE = "2bd806c97f0e00af1a1fc3328fa763a9269723c8db8fac4f93af71db186d6e90";
    // {text, the LONG256 it spells as a LONG256 prints}; a null text is NULL, which prints empty
    private static final String[][] GOOD = {
            {"0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20", "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"},
            {"0X0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F20", "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"},
            {SHA256_ALICE, "0x" + SHA256_ALICE},
            {"0x00000000000000000000000000000000000000000000000000000000000000ab", "0xab"},
            {"12", "0x12"},
            {"abc", "0x0abc"},
            {"f", "0x0f"},
            {"", ""},
            {null, ""}
    };
    // not hex, a prefix without digits, an x after another digit, a leading space, 65 digits with
    // and without the prefix, not ASCII
    private static final String[] REFUSED = {
            "zz", "u33d", "xyz", "0x", "0X", "1x12", " 12", "1" + "0".repeat(64), "0x1" + "0".repeat(64), "й12"
    };
    // the LONG256 columns of the target and the source expression of each: STRING, VARCHAR read
    // from a column, and VARCHAR made by an expression
    private static final String[][] SOURCES = {{"s", "s"}, {"v", "v"}, {"e", "s::VARCHAR"}};
    private static final String[] WAL_MODES = {"BYPASS WAL", "WAL"};
    // the chunked copier splits only a schema wider than one chunk; below that it hands the copy to
    // the single-method copier, so copier type 2 gets this many extra INT columns
    private static final int CHUNKING_PAD_COUNT = 500;

    @Test
    public void testBindVariable() throws Exception {
        // $2 takes the column's type, LONG256, and parses the text it is set from
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dst (i INT, l LONG256)");
            bindVariableService.clear();
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("INSERT INTO dst VALUES ($1, $2)", sqlExecutionContext).popInsertOperation()
            ) {
                for (int i = 0; i < GOOD.length; i++) {
                    bindVariableService.setInt(0, i);
                    bindVariableService.setStr(1, GOOD[i][0]);
                    insert.execute(sqlExecutionContext);
                }
                for (int i = 0; i < GOOD.length; i++) {
                    bindVariableService.setInt(0, 10 + i);
                    bindVariableService.setVarchar(1, GOOD[i][0] == null ? null : new Utf8String(GOOD[i][0]));
                    insert.execute(sqlExecutionContext);
                }
                for (String text : REFUSED) {
                    try {
                        bindVariableService.setStr(1, text);
                        Assert.fail("STRING " + text);
                    } catch (ImplicitCastException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), refusal(text));
                    }
                    try {
                        bindVariableService.setVarchar(1, new Utf8String(text));
                        Assert.fail("VARCHAR " + text);
                    } catch (ImplicitCastException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), refusal(text));
                    }
                }
            }
            assertQuery("dst")
                    .noLeakCheck()
                    .expectSize()
                    .returns(rows("i\tl", 1, 0) + rows(null, 1, 10));
        });
    }

    @Test
    public void testCompareWithText() throws Exception {
        // the rows hold the values the texts spell, written as 0x literals, and the values a reading
        // that skips the first two characters gives: 0x0c for 'abc', 0x00 for '12', 'zz' and the
        // empty text, 0x02 for ' 12', and the hash without its first byte
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (i INT, l LONG256)");
            execute("""
                    INSERT INTO c VALUES
                        (0, '0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20'),
                        (2, '0x2bd806c97f0e00af1a1fc3328fa763a9269723c8db8fac4f93af71db186d6e90'),
                        (3, '0xab'),
                        (4, '0x12'),
                        (5, '0x0abc'),
                        (6, '0x0f'),
                        (7, NULL),
                        (90, '0x0c'),
                        (91, '0x00'),
                        (92, '0x02'),
                        (93, '0xd806c97f0e00af1a1fc3328fa763a9269723c8db8fac4f93af71db186d6e90')
                    """);
            // {text, the row it finds}
            final String[][] finds = {
                    {GOOD[0][0], "0"},
                    {GOOD[1][0], "0"},
                    {SHA256_ALICE, "2"},
                    {GOOD[3][0], "3"},
                    {"12", "4"},
                    {"abc", "5"},
                    {"f", "6"},
                    {"", "7"}
            };
            for (String[] find : finds) {
                // a one-character literal is a CHAR, so the STRING is spelled out
                assertQuery("SELECT i FROM c WHERE l = '" + find[0] + "'" + (find[0].length() == 1 ? "::STRING" : ""))
                        .noLeakCheck()
                        .returns("i\n" + find[1] + '\n');
                bindVariableService.clear();
                bindVariableService.setStr(0, find[0]);
                assertQuery("SELECT i FROM c WHERE l = $1")
                        .noLeakCheck()
                        .returns("i\n" + find[1] + '\n');
            }
            bindVariableService.clear();
            bindVariableService.setStr(0, null);
            assertQuery("SELECT i FROM c WHERE l = $1")
                    .noLeakCheck()
                    .returns("i\n7\n");
            for (String text : REFUSED) {
                bindVariableService.clear();
                assertQuery("SELECT i FROM c WHERE l = '" + text + "'")
                        .noLeakCheck()
                        .failsWith(refusal(text));
                bindVariableService.setStr(0, text);
                assertQuery("SELECT i FROM c WHERE l = $1")
                        .noLeakCheck()
                        .failsWith(refusal(text));
            }
            bindVariableService.clear();
        });
    }

    @Test
    public void testCreateTableAsSelectCast() throws Exception {
        assertMemoryLeak(() -> {
            createSource();
            for (int copierType = 0; copierType < 4; copierType++) {
                node1.setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                for (String wal : WAL_MODES) {
                    final String tail = " TIMESTAMP(ts) PARTITION BY DAY " + wal;
                    // a refused text after good rows fails the statement, and the table goes
                    for (int r = 0; r < REFUSED.length; r++) {
                        for (String[] source : SOURCES) {
                            assertExceptionNoLeakCheck(
                                    "CREATE TABLE dst AS (SELECT i, " + source[1] + " x, ts FROM src WHERE i < 100 OR i = " + (100 + r) + "), CAST(x AS LONG256)" + tail,
                                    -1,
                                    refusal(REFUSED[r]),
                                    false
                            );
                            Assert.assertNull(engine.getTableTokenIfExists("dst"));
                        }
                    }
                    execute("CREATE TABLE dst AS (SELECT i, s, v, s::VARCHAR e, ts" + pads(copierType) + " FROM src WHERE i < 100), CAST(s AS LONG256), CAST(v AS LONG256), CAST(e AS LONG256)" + tail);
                    drainWalQueue();
                    assertQuery("SELECT i, s, v, e FROM dst")
                            .noLeakCheck()
                            .expectSize()
                            .returns(rows("i\ts\tv\te", 3, 0));
                    execute("DROP TABLE dst");
                }
            }
        });
    }

    @Test
    public void testCreateTableAsSelectCastOfHashFunctions() throws Exception {
        // sha256() and md5() return VARCHAR hex digits without 0x; the hashes are the known ones
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (name VARCHAR)");
            execute("INSERT INTO src VALUES ('alice'), ('bob')");
            for (int copierType = 0; copierType < 4; copierType++) {
                node1.setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                execute("CREATE TABLE dst AS (SELECT name, sha256(name) h, md5(name) m" + pads(copierType) + " FROM src), CAST(h AS LONG256), CAST(m AS LONG256)");
                assertQuery("SELECT name, h, m FROM dst")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name\th\tm
                                alice\t0x2bd806c97f0e00af1a1fc3328fa763a9269723c8db8fac4f93af71db186d6e90\t0x6384e2b2184bcbf58eccf10ca7a6563c
                                bob\t0x81b637d8fcd2c6da6359e6963113a1170de795e4b725b84d1e0b4cfd9ec58ce9\t0x9f9d51bc70ef21ca5c14f307980a29d8
                                """);
                execute("DROP TABLE dst");
            }
        });
    }

    @Test
    public void testInsertAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            createSource();
            for (int copierType = 0; copierType < 4; copierType++) {
                node1.setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                for (String wal : WAL_MODES) {
                    execute("CREATE TABLE dst (i INT, s LONG256, v LONG256, e LONG256, ts TIMESTAMP" + padColumns(copierType) + ") TIMESTAMP(ts) PARTITION BY DAY " + wal);
                    execute("INSERT INTO dst SELECT i, s, v, s::VARCHAR, ts" + pads(copierType) + " FROM src WHERE i < 100");
                    drainWalQueue();
                    final String expected = rows("i\ts\tv\te", 3, 0);
                    assertQuery("SELECT i, s, v, e FROM dst")
                            .noLeakCheck()
                            .expectSize()
                            .returns(expected);
                    // a refused text after good rows fails the statement and leaves the table as it was
                    for (int r = 0; r < REFUSED.length; r++) {
                        for (String[] source : SOURCES) {
                            assertExceptionNoLeakCheck(
                                    "INSERT INTO dst (i, " + source[0] + ", ts) SELECT i + 10, " + source[1] + ", ts + 1000 FROM src WHERE i < 100 OR i = " + (100 + r),
                                    -1,
                                    refusal(REFUSED[r]),
                                    false
                            );
                            drainWalQueue();
                            assertQuery("SELECT i, s, v, e FROM dst")
                                    .noLeakCheck()
                                    .expectSize()
                                    .returns(expected);
                        }
                    }
                    // and takes rows again afterwards
                    execute("INSERT INTO dst SELECT i + 10, s, v, s::VARCHAR, ts + 1000" + pads(copierType) + " FROM src WHERE i < 100");
                    drainWalQueue();
                    assertQuery("SELECT i, s, v, e FROM dst")
                            .noLeakCheck()
                            .expectSize()
                            .returns(expected + rows(null, 3, 10));
                    execute("DROP TABLE dst");
                }
            }
        });
    }

    @Test
    public void testInsertAsSelectOutOfOrder() throws Exception {
        // rows older than the table's last row go through the writer's out-of-order memory
        assertMemoryLeak(() -> {
            createSource();
            for (int copierType = 0; copierType < 4; copierType++) {
                node1.setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                execute("CREATE TABLE dst (i INT, s LONG256, v LONG256, e LONG256, ts TIMESTAMP" + padColumns(copierType) + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO dst (i, ts) VALUES (-1, 1_000_000)");
                execute("INSERT INTO dst SELECT i, s, v, s::VARCHAR, ts" + pads(copierType) + " FROM src WHERE i < 100");
                final String expected = rows("i\ts\tv\te", 3, 0) + "-1\t\t\t\n";
                assertQuery("SELECT i, s, v, e FROM dst")
                        .noLeakCheck()
                        .expectSize()
                        .returns(expected);
                for (int r = 0; r < REFUSED.length; r++) {
                    for (String[] source : SOURCES) {
                        assertExceptionNoLeakCheck(
                                "INSERT INTO dst (i, " + source[0] + ", ts) SELECT i + 10, " + source[1] + ", ts FROM src WHERE i < 100 OR i = " + (100 + r),
                                -1,
                                refusal(REFUSED[r]),
                                false
                        );
                        assertQuery("SELECT i, s, v, e FROM dst")
                                .noLeakCheck()
                                .expectSize()
                                .returns(expected);
                    }
                }
                execute("DROP TABLE dst");
            }
        });
    }

    @Test
    public void testInsertValues() throws Exception {
        assertMemoryLeak(() -> {
            for (int copierType = 0; copierType < 4; copierType++) {
                node1.setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierType);
                execute("CREATE TABLE dst (i INT, s LONG256, v LONG256)");
                for (int i = 0; i < GOOD.length; i++) {
                    final String literal = literal(GOOD[i][0]);
                    // a one-character literal is a CHAR, so the STRING is spelled out
                    execute("INSERT INTO dst VALUES (" + i + ", " + literal + "::STRING, " + literal + "::VARCHAR)");
                }
                for (String text : REFUSED) {
                    assertExceptionNoLeakCheck("INSERT INTO dst VALUES (100, " + literal(text) + "::STRING, NULL)", -1, refusal(text), false);
                    assertExceptionNoLeakCheck("INSERT INTO dst VALUES (100, NULL, " + literal(text) + "::VARCHAR)", -1, refusal(text), false);
                }
                assertQuery("dst")
                        .noLeakCheck()
                        .expectSize()
                        .returns(rows("i\ts\tv", 2, 0));
                execute("DROP TABLE dst");
            }
        });
    }

    // the good texts are rows 0 to 8 of src, the refused ones rows 100 and up, later in time
    private static void createSource() throws SqlException {
        execute("CREATE TABLE src (i INT, s STRING, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        for (int i = 0; i < GOOD.length; i++) {
            final String literal = literal(GOOD[i][0]);
            execute("INSERT INTO src VALUES (" + i + ", " + literal + ", " + literal + ", " + i + ")");
        }
        for (int r = 0; r < REFUSED.length; r++) {
            final String literal = literal(REFUSED[r]);
            execute("INSERT INTO src VALUES (" + (100 + r) + ", " + literal + ", " + literal + ", " + (100 + r) + ")");
        }
    }

    private static String literal(String text) {
        return text == null ? "NULL" : "'" + text + "'";
    }

    // INT columns that make copier type 2 split the copy, matching pads()
    private static String padColumns(int copierType) {
        final StringSink sink = new StringSink();
        if (copierType == RecordToRowCopierUtils.COPIER_TYPE_CHUNKED) {
            for (int p = 0; p < CHUNKING_PAD_COUNT; p++) {
                sink.put(", p").put(p).put(" INT");
            }
        }
        return sink.toString();
    }

    // the values of padColumns()
    private static String pads(int copierType) {
        final StringSink sink = new StringSink();
        if (copierType == RecordToRowCopierUtils.COPIER_TYPE_CHUNKED) {
            for (int p = 0; p < CHUNKING_PAD_COUNT; p++) {
                sink.put(", 0 p").put(p);
            }
        }
        return sink.toString();
    }

    // the message names STRING for VARCHAR text too; text that is not ASCII is quoted byte by byte
    private static String refusal(String text) {
        for (int i = 0, n = text.length(); i < n; i++) {
            if (text.charAt(i) > 127) {
                return "inconvertible value: `";
            }
        }
        return "inconvertible value: `" + text + "` [STRING -> LONG256]";
    }

    // the good texts as rows: i from firstId, then the value each spells, once per LONG256 column
    private static String rows(String header, int valueColumns, int firstId) {
        final StringSink sink = new StringSink();
        if (header != null) {
            sink.put(header).put('\n');
        }
        for (int i = 0; i < GOOD.length; i++) {
            sink.put(firstId + i);
            for (int c = 0; c < valueColumns; c++) {
                sink.put('\t').put(GOOD[i][1]);
            }
            sink.put('\n');
        }
        return sink.toString();
    }
}
