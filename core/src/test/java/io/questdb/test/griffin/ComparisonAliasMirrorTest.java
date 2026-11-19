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

import io.questdb.cairo.SqlJitMode;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A negated or swapped comparison returns exactly what its base returns: {@code a != b} and {@code a <> b} what
 * {@code NOT (a = b)} returns, {@code b > a} what {@code a < b} returns, and {@code b <= a} what {@code a >= b}
 * returns. Each test compares the spellings on values where the overloads the spellings bound before disagreed: a
 * text that parses as LONG but not as INT, the INT NULL sentinel spelled as text, an IPv4 address compared as text and
 * as a number, and a UUID compared as text and as a value. {@code OverloadNullTieBreakTest} pins the overload every
 * spelling binds; these tests pin what the user sees.
 */
public class ComparisonAliasMirrorTest extends AbstractCairoTest {

    @Test
    public void testIPv4EqualsVarchar() throws Exception {
        // IPv4 = VARCHAR compares the address as text, so IPv4 != VARCHAR must too: it used to parse the VARCHAR as
        // an address, which rejected a VARCHAR column and read '0.0.0.0' as the NULL address
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ip IPv4, v VARCHAR)");
            execute("""
                    INSERT INTO t VALUES
                        ('1.2.3.4', '1.2.3.4'),
                        ('1.2.3.4', '001.002.003.004'),
                        (NULL, '0.0.0.0'),
                        (NULL, NULL),
                        ('1.2.3.4', NULL)
                    """);
            assertQuery("SELECT ip, v, ip = v eq, ip != v ne, ip <> v ltgt, NOT (ip = v) not_eq, v != ip ne_mirror FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ip\tv\teq\tne\tltgt\tnot_eq\tne_mirror
                            1.2.3.4\t1.2.3.4\ttrue\tfalse\tfalse\tfalse\tfalse
                            1.2.3.4\t001.002.003.004\tfalse\ttrue\ttrue\ttrue\ttrue
                            \t0.0.0.0\tfalse\ttrue\ttrue\ttrue\ttrue
                            \t\ttrue\tfalse\tfalse\tfalse\tfalse
                            1.2.3.4\t\tfalse\ttrue\ttrue\ttrue\ttrue
                            """);
            final String notEqualRows = """
                    ip\tv
                    1.2.3.4\t001.002.003.004
                    \t0.0.0.0
                    1.2.3.4\t
                    """;
            assertFilterInJitModes("SELECT ip, v FROM t WHERE ip != v", notEqualRows);
            assertFilterInJitModes("SELECT ip, v FROM t WHERE NOT (ip = v)", notEqualRows);
            final String constantRows = """
                    ip\teq\tne\tnot_eq
                    1.2.3.4\tfalse\ttrue\ttrue
                    1.2.3.4\tfalse\ttrue\ttrue
                    \tfalse\ttrue\ttrue
                    \tfalse\ttrue\ttrue
                    1.2.3.4\tfalse\ttrue\ttrue
                    """;
            assertQuery("SELECT ip, ip = '0.0.0.0'::VARCHAR eq, ip != '0.0.0.0'::VARCHAR ne, NOT (ip = '0.0.0.0'::VARCHAR) not_eq FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns(constantRows);
            bindVariableService.clear();
            bindVariableService.setVarchar(0, new Utf8String("0.0.0.0"));
            assertQuery("SELECT ip, ip = $1 eq, ip != $1 ne, NOT (ip = $1) not_eq FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns(constantRows);
        });
    }

    @Test
    public void testIPv4OrderedAgainstVarchar() throws Exception {
        // IPv4 < VARCHAR parses the VARCHAR as an address, so VARCHAR > IPv4 must too: it used to compare the address
        // as text, which ordered '10.0.0.1' before '9.0.0.1'
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ip IPv4, v VARCHAR)");
            execute("INSERT INTO t VALUES ('10.0.0.1', '9.0.0.1'), ('9.0.0.1', '10.0.0.1'), ('1.2.3.4', '1.2.3.4'), (NULL, '1.2.3.4')");
            final String constantRows = """
                    ip\tlt\tgt_mirror\tge\tle_mirror
                    10.0.0.1\tfalse\tfalse\ttrue\ttrue
                    9.0.0.1\tfalse\tfalse\ttrue\ttrue
                    1.2.3.4\ttrue\ttrue\tfalse\tfalse
                    \tfalse\tfalse\tfalse\tfalse
                    """;
            assertQuery("""
                    SELECT ip,
                        ip < '9.0.0.1'::VARCHAR lt, '9.0.0.1'::VARCHAR > ip gt_mirror,
                        ip >= '9.0.0.1'::VARCHAR ge, '9.0.0.1'::VARCHAR <= ip le_mirror
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(constantRows);
            bindVariableService.clear();
            bindVariableService.setVarchar(0, new Utf8String("9.0.0.1"));
            assertQuery("SELECT ip, ip < $1 lt, $1 > ip gt_mirror, ip >= $1 ge, $1 <= ip le_mirror FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns(constantRows);
            final String lessRows = """
                    ip
                    1.2.3.4
                    """;
            assertFilterInJitModes("SELECT ip FROM t WHERE ip < '9.0.0.1'::VARCHAR", lessRows);
            assertFilterInJitModes("SELECT ip FROM t WHERE '9.0.0.1'::VARCHAR > ip", lessRows);
            // the address overload takes a constant VARCHAR only, in every spelling
            assertQuery("SELECT ip < v FROM t").noLeakCheck().fails(12, "STRING constant expected");
            assertQuery("SELECT v > ip FROM t").noLeakCheck().fails(7, "STRING constant expected");
            assertQuery("SELECT ip >= v FROM t").noLeakCheck().fails(13, "STRING constant expected");
            assertQuery("SELECT v <= ip FROM t").noLeakCheck().fails(7, "STRING constant expected");
        });
    }

    @Test
    public void testIntEqualsSymbolConstant() throws Exception {
        // INT = SYMBOL constant compares the INT as text with the symbol, so INT != SYMBOL constant must too: it used
        // to parse the symbol as INT, which read '-2147483648' as NULL and '007' as 7
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (i INT)");
            execute("INSERT INTO t VALUES (NULL), (-2_147_483_647), (0), (7)");
            assertQuery("""
                    SELECT i,
                        i = '-2147483648'::SYMBOL eq, i != '-2147483648'::SYMBOL ne, NOT (i = '-2147483648'::SYMBOL) not_eq,
                        i = '007'::SYMBOL eq7, i <> '007'::SYMBOL ne7, NOT (i = '007'::SYMBOL) not_eq7
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i\teq\tne\tnot_eq\teq7\tne7\tnot_eq7
                            null\tfalse\ttrue\ttrue\tfalse\ttrue\ttrue
                            -2147483647\tfalse\ttrue\ttrue\tfalse\ttrue\ttrue
                            0\tfalse\ttrue\ttrue\tfalse\ttrue\ttrue
                            7\tfalse\ttrue\ttrue\tfalse\ttrue\ttrue
                            """);
            final String allRows = """
                    i
                    null
                    -2147483647
                    0
                    7
                    """;
            assertFilterInJitModes("SELECT i FROM t WHERE i != '-2147483648'::SYMBOL", allRows);
            assertFilterInJitModes("SELECT i FROM t WHERE NOT (i = '-2147483648'::SYMBOL)", allRows);
            assertFilterInJitModes("SELECT i FROM t WHERE i != '007'::SYMBOL", allRows);
        });
    }

    @Test
    public void testStringOrderedAgainstNarrowInteger() throws Exception {
        // STRING < BYTE compares as INT, so STRING >= BYTE, STRING > BYTE, STRING <= BYTE and their mirrors must too:
        // some of them used to compare as LONG, which read '-2147483648' as a number rather than as the INT NULL and
        // accepted '2147483648' rather than rejecting it
        final String[] textTypes = {"STRING", "VARCHAR"};
        final String[] integerTypes = {"BYTE", "SHORT"};
        for (String textType : textTypes) {
            for (String integerType : integerTypes) {
                assertMemoryLeak(() -> assertTextOrderedAgainstInteger(textType, integerType));
            }
        }
    }

    @Test
    public void testSymbolEqualsIPv4() throws Exception {
        // SYMBOL = IPv4 parses the symbol as an address, so SYMBOL != IPv4 must too: it used to compare the address
        // as text, which accepted a SYMBOL column that = rejects and told '0.0.0.0' from the NULL address
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ip IPv4)");
            execute("INSERT INTO t VALUES ('1.2.3.4', NULL), ('1.2.3.4', '1.2.3.4')");
            assertQuery("""
                    SELECT ip,
                        '0.0.0.0'::SYMBOL = ip eq, '0.0.0.0'::SYMBOL != ip ne, NOT ('0.0.0.0'::SYMBOL = ip) not_eq,
                        '001.002.003.004'::SYMBOL = ip eq_padded, '001.002.003.004'::SYMBOL <> ip ne_padded,
                        ip != '001.002.003.004'::SYMBOL ne_padded_mirror
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ip\teq\tne\tnot_eq\teq_padded\tne_padded\tne_padded_mirror
                            \ttrue\tfalse\tfalse\tfalse\ttrue\ttrue
                            1.2.3.4\tfalse\ttrue\ttrue\ttrue\tfalse\tfalse
                            """);
            assertFilterInJitModes("SELECT ip FROM t WHERE '0.0.0.0'::SYMBOL != ip", """
                    ip
                    1.2.3.4
                    """);
            // the address overload takes a constant symbol only, in every spelling
            assertQuery("SELECT s = ip FROM t").noLeakCheck().fails(7, "STRING constant expected");
            assertQuery("SELECT s != ip FROM t").noLeakCheck().fails(7, "STRING constant expected");
            assertQuery("SELECT ip <> s FROM t").noLeakCheck().fails(13, "STRING constant expected");
        });
    }

    @Test
    public void testUuidEqualsIPv4() throws Exception {
        // UUID = IPv4 parses the UUID text as an address, which takes a constant only, so UUID != IPv4 must reject a
        // UUID column too: it used to compare them as UUIDs
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (u UUID, ip IPv4)");
            assertQuery("SELECT u = ip FROM t").noLeakCheck().fails(7, "STRING constant expected");
            assertQuery("SELECT u != ip FROM t").noLeakCheck().fails(7, "STRING constant expected");
            assertQuery("SELECT ip <> u FROM t").noLeakCheck().fails(13, "STRING constant expected");
        });
    }

    @Test
    public void testUuidEqualsText() throws Exception {
        // UUID = VARCHAR and UUID = SYMBOL compare the UUID as text, so their negations must too: they used to parse
        // the text as a UUID, which matched an upper-case UUID that = does not match
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (u UUID, v VARCHAR, s SYMBOL)");
            execute("""
                    INSERT INTO t VALUES
                        ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', 'AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA'),
                        ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'),
                        (NULL, NULL, NULL)
                    """);
            assertQuery("""
                    SELECT u = v eq_v, u != v ne_v, NOT (u = v) not_eq_v, v <> u ne_v_mirror,
                        u = s eq_s, u != s ne_s, NOT (u = s) not_eq_s, s <> u ne_s_mirror
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            eq_v\tne_v\tnot_eq_v\tne_v_mirror\teq_s\tne_s\tnot_eq_s\tne_s_mirror
                            false\ttrue\ttrue\ttrue\tfalse\ttrue\ttrue\ttrue
                            true\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse
                            true\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse
                            """);
            final String upperCaseRows = """
                    u
                    aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    """;
            assertFilterInJitModes("SELECT u FROM t WHERE u != v", upperCaseRows);
            assertFilterInJitModes("SELECT u FROM t WHERE u != s", upperCaseRows);
            final String constantRows = """
                    eq\tne\tnot_eq
                    false\ttrue\ttrue
                    false\ttrue\ttrue
                    false\ttrue\ttrue
                    """;
            assertQuery("""
                    SELECT u = 'AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA'::VARCHAR eq, u != 'AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA'::VARCHAR ne,
                        NOT (u = 'AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA'::VARCHAR) not_eq
                    FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns(constantRows);
            bindVariableService.clear();
            bindVariableService.setVarchar(0, new Utf8String("AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"));
            assertQuery("SELECT u = $1 eq, u != $1 ne, NOT (u = $1) not_eq FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns(constantRows);
        });
    }

    private void assertFilterInJitModes(String sql, String expected) throws Exception {
        final int jitMode = sqlExecutionContext.getJitMode();
        try {
            for (int mode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_ENABLED}) {
                sqlExecutionContext.setJitMode(mode);
                assertQuery(sql).noLeakCheck().returns(expected);
            }
        } finally {
            sqlExecutionContext.setJitMode(jitMode);
        }
    }

    private void assertTextOrderedAgainstInteger(String textType, String integerType) throws Exception {
        final String table = "t_" + textType + '_' + integerType;
        execute("CREATE TABLE " + table + " (s " + textType + ", n " + integerType + ")");
        execute("INSERT INTO " + table + " VALUES ('-2147483648', -128), ('-2147483647', -128), ('-1', 0), ('127', 127), ('2147483647', 127), (NULL, 0)");
        // every spelling compares as INT, where '-2147483648' is NULL and NULL orders against nothing
        assertQuery("SELECT s, n, s < n lt, n > s gt_mirror, s >= n ge, n <= s le_mirror, s > n gt, n < s lt_mirror, s <= n le, n >= s ge_mirror FROM " + table)
                .noLeakCheck()
                .expectSize()
                .returns("""
                        s\tn\tlt\tgt_mirror\tge\tle_mirror\tgt\tlt_mirror\tle\tge_mirror
                        -2147483648\t-128\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse
                        -2147483647\t-128\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue\ttrue
                        -1\t0\ttrue\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue\ttrue
                        127\t127\tfalse\tfalse\ttrue\ttrue\tfalse\tfalse\ttrue\ttrue
                        2147483647\t127\tfalse\tfalse\ttrue\ttrue\ttrue\ttrue\tfalse\tfalse
                        \t0\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse
                        """);
        final String lessOrEqualRows = """
                s\tn
                -2147483647\t-128
                -1\t0
                127\t127
                """;
        assertFilterInJitModes("SELECT s, n FROM " + table + " WHERE s <= n", lessOrEqualRows);
        assertFilterInJitModes("SELECT s, n FROM " + table + " WHERE n >= s", lessOrEqualRows);
        final String greaterRows = """
                s\tn
                2147483647\t127
                """;
        assertFilterInJitModes("SELECT s, n FROM " + table + " WHERE s > n", greaterRows);
        assertFilterInJitModes("SELECT s, n FROM " + table + " WHERE n < s", greaterRows);

        // the INT NULL spelled as a constant, a bind variable and an integer constant
        final String nullText = "'-2147483648'::" + textType;
        final String neverRows = """
                n\tle\tge_mirror\tgt\tlt_mirror
                -128\tfalse\tfalse\tfalse\tfalse
                -128\tfalse\tfalse\tfalse\tfalse
                0\tfalse\tfalse\tfalse\tfalse
                127\tfalse\tfalse\tfalse\tfalse
                127\tfalse\tfalse\tfalse\tfalse
                0\tfalse\tfalse\tfalse\tfalse
                """;
        assertQuery("SELECT n, " + nullText + " <= n le, n >= " + nullText + " ge_mirror, " + nullText + " > n gt, n < " + nullText + " lt_mirror FROM " + table)
                .noLeakCheck()
                .expectSize()
                .returns(neverRows);
        bindVariableService.clear();
        if ("STRING".equals(textType)) {
            bindVariableService.setStr(0, "-2147483648");
        } else {
            bindVariableService.setVarchar(0, new Utf8String("-2147483648"));
        }
        assertQuery("SELECT n, $1 <= n le, n >= $1 ge_mirror, $1 > n gt, n < $1 lt_mirror FROM " + table)
                .noLeakCheck()
                .expectSize()
                .returns(neverRows);
        final String bound = "(-128)::" + integerType;
        assertQuery("SELECT s, s <= " + bound + " le, " + bound + " >= s ge_mirror, s > " + bound + " gt, " + bound + " < s lt_mirror FROM " + table)
                .noLeakCheck()
                .expectSize()
                .returns("""
                        s\tle\tge_mirror\tgt\tlt_mirror
                        -2147483648\tfalse\tfalse\tfalse\tfalse
                        -2147483647\ttrue\ttrue\tfalse\tfalse
                        -1\tfalse\tfalse\ttrue\ttrue
                        127\tfalse\tfalse\ttrue\ttrue
                        2147483647\tfalse\tfalse\ttrue\ttrue
                        \tfalse\tfalse\tfalse\tfalse
                        """);

        // a text beyond INT fails in every spelling
        final String error = "inconvertible value: `2147483648` [" + textType + " -> INT]";
        final String wideText = "'2147483648'::" + textType;
        assertQuery("SELECT " + wideText + " >= n FROM " + table).noLeakCheck().failsWith(error);
        assertQuery("SELECT n <= " + wideText + " FROM " + table).noLeakCheck().failsWith(error);
        assertQuery("SELECT " + wideText + " > n FROM " + table).noLeakCheck().failsWith(error);
        assertQuery("SELECT n < " + wideText + " FROM " + table).noLeakCheck().failsWith(error);
    }
}
