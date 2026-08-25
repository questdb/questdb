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

package io.questdb.test.griffin.fuzz;

import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.types.CharType;
import io.questdb.test.griffin.fuzz.types.FuzzColumnType;
import io.questdb.test.griffin.fuzz.types.FuzzColumnTypes;
import io.questdb.test.griffin.fuzz.types.IPv4Type;
import io.questdb.test.griffin.fuzz.types.IntType;
import io.questdb.test.griffin.fuzz.types.Long256Type;
import io.questdb.test.griffin.fuzz.types.SymbolType;
import io.questdb.test.griffin.fuzz.types.TimestampType;
import io.questdb.test.griffin.fuzz.types.UuidType;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 * Pins the WHERE-clause shapes {@link PredicateGenerator} has to keep
 * producing. Every one of them stands for a JIT filter defect the fuzzer used
 * to walk straight past because nothing in the corpus reached the code path:
 * <ul>
 *     <li>an ordering comparison over UUID, which crashed the JVM while
 *     compiling the filter (issue 7546);</li>
 *     <li>an ordering comparison over IPv4, which returned the wrong rows for
 *     every address at or above 128.0.0.0 (issue 7547);</li>
 *     <li>a SYMBOL column against a number spelled without quotes, whose sign
 *     the constant serializer dropped (issue 7548);</li>
 *     <li>a CHAR literal in the half of the code space a signed 16-bit lane
 *     reads as negative (issue 7549).</li>
 * </ul>
 * The generator is random, so each assertion runs over a large corpus rather
 * than a single draw; the seed is fixed so a failure is reproducible.
 */
public class FilterShapeCoverageTest {
    private static final int CORPUS_SIZE = 20_000;
    // Spellings the fuzz table factory must be able to deal out. Kept in sync
    // with FuzzColumnTypes.SINGLETONS by the deck test below.
    private static final String[] SINGLETON_DDLS = {
            "BOOLEAN", "BYTE", "SHORT", "CHAR", "INT", "LONG", "FLOAT", "DOUBLE",
            "DATE", "TIMESTAMP", "STRING", "VARCHAR", "SYMBOL", "LONG256", "UUID", "IPv4"
    };

    @Test
    public void testCharComparesAgainstHighCodePointLiteral() {
        // A CHAR sits in a 16-bit lane the JIT backends sign-extend, so a
        // literal at or above 0x8000 is the one that tells a correct unsigned
        // comparison from a signed one.
        assertCorpusMatches("high code point CHAR literal", Pattern.compile("'[\\u8000-\\uffff]'"));
    }

    @Test
    public void testCharComparesAgainstUnquotedNumber() {
        assertCorpusMatches(
                "CHAR against an unquoted number",
                Pattern.compile("c_char (=|!=) -?\\d|-?\\d+ (=|!=) c_char")
        );
    }

    @Test
    public void testDeckDealsEveryRegisteredType() {
        ObjList<FuzzColumnType> deck = FuzzColumnTypes.shuffledDeck(new Rnd(7, 11));
        Assert.assertEquals("deck holds one entry per registered family",
                SINGLETON_DDLS.length + 2, deck.size());

        CharSequenceHashSet dealt = new CharSequenceHashSet();
        int decimals = 0;
        int arrays = 0;
        for (int i = 0, n = deck.size(); i < n; i++) {
            String ddl = deck.getQuick(i).getDdl();
            if (ddl.startsWith("DECIMAL")) {
                decimals++;
            } else if (ddl.startsWith("DOUBLE[")) {
                arrays++;
            } else {
                Assert.assertTrue("type dealt twice: " + ddl, dealt.add(ddl));
            }
        }
        Assert.assertEquals(1, decimals);
        Assert.assertEquals(1, arrays);
        for (String ddl : SINGLETON_DDLS) {
            Assert.assertTrue("type missing from the deck: " + ddl, dealt.contains(ddl));
        }
    }

    @Test
    public void testIPv4ComparesWithOrderingOperator() {
        assertCorpusMatches("IPv4 ordering comparison", orderingOver("c_ip"));
    }

    @Test
    public void testIPv4LiteralKeepsTheJitEligibleSpelling() {
        // 'x.x.x.x' reaches the filter compiler as an i32 immediate;
        // 'x.x.x.x'::IPv4 is a function node it declines, so a corpus of only
        // the cast form would never compile an IPv4 predicate at all.
        assertCorpusMatches(
                "bare quoted IPv4 literal",
                Pattern.compile("'\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}'(?!::)")
        );
    }

    @Test
    public void testNotInPredicate() {
        assertCorpusMatches("NOT IN predicate", Pattern.compile("NOT IN \\("));
    }

    @Test
    public void testSymbolComparesAgainstNegativeUnquotedNumber() {
        assertCorpusMatches(
                "SYMBOL against a negative unquoted number",
                Pattern.compile("sym (=|!=) -\\d|-\\d+ (=|!=) sym")
        );
    }

    @Test
    public void testUuidComparesWithOrderingOperator() {
        assertCorpusMatches("UUID ordering comparison", orderingOver("c_uuid"));
    }

    private static void assertCorpusMatches(String what, Pattern pattern) {
        String corpus = generateCorpus();
        Assert.assertTrue(
                "the generator produced no " + what + " in " + CORPUS_SIZE + " predicates",
                pattern.matcher(corpus).find()
        );
    }

    private static ObjList<FuzzColumn> columns() {
        ObjList<FuzzColumn> columns = new ObjList<>();
        columns.add(new FuzzColumn("sym", SymbolType.INSTANCE));
        columns.add(new FuzzColumn("c_char", CharType.INSTANCE));
        columns.add(new FuzzColumn("c_ip", IPv4Type.INSTANCE));
        columns.add(new FuzzColumn("c_uuid", UuidType.INSTANCE));
        columns.add(new FuzzColumn("c_l256", Long256Type.INSTANCE));
        columns.add(new FuzzColumn("c_int", IntType.INSTANCE));
        columns.add(new FuzzColumn("ts", TimestampType.INSTANCE));
        return columns;
    }

    private static String generateCorpus() {
        ObjList<FuzzColumn> columns = columns();
        Rnd rnd = new Rnd(0x5eed, 0xf117e5);
        StringSink corpus = new StringSink();
        for (int i = 0; i < CORPUS_SIZE; i++) {
            corpus.put(new PredicateGenerator(rnd, 2).generate(columns, null, null)).put('\n');
        }
        return corpus.toString();
    }

    private static Pattern orderingOver(String column) {
        return Pattern.compile(column + " (<=|>=|<|>) |(<=|>=|<|>) " + column);
    }
}
