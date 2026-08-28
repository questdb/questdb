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
import io.questdb.test.griffin.fuzz.types.BooleanType;
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
 *     every address at or above 128.0.0.0 (issue 7547). Both of those pins
 *     demand TWO DISTINCT columns, which is why the fixture carries a second
 *     IPv4 and a second UUID column -- see {@link #orderingOverColumns};</li>
 *     <li>a SYMBOL column against a number spelled without quotes, whose sign
 *     the constant serializer dropped (issue 7548);</li>
 *     <li>a CHAR literal in the half of the code space a signed 16-bit lane
 *     reads as negative (issue 7549);</li>
 *     <li>a comparison nested as the operand of another comparison, e.g.
 *     {@code (a < b) = true} or {@code (a < b) = (c < d)}. The inner ordering
 *     comparison rewound the JIT filter's IR stream over bytes its siblings had
 *     already emitted, and the native backend answered the short operand stack
 *     with an out-of-bounds pop -- a JVM abort rather than a JIT decline, for
 *     CHAR (issue 7549) and IPv4 (issue 7547) alike. The grammar composed only
 *     NOT / AND / OR above leaf comparisons, so it reached neither operand
 *     order and CI stayed green through the defect.</li>
 * </ul>
 * The generator is random, so each assertion runs over a large corpus rather
 * than a single draw; the seed is fixed so a failure is reproducible.
 */
public class FilterShapeCoverageTest {
    private static final int CORPUS_SIZE = 20_000;
    // A bare quoted IPv4 address, in the spelling the filter compiler reads as
    // an i32 immediate. 'x.x.x.x'::IPv4 is a function node it declines, so the
    // negative lookahead keeps the cast form out.
    private static final String IPV4_LITERAL = "'\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}'(?!::)";
    // One comparison in parentheses -- the operand shape PredicateGenerator's
    // nested production emits. Both operands must carry no parentheses of their
    // own, which leaves out an operand that is itself a cast or a function call;
    // that costs nothing for a find() over the whole corpus, where the plain
    // column-and-literal spelling dominates. The AND / OR lookahead keeps an
    // ordinary conjunction group -- appendPredicate parenthesises those too --
    // from passing as a nested comparison, and (?!::) keeps a cast's
    // parenthesised inner expression out.
    private static final String NESTED_COMPARISON =
            "\\((?![^()\\n]*(?: AND | OR ))[^()\\n]+ (?:<=|>=|!=|<|>|=) [^()\\n]+\\)(?!::)";
    // NESTED_COMPARISON narrowed to an ORDERING inner operator. The lhs pin
    // below asks for this rather than the general form because only the
    // ordering expansion rewinds the IR stream: a generator that kept nesting
    // on the left but made every left-hand inner comparison an equality would
    // never reach the rewind, yet satisfies every pin phrased over
    // NESTED_COMPARISON.
    private static final String NESTED_ORDERING_COMPARISON =
            "\\((?![^()\\n]*(?: AND | OR ))[^()\\n]+ (?:<=|>=|<|>) [^()\\n]+\\)(?!::)";
    // Spellings the fuzz table factory must be able to deal out. Kept in sync
    // with FuzzColumnTypes.SINGLETONS by the deck test below.
    private static final String[] SINGLETON_DDLS = {
            "BOOLEAN", "BYTE", "SHORT", "CHAR", "INT", "LONG", "FLOAT", "DOUBLE",
            "DATE", "TIMESTAMP", "STRING", "VARCHAR", "SYMBOL", "LONG256", "UUID", "IPv4"
    };
    // 8-4-4-4-12, the spelling UuidType.randomLiteral emits.
    private static final String UUID_LITERAL =
            "'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'";

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
    public void testCharOrderingNestsInsideEquality() {
        // The crashing shape's CHAR half. A corpus that nested only an EQUALITY
        // comparison never reaches the ordering expansion, and the ordering
        // expansion is what rewinds the IR stream.
        assertCorpusMatches("nested CHAR ordering comparison", nestedOrderingOver("c_char"));
    }

    @Test
    public void testComparisonNestsOnBothSidesOfEquality() {
        assertCorpusMatches(
                "comparison nested on both sides of an equality",
                Pattern.compile(NESTED_COMPARISON + " (?:=|!=) " + NESTED_COMPARISON)
        );
    }

    @Test
    public void testComparisonNestsOnTheLeftOfEquality() {
        // PostOrderTreeTraversalAlgo descends the rhs first, so only a nested
        // comparison on the LEFT emitted sibling IR ahead of itself and had it
        // rewound away. A generator that nested only on the right would have
        // stayed green through the whole defect, so pin the two orders apart:
        // the lookahead rejects a right operand that opens a nested comparison
        // of its own, leaving this assertion to the (cmp) = <boolean> shape.
        // The inner operator has to be an ORDERING one, because the ordering
        // expansion is the only production that rewinds: left-nesting alone
        // does not reach the defect, and a generator that left-nests only
        // equalities passes every pin phrased over NESTED_COMPARISON.
        assertCorpusMatches(
                "ordering comparison nested on the left of an equality",
                Pattern.compile(NESTED_ORDERING_COMPARISON + " (?:=|!=) (?!\\()")
        );
    }

    @Test
    public void testComparisonNestsOnTheRightOfEquality() {
        // The order that accidentally worked. It still has to be generated: it
        // is the control that tells a truncated IR stream from a wrong one.
        assertCorpusMatches(
                "comparison nested on the right of an equality",
                Pattern.compile("(?:true|false|c_bool) (?:=|!=) " + NESTED_COMPARISON)
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
        // ipv4Col < ipv4Col2 is the shape issue 7547 reported, and the two
        // operands have to be DISTINCT columns for the corpus to hold it --
        // see orderingOverColumns for why a self-comparison is worth nothing.
        assertCorpusMatches("IPv4 ordering over two columns", orderingOverColumns("c_ip", "c_ip2"));
        // The other operand shape: a literal reaches the serializer as an i32
        // immediate rather than a column load, and both halves of the ordering
        // expansion re-traverse whatever they are given.
        assertCorpusMatches("IPv4 ordering against a literal", orderingAgainstLiteral("c_ip", IPV4_LITERAL));
    }

    @Test
    public void testIPv4LiteralKeepsTheJitEligibleSpelling() {
        // 'x.x.x.x' reaches the filter compiler as an i32 immediate;
        // 'x.x.x.x'::IPv4 is a function node it declines, so a corpus of only
        // the cast form would never compile an IPv4 predicate at all.
        assertCorpusMatches("bare quoted IPv4 literal", Pattern.compile(IPV4_LITERAL));
    }

    @Test
    public void testIPv4OrderingNestsInsideEquality() {
        // The crashing shape's IPv4 half. IPv4 and CHAR reach two separate
        // ordering expansions in the serializer, so one of them alone leaves
        // the other's rewind unexercised.
        assertCorpusMatches("nested IPv4 ordering comparison", nestedOrderingOver("c_ip"));
    }

    @Test
    public void testNestedComparisonMeetsABooleanColumn() {
        // A nested comparison against a plain BOOLEAN operand, rather than
        // against another comparison or a boolean constant. The constant is the
        // shape that declines JIT; a column keeps the whole predicate on the
        // compiled filter, so both belong in the corpus.
        assertCorpusMatches(
                "nested comparison against a BOOLEAN column",
                Pattern.compile("c_bool (?:=|!=) " + NESTED_COMPARISON
                        + "|" + NESTED_COMPARISON + " (?:=|!=) c_bool")
        );
    }

    @Test
    public void testNestedComparisonRidesUnderNot() {
        // appendPredicate composes NOT / AND / OR above whatever the leaf
        // produces, so the nested comparison reaches those positions for free.
        // Pin it anyway: "for free" is exactly the kind of claim a later change
        // to the leaf dispatch can quietly break.
        assertCorpusMatches(
                "nested comparison under NOT",
                Pattern.compile("NOT \\(" + NESTED_COMPARISON + " (?:=|!=) ")
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
        // uuidCol > uuidCol2 is the shape issue 7546 crashed the JVM on.
        assertCorpusMatches("UUID ordering over two columns", orderingOverColumns("c_uuid", "c_uuid2"));
        assertCorpusMatches("UUID ordering against a literal", orderingAgainstLiteral("c_uuid", UUID_LITERAL));
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
        columns.add(new FuzzColumn("c_bool", BooleanType.INSTANCE));
        // Two IPv4 and two UUID columns, because the ordering defects these
        // pins stand for are both two-column shapes and one column per type
        // cannot spell them. Adding a column reweights every draw the generator
        // makes, so the whole corpus shifts, but every other pin in this class
        // keeps a wide margin at the new distribution; the thinnest is "nested
        // comparison under NOT" at 176 matches over the 20_000-predicate
        // corpus.
        columns.add(new FuzzColumn("c_ip", IPv4Type.INSTANCE));
        columns.add(new FuzzColumn("c_ip2", IPv4Type.INSTANCE));
        columns.add(new FuzzColumn("c_uuid", UuidType.INSTANCE));
        columns.add(new FuzzColumn("c_uuid2", UuidType.INSTANCE));
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

    /**
     * Matches an ordering comparison over {@code column} that stands as the
     * operand of an outer equality, on either side of it and with the column on
     * either side of the inner operator.
     */
    private static Pattern nestedOrderingOver(String column) {
        String inner = "\\((?![^()\\n]*(?: AND | OR ))(?:"
                + column + " (?:<=|>=|<|>) [^()\\n]+"
                + "|[^()\\n]+ (?:<=|>=|<|>) " + column
                + ")\\)(?!::)";
        return Pattern.compile(inner + " (?:=|!=) |(?:=|!=) " + inner);
    }

    /**
     * Matches an ordering comparison between {@code column} and {@code literal},
     * in either operand order. A constant operand reaches the filter compiler as
     * an immediate rather than a column load, which is the other half of the
     * serializer's ordering expansion and needs its own pin now that
     * {@link #orderingOverColumns} demands two columns.
     */
    private static Pattern orderingAgainstLiteral(String column, String literal) {
        return Pattern.compile(
                "(?:" + column + " (?:<=|>=|<|>) " + literal
                        + "|" + literal + " (?:<=|>=|<|>) " + column + "\\b)"
        );
    }

    /**
     * Matches an ordering comparison between the two DISTINCT columns named, in
     * either operand order.
     * <p>
     * Distinct operands are the whole point. The predecessor of this helper
     * matched {@code c_ip (<=|>=|<|>) } on its own, which a SELF-comparison
     * satisfies -- and a self-comparison stands for nothing here. Measured on a
     * two-row IPv4 table:
     * <ul>
     *     <li>{@code WHERE c_ip < c_ip} plans as "Empty table".
     *     {@code WhereClauseParser.analyzeLess} finds
     *     {@code nodesEqual(lhs, rhs)} on a STRICT operator, sets the model's
     *     intrinsic value to FALSE, and no filter is compiled at all -- with or
     *     without a sibling conjunct beside it;</li>
     *     <li>{@code WHERE c_ip <= c_ip} does reach the compiled filter. The
     *     non-strict arm records a tautology and leaves the node in the
     *     residual. So does a self-comparison under an OR, or one nested inside
     *     another comparison.</li>
     * </ul>
     * What every surviving case has in common is that it is constant-valued for
     * every row: {@code c < c} is false and {@code c <= c} is true whatever the
     * address holds, so it separates a correct unsigned comparison from a signed
     * one exactly nowhere. And
     * {@code CompiledFilterIRSerializer.serializeIPv4Ordering} re-traverses each
     * operand up to six times, which one column on both sides cannot tell apart
     * from a left/right mix-up. Both defects this pin stands for -- issues 7546
     * and 7547 -- were reported as {@code col <op> col2}.
     * <p>
     * The trailing {@code \b} carries weight: without it {@code c_ip2 < c_ip2}
     * would pass, because "{@code < c_ip}" matches a prefix of
     * "{@code < c_ip2}".
     */
    private static Pattern orderingOverColumns(String a, String b) {
        return Pattern.compile(
                "(?:" + a + " (?:<=|>=|<|>) " + b
                        + "|" + b + " (?:<=|>=|<|>) " + a
                        + ")\\b"
        );
    }
}
