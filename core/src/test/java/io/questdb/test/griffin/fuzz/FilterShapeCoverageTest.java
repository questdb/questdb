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

import io.questdb.griffin.SqlException;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.clauses.GroupByClause;
import io.questdb.test.griffin.fuzz.clauses.SampleByClause;
import io.questdb.test.griffin.fuzz.expr.ColumnRefExpr;
import io.questdb.test.griffin.fuzz.expr.ExpressionGenerator;
import io.questdb.test.griffin.fuzz.expr.FuzzExpr;
import io.questdb.test.griffin.fuzz.types.BooleanType;
import io.questdb.test.griffin.fuzz.types.CharType;
import io.questdb.test.griffin.fuzz.types.ColumnKind;
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
 * Four more pins stand beside those and cover what the fuzzer DRAWS rather
 * than what it spells: {@link #testDeckDealsEveryRegisteredType} on the type
 * deck, {@link #testFactoryDealsSameTypeColumnPairs} on the schemas the factory
 * deals off it, {@link #testIdentifierKeySlotDrawFollowsTheTable} on the kind
 * the GROUP BY / SAMPLE BY key-slot pickers draw when they want an identifier,
 * and {@link #testIdentifierKeySlotLandsOnARealColumn} on what the expression
 * generator then fills that slot with. A predicate shape the corpus can spell
 * is worth nothing if no generated table or key slot ever reaches it.
 * <p>
 * The generator is random, so each assertion runs over a large corpus rather
 * than a single draw; the seed is fixed so a failure is reproducible.
 */
public class FilterShapeCoverageTest {
    private static final int CORPUS_SIZE = 20_000;
    // Simulated fuzz runs testFactoryDealsSameTypeColumnPairs drives through
    // FuzzTableFactory. One sweep counts every pinned type at once, so 2_000
    // runs -- about 5_000 tables -- take a single pass measured at ~50ms of the
    // class's ~0.6s, and yield roughly 100 tables of each pinned shape. That
    // yield is what buys the real lower bound below; 400 runs yielded about 20,
    // too few to bound with a margin that does not flake.
    private static final int FACTORY_SWEEP_RUNS = 2_000;
    // Identifier key slots testIdentifierKeySlotLandsOnARealColumn draws per
    // table. Four over the sweep's ~5_000 tables give ~20_000 draws, enough
    // that the fill rate moves by well under a point between seeds.
    private static final int IDENTIFIER_KEY_SLOT_DRAWS_PER_TABLE = 4;
    // Draws testIdentifierKeySlotDrawFollowsTheTable takes from each key-kind
    // picker. One option in seven is the GROUP BY list's identifier slot and
    // one in six is SAMPLE BY's, so 2_000 draws leave a few hundred identifier
    // draws per picker -- far more than the handful a blind draw needs to
    // answer with a kind the fixture carries no column of. The sweep is
    // integer arithmetic over a fixed column list: no engine, no SQL, no
    // native memory.
    private static final int IDENTIFIER_KEY_SLOT_PICKER_DRAWS = 2_000;
    // A bare quoted IPv4 address, in the spelling the filter compiler reads as
    // an i32 immediate. 'x.x.x.x'::IPv4 is a function node it declines, so the
    // negative lookahead keeps the cast form out.
    private static final String IPV4_LITERAL = "'\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}'(?!::)";
    // Floor testIdentifierKeySlotLandsOnARealColumn holds the share of
    // identifier key-slot draws that reach a real column to. That test states
    // the seed sweep the margin comes from.
    private static final int MIN_IDENTIFIER_KEY_SLOT_FILL_PCT = 40;
    // Floor testFactoryDealsSameTypeColumnPairs holds each same-type pair count
    // to. That test states the seed sweep the margin comes from.
    private static final int MIN_SAME_TYPE_PAIR_TABLES = 40;
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
    public void testFactoryDealsSameTypeColumnPairs() throws SqlException {
        // The fixture in columns() is hand-built, so the two-column ordering
        // pins over it stand for PredicateGenerator's ability to spell the
        // shapes. This pin covers the other half of the claim -- that
        // QueryFuzzTest's corpus can actually hold such a schema -- by driving
        // FuzzTableFactory itself.
        //
        // The pair arrives late in a run. The factory deals off ONE shuffled
        // 18-card deck per run (16 singletons plus a DECIMAL and a DOUBLE[]
        // instance), without replacement, and refills it only when it is empty,
        // which never happens once it is filled; only after the deck runs out
        // does dealType fall back to a with-replacement draw. A run builds 2-3
        // tables of 5..12 dealt columns each -- 21.3 columns on average against
        // an 18-card deck, so 65% of runs spend it. A table dealt wholly off a
        // fresh deck repeats no DEALT type, which is why the FIRST table of a
        // run never carries an IPv4 or a UUID pair. That holds for the DEALT
        // columns only: buildColumnList adds `sym` and `ts` outside the deal
        // and both SymbolType and TimestampType also sit in the deck, so a
        // first table pairs those two types constantly -- of 200_000 first
        // tables, 94_291 carried two SYMBOL columns and 94_462 two TIMESTAMP
        // columns. Over the 500_020 tables those same runs built, 10_009
        // (2.00%) carried two IPv4 columns and 9_985 (2.00%) two UUID columns,
        // and not one of those was a first table.
        //
        // What this pin protects is the RUN-LONG deck. Reshuffling per table
        // instead -- clearing the deck at the top of buildColumnList -- caps a
        // table at 12 dealt cards off 18, so no dealt type can repeat and both
        // counts drop to exactly 0 (measured: 0 over 500 seeds x 2_000 runs).
        // Do NOT read the pin as guarding the pickRandom fallback: dealing
        // strictly without replacement, reshuffling a fresh deck whenever the
        // run-long one runs out, leaves the deck boundary inside a table and
        // still yields 64 IPv4-pair and 65 UUID-pair tables at the pinned seed,
        // which clears the bound below.
        //
        // The bound is a floor with room under it, not the observed yield: the
        // pinned seed gives 112 and 104. Two 10_000-seed families -- one of
        // Rnd(0x5eed + i, 0xdec4 + i) for i in [0, 10_000), one of 10_000 seed
        // pairs drawn from Rnd(1, 2) -- put the leanest sweep at 61 IPv4-pair
        // and 65 UUID-pair tables, and no sweep in either family landed at or
        // below 40. Twenty thousand samples evidence a floor rather than prove
        // one, so the bound sits at 40: it fails a change that cuts this
        // shape's frequency by more than about 2.5x, and leaves about 1.5x of
        // headroom under the leanest sweep observed.
        //
        // The sweep is deterministic -- one seeded Rnd, no-op SQL executor and
        // WAL drain, no engine and no native memory, column lists only.
        ObjList<FuzzColumnType> pinnedTypes = new ObjList<>();
        pinnedTypes.add(IPv4Type.INSTANCE);
        pinnedTypes.add(UuidType.INSTANCE);
        int[] pairs = countSameTypePairTables(new Rnd(0x5eed, 0xdec4), pinnedTypes);
        Assert.assertTrue(
                "the factory dealt " + pairs[0] + " tables carrying two IPv4 columns over "
                        + FACTORY_SWEEP_RUNS + " runs, want at least " + MIN_SAME_TYPE_PAIR_TABLES,
                pairs[0] >= MIN_SAME_TYPE_PAIR_TABLES
        );
        Assert.assertTrue(
                "the factory dealt " + pairs[1] + " tables carrying two UUID columns over "
                        + FACTORY_SWEEP_RUNS + " runs, want at least " + MIN_SAME_TYPE_PAIR_TABLES,
                pairs[1] >= MIN_SAME_TYPE_PAIR_TABLES
        );
    }

    @Test
    public void testIdentifierKeySlotDrawFollowsTheTable() {
        // GroupByClause and SampleByClause each hold one identifier slot in
        // their key-kind option list, and both fill it from
        // ExpressionGenerator.pickIdentifierKind rather than drawing one of
        // UUID, IPv4 and LONG256 blind. This pin drives THOSE TWO PICKERS,
        // which is the wiring testIdentifierKeySlotLandsOnARealColumn cannot
        // see: that pin calls pickIdentifierKind itself, so dropping
        // ColumnKind.randomIdentifier back into either option list orphans the
        // table-aware picker -- restoring the whole defect -- and leaves it
        // green. Both pickers are public so this one can reach them.
        //
        // The fixture carries exactly ONE identifier type: an IPv4 column, no
        // UUID and no LONG256. That makes the assertion exact and spares it a
        // rate and a threshold -- a picker that follows the table can only ever
        // answer IPV4, while a blind draw answers UUID or LONG256 two times in
        // three and so fails within the first handful of identifier draws.
        // The shared columns() fixture cannot serve here: it carries all three
        // identifier types, so every kind a blind draw can produce is one it
        // really has a column of.
        ObjList<FuzzColumn> columns = oneIdentifierTypeColumns();
        Rnd rnd = new Rnd(0x5eed, 0xdec4);
        ExpressionGenerator gen = new ExpressionGenerator(rnd, columns, null, 2);
        int groupByDraws = 0;
        int groupByOffTable = 0;
        int sampleByDraws = 0;
        int sampleByOffTable = 0;
        for (int i = 0; i < IDENTIFIER_KEY_SLOT_PICKER_DRAWS; i++) {
            ColumnKind groupByKind = GroupByClause.pickGroupableKind(rnd, gen);
            if (groupByKind.isIdentifier()) {
                groupByDraws++;
                if (groupByKind != ColumnKind.IPV4) {
                    groupByOffTable++;
                }
            }
            ColumnKind sampleByKind = SampleByClause.pickGroupableKind(rnd, gen);
            if (sampleByKind.isIdentifier()) {
                sampleByDraws++;
                if (sampleByKind != ColumnKind.IPV4) {
                    sampleByOffTable++;
                }
            }
        }
        // An option list that stopped offering an identifier kind at all would
        // satisfy the two assertions below for free, so demand the sample
        // actually held identifier draws first.
        Assert.assertTrue(
                "the GROUP BY key-kind picker drew no identifier kind at all over "
                        + IDENTIFIER_KEY_SLOT_PICKER_DRAWS + " draws",
                groupByDraws > 0
        );
        Assert.assertTrue(
                "the SAMPLE BY key-kind picker drew no identifier kind at all over "
                        + IDENTIFIER_KEY_SLOT_PICKER_DRAWS + " draws",
                sampleByDraws > 0
        );
        Assert.assertEquals(
                "the GROUP BY key-kind picker drew an identifier kind the table carries no column of "
                        + groupByOffTable + " times out of " + groupByDraws + " identifier draws",
                0, groupByOffTable
        );
        Assert.assertEquals(
                "the SAMPLE BY key-kind picker drew an identifier kind the table carries no column of "
                        + sampleByOffTable + " times out of " + sampleByDraws + " identifier draws",
                0, sampleByOffTable
        );
    }

    @Test
    public void testIdentifierKeySlotLandsOnARealColumn() throws SqlException {
        // A GROUP BY or SAMPLE BY key slot that asks for "an identifier" has to
        // reach a real UUID, IPv4 or LONG256 COLUMN. A literal in that slot
        // buckets the whole table into one row and exercises nothing.
        //
        // This pin calls pickIdentifierKind itself and measures the FILL RATE
        // the whole generateOfKind path reaches over the tables the factory
        // really deals; testIdentifierKeySlotDrawFollowsTheTable covers the
        // other half, that the two clause pickers still route their identifier
        // slot through it. Neither implies the other: a leaf that stopped
        // preferring a real column would sink the rate below with the wiring
        // pin green, and an option list that went back to a blind draw would
        // fail the wiring pin while this sweep, driving the picker directly,
        // never notices.
        //
        // Giving UUID, IPv4 and LONG256 a kind each is what made the ordering
        // defects above findable and it stands, but a concrete kind matches
        // only its own columns: of the tables FuzzTableFactory deals, 41% carry
        // exactly one of the three types and 18% carry none, so a picker
        // drawing one of the three blind sent the slot to a kind the table had
        // no column of most of the time, and generateLeafOfKind answered with a
        // constant. ExpressionGenerator.pickIdentifierKind draws among the
        // kinds the table carries instead.
        //
        // Both rates over this sweep: the blind uniform draw fills 26.60% of
        // the slots at the pinned seed (25.30%..27.11% over 400 sweeps of two
        // seed families -- Rnd(0x5eed + i, 0xdec4 + i) and seed pairs drawn
        // from Rnd(1, 2)), the table-aware draw 49.22% (47.62%..50.33% over
        // 1_000 sweeps of the same families). The floor sits at 40: it leaves
        // 1.19x of headroom under the leanest table-aware sweep and still fails
        // a return to the blind draw by 1.47x.
        //
        // The kind assertion guards the other half of the fix. Restoring the
        // fill rate DOWNSTREAM instead -- letting generateLeafOfKind hand back a
        // column of whichever identifier kind the table happens to carry --
        // reaches the same 49%, but then 22.6% of the draws come back as a
        // column of a kind other than the one asked for. That breaks the promise
        // generateOfKind makes to its caller, and
        // PredicateGenerator.appendComparison rests on it: it draws both
        // operands of a comparison from one kind, and the three identifier types
        // are mutually incomparable. Today every comparison kind is anchored on
        // a real column, so that variant emits no cross-type comparison as the
        // generators stand; hand it a kind drawn blind, as this slot's picker
        // does, and 23_770 of 107_628 comparisons come out cross-type --
        // c_uuid < '128.0.0.0', the exact noise the split removed.
        int[] counts = countIdentifierKeySlotFills(new Rnd(0x5eed, 0xdec4));
        Assert.assertEquals(
                "the key slot produced " + counts[2] + " expressions of an identifier kind other than the one drawn",
                0, counts[2]
        );
        Assert.assertTrue(
                "identifier key slots landed on a real column " + counts[1] + " times out of " + counts[0]
                        + ", want at least " + MIN_IDENTIFIER_KEY_SLOT_FILL_PCT + "%",
                100L * counts[1] >= (long) MIN_IDENTIFIER_KEY_SLOT_FILL_PCT * counts[0]
        );
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
        //
        // Hand-building the fixture pins what PredicateGenerator can spell, not
        // what QueryFuzzTest's corpus contains.
        // testFactoryDealsSameTypeColumnPairs pins the second half: that
        // FuzzTableFactory really deals a table carrying a same-type pair.
        columns.add(new FuzzColumn("c_ip", IPv4Type.INSTANCE));
        columns.add(new FuzzColumn("c_ip2", IPv4Type.INSTANCE));
        columns.add(new FuzzColumn("c_uuid", UuidType.INSTANCE));
        columns.add(new FuzzColumn("c_uuid2", UuidType.INSTANCE));
        columns.add(new FuzzColumn("c_l256", Long256Type.INSTANCE));
        columns.add(new FuzzColumn("c_int", IntType.INSTANCE));
        columns.add(new FuzzColumn("ts", TimestampType.INSTANCE));
        return columns;
    }

    /**
     * Drives {@link #IDENTIFIER_KEY_SLOT_DRAWS_PER_TABLE} identifier key-slot
     * draws over every table of a {@link #FACTORY_SWEEP_RUNS} sweep, the way
     * {@code GroupByClause} and {@code SampleByClause} fill their key slots:
     * {@link ExpressionGenerator#pickIdentifierKind} settles the kind, then
     * {@link ExpressionGenerator#generateOfKind} builds the expression.
     * <p>
     * Returns {@code {draws, draws that produced a column ref, draws whose
     * expression kind differs from the kind drawn}}. The third counter is the
     * type-coherence check: {@code generateOfKind(kind)} owes its caller an
     * expression OF that kind, whether the table carries a column of it or not.
     */
    private static int[] countIdentifierKeySlotFills(Rnd rnd) throws SqlException {
        int[] counts = new int[3];
        for (int run = 0; run < FACTORY_SWEEP_RUNS; run++) {
            FuzzConfig config = new FuzzConfig(rnd);
            FuzzTableFactory factory = new FuzzTableFactory(config);
            for (int t = 0; t < config.getNumTables(); t++) {
                ObjList<FuzzColumn> dealt = factory.create(rnd, "fuzz_t" + t, sql -> {
                }, () -> {
                }).getColumns();
                ExpressionGenerator gen = new ExpressionGenerator(rnd, dealt, null, 2);
                for (int i = 0; i < IDENTIFIER_KEY_SLOT_DRAWS_PER_TABLE; i++) {
                    ColumnKind kind = gen.pickIdentifierKind();
                    FuzzExpr key = gen.generateOfKind(kind);
                    counts[0]++;
                    if (key instanceof ColumnRefExpr) {
                        counts[1]++;
                    }
                    if (key.getKind() != kind) {
                        counts[2]++;
                    }
                }
            }
        }
        return counts;
    }

    /**
     * Runs {@link #FACTORY_SWEEP_RUNS} simulated fuzz runs through
     * {@link FuzzTableFactory} and returns, per entry of {@code types}, how
     * many of the tables they built carry two or more columns of that type.
     * <p>
     * One sweep answers for every type at once. The counts come off the same
     * corpus, so a second type costs the caller no extra runs -- asking twice
     * with the same seed would rebuild the identical corpus for nothing.
     * <p>
     * Each iteration mirrors {@code QueryFuzzTest.runFuzz}: one
     * {@link FuzzConfig} and one factory per run -- the factory holds the run's
     * type deck, so sharing it across the run's tables is what lets a later
     * table see a spent deck -- and {@code config.getNumTables()} tables dealt
     * off it. The SQL executor and the WAL drain are no-ops, so the sweep
     * builds column lists and DDL strings and touches no storage.
     */
    private static int[] countSameTypePairTables(Rnd rnd, ObjList<FuzzColumnType> types) throws SqlException {
        int[] pairs = new int[types.size()];
        for (int run = 0; run < FACTORY_SWEEP_RUNS; run++) {
            FuzzConfig config = new FuzzConfig(rnd);
            FuzzTableFactory factory = new FuzzTableFactory(config);
            for (int t = 0; t < config.getNumTables(); t++) {
                ObjList<FuzzColumn> dealt = factory.create(rnd, "fuzz_t" + t, sql -> {
                }, () -> {
                }).getColumns();
                for (int k = 0, m = types.size(); k < m; k++) {
                    FuzzColumnType type = types.getQuick(k);
                    int matches = 0;
                    for (int i = 0, n = dealt.size(); i < n; i++) {
                        if (dealt.getQuick(i).getType() == type) {
                            matches++;
                        }
                    }
                    if (matches > 1) {
                        pairs[k]++;
                    }
                }
            }
        }
        return pairs;
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
     * A column list carrying exactly ONE identifier type -- an IPv4 column, and
     * no UUID or LONG256 column. {@link #testIdentifierKeySlotDrawFollowsTheTable}
     * needs exactly one so that "the identifier kind this table carries" is a
     * single value and the pin can assert an equality rather than a rate.
     * <p>
     * The rest of the list only has to give the non-identifier options of the
     * two key-kind pickers something to land on; nothing about it is load
     * bearing beyond carrying no second identifier type.
     */
    private static ObjList<FuzzColumn> oneIdentifierTypeColumns() {
        ObjList<FuzzColumn> columns = new ObjList<>();
        columns.add(new FuzzColumn("sym", SymbolType.INSTANCE));
        columns.add(new FuzzColumn("c_char", CharType.INSTANCE));
        columns.add(new FuzzColumn("c_bool", BooleanType.INSTANCE));
        columns.add(new FuzzColumn("c_ip", IPv4Type.INSTANCE));
        columns.add(new FuzzColumn("c_int", IntType.INSTANCE));
        columns.add(new FuzzColumn("ts", TimestampType.INSTANCE));
        return columns;
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
