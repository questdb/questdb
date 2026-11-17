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

import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.griffin.fuzz.clauses.EquiJoinClause;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.griffin.fuzz.types.DoubleType;
import io.questdb.test.griffin.fuzz.types.IntKeyType;
import io.questdb.test.griffin.fuzz.types.IntType;
import io.questdb.test.griffin.fuzz.types.LongKeyType;
import io.questdb.test.griffin.fuzz.types.SymbolType;
import io.questdb.test.griffin.fuzz.types.TimestampType;
import io.questdb.test.griffin.fuzz.types.VarcharKeyType;
import io.questdb.test.griffin.fuzz.types.VarcharType;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pins the query shapes {@link EquiJoinClause} has to keep producing for QueryFuzzTest, whose
 * parallel join axis compares each of them with the parallel hash join on and off. QueryFuzzTest
 * only proves the queries it happens to draw, so a generator that stops emitting LEFT joins, ON
 * residuals, post-join filters, backward probes or negative limits would leave the operator's
 * paths for them unchecked while the run stays green. Also pins the band that
 * {@link QueryGenerator} carves for the shape out of the SIMPLE range: with the knob off, the
 * generator draws exactly what it drew before the band existed.
 * <p>
 * The generator is random, so each assertion runs over a corpus; the seed is fixed so a failure
 * is reproducible.
 */
public class EquiJoinShapeCoverageTest {
    private static final int CORPUS_SIZE = 4_000;
    private static final int GENERATOR_DRAWS = 20_000;

    @Test
    public void testBandIsCarvedFromSimpleWithoutExtraDraws() {
        final ObjList<FuzzTable> tables = new ObjList<>();
        tables.add(table("t0"));
        tables.add(table("t1"));
        final Rnd rnd = new Rnd(0x5eed, 0xe901);
        // The hash join GROUP BY shape reads the config's table sizes; its own Rnd leaves the
        // generator's draws as they are.
        final FuzzConfig config = new FuzzConfig(new Rnd());
        int equiJoins = 0;
        for (int i = 0; i < GENERATOR_DRAWS; i++) {
            final long s0 = rnd.nextLong();
            final long s1 = rnd.nextLong();
            final Rnd on = new Rnd(s0, s1);
            final Rnd off = new Rnd(s0, s1);
            final GeneratedQuery withBand = QueryGenerator.generate(on, tables, null, false, true, true, true, true, true, true, true, config);
            final GeneratedQuery withoutBand = QueryGenerator.generate(off, tables, null, false, true, true, true, true, true, true, false, config);
            Assert.assertNotEquals(QueryShape.EQUI_JOIN, withoutBand.shape());
            if (withBand.shape() == QueryShape.EQUI_JOIN) {
                equiJoins++;
                // The band comes out of the SIMPLE range, and the band's check draws nothing, so
                // the query the knob turns off is the one SIMPLE drew there before.
                Assert.assertEquals(withoutBand.sql(), QueryShape.SIMPLE, withoutBand.shape());
            } else {
                Assert.assertEquals(withBand.sql(), withoutBand.sql());
                Assert.assertEquals(withBand.shape(), withoutBand.shape());
                Assert.assertEquals(on.getSeed0(), off.getSeed0());
                Assert.assertEquals(on.getSeed1(), off.getSeed1());
            }
        }
        // Five picks in a hundred land in the band.
        Assert.assertTrue("equi-joins: " + equiJoins, equiJoins > GENERATOR_DRAWS * 4 / 100 && equiJoins < GENERATOR_DRAWS * 6 / 100);
        // A single table leaves the band to SIMPLE.
        final ObjList<FuzzTable> single = new ObjList<>();
        single.add(table("t0"));
        for (int i = 0; i < GENERATOR_DRAWS; i++) {
            Assert.assertNotEquals(QueryShape.EQUI_JOIN, QueryGenerator.generate(rnd, single, null, false, true, true, true, true, true, true, true, config).shape());
        }
    }

    @Test
    public void testBareLimitKeepsTheLeftInputUnfiltered() {
        // The optimiser pushes a LIMIT without ORDER BY into the left input's filter, below the
        // join, which drops the rows of a join that loses left rows (questdb/questdb#7982). The
        // fuzz axes see that wherever one form of a query keeps the filter and the other does not,
        // so the generator never pairs a bare LIMIT with a filter on the left input; it orders
        // such a query by the left input's timestamp instead.
        final Rnd rnd = new Rnd(0x5eed, 0x7982);
        final FuzzTable left = table("t0");
        final FuzzTable right = table("t1");
        int bareInner = 0;
        int bareLeft = 0;
        int bareWithOtherFilters = 0;
        int orderedOverFilteredLeft = 0;
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final String sql = EquiJoinClause.generate(rnd, left, right, null, false).sql();
            if (!Pattern.compile(" LIMIT (?:\\d+, )?-?\\d+$").matcher(sql).find()) {
                continue;
            }
            final boolean isLeftJoin = sql.contains(" l LEFT JOIN ");
            final boolean isLeftFiltered = isLeftProjectionFiltered(sql) || hasLeftOnlyCondition(sql, isLeftJoin);
            if (indexAtDepthZero(sql, " ORDER BY ", 0) > 0) {
                orderedOverFilteredLeft += isLeftFiltered ? 1 : 0;
                continue;
            }
            Assert.assertFalse("a bare LIMIT over a filtered left input: " + sql, isLeftFiltered);
            bareInner += isLeftJoin ? 0 : 1;
            bareLeft += isLeftJoin ? 1 : 0;
            bareWithOtherFilters += indexAtDepthZero(sql, " WHERE ", 0) > 0 ? 1 : 0;
        }
        // Bare limits still reach both join types, and the filters of the right input and of the
        // joined pairs; a filtered left input still meets a LIMIT under an order.
        Assert.assertTrue("bare LIMIT over INNER joins: " + bareInner, bareInner > 0);
        Assert.assertTrue("bare LIMIT over LEFT joins: " + bareLeft, bareLeft > 0);
        Assert.assertTrue("bare LIMIT with a WHERE clause: " + bareWithOtherFilters, bareWithOtherFilters > 0);
        Assert.assertTrue("ordered LIMIT over a filtered left input: " + orderedOverFilteredLeft, orderedOverFilteredLeft > 0);
    }

    @Test
    public void testBindFormDrawsTheSameRandomSequence() {
        // QueryFuzzTest regenerates a query with a BindContext from the same rnd state and needs
        // the same query shape back, so building the bind form must not draw anything extra.
        final FuzzTable left = table("t0");
        final FuzzTable right = table("t1");
        final Rnd literalRnd = new Rnd(0x5eed, 0x9506);
        final Rnd bindRnd = new Rnd(0x5eed, 0x9506);
        int bound = 0;
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final GeneratedQuery literal = EquiJoinClause.generate(literalRnd, left, right, null, false);
            final BindContext ctx = new BindContext(new Rnd(i, i), 50);
            final GeneratedQuery bind = EquiJoinClause.generate(bindRnd, left, right, ctx, false);
            Assert.assertEquals("query " + i + ": " + literal.sql(), literalRnd.getSeed0(), bindRnd.getSeed0());
            Assert.assertEquals("query " + i + ": " + literal.sql(), literalRnd.getSeed1(), bindRnd.getSeed1());
            Assert.assertEquals(literal.deterministic(), bind.deterministic());
            Assert.assertEquals(join(literal.sql()), join(bind.sql()));
            bound += ctx.getBindValues().size() > 0 ? 1 : 0;
        }
        Assert.assertTrue("no query bound a value", bound > 0);
    }

    @Test
    public void testBindFormSharesTheLimitWithTheSelectList() {
        // A query with a LIMIT and a projection list projects the left input's INT key plus the
        // LIMIT's value last, and its bind form reads both through the positional $1, which
        // QueryRunner leaves undefined at compile and binds to the value after it: the SELECT list
        // types $1 before the LIMIT reads it, on every plan. A query of SELECT * keeps its literal
        // LIMIT, and a query without a LIMIT binds none.
        final FuzzTable left = table("t0");
        final FuzzTable right = table("t1");
        final Rnd literalRnd = new Rnd(0x5eed, 0x9507);
        final Rnd bindRnd = new Rnd(0x5eed, 0x9507);
        final Pattern limit = Pattern.compile(" LIMIT ((?:\\d+, )?-?)(\\d+)$");
        int positive = 0;
        int negative = 0;
        int ranges = 0;
        int selectStar = 0;
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final GeneratedQuery literal = EquiJoinClause.generate(literalRnd, left, right, null, false);
            final BindContext ctx = new BindContext(new Rnd(i, i), 50);
            final GeneratedQuery bind = EquiJoinClause.generate(bindRnd, left, right, ctx, false);
            final Matcher matcher = limit.matcher(literal.sql());
            final boolean hasLimit = matcher.find();
            if (!hasLimit || literal.sql().startsWith("SELECT * ")) {
                selectStar += hasLimit ? 1 : 0;
                Assert.assertNull(bind.sql(), ctx.getLimitValue());
                Assert.assertFalse(bind.sql(), bind.sql().contains("$1"));
                continue;
            }
            // The range's lower bound, if any, and the sign.
            final String sign = matcher.group(1);
            final String value = matcher.group(2);
            Assert.assertTrue(literal.sql(), Pattern.compile(", l\\.k(?:_v)? \\+ " + value + " AS c\\d+ FROM ").matcher(literal.sql()).find());
            Assert.assertTrue(bind.sql(), Pattern.compile(", l\\.k(?:_v)? \\+ \\$1 AS c\\d+ FROM ").matcher(bind.sql()).find());
            Assert.assertTrue(bind.sql(), bind.sql().endsWith(" LIMIT " + sign + "$1"));
            Assert.assertEquals(bind.sql(), value, ctx.getLimitValue());
            positive += sign.endsWith("-") ? 0 : 1;
            negative += sign.endsWith("-") ? 1 : 0;
            ranges += sign.contains(", ") ? 1 : 0;
        }
        Assert.assertTrue("bound LIMITs: " + positive, positive > 0);
        Assert.assertTrue("bound negative LIMITs: " + negative, negative > 0);
        Assert.assertTrue("bound range LIMITs: " + ranges, ranges > 0);
        Assert.assertTrue("literal LIMITs under SELECT *: " + selectStar, selectStar > 0);
    }

    @Test
    public void testCorpusCoversEveryForm() {
        final String corpus = corpus();
        // The join types the operator runs, each over the four shared keys and their pairs.
        assertCorpusMatches(corpus, "an INNER join", Pattern.compile("[a-z0-9_)] l JOIN "));
        // An INNER join that runs no filter over its pairs, neither an ON residual nor a WHERE clause
        // past the join: its stage hands its chains on, whose lengths a count reads.
        assertCorpusMatches(
                corpus,
                "an INNER join without a pair filter",
                Pattern.compile("^SELECT [^\\n]*[a-z0-9_)] l JOIN [^\\n]* ON [lr]\\.\\w+ = [lr]\\.\\w+(?: AND [lr]\\.\\w+ = [lr]\\.\\w+)*(?: ORDER BY [^\\n()]*)?(?: LIMIT (?:\\d+, )?-?\\d+)?$", Pattern.MULTILINE)
        );
        assertCorpusMatches(corpus, "a LEFT join", Pattern.compile(" l LEFT JOIN "));
        assertCorpusMatches(corpus, "an INT key", Pattern.compile("ON (?:l\\.k = r\\.k|r\\.k = l\\.k)"));
        assertCorpusMatches(corpus, "a SYMBOL key", Pattern.compile("ON (?:l\\.sym = r\\.sym|r\\.sym = l\\.sym)"));
        assertCorpusMatches(corpus, "a LONG key", Pattern.compile("ON (?:l\\.lk = r\\.lk|r\\.lk = l\\.lk)"));
        assertCorpusMatches(corpus, "a VARCHAR key", Pattern.compile("ON (?:l\\.vk = r\\.vk|r\\.vk = l\\.vk)"));
        assertCorpusMatches(corpus, "a SYMBOL against a VARCHAR", Pattern.compile("ON (?:l\\.sym = r\\.vk|r\\.vk = l\\.sym)"));
        assertCorpusMatches(corpus, "an INT against a LONG", Pattern.compile("ON (?:l\\.k = r\\.lk|r\\.lk = l\\.k)"));
        assertCorpusMatches(corpus, "a TIMESTAMP key", Pattern.compile("ON (?:l\\.ts = r\\.ts|r\\.ts = l\\.ts)"));
        assertCorpusMatches(corpus, "a composite key", Pattern.compile("ON [lr]\\.\\w+ = [lr]\\.\\w+ AND [lr]\\.\\w+ = [lr]\\.\\w+"));
        // The inputs: projections with filters, backward scans and LIMIT barriers.
        assertCorpusMatches(corpus, "a filtered projection", Pattern.compile("\\(SELECT [^()]*\\w+_v[^()]* FROM t[01] WHERE "));
        // A build input whose filter reads a VARCHAR column, which the JIT does not compile: the
        // build runs it in rounds over a multi-frame input, whatever its rows.
        Assert.assertTrue(
                "the generator produced no build input filtered over a VARCHAR column in " + CORPUS_SIZE + " queries",
                hasBuildFilterOverVarchar(corpus)
        );
        assertCorpusMatches(corpus, "a backward left input", Pattern.compile("FROM \\(SELECT \\* FROM t[01] ORDER BY ts DESC\\) l "));
        assertCorpusMatches(corpus, "a LIMIT barrier", Pattern.compile("\\(SELECT \\* FROM t[01] LIMIT \\d+\\) [lr]"));
        // The projection, the order and the limits.
        assertCorpusMatches(corpus, "SELECT *", Pattern.compile("^SELECT \\* FROM ", Pattern.MULTILINE));
        assertCorpusMatches(corpus, "columns of both inputs", Pattern.compile("^SELECT [^\\n]*l\\.\\w+ AS c\\d[^\\n]*r\\.\\w+ AS c\\d", Pattern.MULTILINE));
        assertCorpusMatches(corpus, "a backward probe", Pattern.compile(" ORDER BY l\\.ts DESC"));
        assertCorpusMatches(corpus, "an ORDER BY over a projected column", Pattern.compile(" ORDER BY c\\d"));
        assertCorpusMatches(corpus, "a LIMIT", Pattern.compile(" LIMIT \\d+$", Pattern.MULTILINE));
        assertCorpusMatches(corpus, "a negative LIMIT", Pattern.compile(" LIMIT -\\d+$", Pattern.MULTILINE));
        assertCorpusMatches(corpus, "a range LIMIT", Pattern.compile(" LIMIT \\d+, \\d+$", Pattern.MULTILINE));
        assertCorpusMatches(corpus, "a range LIMIT counting from the end", Pattern.compile(" LIMIT \\d+, -\\d+$", Pattern.MULTILINE));
        assertCorpusMatches(corpus, "the fault function", Pattern.compile(" WHERE test_fault\\(\\)"));
    }

    @Test
    public void testCorpusCoversEveryPairFilter() {
        // The ON residuals and the WHERE conjuncts, by the inputs they read: what the operator runs
        // per pair on the workers, and what the optimiser hands to an input's filter instead.
        final Rnd rnd = new Rnd(0x5eed, 0xe7a2);
        final FuzzTable left = table("t0");
        final FuzzTable right = table("t1");
        final int[] residuals = new int[8];
        final int[] conjuncts = new int[8];
        int antiJoins = 0;
        int leftIntervals = 0;
        int rightIntervals = 0;
        int constants = 0;
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final String sql = EquiJoinClause.generate(rnd, left, right, null, false).sql();
            final int join = sql.contains(" l LEFT JOIN ") ? 4 : 0;
            final int on = sql.indexOf(" ON ");
            Assert.assertTrue(sql, on > 0);
            final int where = indexAtDepthZero(sql, " WHERE ", on);
            final int residual = indexAtDepthZero(sql, " AND (", on);
            if (residual > 0 && (where < 0 || residual < where)) {
                residuals[join + sides(sql.substring(residual + 5, closing(sql, residual + 5) + 1))]++;
            }
            if (where > 0) {
                for (int open = where + 7; open < sql.length() && sql.charAt(open) == '('; ) {
                    final int close = closing(sql, open);
                    final String conjunct = sql.substring(open, close + 1);
                    if (conjunct.equals("(r.k IS NULL)")) {
                        antiJoins += join > 0 ? 1 : 0;
                    } else if (conjunct.startsWith("(l.ts >= '2024-01-")) {
                        leftIntervals++;
                    } else if (conjunct.startsWith("(r.ts >= '2024-01-")) {
                        rightIntervals++;
                    } else if (conjunct.matches("\\((?:1 = 0|false|2 < 1|1 = 1)\\)")) {
                        constants++;
                    } else {
                        conjuncts[join + sides(conjunct)]++;
                    }
                    open = close + 1;
                    if (sql.startsWith(" AND ", open)) {
                        open += 5;
                    }
                }
            }
        }
        // [left only, right only, both, neither] for INNER joins, then for LEFT joins. A generated
        // predicate over one input may fold to a constant.
        for (int k = 0; k < 8; k++) {
            if (k % 4 < 3) {
                Assert.assertTrue("residuals of kind " + k + ": " + residuals[k], residuals[k] > 0);
                Assert.assertTrue("WHERE conjuncts of kind " + k + ": " + conjuncts[k], conjuncts[k] > 0);
            }
        }
        Assert.assertTrue(antiJoins > 0);
        Assert.assertTrue(leftIntervals > 0);
        Assert.assertTrue(rightIntervals > 0);
        Assert.assertTrue(constants > 0);
    }

    @Test
    public void testFaultFunctionReachesEverySite() {
        // A fault query carries test_fault() at the head of its WHERE clause, in a join's ON
        // residual, which a LEFT join runs for each pair on the workers, or in the WHERE clause of a
        // join's input, which the build runs for each of its rows; a query without a fault carries
        // none.
        final Rnd rnd = new Rnd(0x5eed, 0xfa17);
        final FuzzTable left = table("t0");
        final FuzzTable right = table("t1");
        final ObjList<FuzzTable> tables = new ObjList<>();
        tables.add(left);
        tables.add(right);
        int where = 0;
        int residual = 0;
        int leftJoinResidual = 0;
        int build = 0;
        int laterBuild = 0;
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final boolean isFault = i % 2 == 0;
            final String sql = EquiJoinClause.generate(rnd, left, right, i % 4 < 2 ? null : tables, null, isFault).sql();
            final int calls = sql.split("test_fault\\(\\)", -1).length - 1;
            Assert.assertEquals(sql, isFault ? 1 : 0, calls);
            if (!isFault) {
                continue;
            }
            if (indexAtDepthZero(sql, " WHERE test_fault()", 0) > 0) {
                where++;
            } else if (sql.contains(" AND (test_fault() OR ")) {
                residual++;
                final int at = sql.indexOf(" AND (test_fault() OR ");
                final int join = sql.lastIndexOf(" JOIN ", at);
                leftJoinResidual += sql.startsWith(" LEFT", join - 5) ? 1 : 0;
            } else {
                final int at = sql.indexOf(" WHERE test_fault())");
                Assert.assertTrue(sql, at > 0);
                final int input = sql.lastIndexOf("JOIN (SELECT * FROM ", at);
                Assert.assertTrue(sql, input > 0);
                build++;
                // An input of a later join of a star.
                laterBuild += sql.indexOf(" JOIN ") < sql.lastIndexOf(" JOIN ", input - 1) ? 1 : 0;
            }
        }
        final String counts = "where " + where + ", residual " + residual + " (left " + leftJoinResidual + "), build " + build + " (later " + laterBuild + ")";
        Assert.assertTrue(counts, where > CORPUS_SIZE / 2 / 5);
        Assert.assertTrue(counts, residual > CORPUS_SIZE / 2 / 5);
        Assert.assertTrue(counts, leftJoinResidual > 0);
        Assert.assertTrue(counts, build > CORPUS_SIZE / 2 / 5);
        Assert.assertTrue(counts, laterBuild > 0);
    }

    @Test
    public void testLimitMakesQueryNonDeterministicForMultisetAxes() {
        // A LIMIT without a total order may keep other rows on the storage shadow, which scans an
        // indexed input in another order, so only the parallel join axis compares its rows.
        final FuzzTable left = table("t0");
        final FuzzTable right = table("t1");
        final Rnd rnd = new Rnd(0x5eed, 0x1e47);
        int limits = 0;
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final GeneratedQuery query = EquiJoinClause.generate(rnd, left, right, null, false);
            final boolean hasLimit = Pattern.compile(" LIMIT (?:\\d+, )?-?\\d+$").matcher(query.sql()).find();
            Assert.assertEquals(query.sql(), !hasLimit, query.deterministic());
            limits += hasLimit ? 1 : 0;
        }
        Assert.assertTrue(limits > 0);
    }

    private static void assertCorpusMatches(String corpus, String what, Pattern pattern) {
        Assert.assertTrue(
                "the generator produced no query with " + what + " in " + CORPUS_SIZE + " queries",
                pattern.matcher(corpus).find()
        );
    }

    // The index of the parenthesis that closes the one at the given index.
    private static int closing(String sql, int open) {
        Assert.assertEquals(sql, '(', sql.charAt(open));
        int depth = 0;
        for (int i = open; i < sql.length(); i++) {
            final char c = sql.charAt(i);
            if (c == '\'') {
                i = sql.indexOf('\'', i + 1);
            } else if (c == '(') {
                depth++;
            } else if (c == ')' && --depth == 0) {
                return i;
            }
        }
        throw new AssertionError("unbalanced parentheses: " + sql);
    }

    private static String corpus() {
        final Rnd rnd = new Rnd(0x5eed, 0xe7a1);
        final FuzzTable left = table("t0");
        final FuzzTable right = table("t1");
        final StringBuilder corpus = new StringBuilder();
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final GeneratedQuery query = EquiJoinClause.generate(rnd, left, right, null, i % 50 == 0);
            corpus.append(query.sql()).append('\n');
        }
        return corpus.toString();
    }

    // The query from its FROM to its ON condition: the inputs and the keys, which a bind variable
    // inside an input's filter leaves out of the comparison.
    private static String join(String sql) {
        final String join = sql.substring(sql.indexOf(" FROM "));
        return join.replaceAll("\\(SELECT [^()]* WHERE [^\\n]*?\\) ([lr]) ", "(input) $1 ")
                .replaceAll(" AND \\(.*", "")
                .replaceAll(" WHERE .*", "")
                .replaceAll(" ORDER BY .*", "")
                .replaceAll(" LIMIT .*", "");
    }

    // True when the query's ON residual (INNER joins only: their WHERE clause hands it to the left
    // input's filter) or one of its WHERE conjuncts reads the left input alone and is no interval
    // over the left input's timestamp, which the scan runs instead of a filter.
    private static boolean hasLeftOnlyCondition(String sql, boolean isLeftJoin) {
        final int on = sql.indexOf(" ON ");
        Assert.assertTrue(sql, on > 0);
        final int where = indexAtDepthZero(sql, " WHERE ", on);
        final int residual = indexAtDepthZero(sql, " AND (", on);
        if (!isLeftJoin && residual > 0 && (where < 0 || residual < where)
                && sides(sql.substring(residual + 5, closing(sql, residual + 5) + 1)) == 0) {
            return true;
        }
        if (where > 0) {
            for (int open = where + 7; open < sql.length() && sql.charAt(open) == '('; ) {
                final int close = closing(sql, open);
                final String conjunct = sql.substring(open, close + 1);
                if (!conjunct.startsWith("(l.ts >= '2024-01-") && sides(conjunct) == 0) {
                    return true;
                }
                open = close + 1;
                if (sql.startsWith(" AND ", open)) {
                    open += 5;
                }
            }
        }
        return false;
    }

    // The first index of the text at or after the given index outside any parentheses and quotes.
    private static int indexAtDepthZero(String sql, String text, int from) {
        int depth = 0;
        for (int i = from; i < sql.length(); i++) {
            final char c = sql.charAt(i);
            if (c == '\'') {
                i = sql.indexOf('\'', i + 1);
            } else if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (depth == 0 && sql.startsWith(text, i)) {
                return i;
            }
        }
        return -1;
    }

    // True when the left input is a projection with a WHERE clause of its own.
    private static boolean isLeftProjectionFiltered(String sql) {
        final int from = indexAtDepthZero(sql, " FROM ", 0);
        Assert.assertTrue(sql, from > 0);
        final int open = from + 6;
        if (sql.charAt(open) != '(') {
            return false;
        }
        final String input = sql.substring(open + 1, closing(sql, open));
        return indexAtDepthZero(input, " WHERE ", 0) > 0;
    }

    // True when a query of the corpus joins, as its right input, a projection whose WHERE clause reads
    // the VARCHAR columns c2 or vk: a filtered input never swaps, so the right input builds.
    private static boolean hasBuildFilterOverVarchar(String corpus) {
        final Pattern varchar = Pattern.compile("\\b(?:c2|vk)\\b");
        for (String sql : corpus.split("\n")) {
            for (int at = sql.indexOf("JOIN (SELECT "); at > -1; at = sql.indexOf("JOIN (SELECT ", at + 1)) {
                final int open = at + 5;
                final int close = closing(sql, open);
                final String input = sql.substring(open + 1, close);
                final int where = indexAtDepthZero(input, " WHERE ", 0);
                if (where > 0 && sql.startsWith(") r ON ", close) && varchar.matcher(input.substring(where)).find()) {
                    return true;
                }
            }
        }
        return false;
    }

    // 0 for a condition over the left input alone, 1 for the right input alone, 2 for both, 3 for
    // neither.
    private static int sides(String condition) {
        final boolean isLeft = Pattern.compile("\\bl\\.").matcher(condition).find();
        final boolean isRight = Pattern.compile("\\br\\.").matcher(condition).find();
        return isLeft ? (isRight ? 2 : 0) : (isRight ? 1 : 3);
    }

    private static FuzzTable table(String name) {
        final ObjList<FuzzColumn> columns = new ObjList<>();
        columns.add(new FuzzColumn("sym", SymbolType.INSTANCE));
        columns.add(new FuzzColumn("k", IntKeyType.INSTANCE));
        columns.add(new FuzzColumn("lk", LongKeyType.INSTANCE));
        columns.add(new FuzzColumn("vk", VarcharKeyType.INSTANCE));
        columns.add(new FuzzColumn("c0", IntType.INSTANCE));
        columns.add(new FuzzColumn("c1", DoubleType.INSTANCE));
        columns.add(new FuzzColumn("c2", VarcharType.INSTANCE));
        columns.add(new FuzzColumn("ts", TimestampType.INSTANCE));
        return new FuzzTable(name, columns, "ts");
    }
}
