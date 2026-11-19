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

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pins the query shapes {@link EquiJoinClause} has to keep producing for QueryFuzzTest, whose
 * parallel join axis compares each of them with the parallel hash join on and off. QueryFuzzTest
 * only proves the queries it happens to draw, so a generator that stops emitting LEFT joins, ON
 * residuals, post-join filters, backward probes or negative limits would leave the operator's
 * paths for them unchecked while the run stays green. Also pins the band that
 * {@link QueryGenerator} carves for the shape out of the SIMPLE range: with the knob off, the
 * generator draws exactly what it drew before the band existed. And pins
 * {@link EquiJoinClause#unwrapLimitForm(String)} and {@link EquiJoinClause#closing(String, int)},
 * through which this test and {@link EquiJoinStarShapeCoverageTest} read a query of a LIMIT form
 * and the parentheses of any query.
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
            // The query that a LIMIT form wraps holds the order that its LIMIT needs, and its WHERE
            // clause, if any, the query's filters: the WHERE that holds no column, which a LIMIT
            // form adds, goes with the form.
            final String sql = EquiJoinClause.unwrapLimitForm(EquiJoinClause.generate(rnd, left, right, null, false).sql());
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
            // The LIMIT forms draw from a generator of their own, which the seeds above do not
            // show, and join() reads the query that a form wraps: both forms of the query take the
            // same LIMIT form, with the same ASOF or LT join, keyed and under a TOLERANCE alike.
            Assert.assertEquals(literal.sql(), EquiJoinClause.limitForm(literal.sql()), EquiJoinClause.limitForm(bind.sql()));
            Assert.assertEquals(literal.sql(), EquiJoinClause.limitFormText(literal.sql()), EquiJoinClause.limitFormText(bind.sql()));
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
            // A LIMIT form keeps the query's own projection list inside it.
            if (!hasLimit || EquiJoinClause.unwrapLimitForm(literal.sql()).startsWith("SELECT * ")) {
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
    public void testClosingFindsTheMatchingParenthesis() {
        // EquiJoinClause.closing() returns the index of the parenthesis that closes the one at the
        // given index: of a pair alone, and of the outer and the inner pairs of nested ones.
        Assert.assertEquals(1, EquiJoinClause.closing("()", 0));
        final String nested = "a (b (c) (d)) e";
        Assert.assertEquals(12, EquiJoinClause.closing(nested, 2));
        Assert.assertEquals(7, EquiJoinClause.closing(nested, 5));
        Assert.assertEquals(11, EquiJoinClause.closing(nested, 9));
        // A parenthesis in a quoted literal neither opens nor closes: the last one closes the first.
        final String quoted = "(l.c2 != ')' AND r.c2 = '((')";
        Assert.assertEquals(quoted.length() - 1, EquiJoinClause.closing(quoted, 0));
        // No parenthesis opens at the given index, and one that opens never closes.
        Assert.assertEquals(
                "no opening parenthesis at 1: (l.k)",
                Assert.assertThrows(AssertionError.class, () -> EquiJoinClause.closing("(l.k)", 1)).getMessage()
        );
        Assert.assertEquals(
                "unbalanced parentheses: ((l.k)",
                Assert.assertThrows(AssertionError.class, () -> EquiJoinClause.closing("((l.k)", 0)).getMessage()
        );
        Assert.assertEquals(
                "unbalanced parentheses: (l.c2 = ')'",
                Assert.assertThrows(AssertionError.class, () -> EquiJoinClause.closing("(l.c2 = ')'", 0)).getMessage()
        );
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
        // The LIMIT forms, whose LIMIT reaches the joins from outside their own query, or through an
        // operator over them: a sub-query, a CTE, a UNION ALL, an ASOF join, keyed and not, under a
        // TOLERANCE, an LT join and a WHERE that holds no column.
        Assert.assertTrue(
                "the generator produced no query with a LIMIT over a sub-query in " + CORPUS_SIZE + " queries",
                Arrays.stream(corpus.split("\n")).anyMatch(sql -> EquiJoinClause.limitForm(sql) == EquiJoinClause.LIMIT_FORM_SUB_QUERY)
        );
        assertCorpusMatches(corpus, "a LIMIT over a CTE", Pattern.compile("^WITH q AS \\(SELECT [^\\n]* JOIN [^\\n]*\\) SELECT \\* FROM q LIMIT (?:\\d+, )?-?\\d+$", Pattern.MULTILINE));
        assertCorpusMatches(corpus, "a LIMIT over a UNION ALL", Pattern.compile(" UNION ALL SELECT [^\\n]* JOIN [^\\n]* LIMIT (?:\\d+, )?-?\\d+$", Pattern.MULTILINE));
        assertCorpusMatches(corpus, "a keyed ASOF join over the joins", Pattern.compile(" JOIN [^\\n]* ASOF JOIN t[01] a ON a\\.sym = l\\.sym(?:_v)? [^\\n]*LIMIT"));
        assertCorpusMatches(corpus, "an ASOF join without a key over the joins", Pattern.compile(" JOIN [^\\n]* ASOF JOIN t[01] a (?:TOLERANCE 1h )?(?:WHERE|ORDER BY|LIMIT) "));
        assertCorpusMatches(corpus, "an ASOF join under a TOLERANCE", Pattern.compile(" ASOF JOIN t[01] a [^\\n]*TOLERANCE 1h [^\\n]*LIMIT"));
        assertCorpusMatches(corpus, "an LT join over the joins", Pattern.compile(" JOIN [^\\n]* LT JOIN t[01] a [^\\n]*LIMIT"));
        assertCorpusMatches(corpus, "a WHERE that holds no column", Pattern.compile(" WHERE now\\(\\) > '2000-01-01' [^\\n]*LIMIT"));
        // The same forms without a LIMIT, through which the rows that a client reads first reach the
        // joins as a LIMIT does.
        final String[] forms = {"a sub-query", "a CTE", "a UNION ALL", "an ASOF join", "an LT join", "a WHERE that holds no column"};
        final int[] formIds = {
                EquiJoinClause.LIMIT_FORM_SUB_QUERY,
                EquiJoinClause.LIMIT_FORM_CTE,
                EquiJoinClause.LIMIT_FORM_UNION_ALL,
                EquiJoinClause.LIMIT_FORM_AS_OF,
                EquiJoinClause.LIMIT_FORM_LT,
                EquiJoinClause.LIMIT_FORM_RUNTIME_CONSTANT_FILTER,
        };
        for (int i = 0; i < forms.length; i++) {
            final int form = formIds[i];
            Assert.assertTrue(
                    "the generator produced no query in " + forms[i] + " without a LIMIT in " + CORPUS_SIZE + " queries",
                    Arrays.stream(corpus.split("\n")).anyMatch(sql -> EquiJoinClause.limitForm(sql) == form && !EquiJoinClause.hasLimit(sql))
            );
        }
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
                residuals[join + sides(sql.substring(residual + 5, EquiJoinClause.closing(sql, residual + 5) + 1))]++;
            }
            if (where > 0) {
                for (int open = where + 7; open < sql.length() && sql.charAt(open) == '('; ) {
                    final int close = EquiJoinClause.closing(sql, open);
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

    @Test
    public void testUnwrapLimitFormInvertsEveryForm() {
        // EquiJoinClause.unwrapLimitForm() takes the query back out of each LIMIT form that
        // applyLimitForm() writes, with its LIMIT as it was, literal or bound, or without one, and
        // limitFormText() returns the text that the form added. No form: no LIMIT, a LIMIT, and a
        // query whose left input is a sub-query, which starts as the sub-query form does.
        assertUnwrapsLimitForm(
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k",
                EquiJoinClause.LIMIT_FORM_NONE,
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k",
                ""
        );
        assertUnwrapsLimitForm(
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k LIMIT 5",
                EquiJoinClause.LIMIT_FORM_NONE,
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k LIMIT 5",
                ""
        );
        assertUnwrapsLimitForm(
                "SELECT * FROM (SELECT * FROM t0 LIMIT 43) l JOIN t1 r ON l.k = r.k AND (l.c0 > r.c0) LIMIT 5",
                EquiJoinClause.LIMIT_FORM_NONE,
                "SELECT * FROM (SELECT * FROM t0 LIMIT 43) l JOIN t1 r ON l.k = r.k AND (l.c0 > r.c0) LIMIT 5",
                ""
        );
        // A sub-query and a CTE over the query.
        assertUnwrapsLimitForm(
                "SELECT * FROM (SELECT * FROM (SELECT * FROM t0 LIMIT 43) l JOIN t1 r ON l.k = r.k ORDER BY l.ts) LIMIT 2, -5",
                EquiJoinClause.LIMIT_FORM_SUB_QUERY,
                "SELECT * FROM (SELECT * FROM t0 LIMIT 43) l JOIN t1 r ON l.k = r.k ORDER BY l.ts LIMIT 2, -5",
                "SELECT * FROM ()"
        );
        // A parenthesis in a literal leaves the sub-query open; one that never closes fails.
        assertUnwrapsLimitForm(
                "SELECT * FROM (SELECT * FROM t0 l JOIN t1 r ON l.k = r.k WHERE (l.c2 != ')')) LIMIT 5",
                EquiJoinClause.LIMIT_FORM_SUB_QUERY,
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k WHERE (l.c2 != ')') LIMIT 5",
                "SELECT * FROM ()"
        );
        // The forms without a LIMIT: the sub-query closes at the query's end, which a query whose left
        // input is a sub-query does not.
        assertUnwrapsLimitForm(
                "SELECT * FROM (SELECT * FROM t0 l JOIN t1 r ON l.k = r.k WHERE (l.c2 != ')'))",
                EquiJoinClause.LIMIT_FORM_SUB_QUERY,
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k WHERE (l.c2 != ')')",
                "SELECT * FROM ()"
        );
        assertUnwrapsLimitForm(
                "SELECT * FROM (SELECT * FROM t0 LIMIT 43) l JOIN t1 r ON l.k = r.k",
                EquiJoinClause.LIMIT_FORM_NONE,
                "SELECT * FROM (SELECT * FROM t0 LIMIT 43) l JOIN t1 r ON l.k = r.k",
                ""
        );
        assertUnwrapsLimitForm(
                "WITH q AS (SELECT * FROM t0 l JOIN t1 r ON l.k = r.k ORDER BY l.ts) SELECT * FROM q",
                EquiJoinClause.LIMIT_FORM_CTE,
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k ORDER BY l.ts",
                "WITH q AS () SELECT * FROM q"
        );
        assertUnwrapsLimitForm(
                "SELECT * FROM t0 l LEFT JOIN t1 r ON l.k = r.k UNION ALL SELECT * FROM t0 l LEFT JOIN t1 r ON l.k = r.k",
                EquiJoinClause.LIMIT_FORM_UNION_ALL,
                "SELECT * FROM t0 l LEFT JOIN t1 r ON l.k = r.k",
                " UNION ALL "
        );
        assertUnwrapsLimitForm(
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k ASOF JOIN t1 a ON a.sym = l.sym",
                EquiJoinClause.LIMIT_FORM_AS_OF,
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k",
                " ASOF JOIN t1 a ON a.sym = l.sym"
        );
        assertUnwrapsLimitForm(
                "SELECT * FROM t0 l LEFT JOIN t1 r ON l.k = r.k LT JOIN t1 a WHERE (r.k IS NULL)",
                EquiJoinClause.LIMIT_FORM_LT,
                "SELECT * FROM t0 l LEFT JOIN t1 r ON l.k = r.k WHERE (r.k IS NULL)",
                " LT JOIN t1 a"
        );
        assertUnwrapsLimitForm(
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k WHERE now() > '2000-01-01' ORDER BY l.ts",
                EquiJoinClause.LIMIT_FORM_RUNTIME_CONSTANT_FILTER,
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k ORDER BY l.ts",
                " WHERE now() > '2000-01-01'"
        );
        assertRejectsLimitForm(
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k UNION ALL SELECT * FROM t0 l JOIN t1 r ON l.k = r.lk",
                EquiJoinClause.LIMIT_FORM_UNION_ALL,
                "a UNION ALL form of two different queries: "
        );
        assertRejectsLimitForm(
                "WITH q AS (SELECT * FROM t0 l JOIN t1 r ON l.k = r.k) SELECT * FROM q WHERE (k > 1)",
                EquiJoinClause.LIMIT_FORM_CTE,
                "a CTE form whose LIMIT does not follow the CTE: "
        );
        final String unclosed = "SELECT * FROM (SELECT * FROM t0 l JOIN t1 r ON l.k = r.k LIMIT 5";
        Assert.assertEquals(
                "unbalanced parentheses: " + unclosed,
                Assert.assertThrows(AssertionError.class, () -> EquiJoinClause.limitForm(unclosed)).getMessage()
        );
        assertUnwrapsLimitForm(
                "WITH q AS (SELECT l.k AS c0, l.k + $1 AS c1 FROM t0 l JOIN t1 r ON l.k = r.k ORDER BY l.ts) SELECT * FROM q LIMIT $1",
                EquiJoinClause.LIMIT_FORM_CTE,
                "SELECT l.k AS c0, l.k + $1 AS c1 FROM t0 l JOIN t1 r ON l.k = r.k ORDER BY l.ts LIMIT $1",
                "WITH q AS () SELECT * FROM q"
        );
        // A UNION ALL of the query with itself, whose text leaves out the second branch.
        assertUnwrapsLimitForm(
                "SELECT * FROM t0 l LEFT JOIN t1 r ON l.k = r.k UNION ALL SELECT * FROM t0 l LEFT JOIN t1 r ON l.k = r.k LIMIT -5",
                EquiJoinClause.LIMIT_FORM_UNION_ALL,
                "SELECT * FROM t0 l LEFT JOIN t1 r ON l.k = r.k LIMIT -5",
                " UNION ALL "
        );
        // ASOF joins under a TOLERANCE, keyed on the left input's renamed SYMBOL key and not, before
        // a WHERE and before the LIMIT.
        assertUnwrapsLimitForm(
                "SELECT * FROM (SELECT sym AS sym_v, k, ts FROM t0) l JOIN t1 r ON l.k = r.k ASOF JOIN t2 a ON a.sym = l.sym_v TOLERANCE 1h WHERE (r.c0 > 1) ORDER BY l.ts LIMIT 5",
                EquiJoinClause.LIMIT_FORM_AS_OF,
                "SELECT * FROM (SELECT sym AS sym_v, k, ts FROM t0) l JOIN t1 r ON l.k = r.k WHERE (r.c0 > 1) ORDER BY l.ts LIMIT 5",
                " ASOF JOIN t2 a ON a.sym = l.sym_v TOLERANCE 1h"
        );
        assertUnwrapsLimitForm(
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k ASOF JOIN t1 a TOLERANCE 1h LIMIT 7, $1",
                EquiJoinClause.LIMIT_FORM_AS_OF,
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k LIMIT 7, $1",
                " ASOF JOIN t1 a TOLERANCE 1h"
        );
        // LT joins, keyed before an ORDER BY and not before a WHERE.
        assertUnwrapsLimitForm(
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k LT JOIN t1 a ON a.sym = l.sym ORDER BY l.ts LIMIT 5",
                EquiJoinClause.LIMIT_FORM_LT,
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k ORDER BY l.ts LIMIT 5",
                " LT JOIN t1 a ON a.sym = l.sym"
        );
        assertUnwrapsLimitForm(
                "SELECT * FROM t0 l LEFT JOIN t1 r ON l.k = r.k LT JOIN t1 a WHERE (r.k IS NULL) LIMIT -$1",
                EquiJoinClause.LIMIT_FORM_LT,
                "SELECT * FROM t0 l LEFT JOIN t1 r ON l.k = r.k WHERE (r.k IS NULL) LIMIT -$1",
                " LT JOIN t1 a"
        );
        // A WHERE that holds no column.
        assertUnwrapsLimitForm(
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k WHERE now() > '2000-01-01' ORDER BY l.ts DESC LIMIT 5",
                EquiJoinClause.LIMIT_FORM_RUNTIME_CONSTANT_FILTER,
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k ORDER BY l.ts DESC LIMIT 5",
                " WHERE now() > '2000-01-01'"
        );
        // Forms that applyLimitForm() does not write: a UNION ALL of two different queries, an LT
        // join under a TOLERANCE, an ASOF join on another key, and a CTE that a WHERE follows.
        assertRejectsLimitForm(
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k UNION ALL SELECT * FROM t0 l JOIN t1 r ON l.k = r.lk LIMIT 5",
                EquiJoinClause.LIMIT_FORM_UNION_ALL,
                "a UNION ALL form of two different queries: "
        );
        assertRejectsLimitForm(
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k LT JOIN t1 a TOLERANCE 1h LIMIT 5",
                EquiJoinClause.LIMIT_FORM_LT,
                "an ASOF or LT join that no LIMIT form writes: "
        );
        assertRejectsLimitForm(
                "SELECT * FROM t0 l JOIN t1 r ON l.k = r.k ASOF JOIN t1 a ON a.k = l.k LIMIT 5",
                EquiJoinClause.LIMIT_FORM_AS_OF,
                "an ASOF or LT join that no LIMIT form writes: "
        );
        assertRejectsLimitForm(
                "WITH q AS (SELECT * FROM t0 l JOIN t1 r ON l.k = r.k) SELECT * FROM q WHERE (k > 1) LIMIT 5",
                EquiJoinClause.LIMIT_FORM_CTE,
                "a CTE form whose LIMIT does not follow the CTE: "
        );
    }

    private static void assertCorpusMatches(String corpus, String what, Pattern pattern) {
        Assert.assertTrue(
                "the generator produced no query with " + what + " in " + CORPUS_SIZE + " queries",
                pattern.matcher(corpus).find()
        );
    }

    // The query reads as the given LIMIT form, but not as applyLimitForm() writes it:
    // unwrapLimitForm() and limitFormText() fail with the given message and the query.
    private static void assertRejectsLimitForm(String sql, int form, String message) {
        Assert.assertEquals(sql, form, EquiJoinClause.limitForm(sql));
        final AssertionError unwrapError = Assert.assertThrows(sql, AssertionError.class, () -> EquiJoinClause.unwrapLimitForm(sql));
        Assert.assertEquals(message + sql, unwrapError.getMessage());
        final AssertionError textError = Assert.assertThrows(sql, AssertionError.class, () -> EquiJoinClause.limitFormText(sql));
        Assert.assertEquals(message + sql, textError.getMessage());
    }

    // The query is of the given LIMIT form, which wraps the given query and adds the given text, and
    // keeps the LIMIT of the query, if any, last.
    private static void assertUnwrapsLimitForm(String sql, int form, String query, String text) {
        Assert.assertEquals(sql, form, EquiJoinClause.limitForm(sql));
        Assert.assertEquals(sql, EquiJoinClause.hasLimit(query), EquiJoinClause.hasLimit(sql));
        Assert.assertEquals(sql, query, EquiJoinClause.unwrapLimitForm(sql));
        Assert.assertEquals(sql, text, EquiJoinClause.limitFormText(sql));
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
    // inside an input's filter leaves out of the comparison, of the query that a LIMIT form, if
    // any, wraps.
    private static String join(String sql) {
        final String query = EquiJoinClause.unwrapLimitForm(sql);
        final String join = query.substring(query.indexOf(" FROM "));
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
                && sides(sql.substring(residual + 5, EquiJoinClause.closing(sql, residual + 5) + 1)) == 0) {
            return true;
        }
        if (where > 0) {
            for (int open = where + 7; open < sql.length() && sql.charAt(open) == '('; ) {
                final int close = EquiJoinClause.closing(sql, open);
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
        final String input = sql.substring(open + 1, EquiJoinClause.closing(sql, open));
        return indexAtDepthZero(input, " WHERE ", 0) > 0;
    }

    // True when a query of the corpus joins, as its right input, a projection whose WHERE clause reads
    // the VARCHAR columns c2 or vk: a filtered input never swaps, so the right input builds.
    private static boolean hasBuildFilterOverVarchar(String corpus) {
        final Pattern varchar = Pattern.compile("\\b(?:c2|vk)\\b");
        for (String sql : corpus.split("\n")) {
            for (int at = sql.indexOf("JOIN (SELECT "); at > -1; at = sql.indexOf("JOIN (SELECT ", at + 1)) {
                final int open = at + 5;
                final int close = EquiJoinClause.closing(sql, open);
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
