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
import io.questdb.test.griffin.fuzz.clauses.GroupingSetsClause;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.griffin.fuzz.types.DoubleType;
import io.questdb.test.griffin.fuzz.types.IntType;
import io.questdb.test.griffin.fuzz.types.SymbolType;
import io.questdb.test.griffin.fuzz.types.TimestampType;
import io.questdb.test.griffin.fuzz.types.VarcharType;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 * Pins the query shapes {@link GroupingSetsClause} has to keep producing for QueryFuzzTest, and
 * the reference rewrite it pairs with each one. QueryFuzzTest only proves the queries it happens
 * to draw, so a generator that stops emitting CUBE, the empty grouping set or SAMPLE BY would
 * leave those paths untested while the run stays green.
 * <p>
 * The generator is random, so each assertion runs over a corpus; the seed is fixed so a failure
 * is reproducible.
 */
public class GroupingSetsShapeCoverageTest {
    private static final int CORPUS_SIZE = 2_000;

    @Test
    public void testBindFormDrawsTheSameRandomSequence() {
        // QueryFuzzTest regenerates a query with a BindContext from the same rnd state and needs
        // the same query shape back, so building the bind form must not draw anything extra.
        final FuzzSource source = source();
        final Rnd literalRnd = new Rnd(0x5eed, 0x9505);
        final Rnd bindRnd = new Rnd(0x5eed, 0x9505);
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final GeneratedQuery literal = GroupingSetsClause.generate(literalRnd, source, null, false);
            final GeneratedQuery bind = GroupingSetsClause.generate(bindRnd, source, new BindContext(new Rnd(i, i), 50), false);
            Assert.assertEquals("query " + i + ": " + literal.sql(), literalRnd.getSeed0(), bindRnd.getSeed0());
            Assert.assertEquals("query " + i + ": " + literal.sql(), literalRnd.getSeed1(), bindRnd.getSeed1());
            Assert.assertEquals(grouping(literal.sql()), grouping(bind.sql()));
        }
    }

    @Test
    public void testCorpusCoversEveryForm() {
        final String corpus = corpus();
        assertCorpusMatches(corpus, "ROLLUP", Pattern.compile("GROUP BY ROLLUP\\("));
        assertCorpusMatches(corpus, "CUBE", Pattern.compile("GROUP BY CUBE\\("));
        assertCorpusMatches(corpus, "explicit GROUPING SETS", Pattern.compile("GROUP BY GROUPING SETS \\("));
        assertCorpusMatches(corpus, "an empty grouping set", Pattern.compile("GROUPING SETS \\([^\\n]*\\(\\)"));
        assertCorpusMatches(corpus, "a plain key before ROLLUP or CUBE", Pattern.compile("GROUP BY [a-z0-9_.]+, (?:ROLLUP|CUBE)\\("));
        assertCorpusMatches(corpus, "SAMPLE BY with ROLLUP", Pattern.compile("SAMPLE BY \\w+ ROLLUP\\("));
        assertCorpusMatches(corpus, "SAMPLE BY with CUBE", Pattern.compile("SAMPLE BY \\w+ CUBE\\("));
        assertCorpusMatches(corpus, "SAMPLE BY with GROUPING SETS", Pattern.compile("SAMPLE BY \\w+ GROUPING SETS \\("));
        assertCorpusMatches(corpus, "GROUPING_ID over several keys", Pattern.compile("grouping_id\\([^)]*, "));
        assertCorpusMatches(corpus, "three keys", Pattern.compile("AS e2, "));
        assertCorpusMatches(corpus, "a WHERE clause", Pattern.compile(" WHERE "));
    }

    @Test
    public void testReferenceHasOneBranchPerGroupingSet() {
        // A rewrite that drops or repeats a branch would hide exactly the lost or duplicated
        // grouping sets the reference axis is there to catch.
        final Rnd rnd = new Rnd(0x5eed, 0xb4a2);
        final FuzzSource source = source();
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final GeneratedQuery query = GroupingSetsClause.generate(rnd, source, null, false);
            final String sql = query.sql();
            Assert.assertNotNull(sql, query.referenceSql());
            Assert.assertEquals(sql + "\n" + query.referenceSql(), groupingSetCount(sql), count(query.referenceSql(), "SELECT ") - count(query.referenceSql(), "(SELECT "));
        }
    }

    private static void assertCorpusMatches(String corpus, String what, Pattern pattern) {
        Assert.assertTrue(
                "the generator produced no query with " + what + " in " + CORPUS_SIZE + " queries",
                pattern.matcher(corpus).find()
        );
    }

    private static String corpus() {
        final Rnd rnd = new Rnd(0x5eed, 0xc0be);
        final FuzzSource source = source();
        final StringBuilder corpus = new StringBuilder();
        for (int i = 0; i < CORPUS_SIZE; i++) {
            corpus.append(GroupingSetsClause.generate(rnd, source, null, false).sql()).append('\n');
        }
        return corpus.toString();
    }

    private static int count(String s, String what) {
        int n = 0;
        for (int i = s.indexOf(what); i >= 0; i = s.indexOf(what, i + 1)) {
            n++;
        }
        return n;
    }

    private static String grouping(String sql) {
        final int at = Math.max(sql.lastIndexOf(" GROUP BY "), sql.lastIndexOf(" SAMPLE BY "));
        return sql.substring(at);
    }

    // The number of grouping sets the query names, counted the way SQL expands them.
    private static int groupingSetCount(String sql) {
        final String grouping = grouping(sql);
        final int open = grouping.indexOf('(');
        final int keys = count(grouping.substring(open), ",") + 1;
        if (grouping.contains("ROLLUP(")) {
            return keys + 1;
        }
        if (grouping.contains("CUBE(")) {
            return 1 << keys;
        }
        // GROUPING SETS ((a, b), (), ...): one set per inner parenthesis.
        return count(grouping, "(") - 1;
    }

    private static FuzzSource source() {
        final ObjList<FuzzColumn> columns = new ObjList<>();
        columns.add(new FuzzColumn("sym", SymbolType.INSTANCE));
        columns.add(new FuzzColumn("c_int", IntType.INSTANCE));
        columns.add(new FuzzColumn("c_vc", VarcharType.INSTANCE));
        columns.add(new FuzzColumn("c_dbl", DoubleType.INSTANCE));
        columns.add(new FuzzColumn("ts", TimestampType.INSTANCE));
        return FuzzSource.direct(new FuzzTable("t", columns, "ts"));
    }
}
