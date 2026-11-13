/*******************************************************************************
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
import io.questdb.test.griffin.fuzz.clauses.HorizonJoinClause;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.griffin.fuzz.types.DoubleType;
import io.questdb.test.griffin.fuzz.types.IntType;
import io.questdb.test.griffin.fuzz.types.SymbolType;
import io.questdb.test.griffin.fuzz.types.TimestampType;
import io.questdb.test.griffin.fuzz.types.VarcharType;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pins the ON clauses {@link HorizonJoinClause} has to keep producing for QueryFuzzTest, and the
 * reference query it pairs with a key it repeats. A HORIZON JOIN key that names one column twice,
 * in another letter case or with the sides swapped, reaches code that a single key never does: the
 * optimiser merges the key pairs, or leaves one column at two key positions. QueryFuzzTest only
 * proves the queries it happens to draw, so a generator that stops repeating the key would leave
 * those paths untested while the run stays green.
 * <p>
 * The generator is random, so each assertion runs over a corpus; the seed is fixed so a failure
 * is reproducible.
 */
public class HorizonJoinShapeCoverageTest {
    private static final int CORPUS_SIZE = 2_000;
    private static final Pattern ON_CLAUSE = Pattern.compile(" ON \\([^)]*\\)");
    private static final String SINGLE_KEY = " ON (t.sym = p.sym)";

    @Test
    public void testBindFormDrawsTheSameRandomSequence() {
        // QueryFuzzTest regenerates a query with a BindContext from the same rnd state and needs
        // the same query shape back, so building the bind form must not draw anything extra.
        final FuzzTable master = table("m");
        final FuzzTable slave = table("s");
        final Rnd literalRnd = new Rnd(0x5eed, 0x4021);
        final Rnd bindRnd = new Rnd(0x5eed, 0x4021);
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final GeneratedQuery literal = HorizonJoinClause.generate(literalRnd, master, slave, null, false);
            final GeneratedQuery bind = HorizonJoinClause.generate(bindRnd, master, slave, new BindContext(new Rnd(i, i), 50), false);
            Assert.assertEquals("query " + i + ": " + literal.sql(), literalRnd.getSeed0(), bindRnd.getSeed0());
            Assert.assertEquals("query " + i + ": " + literal.sql(), literalRnd.getSeed1(), bindRnd.getSeed1());
            Assert.assertEquals(literal.sql(), onClause(literal.sql()), onClause(bind.sql()));
            Assert.assertEquals(literal.sql(), literal.referenceSql() != null, bind.referenceSql() != null);
        }
    }

    @Test
    public void testCorpusCoversEveryKeyForm() {
        final String corpus = corpus();
        assertCorpusContains(corpus, "the single key", SINGLE_KEY);
        assertCorpusContains(corpus, "the key with its sides swapped", " ON (t.sym = p.sym AND p.sym = t.sym)");
        assertCorpusContains(corpus, "an upper case master column", " ON (t.sym = p.sym AND t.SYM = p.sym)");
        assertCorpusContains(corpus, "an upper case slave column", " ON (t.sym = p.sym AND t.sym = p.SYM)");
        assertCorpusContains(corpus, "both columns in upper case", " ON (p.SYM = t.SYM AND t.sym = p.sym)");
        assertCorpusMatches(corpus, "a non-keyed join", Pattern.compile("HORIZON JOIN s p (?:RANGE|LIST)"));
        // the aggregating join, and the projection with and without an outer aggregation
        assertCorpusMatches(corpus, "an aggregating join with a repeated key", Pattern.compile("^SELECT (?!h\\.offset AS e0, t\\.ts)(?!e0, count)[^\\n]* ON \\(t\\.sym = p\\.sym AND ", Pattern.MULTILINE));
        assertCorpusMatches(corpus, "a projection with a repeated key", Pattern.compile("^SELECT h\\.offset AS e0, t\\.ts AS e1[^\\n]* ON \\(t\\.sym = p\\.sym AND ", Pattern.MULTILINE));
        assertCorpusMatches(corpus, "an aggregated projection with a repeated key", Pattern.compile("^SELECT e0, count\\(\\*\\)[^\\n]* ON \\(t\\.sym = p\\.sym AND ", Pattern.MULTILINE));
    }

    @Test
    public void testReferenceRepeatsTheQueryWithTheSingleKey() {
        // A reference that differs from the query in anything but the key would compare two
        // different queries, and one that repeats the key too would compare the query with itself.
        final FuzzTable master = table("m");
        final FuzzTable slave = table("s");
        final Rnd rnd = new Rnd(0x5eed, 0x7e1f);
        int references = 0;
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final GeneratedQuery query = HorizonJoinClause.generate(rnd, master, slave, null, false);
            final String sql = query.sql();
            final String on = onClause(sql);
            if (on == null || SINGLE_KEY.equals(on)) {
                Assert.assertNull(sql, query.referenceSql());
                continue;
            }
            references++;
            Assert.assertEquals(sql, sql.replace(on, SINGLE_KEY), query.referenceSql());
        }
        Assert.assertTrue("the generator repeated the key in none of " + CORPUS_SIZE + " queries", references > 0);
    }

    private static void assertCorpusContains(String corpus, String what, String fragment) {
        Assert.assertTrue(
                "the generator produced no query with " + what + " in " + CORPUS_SIZE + " queries",
                corpus.contains(fragment)
        );
    }

    private static void assertCorpusMatches(String corpus, String what, Pattern pattern) {
        Assert.assertTrue(
                "the generator produced no query with " + what + " in " + CORPUS_SIZE + " queries",
                pattern.matcher(corpus).find()
        );
    }

    private static String corpus() {
        final Rnd rnd = new Rnd(0x5eed, 0xc0be);
        final FuzzTable master = table("m");
        final FuzzTable slave = table("s");
        final StringBuilder corpus = new StringBuilder();
        for (int i = 0; i < CORPUS_SIZE; i++) {
            corpus.append(HorizonJoinClause.generate(rnd, master, slave, null, false).sql()).append('\n');
        }
        return corpus.toString();
    }

    // The ON clause of the query, or null for a non-keyed join.
    private static String onClause(String sql) {
        final Matcher matcher = ON_CLAUSE.matcher(sql);
        return matcher.find() ? matcher.group() : null;
    }

    // The fuzz tables share sym and ts, see HorizonJoinClause.
    private static FuzzTable table(String name) {
        final ObjList<FuzzColumn> columns = new ObjList<>();
        columns.add(new FuzzColumn("sym", SymbolType.INSTANCE));
        columns.add(new FuzzColumn("c_int", IntType.INSTANCE));
        columns.add(new FuzzColumn("c_vc", VarcharType.INSTANCE));
        columns.add(new FuzzColumn("c_dbl", DoubleType.INSTANCE));
        columns.add(new FuzzColumn("ts", TimestampType.INSTANCE));
        return new FuzzTable(name, columns, "ts");
    }
}
