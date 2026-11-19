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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
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

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pins the star joins that {@link EquiJoinClause} draws for QueryFuzzTest, whose parallel join axis
 * compares each query with the parallel hash join on and off, in order: joins after the first one,
 * stacked by the operator into stages, of both types in both orders, keyed on the left input or,
 * for mixed plans, on an earlier join's input; duplicate keys in several joins; residuals and
 * post-join filters over later joins; window functions and orders over post-filtered stars; and
 * first joins that may swap their inputs. QueryFuzzTest only proves the queries it happens to draw,
 * so a generator that stops emitting any of these leaves the operator's paths for them unchecked
 * while the run stays green. {@link EquiJoinShapeCoverageTest} pins the shape of a single join.
 * <p>
 * A query that states its LIMIT in one of the generator's LIMIT forms, see
 * {@link EquiJoinClause#limitForm(String)}, is taken apart as the query that the form wraps, whose
 * joins the form's LIMIT reaches; the corpus pins every form over a star.
 * <p>
 * The generator is random, so each assertion runs over a corpus; the seeds are fixed so a failure
 * is reproducible.
 */
public class EquiJoinStarShapeCoverageTest extends AbstractCairoTest {
    private static final Pattern ALIAS = Pattern.compile("\\b(l|r\\d*)\\.");
    private static final int CORPUS_SIZE = 4_000;
    // Rows that a star query over three fuzz tables of their largest size may return. The fan-out
    // budget keeps the estimate of a query to about 3_600 rows, of which the test's seed draws up to
    // about 5_100; a star without the budget reaches tens of thousands with two joins and hundreds of
    // thousands with three: three joins on the SYMBOL key alone multiply 150 rows by 17 three times.
    private static final int MAX_STAR_ROWS = 10_000;
    private static final Pattern ON_WINDOW_ORDER = Pattern.compile(" OVER \\((?:PARTITION BY \\S+ )?ORDER BY ");
    // Queries the budget test draws, about half of them stars.
    private static final int STAR_BUDGET_DRAWS = 1_000;
    private static final Pattern SINGLE_KEY = Pattern.compile("^(?:l|r\\d*)\\.(?:k|sym|lk|vk)(?:_v)? = (?:l|r\\d*)\\.(?:k|sym|lk|vk)(?:_v)?(?: AND \\(.*)?$");

    @Test
    public void testBareLimitKeepsTheLeftInputUnfiltered() {
        // As for a single join (see EquiJoinShapeCoverageTest), a LIMIT without ORDER BY never
        // meets a filter on the left input, which the optimiser would push the LIMIT into, below the
        // joins (questdb/questdb#7982): an INNER join's residual over the left input alone, which
        // becomes a WHERE conjunct, or a WHERE conjunct over the left input alone.
        final Rnd rnd = new Rnd(0x5eed, 0x7983);
        final ObjList<FuzzTable> tables = tables();
        int bareStars = 0;
        int orderedOverFilteredLeft = 0;
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final String sql = EquiJoinClause.generate(rnd, tables.getQuick(0), tables.getQuick(1), tables, null, false).sql();
            if (!Pattern.compile(" LIMIT (?:\\d+, )?-?\\d+$").matcher(sql).find()) {
                continue;
            }
            final Query query = new Query(sql);
            final boolean isLeftFiltered = query.isLeftProjectionFiltered() || query.hasLeftOnlyCondition();
            if (query.orderBy != null) {
                orderedOverFilteredLeft += isLeftFiltered ? 1 : 0;
                continue;
            }
            Assert.assertFalse("a bare LIMIT over a filtered left input: " + sql, isLeftFiltered);
            // A LIMIT stated on the star itself: a LIMIT form, which reaches the joins from outside
            // their own query, has pins of its own.
            bareStars += query.joins.size() > 1 && query.limitForm == EquiJoinClause.LIMIT_FORM_NONE ? 1 : 0;
        }
        Assert.assertTrue("bare LIMIT over stars: " + bareStars, bareStars > 0);
        Assert.assertTrue("ordered LIMIT over a filtered left input: " + orderedOverFilteredLeft, orderedOverFilteredLeft > 0);
    }

    @Test
    public void testBindFormDrawsTheSameRandomSequence() {
        // QueryFuzzTest regenerates a query with a BindContext from the same rnd state and needs
        // the same query back, so building the bind form must not draw anything extra: the joins,
        // their types, aliases and keys, the window functions, the order and the LIMIT form stay
        // the same.
        final ObjList<FuzzTable> tables = tables();
        final Rnd literalRnd = new Rnd(0x5eed, 0x9507);
        final Rnd bindRnd = new Rnd(0x5eed, 0x9507);
        int bound = 0;
        int boundStars = 0;
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final GeneratedQuery literal = EquiJoinClause.generate(literalRnd, tables.getQuick(1), tables.getQuick(2), tables, null, false);
            final BindContext ctx = new BindContext(new Rnd(i, i), 50);
            final GeneratedQuery bind = EquiJoinClause.generate(bindRnd, tables.getQuick(1), tables.getQuick(2), tables, ctx, false);
            Assert.assertEquals("query " + i + ": " + literal.sql(), literalRnd.getSeed0(), bindRnd.getSeed0());
            Assert.assertEquals("query " + i + ": " + literal.sql(), literalRnd.getSeed1(), bindRnd.getSeed1());
            Assert.assertEquals(literal.deterministic(), bind.deterministic());
            final Query literalQuery = new Query(literal.sql());
            final Query bindQuery = new Query(bind.sql());
            // The LIMIT forms draw from a generator of their own, which the seeds above do not
            // show, and Query reads the query that a form wraps: both forms of the query take the
            // same LIMIT form, with the same ASOF or LT join, keyed and under a TOLERANCE alike.
            Assert.assertEquals(literal.sql(), literalQuery.limitForm, bindQuery.limitForm);
            Assert.assertEquals(literal.sql(), EquiJoinClause.limitFormText(literal.sql()), EquiJoinClause.limitFormText(bind.sql()));
            Assert.assertEquals(literal.sql(), literalQuery.joins.size(), bindQuery.joins.size());
            for (int j = 0, n = literalQuery.joins.size(); j < n; j++) {
                final Join literalJoin = literalQuery.joins.getQuick(j);
                final Join bindJoin = bindQuery.joins.getQuick(j);
                Assert.assertEquals(literal.sql(), literalJoin.isLeftJoin, bindJoin.isLeftJoin);
                Assert.assertEquals(literal.sql(), literalJoin.alias, bindJoin.alias);
                Assert.assertEquals(literal.sql(), literalJoin.key(), bindJoin.key());
            }
            Assert.assertEquals(literal.sql(), literalQuery.windows(), bindQuery.windows());
            Assert.assertEquals(literal.sql(), literalQuery.orderBy, bindQuery.orderBy);
            if (ctx.getBindValues().size() > 0) {
                bound++;
                boundStars += literalQuery.joins.size() > 1 ? 1 : 0;
            }
        }
        Assert.assertTrue("no query bound a value", bound > 0);
        Assert.assertTrue("no star bound a value", boundStars > 0);
    }

    @Test
    public void testCorpusCoversEveryStarForm() {
        final ObjList<Query> corpus = corpus();
        // Stars of every length up to the most joins, and both join types after either.
        for (int joins = 1; joins <= EquiJoinClause.MAX_JOINS; joins++) {
            final int n = joins;
            assertCorpusHas(corpus, joins + " joins", q -> q.joins.size() == n);
        }
        assertCorpusHas(corpus, "a LEFT join after an INNER one", q -> q.hasJoinPair(false, true));
        assertCorpusHas(corpus, "an INNER join after a LEFT one", q -> q.hasJoinPair(true, false));
        assertCorpusHas(corpus, "INNER joins only", q -> q.joins.size() > 2 && q.countJoins(true) == 0);
        assertCorpusHas(corpus, "LEFT joins only", q -> q.joins.size() > 2 && q.countJoins(true) == q.joins.size());
        // Later joins keyed on the left input, the star the operator stacks, on every key kind; and
        // now and then on an earlier join's input, a snowflake that keeps the ordinary join.
        for (String column : new String[]{"k", "sym", "lk", "vk", "ts"}) {
            assertCorpusHas(corpus, "a later join keyed on the left input's " + column, q -> q.hasLaterKey(column, true));
        }
        assertCorpusHas(corpus, "a later join on a composite key", q -> q.hasLaterJoin(j -> j.key().contains(" AND ")));
        assertCorpusHas(corpus, "a later join keyed on an earlier join's input", q -> q.hasLaterKey(null, false));
        // Duplicate keys: two joins of one star on a single key column, whose builds repeat it.
        assertCorpusHas(corpus, "two joins on single-column keys", q -> q.countSingleKeyJoins() >= 2);
        assertCorpusHas(corpus, "three joins on single-column keys", q -> q.countSingleKeyJoins() >= 3);
        // The inputs of later joins.
        assertCorpusHas(corpus, "a later join over a narrowed dimension", q -> q.hasLaterJoin(j -> j.input.matches("\\(SELECT \\* FROM t[012] WHERE ts < '2024-01-01T0[1-6]:00:00.000000Z'\\)")));
        assertCorpusHas(corpus, "a later join over a filtered projection", q -> q.hasLaterJoin(j -> j.input.matches("\\(SELECT .* FROM t[012] WHERE .*\\)") && !j.input.startsWith("(SELECT * ")));
        assertCorpusHas(corpus, "a later join over a LIMIT barrier", q -> q.hasLaterJoin(j -> j.input.matches("\\(SELECT \\* FROM t[012] LIMIT \\d+\\)")));
        assertCorpusHas(corpus, "a later join over the left input's table", q -> q.hasLaterJoin(j -> j.input.equals(q.leftInput)));
        // Residuals of later joins: over the left input, over the join's own input, over both, and
        // over the join's input and an earlier join's, which reads that stage's payload.
        assertCorpusHas(corpus, "a later LEFT join's residual over the left input", q -> q.hasLaterResidual(true, Set.of("l")));
        assertCorpusHas(corpus, "a later LEFT join's residual over the left input and its own", q -> q.hasLaterResidualOverOwnAnd(true, "l"));
        assertCorpusHas(corpus, "a later LEFT join's residual over an earlier join's input and its own", q -> q.hasLaterResidualOverOwnAnd(true, "r"));
        assertCorpusHas(corpus, "a later INNER join's residual over an earlier join's input and its own", q -> q.hasLaterResidualOverOwnAnd(false, "r"));
        // A LEFT join's residual over its own input alone, which the join's build runs whole: on a
        // later join, and on a join that no later residual or post-join filter follows, whose stage
        // then hands its chains to the consumer.
        assertCorpusHas(corpus, "a later LEFT join's residual over its own input alone", q -> q.hasLaterJoin(j -> j.isLeftJoin && j.residual() != null && aliases(j.residual()).equals(Set.of(j.alias))));
        assertCorpusHas(corpus, "a LEFT join's residual over its own input alone with no later residual or post-join filter", Query::hasLastResidualOverOwnInput);
        // Post-join filters of later joins: a WHERE conjunct over a later join's input and another
        // input, or over a later LEFT join's input alone, and the misses of a later LEFT join.
        assertCorpusHas(corpus, "a post-join filter over a later join and the left input", q -> q.hasLaterPostFilter("l"));
        assertCorpusHas(corpus, "a post-join filter over two joined inputs", q -> q.hasLaterPostFilter("r"));
        assertCorpusHas(corpus, "a later LEFT join's misses alone", q -> q.hasConjunct("\\(r\\d+\\.k(?:_v)? IS NULL\\)"));
        assertCorpusHas(corpus, "an interval over a later join's input", q -> q.hasConjunct("\\(r\\d+\\.ts >= '2024-01-.*"));
        // Window functions over the joined rows, in every order the generator draws.
        for (String function : new String[]{"row_number()", "rank()", "dense_rank()", "count(*)", "sum(", "lag(", "first_value("}) {
            assertCorpusHas(corpus, "the window function " + function, q -> q.windows().contains(function));
        }
        assertCorpusHas(corpus, "a window without an order", q -> q.windows().contains(" OVER ()") || q.windows().matches(".* OVER \\(PARTITION BY \\S+\\).*"));
        assertCorpusHas(corpus, "a partitioned window", q -> q.windows().contains(" OVER (PARTITION BY "));
        assertCorpusHas(corpus, "a window in the left input's order", q -> q.windows().matches(".*ORDER BY l\\.ts\\).*"));
        assertCorpusHas(corpus, "a window in the left input's order descending", q -> q.windows().contains("ORDER BY l.ts DESC)"));
        assertCorpusHas(corpus, "a window in a joined input's order", q -> q.windows().matches(".*ORDER BY r\\d*\\.ts.*"));
        // Section 13 item 1 of the design: post-filtered stars under ORDER BY and under window
        // functions, which ask the joined rows for their order.
        assertCorpusHas(corpus, "a post-filtered star under a window function in the left input's order",
                q -> q.joins.size() > 1 && q.hasPostFilter() && ON_WINDOW_ORDER.matcher(q.select).find() && q.windows().contains("ORDER BY l.ts"));
        assertCorpusHas(corpus, "a post-filtered star under a window function without an order",
                q -> q.joins.size() > 1 && q.hasPostFilter() && q.windows().contains(" OVER ()"));
        assertCorpusHas(corpus, "a post-filtered star under ORDER BY the left input's timestamp",
                q -> q.joins.size() > 1 && q.hasPostFilter() && "l.ts".equals(q.orderBy));
        assertCorpusHas(corpus, "a post-filtered star under ORDER BY the left input's timestamp descending",
                q -> q.joins.size() > 1 && q.hasPostFilter() && "l.ts DESC".equals(q.orderBy));
        assertCorpusHas(corpus, "a post-filtered star under ORDER BY a window function",
                q -> q.joins.size() > 1 && q.hasPostFilter() && q.orderBy != null && q.orderBy.startsWith("c") && q.windowAliases().contains(q.orderBy.split(" ")[0]));
        // Limits stated on stars themselves.
        assertCorpusHas(corpus, "a LIMIT over a star", q -> q.joins.size() > 1 && q.limitForm == EquiJoinClause.LIMIT_FORM_NONE && q.limit != null && !q.limit.startsWith("-"));
        assertCorpusHas(corpus, "a negative LIMIT over a star", q -> q.joins.size() > 1 && q.limitForm == EquiJoinClause.LIMIT_FORM_NONE && q.limit != null && q.limit.startsWith("-"));
        assertCorpusHas(corpus, "a range LIMIT over a star", q -> q.joins.size() > 1 && q.limitForm == EquiJoinClause.LIMIT_FORM_NONE && q.limit != null && q.limit.contains(", "));
        // Every LIMIT form over a star, whose LIMIT reaches the star's joins from outside their own
        // query, or through an operator over them.
        assertCorpusHas(corpus, "a LIMIT over a sub-query of a star", q -> q.joins.size() > 1 && q.limitForm == EquiJoinClause.LIMIT_FORM_SUB_QUERY);
        assertCorpusHas(corpus, "a LIMIT over a CTE of a star", q -> q.joins.size() > 1 && q.limitForm == EquiJoinClause.LIMIT_FORM_CTE);
        assertCorpusHas(corpus, "a LIMIT over a UNION ALL of a star with itself", q -> q.joins.size() > 1 && q.limitForm == EquiJoinClause.LIMIT_FORM_UNION_ALL);
        assertCorpusHas(corpus, "a LIMIT over an ASOF join whose master is a star", q -> q.joins.size() > 1 && q.limitForm == EquiJoinClause.LIMIT_FORM_AS_OF);
        assertCorpusHas(corpus, "a LIMIT over an LT join whose master is a star", q -> q.joins.size() > 1 && q.limitForm == EquiJoinClause.LIMIT_FORM_LT);
        assertCorpusHas(corpus, "a LIMIT over a star under a WHERE that holds no column", q -> q.joins.size() > 1 && q.limitForm == EquiJoinClause.LIMIT_FORM_RUNTIME_CONSTANT_FILTER);
    }

    @Test
    public void testFirstJoinsSwapBothWays() {
        // The INNER first joins that may swap their inputs at execution: two unfiltered tables, no
        // condition that the optimiser hands to either input's scan, and no interval on either.
        // They come over a pair of distinct tables, in both orders, whose sizes differ (see
        // FuzzConfig.getTableRows()), and over one table joined with itself, whose sizes are equal.
        final ObjList<Query> corpus = corpus();
        final Set<String> pairs = new HashSet<>();
        int selfJoins = 0;
        int stars = 0;
        int ordered = 0;
        int unordered = 0;
        for (int i = 0, n = corpus.size(); i < n; i++) {
            final Query q = corpus.getQuick(i);
            // A LIMIT form puts a parent over the joins that may ask the joined rows for their order
            // itself, as an ASOF or LT join does, which the classification by order below does not
            // model.
            if (q.limitForm != EquiJoinClause.LIMIT_FORM_NONE || !q.isSwapCandidateShape()) {
                continue;
            }
            final String right = q.joins.getQuick(0).input;
            if (right.equals(q.leftInput)) {
                selfJoins++;
            } else {
                pairs.add(q.leftInput + '/' + right);
            }
            stars += q.joins.size() > 1 ? 1 : 0;
            // A parent that asks the joined rows for their order keeps the inputs; one that does not
            // lets the sizes decide.
            final boolean isOrdered = "l.ts".equals(q.orderBy) || "l.ts DESC".equals(q.orderBy) || ON_WINDOW_ORDER.matcher(q.select).find();
            ordered += isOrdered ? 1 : 0;
            unordered += isOrdered ? 0 : 1;
        }
        Assert.assertTrue("self-joins: " + selfJoins, selfJoins > 0);
        for (String pair : new String[]{"t0/t1", "t1/t0", "t0/t2", "t2/t0", "t1/t2", "t2/t1"}) {
            Assert.assertTrue("no swap-prone first join of " + pair + " in " + pairs, pairs.contains(pair));
        }
        Assert.assertTrue("swap-prone stars: " + stars, stars > 0);
        Assert.assertTrue("swap-prone joins under an order: " + ordered, ordered > 0);
        Assert.assertTrue("swap-prone joins without an order: " + unordered, unordered > 0);
        // A third of the INNER first joins of the star generator are swap-prone, a sixth of all.
        Assert.assertTrue("swap-prone joins: " + (stars + selfJoins), unordered + ordered > CORPUS_SIZE / 12);
    }

    @Test
    public void testSingleJoinDrawsNoStar() {
        // The single join keeps the shape EquiJoinShapeCoverageTest pins: one join, no window. The
        // ASOF or LT join of a LIMIT form goes with the form, which Query leaves out.
        final Rnd rnd = new Rnd(0x5eed, 0xe7a3);
        final ObjList<FuzzTable> tables = tables();
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final GeneratedQuery query = EquiJoinClause.generate(rnd, tables.getQuick(0), tables.getQuick(1), null, false);
            final Query q = new Query(query.sql());
            Assert.assertEquals(query.sql(), 1, q.joins.size());
            Assert.assertEquals(query.sql(), "", q.windows());
        }
    }

    @Test
    public void testStarOutputStaysWithinBudget() throws Exception {
        // Three tables shaped as the fuzz tables are, at their largest: 149, 150 and 151 rows of
        // the same key columns, drawn by the same rnd calls. No star the generator draws over them
        // returns more than MAX_STAR_ROWS rows, while a star with duplicate keys in two joins still
        // returns more rows than its left input holds. A star in a LIMIT form runs in that form.
        assertMemoryLeak(() -> {
            final ObjList<FuzzTable> tables = tables();
            for (int i = 0; i < tables.size(); i++) {
                final StringBuilder ddl = new StringBuilder("CREATE TABLE t").append(i).append(" AS (SELECT ");
                final ObjList<FuzzColumn> columns = tables.getQuick(i).getColumns();
                for (int c = 0, n = columns.size(); c < n; c++) {
                    final FuzzColumn column = columns.getQuick(c);
                    if (c > 0) {
                        ddl.append(", ");
                    }
                    ddl.append(column.getName().equals("ts") ? "timestamp_sequence('2024-01-01', 1_800_000_000L)" : column.getType().getRndCall())
                            .append(' ').append(column.getName());
                }
                ddl.append(" FROM long_sequence(").append(149 + i).append(")) TIMESTAMP(ts) PARTITION BY DAY");
                execute(ddl.toString());
            }
            final Rnd rnd = new Rnd(0x5eed, 0xb0d6);
            int stars = 0;
            long maxRows = 0;
            int multiplied = 0;
            for (int i = 0; i < STAR_BUDGET_DRAWS; i++) {
                final String sql = EquiJoinClause.generate(rnd, tables.getQuick(rnd.nextInt(3)), tables.getQuick(rnd.nextInt(3)), tables, null, false).sql();
                final Query q = new Query(sql);
                if (q.joins.size() < 2) {
                    continue;
                }
                long rows = 0;
                try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        rows++;
                    }
                } catch (SqlException e) {
                    // A generated condition that does not compile fails the query in every plan.
                    continue;
                }
                stars++;
                maxRows = Math.max(maxRows, rows);
                multiplied += q.countSingleKeyJoins() >= 2 && rows > 151 ? 1 : 0;
                Assert.assertTrue(rows + " rows: " + sql, rows <= MAX_STAR_ROWS);
            }
            Assert.assertTrue("stars: " + stars, stars > STAR_BUDGET_DRAWS / 4);
            Assert.assertTrue("max rows: " + maxRows, maxRows > 1_000);
            Assert.assertTrue("stars whose duplicate keys multiplied their rows: " + multiplied, multiplied > 0);
        });
    }

    @Test
    public void testWindowsAndLimitsMakeQueryNonDeterministic() {
        // A LIMIT without a total order may keep other rows on the storage shadow, and a window
        // function numbers rows that tie on its order in the order the join returns them, which the
        // shadow may change, so only the parallel join axis compares the rows of such a query.
        final ObjList<FuzzTable> tables = tables();
        final Rnd rnd = new Rnd(0x5eed, 0x1e48);
        int windows = 0;
        int deterministic = 0;
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final GeneratedQuery query = EquiJoinClause.generate(rnd, tables.getQuick(2), tables.getQuick(0), tables, null, false);
            final Query q = new Query(query.sql());
            final boolean hasWindow = !q.windows().isEmpty();
            Assert.assertEquals(query.sql(), q.limit == null && !hasWindow, query.deterministic());
            windows += hasWindow ? 1 : 0;
            deterministic += query.deterministic() ? 1 : 0;
        }
        Assert.assertTrue(windows > 0);
        Assert.assertTrue(deterministic > CORPUS_SIZE / 2);
    }

    private static void assertCorpusHas(ObjList<Query> corpus, String what, java.util.function.Predicate<Query> predicate) {
        for (int i = 0, n = corpus.size(); i < n; i++) {
            if (predicate.test(corpus.getQuick(i))) {
                return;
            }
        }
        Assert.fail("the generator produced no query with " + what + " in " + CORPUS_SIZE + " queries");
    }

    private static Set<String> aliases(String condition) {
        final Set<String> aliases = new HashSet<>();
        final Matcher matcher = ALIAS.matcher(condition);
        while (matcher.find()) {
            aliases.add(matcher.group(1));
        }
        return aliases;
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

    private static ObjList<Query> corpus() {
        final Rnd rnd = new Rnd(0x5eed, 0xe7a4);
        final ObjList<FuzzTable> tables = tables();
        final ObjList<Query> corpus = new ObjList<>();
        for (int i = 0; i < CORPUS_SIZE; i++) {
            // The left and the first right table as QueryGenerator picks them: two distinct tables.
            final int left = rnd.nextInt(3);
            final int right = (left + 1 + rnd.nextInt(2)) % 3;
            corpus.add(new Query(EquiJoinClause.generate(rnd, tables.getQuick(left), tables.getQuick(right), tables, null, i % 50 == 0).sql()));
        }
        return corpus;
    }

    // The first index of one of the texts at or after the given index outside any parentheses and
    // quotes, or -1.
    private static int indexAtDepthZero(String sql, int from, String... texts) {
        int depth = 0;
        for (int i = from; i < sql.length(); i++) {
            final char c = sql.charAt(i);
            if (c == '\'') {
                i = sql.indexOf('\'', i + 1);
            } else if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (depth == 0) {
                for (String text : texts) {
                    if (sql.startsWith(text, i)) {
                        return i;
                    }
                }
            }
        }
        return -1;
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

    private static ObjList<FuzzTable> tables() {
        final ObjList<FuzzTable> tables = new ObjList<>();
        tables.add(table("t0"));
        tables.add(table("t1"));
        tables.add(table("t2"));
        return tables;
    }

    /**
     * One join of a generated query: its type, its input's SQL, its alias and its ON condition.
     */
    private record Join(boolean isLeftJoin, String input, String alias, String on) {
        // The equalities of the ON condition, without its residual.
        String key() {
            final int residual = indexAtDepthZero(on, 0, " AND (");
            return residual < 0 ? on : on.substring(0, residual);
        }

        // The residual of the ON condition, without its parentheses, or null.
        String residual() {
            final int residual = indexAtDepthZero(on, 0, " AND (");
            return residual < 0 ? null : on.substring(residual + 6, on.length() - 1);
        }
    }

    /**
     * A generated query, taken apart at the top level: the projection, the left input, the joins,
     * the WHERE conjuncts, the ORDER BY and the LIMIT. A query in a LIMIT form is taken apart as
     * the query that the form wraps, see {@link EquiJoinClause#unwrapLimitForm(String)}, and keeps
     * its form in {@code limitForm}.
     */
    private static final class Query {
        final ObjList<String> conjuncts = new ObjList<>();
        final ObjList<Join> joins = new ObjList<>();
        final String leftInput;
        final String limit;
        // One of the LIMIT_FORM_* values of EquiJoinClause.
        final int limitForm;
        final String orderBy;
        final String select;
        final String sql;

        Query(String sql) {
            this.sql = sql;
            limitForm = EquiJoinClause.limitForm(sql);
            final String query = EquiJoinClause.unwrapLimitForm(sql);
            final int from = indexAtDepthZero(query, 0, " FROM ");
            Assert.assertTrue(sql, from > 0);
            select = query.substring("SELECT ".length(), from);
            final int whereAt = indexAtDepthZero(query, from, " WHERE ");
            final int orderAt = indexAtDepthZero(query, from, " ORDER BY ");
            final int limitAt = indexAtDepthZero(query, from, " LIMIT ");
            final int joinsEnd = firstOf(query.length(), whereAt, orderAt, limitAt);
            int at = indexAtDepthZero(query, from, " LEFT JOIN ", " JOIN ");
            Assert.assertTrue(sql, at > 0);
            final String left = query.substring(from + 6, at);
            leftInput = left.substring(0, left.length() - " l".length());
            Assert.assertTrue(sql, left.endsWith(" l"));
            while (at > 0 && at < joinsEnd) {
                final boolean isLeftJoin = query.startsWith(" LEFT JOIN ", at);
                final int inputStart = at + (isLeftJoin ? " LEFT JOIN ".length() : " JOIN ".length());
                final int onAt = indexAtDepthZero(query, inputStart, " ON ");
                final String inputAndAlias = query.substring(inputStart, onAt);
                final int space = inputAndAlias.lastIndexOf(' ');
                int next = indexAtDepthZero(query, onAt + 4, " LEFT JOIN ", " JOIN ");
                if (next < 0 || next > joinsEnd) {
                    next = joinsEnd;
                }
                joins.add(new Join(isLeftJoin, inputAndAlias.substring(0, space), inputAndAlias.substring(space + 1), query.substring(onAt + 4, next)));
                at = next < joinsEnd ? next : -1;
            }
            if (whereAt > 0) {
                final int end = firstOf(query.length(), orderAt, limitAt);
                int open = whereAt + " WHERE ".length();
                if (query.startsWith("test_fault() AND ", open)) {
                    open += "test_fault() AND ".length();
                }
                while (open < end && query.charAt(open) == '(') {
                    final int close = closing(query, open);
                    conjuncts.add(query.substring(open, close + 1));
                    open = close + 1;
                    if (query.startsWith(" AND ", open)) {
                        open += " AND ".length();
                    }
                }
            }
            orderBy = orderAt > 0 ? query.substring(orderAt + " ORDER BY ".length(), limitAt > 0 ? limitAt : query.length()) : null;
            limit = limitAt > 0 ? query.substring(limitAt + " LIMIT ".length()) : null;
        }

        private static int firstOf(int otherwise, int... indexes) {
            int first = otherwise;
            for (int index : indexes) {
                if (index > 0) {
                    first = Math.min(first, index);
                }
            }
            return first;
        }

        int countJoins(boolean isLeftJoin) {
            int count = 0;
            for (int i = 0, n = joins.size(); i < n; i++) {
                count += joins.getQuick(i).isLeftJoin == isLeftJoin ? 1 : 0;
            }
            return count;
        }

        int countSingleKeyJoins() {
            int count = 0;
            for (int i = 0, n = joins.size(); i < n; i++) {
                count += SINGLE_KEY.matcher(joins.getQuick(i).key()).matches() ? 1 : 0;
            }
            return count;
        }

        boolean hasJoinPair(boolean isFirstLeft, boolean isSecondLeft) {
            for (int i = 1, n = joins.size(); i < n; i++) {
                if (joins.getQuick(i - 1).isLeftJoin == isFirstLeft && joins.getQuick(i).isLeftJoin == isSecondLeft) {
                    return true;
                }
            }
            return false;
        }

        boolean hasLaterJoin(java.util.function.Predicate<Join> predicate) {
            for (int i = 1, n = joins.size(); i < n; i++) {
                if (predicate.test(joins.getQuick(i))) {
                    return true;
                }
            }
            return false;
        }

        // A later join whose key reads the left input's column, or, with isOnLeftInput false, whose
        // key reads an earlier join's input.
        boolean hasLaterKey(String column, boolean isOnLeftInput) {
            return hasLaterJoin(j -> {
                final Set<String> aliases = aliases(j.key());
                if (!aliases.contains(j.alias) || aliases.size() != 2) {
                    return false;
                }
                if (!isOnLeftInput) {
                    return !aliases.contains("l");
                }
                return aliases.contains("l") && Pattern.compile("\\bl\\." + column + "(?:_v)?\\b").matcher(j.key()).find();
            });
        }

        // A WHERE conjunct over a later join's input and either the left input or, with "r", an
        // earlier join's input.
        boolean hasLaterPostFilter(String other) {
            for (int i = 0, n = conjuncts.size(); i < n; i++) {
                final Set<String> aliases = aliases(conjuncts.getQuick(i));
                if (aliases.size() != 2) {
                    continue;
                }
                boolean hasLater = false;
                boolean hasOther = false;
                for (String alias : aliases) {
                    hasLater |= alias.matches("r\\d+");
                    hasOther |= other.equals("l") ? alias.equals("l") : alias.startsWith("r");
                }
                if (hasLater && hasOther && (other.equals("l") || !aliases.contains("l"))) {
                    return true;
                }
            }
            return false;
        }

        // A later join of the given type whose residual reads exactly the given aliases.
        boolean hasLaterResidual(boolean isLeftJoin, Set<String> aliases) {
            return hasLaterJoin(j -> j.isLeftJoin == isLeftJoin && j.residual() != null && aliases(j.residual()).equals(aliases));
        }

        // A later join of the given type whose residual reads its own input and either the left
        // input or, with "r", an earlier join's input.
        boolean hasLaterResidualOverOwnAnd(boolean isLeftJoin, String other) {
            return hasLaterJoin(j -> {
                if (j.isLeftJoin != isLeftJoin || j.residual() == null) {
                    return false;
                }
                final Set<String> aliases = aliases(j.residual());
                if (aliases.size() != 2 || !aliases.contains(j.alias)) {
                    return false;
                }
                for (String alias : aliases) {
                    if (!alias.equals(j.alias)) {
                        return other.equals("l") ? alias.equals("l") : alias.startsWith("r");
                    }
                }
                return false;
            });
        }

        boolean hasConjunct(String regex) {
            for (int i = 0, n = conjuncts.size(); i < n; i++) {
                if (conjuncts.getQuick(i).matches(regex)) {
                    return true;
                }
            }
            return false;
        }

        // True when an INNER join's residual, which becomes a WHERE conjunct, or a WHERE conjunct
        // reads the left input alone and is no interval over its timestamp, which the scan runs
        // instead of a filter.
        boolean hasLeftOnlyCondition() {
            for (int i = 0, n = joins.size(); i < n; i++) {
                final Join join = joins.getQuick(i);
                if (!join.isLeftJoin && join.residual() != null && aliases(join.residual()).equals(Set.of("l"))) {
                    return true;
                }
            }
            for (int i = 0, n = conjuncts.size(); i < n; i++) {
                final String conjunct = conjuncts.getQuick(i);
                if (!conjunct.startsWith("(l.ts >= '2024-01-") && aliases(conjunct).equals(Set.of("l"))) {
                    return true;
                }
            }
            return false;
        }

        // A WHERE conjunct that filters joined rows: one over two inputs, or one over a LEFT join's
        // input, which the join's misses reach.
        boolean hasPostFilter() {
            for (int i = 0, n = conjuncts.size(); i < n; i++) {
                final Set<String> aliases = aliases(conjuncts.getQuick(i));
                if (aliases.size() > 1) {
                    return true;
                }
                for (int j = 0, m = joins.size(); j < m; j++) {
                    final Join join = joins.getQuick(j);
                    if (join.isLeftJoin && aliases.contains(join.alias)) {
                        return true;
                    }
                }
            }
            return false;
        }

        // True when the last join with a residual is a LEFT join whose residual reads its own input
        // alone, and no WHERE conjunct filters joined rows.
        boolean hasLastResidualOverOwnInput() {
            for (int i = joins.size() - 1; i > -1; i--) {
                final Join join = joins.getQuick(i);
                final String residual = join.residual();
                if (residual != null) {
                    return join.isLeftJoin && aliases(residual).equals(Set.of(join.alias)) && !hasPostFilter();
                }
            }
            return false;
        }

        // True when the left input is a projection with a WHERE clause of its own.
        boolean isLeftProjectionFiltered() {
            return leftInput.startsWith("(") && indexAtDepthZero(leftInput.substring(1, leftInput.length() - 1), 0, " WHERE ") > 0;
        }

        // An INNER first join of two plain tables, with no condition over the left input alone or
        // the first join's input alone, which the optimiser would hand to that input's scan.
        boolean isSwapCandidateShape() {
            final Join first = joins.getQuick(0);
            if (first.isLeftJoin || !leftInput.matches("t\\d") || !first.input.matches("t\\d")) {
                return false;
            }
            for (int i = 0, n = joins.size(); i < n; i++) {
                final Join join = joins.getQuick(i);
                final String residual = join.residual();
                if (residual == null || join.isLeftJoin) {
                    continue;
                }
                final Set<String> aliases = aliases(residual);
                if (aliases.equals(Set.of("l")) || aliases.equals(Set.of("r"))) {
                    return false;
                }
            }
            for (int i = 0, n = conjuncts.size(); i < n; i++) {
                final Set<String> aliases = aliases(conjuncts.getQuick(i));
                if (aliases.equals(Set.of("l")) || aliases.equals(Set.of("r"))) {
                    return false;
                }
            }
            return true;
        }

        // The aliases of the window functions' columns.
        Set<String> windowAliases() {
            final Set<String> aliases = new HashSet<>();
            final Matcher matcher = Pattern.compile("\\) AS (c\\d+)").matcher(select);
            while (matcher.find()) {
                aliases.add(matcher.group(1));
            }
            return aliases;
        }

        // The window functions of the projection, or an empty string.
        String windows() {
            final StringBuilder windows = new StringBuilder();
            for (int at = select.indexOf(" OVER ("); at >= 0; at = select.indexOf(" OVER (", at + 1)) {
                final int start = select.lastIndexOf(", ", at) + 2;
                windows.append(select, start, closing(select, at + " OVER ".length()) + 1).append(';');
            }
            return windows.toString();
        }
    }
}
