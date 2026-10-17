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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.HashJoinGroupByAggregates;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinGroupByBuildChoiceRecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.assertAgainstBaseline;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.assertDifferential;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.findChoice;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.fused;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.plan;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.result;

/**
 * Fused on/off differential coverage of the key shapes that the other suites reach only partly:
 * text GROUP BY keys through the sharded merge, RIGHT joins over pairs that reconcile two key
 * types, a translated SYMBOL inside a staged key while its dictionaries grow, TIMESTAMP_NS
 * designated timestamps, empty text against NULL in mixed text keys, and every admitted
 * aggregate over a join that pairs no rows.
 */
public class HashJoinGroupByMixedKeysTest extends AbstractCairoTest {
    private static final String FUSED = "Hash Join Group By";
    private static final String[] JOINS = {" JOIN ", " LEFT JOIN ", " RIGHT JOIN "};
    // Pairs whose two sides encode differently, then composites that put a SYMBOL pair or such a
    // pair inside a staged key. A RIGHT join swaps its inputs, so each pair also swaps sides.
    private static final String[] RECONCILED_PAIRS = {
            "ka.ts=kb.tn", "ka.tn=kb.ts", "ka.str=kb.vc", "ka.vc=kb.str",
            "ka.sym=kb.str", "ka.str=kb.sym", "ka.sym=kb.vc", "ka.vc=kb.sym",
            "ka.sym=kb.sym AND ka.l=kb.l",
            "ka.i=kb.i AND ka.sym=kb.sym",
            "ka.sym=kb.sym AND ka.str=kb.vc AND ka.l=kb.l AND ka.ts=kb.tn",
            "ka.str=kb.vc AND ka.sym=kb.str",
            "ka.ts=kb.tn AND ka.tn=kb.ts",
    };

    @Test
    public void testCompositeTranslatedSymbolKeyAcrossDictionaryGrowth() throws Exception {
        // The rebinding suites re-run a lone SYMBOL key, which takes the INT layout. A SYMBOL pair
        // inside a composite key stages the translated key into the build's map instead, so a
        // factory compiled once has to translate through each execution's grown dictionaries.
        assertMemoryLeak(() -> {
            final String[] ons = {
                    "ks.sym=kt.sym AND ks.i=kt.i",
                    "ks.i=kt.i AND ks.sym=kt.sym",
                    "ks.sym=kt.sym AND ks.sym2=kt.sym2",
            };
            for (HashJoinBuildMode mode : HashJoinBuildMode.values()) {
                mode.applyProperties(node1.getConfigurationOverrides());
                for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                    createTranslatedSymbolTables();
                    try (SqlExecutionContextImpl context = context(engine, 4)) {
                        context.changePageFrameSizes(1, 2);
                        final ObjList<String> queries = new ObjList<>();
                        final ObjList<RecordCursorFactory> factories = new ObjList<>();
                        try {
                            for (String on : ons) {
                                for (String join : JOINS) {
                                    queries.add("SELECT ks.sym pk, kt.sym bk, count(*) n, count(kt.i) bi, sum(ks.v) pv, sum(kt.v) bv"
                                            + " FROM ks" + join + "kt ON " + on + " ORDER BY pk, bk");
                                    queries.add("SELECT count(*) n, count(ks.i) pi, count(kt.i) bi, sum(ks.v) pv, sum(kt.v) bv"
                                            + " FROM ks" + join + "kt ON " + on);
                                }
                            }
                            for (int i = 0, n = queries.size(); i < n; i++) {
                                final RecordCursorFactory factory = engine.select(queries.getQuick(i), context);
                                factories.add(factory);
                                final String factoryPlan = plan(factory, context);
                                Assert.assertTrue(queries.getQuick(i) + '\n' + factoryPlan, factoryPlan.contains("symbolKeyJoin: true"));
                            }
                            assertAgainstBaseline(queries, factories, context);
                            // Both dictionaries grow by dozens of texts. The new texts overlap in a
                            // different order, so each takes a different key on each side.
                            execute("""
                                    INSERT INTO ks
                                    SELECT ('n' || x)::SYMBOL, ('m' || (x % 3))::SYMBOL, (x % 2)::INT, x * 0.5,
                                        '2020-01-03'::TIMESTAMP + x * 60_000_000L
                                    FROM long_sequence(40)
                                    """);
                            execute("""
                                    INSERT INTO kt
                                    SELECT ('n' || (60 - x))::SYMBOL, ('m' || (x % 3))::SYMBOL, (x % 2)::INT, x * 0.25,
                                        '2020-01-03'::TIMESTAMP + x * 60_000_000L
                                    FROM long_sequence(50)
                                    """);
                            assertAgainstBaseline(queries, factories, context);
                            // The build gains texts the probe dictionary lacks, and the probe gains
                            // NULL keys, while every compiled factory stays the same.
                            execute("""
                                    INSERT INTO kt VALUES
                                        ('only-kt', 'p', 0, 1.5, '2020-01-04T00:00:00Z'),
                                        ('only-kt', NULL, 1, 2.5, '2020-01-04T01:00:00Z')
                                    """);
                            execute("""
                                    INSERT INTO ks VALUES
                                        (NULL, 'p', 0, 3.5, '2020-01-04T00:00:00Z'),
                                        ('n7', NULL, 1, 4.5, '2020-01-04T01:00:00Z')
                                    """);
                            assertAgainstBaseline(queries, factories, context);
                        } finally {
                            Misc.freeObjList(factories);
                        }
                    }
                    execute("DROP TABLE ks");
                    execute("DROP TABLE kt");
                }
            }
        });
    }

    @Test
    public void testEmptyTextAndNullInMixedTextKeys() throws Exception {
        // STRING, VARCHAR and SYMBOL encode an empty text and NULL differently, and a pair of two
        // of them reconciles both sides to one encoding. An empty text must match only an empty
        // text, whichever side it comes from.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ea (k INT, str STRING, vc VARCHAR, sym SYMBOL, v DOUBLE, t TIMESTAMP) TIMESTAMP(t) PARTITION BY DAY");
            execute("CREATE TABLE eb (k INT, str STRING, vc VARCHAR, sym SYMBOL, v DOUBLE, t TIMESTAMP) TIMESTAMP(t) PARTITION BY DAY");
            // ea holds more rows than eb, so JOIN builds eb, as LEFT JOIN does.
            execute("""
                    INSERT INTO ea VALUES
                        (1, '', '', '', 1.0, '2020-01-01T00:00:00Z'),
                        (2, NULL, NULL, NULL, 2.0, '2020-01-01T01:00:00Z'),
                        (3, 'a', 'a', 'a', 3.0, '2020-01-01T02:00:00Z'),
                        (4, '', NULL, 'a', 4.0, '2020-01-02T00:00:00Z'),
                        (5, NULL, '', '', 5.0, '2020-01-02T01:00:00Z'),
                        (6, ' ', ' ', ' ', 6.0, '2020-01-02T02:00:00Z'),
                        (7, 'a', '', NULL, 7.0, '2020-01-03T00:00:00Z')
                    """);
            // eb writes its SYMBOL texts in another order, so equal texts take different keys.
            execute("""
                    INSERT INTO eb VALUES
                        (1, 'a', 'a', 'a', 10.0, '2020-01-01T00:00:00Z'),
                        (2, NULL, NULL, NULL, 20.0, '2020-01-01T01:00:00Z'),
                        (3, '', '', ' ', 30.0, '2020-01-01T02:00:00Z'),
                        (4, '', '', '', 40.0, '2020-01-02T00:00:00Z'),
                        (5, ' ', NULL, '', 50.0, '2020-01-02T01:00:00Z'),
                        (6, NULL, ' ', NULL, 60.0, '2020-01-02T02:00:00Z')
                    """);
            // An empty STRING matches the empty VARCHARs of eb 3 and 4, never a NULL, and a space
            // matches only a space. NULL keys match each other, as they do in the ordinary hash join.
            assertQuery("SELECT ea.k ak, count(*) n, sum(eb.k) bks FROM ea JOIN eb ON ea.str = eb.vc ORDER BY ak")
                    .withPlanContaining(FUSED)
                    .expectSize()
                    .returns("""
                            ak\tn\tbks
                            1\t2\t7
                            2\t2\t7
                            3\t1\t1
                            4\t2\t7
                            5\t2\t7
                            6\t1\t6
                            7\t1\t1
                            """);
            final String[] columns = {"str", "vc", "sym"};
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                for (String left : columns) {
                    for (String right : columns) {
                        // A lone text pair, and the same pair staged after an INT key.
                        for (String on : new String[]{"ea." + left + "=eb." + right, "ea.k=eb.k AND ea." + left + "=eb." + right}) {
                            for (String join : JOINS) {
                                // The build keeps no text payload, so the probe's text groups: ea's, and
                                // eb's under RIGHT JOIN. The IS NULL flag tells an empty text from NULL,
                                // whatever the printer shows, and the build's k tells which rows matched.
                                final boolean isRight = join.equals(" RIGHT JOIN ");
                                final String text = isRight ? "eb." + right : "ea." + left;
                                final String other = isRight ? "ea.k" : "eb.k";
                                assertDifferential("SELECT " + text + " txt, " + text + " IS NULL isNull, " + other + " other,"
                                        + " count(*) n, sum(ea.v) av, sum(eb.v) bv"
                                        + " FROM ea" + join + "eb ON " + on + " ORDER BY txt, isNull, other", context, true);
                                assertDifferential("SELECT count(*) n, count(ea.k) ak, count(eb.k) bk, sum(ea.v) av, sum(eb.v) bv"
                                        + " FROM ea" + join + "eb ON " + on, context, true);
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testEveryAdmittedAggregateOverEmptyJoinResult() throws Exception {
        // Each aggregate's empty state must print as the ordinary plan's: the keyed shape returns
        // no rows, and the scalar one returns one row of empty states, which a merge of untouched
        // per-worker slots must not change.
        assertMemoryLeak(() -> {
            TestWorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(pool, (db, compiler, ctx) -> {
                for (String table : new String[]{"r", "p"}) {
                    db.execute("CREATE TABLE " + table + " (k INT, g SYMBOL, b BOOLEAN, y BYTE, h SHORT, c CHAR, i INT,"
                            + " l LONG, dt DATE, t TIMESTAMP, ns TIMESTAMP_NS, f FLOAT, d DOUBLE, s SYMBOL, ts TIMESTAMP)"
                            + " TIMESTAMP(ts) PARTITION BY DAY", ctx);
                }
                // r's keys and p's keys never meet, and every row has values.
                insertAggregateRows(db, ctx, "r", 0);
                insertAggregateRows(db, ctx, "p", 10);
                final ObjList<String> aggregates = new ObjList<>();
                addUnaryAggregates("r", aggregates);
                addUnaryAggregates("p", aggregates);
                addBinaryAggregates(aggregates);
                final String[] froms = {
                        // Both inputs hold rows, and no key pairs them.
                        " FROM r JOIN p ON r.k = p.k",
                        // The probe filter keeps no row of the preserved input.
                        " FROM r LEFT JOIN p ON r.k = p.k WHERE r.i < 0",
                        " FROM r RIGHT JOIN p ON r.k = p.k WHERE p.i < 0",
                };
                final ObjHashSet<Class<?>> fusedClasses = new ObjHashSet<>();
                try (SqlExecutionContextImpl context = context(db, 4)) {
                    context.changePageFrameSizes(1, 4);
                    assertEmptyChunks(aggregates, froms, context, fusedClasses);
                    // Empty tables give the build and the probe no page frames at all.
                    db.execute("TRUNCATE TABLE r", ctx);
                    db.execute("TRUNCATE TABLE p", ctx);
                    assertEmptyChunks(aggregates, new String[]{
                            " FROM r JOIN p ON r.k = p.k",
                            " FROM r LEFT JOIN p ON r.k = p.k",
                            " FROM r RIGHT JOIN p ON r.k = p.k"
                    }, context, fusedClasses);
                }
                final StringBuilder missing = new StringBuilder();
                final ObjList<Class<?>> supported = HashJoinGroupByAggregates.getSupportedClasses();
                for (int i = 0, n = supported.size(); i < n; i++) {
                    if (!fusedClasses.contains(supported.getQuick(i))) {
                        missing.append(supported.getQuick(i).getName()).append('\n');
                    }
                }
                Assert.assertEquals("admitted classes that no fused plan compiled", "", missing.toString());
            }, configuration, LOG);
        });
    }

    @Ignore("Known bug #7916 (item 9): SAMPLE BY with ORDER BY on a table-qualified column of an unaliased table throws NPE")
    @Test
    public void testOrderByQualifiedColumnAfterSampleByOverJoin() throws Exception {
        // SqlOptimiser.rewriteSampleBy() resolves ORDER BY columns against join models that carry
        // neither an alias nor a table name expression. The single-table query fails the same way,
        // with the fused path on or off.
        assertMemoryLeak(() -> {
            createTimestampNsTables();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                assertDifferential("SELECT rn.ts, pn.country, sum(rn.energy) e, count(*) n FROM rn LEFT JOIN pn"
                        + " ON rn.plant_id = pn.plant_id SAMPLE BY 1d ALIGN TO CALENDAR ORDER BY rn.ts, pn.country", context, true);
            }
        });
    }

    @Test
    public void testRightJoinReconciledKeyPairs() throws Exception {
        // A RIGHT join builds its left input and probes with the right one, so the planner swaps
        // the two sides of every pair before it reconciles their types. The general key suite
        // runs these pairs under JOIN and LEFT JOIN only.
        assertMemoryLeak(() -> {
            createReconciledKeyTables();
            for (HashJoinBuildMode mode : HashJoinBuildMode.values()) {
                mode.applyProperties(node1.getConfigurationOverrides());
                try (SqlExecutionContextImpl context = context(engine, 4)) {
                    context.changePageFrameSizes(1, mode == HashJoinBuildMode.PARALLEL ? 3 : 8);
                    for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                        for (String on : RECONCILED_PAIRS) {
                            // count(ka.i) turns a lost or invented match into a different number.
                            assertDifferential("SELECT ka.sym pk, kb.sym bk, count(*) n, count(ka.i) pi, sum(ka.v) pv, sum(kb.v) bv"
                                    + " FROM ka RIGHT JOIN kb ON " + on + " ORDER BY pk, bk", context, true);
                            assertDifferential("SELECT count(*) n, count(kb.i) bi, sum(ka.v) pv, sum(kb.v) bv"
                                    + " FROM kb RIGHT JOIN ka ON " + on, context, true);
                            // A post-join filter keeps only the rows that found no match.
                            assertDifferential("SELECT kb.sym bk, count(*) n, sum(kb.v) bv"
                                    + " FROM ka RIGHT JOIN kb ON " + on + " WHERE ka.v IS NULL ORDER BY bk", context, true);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testShardedMergeWithTextGroupByKeys() throws Exception {
        // Every other sharded fused test groups by SYMBOL or numeric keys. Text keys hash and copy
        // their bytes when the merge moves a group into its shard: NULL, empty text, inlined and
        // out-of-line VARCHAR, and multi-byte UTF-8, read from the probe and computed from both inputs.
        assertMemoryLeak(() -> {
            TestWorkerPool pool = new TestWorkerPool(4);
            TestUtils.execute(pool, (db, compiler, ctx) -> {
                db.execute("CREATE TABLE ta (k INT, vc VARCHAR, str STRING, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", ctx);
                db.execute("CREATE TABLE tb (k INT, vc VARCHAR, str STRING, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", ctx);
                db.execute("INSERT INTO ta SELECT (x % 50)::INT, " + text("x", 300, "VARCHAR") + ", " + text("x + 7", 200, "STRING")
                        + ", x * 0.25, timestamp_sequence('2020-01-01', 60_000_000) FROM long_sequence(1_500)", ctx);
                // tb misses some of ta's keys and holds keys that ta lacks.
                db.execute("INSERT INTO tb SELECT (x % 60)::INT, " + text("x + 3", 250, "VARCHAR") + ", " + text("x", 150, "STRING")
                        + ", x * 0.5, timestamp_sequence('2020-01-01', 90_000_000) FROM long_sequence(400)", ctx);
                // The build keeps no text payload, so the text keys come from the probe: ta for JOIN
                // and LEFT JOIN, where tb is the smaller input, and tb for RIGHT JOIN.
                final String[] queries = {
                        "SELECT ta.vc, count(*) n, sum(tb.v) bv FROM ta JOIN tb ON ta.k = tb.k ORDER BY ta.vc",
                        "SELECT ta.str, count(*) n, sum(tb.v) bv FROM ta JOIN tb ON ta.k = tb.k ORDER BY ta.str",
                        "SELECT ta.str, ta.vc, tb.k, count(*) n, avg(ta.v) pv FROM ta LEFT JOIN tb ON ta.k = tb.k ORDER BY ta.str, ta.vc, tb.k",
                        "SELECT tb.vc, tb.str, count(*) n, sum(ta.v) pv, count(ta.k) pk FROM ta RIGHT JOIN tb ON ta.k = tb.k ORDER BY tb.vc, tb.str",
                        "SELECT concat(ta.vc, '|', tb.k) joined, count(*) n FROM ta JOIN tb ON ta.k = tb.k ORDER BY joined",
                        "SELECT tb.k, ta.str, count(*) n, sum(ta.v + tb.v) s FROM ta JOIN tb ON ta.k = tb.k ORDER BY tb.k, ta.str",
                        // A VARCHAR join key that also groups, so the staged key and the group share bytes.
                        "SELECT ta.vc, count(*) n, sum(tb.v) bv FROM ta JOIN tb ON ta.vc = tb.vc ORDER BY ta.vc",
                };
                try (SqlExecutionContextImpl context = context(db, 4)) {
                    for (HashJoinBuildMode mode : HashJoinBuildMode.values()) {
                        mode.applyProperties(node1.getConfigurationOverrides());
                        context.changePageFrameSizes(1, mode == HashJoinBuildMode.PARALLEL ? 16 : 64);
                        for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                            for (String sql : queries) {
                                assertDifferential(sql, context, true);
                                assertSharding(sql, context, threshold == 1, mode == HashJoinBuildMode.PARALLEL);
                            }
                        }
                    }
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testTimestampNsDesignatedJoinKeysAndSampleBy() throws Exception {
        // No other fused test reads a TIMESTAMP_NS designated timestamp. SAMPLE BY buckets the
        // master's nanosecond timestamps, and designated timestamps of both units serve as join keys.
        assertMemoryLeak(() -> {
            createTimestampNsTables();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                for (HashJoinBuildMode mode : HashJoinBuildMode.values()) {
                    mode.applyProperties(node1.getConfigurationOverrides());
                    context.changePageFrameSizes(1, mode == HashJoinBuildMode.PARALLEL ? 3 : 16);
                    for (String sql : new String[]{
                            // A nanosecond master over a nanosecond and a microsecond build.
                            "SELECT rn.ts, sum(rn.energy) e, count(*) n, sum(pn.kwp) k FROM rn JOIN pn ON rn.plant_id = pn.plant_id SAMPLE BY 6h ALIGN TO CALENDAR",
                            "SELECT rn.ts, pn.country c, sum(rn.energy) e, count(*) n FROM rn LEFT JOIN pn ON rn.plant_id = pn.plant_id SAMPLE BY 1d ALIGN TO CALENDAR ORDER BY ts, c",
                            "SELECT rn.ts, sum(rn.energy) e, count(ru.energy) n FROM rn JOIN ru ON rn.plant_id = ru.plant_id SAMPLE BY 12h ALIGN TO CALENDAR",
                            // A microsecond master over a nanosecond build.
                            "SELECT ru.ts, sum(ru.energy) e, count(*) n, sum(pn.kwp) k FROM ru JOIN pn ON ru.plant_id = pn.plant_id SAMPLE BY 6h ALIGN TO CALENDAR",
                            "SELECT ru.ts, sum(ru.energy) e, count(rn.energy) n FROM ru RIGHT JOIN rn ON ru.plant_id = rn.plant_id SAMPLE BY 1d ALIGN TO CALENDAR",
                            // Designated timestamps as keys: nanos against nanos, and against micros,
                            // where rn's sub-microsecond parts must miss ru's whole microseconds.
                            "SELECT count(*) n, sum(a.energy) e, sum(b.energy) be FROM rn a JOIN rn b ON a.ts = b.ts",
                            "SELECT count(*) n, count(ru.energy) m, sum(rn.energy) e FROM rn LEFT JOIN ru ON rn.ts = ru.ts",
                            "SELECT count(*) n, count(ru.energy) m, sum(rn.energy) e FROM rn LEFT JOIN ru ON rn.ts::TIMESTAMP = ru.ts",
                            "SELECT ru.plant_id, count(*) n, count(rn.energy) m FROM rn RIGHT JOIN ru ON rn.ts = ru.ts AND rn.plant_id = ru.plant_id ORDER BY ru.plant_id",
                    }) {
                        if (sql.contains("::TIMESTAMP")) {
                            // A cast key is not a plain column pair, so this one keeps the ordinary plan.
                            assertDifferential(sql, context, false);
                        } else {
                            assertDifferential(sql, context, true);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testTimestampNsDesignatedKeysCompareInNanoseconds() throws Exception {
        // A TIMESTAMP and a TIMESTAMP_NS key compare in nanoseconds: rn's rows sit one nanosecond
        // past ru's, so none of them matches, while the same rows truncated to microseconds do.
        assertMemoryLeak(() -> {
            createTimestampNsTables();
            assertQuery("SELECT count(*) n, count(ru.energy) m FROM rn LEFT JOIN ru ON rn.ts = ru.ts")
                    .withPlanContaining(FUSED)
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            n\tm
                            96\t0
                            """);
            assertQuery("SELECT count(*) n FROM rn JOIN rn b ON rn.ts = b.ts")
                    .withPlanContaining(FUSED)
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            n
                            96
                            """);
        });
    }

    @Test
    public void testTimestampNsIntervalsChooseTheBuild() throws Exception {
        // An INNER join whose input scans a TIMESTAMP_NS interval compiles both orientations and
        // counts the interval's rows to pick the build. Bounds below a microsecond decide whether
        // an hour's first row is in.
        assertMemoryLeak(() -> {
            createTimestampNsTables();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
                final String select = "SELECT pn.country, sum(rn.energy) e, count(*) n, sum(pn.kwp) k FROM rn JOIN pn ON rn.plant_id = pn.plant_id";
                // rn's rows sit one nanosecond past every hour, so the bound at the hour's first
                // nanosecond keeps 4 rows against pn's 8 and builds rn, and one nanosecond later keeps 3.
                assertChoice(select + " WHERE rn.ts >= '2020-01-01T00:00:00.000000001' AND rn.ts < '2020-01-01T04:00:00.000000001' ORDER BY country",
                        context, true, 4);
                assertChoice(select + " WHERE rn.ts > '2020-01-01T00:00:00.000000001' AND rn.ts < '2020-01-01T04:00:00.000000001' ORDER BY country",
                        context, true, 3);
                // A day of rn holds 24 rows, so pn is the build.
                assertChoice(select + " WHERE rn.ts IN '2020-01-02' ORDER BY country", context, false, 8);
                // The interval sits on the nanosecond input pn and keeps its rows at the first 4 nanoseconds.
                assertChoice("SELECT ru.plant_id, sum(ru.energy) e, count(*) n FROM ru JOIN pn ON ru.plant_id = pn.plant_id"
                        + " WHERE pn.ts < '2020-01-01T00:00:00.000000004' ORDER BY ru.plant_id", context, false, 4);
                // Bind variables move the interval between executions of one factory.
                final String sql = select + " WHERE rn.ts >= $1 AND rn.ts < $2 ORDER BY country";
                bindInterval("2020-01-01T00", "2020-01-01T05");
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    final HashJoinGroupByBuildChoiceRecordCursorFactory choice = findChoice(factory);
                    Assert.assertNotNull(plan(factory, context), choice);
                    assertExecution(sql, factory, choice, context, true, 5);
                    bindInterval("2020-01-01T00", "2020-01-02T06");
                    assertExecution(sql, factory, choice, context, false, 8);
                    bindInterval("2020-01-03T10", "2020-01-03T13");
                    assertExecution(sql, factory, choice, context, true, 3);
                }
            }
        });
    }

    private static void addBinaryAggregates(ObjList<String> sink) {
        // Mirrors HashJoinGroupByAggregatesTest: rotating the argument types covers every type in
        // both positions and on both sides without the full cross product.
        final String[] functions = {"covar_samp", "covar_pop", "corr", "regr_slope", "regr_r2",
                "regr_intercept", "weighted_stddev_rel", "weighted_stddev_freq", "weighted_avg", "vwap"};
        final String[] arguments = {"d", "f", "l", "i", "h", "y", "dt", "t", "c"};
        for (int f = 0; f < functions.length; f++) {
            for (int a = 0; a < arguments.length; a++) {
                final String x = arguments[a];
                final String y = arguments[(a + f + 1) % arguments.length];
                sink.add(functions[f] + "(r." + x + ", p." + y + ")");
                sink.add(functions[f] + "(p." + x + ", r." + y + ")");
            }
        }
    }

    private static void addUnaryAggregates(String side, ObjList<String> sink) {
        final String x = side + ".";
        for (String decimal : new String[]{x + "i::DECIMAL(2,0)", x + "d::DECIMAL(4,1)", x + "d::DECIMAL(9,2)",
                x + "l::DECIMAL(18,2)", x + "l::DECIMAL(38,2)", x + "d::DECIMAL(60,2)"}) {
            sink.add("avg(" + decimal + ")");
            sink.add("avg(" + decimal + ", 3)");
            sink.add("sum(" + decimal + ")");
            sink.add("min(" + decimal + ")");
            sink.add("max(" + decimal + ")");
            sink.add("count(" + decimal + ")");
        }
        for (String column : new String[]{"d", "i", "l", "h"}) {
            sink.add("avg(" + x + column + ")");
        }
        for (String column : new String[]{"d", "f", "i", "l", "h"}) {
            sink.add("sum(" + x + column + ")");
        }
        sink.add("sum(" + x + "l::LONG256)");
        sink.add("ksum(" + x + "d)");
        sink.add("nsum(" + x + "d)");
        sink.add("geomean(" + x + "d + 6)");
        for (String column : new String[]{"c", "dt", "d", "f", "i", "l", "h", "t", "ts", "ns", "i::IPv4"}) {
            sink.add("min(" + x + column + ")");
            sink.add("max(" + x + column + ")");
        }
        for (String column : new String[]{"y", "h", "i", "l"}) {
            sink.add("bit_and(" + x + column + ")");
            sink.add("bit_or(" + x + column + ")");
            sink.add("bit_xor(" + x + column + ")");
        }
        sink.add("bool_and(" + x + "b)");
        sink.add("bool_or(" + x + "b)");
        for (String argument : new String[]{"d", "f", "i", "l", "s", "i::IPv4", "l::LONG256", "s::STRING", "s::VARCHAR",
                "l::GEOHASH(5b)", "l::GEOHASH(10b)", "l::GEOHASH(20b)", "l::GEOHASH(40b)"}) {
            sink.add("count(" + x + argument + ")");
        }
        sink.add("count(to_uuid(" + x + "l, " + x + "i))");
        for (String function : new String[]{"stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop",
                "skewness", "skewness_samp", "skewness_pop", "kurtosis", "kurtosis_samp", "kurtosis_pop"}) {
            sink.add(function + "(" + x + "d)");
        }
    }

    /**
     * Compiles the query with the fused plan on and off, compares the rows twice and checks that
     * the fused plan chose the build input with {@code buildRows} rows.
     */
    private static void assertChoice(String sql, SqlExecutionContextImpl context, boolean isAlternate, long buildRows) throws Exception {
        try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
            final HashJoinGroupByBuildChoiceRecordCursorFactory choice = findChoice(factory);
            Assert.assertNotNull(sql + '\n' + plan(factory, context), choice);
            for (int i = 0; i < 2; i++) {
                assertExecution(sql, factory, choice, context, isAlternate, buildRows);
            }
        }
    }

    private static void assertEmptyChunks(
            ObjList<String> aggregates,
            String[] froms,
            SqlExecutionContextImpl context,
            ObjHashSet<Class<?>> fusedClasses
    ) throws Exception {
        final int chunkSize = 32;
        for (int lo = 0, n = aggregates.size(); lo < n; lo += chunkSize) {
            final StringBuilder select = new StringBuilder("count() pairs");
            for (int i = lo, hi = Math.min(lo + chunkSize, n); i < hi; i++) {
                select.append(", ").append(aggregates.getQuick(i));
            }
            for (String from : froms) {
                for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                    assertEmptyResult("SELECT " + select + from, context, fusedClasses, true);
                    assertEmptyResult("SELECT r.g rg, p.g pg, " + select + from + " ORDER BY rg, pg", context, fusedClasses, false);
                }
            }
        }
    }

    private static void assertEmptyResult(
            String sql,
            SqlExecutionContextImpl context,
            ObjHashSet<Class<?>> fusedClasses,
            boolean isScalar
    ) throws Exception {
        final String expected;
        context.setParallelHashJoinGroupByEnabled(false);
        try (RecordCursorFactory baseline = context.getCairoEngine().select(sql, context)) {
            Assert.assertFalse(sql, plan(baseline, context).contains(FUSED));
            expected = result(baseline, context);
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
        // The scalar shape prints a header and one row of empty states, the keyed one only a header.
        Assert.assertEquals(sql, isScalar ? 2 : 1, expected.split("\n").length);
        try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
            Assert.assertTrue(sql, plan(factory, context).contains(FUSED));
            final ObjList<GroupByFunction> functions = fused(factory).getAtom().getFunctions().getGroupByFunctions(-1);
            for (int i = 0, n = functions.size(); i < n; i++) {
                fusedClasses.add(functions.getQuick(i).getClass());
            }
            // The second read catches state that the first one left behind.
            Assert.assertEquals(sql, expected, result(factory, context));
            Assert.assertEquals(sql, expected, result(factory, context));
        }
        Assert.assertNull(context.getMemoryTracker());
    }

    private static void assertExecution(
            String sql,
            RecordCursorFactory factory,
            HashJoinGroupByBuildChoiceRecordCursorFactory choice,
            SqlExecutionContextImpl context,
            boolean isAlternate,
            long buildRows
    ) throws Exception {
        final String expected;
        context.setParallelHashJoinGroupByEnabled(false);
        try (RecordCursorFactory baseline = context.getCairoEngine().select(sql, context)) {
            expected = result(baseline, context);
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
        try (RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertEquals(sql, isAlternate, choice.isAlternateChosen());
            final AsyncHashJoinGroupByRecordCursorFactory chosen = isAlternate ? choice.getAlternate() : choice.getPrimary();
            final StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
            Assert.assertEquals(sql, expected, sink.toString());
            Assert.assertEquals(sql, buildRows, chosen.getAtom().getFrozenBuild().getRowCount());
        }
    }

    // Reads the whole result and checks whether the merge sharded, while the cursor still holds it.
    private static void assertSharding(String sql, SqlExecutionContextImpl context, boolean isSharded, boolean isBuiltInRounds) throws Exception {
        try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
            final AsyncHashJoinGroupByRecordCursorFactory fusedFactory = fused(factory);
            try (RecordCursor cursor = factory.getCursor(context)) {
                Assert.assertTrue(sql, cursor.hasNext());
                Assert.assertEquals(sql, isSharded, fusedFactory.getAtom().isSharded());
                Assert.assertEquals(sql, isBuiltInRounds, fusedFactory.getAtom().isBuiltInRounds());
            }
        }
    }

    private static void bindInterval(String lo, String hi) throws Exception {
        bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp(lo + ":00:00.000000Z"));
        bindVariableService.setTimestamp(1, MicrosFormatUtils.parseTimestamp(hi + ":00:00.000000Z"));
    }

    private static void insertAggregateRows(CairoEngine db, SqlExecutionContext ctx, String table, int keyBase) throws Exception {
        // CHAR holds digits, so a CHAR argument converts to DOUBLE, and no column holds NULL.
        db.execute("INSERT INTO " + table + " SELECT (" + keyBase + " + x % 4)::INT, ('g' || (x % 3))::SYMBOL, x % 2 = 0,"
                + " x::BYTE, x::SHORT, (48 + x % 5)::INT::CHAR, x::INT, x, (x * 1_000)::DATE, (x * 1_000)::TIMESTAMP,"
                + " (x * 1_000_000_123)::TIMESTAMP_NS, (x * 0.25)::FLOAT, x * 0.5, ('s' || x)::SYMBOL,"
                + " (x * 3_600_000_000)::TIMESTAMP FROM long_sequence(12)", ctx);
    }

    // A text column with NULL, empty text, short inlined texts, long out-of-line texts and
    // multi-byte UTF-8 texts, over a domain of the given size.
    private static String text(String x, int domain, String type) {
        return "(CASE WHEN (" + x + ") % 13 = 0 THEN NULL WHEN (" + x + ") % 11 = 0 THEN ''"
                + " WHEN (" + x + ") % 3 = 0 THEN 'a long text that does not fit inline #' || ((" + x + ") % " + domain + ")"
                + " WHEN (" + x + ") % 5 = 0 THEN 'çé' || ((" + x + ") % " + domain + ")"
                + " ELSE 't' || ((" + x + ") % " + domain + ") END)::" + type;
    }

    private void createReconciledKeyTables() throws Exception {
        for (String name : new String[]{"ka", "kb"}) {
            execute("CREATE TABLE " + name + " (i INT, l LONG, ts TIMESTAMP, tn TIMESTAMP_NS, sym SYMBOL, str STRING,"
                    + " vc VARCHAR, v DOUBLE, t TIMESTAMP) TIMESTAMP(t) PARTITION BY DAY");
        }
        // Small overlapping domains with NULLs give both sides duplicate keys, shared keys, keys
        // the other side lacks and NULL keys. kb writes its SYMBOL texts in the reverse order, so
        // equal texts take different keys in the two tables. kb holds fewer rows than ka.
        for (String name : new String[]{"ka", "kb"}) {
            final boolean isReversed = name.equals("kb");
            execute("INSERT INTO " + name + " (i, l, ts, tn, sym, str, vc, v, t)"
                    + " SELECT rnd_int(0, 2, 1), rnd_int(0, 2, 1)::LONG, rnd_int(0, 2, 1)::LONG::TIMESTAMP,"
                    + " rnd_int(0, 2, 1)::LONG::TIMESTAMP::TIMESTAMP_NS, "
                    + (isReversed ? "rnd_symbol('c', 'b', 'a', NULL)" : "rnd_symbol('a', 'b', NULL)")
                    + ", rnd_str('a', 'b', 'c', NULL), rnd_varchar('a', 'b', NULL), x * 0.25,"
                    + " timestamp_sequence('2020-01-01', 3_600_000_000) FROM long_sequence(" + (isReversed ? 40 : 64) + ")");
        }
    }

    private void createTimestampNsTables() throws Exception {
        // rn: 96 hourly rows from 2020-01-01, each one nanosecond past its hour. ru: the same rows in
        // microseconds, on the hour. pn: 8 rows, one per plant, at the first 8 nanoseconds of 2020.
        execute("CREATE TABLE rn (plant_id INT, ts TIMESTAMP_NS, energy DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE ru (plant_id INT, ts TIMESTAMP, energy DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE pn (plant_id INT, ts TIMESTAMP_NS, country SYMBOL, kwp DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO rn
                SELECT (x % 8)::INT, (1_577_836_800_000_000_000L + (x - 1) * 3_600_000_000_000L + 1)::TIMESTAMP_NS, x::DOUBLE
                FROM long_sequence(96)
                """);
        execute("""
                INSERT INTO ru
                SELECT (x % 8)::INT, (1_577_836_800_000_000L + (x - 1) * 3_600_000_000L)::TIMESTAMP, x * 2.0
                FROM long_sequence(96)
                """);
        execute("""
                INSERT INTO pn VALUES
                    (0, '2020-01-01T00:00:00.000000000Z', 'ES', 1),
                    (1, '2020-01-01T00:00:00.000000001Z', 'ES', 2),
                    (2, '2020-01-01T00:00:00.000000002Z', 'IT', 3),
                    (3, '2020-01-01T00:00:00.000000003Z', 'IT', 4),
                    (4, '2020-01-01T00:00:00.000000004Z', 'DE', 5),
                    (5, '2020-01-01T00:00:00.000000005Z', NULL, 6),
                    (6, '2020-01-01T00:00:00.000000006Z', 'ES', NULL),
                    (NULL, '2020-01-01T00:00:00.000000007Z', 'FR', 8)
                """);
    }

    private void createTranslatedSymbolTables() throws Exception {
        // Growing a dictionary past its declared capacity changes the table's metadata version,
        // which asks the holder of a cached plan to compile again, so the default capacity stays.
        for (String name : new String[]{"ks", "kt"}) {
            execute("CREATE TABLE " + name + " (sym SYMBOL, sym2 SYMBOL, i INT, v DOUBLE, t TIMESTAMP) TIMESTAMP(t) PARTITION BY DAY");
        }
        // ks writes a, b, c, d and kt writes d, c, b, e, so no shared text shares a key.
        execute("""
                INSERT INTO ks VALUES
                    ('a', 'p', 0, 1.0, '2020-01-01T00:00:00Z'),
                    ('b', 'q', 1, 2.0, '2020-01-01T01:00:00Z'),
                    ('c', 'p', 0, 3.0, '2020-01-01T02:00:00Z'),
                    ('d', 'q', 1, 4.0, '2020-01-01T03:00:00Z'),
                    (NULL, 'p', 0, 5.0, '2020-01-02T00:00:00Z'),
                    ('b', NULL, 0, 6.0, '2020-01-02T01:00:00Z')
                """);
        execute("""
                INSERT INTO kt VALUES
                    ('d', 'q', 1, 10.0, '2020-01-01T00:00:00Z'),
                    ('c', 'p', 0, 20.0, '2020-01-01T01:00:00Z'),
                    ('b', 'q', 1, 30.0, '2020-01-01T02:00:00Z'),
                    ('e', 'p', 0, 40.0, '2020-01-01T03:00:00Z'),
                    (NULL, 'q', 0, 50.0, '2020-01-02T00:00:00Z'),
                    ('b', 'p', 0, 60.0, '2020-01-02T01:00:00Z')
                """);
    }
}
