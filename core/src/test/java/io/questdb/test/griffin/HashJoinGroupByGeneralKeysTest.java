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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.assertAgainstBaseline;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.assertDifferential;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.fused;

/**
 * SQL-level coverage of the join keys the narrow INT layout cannot carry: every key type a
 * {@link io.questdb.cairo.RecordSink} stages, every pair that reconciles to a third type, and
 * composites of them. Each query runs with the fused hash join GROUP BY on and off, so the
 * map-backed build has to select the rows the ordinary hash join selects.
 * <p>
 * {@code MapHashJoinBuildTest} pins the build itself and {@code HashJoinGroupByCandidateTest}
 * pins the planner's reconciliation and the generated key sinks; this class pins what a query
 * returns once the code generator routes such a key to the operator.
 */
@RunWith(Parameterized.class)
public class HashJoinGroupByGeneralKeysTest extends AbstractCairoTest {
    // Every pair the ordinary hash join reconciles, in both operand orders where the two sides
    // encode differently. INT and a lone SYMBOL pair take the INT layout; the rest stage a key.
    private static final String[] KEY_PAIRS = {
            "ka.i=kb.i", "ka.l=kb.l", "ka.s=kb.s", "ka.b=kb.b", "ka.c=kb.c", "ka.bo=kb.bo",
            "ka.f=kb.f", "ka.d=kb.d", "ka.dt=kb.dt", "ka.ts=kb.ts", "ka.tn=kb.tn",
            "ka.ip=kb.ip", "ka.u=kb.u", "ka.l256=kb.l256",
            "ka.g1=kb.g1", "ka.g2=kb.g2", "ka.g4=kb.g4", "ka.g8=kb.g8",
            "ka.dec8=kb.dec8", "ka.dec16=kb.dec16", "ka.dec32=kb.dec32", "ka.dec64=kb.dec64",
            "ka.dec128=kb.dec128", "ka.dec256=kb.dec256",
            "ka.sym=kb.sym", "ka.str=kb.str", "ka.vc=kb.vc",
            // Pairs that reconcile to a third type, so the two sides encode differently.
            "ka.ts=kb.tn", "ka.tn=kb.ts", "ka.str=kb.vc", "ka.vc=kb.str",
            "ka.sym=kb.str", "ka.str=kb.sym", "ka.sym=kb.vc", "ka.vc=kb.sym",
            // A column both sides leave entirely NULL, so every key is NULL.
            "ka.nul=kb.nul",
    };

    private final HashJoinBuildMode buildMode;
    private final HashJoinPayloadLayout payloadLayout;

    public HashJoinGroupByGeneralKeysTest(HashJoinPayloadLayout payloadLayout, HashJoinBuildMode buildMode) {
        this.payloadLayout = payloadLayout;
        this.buildMode = buildMode;
    }

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Collection<Object[]> parameters() {
        return HashJoinBuildMode.parameters();
    }

    @Before
    public void setUpPayloadLayoutAndBuildMode() {
        payloadLayout.apply(node1.getConfigurationOverrides());
        buildMode.apply(node1.getConfigurationOverrides(), sqlExecutionContext);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testBuildMemoryLimitAndProbeCancellationOnAStagedKey() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTables();
            // Enough rows that the map outgrows its initial capacity and the probe spans frames.
            for (String name : new String[]{"ka", "kb"}) {
                execute("INSERT INTO " + name + " (l, vc, v, t) SELECT x, ('v' || x)::VARCHAR, x*0.5, "
                        + "timestamp_sequence('2020-01-01', 100000) FROM long_sequence(20_000)");
            }
            String sql = "SELECT count(*) n, sum(kb.v) bv FROM ka JOIN kb ON ka.l=kb.l AND ka.vc=kb.vc";
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
                context.with(AllowAllSecurityContext.INSTANCE, null, null, -1, breaker);
                context.changePageFrameSizes(1, 64);
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    AsyncHashJoinGroupByRecordCursorFactory fusedFactory = fused(factory);
                    // A query limit neither the map nor the row heap fits fails the build. Both
                    // are charged to the query's tracker, so the failure credits every byte back.
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64 * 1024);
                    try {
                        try (RecordCursor ignored = factory.getCursor(context)) {
                            Assert.fail();
                        } catch (CairoException expected) {
                            Assert.assertTrue(expected.getMessage(), expected.isOutOfMemory());
                        }
                    } finally {
                        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                    }
                    Assert.assertNull(fusedFactory.getAtom().getFrozenBuild());
                    Assert.assertEquals(0, fusedFactory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    // Cancelling before the reducers take their first frame unwinds the staged-key
                    // probes, whose views own a staging buffer charged to this execution.
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        breaker.cancel();
                        cursor.hasNext();
                        Assert.fail();
                    } catch (CairoException expected) {
                        Assert.assertTrue(expected.getMessage(), expected.isCancellation());
                    }
                    Assert.assertEquals(0, fusedFactory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    Assert.assertNull(context.getMemoryTracker());
                    breaker.reset();
                    // The same factory answers the query after both failures.
                    assertAgainstBaseline(sql, factory, context);
                }
            }
        });
    }

    @Test
    public void testCompositeKeys() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTables();
            insertKeyRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                for (String on : new String[]{
                        "ka.i=kb.i AND ka.l=kb.l",
                        "ka.i=kb.i AND ka.sym=kb.sym",
                        "ka.sym=kb.sym AND ka.str=kb.vc AND ka.l=kb.l AND ka.ts=kb.tn",
                        "ka.vc=kb.str AND ka.dt=kb.dt AND ka.c=kb.c",
                        "ka.l=kb.l AND ka.g8=kb.g8",
                        "ka.dec64=kb.dec64 AND ka.u=kb.u AND ka.bo=kb.bo",
                        // One input's STRING key writes VARCHAR while the other's writes STRING,
                        // so the two inputs must not share one set of per-column encoding flags.
                        "ka.str=kb.vc AND ka.sym=kb.str",
                        "ka.ts=kb.tn AND ka.tn=kb.ts",
                }) {
                    assertKeyPair(on, context);
                }
                // Projections that put the key columns at different indexes on the two sides.
                assertDifferential("SELECT count(*) n, sum(p.v) pv FROM (SELECT v, sym, l FROM ka) p "
                        + "JOIN (SELECT l, i, sym FROM kb) b ON p.sym=b.sym AND p.l=b.l", context, true);
            }
        });
    }

    @Test
    public void testEveryReconciledKeyPair() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTables();
            insertKeyRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                    for (String on : KEY_PAIRS) {
                        assertKeyPair(on, context);
                    }
                }
            }
        });
    }

    @Test
    public void testKeyColumnTopsAndParquet() throws Exception {
        assertMemoryLeak(() -> {
            // The key columns arrive after the first partition, so its rows read NULL through a
            // column top on both the probe and the build side.
            for (String name : new String[]{"ka", "kb"}) {
                execute("CREATE TABLE " + name + " (v DOUBLE, t TIMESTAMP) TIMESTAMP(t) PARTITION BY DAY");
                execute("INSERT INTO " + name + " SELECT x*0.25, timestamp_sequence('2020-01-01', 3600000000) FROM long_sequence(12)");
                execute("ALTER TABLE " + name + " ADD COLUMN i INT");
                execute("ALTER TABLE " + name + " ADD COLUMN l LONG");
                execute("ALTER TABLE " + name + " ADD COLUMN vc VARCHAR");
                execute("ALTER TABLE " + name + " ADD COLUMN sym SYMBOL");
            }
            execute("INSERT INTO ka SELECT x*0.5, timestamp_sequence('2020-01-03', 3600000000), (x%4)::int, x%4, "
                    + "('v' || (x%4))::VARCHAR, ('s' || (x%4))::SYMBOL FROM long_sequence(24)");
            execute("INSERT INTO kb SELECT x*0.75, timestamp_sequence('2020-01-03', 3600000000), (x%5)::int, x%5, "
                    + "('v' || (x%5))::VARCHAR, ('s' || (x%5))::SYMBOL FROM long_sequence(24)");
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                // Native, then one partition of each table in parquet, then all of them.
                for (int format = 0; format < 3; format++) {
                    if (format > 0) {
                        for (String name : new String[]{"ka", "kb"}) {
                            execute("ALTER TABLE " + name + " CONVERT PARTITION TO PARQUET WHERE "
                                    + (format == 1 ? "t < '2020-01-02'" : "t < '2020-01-04'"));
                        }
                    }
                    for (String on : new String[]{"ka.l=kb.l", "ka.vc=kb.vc", "ka.sym=kb.vc",
                            "ka.l=kb.l AND ka.vc=kb.vc"}) {
                        assertKeyPair(on, context);
                    }
                }
            }
        });
    }

    @Test
    public void testStagedKeysUnderProbeFiltersAndSinkTypes() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTables();
            insertKeyRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                for (int sinkType : new int[]{0, RecordSinkFactory.SINK_TYPE_SINGLE_METHOD,
                        RecordSinkFactory.SINK_TYPE_CHUNKED,
                        RecordSinkFactory.SINK_TYPE_LOOPING}) {
                    setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, sinkType);
                    for (boolean jit : new boolean[]{false, true}) {
                        context.setJitMode(jit ? SqlJitMode.JIT_MODE_FORCE_SCALAR
                                : SqlJitMode.JIT_MODE_DISABLED);
                        for (String on : new String[]{"ka.l=kb.l", "ka.vc=kb.str", "ka.l=kb.l AND ka.sym=kb.sym"}) {
                            // The probe filter narrows the frame before the staged key is built.
                            assertDifferential("SELECT kb.sym bk, count(*) n, sum(kb.v) bv FROM ka JOIN kb ON " + on
                                    + " WHERE ka.i > 0 ORDER BY bk", context, true);
                            assertDifferential("SELECT count(*) n, sum(ka.v) pv FROM ka LEFT JOIN kb ON " + on
                                    + " WHERE ka.i > 0", context, true);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testTranslatedSymbolKeys() throws Exception {
        assertMemoryLeak(() -> {
            // Two dictionaries that hold the same texts under different keys, each with a text
            // the other lacks, and NULLs on both sides. A SYMBOL pair compares as an int wherever
            // it sits, so the probe translates its key before it looks one up or stages one.
            for (String name : new String[]{"ks", "kt"}) {
                execute("CREATE TABLE " + name + " (sym SYMBOL, sym2 SYMBOL, i INT, v DOUBLE, t TIMESTAMP) "
                        + "TIMESTAMP(t) PARTITION BY DAY");
            }
            // ks writes a, b, c, d and kt writes d, c, b, e, so no shared text shares a key,
            // 'a' is missing from kt and 'e' from ks. sym2 reverses its two texts the same way.
            execute("""
                    INSERT INTO ks VALUES
                    ('a', 'p', 0, 1.0, '2020-01-01T00:00:00Z'),
                    ('b', 'q', 1, 2.0, '2020-01-01T01:00:00Z'),
                    ('c', 'p', 0, 3.0, '2020-01-01T02:00:00Z'),
                    ('d', 'q', 1, 4.0, '2020-01-01T03:00:00Z'),
                    (NULL, 'p', 0, 5.0, '2020-01-02T00:00:00Z'),
                    ('a', NULL, 1, 6.0, '2020-01-02T01:00:00Z'),
                    ('b', 'q', 0, 7.0, '2020-01-02T02:00:00Z'),
                    ('d', NULL, 1, 8.0, '2020-01-02T03:00:00Z')""");
            execute("""
                    INSERT INTO kt VALUES
                    ('d', 'q', 1, 10.0, '2020-01-01T00:00:00Z'),
                    ('c', 'p', 0, 20.0, '2020-01-01T01:00:00Z'),
                    ('b', 'q', 1, 30.0, '2020-01-01T02:00:00Z'),
                    ('e', 'p', 0, 40.0, '2020-01-01T03:00:00Z'),
                    (NULL, 'q', 1, 50.0, '2020-01-02T00:00:00Z'),
                    ('d', NULL, 0, 60.0, '2020-01-02T01:00:00Z'),
                    ('b', 'p', 1, 70.0, '2020-01-02T02:00:00Z'),
                    ('e', 'q', 0, 80.0, '2020-01-02T03:00:00Z')""");
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                    for (String on : new String[]{
                            // The INT layout, where the reducer translates before the lookup.
                            "ks.sym=kt.sym",
                            // Staged keys, where the sink reads the translating probe record.
                            "ks.sym=kt.sym AND ks.sym2=kt.sym2",
                            "ks.i=kt.i AND ks.sym=kt.sym",
                            "ks.sym=kt.sym AND ks.i=kt.i",
                            "ks.sym2=kt.sym2 AND ks.i=kt.i AND ks.sym=kt.sym",
                    }) {
                        assertDifferential("SELECT ks.sym pk, kt.sym bk, count(*) n, count(kt.i) bi, sum(kt.v) bv "
                                + "FROM ks JOIN kt ON " + on + " ORDER BY pk, bk", context, true);
                        assertDifferential("SELECT count(*) n, count(kt.i) bi, sum(ks.v) pv, sum(kt.v) bv "
                                + "FROM ks LEFT JOIN kt ON " + on, context, true);
                        // A probe filter narrows the frame before the key is translated.
                        assertDifferential("SELECT count(*) n, count(kt.i) bi, sum(ks.v) pv "
                                + "FROM ks LEFT JOIN kt ON " + on + " WHERE ks.v > 2.0", context, true);
                    }
                }
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, Integer.MAX_VALUE);
                // Both layouts translate, so both report the translation in their plan, and a
                // pair that compares as text does not.
                for (String on : new String[]{"ks.sym=kt.sym", "ks.i=kt.i AND ks.sym=kt.sym"}) {
                    assertPlanContains("SELECT count(*) FROM ks JOIN kt ON " + on, "symbolKeyJoin: true", context);
                }
                assertPlanExcludes("SELECT count(*) FROM ks JOIN kt ON ks.i=kt.i", "symbolKeyJoin", context);
            }
        });
    }

    private static void assertPlanContains(String sql, String expected, SqlExecutionContextImpl context) throws Exception {
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            String actual = HashJoinGroupByQualificationTest.plan(factory, context);
            Assert.assertTrue(sql + "\n" + actual, actual.contains(expected));
        }
    }

    private static void assertPlanExcludes(String sql, String unexpected, SqlExecutionContextImpl context) throws Exception {
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            String actual = HashJoinGroupByQualificationTest.plan(factory, context);
            Assert.assertFalse(sql + "\n" + actual, actual.contains(unexpected));
        }
    }

    private static void assertKeyPair(String on, SqlExecutionContextImpl context) throws Exception {
        // A keyed shape with a SYMBOL payload, and a scalar one over an outer join, whose
        // count(kb.i) turns a lost match into a different number.
        assertDifferential("SELECT ka.sym pk, kb.sym bk, count(*) n, sum(kb.v) bv FROM ka JOIN kb ON " + on
                + " ORDER BY pk, bk", context, true);
        assertDifferential("SELECT count(*) n, count(kb.i) bi, sum(ka.v) pv, sum(kb.v) bv FROM ka LEFT JOIN kb ON "
                + on, context, true);
    }

    private void createKeyTables() throws Exception {
        for (String name : new String[]{"ka", "kb"}) {
            execute("CREATE TABLE " + name + " (i INT, l LONG, s SHORT, b BYTE, c CHAR, bo BOOLEAN, f FLOAT, d DOUBLE, "
                    + "dt DATE, ts TIMESTAMP, tn TIMESTAMP_NS, ip IPV4, u UUID, l256 LONG256, "
                    + "g1 GEOHASH(1c), g2 GEOHASH(3c), g4 GEOHASH(6c), g8 GEOHASH(8c), "
                    + "dec8 DECIMAL(2,1), dec16 DECIMAL(4,1), dec32 DECIMAL(9,2), dec64 DECIMAL(18,2), "
                    + "dec128 DECIMAL(38,2), dec256 DECIMAL(50,2), "
                    + "sym SYMBOL, str STRING, vc VARCHAR, nul DOUBLE, v DOUBLE, t TIMESTAMP) "
                    + "TIMESTAMP(t) PARTITION BY DAY");
        }
    }

    private void insertKeyRows() throws Exception {
        // Small overlapping domains with NULLs, so both sides carry duplicate keys, shared keys,
        // keys the other side lacks and NULL keys. nul stays NULL: every key over it is NULL.
        // The two tables draw from the same seed but write their symbol texts in a different
        // order, so equal text takes different symbol keys in ka and kb.
        for (String name : new String[]{"ka", "kb"}) {
            boolean reversed = name.equals("kb");
            execute("INSERT INTO " + name + " (i, l, s, b, c, bo, f, d, dt, ts, tn, ip, u, l256, "
                    + "g1, g2, g4, g8, dec8, dec16, dec32, dec64, dec128, dec256, sym, str, vc, v, t) "
                    + "SELECT rnd_int(0, 2, 1), rnd_int(0, 2, 1)::long, rnd_short(0, 2), rnd_byte(0, 2), "
                    + "rnd_str('a', 'b', NULL)::char, rnd_boolean(), "
                    + "rnd_int(0, 2, 1)::float, rnd_int(0, 2, 1)::double, "
                    + "rnd_int(0, 2, 1)::long::date, rnd_int(0, 2, 1)::long::timestamp, "
                    + "rnd_int(0, 2, 1)::long::timestamp::timestamp_ns, "
                    + "rnd_ipv4('10.0.0.1/30', 1), "
                    + "rnd_str('11111111-1111-1111-1111-111111111111', "
                    + "'22222222-2222-2222-2222-222222222222', NULL)::uuid, "
                    + "rnd_str('0x01', '0x02', NULL)::long256, "
                    + "rnd_str('s', 'e', NULL)::geohash(1c), rnd_str('sp0', 'ezs', NULL)::geohash(3c), "
                    + "rnd_str('sp052w', 'ezs42e', NULL)::geohash(6c), "
                    + "rnd_str('sp052w92', 'ezs42e44', NULL)::geohash(8c), "
                    + "rnd_int(0, 2, 1)::decimal(2,1), rnd_int(0, 2, 1)::decimal(4,1), "
                    + "rnd_int(0, 2, 1)::decimal(9,2), rnd_int(0, 2, 1)::decimal(18,2), "
                    + "rnd_int(0, 2, 1)::decimal(38,2), rnd_int(0, 2, 1)::decimal(50,2), "
                    + (reversed ? "rnd_symbol('b', 'a', NULL)" : "rnd_symbol('a', 'b', NULL)")
                    + ", rnd_str('a', 'b', NULL), rnd_varchar('a', 'b', NULL), "
                    + "x*0.25, timestamp_sequence('2020-01-01', 3600000000) FROM long_sequence(64)");
        }
    }
}
